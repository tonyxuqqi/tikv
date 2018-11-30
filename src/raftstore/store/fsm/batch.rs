// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! This is the core implementation of a batch system. Generally there will be two
//! different kind of FSMs in TiKV's FSM system. One is normal FSM, which usually
//! represents a peer, the other is control FSM, which usually represents something
//! that controls how the former is created or metrics are collected.

use crossbeam_channel;
use std::cell::Cell;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::mpsc::{SendError, TrySendError};
use std::sync::{Arc, Mutex};
use std::borrow::Cow;
use util::collections::HashMap;
use util::Either;
use util::mpsc;

// The FSM is notified.
const NOTIFYSTATE_NOTIFIED: usize = 0;
// The FSM is idle.
const NOTIFYSTATE_IDLE: usize = 1;
// The FSM is expected to be dropped.
const NOTIFYSTATE_DROP: usize = 2;

/// A state controls how the FSM is notified when new messages is received.
struct State<N> {
    status: AtomicUsize,
    data: AtomicPtr<N>,
}

impl<N> Drop for State<N> {
    fn drop(&mut self) {
        let ptr = self.data.swap(ptr::null_mut(), Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe { Box::from_raw(ptr) };
        }
    }
}

pub trait FsmScheduler {
    type Fsm;

    fn schedule(&self, fsm: Box<Self::Fsm>);
    fn shutdown(&self);
}

pub trait Fsm: Sized {
    type Message;

    fn notify(&self);

    fn set_mailbox(&mut self, mailbox: Cow<BasicMailbox<Self>>);
    fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>>;
}

pub struct BasicMailbox<N: Fsm> {
    sender: mpsc::LooseBoundedSender<N::Message>,
    state: Arc<State<N>>,
}

impl<N: Fsm> BasicMailbox<N> {
    #[inline]
    fn new(sender: mpsc::LooseBoundedSender<N::Message>, fsm: Box<N>) -> BasicMailbox<N> {
        BasicMailbox {
            sender,
            state: Arc::new(State {
                status: AtomicUsize::new(NOTIFYSTATE_IDLE),
                data: AtomicPtr::new(Box::into_raw(fsm)),
            }),
        }
    }

    fn maybe_catch_fsm(&self) -> Option<Box<N>> {
        match self
            .state
            .status
            .swap(NOTIFYSTATE_NOTIFIED, Ordering::AcqRel)
        {
            NOTIFYSTATE_NOTIFIED => return None,
            NOTIFYSTATE_IDLE => {}
            _ => return None,
        }

        let p = self.state.data.swap(ptr::null_mut(), Ordering::AcqRel);
        if !p.is_null() {
            Some(unsafe { Box::from_raw(p) })
        } else {
            panic!("inconsistent status and data, something should be wrong.");
        }
    }

    #[inline]
    fn notify<S: FsmScheduler<Fsm = N>>(&self, scheduler: &S) {
        match self.maybe_catch_fsm() {
            None => {}
            Some(mut n) => {
                n.set_mailbox(Cow::Borrowed(self));
                scheduler.schedule(n);
            }
        }
    }

    #[inline]
    fn release(&self, fsm: Box<N>) {
        let previous = self.state.data.swap(Box::into_raw(fsm), Ordering::AcqRel);
        let mut previous_status = NOTIFYSTATE_NOTIFIED;
        if previous.is_null() {
            previous_status = self.state.status.compare_and_swap(NOTIFYSTATE_NOTIFIED, NOTIFYSTATE_IDLE, Ordering::AcqRel);
            match previous_status {
                NOTIFYSTATE_NOTIFIED => return,
                NOTIFYSTATE_DROP => {
                    let ptr = self.state.data.swap(ptr::null_mut(), Ordering::AcqRel);
                    unsafe { Box::from_raw(ptr) };
                    return;
                }
                _ => {}
            }
        }
        panic!("invalid release state: {:?} {}", previous, previous_status);
    }

    #[inline]
    pub fn force_send<S: FsmScheduler<Fsm = N>>(
        &self,
        msg: N::Message,
        scheduler: &S,
    ) -> Result<(), SendError<N::Message>> {
        self.sender.force_send(msg)?;
        self.notify(scheduler);
        Ok(())
    }

    #[inline]
    pub fn try_send<S: FsmScheduler<Fsm = N>>(
        &self,
        msg: N::Message,
        scheduler: &S,
    ) -> Result<(), TrySendError<N::Message>> {
        self.sender.try_send(msg)?;
        self.notify(scheduler);
        Ok(())
    }

    #[inline]
    fn close(&self) {
        self.sender.close();
        match self.state.status.swap(NOTIFYSTATE_DROP, Ordering::AcqRel) {
            NOTIFYSTATE_NOTIFIED | NOTIFYSTATE_DROP => return,
            _ => {}
        }

        let ptr = self.state.data.swap(ptr::null_mut(), Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe {
                Box::from_raw(ptr);
            }
        }
    }
}

impl<N: Fsm> Clone for BasicMailbox<N> {
    #[inline]
    fn clone(&self) -> BasicMailbox<N> {
        BasicMailbox {
            sender: self.sender.clone(),
            state: self.state.clone(),
        }
    }
}

pub struct Mailbox<N: Fsm, S: FsmScheduler<Fsm = N>> {
    mailbox: BasicMailbox<N>,
    scheduler: S,
}

impl<N: Fsm, S: FsmScheduler<Fsm = N>> Mailbox<N, S> {
    #[inline]
    pub fn force_send(&self, msg: N::Message) -> Result<(), SendError<N::Message>> {
        self.mailbox.force_send(msg, &self.scheduler)
    }

    #[inline]
    pub fn try_send(&self, msg: N::Message) -> Result<(), TrySendError<N::Message>> {
        self.mailbox.try_send(msg, &self.scheduler)
    }
}

pub struct Router<N: Fsm, C: Fsm, Ns, Cs> {
    normals: Arc<Mutex<HashMap<u64, BasicMailbox<N>>>>,
    caches: Cell<HashMap<u64, BasicMailbox<N>>>,
    control_box: BasicMailbox<C>,
    // These two schedulers should be unified as single one. However
    // it's not possible to write FsmScheduler<Fsm=C> + FsmScheduler<Fsm=N>
    // for now.
    normal_scheduler: Ns,
    control_scheduler: Cs,
}

impl<N, C, Ns, Cs> Router<N, C, Ns, Cs>
where
    N: Fsm,
    C: Fsm,
    Ns: FsmScheduler<Fsm = N> + Clone,
    Cs: FsmScheduler<Fsm = C> + Clone,
{
    fn new(
        control_box: BasicMailbox<C>,
        normal_scheduler: Ns,
        control_scheduler: Cs,
    ) -> Router<N, C, Ns, Cs> {
        Router {
            normals: Arc::default(),
            caches: Cell::default(),
            control_box,
            normal_scheduler,
            control_scheduler,
        }
    }
    
    #[inline]
    fn check_do<F, R>(&self, region_id: u64, mut f: F) -> Option<R>
    where
        F: FnMut(&BasicMailbox<N>) -> Option<R>
    {
        let caches = unsafe { &mut *self.caches.as_ptr() };
        let mut connected = true;
        if let Some(mailbox) = caches.get(&region_id) {
            match f(mailbox) {
                Some(r) => return Some(r),
                None => {
                    connected = false;
                }
            }
        }

        let mailbox = 'fetch_box: {
            let mut boxes = self.normals.lock().unwrap();
            if let Some(mailbox) = boxes.get_mut(&region_id) {
                break 'fetch_box mailbox.clone();
            }
            drop(boxes);
            if !connected {
                caches.remove(&region_id);
            }
            return None;
        };

        let res = f(&mailbox);
        if res.is_some() {
            caches.insert(region_id, mailbox);
        } else if !connected {
            caches.remove(&region_id);
        }
        res
    }

    pub fn mailbox(&self, region_id: u64) -> Option<Mailbox<N, Ns>> {
        self.check_do(region_id, |mailbox| {
            if mailbox.sender.is_alive() {
                Some(Mailbox {
                    mailbox: mailbox.clone(),
                    scheduler: self.normal_scheduler.clone(),
                })
            } else {
                None
            }
        })
    }

    pub fn control_mailbox(&self) -> Mailbox<C, Cs> {
        Mailbox {
            mailbox: self.control_box.clone(),
            scheduler: self.control_scheduler.clone(),
        }
    }

    pub fn try_send(&self, region_id: u64, mut msg: N::Message) -> Either<Result<(), TrySendError<N::Message>>, N::Message> {
        let mut msg = Some(msg);
        let mut check_times = 0;
        let res = self.check_do(region_id, |mailbox| {
            check_times += 1;
            let m = msg.take().unwrap();
            match mailbox.try_send(m, &self.normal_scheduler) {
                Ok(()) => Some(Ok(())),
                r @ Err(TrySendError::Full(_)) => {
                    // TODO: report channel full
                    Some(r)
                }
                Err(TrySendError::Disconnected(m)) => {
                    msg = Some(m);
                    None
                }
            }
        });
        match res {
            Some(r) => Either::Left(r),
            None => {
                if check_times == 1 {
                    Either::Right(msg.unwrap())
                } else {
                    Either::Left(Err(TrySendError::Disconnected(msg.unwrap())))
                }
            },
        }
    }

    pub fn send(&self, region_id: u64, msg: N::Message) -> Result<(), TrySendError<N::Message>> {
        match self.try_send(region_id, msg) {
            Either::Left(res) => res,
            Either::Right(m) => Err(TrySendError::Disconnected(m)),
        }
    }

    pub fn force_send(&self, region_id: u64, msg: N::Message) -> Result<(), SendError<N::Message>> {
        match self.send(region_id, msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(m)) => {
                let caches = unsafe { &mut *self.caches.as_ptr() };
                caches[&region_id].force_send(m, &self.normal_scheduler)
            }
            Err(TrySendError::Disconnected(m)) => Err(SendError(m)),
        }
    }

    pub fn send_control(&self, msg: C::Message) -> Result<(), TrySendError<C::Message>> {
        match self.control_box.try_send(msg, &self.control_scheduler) {
            Ok(()) => Ok(()),
            r @ Err(TrySendError::Full(_)) => {
                // TODO: record metrics.
                r
            }
            r => r,
        }
    }

    pub fn broadcast_shutdown(&self) {
        info!("broadcasting shutdown");
        unsafe { &mut *self.caches.as_ptr() }.clear();
        let mut mailboxes = self.normals.lock().unwrap();
        for (region_id, mailbox) in mailboxes.drain() {
            debug!("[region {}] shutdown mailbox", region_id);
            mailbox.close();
        }
        self.control_box.close();
        self.normal_scheduler.shutdown();
        self.control_scheduler.shutdown();
    }

    pub fn stop(&self, region_id: u64) {
        info!("[region {}] shutdown mailbox", region_id);
        let mut mailboxes = self.normals.lock().unwrap();
        if let Some(mb) = mailboxes.remove(&region_id) {
            mb.close();
        }
    }
}

impl<N: Fsm, C: Fsm, Ns: Clone, Cs: Clone> Clone for Router<N, C, Ns, Cs> {
    fn clone(&self) -> Router<N, C, Ns, Cs> {
        Router {
            normals: self.normals.clone(),
            caches: Cell::default(),
            control_box: self.control_box.clone(),
            // These two schedulers should be unified as single one. However
            // it's not possible to write FsmScheduler<Fsm=C> + FsmScheduler<Fsm=N>
            // for now.
            normal_scheduler: self.normal_scheduler.clone(),
            control_scheduler: self.control_scheduler.clone(),
        }
    }
}

enum FsmTypes<N, C> {
    Normal(Box<N>),
    Control(Box<C>),
    Empty,
}

struct NormalScheduler<N, C> {
    sender: crossbeam_channel::Sender<FsmTypes<N, C>>,
}

impl<N, C> FsmScheduler for NormalScheduler<N, C> {
    type Fsm = N;

    fn schedule(&self, fsm: Box<N>) {
        self.sender.send(FsmTypes::Normal(fsm));
    }

    fn shutdown(&self) {
        for _ in 0..100 {
            self.sender.send(FsmTypes::Empty);
        }
    }
}

struct ControlScheduler<N, C> {
    sender: crossbeam_channel::Sender<FsmTypes<N, C>>,
}

impl<N, C> FsmScheduler for ControlScheduler<N, C> {
    type Fsm = C;

    fn schedule(&self, fsm: Box<C>) {
        self.sender.send(FsmTypes::Control(fsm));
    }

    fn shutdown(&self) {
        for _ in 0..100 {
            self.sender.send(FsmTypes::Empty);
        }
    }
}

pub struct Batch<N, C> {
    normals: Vec<Box<N>>,
    control: Option<Box<C>>,
}

impl<N: Fsm, C: Fsm> Batch<N, C> {
    pub fn with_capacity(cap: usize) -> Batch<N, C> {
        Batch {
            normals: Vec::with_capacity(cap),
            control: None,
        }
    }

    pub fn push(&mut self, fsm: FsmTypes<N, C>) -> bool {
        match fsm {
            FsmTypes::Normal(n) => self.normals.push(n),
            FsmTypes::Control(c) => {
                assert!(self.control.is_none());
                self.control = Some(c);
            },
            FsmTypes::Empty => return false,
        }
        true
    }

    pub fn release(&mut self, index: usize, channel_pos: usize) -> bool {
        let mut fsm = self.normals.swap_remove(index);
        let mailbox = fsm.take_mailbox().unwrap();
        mailbox.release(fsm);
        if mailbox.sender.len() == channel_pos {
            true
        } else {
            match mailbox.maybe_catch_fsm() {
                None => true,
                Some(mut s) => {
                    s.set_mailbox(Cow::Owned(mailbox));
                    let last_index = self.normals.len();
                    self.normals.push(s);
                    self.normals.swap(index, last_index);
                    false
                }
            }
        }
    }

    pub fn remove(&mut self, index: usize) {
        let mut fsm = self.normals.swap_remove(index);
        let mailbox = fsm.take_mailbox().unwrap();
        if mailbox.sender.is_empty() {
            mailbox.release(fsm);
        } else {
            fsm.set_mailbox(Cow::Owned(mailbox));
            let last_index = self.normals.len();
            self.normals.push(fsm);
            self.normals.swap(index, last_index);
        }
    }

    pub fn release_control(&mut self, control_box: &BasicMailbox<C>, channel_pos: usize) -> bool {
        let s = self.control.take().unwrap();
        control_box.release(s);
        if control_box.sender.len() == channel_pos {
            true
        } else {
            match control_box.maybe_catch_fsm() {
                None => true,
                Some(s) => {
                    self.control = Some(s);
                    false
                }
            }
        }
    }

    pub fn remove_control(&mut self, control_box: &BasicMailbox<C>) {
        if control_box.sender.is_empty() {
            let s = self.control.take().unwrap();
            control_box.release(s);
        }
    }
}


