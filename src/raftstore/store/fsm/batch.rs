// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! This is the core implementation of a batch system. Generally there will be two
//! different kind of FSMs in TiKV's FSM system. One is normal FSM, which usually
//! represents a peer, the other is control FSM, which usually represents something
//! that controls how the former is created or metrics are collected.

use super::router::{BasicMailbox, Managed, Router};
use chocolates::thread_pool::{
    self, Config, LazyConfig, PoolContext, Remote, Runner, RunnerFactory,
};
use std::marker::PhantomData;
use std::time::Duration;
use tikv_util::mpsc;

/// `FsmScheduler` schedules `Fsm` for later handles.
pub trait FsmScheduler {
    type Fsm: Fsm;

    /// Schedule a Fsm for later handles.
    fn schedule(&self, fsm: Managed<Self::Fsm>);
}

/// A Fsm is a finite state machine. It should be able to be notified for
/// updating internal state according to incoming messages.
pub trait Fsm {
    type Message: Send;
}

/// A unify type for FSMs so that they can be sent to channel easily.
enum FsmTypes<N, C> {
    Normal(Managed<N>),
    Control(Managed<C>),
}

// A macro to introduce common definition of scheduler.
macro_rules! impl_sched {
    ($name:ident, $ty:path, Fsm = $fsm:tt) => {
        pub struct $name<N, C> {
            sender: Remote<FsmTypes<N, C>>,
        }

        impl<N, C> Clone for $name<N, C> {
            #[inline]
            fn clone(&self) -> $name<N, C> {
                $name {
                    sender: self.sender.clone(),
                }
            }
        }

        impl<N, C> FsmScheduler for $name<N, C>
        where
            $fsm: Fsm,
        {
            type Fsm = $fsm;

            #[inline]
            fn schedule(&self, fsm: Managed<$fsm>) {
                self.sender.spawn($ty(fsm))
            }
        }
    };
}

impl_sched!(NormalScheduler, FsmTypes::Normal, Fsm = N);
impl_sched!(ControlScheduler, FsmTypes::Control, Fsm = C);

/// A handler that poll all FSM in ready.
///
/// A General process works like following:
/// ```text
/// loop {
///     begin
///     if control is ready:
///         handle_control
///     foreach ready normal:
///         handle_normal
///     end
/// }
/// ```
///
/// Note that, every poll thread has its own handler, which doesn't have to be
/// Sync.
pub trait PollHandler<N, C> {
    /// This function is called at the very beginning of every round.
    fn begin(&mut self, batch_size: usize);

    /// This function is called when handling readiness for control FSM.
    fn handle_control(&mut self, control: &mut Managed<C>) -> bool;

    /// This function is called when handling readiness for normal FSM.
    fn handle_normal(&mut self, normal: &mut Managed<N>) -> bool;

    /// This function is called at the end of every round.
    fn end(&mut self, batch: &mut [Managed<N>]);
}

/// Internal poller that fetches batch and call handler hooks for readiness.
struct Poller<N: Fsm, C: Fsm, Handler> {
    pending_normal: Vec<Managed<N>>,
    pending_control: Option<Managed<C>>,
    executed_count: Vec<usize>,
    exhausted_fsms: Vec<usize>,
    handler: Handler,
    max_batch_size: usize,
    remote: Option<Remote<FsmTypes<N, C>>>,
    max_spin_time: usize,
}

impl<N: Fsm, C: Fsm, Handler: PollHandler<N, C>> Poller<N, C, Handler> {
    fn handle_control(&mut self) {
        if self.pending_control.is_none() {
            return;
        }

        self.executed_count[0] += 1;
        if self
            .handler
            .handle_control(self.pending_control.as_mut().unwrap())
        {
            match self.pending_control.take().unwrap().release() {
                None => {
                    self.executed_count[0] = 0;
                    return;
                }
                Some(c) => self.pending_control = Some(c),
            }
        }
        if self.executed_count[0] >= self.max_spin_time {
            self.remote
                .as_ref()
                .unwrap()
                .spawn(FsmTypes::Control(self.pending_control.take().unwrap()));
            self.executed_count[0] = 0;
        }
    }

    fn handle_normal(&mut self) {
        for (i, normal) in self.pending_normal.iter_mut().enumerate() {
            self.executed_count[i + 1] += 1;
            if self.handler.handle_normal(normal)
                || self.executed_count[i + 1] >= self.max_spin_time
            {
                self.exhausted_fsms.push(i);
            }
        }
    }

    fn round(&mut self) {
        let batch_size = self.pending_normal.len() + self.pending_control.is_some() as usize;
        if batch_size == 0 {
            return;
        }
        self.handler.begin(batch_size);
        self.handle_control();
        self.handle_normal();
        self.handler.end(&mut self.pending_normal);
        for r in self.exhausted_fsms.iter().rev() {
            let n = self.pending_normal.swap_remove(*r);
            let c = self.executed_count.swap_remove(*r + 1);
            if let Some(n) = n.release() {
                if c >= self.max_spin_time {
                    self.remote.as_ref().unwrap().spawn(FsmTypes::Normal(n));
                    continue;
                }
                self.pending_normal.push(n);
                self.executed_count.push(c);
            }
        }
        self.exhausted_fsms.clear();
    }
}

impl<N: Fsm, C: Fsm, Handler: PollHandler<N, C>> Runner for Poller<N, C, Handler> {
    type Task = FsmTypes<N, C>;

    fn start(&mut self, ctx: &mut PoolContext<Self::Task>) {
        self.pending_normal = Vec::with_capacity(self.max_batch_size);
        self.executed_count.push(0);
        self.remote = Some(ctx.remote());
    }

    fn handle(&mut self, _ctx: &mut PoolContext<Self::Task>, task: Self::Task) -> bool {
        match task {
            FsmTypes::Control(c) => self.pending_control = Some(c),
            FsmTypes::Normal(n) => {
                self.pending_normal.push(n);
                self.executed_count.push(0);
            }
        }
        while self.pending_normal.len() + self.pending_control.is_some() as usize
            == self.max_batch_size
        {
            self.round();
        }
        true
    }

    fn pause(&mut self, _ctx: &PoolContext<Self::Task>) -> bool {
        self.round();
        self.pending_normal.is_empty() && self.pending_control.is_none()
    }
}

/// A builder trait that can build up poll handlers.
pub trait HandlerBuilder<N, C> {
    type Handler: PollHandler<N, C>;

    fn build(&mut self) -> Self::Handler;
}

struct PollerBuilder<N, C, B> {
    builder: B,
    max_spin_time: usize,
    max_batch_size: usize,
    _mark: PhantomData<(N, C)>,
}

impl<N, C, B> RunnerFactory for PollerBuilder<N, C, B>
where
    N: Fsm,
    C: Fsm,
    B: HandlerBuilder<N, C>,
{
    type Runner = Poller<N, C, B::Handler>;

    fn produce(&mut self) -> Poller<N, C, B::Handler> {
        Poller {
            pending_normal: vec![],
            pending_control: None,
            executed_count: vec![],
            exhausted_fsms: vec![],
            handler: self.builder.build(),
            max_batch_size: self.max_batch_size,
            remote: None,
            max_spin_time: self.max_spin_time,
        }
    }
}

/// A system that can poll FSMs concurrently and in batch.
///
/// To use the system, two type of FSMs and their PollHandlers need
/// to be defined: Normal and Control. Normal FSM handles the general
/// task while Control FSM creates normal FSM instances.
pub struct BatchSystem<N: Fsm, C: Fsm> {
    name_prefix: Option<String>,
    config: Option<LazyConfig<FsmTypes<N, C>>>,
    router: BatchRouter<N, C>,
    max_batch_size: usize,
    max_spin_time: usize,
    pool: Option<thread_pool::ThreadPool<FsmTypes<N, C>>>,
}

impl<N, C> BatchSystem<N, C>
where
    N: Fsm + Send + 'static,
    C: Fsm + Send + 'static,
{
    pub fn router(&self) -> &BatchRouter<N, C> {
        &self.router
    }

    /// Start the batch system.
    pub fn spawn<B>(&mut self, name_prefix: String, builder: B)
    where
        B: HandlerBuilder<N, C>,
        B::Handler: Send + 'static,
    {
        let builder = PollerBuilder {
            builder,
            max_spin_time: self.max_spin_time,
            max_batch_size: self.max_batch_size,
            _mark: PhantomData,
        };
        let pool = self
            .config
            .take()
            .unwrap()
            .name(&name_prefix)
            .spawn(builder);
        self.name_prefix = Some(name_prefix);
        self.pool = Some(pool);
    }

    /// Shutdown the batch system and wait till all background threads exit.
    pub fn shutdown(&mut self) {
        if self.pool.is_none() {
            return;
        }
        let name_prefix = self.name_prefix.take().unwrap();
        info!("shutdown batch system {}", name_prefix);
        self.router.broadcast_shutdown();
        self.pool.take().unwrap().shutdown();
        info!("batch system {} is stopped.", name_prefix);
    }
}

pub type BatchRouter<N, C> = Router<N, C, NormalScheduler<N, C>, ControlScheduler<N, C>>;

/// Create a batch system with the given thread name prefix and pool size.
///
/// `sender` and `controller` should be paired.
pub fn create_system<N: Fsm, C: Fsm>(
    pool_size: usize,
    max_batch_size: usize,
    max_spin_time: usize,
    sender: mpsc::LooseBoundedSender<C::Message>,
    controller: Box<C>,
) -> (BatchRouter<N, C>, BatchSystem<N, C>) {
    let control_box = BasicMailbox::new(sender, controller);
    let (remote, lazy_cfg) = Config::new("")
        .max_thread_count(pool_size)
        .max_wait_time(Duration::from_millis(50))
        .freeze();
    let normal_scheduler = NormalScheduler {
        sender: remote.clone(),
    };
    let control_scheduler = ControlScheduler { sender: remote };
    let router = Router::new(control_box, normal_scheduler, control_scheduler);
    let system = BatchSystem {
        name_prefix: None,
        config: Some(lazy_cfg),
        router: router.clone(),
        max_batch_size,
        max_spin_time,
        pool: None,
    };
    (router, system)
}

#[cfg(test)]
pub mod tests {
    use super::super::router::*;
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    pub type Message = Option<Box<dyn FnOnce(&mut Runner) + Send>>;

    pub struct Runner {
        recv: mpsc::Receiver<Message>,
        pub sender: Option<mpsc::Sender<()>>,
    }

    impl Fsm for Runner {
        type Message = Message;
    }

    pub fn new_runner(cap: usize) -> (mpsc::LooseBoundedSender<Message>, Box<Runner>) {
        let (tx, rx) = mpsc::loose_bounded(cap);
        let fsm = Runner {
            recv: rx,
            sender: None,
        };
        (tx, Box::new(fsm))
    }

    #[derive(Add, PartialEq, Debug, Default, AddAssign, Clone, Copy)]
    struct HandleMetrics {
        begin: usize,
        control: usize,
        normal: usize,
    }

    pub struct Handler {
        local: HandleMetrics,
        metrics: Arc<Mutex<HandleMetrics>>,
    }

    impl PollHandler<Runner, Runner> for Handler {
        fn begin(&mut self, _batch_size: usize) {
            self.local.begin += 1;
        }

        fn handle_control(&mut self, control: &mut Managed<Runner>) -> bool {
            self.local.control += 1;
            control.reset_dirty_flag();
            let len = control.recv.len();
            for _ in 0..len {
                if let Ok(Some(r)) = control.recv.try_recv() {
                    r(control);
                }
            }
            true
        }

        fn handle_normal(&mut self, normal: &mut Managed<Runner>) -> bool {
            self.local.normal += 1;
            normal.reset_dirty_flag();
            let len = normal.recv.len();
            for _ in 0..len {
                if let Ok(Some(r)) = normal.recv.try_recv() {
                    r(normal);
                }
            }
            true
        }

        fn end(&mut self, _normals: &mut [Managed<Runner>]) {
            let mut c = self.metrics.lock().unwrap();
            *c += self.local;
            self.local = HandleMetrics::default();
        }
    }

    pub struct Builder {
        metrics: Arc<Mutex<HandleMetrics>>,
    }

    impl Builder {
        pub fn new() -> Builder {
            Builder {
                metrics: Arc::default(),
            }
        }
    }

    impl HandlerBuilder<Runner, Runner> for Builder {
        type Handler = Handler;

        fn build(&mut self) -> Handler {
            Handler {
                local: HandleMetrics::default(),
                metrics: self.metrics.clone(),
            }
        }
    }

    #[test]
    fn test_batch() {
        let (control_tx, control_fsm) = new_runner(10);
        let (router, mut system) = super::create_system(2, 2, 16, control_tx, control_fsm);
        let builder = Builder::new();
        let metrics = builder.metrics.clone();
        system.spawn("test".to_owned(), builder);
        let mut expected_metrics = HandleMetrics::default();
        assert_eq!(*metrics.lock().unwrap(), expected_metrics);
        let (tx, rx) = mpsc::unbounded();
        let tx_ = tx.clone();
        let r = router.clone();
        router
            .send_control(Some(Box::new(move |_: &mut Runner| {
                let (tx, runner) = new_runner(10);
                let mailbox = BasicMailbox::new(tx, runner);
                tx_.send(1).unwrap();
                r.register(1, mailbox);
            })))
            .unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(1));
        let tx_ = tx.clone();
        router
            .send(
                1,
                Some(Box::new(move |_: &mut Runner| {
                    tx_.send(2).unwrap();
                })),
            )
            .unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(3)), Ok(2));
        system.shutdown();
        expected_metrics.control = 1;
        expected_metrics.normal = 1;
        expected_metrics.begin = 2;
        assert_eq!(*metrics.lock().unwrap(), expected_metrics);
    }
}
