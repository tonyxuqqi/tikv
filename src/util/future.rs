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

use futures::sync::oneshot;
use futures::{Future, Async, Poll};
use futures::task::AtomicTask;
use std::boxed;
use std::sync::{Arc, Weak};

/// Generated a paired future and callback so that when callback is being called, its result
/// is automatically passed as a future result.
pub fn paired_future_callback<T>() -> (Box<boxed::FnBox(T) + Send>, oneshot::Receiver<T>)
where
    T: Send + 'static,
{
    let (tx, future) = oneshot::channel::<T>();
    let callback = box move |result| {
        let r = tx.send(result);
        if r.is_err() {
            warn!("paired_future_callback: Failed to send result to the future rx, discarded.");
        }
    };
    (callback, future)
}

#[derive(Default)]
struct Latch {
    task: AtomicTask,
}

impl Drop for Latch {
    fn drop(&mut self) {
        self.task.notify()
    }
}

#[derive(Clone)]
pub struct CountDownLatch {
    latch: Arc<Latch>,
}

pub struct CountDownFuture {
    latch: Weak<Latch>,
}

pub fn count_down_latch() -> (CountDownLatch, CountDownFuture) {
    let latch = Arc::new(Latch::default());
    let f = CountDownFuture { latch: Arc::downgrade(&latch) };
    (CountDownLatch { latch }, f)
}

impl Future for CountDownFuture {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        match self.latch.upgrade() {
            None => Ok(Async::Ready(())),
            Some(l) => {
                l.task.register();
                Ok(Async::NotReady)
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use std::thread;
    use futures::future;

    #[test]
    fn test_count_down_latch() {
        let mut timer = Instant::now();
        let (_, f) = count_down_latch();
        f.wait().unwrap();
        assert!(timer.elapsed() < Duration::from_millis(100));

        timer = Instant::now();
        let (l, f) = count_down_latch();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            drop(l);
        });
        f.wait().unwrap();
        assert!(timer.elapsed() >= Duration::from_millis(100));

        timer = Instant::now();
        let (l, f) = count_down_latch();
        for i in 1..3 {
            let l = l.clone();
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(i * 100));
                drop(l);
            });
        }
        drop(l);
        f.wait().unwrap();
        assert!(timer.elapsed() >= Duration::from_millis(200));
    }
}
