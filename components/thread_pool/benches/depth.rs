#![feature(test)]
#![deny(warnings, rust_2018_idioms)]

extern crate test;

const ITER: usize = 20_000;

mod tokio_threadpool {
    use futures::future;
    use std::sync::mpsc;
    use tokio_threadpool::*;

    #[bench]
    fn chained_spawn(b: &mut test::Bencher) {
        let threadpool = ThreadPool::new();

        fn spawn(pool_tx: Sender, res_tx: mpsc::Sender<()>, n: usize) {
            if n == 0 {
                res_tx.send(()).unwrap();
            } else {
                let pool_tx2 = pool_tx.clone();
                pool_tx
                    .spawn(future::lazy(move || {
                        spawn(pool_tx2, res_tx, n - 1);
                        Ok(())
                    }))
                    .unwrap();
            }
        }

        b.iter(move || {
            let (res_tx, res_rx) = mpsc::channel();

            spawn(threadpool.sender().clone(), res_tx, super::ITER);
            res_rx.recv().unwrap();
        });
    }
}

mod cpupool {
    use futures::future::{self, Executor};
    use futures_cpupool::*;
    use num_cpus;
    use std::sync::mpsc;

    #[bench]
    fn chained_spawn(b: &mut test::Bencher) {
        let pool = CpuPool::new(num_cpus::get());

        fn spawn(pool: CpuPool, res_tx: mpsc::Sender<()>, n: usize) {
            if n == 0 {
                res_tx.send(()).unwrap();
            } else {
                let pool2 = pool.clone();
                pool.execute(future::lazy(move || {
                    spawn(pool2, res_tx, n - 1);
                    Ok(())
                }))
                .ok()
                .unwrap();
            }
        }

        b.iter(move || {
            let (res_tx, res_rx) = mpsc::channel();

            spawn(pool.clone(), res_tx, super::ITER);
            res_rx.recv().unwrap();
        });
    }
}

mod thread_pool_callback {
    use std::sync::mpsc;
    use thread_pool::Config;
    use thread_pool::callback::{RunnerFactory, SchedulerRef};

    #[bench]
    fn chained_spawn(b: &mut test::Bencher) {
        let pool = Config::new("chain-spawn").spawn(RunnerFactory::new());

        fn spawn(c: &mut SchedulerRef<'_>, res_tx: mpsc::Sender<()>, n: usize) {
            if n == 0 {
                res_tx.send(()).unwrap();
            } else {
                c.spawn_once(move |c| {
                    spawn(c, res_tx, n - 1);
                })
            }
        }

        b.iter(move || {
            let (res_tx, res_rx) = mpsc::channel();
            pool.spawn_once(move |c| spawn(c, res_tx, super::ITER));
            res_rx.recv().unwrap();
        });
    }
}
