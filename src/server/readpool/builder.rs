// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::future_pool::{Builder as FuturePoolBuilder, NoopFactory, TickRunnerFactory};

use super::config::Config;

pub struct Builder<T> {
    low: FuturePoolBuilder<T>,
    normal: FuturePoolBuilder<T>,
    high: FuturePoolBuilder<T>,
}

impl<T> Builder<T>
where
    T: TickRunnerFactory + Clone + Send,
    T::TickRunner: 'static,
{
    pub fn new(name: impl AsRef<str>, t: T) -> Builder<T> {
        let name = name.as_ref();
        Builder {
            low: FuturePoolBuilder::new(format!("{}-low", name), t.clone()),
            normal: FuturePoolBuilder::new(format!("{}-normal", name), t.clone()),
            high: FuturePoolBuilder::new(format!("{}-high", name), t),
        }
    }

    pub fn build(self, cfg: &Config) -> super::ReadPool {
        let pool_low = self
            .low
            .pool_size(cfg.low_concurrency)
            .stack_size(cfg.stack_size.0 as usize)
            .build();
        let pool_normal = self
            .normal
            .pool_size(cfg.normal_concurrency)
            .stack_size(cfg.stack_size.0 as usize)
            .build();
        let pool_high = self
            .high
            .pool_size(cfg.high_concurrency)
            .stack_size(cfg.stack_size.0 as usize)
            .build();

        super::ReadPool {
            pool_low,
            pool_normal,
            pool_high,
            max_tasks_low: cfg.max_tasks_per_worker_low * cfg.low_concurrency,
            max_tasks_normal: cfg.max_tasks_per_worker_normal * cfg.normal_concurrency,
            max_tasks_high: cfg.max_tasks_per_worker_high * cfg.high_concurrency,
        }
    }
}

impl Builder<NoopFactory> {
    pub fn build_for_test() -> super::ReadPool {
        Builder::new("readpool-for-test", NoopFactory).build(&Config::default_for_test())
    }
}
