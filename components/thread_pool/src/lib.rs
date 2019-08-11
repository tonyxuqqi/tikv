pub mod callback;

use std::thread::{JoinHandle, Builder};
use std::sync::{Arc, Mutex};
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Instant, Duration};
use crossbeam_deque::Steal;
use parking_lot_core::{ParkResult, ParkToken, UnparkToken, FilterOp};

const SHUTDOWN_MARK: usize = 0x800000000;

struct SchedUnit<Task> {
    task: Task,
    sched_time: Instant,
}

impl<Task> SchedUnit<Task> {
    fn new(task: Task) -> SchedUnit<Task> {
        SchedUnit {
            task,
            sched_time: Instant::now(),
        }
    }
}

pub struct PoolContext<Task> {
    local_queue: LocalQueue<Task>,
    handled_global: usize,
    handled_miss: usize,
    handled_local: usize,
    handled_steal: usize,
    park_cnt: Vec<usize>,
}

impl<Task> PoolContext<Task> {
    fn new(queue: LocalQueue<Task>) -> PoolContext<Task> {
        PoolContext {
            handled_global: 0,
            handled_miss: 0,
            handled_steal: 0,
            handled_local: 0,
            park_cnt: vec![0; queue.core.stealers.len() + 1],
            local_queue: queue,
        }
    }

    pub fn spawn(&self, t: impl Into<Task>) {
        self.local_queue.push(t.into());
    }

    pub fn spawn_handle(&self) -> SpawnHandle<Task> {
        SpawnHandle {
            core: self.local_queue.core.clone(),
        }
    }

    fn deque_a_task(&mut self) -> (Steal<SchedUnit<Task>>, bool) {
        if let Some(e) = self.local_queue.local.pop() {
            self.handled_local += 1;
            return (Steal::Success(e), true);
        }
        let mut need_retry = false;
        match self.local_queue.core.global.steal() {
            e @ Steal::Success(_) => {
                self.handled_global += 1;
                return (e, false);
            },
            Steal::Retry => need_retry = true,
            _ => {}
        }
        for (pos, stealer) in self.local_queue.core.stealers.iter().enumerate() {
            if pos != self.local_queue.pos {
                match stealer.steal() {
                    e @ Steal::Success(_) => {
                        self.handled_steal += 1;
                        return (e, false);
                    },
                    Steal::Retry => need_retry = true,
                    _ => {}
                }
            }
        }
        self.handled_miss += 1;
        if need_retry {
            (Steal::Retry, false)
        } else {
            (Steal::Empty, false)
        }
    }
    
    fn park(&mut self, timeout: Option<Duration>, skip_check: bool) -> (Steal<SchedUnit<Task>>, bool) {
        let address = &* self.local_queue.core as *const QueueCore<Task> as usize;
        let timeout = timeout.map(|t| Instant::now() + t);
        let token = ParkToken(self.local_queue.pos);
        let mut task = (Steal::Empty, false);
        let res = unsafe { parking_lot_core::park(address, || {
            self.local_queue.core.sleep();
            let should_shutdown = self.local_queue.core.should_shutdown();
            if should_shutdown {
                false
            } else if skip_check {
                true
            } else {
                task = self.deque_a_task();
                task.0.is_empty()
            }
        }, || {}, |_, _| (), token, timeout) };
        match res {
            ParkResult::TimedOut => (Steal::Empty, false),
            ParkResult::Invalid => {
                if !task.0.is_empty() {
                    self.local_queue.core.wake_up();
                }
                task
            },
            ParkResult::Unparked(from) => {
                // println!("{} unpark by {}", self.local_queue.pos, from.0);
                self.local_queue.core.wake_up();
                self.park_cnt[from.0] += 1;
                (Steal::Retry, false)
            },
        }
    }

    fn dump_metrics(&self) {
        println!("{} park {:?} global {} local {} steal {} miss {}", self.local_queue.pos, self.park_cnt, self.handled_global, self.handled_local, self.handled_steal, self.handled_miss);
    }
}

pub struct SpawnHandle<Task> {
    core: Arc<QueueCore<Task>>,
}

impl<Task> SpawnHandle<Task> {
    pub fn spawn(&self, t: impl Into<Task>) {
        self.core.push(t.into());
    }
}

pub trait Runner {
    type Task;

    fn start(&mut self, _ctx: &PoolContext<Self::Task>) {}
    fn handle(&mut self, ctx: &mut PoolContext<Self::Task>, task: Self::Task);
    fn pause(&mut self, _ctx: &PoolContext<Self::Task>) {}
    fn resume(&mut self, _ctx: &PoolContext<Self::Task>) {}
    fn end(&mut self, _ctx: &PoolContext<Self::Task>) {}
}

pub trait RunnerFactory {
    type Runner: Runner;

    fn produce(&mut self) -> Self::Runner;
}

struct QueueCore<Task> {
    global: crossbeam_deque::Injector<SchedUnit<Task>>,
    stealers: Vec<crossbeam_deque::Stealer<SchedUnit<Task>>>,
    running_count: AtomicUsize,
    locals: Mutex<Vec<Option<crossbeam_deque::Worker<SchedUnit<Task>>>>>,
}

impl<Task> QueueCore<Task> {
    fn should_shutdown(&self) -> bool {
        self.running_count.load(Ordering::SeqCst) & SHUTDOWN_MARK == SHUTDOWN_MARK
    }

    fn shutdown(&self) {
        self.running_count.fetch_add(SHUTDOWN_MARK, Ordering::SeqCst);
    }

    fn sleep(&self) {
        self.running_count.fetch_sub(1, Ordering::SeqCst);
    }

    fn wake_up(&self) {
        self.running_count.fetch_add(1, Ordering::SeqCst);
    }

    fn push(&self, task: Task) {
        self.global.push(SchedUnit::new(task));
    }
}

struct LocalQueue<Task> {
    local: crossbeam_deque::Worker<SchedUnit<Task>>,
    core: Arc<QueueCore<Task>>,
    pos: usize,
}

impl<Task> LocalQueue<Task> {
    fn unpark_one(&self) -> bool {
        let cnt = self.core.running_count.load(Ordering::SeqCst);
        if cnt | SHUTDOWN_MARK == SHUTDOWN_MARK | self.core.stealers.len() {
            return false;
        }

        let address = &* self.core as *const QueueCore<Task> as usize;
        let token = UnparkToken(self.pos);
        let res = unsafe { parking_lot_core::unpark_one(address, |_| token) };
        res.unparked_threads > 0
    }

    fn push(&self, task: Task) {
        self.local.push(SchedUnit::new(task));
    }
}

struct Queues<Task> {
    core: Arc<QueueCore<Task>>,
}

impl<Task> Queues<Task> {
    fn acquire_local_queue(&self) -> LocalQueue<Task> {
        let mut locals = self.core.locals.lock().unwrap();
        for (pos, l) in locals.iter_mut().enumerate() {
            if l.is_some() {
                return LocalQueue {
                    local: l.take().unwrap(),
                    core: self.core.clone(),
                    pos,
                };
            }
        }
        unreachable!()
    }

    fn release_local_queue(&mut self, q: LocalQueue<Task>) {
        let mut locals = self.core.locals.lock().unwrap();
        assert!(locals[q.pos].replace(q.local).is_none());
    }

    fn unpark_one(&self) -> bool {
        let cnt = self.core.running_count.load(Ordering::SeqCst);
        if cnt ^ SHUTDOWN_MARK > SHUTDOWN_MARK {
            return false;
        }

        let address = &* self.core as *const QueueCore<Task> as usize;
        let token = UnparkToken(self.core.stealers.len());
        let res = unsafe { parking_lot_core::unpark_filter(address, |ParkToken(id)| {
            if id == 0 || cnt >= SHUTDOWN_MARK {
                FilterOp::Unpark
            } else {
                FilterOp::Skip
            }
        },|_| token) };
        res.unparked_threads > 0
    }

    fn push(&self, task: Task) {
        self.core.push(task);
    }
}

fn elapsed(start: Instant, end: Instant) -> Duration {
    if start < end {
        end.duration_since(start)
    } else {
        Duration::from_secs(0)
    }
}

pub struct WorkerThread<R: Runner> {
    local: LocalQueue<R::Task>,
    runner: R,
    sched_config: SchedConfig,
}

impl<R: Runner> WorkerThread<R> {
    fn new(local: LocalQueue<R::Task>, runner: R, sched_config: SchedConfig) -> WorkerThread<R> {
        WorkerThread {
            local,
            runner,
            sched_config,
        }
    }

    fn run(mut self, pause: bool) {
        let mut ctx = PoolContext::new(self.local);
        ctx.local_queue.core.wake_up();
        if pause {
            let _ = ctx.park(self.sched_config.max_idle_time, true);
        }
        let mut last_spawn_time = Instant::now();
        self.runner.start(&ctx);
        'out: while !ctx.local_queue.core.should_shutdown() {
            let (t, is_local) = match ctx.deque_a_task() {
                (Steal::Success(e), b) => (e, b),
                (Steal::Empty, _) => {
                    let mut tried_times = 0;
                    'inner: loop {
                        match ctx.deque_a_task() {
                            (Steal::Success(e), b) => break 'inner (e, b),
                            (Steal::Empty, _) => tried_times += 1,
                            (Steal::Retry, _) => continue 'out,
                        }
                        if tried_times > self.sched_config.max_inplace_spin {
                            self.runner.pause(&ctx);
                            match ctx.park(self.sched_config.max_idle_time, false) {
                                (Steal::Retry, _) => {
                                    self.runner.resume(&ctx);
                                    continue 'out;
                                },
                                (Steal::Success(e), b) => {
                                    self.runner.resume(&ctx);
                                    break 'inner (e, b);
                                },
                                (Steal::Empty, _) => break 'out,
                            }
                        }
                    }
                },
                (Steal::Retry, _) => continue,
            };
            let now = Instant::now();
            if elapsed(t.sched_time, now) >= self.sched_config.max_wait_time || is_local && elapsed(last_spawn_time, now) >= self.sched_config.local_spawn_backoff {
                ctx.local_queue.unpark_one();
                last_spawn_time = now;
            }
            self.runner.handle(&mut ctx, t.task);
        }
        // ctx.dump_metrics();
        self.runner.end(&ctx);
    }
}

#[derive(Clone)]
struct SchedConfig {
    max_thread_count: usize,
    min_thread_count: usize,
    max_inplace_spin: usize,
    max_idle_time: Option<Duration>,
    max_wait_time: Duration,
    local_spawn_backoff: Duration,
}

pub struct Config {
    name_prefix: String,
    sched_config: SchedConfig,
}

impl Config {
    pub fn new(name_prefix: impl Into<String>) -> Config {
        Config {
            name_prefix: name_prefix.into(),
            sched_config: SchedConfig {
                max_thread_count: num_cpus::get(),
                min_thread_count: 1,
                max_inplace_spin: 4,
                max_idle_time: None,
                max_wait_time: Duration::from_millis(1),
                local_spawn_backoff: Duration::from_micros(1000),
            },
        }
    }

    pub fn max_thread_count(&mut self, count: usize) -> &mut Config {
        if count > 0 {
            self.sched_config.max_thread_count = count;
        }
        self
    }

    pub fn min_thread_count(&mut self, count: usize) -> &mut Config {
        if count > 0 {
            self.sched_config.min_thread_count = count;
        }
        self
    }

    pub fn max_inplace_spin(&mut self, count: usize) -> &mut Config {
        self.sched_config.max_inplace_spin = count;
        self
    }

    pub fn max_idle_time(&mut self, time: Duration) -> &mut Config {
        self.sched_config.max_idle_time = Some(time);
        self
    }

    pub fn max_wait_time(&mut self, time: Duration) -> &mut Config {
        self.sched_config.max_wait_time = time;
        self
    }

    pub fn local_spawn_backoff(&mut self, time: Duration) -> &mut Config {
        self.sched_config.local_spawn_backoff = time;
        self
    }

    pub fn spawn<F>(&self, mut factory: F) -> ThreadPool<<<F as RunnerFactory>::Runner as Runner>::Task>
    where F: RunnerFactory,
        F::Runner: Send + 'static,
        <<F as RunnerFactory>::Runner as Runner>::Task: Send,
    {
        let injector = crossbeam_deque::Injector::new();
        let mut workers = Vec::with_capacity(self.sched_config.max_thread_count);
        let mut stealers = Vec::with_capacity(self.sched_config.max_thread_count);
        for _ in 0..self.sched_config.max_thread_count {
            let w = crossbeam_deque::Worker::new_lifo();
            stealers.push(w.stealer());
            workers.push(Some(w));
        }
        let queues = Queues {
            core: Arc::new(QueueCore {
                global: injector,
                stealers,
                locals: Mutex::new(workers),
                running_count: AtomicUsize::new(0),
            }),
        };
        let mut threads = Vec::with_capacity(self.sched_config.max_thread_count);
        for i in 0..self.sched_config.max_thread_count {
            let r = factory.produce();
            let local_queue = queues.acquire_local_queue();
            let th = WorkerThread::new(local_queue, r, self.sched_config.clone());
            let pause = i >= self.sched_config.min_thread_count;
            threads.push(Builder::new().name(format!("{}-{}", self.name_prefix, i)).spawn(move || {
                th.run(pause);
            }).unwrap());
        }
        ThreadPool {
            queues,
            threads: Mutex::new(threads),
        }
    }
}

pub struct ThreadPool<Task> {
    queues: Queues<Task>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl<Task> ThreadPool<Task> {
    pub fn spawn(&self, t: impl Into<Task>) {
        self.queues.push(t.into());
        self.queues.unpark_one();
    }

    pub fn shutdown(&self) {
        self.queues.core.shutdown();
        let mut threads = mem::replace(&mut *self.threads.lock().unwrap(), Vec::new());
        for _ in 0..threads.len() {
            self.queues.unpark_one();
        }
        for j in threads.drain(..) {
            j.join().unwrap();
        }
    }
}

impl<Task> Drop for ThreadPool<Task> {
    fn drop(&mut self) {
        self.shutdown();
    }
}
