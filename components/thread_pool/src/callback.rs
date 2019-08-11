use crate::{PoolContext, SpawnHandle};

pub enum Task {
    Once(Box<dyn FnOnce(&mut SchedulerRef<'_>) + Send>),
    Mut(Box<dyn FnMut(&mut SchedulerRef<'_>) + Send>),
}

pub struct Runner {
    max_inplace_spin: usize,
}

impl crate::Runner for Runner {
    type Task = Task;

    fn handle(&mut self, ctx: &mut PoolContext<Task>, mut task: Task) {
        let mut sched_ref = SchedulerRef {
            ctx,
            rerun: false,
        };
        match task {
            Task::Mut(ref mut r) => {
                let mut tried_times = 0;
                loop {
                    r(&mut sched_ref);
                    if !sched_ref.rerun {
                        return;
                    }
                    // TODO: fix the bug here when set to true.
                    sched_ref.rerun = false;
                    tried_times += 1;
                    if tried_times == self.max_inplace_spin {
                        break;
                    }
                }
            }
            Task::Once(r) => {
                r(&mut sched_ref);
                return;
            }
        }
        ctx.spawn(task);
    }
}

pub struct SchedulerRef<'a> {
    ctx: &'a mut PoolContext<Task>,
    rerun: bool,
}

impl<'a> SchedulerRef<'a> {
    pub fn spawn_once(&mut self, t: impl FnOnce(&mut SchedulerRef<'_>) + Send + 'static) {
        self.ctx.spawn(Task::Once(Box::new(t)));
    }

    pub fn spawn_mut(&mut self, t: impl FnMut(&mut SchedulerRef<'_>) + Send + 'static) {
        self.ctx.spawn(Task::Mut(Box::new(t)))
    }

    pub fn rerun(&mut self) {
        self.rerun = true;
    }

    pub fn to_owned(&self) -> Scheduler {
        Scheduler {
            handle: self.ctx.spawn_handle(),
        }
    }
}

pub struct Scheduler {
    handle: SpawnHandle<Task>,
}

impl Scheduler {
    pub fn spawn_once(&self, t: impl FnOnce(&mut SchedulerRef<'_>) + Send + 'static) {
        self.handle.spawn(Task::Once(Box::new(t)));
    }

    pub fn spawn_mut(&self, t: impl FnMut(&mut SchedulerRef<'_>) + Send + 'static) {
        self.handle.spawn(Task::Mut(Box::new(t)))
    }
}

pub struct RunnerFactory {
    max_inplace_spin: usize,
}

impl RunnerFactory {
    pub fn new() -> RunnerFactory {
        RunnerFactory {
            max_inplace_spin: 4,
        }
    }

    pub fn set_max_inplace_spin(&mut self, count: usize) {
        self.max_inplace_spin = count;
    }
}

impl crate::RunnerFactory for RunnerFactory {
    type Runner = Runner;

    fn produce(&mut self) -> Runner {
        Runner {
            max_inplace_spin: self.max_inplace_spin,
        }
    }
}

impl crate::ThreadPool<Task> {
    pub fn spawn_once(&self, t: impl FnOnce(&mut SchedulerRef<'_>) + Send + 'static) {
        self.spawn(Task::Once(Box::new(t)));
    }

    pub fn spawn_mut(&self, t: impl FnMut(&mut SchedulerRef<'_>) + Send + 'static) {
        self.spawn(Task::Mut(Box::new(t)))
    }
}
