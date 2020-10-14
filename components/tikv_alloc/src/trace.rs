// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::lazy::Lazy;
use std::sync::Mutex;

pub enum Id {
    Name(&'static str),
    Number(u64),
}

pub struct MemoryTrace {
    id: Id,
    size: Option<usize>,
    children: Vec<MemoryTrace>,
}

impl MemoryTrace {
    fn new(id: Id) -> MemoryTrace {
        MemoryTrace {
            id,
            size: None,
            children: vec![],
        }
    }

    pub fn add_sub_trace(&mut self, id: Id) -> &mut MemoryTrace {
        self.children.push(MemoryTrace::new(id));
        self.children.last_mut().unwrap()
    }

    pub fn add_sub_trace_with_capacity(&mut self, id: Id, cap: usize) -> &mut MemoryTrace {
        let mut trace = MemoryTrace::new(id);
        trace.children.reserve(cap);
        self.children.push(trace);
        self.children.last_mut().unwrap()
    }

    pub fn set_size(&mut self, size: usize) {
        self.size = Some(size);
    }
}

pub trait MemoryTraceProvider {
    fn trace(&mut self, dump: &mut MemoryTrace);
}

pub struct MemoryTraceManager {
    providers: Vec<Box<dyn MemoryTraceProvider>>,
}

impl MemoryTraceManager {
    pub fn snapshot(&mut self) -> MemoryTrace {
        let mut trace = MemoryTrace::new(Id::Name("TiKV"));
        for provider in &mut self.providers {
            provider.trace(&mut trace);
        }
        trace
    }

    pub fn register_provider(&mut self, provider: Box<dyn MemoryTraceProvider + Send + 'static>) {
        self.providers.push(provider);
    }
}

static GLOBAL_MEMORY_TRACE_MANAGER: Lazy<Mutex<MemoryTraceManager>> = Lazy::new(|| {
    Mutex::new(MemoryTraceManager {
        providers: vec![],
    })
});

pub fn register_provider(provider: Box<dyn MemoryTraceProvider + Send + 'static>) {
    let mut manager = GLOBAL_MEMORY_TRACE_MANAGER.lock().unwrap();
    manager.register_provider(provider);
}

pub fn snapshot() -> MemoryTrace {
    let mut manager = GLOBAL_MEMORY_TRACE_MANAGER.lock().unwrap();
    manager.trace()
}
