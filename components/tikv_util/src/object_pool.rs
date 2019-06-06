// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::ptr::NonNull;
use std::{u64, ptr, mem, usize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicPtr, Ordering};

const WORD_SIZE: usize = 64;
const WORD_PER_SLICE: usize = 4;
const POOL_SLICE_SIZE: usize = WORD_SIZE * WORD_PER_SLICE;

pub struct Pointer<T> {
    ptr: NonNull<T>,
    offset: usize,
}

impl<T> Pointer<T> {
    fn new(ptr: NonNull<T>, offset: usize) -> Pointer<T> {
        Pointer {
            ptr,
            offset,
        }
    }
}

impl<T> Pointer<T> {
    #[inline]
    pub fn take(mut self) -> T {
        let a = unsafe { ptr::read(self.ptr.as_mut()) };
        self.release_slot();
        a
    }

    #[inline]
    fn release_slot(&mut self) {
        let root = unsafe { &mut *(self.ptr.as_ptr().sub(self.offset) as *mut PoolSlice<T>) };
        root.free_slots[self.offset / WORD_SIZE].fetch_sub(1 << (self.offset % WORD_SIZE), Ordering::SeqCst);
        self.offset = usize::MAX;
    }
}

unsafe impl<T: Sync> Sync for Pointer<T> {}
unsafe impl<T: Send> Send for Pointer<T> {}

impl<T> AsRef<T> for Pointer<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        unsafe { self.ptr.as_ref() }
    }
}

impl<T> AsMut<T> for Pointer<T> {
    #[inline]
    fn as_mut(&mut self) -> &mut T {
        unsafe { self.ptr.as_mut() }
    }
}

impl<T> Drop for Pointer<T> {
    fn drop(&mut self) {
        if self.offset > POOL_SLICE_SIZE {
            return;
        }
        unsafe { ptr::drop_in_place(self.ptr.as_ptr()); }
        self.release_slot();
    }
}

struct PoolSlice<T> {
    ptr: [T; POOL_SLICE_SIZE],
    free_slots: [AtomicU64; WORD_PER_SLICE],
    next: AtomicPtr<PoolSlice<T>>,
}

fn new_pool_slice<T>() -> Box<PoolSlice<T>> {
    box PoolSlice {
        ptr: unsafe { mem::uninitialized() },
        free_slots: unsafe { mem::transmute([u64::MAX; WORD_PER_SLICE]) },
        next: AtomicPtr::new(ptr::null_mut()),
    }
}

struct PoolHead<T> {
    ptr: Box<PoolSlice<T>>,
}

impl<T> PoolHead<T> {
    #[inline]
    fn new(ptr: Box<PoolSlice<T>>) -> PoolHead<T> {
        PoolHead { ptr }
    }
    #[inline]
    fn head_ptr(&self) -> *const PoolSlice<T> {
        self.ptr.as_ref()
    }
}

impl<T> Drop for PoolHead<T> {
    fn drop(&mut self) {
        let mut cursor = self.ptr.next.load(Ordering::SeqCst);
        while !cursor.is_null() {
            let p = unsafe { Box::from_raw(cursor) };
            cursor = p.next.load(Ordering::SeqCst);
            mem::forget(p);
        }
    }
}

pub struct ObjectPool<T> {
    head: Arc<PoolHead<T>>,
    cursor: NonNull<PoolSlice<T>>,
}

impl<T> ObjectPool<T> {
    pub fn new() -> ObjectPool<T> {
        let mut head = new_pool_slice();
        let cursor = unsafe { NonNull::new_unchecked(head.as_mut() as *mut PoolSlice<T>) };
        ObjectPool {
            head: Arc::new(PoolHead::new(head)),
            cursor,
        }
    }

    #[inline]
    pub fn alloc_raw(&mut self) -> Pointer<T> {
        let mut visited_end: bool = false;
        loop {
            let next = {
                let cursor = unsafe { self.cursor.as_ref() };
                let mut i = 0;
                while i < WORD_PER_SLICE {
                    let occupied = cursor.free_slots[i].load(Ordering::SeqCst);
                    if occupied == 0 {
                        continue;
                    }
                    let pos = occupied.trailing_zeros();
                    let new_occupied = occupied - (1 << pos as u64);
                    let last_occupied = cursor.free_slots[i].compare_and_swap(occupied, new_occupied, Ordering::SeqCst);
                    if last_occupied == occupied {
                        let offset = WORD_SIZE * i + pos as usize;
                        let ptr = unsafe { cursor.ptr.get_unchecked(offset) as *const _ as *mut _ };
                        return Pointer::new(unsafe { NonNull::new_unchecked(ptr) }, offset);
                    }
                    i += 1;
                }
                cursor.next.load(Ordering::SeqCst)
            };
            self.cursor = if !next.is_null() {
                unsafe { NonNull::new_unchecked(next) }
            } else if !visited_end {
                visited_end = true;
                unsafe { NonNull::new_unchecked(self.head.head_ptr() as *mut _) }
            } else {
                break;
            }
        }
        self.insert_slice();
        self.alloc_raw()
    }

    fn insert_slice(&self) {
        let s = new_pool_slice();
        let s_p = Box::into_raw(s);
        {
            let cursor = unsafe { self.cursor.as_ref() };
            let last = cursor.next.compare_and_swap(ptr::null_mut(), s_p, Ordering::SeqCst);
            if !last.is_null() {
                unsafe { Box::from_raw(s_p) };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use test::Bencher;

    pub struct LargeStruct {
        a: [usize; 12],
    }

    pub fn move_raw_check(a: LargeStruct) {
        for i in 0..12 {
            assert_eq!(a.a[i], i);
        }
    }

    pub fn move_pointer_check(a: Pointer<LargeStruct>) {
        for i in 0..12 {
            assert_eq!(a.as_ref().a[i], i);
        }
    }

    #[bench]
    fn bench_stack_init(b: &mut Bencher) {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            while let Ok(s) = rx.recv() {
                move_raw_check(s);
            }
        });
        b.iter(|| {
            let mut a = LargeStruct { a: [0; 12] };
            for i in 0..12 {
                a.a[i] = i;
            }
            tx.send(a).unwrap();
        })
    }

    #[bench]
    fn bench_pool_init(b: &mut Bencher) {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            while let Ok(s) = rx.recv() {
                move_pointer_check(s);
            }
        });
        let mut pool: ObjectPool<LargeStruct> = ObjectPool::new();
        b.iter(|| {
            let mut a = pool.alloc_raw();
            for i in 0..12 {
                a.as_mut().a[i] = i;
            }
            tx.send(a).unwrap();
        })
    }
}
