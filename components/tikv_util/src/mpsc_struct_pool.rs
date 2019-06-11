// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::ptr::{self, NonNull};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::mem;

const INITIAL_FLAGS: usize = 0xAAAAAAAAAAAAAAAA;

pub struct Pointer<T> {
    ptr: NonNull<T>,
    root: NonNull<Block<T>>,
}

impl<T> Pointer<T> {
    #[inline]
    pub fn take(mut self, gc: &mut Gc<T>) -> T {
        let a = unsafe { ptr::read(self.ptr.as_mut()) };
        gc.reclaim(self);
        a
    }

    #[inline]
    fn mask(&self) -> usize {
        let offset = unsafe { self.ptr.as_ptr().offset_from(self.root.as_ref().t) };
        1 << (offset * 2)
    }
}

impl<T> Deref for Pointer<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        unsafe { self.ptr.as_ref() }
    }
}

impl<T> DerefMut for Pointer<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        unsafe { self.ptr.as_mut() }
    }
}

impl<T> Drop for Pointer<T> {
    fn drop(&mut self) {
        unsafe { ptr::drop_in_place(self.ptr.as_ptr()); }
        let mask = self.mask();
        unsafe { self.root.as_ref().occupied.fetch_add(mask, Ordering::SeqCst); }
    }
}

unsafe impl<T: Sync> Sync for Pointer<T> {}
unsafe impl<T: Send> Send for Pointer<T> {}

pub struct Gc<T> {
    trash: Vec<(NonNull<Block<T>>, usize)>,
}

impl<T> Gc<T> {
    #[inline]
    pub fn collect(&mut self, pointer: Pointer<T>) {
        unsafe { ptr::drop_in_place(pointer.ptr.as_ptr()); }
        self.reclaim(pointer);
    }

    fn reclaim(&mut self, pointer: Pointer<T>) {
        let mask = pointer.mask();
        for (ptr, freed) in &mut self.trash {
            if *ptr == pointer.root {
                *freed += mask;
                mem::forget(pointer);
                return;
            }
        }
        self.trash.push((pointer.root, mask));
        mem::forget(pointer);
    }

    pub fn gc(&mut self) {
        unsafe {
            for (ptr, freed) in &mut self.trash {
                ptr.as_ref().occupied.fetch_add(*freed, Ordering::SeqCst);
            }
            self.trash.set_len(0);
        }
    }
}

impl<T> Drop for Gc<T> {
    fn drop(&mut self) {
        self.gc();
    }
}

unsafe impl<T: Send> Send for Gc<T> {}

struct Block<T> {
    occupied: AtomicUsize,
    cache: usize,
    t: *mut T,
    next: *mut Block<T>,
}

impl<T> Block<T> {
    fn new() -> Box<Block<T>> {
        let mut v = Vec::with_capacity(32);
        let t = v.as_mut_ptr();
        mem::forget(v);
        box Block {
            occupied: AtomicUsize::new(INITIAL_FLAGS),
            cache: INITIAL_FLAGS,
            t,
            next: ptr::null_mut(),
        }
    }
}

pub struct Allocator<T> {
    block: Box<Block<T>>,
    cursor: NonNull<Block<T>>,
}

impl<T> Allocator<T> {
    #[inline]
    pub fn alloc<F>(&mut self, init: F) -> Pointer<T>
    where F: FnOnce(&mut T)
    {
        let mut cursor = self.cursor;
        let f = unsafe { cursor.as_mut().cache & INITIAL_FLAGS };
        let pos = f.trailing_zeros();
        if pos < 64 {
            let offset = pos / 2;
            unsafe { cursor.as_mut().cache -= 1 << offset; }
            let data = unsafe { cursor.as_ref().t.add(offset as usize) };
            unsafe { init(&mut *data); }
            return Pointer {
                ptr: unsafe { NonNull::new_unchecked(data) },
                root: cursor,
            };
        }
        unsafe { self.flush_and_loop(init) }
    }

    unsafe fn flush_and_loop<F>(&mut self, init: F) -> Pointer<T>
    where F: FnOnce(&mut T)
    {
        let mut cursor = self.cursor;
        let mut end = self.cursor;
        loop {
            let cache = cursor.as_ref().cache;
            let previous = cursor.as_ref().occupied.fetch_sub(cache, Ordering::SeqCst);
            cursor.as_mut().cache = previous - cache;
            if cursor.as_ref().cache & INITIAL_FLAGS > 0 {
                self.cursor = cursor;
                return self.alloc(init);
            }

            if cursor.as_ref().next.is_null() {
                end = cursor;
                cursor = NonNull::new_unchecked(self.block.as_mut());
            } else {
                cursor = NonNull::new_unchecked(cursor.as_ref().next);
            }
            if cursor == self.cursor {
                cursor = end;
                break;
            }
            if cursor.as_ref().cache & INITIAL_FLAGS > 0 {
                self.cursor = cursor;
                return self.alloc(init);
            }
        }
        let block = Block::new();
        cursor.as_mut().next = Box::into_raw(block);
        self.cursor = NonNull::new_unchecked(cursor.as_ref().next);
        self.alloc(init)
    }
}

pub fn spawn<T>() -> (Allocator<T>, Gc<T>) {
    let mut block = Block::new();
    (Allocator {
        cursor: unsafe { NonNull::new_unchecked(block.as_mut()) },
        block,
    }, Gc {
        trash: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use test::Bencher;

    const ARRAY_SIZE: usize = 20;

    pub struct LargeStruct {
        a: [usize; ARRAY_SIZE],
    }

    pub fn move_raw_check(a: LargeStruct) {
        for i in 0..ARRAY_SIZE {
            assert_eq!(a.a[i], i);
        }
    }

    pub fn move_pointer_check(a: Pointer<LargeStruct>, gc: &mut Gc<LargeStruct>) {
        for i in 0..ARRAY_SIZE {
            assert_eq!(a.a[i], i);
        }
        gc.collect(a);
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
            let mut a = LargeStruct { a: [0; ARRAY_SIZE] };
            for i in 0..ARRAY_SIZE {
                a.a[i] = i;
            }
            tx.send(a).unwrap();
        })
    }

    #[bench]
    fn bench_pool_init(b: &mut Bencher) {
        let (tx, rx) = mpsc::channel();
        let (mut allocator, mut gc) = super::spawn();
        let t = std::thread::spawn(move || {
            let mut counter = 0;
            while let Ok(s) = rx.recv() {
                move_pointer_check(s, &mut gc);
                counter += 1;
                if counter > 16 {
                    gc.gc();
                }
            }
        });
        b.iter(|| {
            let a = allocator.alloc(|p| {
                for i in 0..ARRAY_SIZE {
                    p.a[i] = i;
                }
            });
            tx.send(a).unwrap();
        });
        drop(tx);
        t.join().unwrap();
    }
}
