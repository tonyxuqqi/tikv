// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::ptr::NonNull;
use std::{cmp, u64, ptr, mem};
use std::alloc::Layout;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};

const POOL_SLICE_SIZE: usize = 64;

struct PoolSlice<T> {
    ptr: Box<[T; POOL_SLICE_SIZE]>,
    occupied: AtomicU64,
    next: Option<Box<PoolSlice<T>>>,
}

fn new_pool_slice<T>() -> Box<PoolSlice<T>> {
    Box::new(PoolSlice {
        ptr: Box::new(unsafe { mem::uninitialized() }),
        occupied: AtomicU64::new(u64::MAX),
        next: None,
    })
}

pub struct ObjectPool<T> {
    head: NonNull<PoolSlice<T>>,
    layout: Layout,
    available_index: usize,
}

pub struct Pointer<T> {
    ptr: NonNull<T>,
    offset: usize,
    _data: PhantomData<T>,
}

impl<T> Pointer<T> {
    fn new(ptr: NonNull<T>, offset: usize) -> Pointer<T> {
        Pointer {
            ptr,
            offset,
            _data: PhantomData,
        }
    }
}

impl<T> ObjectPool<T> {
    pub fn new() -> ObjectPool<T> {
        ObjectPool {
            head: NonNull::new_unchecked(ptr: *mut T)
            slices: Vec::with_capacity(cap),
            available_index: 0,
            layout: Layout::array::<T>(POOL_SLICE_SIZE).unwrap(),
        }
    }

    pub unsafe fn alloc_raw(&mut self) -> (&mut T, Pointer<T>) {
        let available_index = self.available_index;
        loop {
            let slice = self.slices.get_unchecked_mut(available_index);
            let occupied = slice.occupied.load(Ordering::SeqCst);
            if occupied != 0 {
                let pos = occupied.trailing_zeros();
                let new_occupied = occupied - (1 << pos as u64);
                let last_occupied = slice.occupied.compare_and_swap(occupied, new_occupied, Ordering::SeqCst);
                if last_occupied == occupied {
                    let pointer = Pointer::new(self.available_index, pos as usize);
                    return (&mut *slice.ptr.as_ptr().add(pos as usize), pointer);
                }
                if last_occupied != 0 {
                    continue;
                }
            }
            available_index += 1;
            if available_index == self.slices.len() {
                available_index = 0;
            } else if available_index == self.available_index {
                break;
            }
        }
        let slice = alloc::alloc(self.layout);
        self.available_index = self.slices.len();
        self.slices.push(PoolSlice {
            ptr: NonNull::new_unchecked(slice as *mut T),
            occupied: u64::MAX,
        });
        self.alloc_raw()
    }

    pub fn get_mut<'a>(&self, pos: &'a mut Pointer<T>) -> &'a mut T {
        let (slice_pos, in_slice_pos) = pos.coordinate();
        let slice = unsafe { self.slices.get_unchecked_mut(slice_pos) };
        unsafe { &mut * slice.ptr.as_ptr().add(in_slice_pos) }
    }

    pub fn get<'a>(&self, pos: &'a Pointer<T>) -> &'a T {
        let (slice_pos, in_slice_pos) = pos.coordinate();
        let slice = unsafe { self.slices.get_unchecked_mut(slice_pos) };
        unsafe { &* slice.ptr.as_ptr().add(in_slice_pos) }
    }

    pub fn free(&self, pos: Pointer<T>) {
        let (slice_pos, in_slice_pos) = pos.coordinate();
        unsafe {
            let slice = self.slices.get_unchecked_mut(slice_pos);
            let p = slice.ptr.as_ptr().add(in_slice_pos);
            ptr::drop_in_place(p);
            slice.occupied += 1 << in_slice_pos;
        }
    }
}
