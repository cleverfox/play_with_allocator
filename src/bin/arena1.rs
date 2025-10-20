#![allow(unused_imports)]
use allocator_api2::alloc::{AllocError, Allocator, Layout};
use allocator_api2::vec::Vec;
use std::num::NonZeroIsize;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

struct ArenaAllocInt {
    buf: NonNull<u8>,
    cap: usize,
    offset: AtomicUsize,
    align: usize,
}

impl Drop for ArenaAllocInt {
    fn drop(&mut self) {
        unsafe {
            let layout = Layout::from_size_align(self.cap, self.align).unwrap();
            std::alloc::dealloc(self.buf.as_ptr(), layout);
        }
    }
}

#[derive(Clone)]
pub struct ArenaAlloc(Arc<ArenaAllocInt>);

unsafe impl Send for ArenaAlloc {}
unsafe impl Sync for ArenaAlloc {}

impl ArenaAlloc {
    pub fn with_capacity(bytes: usize) -> Self {
        let align = std::mem::align_of::<usize>().max(8); // 8 byte align for 64bit machines
        let layout = Layout::from_size_align(bytes.max(1), align).expect("bad layout");
        let ptr = unsafe { std::alloc::alloc(layout) };
        let buf = NonNull::new(ptr).expect("arena region allocation failed");
        ArenaAlloc(Arc::new(ArenaAllocInt {
            buf,
            cap: bytes,
            offset: AtomicUsize::new(0),
            align,
        }))
    }

    #[inline]
    fn align_up(x: usize, align: usize) -> usize {
        debug_assert!(align.is_power_of_two());
        (x + (align - 1)) & !(align - 1)
    }

    fn bump_alloc(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let base = self.0.buf.as_ptr() as usize;
        let size = layout.size();
        let align = layout.align().max(1);

        // CAS loop to reserve [start, start+size)
        let mut cur = self.0.offset.load(Ordering::Relaxed);
        loop {
            let start = Self::align_up(base + cur, align) - base;
            let new_off = start.checked_add(size).ok_or(AllocError)?;
            if new_off > self.0.cap {
                println!(
                    "Filed to allocate {} bytes at offset {} AllocError",
                    size, start
                );
                return Err(AllocError);
            }
            match self.0.offset.compare_exchange_weak(
                cur,
                new_off,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    println!("Allocated {} bytes at offset {}", size, start);
                    let p = (base + start) as *mut u8;
                    // SAFETY: p points into the reserved region, size <= cap, aligned per `align`.
                    let nn = NonNull::slice_from_raw_parts(NonNull::new(p).unwrap(), size);
                    return Ok(nn);
                }
                Err(actual) => {
                    println!(
                        "Failed to allocate {} bytes at offset {} err {}",
                        size, start, actual
                    );
                    cur = actual;
                }
            }
        }
    }
}

unsafe impl Allocator for ArenaAlloc {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let ptr = self.bump_alloc(layout);
        println!("Alloc {} bytes {:#x}", layout.size(), ptr.unwrap().addr());
        ptr
    }
    unsafe fn deallocate(&self, _ptr: NonNull<u8>, _layout: Layout) {
        println!("Deallocating memory {:?} layout {:?}", _ptr, _layout);
    }
}

// =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-

type AVec<T> = Vec<T, ArenaAlloc>;

pub trait MyClassImpl: Sync + Send {
    fn to_cell(self) -> Cell;
    fn sum_a(&self) -> u64;
}

pub struct Cell(Arc<dyn MyClassImpl>);

impl Drop for MyClass {
    fn drop(&mut self) {
        println!("Drop {:?} a {:?}", self.id, self.a)
    }
}
impl Cell {
    pub fn new(id: u64, cnt_a: usize, cnt_b: usize, cnt_c: usize) -> Self {
        Cell(Arc::new(MyClass::new(id, cnt_a, cnt_b, cnt_c)))
    }
}
impl MyClassImpl for MyClass {
    fn to_cell(self) -> Cell {
        Cell(Arc::new(self))
    }

    fn sum_a(&self) -> u64 {
        println!("sum_a");
        //self.a.iter().map(|&x| x as u64).sum()
        self.sum_a()
    }
}

pub struct MyClass {
    #[allow(dead_code)]
    arena: ArenaAlloc, // keep the arena alive as long as the object
    id: u64,
    a: AVec<u32>,
    b: AVec<u8>,
    neighbour: AVec<Cell>,
}

impl MyClass {
    pub fn new(id: u64, cnt_a: usize, cnt_b: usize, cnt_c: usize) -> Self {
        let estimated_bytes = Self::estimate_size(cnt_a, cnt_b, cnt_c);
        println!("Creating MyClass with estimated_bytes: {}", estimated_bytes);
        let arena = ArenaAlloc::with_capacity(estimated_bytes);

        let mut a: AVec<u32> = AVec::new_in(arena.clone());
        let mut b: AVec<u8> = AVec::new_in(arena.clone());
        let mut neighbour: AVec<Cell> = AVec::new_in(arena.clone());

        // Reserve to avoid allocator fallback re-growth paths (optional)
        println!("Reserve A {} elements", cnt_a);
        a.reserve_exact(cnt_a);
        println!("Reserve B {} elements", cnt_b);
        b.reserve_exact(cnt_b);
        println!("Reserve Neighbour {} element", cnt_c);
        neighbour.reserve_exact(cnt_c);

        println!("Fillup a");
        for i in 0..cnt_a as u32 {
            a.push(i);
        }
        println!("Fillup B");
        b.extend((0u8..=255).cycle().take(cnt_b));

        println!("Done");
        MyClass {
            arena,
            id,
            a,
            b,
            neighbour,
        }
    }
    fn estimate_size(ca: usize, cb: usize, cc: usize) -> usize {
        let a_bytes = ca * std::mem::size_of::<u32>();
        let b_bytes = cb * std::mem::size_of::<u8>();
        let c_bytes = cc * std::mem::size_of::<Cell>();
        (a_bytes + b_bytes + c_bytes) + 64 * 1024 // +64KiB headroom
    }
    pub fn add_neighbour(&mut self, neighbour: Cell) {
        self.neighbour.push(neighbour);
    }

    pub fn stats(&self) -> (usize, usize, usize) {
        (self.a.len(), self.b.len(), self.neighbour.len())
    }

    pub fn sum_a(&self) -> u64 {
        self.a.iter().map(|&x| x as u64).sum()
    }

    pub fn sum_r(&self) -> u64 {
        println!("sum_r");
        self.neighbour.iter().map(|x| x.0.sum_a()).sum()
    }
}

unsafe impl Send for MyClass {}
unsafe impl Sync for MyClass {}

fn main() {
    let _profiler = dhat::Profiler::new_heap();
    let mut parent = MyClass::new(0, 1, 2, 10);
    for i in 0..4 {
        let a = (i as usize + 1) * 10;
        let obj = Cell::new(i + 1, a, 200, 0);
        parent.add_neighbour(obj);

        //let mut handles: Vec<std::thread::JoinHandle<u64>> = Vec::new();
        // let mut handles: Vec<u64> = Vec::new();
        // for _ in 0..2 {
        //     let obj_cloned = Arc::clone(&obj);
        //     //handles.push(thread::spawn(move || obj_cloned.sum_a()));
        //     handles.push(obj_cloned.sum_a());
        // }

        // //let total: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        // let total: u64 = handles.into_iter().sum();
        // println!("Sum(a) across threads (redundant work): {total}");
    }
    println!("ChSum {:?}", parent.stats());
    println!("Sum {:?}", parent.sum_r());
}
