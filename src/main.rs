use std::io::{self, BufWriter, Read, Write};
use std::time::Instant;

// A Read that yields N bytes, returning at most `chunk` per call (like a pipe).
struct Src {
    left: usize,
    chunk: usize,
}
impl Read for Src {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = buf.len().min(self.left).min(self.chunk);
        for b in &mut buf[..n] {
            *b = 7;
        }
        self.left -= n;
        Ok(n)
    }
}

fn main() {
    const N: usize = 1 << 30;
    for _ in 0..2 {
        // spool: 8 KiB io::copy vs BufWriter 1 MiB
        let mut r: Box<dyn Read + Send> = Box::new(Src {
            left: N,
            chunk: 64 << 10,
        });
        let mut f = tempfile::tempfile().unwrap();
        let t = Instant::now();
        io::copy(&mut r, &mut f).unwrap();
        println!("spool io::copy -> File      : {:?}", t.elapsed());

        let mut r: Box<dyn Read + Send> = Box::new(Src {
            left: N,
            chunk: 64 << 10,
        });
        let f = tempfile::tempfile().unwrap();
        let t = Instant::now();
        let mut w = BufWriter::with_capacity(1 << 20, f);
        io::copy(&mut r, &mut w).unwrap();
        w.flush().unwrap();
        println!("spool io::copy -> BufWriter : {:?}", t.elapsed());

        // read_to_end: grow vs presized
        let mut r: Box<dyn Read + Send> = Box::new(Src {
            left: N,
            chunk: usize::MAX,
        });
        let t = Instant::now();
        let mut v = Vec::new();
        r.read_to_end(&mut v).unwrap();
        println!(
            "read_to_end grow            : {:?} cap={}",
            t.elapsed(),
            v.capacity()
        );
        drop(v);

        let mut r: Box<dyn Read + Send> = Box::new(Src {
            left: N,
            chunk: usize::MAX,
        });
        let t = Instant::now();
        let mut v = Vec::with_capacity(N);
        r.read_to_end(&mut v).unwrap();
        println!(
            "read_to_end presized        : {:?} cap={}",
            t.elapsed(),
            v.capacity()
        );
    }
}
