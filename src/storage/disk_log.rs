use anyhow::Result;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Record: [u8 type=1][u64 seq][u32 len][bytes]
#[derive(Clone)]
pub struct DiskLog {
    path: PathBuf,
    writer: Arc<Mutex<BufWriter<File>>>,
    seq: Arc<AtomicU64>,
    ack_path: PathBuf,
}

impl DiskLog {
    pub fn open<P: AsRef<Path>>(dir: P, topic: &str, part: u32) -> Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let path = dir.join(format!("{}-{}.log", topic, part));
        let ack_path = dir.join(format!("{}-{}.ack", topic, part));
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        // scan last seq
        let mut last = 0u64;
        if f.metadata()?.len() > 0 {
            let mut r = &f;
            r.seek(SeekFrom::Start(0))?;
            let mut buf = Vec::new();
            r.read_to_end(&mut buf)?;
            let mut off = 0usize;
            while off + 13 <= buf.len() {
                let _t = buf[off];
                let seq = u64::from_be_bytes(buf[off + 1..off + 9].try_into().unwrap());
                let n = u32::from_be_bytes(buf[off + 9..off + 13].try_into().unwrap()) as usize;
                off += 13 + n;
                last = seq;
            }
        }
        let writer = BufWriter::new(OpenOptions::new().create(true).append(true).open(&path)?);
        Ok(Self {
            path,
            writer: Arc::new(Mutex::new(writer)),
            seq: Arc::new(AtomicU64::new(last)),
            ack_path,
        })
    }

    pub fn append(&self, payload: &[u8]) -> Result<u64> {
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let mut rec = Vec::with_capacity(13 + payload.len());
        rec.push(1u8);
        rec.extend_from_slice(&seq.to_be_bytes());
        rec.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        rec.extend_from_slice(payload);
        let mut w = self.writer.lock().unwrap();
        w.write_all(&rec)?;
        w.flush()?;
        if let Ok(f) = w.get_ref().try_clone() {
            f.sync_all()?;
        }
        Ok(seq)
    }

    pub fn read_acked(&self) -> Result<u64> {
        if !self.ack_path.exists() {
            return Ok(0);
        }
        let mut f = File::open(&self.ack_path)?;
        let mut b = [0u8; 8];
        if f.read(&mut b)? < 8 {
            return Ok(0);
        }
        Ok(u64::from_be_bytes(b))
    }

    pub fn write_acked(&self, s: u64) -> Result<()> {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.ack_path)?;
        f.write_all(&s.to_be_bytes())?;
        f.sync_all()?;
        Ok(())
    }

    /// (seq,payload) of unacked
    pub fn replay_unacked(&self) -> Result<Vec<(u64, Vec<u8>)>> {
        let acked = self.read_acked()?;
        let mut f = File::open(&self.path)?;
        f.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        let mut off = 0usize;
        let mut out = Vec::new();
        while off + 13 <= buf.len() {
            let t = buf[off];
            let seq = u64::from_be_bytes(buf[off + 1..off + 9].try_into().unwrap());
            let n = u32::from_be_bytes(buf[off + 9..off + 13].try_into().unwrap()) as usize;
            let s = off + 13;
            let e = s + n;
            if e > buf.len() {
                break;
            }
            if t == 1 && seq > acked {
                out.push((seq, buf[s..e].to_vec()));
            }
            off = e;
        }
        Ok(out)
    }

    pub fn read_last_n(&self, n: usize) -> Result<Vec<Vec<u8>>> {
        let mut f = File::open(&self.path)?;
        f.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        if buf.is_empty() {
            return Ok(Vec::new());
        }
        let mut off = 0usize;
        let mut out = Vec::new();
        while off + 13 <= buf.len() {
            let t = buf[off];
            let seq = u64::from_be_bytes(buf[off + 1..off + 9].try_into().unwrap());
            let len = u32::from_be_bytes(buf[off + 9..off + 13].try_into().unwrap()) as usize;
            let s = off + 13;
            let e = s + len;
            if e > buf.len() {
                break;
            }
            if t == 1 {
                out.push(buf[s..e].to_vec());
            }
            off = e;
        }
        let start = out.len().saturating_sub(n);
        Ok(out.split_off(start))
    }
}
