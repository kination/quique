use bytes::{Buf, BufMut, BytesMut};
use thiserror::Error;

pub const MAGIC: u32 = 0x51425553; // 'QBUS'
pub const VERSION: u8 = 1;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    CreateTopic = 0x01,
    Produce = 0x02,
    Consume = 0x03,
    Metadata = 0x04,
    Read = 0x05,
}

impl TryFrom<u8> for Op {
    type Error = ProtoError;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        Ok(match v {
            0x01 => Op::CreateTopic,
            0x02 => Op::Produce,
            0x03 => Op::Consume,
            0x04 => Op::Metadata,
            0x05 => Op::Read,
            _ => return Err(ProtoError::InvalidOpcode(v)),
        })
    }
}

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Ok = 0,
    Redirect = 10, // 다른 노드로 가라
    Empty = 11,
    TopicExists = 12,
    NotFound = 13,
    BadRequest = 400,
    ServerError = 500,
}

#[derive(Debug, Error)]
pub enum ProtoError {
    #[error("invalid magic: {0:#x}")]
    InvalidMagic(u32),
    #[error("invalid version: {0}")]
    InvalidVersion(u8),
    #[error("invalid opcode: {0}")]
    InvalidOpcode(u8),
    #[error("short frame")]
    Short,
}

/// 16B header: magic:u32 | ver:u8 | op:u8 | flags:u8 | rsvd:u8 | stream_id:u32 | body_len:u32
#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub magic: u32,
    pub version: u8,
    pub op: Op,
    pub flags: u8,
    pub stream_id: u32,
    pub body_len: u32,
}
impl Header {
    pub const LEN: usize = 16;
    pub fn encode(&self, dst: &mut BytesMut) {
        dst.put_u32(MAGIC);
        dst.put_u8(VERSION);
        dst.put_u8(self.op as u8);
        dst.put_u8(self.flags);
        dst.put_u8(0);
        dst.put_u32(self.stream_id);
        dst.put_u32(self.body_len);
    }
    pub fn decode(src: &mut BytesMut) -> Result<Option<Self>, ProtoError> {
        if src.len() < Self::LEN {
            return Ok(None);
        }
        let mut cur = &src[..];
        let magic = cur.get_u32();
        if magic != MAGIC {
            return Err(ProtoError::InvalidMagic(magic));
        }
        let ver = cur.get_u8();
        if ver != VERSION {
            return Err(ProtoError::InvalidVersion(ver));
        }
        let op = Op::try_from(cur.get_u8())?;
        let flags = cur.get_u8();
        let _r = cur.get_u8();
        let stream_id = cur.get_u32();
        let body_len = cur.get_u32();
        src.advance(Self::LEN);
        Ok(Some(Header {
            magic,
            version: ver,
            op,
            flags,
            stream_id,
            body_len,
        }))
    }
}

// TLV helpers (string, bytes, u32)
pub fn put_str(buf: &mut BytesMut, s: &str) {
    buf.put_u16(s.len() as u16);
    buf.extend_from_slice(s.as_bytes());
}
pub fn get_str(b: &mut &[u8]) -> Option<String> {
    if b.len() < 2 {
        return None;
    }
    let len = u16::from_be_bytes([b[0], b[1]]) as usize;
    *b = &b[2..];
    if b.len() < len {
        return None;
    }
    let s = String::from_utf8_lossy(&b[..len]).to_string();
    *b = &b[len..];
    Some(s)
}
pub fn put_bytes(buf: &mut BytesMut, v: &[u8]) {
    buf.put_u32(v.len() as u32);
    buf.extend_from_slice(v);
}
pub fn get_bytes(b: &mut &[u8]) -> Option<Vec<u8>> {
    if b.len() < 4 {
        return None;
    }
    let n = u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize;
    *b = &b[4..];
    if b.len() < n {
        return None;
    }
    let v = b[..n].to_vec();
    *b = &b[n..];
    Some(v)
}
pub fn put_u32(buf: &mut BytesMut, v: u32) {
    buf.put_u32(v);
}
pub fn get_u32(b: &mut &[u8]) -> Option<u32> {
    if b.len() < 4 {
        return None;
    }
    let v = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    *b = &b[4..];
    Some(v)
}
pub fn put_status(buf: &mut BytesMut, st: Status) {
    buf.put_u16(st as u16);
}
