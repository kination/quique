use bytes::{Buf, BufMut, BytesMut};
use thiserror::Error;

pub const MAGIC: u32 = 0x544F5151; // 'TOQQ'
pub const VERSION: u8 = 1;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    CreateTopic = 0x01,
    Produce = 0x02,
    Consume = 0x03,
    Metadata = 0x04,
    Read = 0x05,
    CreateQueue = 0x06,
    BindQueue = 0x07,
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
            0x06 => Op::CreateQueue,
            0x07 => Op::BindQueue,
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

impl From<u16> for Status {
    fn from(v: u16) -> Self {
        match v {
            0 => Status::Ok,
            10 => Status::Redirect,
            11 => Status::Empty,
            12 => Status::TopicExists,
            13 => Status::NotFound,
            400 => Status::BadRequest,
            _ => Status::ServerError,
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_encode_decode() {
        let mut buf = BytesMut::new();
        let header = Header {
            magic: MAGIC,
            version: VERSION,
            op: Op::Produce,
            flags: 0,
            stream_id: 123,
            body_len: 456,
        };

        header.encode(&mut buf);
        assert_eq!(buf.len(), Header::LEN);

        let decoded = Header::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.magic, MAGIC);
        assert_eq!(decoded.version, VERSION);
        assert_eq!(decoded.op, Op::Produce);
        assert_eq!(decoded.stream_id, 123);
        assert_eq!(decoded.body_len, 456);
    }

    #[test]
    fn test_header_decode_invalid_magic() {
        let mut buf = BytesMut::new();
        buf.put_u32(0xDEADBEEF);
        buf.put_u8(VERSION);
        buf.put_u8(Op::Produce as u8);
        buf.put_u8(0);
        buf.put_u8(0);
        buf.put_u32(0);
        buf.put_u32(0);

        let result = Header::decode(&mut buf);
        assert!(matches!(result, Err(ProtoError::InvalidMagic(_))));
    }

    #[test]
    fn test_header_decode_invalid_version() {
        let mut buf = BytesMut::new();
        buf.put_u32(MAGIC);
        buf.put_u8(99);
        buf.put_u8(Op::Produce as u8);
        buf.put_u8(0);
        buf.put_u8(0);
        buf.put_u32(0);
        buf.put_u32(0);

        let result = Header::decode(&mut buf);
        assert!(matches!(result, Err(ProtoError::InvalidVersion(_))));
    }

    #[test]
    fn test_header_decode_short_buffer() {
        let mut buf = BytesMut::new();
        buf.put_u32(MAGIC);
        buf.put_u8(VERSION);

        let result = Header::decode(&mut buf).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_op_try_from() {
        assert_eq!(Op::try_from(0x01).unwrap(), Op::CreateTopic);
        assert_eq!(Op::try_from(0x02).unwrap(), Op::Produce);
        assert_eq!(Op::try_from(0x03).unwrap(), Op::Consume);
        assert!(Op::try_from(0xFF).is_err());
    }

    #[test]
    fn test_status_from() {
        assert_eq!(Status::from(0), Status::Ok);
        assert_eq!(Status::from(10), Status::Redirect);
        assert_eq!(Status::from(11), Status::Empty);
        assert_eq!(Status::from(999), Status::ServerError);
    }

    #[test]
    fn test_put_get_str() {
        let mut buf = BytesMut::new();
        put_str(&mut buf, "hello");

        let mut slice = &buf[..];
        let result = get_str(&mut slice).unwrap();
        assert_eq!(result, "hello");
        assert_eq!(slice.len(), 0);
    }

    #[test]
    fn test_put_get_str_empty() {
        let mut buf = BytesMut::new();
        put_str(&mut buf, "");

        let mut slice = &buf[..];
        let result = get_str(&mut slice).unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_get_str_short_buffer() {
        let buf = vec![0, 10];
        let mut slice = &buf[..];
        let result = get_str(&mut slice);
        assert!(result.is_none());
    }

    #[test]
    fn test_put_get_bytes() {
        let mut buf = BytesMut::new();
        let data = vec![1, 2, 3, 4, 5];
        put_bytes(&mut buf, &data);

        let mut slice = &buf[..];
        let result = get_bytes(&mut slice).unwrap();
        assert_eq!(result, data);
        assert_eq!(slice.len(), 0);
    }

    #[test]
    fn test_put_get_bytes_empty() {
        let mut buf = BytesMut::new();
        put_bytes(&mut buf, &[]);

        let mut slice = &buf[..];
        let result = get_bytes(&mut slice).unwrap();
        assert_eq!(result, Vec::<u8>::new());
    }

    #[test]
    fn test_get_bytes_short_buffer() {
        let buf = vec![0, 0, 0, 10];
        let mut slice = &buf[..];
        let result = get_bytes(&mut slice);
        assert!(result.is_none());
    }

    #[test]
    fn test_put_get_u32() {
        let mut buf = BytesMut::new();
        put_u32(&mut buf, 12345);

        let mut slice = &buf[..];
        let result = get_u32(&mut slice).unwrap();
        assert_eq!(result, 12345);
        assert_eq!(slice.len(), 0);
    }

    #[test]
    fn test_get_u32_short_buffer() {
        let buf = vec![0, 0];
        let mut slice = &buf[..];
        let result = get_u32(&mut slice);
        assert!(result.is_none());
    }

    #[test]
    fn test_put_status() {
        let mut buf = BytesMut::new();
        put_status(&mut buf, Status::Ok);
        assert_eq!(buf.len(), 2);
        assert_eq!(u16::from_be_bytes([buf[0], buf[1]]), 0);

        buf.clear();
        put_status(&mut buf, Status::BadRequest);
        assert_eq!(u16::from_be_bytes([buf[0], buf[1]]), 400);
    }

    #[test]
    fn test_multiple_strings() {
        let mut buf = BytesMut::new();
        put_str(&mut buf, "first");
        put_str(&mut buf, "second");
        put_str(&mut buf, "third");

        let mut slice = &buf[..];
        assert_eq!(get_str(&mut slice).unwrap(), "first");
        assert_eq!(get_str(&mut slice).unwrap(), "second");
        assert_eq!(get_str(&mut slice).unwrap(), "third");
        assert_eq!(slice.len(), 0);
    }

    #[test]
    fn test_mixed_tlv() {
        let mut buf = BytesMut::new();
        put_str(&mut buf, "topic");
        put_bytes(&mut buf, &[1, 2, 3]);
        put_u32(&mut buf, 999);

        let mut slice = &buf[..];
        assert_eq!(get_str(&mut slice).unwrap(), "topic");
        assert_eq!(get_bytes(&mut slice).unwrap(), vec![1, 2, 3]);
        assert_eq!(get_u32(&mut slice).unwrap(), 999);
        assert_eq!(slice.len(), 0);
    }
}
