//! WSJT-X UDP wire-format parser.
//!
//! WSJT-X (and JTDX/MSHV, which speak the same protocol) frames every
//! datagram as `magic` (`0xadbccbda`), `schema_version` (`quint32`),
//! `message_type` (`quint32`), and a type-specific body. Strings use Qt's
//! `QDataStream` `QString` encoding: a signed `qint32` byte-length prefix
//! (big-endian), then UTF-8 bytes; length `-1` is "null" (treated as
//! empty).
//!
//! Only message type 12 (`Logged ADIF`) is decoded — every other type is
//! reported as `Ok(None)` so the caller can ignore it without
//! distinguishing parse errors from "not interesting." Non-WSJT-X
//! traffic (bad magic, truncation, invalid UTF-8) surfaces as `Err`.

use thiserror::Error;

pub(super) const MAGIC: u32 = 0xadbc_cbda;
pub(super) const MSG_TYPE_LOGGED_ADIF: u32 = 12;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WsjtxError {
    #[error("malformed WSJT-X packet: {0}")]
    Parse(Box<str>),
}

/// Parse a WSJT-X UDP datagram. Returns `Ok(Some(adif))` for a
/// `LoggedADIF` (type 12) message, `Ok(None)` for any other valid
/// WSJT-X message we don't care about, and `Err` for packets that
/// don't look like WSJT-X at all (bad magic, truncated, invalid UTF-8).
pub(super) fn parse_logged_adif(bytes: &[u8]) -> Result<Option<Box<str>>, WsjtxError> {
    let mut c = Cursor::new(bytes);
    let magic = c.read_u32()?;
    if magic != MAGIC {
        return Err(WsjtxError::Parse(
            format!("bad magic: 0x{magic:08x}").into(),
        ));
    }
    // schema_version is read for the side effect of validating it
    // exists but we don't gate on a specific version — WSJT-X has only
    // ever appended fields to existing message types, so the prefix
    // we read for type 12 is stable across schemas.
    let _schema = c.read_u32()?;
    let message_type = c.read_u32()?;
    if message_type != MSG_TYPE_LOGGED_ADIF {
        return Ok(None);
    }
    // Type-12 body: `id` (sender program id like "WSJT-X"), `adif_text`.
    let _id = c.read_qstring()?;
    let adif = c.read_qstring()?;
    Ok(Some(adif))
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn read_u32(&mut self) -> Result<u32, WsjtxError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// Read a Qt-encoded `QString`: signed `qint32` byte-length (big-
    /// endian) followed by UTF-8 bytes. A length of `-1` is "null" and
    /// resolves to an empty string.
    fn read_qstring(&mut self) -> Result<Box<str>, WsjtxError> {
        let len = self.read_u32()? as i32;
        if len < 0 {
            return Ok("".into());
        }
        let len = len as usize;
        let bytes = self.take(len)?;
        std::str::from_utf8(bytes)
            .map(Into::into)
            .map_err(|_| WsjtxError::Parse("invalid UTF-8 in QString field".into()))
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], WsjtxError> {
        let end = self.pos.checked_add(n).ok_or_else(|| {
            WsjtxError::Parse(format!("length overflow at offset {}", self.pos).into())
        })?;
        if end > self.buf.len() {
            return Err(WsjtxError::Parse(
                format!(
                    "truncated: need {n} bytes at offset {pos}, have {remaining}",
                    pos = self.pos,
                    remaining = self.buf.len() - self.pos,
                )
                .into(),
            ));
        }
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }
}

/// Test-only packet builders. `pub(super)` so the integration tests in
/// `wsjtx/mod.rs` can reuse the same byte layout the protocol parser
/// validates against.
#[cfg(test)]
pub(super) mod test_packets {
    use super::{MAGIC, MSG_TYPE_LOGGED_ADIF};

    /// Encode a Qt `QString` into a buffer: signed `qint32` big-endian
    /// length, then UTF-8 bytes.
    pub fn push_qstring(out: &mut Vec<u8>, s: &str) {
        let len = s.len() as i32;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(s.as_bytes());
    }

    pub fn encode_packet(message_type: u32, id: &str, body: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&MAGIC.to_be_bytes());
        out.extend_from_slice(&3u32.to_be_bytes()); // schema version 3
        out.extend_from_slice(&message_type.to_be_bytes());
        push_qstring(&mut out, id);
        body(&mut out);
        out
    }

    pub fn encode_logged_adif(id: &str, adif: &str) -> Vec<u8> {
        encode_packet(MSG_TYPE_LOGGED_ADIF, id, |out| push_qstring(out, adif))
    }

    pub fn encode_qso_logged(id: &str) -> Vec<u8> {
        // Type 5 — we don't need to match the full body, just the
        // header + id is enough to assert the parser ignores it.
        encode_packet(5, id, |_| {})
    }
}

#[cfg(test)]
mod tests {
    use super::test_packets::*;
    use super::*;

    #[test]
    fn parse_logged_adif_extracts_string() {
        let pkt = encode_logged_adif("WSJT-X", "<CALL:3>K1B <MODE:3>FT8 <EOR>");
        let adif = parse_logged_adif(&pkt).unwrap().unwrap();
        assert_eq!(&*adif, "<CALL:3>K1B <MODE:3>FT8 <EOR>");
    }

    #[test]
    fn parse_returns_none_for_non_logged_adif_messages() {
        let pkt = encode_qso_logged("WSJT-X");
        assert_eq!(parse_logged_adif(&pkt).unwrap(), None);
    }

    #[test]
    fn parse_rejects_wrong_magic() {
        let mut pkt = encode_logged_adif("WSJT-X", "x");
        pkt[0] = 0; // corrupt magic
        let err = parse_logged_adif(&pkt).unwrap_err();
        assert!(matches!(err, WsjtxError::Parse(_)), "got {err:?}");
    }

    #[test]
    fn parse_rejects_truncated_packet() {
        let pkt = encode_logged_adif("WSJT-X", "hello");
        // Cut off mid-way through the ADIF string.
        let err = parse_logged_adif(&pkt[..pkt.len() - 3]).unwrap_err();
        assert!(matches!(err, WsjtxError::Parse(_)), "got {err:?}");
    }

    #[test]
    fn parse_rejects_invalid_utf8_string() {
        // Build a pathological packet by hand: schema 3, type 12, id
        // "WSJT-X", then an ADIF length of 2 with non-UTF-8 bytes.
        let mut bad = Vec::new();
        bad.extend_from_slice(&MAGIC.to_be_bytes());
        bad.extend_from_slice(&3u32.to_be_bytes());
        bad.extend_from_slice(&MSG_TYPE_LOGGED_ADIF.to_be_bytes());
        push_qstring(&mut bad, "WSJT-X");
        bad.extend_from_slice(&2i32.to_be_bytes()); // len 2 for adif
        bad.push(0xff);
        bad.push(0xfe);
        let err = parse_logged_adif(&bad).unwrap_err();
        assert!(
            matches!(err, WsjtxError::Parse(ref msg) if msg.contains("UTF-8")),
            "got {err:?}",
        );
    }

    #[test]
    fn parse_handles_null_qstring_as_empty() {
        // schema 3, type 12, id len = -1 (null), adif len = 0
        let mut bad = Vec::new();
        bad.extend_from_slice(&MAGIC.to_be_bytes());
        bad.extend_from_slice(&3u32.to_be_bytes());
        bad.extend_from_slice(&MSG_TYPE_LOGGED_ADIF.to_be_bytes());
        bad.extend_from_slice(&(-1i32).to_be_bytes()); // null id
        bad.extend_from_slice(&0i32.to_be_bytes()); // empty adif
        let adif = parse_logged_adif(&bad).unwrap().unwrap();
        assert_eq!(&*adif, "");
    }
}
