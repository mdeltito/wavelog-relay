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

/// Best-effort ADIF inspector for log lines. Missing fields yield `None`.
#[derive(Debug, Default)]
pub(super) struct AdifSummary {
    callsign: Option<Box<str>>,
    mode: Option<Box<str>>,
    band: Option<Box<str>>,
}

impl AdifSummary {
    pub(super) fn callsign(&self) -> &str {
        self.callsign.as_deref().unwrap_or("?")
    }
    pub(super) fn mode(&self) -> &str {
        self.mode.as_deref().unwrap_or("?")
    }
    pub(super) fn band(&self) -> &str {
        self.band.as_deref().unwrap_or("?")
    }
}

pub(super) fn summarize_adif(adif: &str) -> AdifSummary {
    AdifSummary {
        callsign: adif_field(adif, "CALL").map(Into::into),
        mode: adif_field(adif, "MODE").map(Into::into),
        band: adif_field(adif, "BAND").map(Into::into),
    }
}

/// Locate an ADIF field's value by tag (case-insensitive). Sentinel
/// tags (`<EOR>`, `<EOH>`) have no length and are skipped.
fn adif_field<'a>(adif: &'a str, name: &str) -> Option<&'a str> {
    let mut cursor = 0;
    while let Some(off) = adif[cursor..].find('<') {
        let header_start = cursor + off + 1;
        let close = adif[header_start..].find('>')?;
        let header_end = header_start + close;
        let header = &adif[header_start..header_end];
        let mut parts = header.split(':');
        let tag = parts.next().unwrap_or("");
        let Some(len_str) = parts.next() else {
            // No length present (e.g. <EOR>, <EOH>): skip and continue.
            cursor = header_end + 1;
            continue;
        };
        let Ok(len) = len_str.parse::<usize>() else {
            cursor = header_end + 1;
            continue;
        };
        let value_start = header_end + 1;
        let value_end = value_start.checked_add(len)?;
        if value_end > adif.len() {
            return None;
        }
        if tag.eq_ignore_ascii_case(name) {
            return Some(&adif[value_start..value_end]);
        }
        cursor = value_end;
    }
    None
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

    /// Qt `QString`: big-endian `qint32` length, then UTF-8. `-1` is null (empty).
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

/// `pub(super)` packet builders shared with integration tests.
#[cfg(test)]
pub(super) mod test_packets {
    use super::{MAGIC, MSG_TYPE_LOGGED_ADIF};

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
    fn adif_summary_extracts_known_fields() {
        let adif = "<CALL:5>VK3AB <MODE:3>FT8 <BAND:3>20M <FREQ:8>14.07400 <EOR>";
        let s = summarize_adif(adif);
        assert_eq!(s.callsign(), "VK3AB");
        assert_eq!(s.mode(), "FT8");
        assert_eq!(s.band(), "20M");
    }

    #[test]
    fn adif_summary_returns_placeholder_for_missing_fields() {
        let s = summarize_adif("<EOR>");
        assert_eq!(s.callsign(), "?");
        assert_eq!(s.mode(), "?");
        assert_eq!(s.band(), "?");
    }

    #[test]
    fn adif_summary_is_case_insensitive_on_tag_names() {
        let adif = "<call:5>VK3AB <Mode:3>FT8 <eor>";
        let s = summarize_adif(adif);
        assert_eq!(s.callsign(), "VK3AB");
        assert_eq!(s.mode(), "FT8");
    }

    #[test]
    fn adif_summary_handles_data_type_suffix() {
        // ADIF lets fields carry an optional type suffix: <FREQ:8:N>...
        // The leading length must still apply; the type code is ignored.
        let adif = "<CALL:5:S>VK3AB <FREQ:8:N>14.07400 <EOR>";
        let s = summarize_adif(adif);
        assert_eq!(s.callsign(), "VK3AB");
    }

    #[test]
    fn adif_summary_skips_sentinel_tags_without_aborting() {
        // <EOH> appears before any field; must not stop the scan.
        let adif = "<EOH><CALL:5>VK3AB <EOR>";
        let s = summarize_adif(adif);
        assert_eq!(s.callsign(), "VK3AB");
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
