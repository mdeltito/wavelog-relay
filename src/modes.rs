//! Click-to-tune mode parsing and hamlib mapping.
//!
//! Wavelog sends the mode as a lowercase path segment in the CAT URL
//! (`http://<listener>/<freq_hz>/<mode>`). The accepted set mirrors the
//! `validModes` whitelist in Wavelog's `assets/js/cat.js`
//! (`performRadioTuning`). Two of those modes — `pkt` and `dig` — are
//! ambiguous between USB- and LSB-side packet variants; their concrete
//! hamlib target is resolved through [`ModeOverrides`], which is loaded
//! from the optional `[mode_overrides]` TOML table.

use std::str::FromStr;

use serde::Deserialize;
use thiserror::Error;

/// A click-to-tune mode parsed from a Wavelog CAT URL path segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Mode {
    Lsb,
    Usb,
    Cw,
    Fm,
    Am,
    Rtty,
    /// Generic packet. Resolved via [`ModeOverrides::pkt`].
    Pkt,
    /// Generic digital. Resolved via [`ModeOverrides::dig`].
    Dig,
    PktLsb,
    PktUsb,
    PktFm,
}

#[derive(Debug, Error)]
pub enum ModeParseError {
    #[error("unrecognized wavelog mode `{0}`")]
    Unknown(Box<str>),
}

impl FromStr for Mode {
    type Err = ModeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "lsb" => Self::Lsb,
            "usb" => Self::Usb,
            "cw" => Self::Cw,
            "fm" => Self::Fm,
            "am" => Self::Am,
            "rtty" => Self::Rtty,
            "pkt" => Self::Pkt,
            "dig" => Self::Dig,
            "pktlsb" => Self::PktLsb,
            "pktusb" => Self::PktUsb,
            "pktfm" => Self::PktFm,
            other => return Err(ModeParseError::Unknown(other.into())),
        })
    }
}

impl Mode {
    /// Resolve to a concrete [`HamlibMode`], substituting `Pkt` and
    /// `Dig` with the user's override targets.
    #[must_use]
    pub fn resolve(self, overrides: &ModeOverrides) -> HamlibMode {
        match self {
            Self::Lsb => HamlibMode::Lsb,
            Self::Usb => HamlibMode::Usb,
            Self::Cw => HamlibMode::Cw,
            Self::Fm => HamlibMode::Fm,
            Self::Am => HamlibMode::Am,
            Self::Rtty => HamlibMode::Rtty,
            Self::PktLsb => HamlibMode::PktLsb,
            Self::PktUsb => HamlibMode::PktUsb,
            Self::PktFm => HamlibMode::PktFm,
            Self::Pkt => overrides.pkt,
            Self::Dig => overrides.dig,
        }
    }
}

/// A hamlib mode string suitable for the rigctld `M <mode> <passband>`
/// command and for the `mode` field in Wavelog's `/api/radio` JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HamlibMode {
    Lsb,
    Usb,
    Cw,
    Fm,
    Am,
    Rtty,
    PktLsb,
    PktUsb,
    PktFm,
}

impl HamlibMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lsb => "LSB",
            Self::Usb => "USB",
            Self::Cw => "CW",
            Self::Fm => "FM",
            Self::Am => "AM",
            Self::Rtty => "RTTY",
            Self::PktLsb => "PKTLSB",
            Self::PktUsb => "PKTUSB",
            Self::PktFm => "PKTFM",
        }
    }
}

/// Resolution targets for the ambiguous `pkt` and `dig` Wavelog modes.
/// Both default to `PKTUSB`; override via the `[mode_overrides]` TOML table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct ModeOverrides {
    pub pkt: HamlibMode,
    pub dig: HamlibMode,
}

impl Default for ModeOverrides {
    fn default() -> Self {
        Self {
            pkt: HamlibMode::PktUsb,
            dig: HamlibMode::PktUsb,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_wavelog_mode() {
        let cases = [
            ("lsb", Mode::Lsb),
            ("usb", Mode::Usb),
            ("cw", Mode::Cw),
            ("fm", Mode::Fm),
            ("am", Mode::Am),
            ("rtty", Mode::Rtty),
            ("pkt", Mode::Pkt),
            ("dig", Mode::Dig),
            ("pktlsb", Mode::PktLsb),
            ("pktusb", Mode::PktUsb),
            ("pktfm", Mode::PktFm),
        ];
        for (s, expected) in cases {
            assert_eq!(s.parse::<Mode>().unwrap(), expected, "parsing `{s}`");
        }
    }

    #[test]
    fn rejects_unknown_wavelog_mode() {
        let err = "xyz".parse::<Mode>().unwrap_err();
        let ModeParseError::Unknown(payload) = err;
        assert_eq!(&*payload, "xyz");
    }

    #[test]
    fn rejects_uppercase_wavelog_mode() {
        assert!("USB".parse::<Mode>().is_err());
    }

    #[test]
    fn rejects_empty_mode() {
        assert!("".parse::<Mode>().is_err());
    }

    #[test]
    fn resolve_concrete_modes_ignores_overrides() {
        let overrides = ModeOverrides {
            pkt: HamlibMode::PktLsb,
            dig: HamlibMode::PktFm,
        };
        assert_eq!(Mode::Usb.resolve(&overrides), HamlibMode::Usb);
        assert_eq!(Mode::Cw.resolve(&overrides), HamlibMode::Cw);
        assert_eq!(Mode::PktUsb.resolve(&overrides), HamlibMode::PktUsb);
        assert_eq!(Mode::PktLsb.resolve(&overrides), HamlibMode::PktLsb);
    }

    #[test]
    fn resolve_pkt_and_dig_default_to_pktusb() {
        let overrides = ModeOverrides::default();
        assert_eq!(Mode::Pkt.resolve(&overrides), HamlibMode::PktUsb);
        assert_eq!(Mode::Dig.resolve(&overrides), HamlibMode::PktUsb);
    }

    #[test]
    fn resolve_pkt_and_dig_use_overrides() {
        let overrides = ModeOverrides {
            pkt: HamlibMode::PktLsb,
            dig: HamlibMode::PktFm,
        };
        assert_eq!(Mode::Pkt.resolve(&overrides), HamlibMode::PktLsb);
        assert_eq!(Mode::Dig.resolve(&overrides), HamlibMode::PktFm);
    }

    #[test]
    fn hamlib_as_str_covers_every_variant() {
        let cases = [
            (HamlibMode::Lsb, "LSB"),
            (HamlibMode::Usb, "USB"),
            (HamlibMode::Cw, "CW"),
            (HamlibMode::Fm, "FM"),
            (HamlibMode::Am, "AM"),
            (HamlibMode::Rtty, "RTTY"),
            (HamlibMode::PktLsb, "PKTLSB"),
            (HamlibMode::PktUsb, "PKTUSB"),
            (HamlibMode::PktFm, "PKTFM"),
        ];
        for (mode, expected) in cases {
            assert_eq!(mode.as_str(), expected);
        }
    }

    #[test]
    fn mode_overrides_toml_empty_yields_defaults() {
        let parsed: ModeOverrides = toml::from_str("").unwrap();
        assert_eq!(parsed, ModeOverrides::default());
    }

    #[test]
    fn mode_overrides_toml_partial_keeps_missing_default() {
        let parsed: ModeOverrides = toml::from_str(r#"dig = "PKTLSB""#).unwrap();
        assert_eq!(parsed.pkt, HamlibMode::PktUsb);
        assert_eq!(parsed.dig, HamlibMode::PktLsb);
    }

    #[test]
    fn mode_overrides_toml_full_overrides_both() {
        let parsed: ModeOverrides = toml::from_str(
            r#"
                pkt = "PKTFM"
                dig = "USB"
            "#,
        )
        .unwrap();
        assert_eq!(parsed.pkt, HamlibMode::PktFm);
        assert_eq!(parsed.dig, HamlibMode::Usb);
    }

    #[test]
    fn mode_overrides_toml_rejects_unknown_value() {
        let result: Result<ModeOverrides, _> = toml::from_str(r#"dig = "FOO""#);
        assert!(result.is_err());
    }

    #[test]
    fn mode_overrides_toml_rejects_lowercase_value() {
        let result: Result<ModeOverrides, _> = toml::from_str(r#"pkt = "pktusb""#);
        assert!(result.is_err());
    }
}
