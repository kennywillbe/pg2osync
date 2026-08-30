//! The key material behind the `pseudonym` transform.
//!
//! AES-SIV (RFC 5297) is deterministic: the same key, plaintext and associated
//! data always give the same token, which is what lets a join survive
//! pseudonymisation. `docs/decisions.md` says why that is preferred to a keyed
//! hash, and `docs/configuration.md` documents the construction a key holder
//! needs in order to decrypt.

use aes_siv::KeyInit;
use aes_siv::siv::Aes256Siv;
use base64::Engine;
use zeroize::Zeroize;

/// AES-256-SIV takes two 256-bit keys: the CMAC key first, then the CTR key.
const KEY_BYTES: usize = 64;

/// A 64-byte AES-256-SIV key, read from the environment.
///
/// `Debug` is hand-written and `Drop` zeroizes, because the structures holding
/// this are printed on error paths of a binary whose whole promise here is that
/// the key never leaves the process.
#[derive(Clone, PartialEq, Eq)]
pub struct PseudonymKey([u8; KEY_BYTES]);

impl std::fmt::Debug for PseudonymKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PseudonymKey(redacted)")
    }
}

impl Drop for PseudonymKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

/// Why a configured key is not a key. No variant carries the value: these are
/// printed, and what they would print is the secret itself.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum KeyError {
    #[error("must be {expected} hex characters (a {bytes}-byte AES-256-SIV key); got {got}")]
    Length {
        expected: usize,
        bytes: usize,
        got: usize,
    },
    #[error("must be hex; character {at} is not a hex digit")]
    NotHex { at: usize },
}

impl PseudonymKey {
    /// Parse 128 hex characters, in either case, with surrounding whitespace
    /// trimmed — a key pasted into an environment file usually brings a
    /// newline with it.
    pub fn from_hex(text: &str) -> Result<Self, KeyError> {
        let chars: Vec<char> = text.trim().chars().collect();
        if chars.len() != KEY_BYTES * 2 {
            return Err(KeyError::Length {
                expected: KEY_BYTES * 2,
                bytes: KEY_BYTES,
                got: chars.len(),
            });
        }
        let mut key = [0u8; KEY_BYTES];
        // the length was checked above, so there is no remainder to consider
        let (pairs, _) = chars.as_chunks::<2>();
        for (i, [hi_char, lo_char]) in pairs.iter().enumerate() {
            let hi = hi_char.to_digit(16).ok_or(KeyError::NotHex { at: i * 2 })?;
            let lo = lo_char
                .to_digit(16)
                .ok_or(KeyError::NotHex { at: i * 2 + 1 })?;
            key[i] = (hi * 16 + lo) as u8;
        }
        Ok(Self(key))
    }

    /// The token for `plaintext` under `scope`.
    ///
    /// `None` is unreachable — AES-SIV fails only on more than 126 headers and
    /// this passes exactly one — but the caller needs somewhere to fail closed
    /// to rather than publish the plaintext it was asked to hide.
    pub fn token(&self, scope: &str, plaintext: &[u8]) -> Option<String> {
        let mut siv = Aes256Siv::new(&self.0.into());
        let sealed = siv.encrypt([scope.as_bytes()], plaintext).ok()?;
        // base64url unpadded: tokens land in keyword fields, URLs and query
        // strings, where `+`, `/` and `=` all have to be escaped.
        Some(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sealed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\
                       202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f";

    fn key() -> PseudonymKey {
        PseudonymKey::from_hex(KEY).expect("a valid key")
    }

    /// A dependency bump that changes the construction — which half of the key
    /// is the MAC key, how the header is fed in, the base64 alphabet —
    /// silently invalidates every token already in an index. This vector was
    /// produced by the crate itself and then frozen, so such a bump fails here
    /// instead of in production.
    #[test]
    fn known_answer() {
        assert_eq!(
            key().token("public.users.email", b"alice@example.com"),
            Some("b755fBOIourHdXmQbAvHon5gj2lfvpmWx9gqaznhJdYd".to_string())
        );
    }

    #[test]
    fn deterministic() {
        assert_eq!(
            key().token("public.users.email", b"alice@example.com"),
            key().token("public.users.email", b"alice@example.com")
        );
    }

    #[test]
    fn scope_separates_contexts() {
        assert_ne!(
            key().token("public.users.email", b"alice@example.com"),
            key().token("public.staff.email", b"alice@example.com")
        );
    }

    #[test]
    fn different_inputs_differ() {
        assert_ne!(
            key().token("public.users.email", b"alice@example.com"),
            key().token("public.users.email", b"bob@example.com")
        );
    }

    #[test]
    fn length_is_the_iv_plus_the_plaintext() {
        let token = key().token("s", b"0123456789").expect("a token");
        let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(token)
            .expect("base64url");
        assert_eq!(raw.len(), 16 + 10);
    }

    #[test]
    fn accepts_upper_case_and_surrounding_whitespace() {
        let padded = format!("  {}\n", KEY.to_uppercase());
        assert_eq!(PseudonymKey::from_hex(&padded), Ok(key()));
    }

    #[test]
    fn refuses_a_short_key() {
        assert_eq!(
            PseudonymKey::from_hex(&KEY[..64]),
            Err(KeyError::Length {
                expected: 128,
                bytes: 64,
                got: 64
            })
        );
    }

    #[test]
    fn refuses_a_long_key() {
        assert_eq!(
            PseudonymKey::from_hex(&format!("{KEY}ab")),
            Err(KeyError::Length {
                expected: 128,
                bytes: 64,
                got: 130
            })
        );
    }

    #[test]
    fn refuses_a_non_hex_digit() {
        assert_eq!(
            PseudonymKey::from_hex(&format!("g{}", &KEY[1..])),
            Err(KeyError::NotHex { at: 0 })
        );
    }

    #[test]
    fn no_message_echoes_the_value() {
        for text in ["deadbeef", &format!("{KEY}ab"), &format!("{}z", &KEY[1..])] {
            let message = PseudonymKey::from_hex(text)
                .expect_err("not a key")
                .to_string();
            assert!(!message.contains(text), "{message} echoes its input");
        }
    }

    #[test]
    fn debug_hides_the_key() {
        let shown = format!("{:?}", key());
        assert_eq!(shown, "PseudonymKey(redacted)");
    }
}
