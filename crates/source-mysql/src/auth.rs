//! MySQL authentication plugins.
//!
//! Pure functions only: the scrambles are easy to get subtly wrong and easy to
//! test, so they live apart from the connection state machine that uses them.

use anyhow::{Context as _, Result, bail};
use sha1::Sha1;
use sha2::{Digest, Sha256};

/// The OAEP hash has to come from the digest generation `rsa` was built
/// against, which is older than the one the rest of this crate uses.
use sha1_oaep::Sha1 as Sha1Oaep;

/// `mysql_native_password`: SHA1(password) XOR SHA1(nonce || SHA1(SHA1(password)))
pub fn native_password(password: &[u8], nonce: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let stage1 = Sha1::digest(password);
    let stage2 = Sha1::digest(stage1);
    let stage3 = Sha1::new()
        .chain_update(nonce)
        .chain_update(stage2)
        .finalize();
    stage1
        .iter()
        .zip(stage3.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}

/// `caching_sha2_password` fast path:
/// SHA256(password) XOR SHA256(SHA256(SHA256(password)) || nonce)
///
/// Succeeds only while the server still has this account in its cache; a cold
/// server answers with a request for full authentication instead.
pub fn caching_sha2_fast(password: &[u8], nonce: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let stage1 = Sha256::digest(password);
    let stage2 = Sha256::digest(stage1);
    let stage3 = Sha256::new()
        .chain_update(stage2)
        .chain_update(nonce)
        .finalize();
    stage1
        .iter()
        .zip(stage3.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}

/// The password as `caching_sha2_password` full authentication sends it over an
/// encrypted connection: cleartext, NUL-terminated.
///
/// Only ever correct on a TLS connection — the caller must refuse to use this
/// on a plaintext socket.
pub fn cleartext_password(password: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(password.len() + 1);
    out.extend_from_slice(password);
    out.push(0);
    out
}

/// Full authentication without TLS: the NUL-terminated password is XORed with
/// the nonce and then encrypted with the server's public key, so it is never
/// recoverable from the wire even though the socket is in the clear.
pub fn rsa_encrypted_password(
    password: &[u8],
    nonce: &[u8],
    public_key_pem: &[u8],
) -> Result<Vec<u8>> {
    use rsa::RsaPublicKey;
    use rsa::pkcs8::DecodePublicKey;

    if nonce.is_empty() {
        bail!("server sent an empty nonce for RSA authentication");
    }
    let pem = std::str::from_utf8(public_key_pem)
        .context("server public key is not valid UTF-8")?
        .trim();
    let key = RsaPublicKey::from_public_key_pem(pem)
        .context("cannot parse the server's caching_sha2_password public key")?;

    let obfuscated: Vec<u8> = cleartext_password(password)
        .iter()
        .enumerate()
        .map(|(i, byte)| byte ^ nonce[i % nonce.len()])
        .collect();

    // MySQL encrypts with OAEP/SHA-1, which is what the server decrypts with
    let padding = rsa::Oaep::new::<Sha1Oaep>();
    key.encrypt(&mut rand_oaep::thread_rng(), padding, &obfuscated)
        .context("cannot encrypt the password with the server's public key")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_password_matches_the_reference_construction() {
        let password = b"secret";
        let nonce = [7u8; 20];
        let token = native_password(password, &nonce);

        let stage1 = Sha1::digest(password);
        let stage2 = Sha1::digest(stage1);
        let stage3 = Sha1::new()
            .chain_update(nonce)
            .chain_update(stage2)
            .finalize();
        let expected: Vec<u8> = stage1
            .iter()
            .zip(stage3.iter())
            .map(|(a, b)| a ^ b)
            .collect();
        assert_eq!(token, expected);
        assert_eq!(token.len(), 20);
    }

    #[test]
    fn caching_sha2_hashes_the_nonce_last() {
        // the order of the concatenation is the easiest thing to get wrong,
        // and it fails as an authentication error with no other clue
        let password = b"secret";
        let nonce = [3u8; 20];
        let token = caching_sha2_fast(password, &nonce);

        let stage1 = Sha256::digest(password);
        let stage2 = Sha256::digest(stage1);
        let stage3 = Sha256::new()
            .chain_update(stage2)
            .chain_update(nonce)
            .finalize();
        let expected: Vec<u8> = stage1
            .iter()
            .zip(stage3.iter())
            .map(|(a, b)| a ^ b)
            .collect();
        assert_eq!(token, expected);
        assert_eq!(token.len(), 32);
    }

    #[test]
    fn an_empty_password_sends_an_empty_token() {
        assert!(native_password(b"", &[1; 20]).is_empty());
        assert!(caching_sha2_fast(b"", &[1; 20]).is_empty());
    }

    #[test]
    fn cleartext_is_nul_terminated() {
        assert_eq!(cleartext_password(b"pw"), b"pw\0");
        assert_eq!(cleartext_password(b""), b"\0");
    }

    #[test]
    fn rsa_path_round_trips_through_the_matching_private_key() {
        use rsa::pkcs8::EncodePublicKey;
        use rsa::{Oaep, RsaPrivateKey, RsaPublicKey};

        let mut rng = rand_oaep::thread_rng();
        let private = RsaPrivateKey::new(&mut rng, 2048).expect("key generated");
        let public = RsaPublicKey::from(&private);
        let pem = public
            .to_public_key_pem(rsa::pkcs8::LineEnding::LF)
            .expect("pem encoded");

        let password = b"hunter2";
        let nonce: Vec<u8> = (0..20u8).collect();
        let ciphertext =
            rsa_encrypted_password(password, &nonce, pem.as_bytes()).expect("encrypted");

        let decrypted = private
            .decrypt(Oaep::new::<Sha1Oaep>(), &ciphertext)
            .expect("decrypted");
        let recovered: Vec<u8> = decrypted
            .iter()
            .enumerate()
            .map(|(i, b)| b ^ nonce[i % nonce.len()])
            .collect();
        assert_eq!(recovered, cleartext_password(password));
    }

    #[test]
    fn a_malformed_public_key_is_an_error_not_a_panic() {
        assert!(rsa_encrypted_password(b"pw", &[1, 2, 3], b"not a pem").is_err());
    }
}
