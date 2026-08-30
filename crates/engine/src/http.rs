//! The little bit of HTTP the two built-in endpoints need.
//!
//! Both serve a handful of GETs on an operator-chosen port, which is far less
//! than a web framework is for. Keeping the parsing here means the metrics and
//! read-your-writes endpoints agree on what a request is and, more to the
//! point, on what counts as authorised.

/// The target of a `GET`, or `None` for anything else.
pub fn request_target(request: &str) -> Option<&str> {
    let first_line = request.lines().next()?;
    let mut parts = first_line.split_whitespace();
    let method = parts.next()?;
    if method != "GET" {
        return None;
    }
    parts.next()
}

/// Split a request target into its path and its raw query string.
pub fn split_target(target: &str) -> (&str, &str) {
    target.split_once('?').unwrap_or((target, ""))
}

/// The value of one request header, matched case-insensitively by name.
///
/// Not used for the token: that comparison must not exit at the first wrong
/// byte, which is what `authorized` below is for.
pub fn header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    request.lines().find_map(|line| {
        let (header, value) = line.split_once(':')?;
        header.eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

/// Whether the request carries `Authorization: Bearer <expected>`.
pub fn authorized(request: &str, expected: &str) -> bool {
    request.lines().any(|line| {
        let Some((name, value)) = line.split_once(':') else {
            return false;
        };
        name.eq_ignore_ascii_case("authorization")
            && value
                .trim()
                .strip_prefix("Bearer ")
                .map(str::trim)
                .is_some_and(|token| constant_time_eq(token.as_bytes(), expected.as_bytes()))
    })
}

/// Compare without an early exit.
///
/// A comparison that stops at the first wrong byte takes measurably longer for
/// a token that shares a longer prefix, which is enough to recover the token
/// one byte at a time over many requests. The length is not hidden — that is
/// the standard trade, and a token's length is not the secret.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Whether a bind address only accepts connections from this host.
///
/// The question every "is this exposed?" warning actually asks. Matching on the
/// string "127.0.0.1" answers it wrongly for `::1` and for `localhost`, and
/// wrongly in the direction that stays quiet.
pub fn is_loopback(bind: &str) -> bool {
    let host = bind.rsplit_once(':').map(|(h, _)| h).unwrap_or(bind);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.parse::<std::net::IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(host == "localhost")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with(header: &str) -> String {
        format!("GET /metrics HTTP/1.1\r\nHost: h\r\n{header}\r\n\r\n")
    }

    #[test]
    fn only_get_is_answered() {
        assert_eq!(request_target("GET /metrics HTTP/1.1"), Some("/metrics"));
        assert_eq!(request_target("POST /metrics HTTP/1.1"), None);
        assert_eq!(request_target(""), None);
    }

    #[test]
    fn the_query_string_is_not_part_of_the_path() {
        assert_eq!(split_target("/synced?timeout=5"), ("/synced", "timeout=5"));
        assert_eq!(split_target("/metrics"), ("/metrics", ""));
    }

    #[test]
    fn a_header_is_read_by_name_whatever_its_case() {
        let request = with("TraceParent: 00-4bf9-00f0-01");
        assert_eq!(header(&request, "traceparent"), Some("00-4bf9-00f0-01"));
        assert_eq!(header(&request, "Host"), Some("h"));
        assert_eq!(header(&request, "x-request-id"), None);
    }

    #[test]
    fn the_token_must_match_exactly() {
        assert!(authorized(&with("Authorization: Bearer secret"), "secret"));
        assert!(
            authorized(&with("authorization: Bearer secret"), "secret"),
            "header names are case-insensitive"
        );
        assert!(!authorized(&with("Authorization: Bearer other"), "secret"));
        assert!(
            !authorized(&with("Authorization: Bearer secretx"), "secret"),
            "a longer token that starts with the right bytes is still wrong"
        );
        assert!(!authorized(&with("X-Token: secret"), "secret"));
        assert!(!authorized(&with("Host: h"), "secret"));
    }

    #[test]
    fn comparison_covers_every_byte() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn reachability_is_about_the_address_not_its_spelling() {
        assert!(is_loopback("127.0.0.1:9100"));
        assert!(is_loopback("localhost:9100"));
        assert!(is_loopback("[::1]:9100"));
        assert!(!is_loopback("0.0.0.0:9100"));
        assert!(!is_loopback("10.1.2.3:9100"));
    }
}
