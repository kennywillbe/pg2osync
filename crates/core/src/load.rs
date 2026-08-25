//! How far an initial load got, so a restart can carry on instead of starting
//! over.
//!
//! State lives in the target, like the checkpoint, because that is the only
//! place both halves of a restart can see. It is written *behind* a durability
//! barrier, so a crash can lose forward progress but can never claim a range
//! that was not written.

use serde_json::{Value, json};

/// One table's load progress.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LoadProgress {
    /// The key boundaries the ranges were cut at, stored rather than recomputed:
    /// they come from a random sample, so a second run would cut the table
    /// elsewhere and `done` would name a different span of rows.
    pub boundaries: Vec<String>,
    /// How many leading ranges are durably written.
    pub done: usize,
    /// Set once every range is written, so a restart skips the table without
    /// having to compare counts.
    pub finished: bool,
}

impl LoadProgress {
    pub fn to_doc(&self) -> Value {
        json!({
            "boundaries": self.boundaries,
            "done": self.done,
            "finished": self.finished,
        })
    }

    /// Parse a stored document. A document we cannot read is treated as absent
    /// by the caller, which costs a reload and never a gap.
    pub fn from_doc(src: &Value) -> Option<Self> {
        Some(Self {
            boundaries: src["boundaries"]
                .as_array()?
                .iter()
                .map(|v| v.as_str().map(str::to_string))
                .collect::<Option<Vec<_>>>()?,
            done: src["done"].as_u64()? as usize,
            finished: src["finished"].as_bool()?,
        })
    }
}

/// Where one table's progress is stored, as a document id.
///
/// Two pipelines may share a target, so the stream is part of the name for the
/// same reason it is part of the checkpoint's.
pub fn load_progress_key(stream: &crate::checkpoint::StreamId, qualified_table: &str) -> String {
    let tame = |s: &str| -> String {
        s.chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    };
    format!(
        "load-{}-{}-{}",
        tame(&stream.source),
        tame(&stream.stream),
        tame(qualified_table)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::StreamId;

    #[test]
    fn progress_round_trips() {
        let p = LoadProgress {
            boundaries: vec!["'a'".into(), "'b'".into()],
            done: 1,
            finished: false,
        };
        assert_eq!(LoadProgress::from_doc(&p.to_doc()), Some(p));
    }

    #[test]
    fn an_unreadable_document_is_absent_rather_than_wrong() {
        // an older or corrupt layout must cost a reload, never a skipped range
        assert_eq!(LoadProgress::from_doc(&json!({"done": 3})), None);
    }

    #[test]
    fn two_streams_do_not_share_a_progress_document() {
        let stream = |s: &str| StreamId {
            source: "postgres".into(),
            stream: s.into(),
            publication: "pub".into(),
        };
        assert_ne!(
            load_progress_key(&stream("one"), "public.users"),
            load_progress_key(&stream("two"), "public.users")
        );
    }

    #[test]
    fn a_key_is_safe_as_a_document_id() {
        let key = load_progress_key(
            &StreamId {
                source: "postgres".into(),
                stream: "s".into(),
                publication: "p".into(),
            },
            "we\"ird.tbl",
        );
        assert_eq!(key, "load-postgres-s-we_ird_tbl");
    }
}
