//! How far an initial load got, so a restart can carry on instead of starting
//! over.
//!
//! State lives in the target, like the checkpoint, because that is the only
//! place both halves of a restart can see. It is written *behind* a durability
//! barrier, so a crash can lose forward progress but can never claim a range
//! that was not written.

use serde_json::{Value, json};

/// One table's load progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadProgress {
    pub cursor: LoadCursor,
    /// Set once the whole table is written, so a restart skips it without
    /// having to compare counts.
    pub finished: bool,
}

/// How a loader says where it got to, which follows how it cuts the table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadCursor {
    /// Boundaries cut in advance plus how many leading ranges are durably
    /// written. PostgreSQL reads a heap whose order says nothing about the key,
    /// so the cuts are sampled up front; they are stored rather than recomputed
    /// because a second sample would cut the table elsewhere and `done` would
    /// then name a different span of rows.
    Ranges {
        boundaries: Vec<String>,
        done: usize,
    },
    /// The last key durably written; the next chunk starts after it. InnoDB
    /// stores rows in key order, so chunks are discovered as they are read and
    /// there is nothing to cut in advance — which makes the resume point exact
    /// rather than dependent on a sample being reproducible.
    After(Vec<String>),
}

impl LoadProgress {
    pub fn to_doc(&self) -> Value {
        let mut doc = match &self.cursor {
            LoadCursor::Ranges { boundaries, done } => json!({
                "boundaries": boundaries,
                "done": done,
            }),
            LoadCursor::After(key) => json!({ "after": key }),
        };
        doc["finished"] = Value::Bool(self.finished);
        doc
    }

    /// Parse a stored document. A document we cannot read is treated as absent
    /// by the caller, which costs a reload and never a gap.
    pub fn from_doc(src: &Value) -> Option<Self> {
        let finished = src["finished"].as_bool()?;
        // `after` distinguishes the two shapes, so documents written before
        // there was more than one still read back correctly
        let cursor = match src.get("after") {
            Some(after) => LoadCursor::After(strings(after)?),
            None => LoadCursor::Ranges {
                boundaries: strings(&src["boundaries"])?,
                done: src["done"].as_u64()? as usize,
            },
        };
        Some(Self { cursor, finished })
    }
}

fn strings(src: &Value) -> Option<Vec<String>> {
    src.as_array()?
        .iter()
        .map(|v| v.as_str().map(str::to_string))
        .collect()
}

/// True when a previous load left a table unfinished, so this run has to carry
/// it on rather than trust the checkpoint and skip it.
///
/// A checkpoint says where streaming got to, which with a load recording its
/// own progress is a separate fact; trusting it alone is what silently skips a
/// load.
pub async fn unfinished<'a>(
    sink: &dyn crate::sink::Sink,
    stream: &crate::checkpoint::StreamId,
    tables: impl IntoIterator<Item = &'a str>,
) -> Result<bool, crate::error::CoreError> {
    for table in tables {
        let stored = sink
            .read_state(&load_progress_key(stream, table))
            .await?
            .as_ref()
            .and_then(LoadProgress::from_doc);
        if stored.is_some_and(|p| !p.finished) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// What one load run covers.
///
/// Shared by both sources' loaders so they cannot drift on what a scope means:
/// the initial load reads everything and remembers where it got to, an on-demand
/// re-snapshot reads one table and remembers nothing.
#[derive(Debug, Clone, Default)]
pub struct LoadScope {
    /// Only this qualified table, or every configured one.
    pub only: Option<String>,
    /// An extra SQL predicate ANDed into every chunk's read, from
    /// `resnapshot --where`. Verbatim SQL: the operator typed it for this one
    /// run and it is theirs to get right.
    pub filter: Option<String>,
    /// The configured row filter of each table, by qualified name. Carried as
    /// the parsed predicate rather than as rendered SQL because each source
    /// spells identifiers and strings its own way, and the loader building
    /// the statement is the only place that knows which.
    pub table_filters: std::collections::HashMap<String, crate::filter::Filter>,
    /// Whether progress is recorded, so an interruption resumes instead of
    /// starting over.
    ///
    /// A re-snapshot records none. Its bookkeeping would live under the key the
    /// initial load uses, and leaving it behind would be read as an unfinished
    /// initial load at the next pipeline start — the silent skip that
    /// [`unfinished`] exists to prevent. Restarting a re-snapshot is cheap;
    /// corrupting the load's own state is not.
    pub resumable: bool,
}

impl LoadScope {
    /// Every configured table, resumable.
    pub fn initial_load() -> Self {
        Self {
            only: None,
            filter: None,
            table_filters: Default::default(),
            resumable: true,
        }
    }

    /// One table, optionally narrowed, recording nothing.
    pub fn resnapshot(qualified_table: &str, filter: Option<String>) -> Self {
        Self {
            only: Some(qualified_table.to_string()),
            filter,
            table_filters: Default::default(),
            resumable: false,
        }
    }

    /// Builder form, so neither constructor grows an argument every caller
    /// would have to pass.
    pub fn with_table_filters(
        mut self,
        filters: std::collections::HashMap<String, crate::filter::Filter>,
    ) -> Self {
        self.table_filters = filters;
        self
    }

    pub fn table_filter(&self, qualified_table: &str) -> Option<&crate::filter::Filter> {
        self.table_filters.get(qualified_table)
    }

    /// Whether this run should read `qualified_table` at all.
    pub fn covers(&self, qualified_table: &str) -> bool {
        self.only
            .as_deref()
            .is_none_or(|only| only == qualified_table)
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

/// How a load's summary line names the operator's ceiling on its rate.
///
/// Shared by both sources so the number an operator configured is spelled the
/// same wherever the load reports itself; an unexpectedly slow load is then
/// explained on the line that shows it, not in another one further up.
pub fn rate_cap_note(max_rows_per_sec: Option<u32>) -> String {
    max_rows_per_sec
        .map(|n| format!(", capped at {n} rows/s"))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::StreamId;

    #[test]
    fn progress_round_trips() {
        for cursor in [
            LoadCursor::Ranges {
                boundaries: vec!["'a'".into(), "'b'".into()],
                done: 1,
            },
            LoadCursor::After(vec!["42".into(), "'acme'".into()]),
        ] {
            let p = LoadProgress {
                cursor,
                finished: false,
            };
            assert_eq!(LoadProgress::from_doc(&p.to_doc()), Some(p));
        }
    }

    #[test]
    fn a_document_written_before_there_were_two_shapes_still_reads() {
        let stored = json!({"boundaries": ["'a'"], "done": 0, "finished": false});
        assert!(matches!(
            LoadProgress::from_doc(&stored).map(|p| p.cursor),
            Some(LoadCursor::Ranges { done: 0, .. })
        ));
    }

    #[test]
    fn an_unreadable_document_is_absent_rather_than_wrong() {
        // an older or corrupt layout must cost a reload, never a skipped range
        assert_eq!(LoadProgress::from_doc(&json!({"done": 3})), None);
    }

    #[test]
    fn a_scope_selects_one_table_or_all_of_them() {
        let all = LoadScope::initial_load();
        assert!(all.covers("public.users") && all.covers("public.orders"));
        assert!(
            all.resumable,
            "an initial load has somewhere to resume from"
        );

        let one = LoadScope::resnapshot("public.users", Some("tenant = 1".into()));
        assert!(one.covers("public.users"));
        assert!(!one.covers("public.orders"));
        assert!(
            !one.resumable,
            "its progress would be read as an unfinished initial load"
        );
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
