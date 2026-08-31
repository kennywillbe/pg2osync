//! A conformance suite every `Sink` implementation can run against a live
//! instance of its target.
//!
//! The trait's doc comments state the contract; until this existed, only the
//! e2e suites proved it, one target at a time, and each new sink re-discovered
//! the same rules the hard way. What is asserted here is the part of the
//! contract that is the same everywhere:
//!
//! - a replay of an acknowledged batch overwrites rather than duplicates
//! - a truncate at a position clears what came before it and keeps what came
//!   after
//! - `get_documents` answers in request order, with a hole where a document is
//!   not there
//! - a batch holding one document the target refuses still writes the others,
//!   and reports the refusal rather than raising it
//! - a checkpoint that was written reads back, and one belonging to another
//!   stream does not
//!
//! Behind the `testkit` feature so nothing of it reaches a release binary.
//!
//! Two things are the caller's, because only the caller knows them: the
//! documents its target accepts, and a document its target refuses. A target
//! with no document it can refuse — a schema-less one — leaves the second out,
//! and the partial-batch check is skipped rather than faked. Skipping is
//! reported, so an opt-out is a line in the output rather than a silence.

use crate::checkpoint::{Checkpoint, StreamId};
use crate::error::CoreError;
use crate::lsn::Lsn;
use crate::sink::{DocumentOp, LsnOp, Sink};
use serde_json::Value;

/// The suite, bound to one index of one target.
pub struct SinkTestHarness {
    index: String,
    document: Box<dyn Fn(&str) -> Value + Send + Sync>,
    unacceptable: Option<Value>,
}

/// What ran, so a caller can print it and a reader can see what was skipped.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Conformance {
    pub passed: Vec<String>,
    pub skipped: Vec<String>,
}

impl SinkTestHarness {
    /// `document` builds the document for an id; it must be one the target
    /// accepts, and the harness compares what it reads back against it.
    pub fn new(index: &str, document: impl Fn(&str) -> Value + Send + Sync + 'static) -> Self {
        Self {
            index: index.to_string(),
            document: Box::new(document),
            unacceptable: None,
        }
    }

    /// A document this target will refuse permanently — a value of the wrong
    /// type for a declared field, a column that is not there.
    ///
    /// Without one, the partial-batch behaviour cannot be provoked and is
    /// reported as skipped.
    pub fn with_unacceptable_document(mut self, doc: Value) -> Self {
        self.unacceptable = Some(doc);
        self
    }

    /// Run every check, in order, stopping at the first failure.
    ///
    /// The index must already be ready — the caller has run `ensure_ready`,
    /// because only it knows the mapping or DDL that goes with the documents it
    /// builds.
    pub async fn run(&self, sink: &dyn Sink) -> Result<Conformance, CoreError> {
        let mut report = Conformance::default();
        self.read_back(sink).await.map_err(named("read back"))?;
        report.passed.push("read back".into());

        self.idempotent_replay(sink)
            .await
            .map_err(named("idempotent replay"))?;
        report.passed.push("idempotent replay".into());

        if sink.truncates_at_a_position() {
            self.versioned_truncate(sink)
                .await
                .map_err(named("versioned truncate"))?;
            report.passed.push("versioned truncate".into());
        } else {
            // A target that records no position for the documents it holds has
            // nothing for a truncate to compare against.
            report.skipped.push("versioned truncate".into());
        }

        match &self.unacceptable {
            Some(doc) => {
                self.partial_batch(sink, doc.clone())
                    .await
                    .map_err(named("partial batch"))?;
                report.passed.push("partial batch".into());
            }
            None => report.skipped.push("partial batch".into()),
        }

        self.checkpoint_durability(sink)
            .await
            .map_err(named("checkpoint durability"))?;
        report.passed.push("checkpoint durability".into());
        Ok(report)
    }

    /// What `get_documents` owes the engine completing a TOAST marker: the
    /// answers in request order, `None` where there is no document, every
    /// field of the document that was written, and a form the target accepts
    /// back — because completing a TOAST marker means writing the read-back
    /// document again.
    ///
    /// Deliberately not "the same JSON that went in". A target renders what it
    /// stored in its own vocabulary — a `vector` column comes back as pgvector's
    /// text form rather than the array that was written — and demanding the
    /// original spelling would fail a target that loses nothing.
    async fn read_back(&self, sink: &dyn Sink) -> Result<(), CoreError> {
        self.clear(sink).await?;
        let ids = ["kit-1", "kit-2"];
        sink.write(self.batch(&ids, 100)).await?;
        sink.refresh(std::slice::from_ref(&self.index)).await?;
        let asked = vec![
            ("kit-2".to_string(), None),
            ("kit-absent".to_string(), None),
            ("kit-1".to_string(), None),
        ];
        let found = sink.get_documents(&self.index, &asked).await?;
        if found.len() != asked.len() {
            return fail(format!(
                "asked for {} documents and got {} answers",
                asked.len(),
                found.len()
            ));
        }
        if found[1].is_some() {
            return fail("a document that was never written came back");
        }
        for (nth, id) in [(0, "kit-2"), (2, "kit-1")] {
            let Some(doc) = &found[nth] else {
                return fail(format!("{id} was written but did not come back"));
            };
            let wrote = (self.document)(id);
            for field in wrote.as_object().into_iter().flatten().map(|(f, _)| f) {
                if doc.get(field).is_none_or(Value::is_null) {
                    return fail(format!("{id} came back without its {field}"));
                }
            }
        }
        // A read-back the target cannot take again is a completed TOAST
        // document the pipeline would then be unable to write.
        let Some(read) = found[0].clone() else {
            return fail("kit-2 was written but did not come back");
        };
        sink.write(vec![LsnOp {
            lsn: Lsn(150),
            op: DocumentOp::Upsert {
                index: self.index.clone(),
                id: "kit-2".into(),
                routing: None,
                doc: read.clone(),
                version: Some(150),
                pipeline: None,
            },
        }])
        .await?;
        sink.refresh(std::slice::from_ref(&self.index)).await?;
        let again = sink
            .get_documents(&self.index, &[("kit-2".to_string(), None)])
            .await?;
        if again.first().cloned().flatten() != Some(read) {
            return fail("a document written back as it was read came back different");
        }
        Ok(())
    }

    /// At-least-once delivery means the same batch arrives again after any
    /// restart; the target has to end up holding one document, not two.
    async fn idempotent_replay(&self, sink: &dyn Sink) -> Result<(), CoreError> {
        self.clear(sink).await?;
        let ids = ["kit-1", "kit-2", "kit-3"];
        let first = sink.write(self.batch(&ids, 200)).await?;
        let again = sink.write(self.batch(&ids, 200)).await?;
        if first.max_lsn != again.max_lsn {
            return fail(format!(
                "a replay of one batch acknowledged {} the first time and {} the second",
                first.max_lsn, again.max_lsn
            ));
        }
        if !first.rejected.is_empty() || !again.rejected.is_empty() {
            return fail("a batch of documents the target accepts reported a rejection");
        }
        sink.refresh(std::slice::from_ref(&self.index)).await?;
        if let Some(count) = sink.count_documents(&self.index).await?
            && count != ids.len() as u64
        {
            return fail(format!(
                "{} documents written twice left {count} behind",
                ids.len()
            ));
        }
        Ok(())
    }

    /// A truncate happens at a position: everything written before it loses,
    /// everything written after it survives — including a row re-inserted
    /// moments later, which is what a source-side TRUNCATE followed by an
    /// INSERT looks like from here.
    async fn versioned_truncate(&self, sink: &dyn Sink) -> Result<(), CoreError> {
        self.clear(sink).await?;
        sink.write(self.batch(&["kit-old"], 300)).await?;
        sink.write(self.batch(&["kit-new"], 500)).await?;
        sink.truncate_index(&self.index, Some(400), None).await?;
        sink.refresh(std::slice::from_ref(&self.index)).await?;
        let asked = vec![("kit-old".to_string(), None), ("kit-new".to_string(), None)];
        let found = sink.get_documents(&self.index, &asked).await?;
        if found.first().is_none_or(Option::is_some) {
            return fail("a document written before the truncate survived it");
        }
        if found.get(1).is_none_or(Option::is_none) {
            return fail("a document written after the truncate was cleared by it");
        }
        Ok(())
    }

    /// One document the target refuses must not take the rest of the batch with
    /// it: every rejection in a batch is reported, and the operations around it
    /// still land.
    async fn partial_batch(&self, sink: &dyn Sink, unacceptable: Value) -> Result<(), CoreError> {
        self.clear(sink).await?;
        let batch = vec![
            self.upsert("kit-before", 600),
            LsnOp {
                lsn: Lsn(601),
                op: DocumentOp::Upsert {
                    index: self.index.clone(),
                    id: "kit-bad".into(),
                    routing: None,
                    doc: unacceptable,
                    version: Some(601),
                    pipeline: None,
                },
            },
            self.upsert("kit-after", 602),
        ];
        let ack = sink.write(batch).await?;
        if ack.rejected.len() != 1 {
            return fail(format!(
                "a batch with one unacceptable document reported {} rejections",
                ack.rejected.len()
            ));
        }
        if ack.rejected[0].doc_id != "kit-bad" {
            return fail(format!(
                "the rejection names {} rather than the document that caused it",
                ack.rejected[0].doc_id
            ));
        }
        if ack.max_lsn != Lsn(602) {
            return fail(format!(
                "a batch that was written reported {} rather than its highest position",
                ack.max_lsn
            ));
        }
        sink.refresh(std::slice::from_ref(&self.index)).await?;
        let asked = vec![
            ("kit-before".to_string(), None),
            ("kit-after".to_string(), None),
        ];
        for (nth, doc) in sink
            .get_documents(&self.index, &asked)
            .await?
            .iter()
            .enumerate()
        {
            if doc.is_none() {
                return fail(format!(
                    "{} was lost with the refused document beside it",
                    asked[nth].0
                ));
            }
        }
        Ok(())
    }

    /// The checkpoint is what a restart resumes from, so it has to read back
    /// exactly, and never for a stream it does not belong to.
    async fn checkpoint_durability(&self, sink: &dyn Sink) -> Result<(), CoreError> {
        let mine = StreamId {
            source: crate::checkpoint::SOURCE_POSTGRES.into(),
            stream: "pg2osync_testkit".into(),
            publication: "pg2osync_testkit_pub".into(),
        };
        let other = StreamId {
            stream: "pg2osync_testkit_other".into(),
            ..mine.clone()
        };
        if sink.read_checkpoint(&other).await?.is_some() {
            return fail("a stream nothing ever wrote for has a checkpoint");
        }
        let checkpoint = Checkpoint {
            stream: mine.clone(),
            token: 0x1B4F2A8,
            position: "0/1B4F2A8".into(),
        };
        sink.write_checkpoint(&checkpoint).await?;
        match sink.read_checkpoint(&mine).await? {
            Some(read) if read == checkpoint => {}
            Some(read) => {
                return fail(format!(
                    "the checkpoint read back as {read:?} rather than {checkpoint:?}"
                ));
            }
            None => return fail("the checkpoint that was written did not read back"),
        }
        if sink.read_checkpoint(&other).await?.is_some() {
            return fail("one stream's checkpoint was returned for another's");
        }
        Ok(())
    }

    /// Every document this suite writes, gone, whatever position it carried.
    async fn clear(&self, sink: &dyn Sink) -> Result<(), CoreError> {
        sink.truncate_index(&self.index, None, None).await?;
        sink.refresh(std::slice::from_ref(&self.index)).await
    }

    fn batch(&self, ids: &[&str], version: u64) -> Vec<LsnOp> {
        ids.iter()
            .enumerate()
            .map(|(nth, id)| self.upsert(id, version + nth as u64))
            .collect()
    }

    fn upsert(&self, id: &str, version: u64) -> LsnOp {
        LsnOp {
            lsn: Lsn(version),
            op: DocumentOp::Upsert {
                index: self.index.clone(),
                id: id.to_string(),
                routing: None,
                doc: (self.document)(id),
                version: Some(version),
                pipeline: None,
            },
        }
    }
}

fn fail(why: impl Into<String>) -> Result<(), CoreError> {
    Err(CoreError::Sink(why.into()))
}

/// Which check failed, in front of why: a bare expectation says nothing about
/// which rule of the contract was broken.
fn named(check: &'static str) -> impl Fn(CoreError) -> CoreError {
    move |e| CoreError::Sink(format!("{check}: {e}"))
}
