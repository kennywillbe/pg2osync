//! Walk an index against its source and remove documents whose row is gone.
//!
//! Streaming replication is at-least-once and idempotent, so the index should
//! never drift — but "should never" is not a guarantee anyone can check. Poll
//! mode genuinely cannot see a hard delete, an incident can leave the two out
//! of step, and until now there was no tool that could say whether they were.
//!
//! Documents are compared by key rather than by content: only keys move
//! between the two sides, which is what makes this far cheaper than a reindex.

use anyhow::{Context as _, Result};
use pg2osync_core::sink::{DocumentOp, LsnOp, Sink};
use pg2osync_engine::mapping::IndexTarget;
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::Arc;

/// How many documents to read from the index, and check against the source,
/// at a time. Bounds both the query size and the memory this holds.
const PAGE: usize = 1000;

pub struct Table {
    pub qualified: String,
    /// The fixed index, or the template each row renders one from. A
    /// template's pattern claims every index it matches, so each of those is
    /// walked — the same claim a TRUNCATE makes.
    pub target: IndexTarget,
    pub key_column: String,
    /// The join field and this table's relation name in it, when the index is
    /// shared: the scan is narrowed to this table's documents, or the other
    /// table's would all read as orphans of this one.
    pub scope: Option<(String, String)>,
    pub soft_delete: Option<String>,
    /// The table's row filter: a row that no longer matches it still exists,
    /// and must still count as gone — the index is not supposed to hold it.
    pub filter: Option<pg2osync_core::filter::Filter>,
}

/// What one concrete index held.
pub struct Report {
    pub index: String,
    pub scanned: usize,
    /// Each orphan's id with the routing the index holds it under, which is
    /// what its delete has to name to reach the same shard.
    pub orphaned: Vec<(String, Option<String>)>,
    /// Documents whose row is live but renders another index: the id, its
    /// routing, and the index the row belongs in. Only a templated table has
    /// any — a fixed index is where every row renders.
    pub stray: Vec<(String, Option<String>, String)>,
}

/// Where a document stands against its row.
#[derive(Debug, PartialEq, Eq)]
pub enum Verdict {
    Ok,
    /// The row is gone, filtered out or soft-deleted.
    Orphan,
    /// The row is live, but the stream files it in another index — a copy
    /// left behind by a move nobody saw, or a document written by hand.
    Stray {
        belongs_in: String,
    },
}

/// Judge a document found in `found_in` by the indices its key's live rows
/// render now, `None` when the source holds no row the index should have.
///
/// A set rather than one name, because the key column need not be unique: a
/// composite real key with one declared key column is accepted under
/// `REPLICA IDENTITY FULL`, and then (acme, 1) and (globex, 1) are two live
/// documents sharing key 1. Keeping one name per key would call the other a
/// stray, and `--delete` would remove a document whose row is live.
pub fn classify(found_in: &str, expected: Option<&BTreeSet<String>>) -> Verdict {
    match expected {
        None => Verdict::Orphan,
        Some(indices) if indices.contains(found_in) => Verdict::Ok,
        Some(indices) => Verdict::Stray {
            belongs_in: indices
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(" or "),
        },
    }
}

/// Gather each key's rendered indices, every row of a shared key kept.
fn by_key(
    rendered: impl IntoIterator<Item = (String, String)>,
) -> HashMap<String, BTreeSet<String>> {
    let mut expected: HashMap<String, BTreeSet<String>> = HashMap::new();
    for (key, index) in rendered {
        expected.entry(key).or_default().insert(index);
    }
    expected
}

/// Compare a table's index, or every index its template claims, against the
/// table, one report per concrete index.
///
/// `delete` decides whether anything is removed. Reporting is the default
/// everywhere it is called from: a misconfigured key column would otherwise
/// empty an index, and finding that out afterwards is not a recoverable
/// mistake. An index with nothing in it is reported and left alone: removing
/// one is a decision about the cluster, not about documents.
pub async fn table(
    client: &tokio_postgres::Client,
    sink: &Arc<dyn Sink>,
    table: &Table,
    delete: bool,
) -> Result<Vec<Report>> {
    let (indices, columns) = match &table.target {
        IndexTarget::Static(name) => (vec![name.clone()], Vec::new()),
        IndexTarget::Template { template, .. } => {
            let pattern = table.target.pattern();
            let indices = sink
                .list_indices(&pattern)
                .await
                .with_context(|| format!("cannot list the indices {pattern} matches"))?;
            (
                indices,
                template_columns(client, table, &template.columns()).await?,
            )
        }
    };
    let columns = &columns;
    walk_then_delete(
        indices,
        |index| async move {
            // An index made by hand that matches the pattern may hold nothing,
            // and sorting by a field no document ever mapped is an error rather
            // than an empty page. A fixed index keeps the walk it always had.
            if matches!(table.target, IndexTarget::Template { .. })
                && sink
                    .count_documents(&index)
                    .await
                    .with_context(|| format!("cannot count index {index}"))?
                    .unwrap_or(0)
                    == 0
            {
                return Ok(Report {
                    index,
                    scanned: 0,
                    orphaned: Vec::new(),
                    stray: Vec::new(),
                });
            }
            one_index(client, sink, table, columns, index).await
        },
        delete,
        |ops| async move {
            let ack = sink.write(ops).await.context("deleting orphans failed")?;
            // The engine logs the pipeline's refusals; this pass writes past it.
            for rejection in &ack.rejected {
                tracing::error!(target: "pg2osync::sink",
                    "PERMANENT rejection {}/{} while deleting an orphan: {}",
                    rejection.index, rejection.doc_id, rejection.reason);
            }
            Ok(())
        },
    )
    .await
}

/// Judge every index first and delete only once all of them were judged.
///
/// A walk that stops partway — a page the target refuses, a live row that
/// cannot render a name — has not seen the whole table, and acting on half
/// of what it found would leave the indices in a state no report described.
/// Deleting per index would do exactly that for every index before the one
/// that failed. One bulk write for everything can still fail partway on the
/// target's side; the deletes are unversioned and idempotent, so running
/// reconcile again finishes the job.
async fn walk_then_delete<J, JF, W, WF>(
    indices: Vec<String>,
    mut judge: J,
    delete: bool,
    write: W,
) -> Result<Vec<Report>>
where
    J: FnMut(String) -> JF,
    JF: Future<Output = Result<Report>>,
    W: FnOnce(Vec<LsnOp>) -> WF,
    WF: Future<Output = Result<()>>,
{
    let mut reports = Vec::with_capacity(indices.len());
    for index in indices {
        reports.push(judge(index).await?);
    }
    let ops: Vec<LsnOp> = reports.iter().flat_map(doomed).collect();
    if delete && !ops.is_empty() {
        write(ops).await?;
    }
    Ok(reports)
}

/// The deletes one index's findings call for.
fn doomed(report: &Report) -> impl Iterator<Item = LsnOp> + '_ {
    let orphans = report.orphaned.iter().cloned();
    let strays = report
        .stray
        .iter()
        .map(|(id, routing, _)| (id.clone(), routing.clone()));
    orphans.chain(strays).map(|(id, routing)| LsnOp {
        // reconciliation has no source position of its own, and must
        // never move the checkpoint
        lsn: pg2osync_core::lsn::Lsn(0),
        op: DocumentOp::Delete {
            // the index it was found in, never the template's pattern:
            // a stray's real document lives under the same id elsewhere
            index: report.index.clone(),
            id,
            routing,
            // unversioned on purpose: the row is gone from the source, or
            // lives elsewhere, so there is no position that says when this
            // copy stopped being right, and a guessed version could lose
            // to a stale document
            version: None,
        },
    })
}

async fn one_index(
    client: &tokio_postgres::Client,
    sink: &Arc<dyn Sink>,
    table: &Table,
    columns: &[(String, u32)],
    index: String,
) -> Result<Report> {
    let mut report = Report {
        index,
        scanned: 0,
        orphaned: Vec::new(),
        stray: Vec::new(),
    };
    let mut after: Option<Value> = None;

    let only = table
        .scope
        .as_ref()
        .map(|(field, name)| (field.as_str(), name.as_str()));
    loop {
        let page = sink
            .scan_keys(
                &report.index,
                &table.key_column,
                only,
                after.as_ref(),
                PAGE + 1,
            )
            .await
            .with_context(|| format!("cannot page index {}", report.index))?;
        if page.is_empty() {
            break;
        }
        let (judged, next) = settle_page(&page, PAGE)
            .with_context(|| format!("cannot page index {}", report.index))?;
        let page = &page[..judged];
        after = next;
        report.scanned += page.len();

        let keys: Vec<String> = page.iter().map(|(_, key, _)| render_key(key)).collect();
        let expected = expected_indices(client, table, columns, &keys).await?;
        for (id, key, routing) in page {
            match classify(&report.index, expected.get(&render_key(key))) {
                Verdict::Ok => {}
                Verdict::Orphan => report.orphaned.push((id.clone(), routing.clone())),
                Verdict::Stray { belongs_in } => {
                    report.stray.push((id.clone(), routing.clone(), belongs_in))
                }
            }
        }
    }

    Ok(report)
}

/// How much of a page to judge now, and the key the next page starts after.
///
/// The key column need not be unique within an index — a composite real key
/// with one declared key column puts (acme, 1000) and (globex, 1000) side by
/// side — and `search_after` resumes strictly after a key, so a full page
/// that ends partway through a key's copies would never see the rest. The
/// copies of the last key are held back for the next page instead, which
/// starts after the key before them and so begins with all of them. A short
/// page holds everything left and is judged whole.
///
/// The page is asked for one document past `limit`, so exactly `limit` copies
/// of one key still leave room for what follows them and only more than
/// `limit` fail, as the documented limit says.
fn settle_page(
    page: &[(String, Value, Option<String>)],
    limit: usize,
) -> Result<(usize, Option<Value>)> {
    let last = page.last().map(|(_, key, _)| key);
    if page.len() <= limit {
        return Ok((page.len(), last.cloned()));
    }
    let judged = page
        .iter()
        .rposition(|(_, key, _)| Some(key) != last)
        .map_or(0, |n| n + 1);
    if judged == 0 {
        // moving past this key would skip copies nobody has seen, and a
        // walk that deletes must not pretend it saw the whole index
        anyhow::bail!(
            "more than {limit} documents share key {} in one index, which one page cannot hold",
            last.map(render_key).unwrap_or_default()
        );
    }
    Ok((judged, Some(page[judged - 1].1.clone())))
}

/// The template's columns with the type each has in the source, which is
/// what decides how its text is decoded.
async fn template_columns(
    client: &tokio_postgres::Client,
    table: &Table,
    names: &[&str],
) -> Result<Vec<(String, u32)>> {
    let catalog = crate::backfill::columns_of(client, &table.qualified)
        .await
        .with_context(|| format!("cannot read the columns of {}", table.qualified))?;
    names
        .iter()
        .map(|name| {
            catalog
                .iter()
                .find(|col| col.name == *name)
                .map(|col| (col.name.clone(), col.type_oid))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "the index template names {name}, which {} does not have",
                        table.qualified
                    )
                })
        })
        .collect()
}

/// For each of these keys the source still has a row for, the indices the
/// stream would file its rows in.
///
/// Keys are compared as text: the key column may be an integer, a uuid or
/// anything else, and the index reports whatever it stored. A cast on the
/// column costs the index on it, which for a pass that runs occasionally is
/// the right side of that trade. The template's columns are read as text
/// too, exactly as the initial load reads them, and decoded by the same
/// type map, so the name rendered here is the one the stream renders.
async fn expected_indices(
    client: &tokio_postgres::Client,
    table: &Table,
    columns: &[(String, u32)],
    keys: &[String],
) -> Result<HashMap<String, BTreeSet<String>>> {
    // a soft-deleted row still exists, and must still count as gone
    let mut filter = match &table.soft_delete {
        Some(predicate) => format!(" AND NOT ({predicate})"),
        None => String::new(),
    };
    if let Some(row_filter) = &table.filter {
        filter.push_str(&format!(
            " AND ({})",
            row_filter.to_sql(&crate::backfill::pg_dialect_bare())
        ));
    }
    let key = quote_ident(&table.key_column);
    let selected: String = columns
        .iter()
        .map(|(name, _)| format!(", {}::text", quote_ident(name)))
        .collect();
    let sql = format!(
        "SELECT {key}::text{selected} FROM {tbl} WHERE {key}::text = ANY($1){filter}",
        tbl = qualify(&table.qualified),
    );
    let rows = client
        .query(&sql, &[&keys])
        .await
        .with_context(|| format!("cannot read keys of {}", table.qualified))?;
    let rendered = rows
        .iter()
        .map(|row| {
            let key: String = row.get(0);
            let values: Vec<Option<String>> = (1..=columns.len()).map(|i| row.get(i)).collect();
            // a live row whose name cannot be rendered is the row the stream
            // halts on; deciding here where its document belongs would be
            // inventing a name, so the walk stops the same way
            let index = render_row(&table.target, columns, &values)
                .map_err(|e| anyhow::anyhow!("{}: the row with key {key}: {e}", table.qualified))?;
            Ok((key, index))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(by_key(rendered))
}

/// The index one row renders, from its template columns as text.
fn render_row(
    target: &IndexTarget,
    columns: &[(String, u32)],
    values: &[Option<String>],
) -> Result<String> {
    let mut doc = serde_json::Map::new();
    for ((name, oid), value) in columns.iter().zip(values) {
        let decoded = pg2osync_source::typemap::convert(*oid, value.as_deref().map(str::as_bytes))
            .map_err(|e| anyhow::anyhow!("column {name}: {e}"))?;
        doc.insert(name.clone(), decoded);
    }
    target
        .render(&Value::Object(doc))
        .map_err(|e| anyhow::anyhow!(e))
}

/// The index's form of a key as text, matching what `::text` produces.
fn render_key(key: &Value) -> String {
    match key {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn qualify(qualified: &str) -> String {
    match qualified.split_once('.') {
        Some((schema, table)) => format!("{}.{}", quote_ident(schema), quote_ident(table)),
        None => quote_ident(qualified),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn keys_render_the_way_a_text_cast_does() {
        assert_eq!(render_key(&json!(42)), "42");
        assert_eq!(render_key(&json!("abc")), "abc", "no JSON quotes around it");
        assert_eq!(render_key(&json!("42")), "42");
    }

    #[test]
    fn identifiers_are_quoted() {
        assert_eq!(qualify("we\"ird.tbl"), "\"we\"\"ird\".\"tbl\"");
        assert_eq!(qualify("plain"), "\"plain\"");
    }

    #[test]
    fn a_document_is_judged_by_where_its_row_would_file_it() {
        let acme = BTreeSet::from(["events-acme".to_string()]);
        let globex = BTreeSet::from(["events-globex".to_string()]);
        assert_eq!(classify("events-acme", None), Verdict::Orphan);
        assert_eq!(classify("events-acme", Some(&acme)), Verdict::Ok);
        assert_eq!(
            classify("events-acme", Some(&globex)),
            Verdict::Stray {
                belongs_in: "events-globex".into()
            }
        );
    }

    #[test]
    fn a_key_two_live_rows_share_is_at_home_in_both_their_indices() {
        // a composite real key with one declared key column: (acme, 1) and
        // (globex, 1) are two rows, two documents, one key
        let expected = by_key([
            ("1".to_string(), "events-acme".to_string()),
            ("1".to_string(), "events-globex".to_string()),
            ("2".to_string(), "events-acme".to_string()),
        ]);
        assert_eq!(classify("events-acme", expected.get("1")), Verdict::Ok);
        assert_eq!(classify("events-globex", expected.get("1")), Verdict::Ok);
        assert_eq!(
            classify("events-globex", expected.get("2")),
            Verdict::Stray {
                belongs_in: "events-acme".into()
            }
        );
        assert_eq!(classify("events-acme", expected.get("3")), Verdict::Orphan);
        assert_eq!(
            classify("events-other", expected.get("1")),
            Verdict::Stray {
                belongs_in: "events-acme or events-globex".into()
            },
            "every index the key's rows render is named"
        );
    }

    fn template(spec: &str) -> IndexTarget {
        IndexTarget::Template {
            spec: spec.into(),
            template: pg2osync_engine::mapping::IdTemplate::parse(spec, &["id".into()])
                .expect("a usable template"),
        }
    }

    /// The index the stream files a row under, from the bytes pgoutput sends
    /// for it: the same decoder, the same template, the same raw row.
    fn streamed(target: &IndexTarget, oid: u32, wal: &str) -> Result<String, String> {
        use pg2osync_source::pgoutput::{
            Relation, RelationColumn, ReplicaIdentity, Tuple, TupleValue,
        };
        let rel = Relation {
            rel_id: 1,
            schema: "public".into(),
            name: "events".into(),
            replica_identity: ReplicaIdentity::Full,
            columns: vec![
                RelationColumn {
                    name: "id".into(),
                    type_oid: 23,
                    typmod: -1,
                    in_replica_identity: true,
                },
                RelationColumn {
                    name: "tenant".into(),
                    type_oid: oid,
                    typmod: -1,
                    in_replica_identity: true,
                },
            ],
        };
        let tuple = Tuple(vec![
            TupleValue::Text(b"1".to_vec()),
            TupleValue::Text(wal.as_bytes().to_vec()),
        ]);
        let change = pg2osync_source::docbuild::build_row_change(
            &rel,
            pg2osync_source::docbuild::Incoming::Insert(tuple),
            Some(&["id".to_string()]),
            false,
        )
        .expect("a decodable row");
        let pg2osync_core::event::RowKind::Insert { doc, .. } = change.kind else {
            unreachable!("an insert was built");
        };
        target.render(&doc)
    }

    #[test]
    fn reconcile_renders_the_index_the_stream_renders() {
        let target = template("events-{tenant}");
        // (type oid, what pgoutput sends, what a ::text cast reads back):
        // int4, numeric, bool, date, text. A bool is `t` on the wire and
        // `true` as text, which is exactly where two decoders could part.
        let cases: [(u32, &str, &str); 5] = [
            (23, "42", "42"),
            (1700, "10", "10"),
            (16, "t", "true"),
            (1082, "2024-01-31", "2024-01-31"),
            (25, "acme", "acme"),
        ];
        for (oid, wal, text) in cases {
            let ours = render_row(&target, &[("tenant".into(), oid)], &[Some(text.into())])
                .map_err(|e| e.to_string());
            assert_eq!(ours, streamed(&target, oid, wal), "type {oid}");
        }
        assert_eq!(
            render_row(&target, &[("tenant".into(), 16)], &[Some("true".into())])
                .expect("a usable name"),
            "events-true"
        );
        // a fractional numeric renders a name the target refuses, and both
        // sides have to refuse it rather than one of them inventing a name
        let ours = render_row(&target, &[("tenant".into(), 1700)], &[Some("10.5".into())]);
        assert!(ours.is_err());
        assert!(streamed(&target, 1700, "10.5").is_err());
    }

    #[test]
    fn a_row_with_a_null_index_column_is_an_error_not_a_name() {
        let target = template("events-{tenant}");
        assert!(render_row(&target, &[("tenant".into(), 25)], &[None]).is_err());
    }

    #[test]
    fn a_fixed_index_renders_itself_whatever_the_row() {
        let target = IndexTarget::Static("users".into());
        assert_eq!(render_row(&target, &[], &[]).expect("a name"), "users");
    }

    fn hits(keys: &[i64]) -> Vec<(String, Value, Option<String>)> {
        keys.iter()
            .enumerate()
            .map(|(n, key)| (format!("doc{n}"), json!(key), None))
            .collect()
    }

    #[test]
    fn a_key_split_by_the_page_end_is_left_whole_for_the_next_page() {
        // (acme, 1000) and (globex, 1000) in one index share key 1000; resuming
        // after 1000 would skip whichever copy the page did not reach
        let (judged, after) = settle_page(&hits(&[998, 999, 1000]), 2).expect("progress");
        assert_eq!(judged, 2);
        assert_eq!(after, Some(json!(999)));
        let (judged, after) = settle_page(&hits(&[998, 1000, 1000]), 2).expect("progress");
        assert_eq!(
            judged, 1,
            "every copy of the last key waits for the next page"
        );
        assert_eq!(after, Some(json!(998)));
    }

    #[test]
    fn a_short_page_is_the_last_and_is_judged_whole() {
        let (judged, after) = settle_page(&hits(&[1, 2, 2]), 3).expect("progress");
        assert_eq!(judged, 3);
        assert_eq!(after, Some(json!(2)));
    }

    #[test]
    fn exactly_the_limit_of_one_key_still_fits() {
        // the documented limit is "more than" the page size; a page that is
        // only the limit's worth of one key has to settle, alone or followed
        let (judged, after) = settle_page(&hits(&[7, 7, 7]), 3).expect("fits");
        assert_eq!(judged, 3);
        assert_eq!(after, Some(json!(7)));
        let (judged, after) = settle_page(&hits(&[7, 7, 7, 8]), 3).expect("fits");
        assert_eq!(judged, 3);
        assert_eq!(after, Some(json!(7)));
    }

    #[test]
    fn a_full_page_of_one_key_is_an_error_not_a_skip() {
        assert!(settle_page(&hits(&[7, 7, 7, 7]), 3).is_err());
    }

    fn found(index: &str, orphan: &str, stray: &str) -> Report {
        Report {
            index: index.into(),
            scanned: 2,
            orphaned: vec![(orphan.into(), None)],
            stray: vec![(stray.into(), Some("r".into()), "events-elsewhere".into())],
        }
    }

    fn deleted(ops: &[LsnOp]) -> Vec<(String, String, Option<String>)> {
        ops.iter()
            .map(|op| match &op.op {
                DocumentOp::Delete {
                    index, id, routing, ..
                } => (index.clone(), id.clone(), routing.clone()),
                other => panic!("reconcile only deletes, got {other:?}"),
            })
            .collect()
    }

    #[tokio::test]
    async fn a_failing_later_index_deletes_nothing_from_the_earlier_ones() {
        let writes = std::cell::RefCell::new(Vec::new());
        let outcome = walk_then_delete(
            vec!["events-acme".into(), "events-globex".into()],
            |index| async move {
                if index == "events-acme" {
                    Ok(found(&index, "1", "2"))
                } else {
                    anyhow::bail!("cannot page index {index}")
                }
            },
            true,
            |ops| async {
                writes.borrow_mut().push(ops);
                Ok(())
            },
        )
        .await;
        assert!(outcome.is_err());
        assert!(
            writes.borrow().is_empty(),
            "a walk that did not finish must not act on what it saw so far"
        );
    }

    #[tokio::test]
    async fn a_finished_walk_deletes_every_index_s_findings_in_one_write() {
        let writes = std::cell::RefCell::new(Vec::new());
        let reports = walk_then_delete(
            vec!["events-acme".into(), "events-globex".into()],
            |index| async move {
                let (orphan, stray) = if index == "events-acme" {
                    ("1", "2")
                } else {
                    ("3", "4")
                };
                Ok(found(&index, orphan, stray))
            },
            true,
            |ops| async {
                writes.borrow_mut().push(ops);
                Ok(())
            },
        )
        .await
        .expect("the walk finished");
        assert_eq!(reports.len(), 2);
        let writes = writes.into_inner();
        assert_eq!(writes.len(), 1);
        assert_eq!(
            deleted(&writes[0]),
            vec![
                ("events-acme".into(), "1".into(), None),
                ("events-acme".into(), "2".into(), Some("r".into())),
                ("events-globex".into(), "3".into(), None),
                ("events-globex".into(), "4".into(), Some("r".into())),
            ],
            "each delete names the index its document was found in"
        );
    }

    #[tokio::test]
    async fn without_delete_a_finished_walk_writes_nothing() {
        let writes = std::cell::RefCell::new(Vec::new());
        walk_then_delete(
            vec!["events-acme".into()],
            |index| async move { Ok(found(&index, "1", "2")) },
            false,
            |ops| async {
                writes.borrow_mut().push(ops);
                Ok(())
            },
        )
        .await
        .expect("the walk finished");
        assert!(writes.borrow().is_empty());
    }
}
