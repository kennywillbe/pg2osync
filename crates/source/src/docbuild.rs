//! Tuple → document construction.
//!
//! Turns decoded pgoutput tuples into `core::RowChange` values: JSON docs,
//! primary-key extraction and unchanged-TOAST surfacing/completion.

use crate::pgoutput::{Relation, ReplicaIdentity, Tuple, TupleValue};
use crate::typemap;
use pg2osync_core::event::{RowChange, RowKind};
use serde_json::{Map, Value};

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error(
        "table {schema}.{name} has REPLICA IDENTITY NOTHING; deletes and updates \
             cannot be replicated. Run: ALTER TABLE {schema}.{name} REPLICA IDENTITY FULL"
    )]
    NoReplicaIdentity { schema: String, name: String },

    #[error("column '{col}' of {schema}.{name}: {source}")]
    ColumnConvert {
        col: String,
        schema: String,
        name: String,
        source: typemap::TypeError,
    },

    /// An append-only table has no key, so a document written under a
    /// content hash cannot be found again from the row that replaced it.
    #[error(
        "{schema}.{name}: {what} arrived on an append-only table; nothing can say \
         which document it is"
    )]
    AppendOnly {
        schema: String,
        name: String,
        what: &'static str,
    },
}

fn convert_column(
    rel: &Relation,
    col_name: &str,
    type_oid: u32,
    raw: Option<&[u8]>,
) -> Result<Value, BuildError> {
    typemap::convert(type_oid, raw).map_err(|source| BuildError::ColumnConvert {
        col: col_name.to_string(),
        schema: rel.schema.clone(),
        name: rel.name.clone(),
        source,
    })
}

/// Render the primary key as the document identity value:
/// scalar when single-column, object keyed by column names when composite.
///
/// `key_columns` is the table's primary key as the catalogue reports it.
/// Under REPLICA IDENTITY FULL pgoutput flags *every* column as part of the
/// identity, so the flags alone would turn the whole row into the key — a
/// document id that changes with any column, never matching what the initial
/// load filed the row under. The flags are the fallback for a relation the
/// caller knows no key for.
fn extract_pk(
    rel: &Relation,
    tuple: &Tuple,
    key_columns: Option<&[String]>,
) -> Result<Value, BuildError> {
    let is_key = |col: &crate::pgoutput::RelationColumn| match key_columns {
        Some(keys) if rel.replica_identity == ReplicaIdentity::Full && !keys.is_empty() => {
            keys.iter().any(|k| k == &col.name)
        }
        _ => col.in_replica_identity,
    };
    let mut obj = Map::new();
    let mut scalar: Option<Value> = None;
    for (idx, col) in rel.columns.iter().enumerate() {
        if !is_key(col) {
            continue;
        }
        let v = match tuple.get(idx) {
            Some(TupleValue::Text(bytes)) => {
                convert_column(rel, &col.name, col.type_oid, Some(bytes))?
            }
            Some(_) | None => Value::Null,
        };
        scalar.get_or_insert(v.clone());
        obj.insert(col.name.clone(), v);
    }
    if obj.is_empty() {
        return Err(BuildError::NoReplicaIdentity {
            schema: rel.schema.clone(),
            name: rel.name.clone(),
        });
    }
    Ok(if obj.len() == 1 {
        scalar.expect("single entry inserted above")
    } else {
        Value::Object(obj)
    })
}

/// Build the full-row JSON document, applying TOAST rules:
/// - 'u'-marked column filled from `old_tuple` when REPLICA IDENTITY FULL provided one
/// - otherwise recorded in `unchanged_toast_columns` for engine-side read-back
fn build_doc<'a>(
    rel: &Relation,
    new_tuple: &Tuple,
    old_values: Option<&'a [Option<&'a [u8]>]>,
) -> Result<(Value, Vec<String>), BuildError> {
    let mut doc = Map::new();
    let mut toast_missing = Vec::new();
    for (idx, col) in rel.columns.iter().enumerate() {
        match new_tuple.get(idx) {
            Some(TupleValue::Null) => {
                doc.insert(col.name.clone(), Value::Null);
            }
            Some(TupleValue::UnchangedToast) => {
                // RIF FULL's old tuple carries the real value even for TOASTed
                // columns, because FULL forces the whole old row into WAL.
                if let Some(old) = old_values.and_then(|o| o.get(idx)).and_then(|v| *v) {
                    let v = convert_column(rel, &col.name, col.type_oid, Some(old))?;
                    doc.insert(col.name.clone(), v);
                } else {
                    toast_missing.push(col.name.clone());
                    doc.insert(col.name.clone(), Value::Null);
                }
            }
            Some(TupleValue::Text(bytes)) => {
                let v = convert_column(rel, &col.name, col.type_oid, Some(bytes))?;
                doc.insert(col.name.clone(), v);
            }
            None => {}
        }
    }
    Ok((Value::Object(doc), toast_missing))
}

/// Extract raw byte slices of a tuple for reuse across old/new comparisons.
pub fn tuple_slices(tuple: &Tuple) -> Vec<Option<&[u8]>> {
    tuple
        .0
        .iter()
        .map(|v| match v {
            TupleValue::Text(b) => Some(b.as_slice()),
            _ => None,
        })
        .collect()
}

pub enum Incoming {
    Insert(Tuple),
    /// (key-or-old tuple kind already resolved by caller, new tuple)
    Update(Option<Tuple>, Tuple),
    Delete(Tuple),
}

impl Incoming {
    /// The tuple carrying FK-relevant values: the new image for insert/update,
    /// the key/old image for delete.
    pub fn tuple(&self) -> &Tuple {
        match self {
            Incoming::Insert(t) | Incoming::Update(_, t) | Incoming::Delete(t) => t,
        }
    }

    /// Every tuple that can carry the foreign key locating what this row
    /// belongs to: the image above first, and behind it an update's old one.
    ///
    /// A row whose foreign key moved belongs to two parents in one change, and
    /// the old image is the only place the one it left is named.
    pub fn tuples(&self) -> Vec<&Tuple> {
        match self {
            Incoming::Insert(t) | Incoming::Delete(t) => vec![t],
            Incoming::Update(None, t) => vec![t],
            Incoming::Update(Some(old), t) => vec![t, old],
        }
    }
}

/// `append_only` declares the table keyless: an insert carries no key (the
/// engine files it under a hash of its content), and an update or delete is
/// an error, because no key means nothing can say which document it is.
pub fn build_row_change(
    rel: &Relation,
    incoming: Incoming,
    key_columns: Option<&[String]>,
    append_only: bool,
) -> Result<RowChange, BuildError> {
    let change = |kind: RowKind| RowChange {
        schema: rel.schema.clone(),
        table: rel.name.clone(),
        kind,
        // the decoder sees one message; the position belongs to the
        // transaction around it, which only the caller tracks
        version: None,
    };
    if append_only && !matches!(incoming, Incoming::Insert(_)) {
        return Err(BuildError::AppendOnly {
            schema: rel.schema.clone(),
            name: rel.name.clone(),
            what: match incoming {
                Incoming::Update(..) => "an UPDATE",
                _ => "a DELETE",
            },
        });
    }
    // without a replica identity there is no key to address the row on
    // update/delete; inserts are unaffected
    if !matches!(incoming, Incoming::Insert(_)) {
        check_delete_capability(rel)?;
    }
    match incoming {
        Incoming::Insert(new) => {
            let pk = if append_only {
                Value::Null
            } else {
                extract_pk(rel, &new, key_columns)?
            };
            let (doc, _) = build_doc(rel, &new, None)?;
            Ok(change(RowKind::Insert { pk, doc }))
        }
        Incoming::Update(old, new) => {
            // The key must come from the new tuple: it says where the row lives
            // now. The old key addresses the document the row used to occupy,
            // which the engine has to remove when the two differ.
            let pk = extract_pk(rel, &new, key_columns)?;
            let previous_pk = old
                .as_ref()
                .and_then(|prev| extract_pk(rel, prev, key_columns).ok());
            let old_slices = old.as_ref().map(tuple_slices);
            let (doc, toast) = build_doc(rel, &new, old_slices.as_deref())?;
            // Only REPLICA IDENTITY FULL guarantees the old tuple carries the
            // whole row, and a partial before-image would be worse than none:
            // the columns it omits would read as NULL to a derived id.
            let before = match (old.as_ref(), rel.replica_identity) {
                (Some(prev), ReplicaIdentity::Full) => Some(build_doc(rel, prev, None)?.0),
                _ => None,
            };
            Ok(change(RowKind::Update {
                pk,
                previous_pk,
                doc,
                unchanged_toast_columns: toast,
                before,
            }))
        }
        Incoming::Delete(key) => {
            let pk = extract_pk(rel, &key, key_columns)?;
            let before = if rel.replica_identity == ReplicaIdentity::Full {
                Some(build_doc(rel, &key, None)?.0)
            } else {
                None
            };
            Ok(change(RowKind::Delete { pk, before }))
        }
    }
}

/// Startup guard: tables with REPLICA IDENTITY NOTHING will
/// fail at delete time unless fixed now.
pub fn check_delete_capability(rel: &Relation) -> Result<(), BuildError> {
    if rel.replica_identity == ReplicaIdentity::None {
        return Err(BuildError::NoReplicaIdentity {
            schema: rel.schema.clone(),
            name: rel.name.clone(),
        });
    }
    Ok(())
}

/// Index of a named column inside a relation, for tuple access.
pub fn column_index(rel: &Relation, name: &str) -> Option<usize> {
    rel.columns.iter().position(|c| c.name == name)
}

/// Typed JSON value of one tuple column (raw wire bytes to JSON).
pub fn convert_column_at(rel: &Relation, idx: usize, tuple: &Tuple) -> Result<Value, BuildError> {
    let col = &rel.columns[idx];
    let raw = match tuple.get(idx) {
        Some(TupleValue::Text(b)) => Some(b.as_slice()),
        _ => None,
    };
    convert_column(rel, &col.name, col.type_oid, raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgoutput;

    #[test]
    fn an_update_carries_the_image_it_had_as_well_as_the_one_it_has() {
        // A row whose foreign key moved belongs to two parents in one change,
        // and the parent it left is named nowhere but the before-image.
        let new = tuple(vec![TupleValue::Text(b"2".to_vec())]);
        let old = tuple(vec![TupleValue::Text(b"1".to_vec())]);
        let moved = Incoming::Update(Some(old.clone()), new.clone());
        assert_eq!(moved.tuples(), vec![&new, &old]);
        assert_eq!(
            Incoming::Update(None, new.clone()).tuples(),
            vec![&new],
            "a replica identity that carries no before-image names one parent"
        );
        assert_eq!(Incoming::Insert(new.clone()).tuples(), vec![&new]);
        assert_eq!(Incoming::Delete(old.clone()).tuples(), vec![&old]);
        assert_eq!(moved.tuple(), &new, "the image above is still the row");
    }

    fn users_relation(replica_identity: pgoutput::ReplicaIdentity) -> Relation {
        Relation {
            rel_id: 16385,
            schema: "public".into(),
            name: "users".into(),
            replica_identity,
            columns: vec![
                pgoutput::RelationColumn {
                    name: "id".into(),
                    type_oid: 20, // int8
                    typmod: -1,
                    in_replica_identity: true,
                },
                pgoutput::RelationColumn {
                    name: "email".into(),
                    type_oid: 25, // text
                    typmod: -1,
                    in_replica_identity: false,
                },
                pgoutput::RelationColumn {
                    name: "score".into(),
                    type_oid: 1700, // numeric
                    typmod: -1,
                    in_replica_identity: false,
                },
                pgoutput::RelationColumn {
                    name: "bio".into(),
                    type_oid: 25, // text
                    typmod: -1,
                    in_replica_identity: false,
                },
            ],
        }
    }

    fn tuple(vals: Vec<TupleValue>) -> Tuple {
        Tuple(vals)
    }

    #[test]
    fn insert_builds_full_doc_with_scalar_pk() {
        let rel = users_relation(pgoutput::ReplicaIdentity::Default);
        let t = tuple(vec![
            TupleValue::Text(b"42".to_vec()),
            TupleValue::Text(b"a@x.io".to_vec()),
            TupleValue::Text(b"12345678901234567890.5".to_vec()),
            TupleValue::Null,
        ]);
        let change = build_row_change(&rel, Incoming::Insert(t), None, false).unwrap();
        assert_eq!(change.schema, "public");
        match change.kind {
            RowKind::Insert { pk, doc } => {
                assert_eq!(pk, serde_json::json!(42));
                assert_eq!(doc["email"], serde_json::json!("a@x.io"));
                // numeric precision preserved as string
                assert_eq!(doc["score"], serde_json::json!("12345678901234567890.5"));
                assert_eq!(doc["bio"], Value::Null);
            }
            other => panic!("unexpected kind {other:?}"),
        }
    }

    #[test]
    fn update_completes_toast_from_rif_full_old_tuple() {
        let rel = users_relation(pgoutput::ReplicaIdentity::Full);
        let old = tuple(vec![
            TupleValue::Text(b"42".to_vec()),
            TupleValue::Text(b"a@x.io".to_vec()),
            TupleValue::Null,
            // FULL forces the whole old row into WAL, so the real value is here
            TupleValue::Text(b"long old bio".to_vec()),
        ]);
        let new = tuple(vec![
            TupleValue::Text(b"42".to_vec()),
            TupleValue::UnchangedToast, // unchanged large column
            TupleValue::Text(b"99.9".to_vec()),
            TupleValue::UnchangedToast,
        ]);
        let change = build_row_change(&rel, Incoming::Update(Some(old), new), None, false).unwrap();
        match change.kind {
            RowKind::Update {
                pk,
                doc,
                unchanged_toast_columns,
                ..
            } => {
                assert_eq!(pk, serde_json::json!(42));
                // completed from old tuple, NOT marked missing
                assert_eq!(doc["bio"], serde_json::json!("long old bio"));
                assert!(unchanged_toast_columns.is_empty());
            }
            other => panic!("unexpected kind {other:?}"),
        }
    }

    #[test]
    fn changing_the_key_reports_where_the_row_moved_from() {
        // the key addresses the document, so an update that changes it must
        // report both ends or the old document is stranded forever
        let rel = users_relation(pgoutput::ReplicaIdentity::Default);
        let old = tuple(vec![
            TupleValue::Text(b"1".to_vec()),
            TupleValue::Text(b"a@x.io".to_vec()),
            TupleValue::Null,
            TupleValue::Text(b"bio".to_vec()),
        ]);
        let new = tuple(vec![
            TupleValue::Text(b"2".to_vec()),
            TupleValue::Text(b"a@x.io".to_vec()),
            TupleValue::Null,
            TupleValue::Text(b"bio".to_vec()),
        ]);
        let change = build_row_change(&rel, Incoming::Update(Some(old), new), None, false).unwrap();
        match change.kind {
            RowKind::Update {
                pk, previous_pk, ..
            } => {
                assert_eq!(pk, serde_json::json!(2), "the row lives at its new key");
                assert_eq!(previous_pk, Some(serde_json::json!(1)));
            }
            other => panic!("unexpected kind {other:?}"),
        }
    }

    #[test]
    fn an_update_that_keeps_its_key_reports_the_same_key_twice() {
        let rel = users_relation(pgoutput::ReplicaIdentity::Full);
        let old = tuple(vec![
            TupleValue::Text(b"42".to_vec()),
            TupleValue::Text(b"old@x.io".to_vec()),
            TupleValue::Null,
            TupleValue::Text(b"bio".to_vec()),
        ]);
        let new = tuple(vec![
            TupleValue::Text(b"42".to_vec()),
            TupleValue::Text(b"new@x.io".to_vec()),
            TupleValue::Null,
            TupleValue::Text(b"bio".to_vec()),
        ]);
        let change = build_row_change(&rel, Incoming::Update(Some(old), new), None, false).unwrap();
        match change.kind {
            RowKind::Update {
                pk, previous_pk, ..
            } => {
                // REPLICA IDENTITY FULL always sends the old tuple, so equal
                // keys are the common case and must not read as a move
                assert_eq!(previous_pk, Some(pk));
            }
            other => panic!("unexpected kind {other:?}"),
        }
    }

    #[test]
    fn update_without_old_marks_toast_columns_incomplete() {
        let rel = users_relation(pgoutput::ReplicaIdentity::Default);
        let new = tuple(vec![
            TupleValue::Text(b"42".to_vec()),
            TupleValue::Text(b"a@x.io".to_vec()),
            TupleValue::Null,
            TupleValue::UnchangedToast,
        ]);
        let change = build_row_change(&rel, Incoming::Update(None, new), None, false).unwrap();
        match change.kind {
            RowKind::Update {
                unchanged_toast_columns,
                ..
            } => assert_eq!(unchanged_toast_columns, vec!["bio".to_string()]),
            other => panic!("unexpected kind {other:?}"),
        }
    }

    #[test]
    fn delete_uses_key_tuple() {
        let rel = users_relation(pgoutput::ReplicaIdentity::Default);
        let key = tuple(vec![
            TupleValue::Text(b"42".to_vec()),
            TupleValue::Null,
            TupleValue::Null,
            TupleValue::Null,
        ]);
        let change = build_row_change(&rel, Incoming::Delete(key), None, false).unwrap();
        assert!(matches!(change.kind, RowKind::Delete { pk, .. } if pk == serde_json::json!(42)));
    }

    #[test]
    fn replica_identity_nothing_is_rejected_upfront() {
        let rel = users_relation(pgoutput::ReplicaIdentity::None);
        let key = tuple(vec![TupleValue::Null; 4]);
        assert!(build_row_change(&rel, Incoming::Delete(key), None, false).is_err());
    }

    #[test]
    fn composite_pk_becomes_object() {
        let mut rel = users_relation(pgoutput::ReplicaIdentity::Index);
        rel.columns.push(pgoutput::RelationColumn {
            name: "tenant_id".into(),
            type_oid: 23,
            typmod: -1,
            in_replica_identity: true,
        });
        let key = tuple(vec![
            TupleValue::Text(b"42".to_vec()),
            TupleValue::Null,
            TupleValue::Null,
            TupleValue::Null,
            TupleValue::Text(b"7".to_vec()),
        ]);
        let change = build_row_change(&rel, Incoming::Delete(key), None, false).unwrap();
        match change.kind {
            RowKind::Delete { pk, .. } => {
                assert_eq!(pk, serde_json::json!({"id": 42, "tenant_id": 7}))
            }
            other => panic!("unexpected kind {other:?}"),
        }
    }

    #[test]
    fn a_full_old_tuple_becomes_the_update_before_image() {
        let rel = users_relation(pgoutput::ReplicaIdentity::Full);
        let old = tuple(vec![
            TupleValue::Text(b"42".to_vec()),
            TupleValue::Text(b"a@x.io".to_vec()),
            TupleValue::Text(b"1".to_vec()),
            TupleValue::Text(b"bio".to_vec()),
        ]);
        let new = tuple(vec![
            TupleValue::Text(b"42".to_vec()),
            TupleValue::Text(b"b@x.io".to_vec()),
            TupleValue::Text(b"1".to_vec()),
            TupleValue::Text(b"bio".to_vec()),
        ]);
        let change = build_row_change(&rel, Incoming::Update(Some(old), new), None, false).unwrap();
        match change.kind {
            RowKind::Update { before, .. } => assert_eq!(
                before,
                Some(serde_json::json!({"id": 42, "email": "a@x.io", "score": "1", "bio": "bio"})),
                "the before-image carries the row as it was, by column name"
            ),
            other => panic!("unexpected kind {other:?}"),
        }
    }

    #[test]
    fn a_default_identity_update_carries_no_before_image() {
        // a key-only old tuple would read as NULLs in every other column,
        // which is worse for a derived id than carrying nothing at all
        let rel = users_relation(pgoutput::ReplicaIdentity::Default);
        let old = tuple(vec![
            TupleValue::Text(b"1".to_vec()),
            TupleValue::Null,
            TupleValue::Null,
            TupleValue::Null,
        ]);
        let new = tuple(vec![
            TupleValue::Text(b"2".to_vec()),
            TupleValue::Text(b"a@x.io".to_vec()),
            TupleValue::Text(b"1".to_vec()),
            TupleValue::Text(b"bio".to_vec()),
        ]);
        let change = build_row_change(&rel, Incoming::Update(Some(old), new), None, false).unwrap();
        match change.kind {
            RowKind::Update {
                before,
                previous_pk,
                ..
            } => {
                assert_eq!(before, None, "nothing about the old row is reliable");
                assert_eq!(previous_pk, Some(serde_json::json!(1)), "the key still is");
            }
            other => panic!("unexpected kind {other:?}"),
        }
    }

    #[test]
    fn a_full_key_tuple_is_the_delete_before_image() {
        let rel = users_relation(pgoutput::ReplicaIdentity::Full);
        let key = tuple(vec![
            TupleValue::Text(b"42".to_vec()),
            TupleValue::Text(b"a@x.io".to_vec()),
            TupleValue::Text(b"7".to_vec()),
            TupleValue::Null,
        ]);
        let change = build_row_change(&rel, Incoming::Delete(key), None, false).unwrap();
        match change.kind {
            RowKind::Delete { before, .. } => assert_eq!(
                before,
                Some(serde_json::json!({"id": 42, "email": "a@x.io", "score": "7", "bio": null}))
            ),
            other => panic!("unexpected kind {other:?}"),
        }
    }

    #[test]
    fn under_replica_identity_full_the_key_is_still_the_primary_key() {
        // pgoutput flags every column as identity under FULL; the catalogue's
        // key is what the load filed the row under, and what a document is
        let mut rel = users_relation(pgoutput::ReplicaIdentity::Full);
        for col in &mut rel.columns {
            col.in_replica_identity = true;
        }
        let row = tuple(vec![
            TupleValue::Text(b"42".to_vec()),
            TupleValue::Text(b"a@x.io".to_vec()),
            TupleValue::Text(b"7".to_vec()),
            TupleValue::Null,
        ]);
        let keys = vec!["id".to_string()];
        let change =
            build_row_change(&rel, Incoming::Insert(row.clone()), Some(&keys), false).unwrap();
        match change.kind {
            RowKind::Insert { pk, .. } => assert_eq!(pk, serde_json::json!(42)),
            other => panic!("unexpected kind {other:?}"),
        }
        // without a known key the flags are all there is
        let change = build_row_change(&rel, Incoming::Delete(row), None, false).unwrap();
        match change.kind {
            RowKind::Delete { pk, .. } => assert!(pk.is_object(), "the whole row: {pk}"),
            other => panic!("unexpected kind {other:?}"),
        }
    }

    #[test]
    fn an_append_only_insert_carries_no_key() {
        // the engine files the row under a hash of its content, so a key
        // here would only be something for the two paths to disagree about
        let mut rel = users_relation(pgoutput::ReplicaIdentity::Default);
        for col in &mut rel.columns {
            col.in_replica_identity = false;
        }
        let row = tuple(vec![
            TupleValue::Text(b"42".to_vec()),
            TupleValue::Text(b"a@x.io".to_vec()),
            TupleValue::Null,
            TupleValue::Null,
        ]);
        let change = build_row_change(&rel, Incoming::Insert(row), None, true).unwrap();
        match change.kind {
            RowKind::Insert { pk, doc } => {
                assert_eq!(pk, Value::Null);
                assert_eq!(doc["email"], serde_json::json!("a@x.io"));
            }
            other => panic!("unexpected kind {other:?}"),
        }
    }

    #[test]
    fn an_update_or_delete_on_an_append_only_table_is_refused() {
        let rel = users_relation(pgoutput::ReplicaIdentity::Full);
        let row = tuple(vec![
            TupleValue::Text(b"42".to_vec()),
            TupleValue::Text(b"a@x.io".to_vec()),
            TupleValue::Null,
            TupleValue::Null,
        ]);
        let err = build_row_change(&rel, Incoming::Update(None, row.clone()), None, true)
            .expect_err("no key, so no document to replace");
        assert_eq!(
            err.to_string(),
            "public.users: an UPDATE arrived on an append-only table; nothing can say \
             which document it is"
        );
        let err = build_row_change(&rel, Incoming::Delete(row), None, true)
            .expect_err("no key, so no document to remove");
        assert!(
            err.to_string()
                .contains("a DELETE arrived on an append-only table"),
            "{err}"
        );
    }

    #[test]
    fn a_default_identity_delete_carries_no_before_image() {
        let rel = users_relation(pgoutput::ReplicaIdentity::Default);
        let key = tuple(vec![
            TupleValue::Text(b"42".to_vec()),
            TupleValue::Null,
            TupleValue::Null,
            TupleValue::Null,
        ]);
        let change = build_row_change(&rel, Incoming::Delete(key), None, false).unwrap();
        match change.kind {
            RowKind::Delete { before, .. } => assert_eq!(before, None),
            other => panic!("unexpected kind {other:?}"),
        }
    }
}
