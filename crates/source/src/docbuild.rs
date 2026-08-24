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
fn extract_pk(rel: &Relation, tuple: &Tuple) -> Result<Value, BuildError> {
    let mut obj = Map::new();
    let mut scalar: Option<Value> = None;
    for (idx, col) in rel.columns.iter().enumerate() {
        if !col.in_replica_identity {
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
}

pub fn build_row_change(rel: &Relation, incoming: Incoming) -> Result<RowChange, BuildError> {
    let change = |kind: RowKind| RowChange {
        schema: rel.schema.clone(),
        table: rel.name.clone(),
        kind,
    };
    // without a replica identity there is no key to address the row on
    // update/delete; inserts are unaffected
    if !matches!(incoming, Incoming::Insert(_)) {
        check_delete_capability(rel)?;
    }
    match incoming {
        Incoming::Insert(new) => {
            let pk = extract_pk(rel, &new)?;
            let (doc, _) = build_doc(rel, &new, None)?;
            Ok(change(RowKind::Insert { pk, doc }))
        }
        Incoming::Update(old, new) => {
            // The key must come from the new tuple: it says where the row lives
            // now. The old key addresses the document the row used to occupy,
            // which the engine has to remove when the two differ.
            let pk = extract_pk(rel, &new)?;
            let previous_pk = old.as_ref().and_then(|prev| extract_pk(rel, prev).ok());
            let old_slices = old.as_ref().map(tuple_slices);
            let (doc, toast) = build_doc(rel, &new, old_slices.as_deref())?;
            Ok(change(RowKind::Update {
                pk,
                previous_pk,
                doc,
                unchanged_toast_columns: toast,
            }))
        }
        Incoming::Delete(key) => {
            let pk = extract_pk(rel, &key)?;
            Ok(change(RowKind::Delete { pk }))
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
        let change = build_row_change(&rel, Incoming::Insert(t)).unwrap();
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
        let change = build_row_change(&rel, Incoming::Update(Some(old), new)).unwrap();
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
        let change = build_row_change(&rel, Incoming::Update(Some(old), new)).unwrap();
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
        let change = build_row_change(&rel, Incoming::Update(Some(old), new)).unwrap();
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
        let change = build_row_change(&rel, Incoming::Update(None, new)).unwrap();
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
        let change = build_row_change(&rel, Incoming::Delete(key)).unwrap();
        assert!(matches!(change.kind, RowKind::Delete { pk } if pk == serde_json::json!(42)));
    }

    #[test]
    fn replica_identity_nothing_is_rejected_upfront() {
        let rel = users_relation(pgoutput::ReplicaIdentity::None);
        let key = tuple(vec![TupleValue::Null; 4]);
        assert!(build_row_change(&rel, Incoming::Delete(key)).is_err());
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
        let change = build_row_change(&rel, Incoming::Delete(key)).unwrap();
        match change.kind {
            RowKind::Delete { pk } => assert_eq!(pk, serde_json::json!({"id": 42, "tenant_id": 7})),
            other => panic!("unexpected kind {other:?}"),
        }
    }
}
