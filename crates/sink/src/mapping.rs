//! Comparing a configured index mapping against the one the target has.
//!
//! The target normalises what it is given — booleans for string flags, dropped
//! defaults, reordered keys — so an equality check on the two JSON documents
//! reports differences for a mapping that is in fact the one asked for. What
//! can be checked honestly is containment: every field named in the config
//! exists, and has the type the config says.

use serde_json::{Map, Value};

/// What a live index does not match about the configured mapping.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct MappingReport {
    /// Fields the config declares that the index does not have. Dynamic
    /// mapping will invent a type for these from whatever arrives first.
    pub missing: Vec<String>,
    /// Fields the index maps to a different type. Documents that disagree
    /// with a mapping are rejected, and a rejection stops the pipeline.
    pub conflicting: Vec<String>,
}

impl MappingReport {
    pub fn is_empty(&self) -> bool {
        self.missing.is_empty() && self.conflicting.is_empty()
    }
}

/// Turn what an operator wrote into an index-creation body.
///
/// Both spellings are accepted because both are what people have to hand: the
/// body of a `PUT /index` call copied from a console, or just the mapping's
/// properties. Anything carrying `mappings` or `settings` is taken as the
/// whole body.
pub fn create_body(configured: &Value) -> Value {
    match configured {
        Value::Object(map) if map.contains_key("mappings") || map.contains_key("settings") => {
            configured.clone()
        }
        other => serde_json::json!({ "mappings": other }),
    }
}

/// Compare a configured mapping against the one the index reports.
///
/// `live` is the mapping object as the target returns it — the value under the
/// index name, or the whole response for a target that returns it bare.
pub fn compare(configured: &Value, live: &Value) -> MappingReport {
    let desired = create_body(configured);
    let want = desired.get("mappings").and_then(Value::as_object);
    let have = live
        .get("mappings")
        .and_then(Value::as_object)
        .or_else(|| live.as_object());
    let (Some(want), Some(have)) = (want, have) else {
        return MappingReport::default();
    };
    let mut report = MappingReport::default();
    walk(want, have, "", &mut report);
    report
}

fn walk(want: &Map<String, Value>, have: &Map<String, Value>, path: &str, out: &mut MappingReport) {
    let Some(properties) = want.get("properties").and_then(Value::as_object) else {
        return;
    };
    let live_properties = have.get("properties").and_then(Value::as_object);
    for (name, spec) in properties {
        let field = if path.is_empty() {
            name.clone()
        } else {
            format!("{path}.{name}")
        };
        let Some(live) = live_properties
            .and_then(|p| p.get(name))
            .and_then(Value::as_object)
        else {
            out.missing.push(field);
            continue;
        };
        let Some(spec) = spec.as_object() else {
            continue;
        };
        // an object with properties and no type of its own is a container:
        // what matters about it is the leaves underneath
        match (spec.get("type"), live.get("type")) {
            (Some(wanted), Some(actual)) if wanted != actual => out.conflicting.push(format!(
                "{field} is {} but the index has {}",
                render(wanted),
                render(actual)
            )),
            // A join with the right type but the wrong relations is the worse
            // mismatch: every document is accepted, and none can be queried
            // as parent or child of the other.
            (Some(wanted), Some(_)) if wanted == "join" => {
                let want = relations(spec);
                let have = relations(live);
                if want != have {
                    out.conflicting.push(format!(
                        "{field} is a join with relations {} but the index has {}",
                        Value::Object(want),
                        Value::Object(have)
                    ));
                }
            }
            // a container maps to "object" implicitly, so a declared type where
            // the index has none is only a conflict if the index made it a leaf
            (Some(wanted), None) if !live.contains_key("properties") => {
                out.conflicting.push(format!(
                    "{field} is {} but the index has no type for it",
                    render(wanted)
                ))
            }
            _ => {}
        }
        walk(spec, live, &field, out);
    }
}

/// A join field's `relations` as the target holds them: a parent mapped to a
/// list of children, since a single child written as a string comes back
/// normalised into a one-element list.
fn relations(spec: &Map<String, Value>) -> Map<String, Value> {
    spec.get("relations")
        .and_then(Value::as_object)
        .map(|rel| {
            rel.iter()
                .map(|(parent, children)| {
                    let children = match children {
                        Value::String(one) => Value::Array(vec![Value::String(one.clone())]),
                        other => other.clone(),
                    };
                    (parent.clone(), children)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn render(v: &Value) -> String {
    v.as_str()
        .map(str::to_string)
        .unwrap_or_else(|| v.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn both_spellings_produce_the_same_creation_body() {
        let bare = json!({"properties": {"id": {"type": "long"}}});
        let full = json!({"mappings": {"properties": {"id": {"type": "long"}}}});
        assert_eq!(create_body(&bare), full);
        assert_eq!(create_body(&full), full, "already a body, left alone");
    }

    #[test]
    fn settings_alone_are_taken_as_a_whole_body() {
        let body = json!({"settings": {"number_of_shards": 3}});
        assert_eq!(create_body(&body), body);
    }

    #[test]
    fn a_matching_index_reports_nothing() {
        let want = json!({"properties": {"id": {"type": "long"}}});
        // the target fills in and reorders; containment is what can be checked
        let live = json!({"mappings": {"properties": {
            "id": {"type": "long"},
            "invented_by_dynamic_mapping": {"type": "text"}
        }}});
        assert!(compare(&want, &live).is_empty());
    }

    #[test]
    fn a_type_that_disagrees_is_named() {
        let want = json!({"properties": {"price": {"type": "double"}}});
        let live = json!({"mappings": {"properties": {"price": {"type": "text"}}}});
        let report = compare(&want, &live);
        assert!(
            report.conflicting[0].contains("price is double"),
            "{report:?}"
        );
        assert!(report.missing.is_empty());
    }

    #[test]
    fn a_field_the_index_does_not_have_is_named_separately() {
        let want = json!({"properties": {"vector": {"type": "knn_vector"}}});
        let live = json!({"mappings": {"properties": {}}});
        let report = compare(&want, &live);
        assert_eq!(report.missing, vec!["vector"]);
        assert!(report.conflicting.is_empty());
    }

    #[test]
    fn nested_fields_are_compared_by_path() {
        let want = json!({"properties": {"order": {"properties": {"total": {"type": "double"}}}}});
        let live = json!({"mappings": {"properties": {
            "order": {"properties": {"total": {"type": "keyword"}}}
        }}});
        let report = compare(&want, &live);
        assert!(
            report.conflicting[0].starts_with("order.total is double"),
            "{report:?}"
        );
    }

    #[test]
    fn a_container_without_a_declared_type_is_not_a_conflict() {
        let want = json!({"properties": {"order": {"properties": {"id": {"type": "long"}}}}});
        let live = json!({"mappings": {"properties": {
            "order": {"type": "object", "properties": {"id": {"type": "long"}}}
        }}});
        assert!(compare(&want, &live).is_empty());
    }

    #[test]
    fn a_join_whose_relations_disagree_is_a_conflict() {
        let want = json!({"properties": {"relation": {
            "type": "join", "relations": {"customer": ["order"]}
        }}});
        let live = json!({"mappings": {"properties": {"relation": {
            "type": "join", "relations": {"customer": ["invoice"]}
        }}}});
        let report = compare(&want, &live);
        assert!(report.missing.is_empty());
        assert_eq!(
            report.conflicting,
            vec![
                "relation is a join with relations {\"customer\":[\"order\"]} \
                 but the index has {\"customer\":[\"invoice\"]}"
            ]
        );
    }

    #[test]
    fn a_single_child_written_as_a_string_matches_its_normalised_form() {
        // the target turns `"customer": "order"` into `"customer": ["order"]`
        let want = json!({"properties": {"relation": {
            "type": "join", "relations": {"customer": "order"}
        }}});
        let live = json!({"mappings": {"properties": {"relation": {
            "type": "join", "eager_global_ordinals": true,
            "relations": {"customer": ["order"]}
        }}}});
        assert!(compare(&want, &live).is_empty());
    }

    #[test]
    fn an_index_with_no_mapping_at_all_reports_what_is_missing() {
        let want = json!({"properties": {"id": {"type": "long"}}});
        assert_eq!(compare(&want, &json!({})).missing, vec!["id"]);
    }
}
