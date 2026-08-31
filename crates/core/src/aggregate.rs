//! What an aggregate over a child table is, independent of where it is read
//! from.
//!
//! An aggregate is one more shape of child: `[[sync.x.children]]` embeds a
//! child table's rows, `[[sync.x.aggregates]]` embeds a single number derived
//! from them. Everything around it is the children machinery — the child table
//! is watched, a changed row names the parents to refresh, and the read is one
//! grouped query per aggregate per transaction.
//!
//! What an aggregate *is* lives here rather than in either source crate, for
//! the reason the child spec does: two readers must not be able to answer
//! differently what field a number lands under, or what a parent with no
//! matching rows gets.

use crate::filter::Filter;
use serde_json::Value;

/// One configured `[[sync.x.aggregates]]` entry, fully qualified.
#[derive(Debug, Clone)]
pub struct AggregateSpec {
    pub schema: String,
    pub table: String,
    /// Field on the parent document the number lands under.
    pub field: String,
    /// Column on the aggregated table holding the parent's key.
    pub foreign_key: String,
    /// Parent column the foreign key references — the parent's key.
    pub parent_column: String,
    /// Which rows of the table count, from the same restricted predicate
    /// `[sync.x] where` takes. Rendered by `Filter::to_sql` for both dialects,
    /// so an aggregate is not a second place where SQL is written.
    pub filter: Option<Filter>,
}

impl AggregateSpec {
    pub fn new(
        qualified: &str,
        field: &str,
        foreign_key: &str,
        parent_column: &str,
    ) -> Result<Self, crate::error::CoreError> {
        let (schema, table) = qualified.split_once('.').ok_or_else(|| {
            crate::error::CoreError::Other(format!(
                "aggregate table {qualified:?} must be schema-qualified"
            ))
        })?;
        Ok(Self {
            schema: schema.into(),
            table: table.into(),
            field: field.into(),
            foreign_key: foreign_key.into(),
            parent_column: parent_column.into(),
            filter: None,
        })
    }

    pub fn qualified(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    /// Whether this aggregate counts rows of that table.
    pub fn reads(&self, schema: &str, table: &str) -> bool {
        self.schema == schema && self.table == table
    }
}

/// Put one aggregate's number on a parent document.
///
/// A parent no row matched is zero rather than absent: `open_deals = 0` has to
/// find the parents that have none, and a missing field would make that query
/// impossible to write.
pub fn apply_count(doc: &mut Value, spec: &AggregateSpec, count: i64) {
    if let Value::Object(map) = doc {
        map.insert(spec.field.clone(), Value::from(count));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> AggregateSpec {
        AggregateSpec::new("public.deals", "open_deals", "contact_id", "id").expect("qualified")
    }

    #[test]
    fn aggregate_tables_are_schema_qualified() {
        assert!(AggregateSpec::new("deals", "n", "contact_id", "id").is_err());
        let spec = spec();
        assert_eq!(spec.qualified(), "public.deals");
        assert!(spec.reads("public", "deals"));
        assert!(!spec.reads("public", "contacts"));
    }

    #[test]
    fn a_parent_nothing_matched_carries_a_zero() {
        let mut doc = json!({"id": 1});
        apply_count(&mut doc, &spec(), 0);
        assert_eq!(doc["open_deals"], json!(0));
        assert!(
            doc.as_object().expect("object").contains_key("open_deals"),
            "the field is present either way, or `open_deals = 0` finds nothing"
        );
    }

    #[test]
    fn the_number_lands_under_the_configured_field() {
        let mut doc = json!({"id": 1, "name": "acme"});
        apply_count(&mut doc, &spec(), 7);
        assert_eq!(doc["open_deals"], json!(7));
        assert_eq!(doc["name"], json!("acme"), "nothing else moves");
    }
}
