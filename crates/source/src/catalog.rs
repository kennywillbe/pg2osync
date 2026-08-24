//! Catalog inspection and management: wal_level checks, publication/slot
//! lifecycle, replica-identity and primary-key discovery.

use anyhow::{Context, Result, bail};
use pg2osync_core::Lsn;
use tokio_postgres::Client;

pub struct TableCatalogInfo {
    pub relreplident: char,
    pub pk_columns: Vec<String>,
}

pub async fn check_wal_level(client: &Client) -> Result<()> {
    let level: String = client
        .query_one(
            "SELECT setting FROM pg_settings WHERE name = 'wal_level'",
            &[],
        )
        .await?
        .get(0);
    if level != "logical" {
        bail!(
            "wal_level is '{level}' but must be 'logical'; \
             set `wal_level = logical` in postgresql.conf and restart PostgreSQL"
        );
    }
    Ok(())
}

/// Fetch replica identity mode and primary-key columns for one table.
pub async fn table_info(client: &Client, schema: &str, name: &str) -> Result<TableCatalogInfo> {
    let row = client
        .query_one(
            r#"
            SELECT c.relreplident::text,
                   COALESCE((
                       SELECT array_agg(a.attname ORDER BY k.ord)
                       FROM pg_index i
                       JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
                           ON true
                       JOIN pg_attribute a
                           ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                       WHERE i.indrelid = c.oid AND i.indisprimary
                   ), '{}')
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND c.relname = $2
            "#,
            &[&schema, &name],
        )
        .await
        .with_context(|| format!("table {schema}.{name} not found or not inspectable"))?;
    let repl: String = row.get::<_, String>(0);
    Ok(TableCatalogInfo {
        // pg_class.relreplident arrives as a 1-byte "char"; via text protocol
        // it surfaces as a single-character string
        relreplident: repl.chars().next().unwrap_or('d'),
        pk_columns: row.get(1),
    })
}

/// Create the publication covering exactly the configured tables if missing;
/// detect drift when it exists.
///
/// `publish_via_partition_root` makes partitioned tables emit events under the
/// parent relid so they match config entries.
pub async fn ensure_publication(
    client: &Client,
    publication: &str,
    qualified_tables: &[String],
) -> Result<()> {
    let existing: Option<String> = client
        .query_opt(
            "SELECT pubname FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await?
        .map(|r| r.get(0));

    match existing {
        None => {
            let table_list = qualified_tables.join(", ");
            client
                .execute(
                    &format!(
                        "CREATE PUBLICATION {publication} FOR TABLE {table_list} \
                         WITH (publish_via_partition_root = true)"
                    ),
                    &[],
                )
                .await
                .with_context(|| format!("CREATE PUBLICATION {publication} failed"))?;
            tracing::info!(target: "pg2osync::catalog", "created publication {publication}");
        }
        Some(_) => {
            let configured: std::collections::BTreeSet<String> =
                qualified_tables.iter().cloned().collect();
            let rows = client
                .query(
                    "SELECT schemaname, tablename FROM pg_publication_tables \
                     WHERE pubname = $1",
                    &[&publication],
                )
                .await?;
            let mut current = std::collections::BTreeSet::new();
            for r in rows {
                let s: String = r.get(0);
                let t: String = r.get(1);
                current.insert(format!("{s}.{t}"));
            }
            if current != configured {
                return Err(anyhow::anyhow!(
                    "publication {publication} covers {current:?} but config wants {configured:?}; \
                     drop and recreate it or align the config (drift is never auto-applied)"
                ));
            }
            tracing::debug!(target: "pg2osync::catalog", "publication {publication} matches config");
        }
    }
    Ok(())
}

/// Ensure the logical replication slot exists; returns its confirmed_flush_lsn.
pub async fn ensure_slot(client: &Client, slot_name: &str) -> Result<Option<String>> {
    match client
        .query_opt(
            "SELECT plugin FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await?
    {
        Some(row) => {
            let plugin: String = row.get(0);
            if plugin != "pgoutput" {
                bail!("slot {slot_name} uses plugin '{plugin}', expected pgoutput");
            }
            Ok(None)
        }
        None => {
            client
                .execute(
                    "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                    &[&slot_name],
                )
                .await
                .with_context(|| format!("creating slot {slot_name} failed"))?;
            tracing::info!(target: "pg2osync::catalog", "created replication slot {slot_name}");
            Ok(None)
        }
    }
}

pub async fn drop_slot(client: &Client, slot_name: &str) -> Result<()> {
    let exists = client
        .query_opt(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await?;
    if exists.is_none() {
        tracing::info!(target: "pg2osync::catalog", "slot {slot_name} already absent");
        return Ok(());
    }
    client
        .execute("SELECT pg_drop_replication_slot($1)", &[&slot_name])
        .await
        .with_context(|| format!("dropping slot {slot_name} failed"))?;
    tracing::info!(target: "pg2osync::catalog", "dropped replication slot {slot_name}");
    Ok(())
}

/// Drop the publication if it exists.
///
/// Teardown must be idempotent: a half-finished decommission should be safe to
/// re-run, so a missing publication is success, not an error.
pub async fn drop_publication(client: &Client, publication: &str) -> Result<()> {
    client
        .execute(&format!("DROP PUBLICATION IF EXISTS {publication}"), &[])
        .await
        .with_context(|| format!("dropping publication {publication} failed"))?;
    tracing::info!(target: "pg2osync::catalog", "dropped publication {publication}");
    Ok(())
}

/// Current confirmed_flush_lsn of a slot, if it exists.
pub async fn confirmed_flush_lsn(client: &Client, slot_name: &str) -> Result<Option<Lsn>> {
    let row = client
        .query_opt(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await?;
    // a freshly created slot has a NULL confirmed_flush_lsn until first use
    match row.and_then(|r| r.get::<_, Option<String>>(0)) {
        Some(text) => Ok(Some(text.parse()?)),
        None => Ok(None),
    }
}
