//! Catalog inspection and management: wal_level checks, publication/slot
//! lifecycle, replica-identity and primary-key discovery.

use crate::error::{Context as _, Result, SourceError};
use pg2osync_core::Lsn;
use tokio_postgres::Client;

pub struct TableCatalogInfo {
    pub relreplident: char,
    pub pk_columns: Vec<String>,
}

/// A managed PostgreSQL, recognised so an error can name the switch that
/// service calls logical replication instead of the one `postgresql.conf` has.
///
/// Detection is fail-open on purpose: an unrecognised server gets the plain
/// message, never a guess about a service it is not running on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedService {
    Rds,
    Aurora,
    Supabase,
}

impl ManagedService {
    /// Aurora exposes the RDS parameter too, so it is decided first: naming
    /// the instance parameter group on a cluster sends the reader to a screen
    /// where the change has no effect.
    fn from_probe(rds_guc: bool, aurora_version: bool, supabase_admin: bool) -> Option<Self> {
        if aurora_version {
            Some(Self::Aurora)
        } else if rds_guc {
            Some(Self::Rds)
        } else if supabase_admin {
            Some(Self::Supabase)
        } else {
            None
        }
    }

    /// How this service turns logical decoding on.
    pub fn wal_level_remedy(self) -> &'static str {
        match self {
            Self::Rds => {
                "on Amazon RDS the server takes no postgresql.conf: set \
                 `rds.logical_replication = 1` in the instance's parameter group \
                 and reboot the instance"
            }
            Self::Aurora => {
                "on Aurora PostgreSQL set `rds.logical_replication = 1` in the DB \
                 *cluster* parameter group — the instance parameter group ignores \
                 it — and reboot the writer"
            }
            Self::Supabase => {
                "Supabase ships `wal_level = logical`, so a server reporting \
                 anything else has it overridden in the project's database settings"
            }
        }
    }

    /// How this service hands out the right to create a replication slot.
    pub fn replication_role_remedy(self) -> &'static str {
        match self {
            Self::Rds | Self::Aurora => {
                "a managed instance grants no REPLICATION attribute directly: \
                 `GRANT rds_replication TO <role>` as the master user instead"
            }
            Self::Supabase => {
                "on Supabase the built-in `postgres` role already has REPLICATION; \
                 connect as it, or have `supabase_admin` grant a role of your own"
            }
        }
    }
}

/// Which managed service this server is, if it is recognisably one.
///
/// One query over catalogs, and every failure — including a catalog a service
/// restricts — reads as "not recognised", so this can never turn a diagnosable
/// problem into a connection error.
pub async fn detect_managed_service(client: &Client) -> Option<ManagedService> {
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_settings WHERE name = 'rds.logical_replication'),
                    to_regprocedure('aurora_version()') IS NOT NULL,
                    EXISTS(SELECT 1 FROM pg_roles WHERE rolname = 'supabase_admin')",
            &[],
        )
        .await
        .ok()?;
    ManagedService::from_probe(row.get(0), row.get(1), row.get(2))
}

pub async fn check_wal_level(client: &Client) -> Result<()> {
    let level: String = client
        .query_one(
            "SELECT setting FROM pg_settings WHERE name = 'wal_level'",
            &[],
        )
        .await
        .catalog_ctx(|| "cannot read wal_level".into())?
        .get(0);
    if level != "logical" {
        let service = match detect_managed_service(client).await {
            Some(service) => format!("; {}", service.wal_level_remedy()),
            None => String::new(),
        };
        return Err(SourceError::Config(format!(
            "wal_level is '{level}' but must be 'logical'; \
             set `wal_level = logical` in postgresql.conf and restart PostgreSQL{service}"
        )));
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
        .catalog_ctx(|| format!("table {schema}.{name} not found or not inspectable"))?;
    let repl: String = row.get::<_, String>(0);
    Ok(TableCatalogInfo {
        // pg_class.relreplident arrives as a 1-byte "char"; via text protocol
        // it surfaces as a single-character string
        relreplident: repl.chars().next().unwrap_or('d'),
        pk_columns: row.get(1),
    })
}

/// What the current role can and cannot do, so `validate` can report a
/// privilege problem instead of letting `run` fail halfway through bootstrap.
#[derive(Debug, Clone)]
pub struct Preflight {
    pub publication_exists: bool,
    pub slot_exists: bool,
    /// `REPLICATION` attribute or superuser: required to create a slot and to
    /// open a replication connection at all.
    pub can_replicate: bool,
    /// `CREATE` on the database: required to create a publication.
    pub can_create_in_database: bool,
    /// Tables the role does not own. PostgreSQL requires ownership to publish
    /// a table, and grants cannot substitute for it.
    pub tables_not_owned: Vec<String>,
    /// Tables the role cannot read, which breaks the initial load.
    pub tables_not_readable: Vec<String>,
    /// The managed service this server is, when it is recognisably one, so a
    /// refusal can name the switch that service actually has.
    pub service: Option<ManagedService>,
}

impl Preflight {
    /// Whether `bootstrap` can create what is still missing.
    pub fn can_bootstrap(&self) -> bool {
        (self.publication_exists
            || (self.can_create_in_database && self.tables_not_owned.is_empty()))
            && (self.slot_exists || self.can_replicate)
    }

    /// DDL a privileged role can run so the sync role only has to consume.
    pub fn setup_sql(&self, publication: &str, slot: &str, tables: &[String]) -> Vec<String> {
        let mut sql = Vec::new();
        if !self.publication_exists {
            sql.push(format!(
                "CREATE PUBLICATION {publication} FOR TABLE {} \
                 WITH (publish_via_partition_root = true);",
                tables.join(", ")
            ));
        }
        if !self.slot_exists {
            sql.push(format!(
                "SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput');"
            ));
        }
        sql
    }
}

/// The whole script a DBA needs, derived from the config rather than copied
/// out of documentation.
///
/// Printed rather than executed: every statement here needs privileges the
/// pipeline's own role should not have, and a script someone can read before
/// running is the difference between a tool that works and one that is
/// pleasant to adopt.
pub fn setup_script(user: &str, tables: &[String], publication: &str, slot: &str) -> String {
    let mut out = String::new();
    let schemas: Vec<&str> = {
        let mut seen: Vec<&str> = tables
            .iter()
            .filter_map(|t| t.split_once('.').map(|(s, _)| s))
            .collect();
        seen.sort_unstable();
        seen.dedup();
        seen
    };

    out.push_str("-- pg2osync source setup. Review before running.\n");
    out.push_str("-- Run as a superuser or the owner of the tables below.\n\n");

    out.push_str("-- 1. Logical decoding. This one needs a RESTART, not a reload,\n");
    out.push_str("--    and it is the step people discover last.\n");
    out.push_str("--    Check first:  SHOW wal_level;\n");
    out.push_str("ALTER SYSTEM SET wal_level = 'logical';\n");
    out.push_str("-- then restart PostgreSQL.\n\n");

    out.push_str("-- 2. A role for the pipeline. REPLICATION is what lets it open\n");
    out.push_str("--    the stream; it does not grant any data access on its own.\n");
    out.push_str(&format!(
        "CREATE ROLE {} WITH LOGIN REPLICATION PASSWORD 'change-me';\n\n",
        quote_ident(user)
    ));

    out.push_str("-- 3. Read access. The initial load reads every synced table.\n");
    for schema in &schemas {
        out.push_str(&format!(
            "GRANT USAGE ON SCHEMA {} TO {};\n",
            quote_ident(schema),
            quote_ident(user)
        ));
    }
    for table in tables {
        out.push_str(&format!(
            "GRANT SELECT ON {table} TO {};\n",
            quote_ident(user)
        ));
    }

    out.push_str("\n-- 4. The publication. Publishing a table requires owning it,\n");
    out.push_str("--    which is why this cannot be done by the pipeline's role.\n");
    out.push_str(&format!(
        "CREATE PUBLICATION {publication} FOR TABLE {} \
         WITH (publish_via_partition_root = true);\n\n",
        tables.join(", ")
    ));

    out.push_str("-- 5. The slot. pg2osync creates this itself on first run;\n");
    out.push_str("--    create it here if the pipeline's role may not.\n");
    out.push_str(&format!(
        "-- SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput');\n\n"
    ));

    out.push_str("-- 6. Cap what an unread slot can retain, so a stopped pipeline\n");
    out.push_str("--    cannot fill this disk. Past the limit the slot is\n");
    out.push_str("--    invalidated and pg2osync falls back to a full load.\n");
    out.push_str("ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';\n");
    out.push_str("SELECT pg_reload_conf();\n");
    out
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Inspect the current role's privileges against the configured objects.
pub async fn preflight(
    client: &Client,
    publication: &str,
    slot_name: &str,
    qualified_tables: &[String],
) -> Result<Preflight> {
    let row = client
        .query_one(
            "SELECT (r.rolreplication OR r.rolsuper) AS can_replicate,
                    has_database_privilege(current_database(), 'CREATE') AS can_create,
                    EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1) AS pub_exists,
                    EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $2) AS slot_exists,
                    EXISTS(SELECT 1 FROM pg_settings WHERE name = 'rds.logical_replication') AS rds,
                    to_regprocedure('aurora_version()') IS NOT NULL AS aurora,
                    EXISTS(SELECT 1 FROM pg_roles WHERE rolname = 'supabase_admin') AS supabase
             FROM pg_roles r WHERE r.rolname = current_user",
            &[&publication, &slot_name],
        )
        .await
        .catalog_ctx(|| "cannot inspect the current role's privileges".into())?;

    let mut tables_not_owned = Vec::new();
    let mut tables_not_readable = Vec::new();
    for table in qualified_tables {
        // pg_has_role covers membership in the owning role, which is how most
        // deployments grant ownership in practice
        let checks = client
            .query_one(
                "SELECT pg_has_role(c.relowner, 'USAGE') AS owns,
                        has_table_privilege(c.oid, 'SELECT') AS reads
                 FROM pg_class c WHERE c.oid = ($1::text)::regclass",
                &[&table],
            )
            .await
            .catalog_ctx(|| format!("cannot inspect table {table}"))?;
        if !checks.get::<_, bool>("owns") {
            tables_not_owned.push(table.clone());
        }
        if !checks.get::<_, bool>("reads") {
            tables_not_readable.push(table.clone());
        }
    }

    Ok(Preflight {
        publication_exists: row.get("pub_exists"),
        slot_exists: row.get("slot_exists"),
        can_replicate: row.get("can_replicate"),
        can_create_in_database: row.get("can_create"),
        tables_not_owned,
        tables_not_readable,
        service: ManagedService::from_probe(row.get("rds"), row.get("aurora"), row.get("supabase")),
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
        .await
        .catalog_ctx(|| format!("cannot read publication {publication}"))?
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
                .catalog_ctx(|| format!("CREATE PUBLICATION {publication} failed"))?;
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
                .await
                .catalog_ctx(|| format!("cannot read the tables of publication {publication}"))?;
            let mut current = std::collections::BTreeSet::new();
            for r in rows {
                let s: String = r.get(0);
                let t: String = r.get(1);
                current.insert(format!("{s}.{t}"));
            }
            if current != configured {
                return Err(SourceError::Config(format!(
                    "publication {publication} covers {current:?} but config wants {configured:?}; \
                     drop and recreate it or align the config (drift is never auto-applied)"
                )));
            }
            tracing::debug!(target: "pg2osync::catalog", "publication {publication} matches config");
        }
    }
    Ok(())
}

/// One replication slot as the server sees it.
#[derive(Debug, Clone)]
pub struct SlotInfo {
    pub name: String,
    pub active: bool,
    /// WAL the slot is holding back, in bytes. `None` before the slot's first
    /// use, when it has no confirmed position yet.
    pub retained_bytes: Option<i64>,
    /// The server's own verdict: `reserved`, `extended`, `unreserved` or
    /// `lost`. A lost slot cannot be resumed from at all, which is worth
    /// naming rather than leaving to be inferred from a large number.
    pub wal_status: String,
}

impl SlotInfo {
    pub fn retained_pretty(&self) -> String {
        match self.retained_bytes {
            None => "unknown".into(),
            Some(bytes) => {
                let units = ["B", "kB", "MB", "GB", "TB"];
                let mut value = bytes as f64;
                let mut unit = 0;
                while value >= 1024.0 && unit < units.len() - 1 {
                    value /= 1024.0;
                    unit += 1;
                }
                format!("{value:.0} {}", units[unit])
            }
        }
    }
}

/// Every logical slot on the server, largest retention first.
///
/// Reporting only the configured slot is what let ten forgotten ones pile up
/// here: each was created by a run with a different `slot_name`, and nothing
/// that anybody looked at ever mentioned them again.
pub async fn all_slots(client: &Client) -> Result<Vec<SlotInfo>> {
    let rows = client
        .query(
            "SELECT slot_name, active, \
                    pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint, \
                    wal_status \
             FROM pg_replication_slots \
             WHERE slot_type = 'logical' \
             ORDER BY 3 DESC NULLS LAST, 1",
            &[],
        )
        .await
        .catalog_ctx(|| "listing replication slots failed".into())?;
    Ok(rows
        .iter()
        .map(|r| SlotInfo {
            name: r.get(0),
            active: r.get(1),
            retained_bytes: r.get(2),
            wal_status: r.get::<_, Option<String>>(3).unwrap_or_default(),
        })
        .collect())
}

/// Say something about slots nobody is reading.
///
/// An inactive slot pins WAL forever at no cost to us and unbounded cost to the
/// database's disk. Dropping one automatically would be wrong — it may belong
/// to another consumer — so the most this can do is make it impossible to miss.
pub async fn warn_about_idle_slots(client: &Client, configured: &str) {
    let slots = match all_slots(client).await {
        Ok(slots) => slots,
        // this is advice, never a reason to fail a run
        Err(e) => {
            tracing::debug!(target: "pg2osync::catalog", "cannot list slots: {e}");
            return;
        }
    };
    for slot in slots.iter().filter(|s| !s.active) {
        let retained = slot.retained_pretty();
        if slot.name == configured {
            if slot.retained_bytes.unwrap_or(0) >= IDLE_SLOT_WARN_BYTES {
                tracing::warn!(target: "pg2osync::catalog",
                    "slot {} was idle and is holding {retained} of WAL; \
                     that is the state that precedes a full disk", slot.name);
            }
            continue;
        }
        tracing::warn!(target: "pg2osync::catalog",
            "replication slot {} is inactive and holding {retained} of WAL. \
             If it is a former slot_name of this pipeline, drop it: \
             SELECT pg_drop_replication_slot('{}');", slot.name, slot.name);
    }
}

/// Big enough that a healthy restart does not trip it, small enough to arrive
/// well before the disk does.
const IDLE_SLOT_WARN_BYTES: i64 = 1024 * 1024 * 1024;

/// Ensure the logical replication slot exists; returns its confirmed_flush_lsn.
pub async fn ensure_slot(client: &Client, slot_name: &str) -> Result<Option<String>> {
    match client
        .query_opt(
            "SELECT plugin FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .catalog_ctx(|| format!("cannot read replication slot {slot_name}"))?
    {
        Some(row) => {
            let plugin: String = row.get(0);
            if plugin != "pgoutput" {
                return Err(SourceError::Config(format!(
                    "slot {slot_name} uses plugin '{plugin}', expected pgoutput"
                )));
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
                .catalog_ctx(|| format!("creating slot {slot_name} failed"))?;
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
        .await
        .catalog_ctx(|| format!("cannot read replication slot {slot_name}"))?;
    if exists.is_none() {
        tracing::info!(target: "pg2osync::catalog", "slot {slot_name} already absent");
        return Ok(());
    }
    client
        .execute("SELECT pg_drop_replication_slot($1)", &[&slot_name])
        .await
        .catalog_ctx(|| format!("dropping slot {slot_name} failed"))?;
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
        .catalog_ctx(|| format!("dropping publication {publication} failed"))?;
    tracing::info!(target: "pg2osync::catalog", "dropped publication {publication}");
    Ok(())
}

/// How much room a slot has before the server starts discarding WAL it still
/// needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotPressure {
    /// `reserved`, `extended`, `unreserved` or `lost`. Anything past
    /// `extended` means the slot is living on borrowed WAL.
    pub wal_status: String,
    /// Bytes that may still be written before this slot risks invalidation.
    /// `None` when `max_slot_wal_keep_size = -1`, where there is no limit to
    /// approach — and no protection either.
    pub safe_wal_size: Option<i64>,
    /// WAL the slot is holding back, which is the number to watch when there is
    /// no limit to measure against.
    pub retained_bytes: Option<i64>,
}

impl SlotPressure {
    /// The slot has been invalidated: it can no longer be used to stream, and
    /// nothing recovers it.
    pub fn lost(&self) -> bool {
        self.wal_status == "lost"
    }

    /// The slot is past the WAL the server promised to keep, so every further
    /// byte written brings invalidation closer.
    ///
    /// This is PostgreSQL's own answer rather than a threshold of ours:
    /// `reserved` means inside `max_slot_wal_keep_size`, anything else means
    /// beyond it. With no limit configured the status stays `reserved`, which
    /// is honest — there is no line to stay behind, and no protection either.
    pub fn straining(&self) -> bool {
        self.wal_status != "reserved"
    }
}

/// Read one slot's pressure. Cheap on purpose: `pg_get_replication_slots` is a
/// shared-memory read under one LWLock, not a catalog scan, so it can be polled
/// as often as a load needs to.
pub async fn slot_pressure(client: &Client, slot_name: &str) -> Result<Option<SlotPressure>> {
    let row = client
        .query_opt(
            "SELECT wal_status, safe_wal_size::bigint,                     pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint              FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .catalog_ctx(|| "cannot read replication slot pressure".into())?;
    Ok(row.map(|r| SlotPressure {
        // restart_lsn, not confirmed_flush_lsn: the former is what actually
        // pins WAL, and the two can be far apart during a load
        wal_status: r.get::<_, Option<String>>(0).unwrap_or_default(),
        safe_wal_size: r.get(1),
        retained_bytes: r.get(2),
    }))
}

/// Every logical slot on the server, with what it is holding.
///
/// All of them and not only ours: an abandoned slot from a former `slot_name`
/// pins WAL just as effectively, and it is invisible to anyone who only asks
/// about the name in the config. Reporting it is the difference between an
/// alert that fires and an outage nobody saw coming.
pub async fn all_slot_pressure(client: &Client) -> Result<Vec<(String, bool, SlotPressure)>> {
    let rows = client
        .query(
            "SELECT slot_name, active, wal_status, safe_wal_size::bigint, \
                    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint \
             FROM pg_replication_slots WHERE slot_type = 'logical' ORDER BY slot_name",
            &[],
        )
        .await
        .catalog_ctx(|| "cannot read replication slots".into())?;
    Ok(rows
        .iter()
        .map(|r| {
            (
                r.get::<_, String>(0),
                r.get::<_, bool>(1),
                SlotPressure {
                    wal_status: r.get::<_, Option<String>>(2).unwrap_or_default(),
                    safe_wal_size: r.get(3),
                    retained_bytes: r.get(4),
                },
            )
        })
        .collect())
}

/// Current confirmed_flush_lsn of a slot, if it exists.
pub async fn confirmed_flush_lsn(client: &Client, slot_name: &str) -> Result<Option<Lsn>> {
    let row = client
        .query_opt(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .catalog_ctx(|| format!("cannot read replication slot {slot_name}"))?;
    // a freshly created slot has a NULL confirmed_flush_lsn until first use
    match row.and_then(|r| r.get::<_, Option<String>>(0)) {
        Some(text) => Ok(Some(text.parse().map_err(pg2osync_core::CoreError::from)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_setup_script_covers_what_the_pipeline_cannot_do_itself() {
        let script = setup_script(
            "svc",
            &["public.users".into(), "shop.orders".into()],
            "pg2osync_pub",
            "pg2osync_slot",
        );
        assert!(script.contains("wal_level = 'logical'"), "{script}");
        assert!(script.contains("RESTART"), "the step people find last");
        assert!(script.contains("CREATE ROLE \"svc\" WITH LOGIN REPLICATION"));
        assert!(script.contains("GRANT SELECT ON public.users TO \"svc\""));
        assert!(script.contains("CREATE PUBLICATION pg2osync_pub FOR TABLE"));
        assert!(script.contains("max_slot_wal_keep_size"));
    }

    #[test]
    fn every_schema_is_granted_once() {
        let script = setup_script(
            "svc",
            &["public.a".into(), "public.b".into(), "shop.c".into()],
            "p",
            "s",
        );
        assert_eq!(
            script.matches("GRANT USAGE ON SCHEMA \"public\"").count(),
            1
        );
        assert_eq!(script.matches("GRANT USAGE ON SCHEMA \"shop\"").count(), 1);
    }

    #[test]
    fn a_role_name_cannot_break_out_of_its_quoting() {
        let script = setup_script("we\"ird", &["public.a".into()], "p", "s");
        assert!(script.contains("\"we\"\"ird\""), "{script}");
    }

    use super::*;

    #[test]
    fn aurora_is_recognised_before_the_rds_parameter_it_also_has() {
        assert_eq!(
            ManagedService::from_probe(true, true, false),
            Some(ManagedService::Aurora)
        );
        assert!(
            ManagedService::Aurora
                .wal_level_remedy()
                .contains("cluster")
        );
    }

    #[test]
    fn each_service_names_its_own_switch() {
        assert_eq!(
            ManagedService::from_probe(true, false, false),
            Some(ManagedService::Rds)
        );
        assert_eq!(
            ManagedService::from_probe(false, false, true),
            Some(ManagedService::Supabase)
        );
        assert!(
            ManagedService::Rds
                .wal_level_remedy()
                .contains("rds.logical_replication")
        );
        assert!(
            ManagedService::Rds
                .replication_role_remedy()
                .contains("rds_replication")
        );
        assert!(
            ManagedService::Supabase
                .replication_role_remedy()
                .contains("postgres")
        );
    }

    #[test]
    fn an_unrecognised_server_keeps_the_plain_message() {
        assert_eq!(ManagedService::from_probe(false, false, false), None);
    }

    #[test]
    fn retention_is_reported_in_units_an_operator_reads() {
        let slot = |bytes| SlotInfo {
            name: "s".into(),
            active: false,
            retained_bytes: bytes,
            wal_status: "reserved".into(),
        };
        assert_eq!(slot(Some(512)).retained_pretty(), "512 B");
        assert_eq!(slot(Some(3 * 1024 * 1024 * 1024)).retained_pretty(), "3 GB");
        // a slot created but never read has no confirmed position yet
        assert_eq!(slot(None).retained_pretty(), "unknown");
    }
}
