//! Rebuild one table's index under a fresh name, then move an alias onto it.
//!
//! Changing a mapping means a new index, so the three steps a rebuild took
//! until now — edit the config to a new index name, `resnapshot`, `switch-alias`
//! — are one command here. The new index is `<index>-<unix seconds>`, filled by
//! the same chunked reader the initial load uses, counted against the source,
//! and given the alias in one atomic request. On a target with no alias
//! namespace the last step is a swap of the two names instead, so `--alias` has
//! to be the index the section already writes to and the fresh name is left
//! holding the documents from before the rebuild.
//!
//! What it will not do is run beside a live stream, and that is the one place it
//! parts company with a re-snapshot. A re-snapshot is safe beside the stream
//! because a copied row and a streamed change meet in the *same* index and the
//! higher position wins. In a fresh index the stream is not writing to there is
//! nothing to compare against: a row updated during the load would be wrong
//! there for good, and the count would still add up. So a rebuild closes the
//! window the way the initial load does — its rows carry position `0`, the
//! checkpoint does not move, and restarting the pipeline against the new index
//! replays everything committed since.

use anyhow::{Context as _, Result, bail};
use pg2osync_core::load::LoadScope;
use pg2osync_core::sink::IndexSpec;

use crate::config::AppConfig;
use crate::{resnapshot, run};

/// The index a rebuild writes into, derived from the one in the config.
///
/// A trailing `-<unix seconds>` is replaced rather than appended, so rebuilding
/// `users-1756512345` gives `users-<now>` and not a name that grows a timestamp
/// per rebuild. Nine digits is every second from 2001 onwards, which is far
/// enough from a name that merely ends in a number to tell the two apart.
pub(crate) fn fresh_index_name(base: &str, now: u64) -> String {
    let stem = base
        .rsplit_once('-')
        .filter(|(head, ts)| {
            !head.is_empty() && ts.len() >= 9 && ts.chars().all(|c| c.is_ascii_digit())
        })
        .map_or(base, |(head, _)| head);
    format!("{stem}-{now}")
}

/// What the source said before the load, and what it said after.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// The source did not move and the index holds exactly what it holds.
    Exact,
    /// The source moved under the load, and the document count is inside what
    /// it moved through. The restart's replay closes the rest.
    WithinDrift,
    /// A count no reading of the source explains; the alias stays where it is.
    Mismatch,
}

/// Whether `documents` is explained by a source that went from `before` to
/// `after` while the load ran.
///
/// An append-only table is only ever bounded from above: rows a source cannot
/// tell apart are one document under one content hash, so fewer documents than
/// rows is the declaration working, not a loss.
pub(crate) fn verdict(before: u64, after: u64, documents: u64, append_only: bool) -> Verdict {
    let low = before.min(after);
    let high = before.max(after);
    if append_only {
        return match (documents <= high, documents == before && before == after) {
            (false, _) => Verdict::Mismatch,
            (true, true) => Verdict::Exact,
            (true, false) => Verdict::WithinDrift,
        };
    }
    if before == after && documents == before {
        Verdict::Exact
    } else if (low..=high).contains(&documents) {
        Verdict::WithinDrift
    } else {
        Verdict::Mismatch
    }
}

/// Rebuild the index `qualified_table` is mapped to, and point `alias` at it.
pub async fn run_for(
    cfg: &AppConfig,
    source_url: &str,
    admin_url: &str,
    target_password: Option<String>,
    qualified_table: &str,
    alias: &str,
    drop_old: bool,
) -> Result<()> {
    let Some((key, tbl)) = cfg.sync.iter().find(|(_, t)| t.table == qualified_table) else {
        bail!(
            "{qualified_table} is not in this config; a rebuild fills a fresh copy of the \
             index the table is mapped to, so there is nothing to rebuild. Configured: {}",
            cfg.sync
                .values()
                .map(|t| t.table.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    };
    let (key, tbl) = (key.clone(), tbl.clone());
    let index = tbl.index_name(&key);
    refuse_unsupported(cfg, &key, &tbl, alias)?;
    let swapped = alias_is_the_index(cfg);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let fresh = fresh_index_name(&index, now);
    pg2osync_engine::mapping::check_index_name(&fresh).map_err(|e| {
        anyhow::anyhow!("[sync.{key}] the rebuilt index would be {fresh:?}, which {e}")
    })?;

    let sink = run::build_sink(cfg, target_password)?;
    refuse_a_live_stream(cfg, source_url, sink.as_ref()).await?;
    if sink.index_exists(&fresh).await? {
        bail!(
            "{fresh} already exists; a rebuild will not write into an index it did not create. \
             Delete it, or wait a second and run this again"
        );
    }

    let before = source_count(cfg, source_url, &tbl).await?;

    // Everything downstream — the engine's table map, the index specs, the
    // refresh — is derived from the config, so pointing the load at another
    // index is one field and no second code path.
    let mut rebuilt = cfg.clone();
    match rebuilt.sync.get_mut(&key) {
        Some(section) => section.index = Some(fresh.clone()),
        // `key` came out of this very map a moment ago, so the miss cannot
        // happen; saying so is still cheaper than an unwrap
        None => bail!("[sync.{key}] is not in this config"),
    }

    let specs: Vec<IndexSpec> = run::index_specs(&rebuilt)?
        .into_iter()
        .filter(|spec| spec.name == fresh)
        .collect();
    sink.ensure_ready(&specs).await?;

    println!("rebuilding {index} as {fresh} from {qualified_table} ({before} row(s))");
    let started = std::time::Instant::now();
    // Unlike a re-snapshot's, refresh is suspended for the duration: no alias
    // points at this index yet, so there is no search to keep serving and
    // nothing to trade away.
    let saved = sink.begin_bulk_load(std::slice::from_ref(&fresh)).await?;
    let scope = LoadScope::resnapshot(qualified_table, None)
        .with_table_filters(run::table_filters(&rebuilt)?);
    let read = resnapshot::load_one(&rebuilt, source_url, admin_url, sink.clone(), &scope).await;
    sink.end_bulk_load(&saved).await?;
    read?;
    sink.refresh(std::slice::from_ref(&fresh)).await?;

    let documents = sink
        .count_documents(&fresh)
        .await?
        .with_context(|| format!("{fresh} is gone; something removed it while it was filling"))?;
    let after = source_count(cfg, source_url, &tbl).await?;
    match verdict(before, after, documents, tbl.append_only) {
        Verdict::Exact => {}
        Verdict::WithinDrift => println!(
            "{qualified_table} moved under the load: {before} row(s) before, {after} after, \
             {documents} document(s) written. The restart below replays everything committed \
             since, which closes the difference."
        ),
        Verdict::Mismatch => {
            bail!(
                "{fresh} holds {documents} document(s), which {qualified_table} does not \
                 explain: {before} row(s) before the load, {after} after. Nothing was switched \
                 and {index} is untouched. Investigate, then remove the rebuilt index with a \
                 {}",
                delete_hint(swapped, &fresh)
            );
        }
    }

    sink.switch_alias(alias, &fresh).await?;
    let elapsed = started.elapsed().as_secs_f64();
    // The swap runs both ways, so on a target whose alias is an index the
    // previous documents end up under the fresh name rather than the old one.
    let previous = if swapped { &fresh } else { &index };
    if swapped {
        println!(
            "{index} now holds the rebuilt documents ({documents} documents, {elapsed:.1}s); \
             the ones it held before are now {fresh}"
        );
    } else {
        println!("alias {alias} now points at {fresh} ({documents} documents, {elapsed:.1}s)");
    }
    if drop_old {
        sink.delete_index(previous).await?;
        println!("{previous} deleted");
    }
    println!("Next:");
    let mut step = 1;
    if !swapped {
        println!("  1. set index = \"{fresh}\" in [sync.{key}]");
        step = 2;
    }
    println!(
        "  {step}. start the pipeline again; the checkpoint did not move, so it replays \
         everything committed since the rebuild started"
    );
    if !drop_old {
        println!(
            "  {}. once satisfied, delete the documents from before the rebuild: {}",
            step + 1,
            delete_hint(swapped, previous)
        );
    }
    Ok(())
}

/// Everything a rebuild cannot do, said before anything is created.
fn refuse_unsupported(
    cfg: &AppConfig,
    key: &str,
    tbl: &crate::config::TableSync,
    alias: &str,
) -> Result<()> {
    let index = tbl.index_name(key);
    let index = index.as_str();
    // an alias points at one index, and a template's glob is not one
    if tbl.is_templated() {
        bail!(
            "[sync.{key}] chooses its index per row: a rebuild fills one index and points an \
             alias at it, and neither is a thing a template has"
        );
    }
    if cfg.shared_indexes().contains(index) {
        let what = if cfg.is_join_index(index) {
            "is a join pair's index"
        } else {
            "is fed by more than one table"
        };
        bail!(
            "[sync.{key}] index {index} {what}: a rebuild reads one table, so the fresh index \
             would hold {}'s documents and nothing else. Rebuild it with a second instance of \
             the whole config, as docs/operations.md describes",
            tbl.table
        );
    }
    // a fanned row's documents are counted per element, so the source's row
    // count says nothing about what the index should hold
    if tbl.fan_out.is_some() {
        bail!(
            "[sync.{key}] configures fan_out: a rebuild checks the documents it wrote against \
             the source's row count, and one row here is many documents"
        );
    }
    if alias_is_the_index(cfg) {
        if alias != index {
            bail!(
                "--alias {alias}: this target has no aliases, so a rebuild swaps the fresh index \
                 into the name readers already use, and that name is {index} for [sync.{key}]. \
                 Any other value would build an index nothing reads"
            );
        }
    } else if alias == index {
        // With require_alias the section names the alias by construction —
        // validate proves it is one — so this is the *only* invocation an
        // operator could reach for, and it is still not one a rebuild can do:
        // it needs a fresh index to fill and a separate name to point at it.
        if cfg.target.require_alias {
            bail!(
                "[target] require_alias is set, so [sync.{key}] names the alias {index} and not \
                 an index, and a rebuild needs both a fresh index to fill and a name to point at \
                 it. Unset require_alias for the rebuild — the pipeline is stopped for it anyway \
                 — then leave index = \"{index}\" in place instead of the new name it prints, and \
                 set require_alias again"
            );
        }
        bail!(
            "--alias {alias} is the index [sync.{key}] already writes to: an alias and an index \
             cannot share a name, and the rebuild would have nowhere to point it"
        );
    }
    Ok(())
}

/// Whether the name readers use is an index of the target rather than a pointer
/// to one.
///
/// Meilisearch has no alias namespace: `switch_alias` there exchanges the
/// contents of the two names, so the live index uid *is* the alias, and after
/// the swap the fresh name holds the documents from before the rebuild. Nothing
/// about the rebuild itself changes — only which name `--alias` may be, which
/// name holds the rollback, and what there is left to do afterwards.
fn alias_is_the_index(cfg: &AppConfig) -> bool {
    cfg.target.flavor == "meilisearch"
}

/// How a reader of a printed message would remove `index` by hand.
fn delete_hint(swapped: bool, index: &str) -> String {
    if swapped {
        format!("DELETE /indexes/{index}")
    } else {
        format!("DELETE /{index}")
    }
}

/// Refuse unless the stream is demonstrably stopped.
///
/// Positive evidence only, and no `--force`: a rebuilt index the stream never
/// wrote to has no way to order a row the load read stale against the change
/// that superseded it, so a rebuild beside a live stream silently keeps the
/// wrong value.
async fn refuse_a_live_stream(
    cfg: &AppConfig,
    source_url: &str,
    sink: &dyn pg2osync_core::sink::Sink,
) -> Result<()> {
    if cfg.source.flavor != "mysql" {
        let client = crate::connect_pg(cfg, source_url).await?;
        let slots = pg2osync_source::catalog::all_slots(&client).await?;
        if slots
            .iter()
            .any(|s| s.name == cfg.source.slot_name && s.active)
        {
            bail!(
                "replication slot {} has a reader attached, so the pipeline is still running. \
                 A rebuild fills an index the stream is not writing to, so a row that changes \
                 during it would be wrong there for good. Stop the pipeline and run this again",
                cfg.source.slot_name
            );
        }
    }
    // The slot says nothing on MySQL and nothing about a pipeline that is
    // between reconnects on PostgreSQL; a checkpoint that moves says it on
    // both.
    let stream = cfg.stream_id();
    let position = |c: Option<pg2osync_core::checkpoint::Checkpoint>| c.map(|c| c.position);
    let first = position(sink.read_checkpoint(&stream).await?);
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let second = position(sink.read_checkpoint(&stream).await?);
    if first != second {
        bail!(
            "the checkpoint moved from {} to {} while this was checking, so something is still \
             streaming into the target. Stop the pipeline and run this again",
            first.unwrap_or_else(|| "none".into()),
            second.unwrap_or_else(|| "none".into())
        );
    }
    Ok(())
}

/// How many rows the section would index right now.
///
/// The section's `where` is applied, rendered through the source's own dialect:
/// the load reads the same predicate, so a count that ignored it would compare
/// two different questions.
async fn source_count(
    cfg: &AppConfig,
    source_url: &str,
    tbl: &crate::config::TableSync,
) -> Result<u64> {
    let filter = tbl
        .filter
        .as_deref()
        .map(pg2osync_core::filter::Filter::parse)
        .transpose()
        .map_err(|e| anyhow::anyhow!("where predicate of {}: {e}", tbl.table))?;
    if cfg.source.flavor == "mysql" {
        let (schema, name) = crate::backfill::split_qualified(&tbl.table);
        let where_clause = filter
            .map(|f| {
                format!(
                    " WHERE ({})",
                    f.to_sql(&pg2osync_source_mysql::catalog::dialect())
                )
            })
            .unwrap_or_default();
        let sql = format!(
            "SELECT count(*) FROM {}.{}{where_clause}",
            pg2osync_source_mysql::catalog::quote_ident(schema),
            pg2osync_source_mysql::catalog::quote_ident(name)
        );
        let mut conn = crate::mysql_source(cfg, source_url)?
            .admin_connection()
            .await?;
        let row = conn.query_text_row(&sql).await?;
        row.first()
            .and_then(|v| v.as_ref())
            .and_then(|v| v.parse::<u64>().ok())
            .with_context(|| format!("counting {} returned nothing", tbl.table))
    } else {
        let where_clause = filter
            .map(|f| format!(" WHERE ({})", f.to_sql(&crate::backfill::pg_dialect_bare())))
            .unwrap_or_default();
        let sql = format!("SELECT count(*) FROM {}{where_clause}", tbl.table);
        let client = crate::connect_pg(cfg, source_url).await?;
        let count: i64 = client
            .query_one(&sql, &[])
            .await
            .with_context(|| format!("counting {} failed", tbl.table))?
            .get(0);
        Ok(count.max(0) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(sections: &str) -> AppConfig {
        toml::from_str(&format!(
            "[source]\nurl = \"postgres://u@h/db\"\n[target]\nurl = \"http://t\"\n{sections}"
        ))
        .expect("parses")
    }

    #[test]
    fn a_rebuild_names_the_index_after_the_second_it_started() {
        assert_eq!(fresh_index_name("users", 1_756_512_345), "users-1756512345");
    }

    #[test]
    fn a_second_rebuild_replaces_the_timestamp_rather_than_appending_one() {
        assert_eq!(
            fresh_index_name("users-1756512345", 1_789_000_000),
            "users-1789000000"
        );
        // a name that merely ends in a number keeps it: only a timestamp goes
        assert_eq!(fresh_index_name("users-v2", 100), "users-v2-100");
        assert_eq!(fresh_index_name("orders-2024", 100), "orders-2024-100");
    }

    #[test]
    fn a_rebuilt_name_is_still_a_legal_index_name() {
        for base in ["users", "users-1756512345", "a_b-c"] {
            let name = fresh_index_name(base, 1_756_512_345);
            pg2osync_engine::mapping::check_index_name(&name)
                .unwrap_or_else(|e| panic!("{name} {e}"));
        }
    }

    #[test]
    fn a_templated_a_shared_and_a_fanned_section_are_all_refused() {
        let templated =
            config("[sync.events]\ntable = \"public.events\"\nindex = \"e-{tenant}\"\n");
        let err = refuse_unsupported(&templated, "events", &templated.sync["events"], "e")
            .expect_err("refused");
        assert!(err.to_string().contains("per row"), "{err}");

        let shared = config(
            "[sync.users]\ntable = \"public.users\"\nindex = \"search\"\nid = \"u-{id}\"\n\
             [sync.orders]\ntable = \"public.orders\"\nindex = \"search\"\nid = \"o-{id}\"\n",
        );
        let err =
            refuse_unsupported(&shared, "users", &shared.sync["users"], "s").expect_err("refused");
        assert!(err.to_string().contains("more than one table"), "{err}");

        let joined = config(
            "[sync.customers]\ntable = \"public.customers\"\nindex = \"shop\"\n\
             [sync.customers.join]\nfield = \"rel\"\nname = \"customer\"\n\
             [sync.orders]\ntable = \"public.orders\"\nindex = \"shop\"\nid = \"o-{id}\"\n\
             [sync.orders.join]\nfield = \"rel\"\nname = \"order\"\nparent = \"customer_id\"\n",
        );
        let err = refuse_unsupported(&joined, "orders", &joined.sync["orders"], "s")
            .expect_err("refused");
        assert!(err.to_string().contains("join pair"), "{err}");

        let fanned = config(
            "[sync.docs]\ntable = \"public.docs\"\nindex = \"docs\"\n\
             [sync.docs.fan_out]\nfield = \"tags\"\nid = \"{id}-{tag}\"\n",
        );
        let err =
            refuse_unsupported(&fanned, "docs", &fanned.sync["docs"], "d").expect_err("refused");
        assert!(err.to_string().contains("fan_out"), "{err}");
    }

    #[test]
    fn where_the_alias_is_the_index_only_that_name_is_accepted() {
        let plain = "[sync.users]\ntable = \"public.users\"\nindex = \"users\"\n";
        let meili = toml::from_str::<AppConfig>(&format!(
            "[source]\nurl = \"postgres://u@h/db\"\n\
             [target]\nurl = \"http://t\"\nflavor = \"meilisearch\"\n{plain}"
        ))
        .expect("parses");
        let err =
            refuse_unsupported(&meili, "users", &meili.sync["users"], "live").expect_err("refused");
        assert!(err.to_string().contains("nothing reads"), "{err}");
        // and the value the other targets refuse is the only one this one takes
        refuse_unsupported(&meili, "users", &meili.sync["users"], "users")
            .expect("the live index uid is the name a swap moves the documents into");
        assert_eq!(delete_hint(true, "users-1"), "DELETE /indexes/users-1");
        assert_eq!(delete_hint(false, "users-1"), "DELETE /users-1");
    }

    #[test]
    fn an_alias_that_is_the_index_is_refused() {
        let plain = "[sync.users]\ntable = \"public.users\"\nindex = \"users\"\n";
        let cfg = config(plain);
        let err =
            refuse_unsupported(&cfg, "users", &cfg.sync["users"], "users").expect_err("refused");
        assert!(err.to_string().contains("cannot share a name"), "{err}");
        refuse_unsupported(&cfg, "users", &cfg.sync["users"], "live")
            .expect("an ordinary section is fine");
    }

    #[test]
    fn a_section_that_has_to_name_an_alias_is_told_what_a_rebuild_needs() {
        // require_alias leaves the section naming the alias, so this is the
        // only invocation left — and the refusal has to say why rather than
        // claim a name collision that is not the problem here
        let requiring = toml::from_str::<AppConfig>(
            "[source]\nurl = \"postgres://u@h/db\"\n\
             [target]\nurl = \"http://t\"\nrequire_alias = true\n\
             [sync.users]\ntable = \"public.users\"\nindex = \"users_live\"\n",
        )
        .expect("parses");
        let err = refuse_unsupported(&requiring, "users", &requiring.sync["users"], "users_live")
            .expect_err("refused");
        assert!(err.to_string().contains("require_alias is set"), "{err}");
    }

    #[test]
    fn redirecting_the_load_is_one_field_of_the_config() {
        let cfg = config(
            "[sync.users]\ntable = \"public.users\"\nindex = \"users\"\n\
             [sync.orders]\ntable = \"public.orders\"\nindex = \"orders\"\n",
        );
        let mut rebuilt = cfg.clone();
        rebuilt.sync.get_mut("users").expect("section").index = Some("users-1756512345".into());

        let specs = run::index_specs(&rebuilt).expect("specs");
        assert!(
            specs.iter().any(|s| s.name == "users-1756512345"),
            "the fresh index is the one ensure_ready is asked for: {specs:?}"
        );
        assert!(
            !specs.iter().any(|s| s.name == "users"),
            "nothing points at the old index any more: {specs:?}"
        );
        assert!(
            specs.iter().any(|s| s.name == "orders"),
            "and the sections that were not rebuilt are untouched: {specs:?}"
        );
    }

    #[test]
    fn a_count_is_accepted_only_where_the_source_explains_it() {
        assert_eq!(verdict(100, 100, 100, false), Verdict::Exact);
        assert_eq!(verdict(100, 103, 101, false), Verdict::WithinDrift);
        assert_eq!(verdict(103, 100, 100, false), Verdict::WithinDrift);
        assert_eq!(verdict(100, 100, 99, false), Verdict::Mismatch);
        assert_eq!(verdict(100, 103, 104, false), Verdict::Mismatch);
        // rows a source cannot tell apart are one document, so an append-only
        // table is only ever bounded from above
        assert_eq!(verdict(100, 100, 98, true), Verdict::WithinDrift);
        assert_eq!(verdict(100, 100, 100, true), Verdict::Exact);
        assert_eq!(verdict(100, 103, 104, true), Verdict::Mismatch);
    }
}
