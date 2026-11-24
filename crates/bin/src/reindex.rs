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
//!
//! A templated table is rebuilt as a fresh generation of its template rather
//! than as one index, and every alias its section names moves in one request.

use anyhow::{Context as _, Result, bail};
use pg2osync_core::load::LoadScope;
use pg2osync_core::sink::{IndexSpec, Sink, index_patterns_overlap};
use pg2osync_engine::mapping::IndexTarget;
use std::collections::BTreeSet;

use crate::config::AppConfig;
use crate::workspace::{Source, Workspace};
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

/// The template a rebuild of a templated table fills: a generation marker at
/// whichever end of the spec is a literal, replacing the one a previous
/// rebuild put there.
///
/// The marker must sit inside a literal end, because one next to a
/// placeholder leaves the old pattern claiming the new names: `events-*`
/// matches `events-acme-<ts>`, and then the old configuration's TRUNCATE and
/// reconcile would claim the rebuild. `g` and nine or more digits is a marker
/// for the same reason a fixed rebuild's timestamp is one.
pub(crate) fn fresh_template_spec(spec: &str, now: u64) -> Result<String, String> {
    let is_generation =
        |m: &str| m.len() >= 10 && m.starts_with('g') && m[1..].chars().all(|c| c.is_ascii_digit());
    if !spec.starts_with('{') {
        let rest = spec
            .split_once('-')
            .filter(|(marker, rest)| is_generation(marker) && !rest.is_empty())
            .map_or(spec, |(_, rest)| rest);
        return Ok(format!("g{now}-{rest}"));
    }
    if spec.ends_with('}') {
        return Err(format!(
            "index {spec:?} has a placeholder at both ends, so a rebuilt generation has no \
             literal to be told apart by and the old pattern would claim it"
        ));
    }
    let rest = spec
        .rsplit_once('-')
        .filter(|(rest, marker)| is_generation(marker) && !rest.is_empty())
        .map_or(spec, |(rest, _)| rest);
    Ok(format!("{rest}-g{now}"))
}

/// Every `(alias, index)` pair for the indices a template rendered: each
/// alias rendered from the values that rendered its index.
///
/// With one placeholder the name is that value. With more, the name cannot
/// be split back into them, so the source's distinct values of the
/// template's columns render both names, and only pairs whose index is
/// listed are kept. An index whose rows are all gone renders no pair, and
/// [`switch_generation`] refuses to leave it without an alias.
pub(crate) async fn alias_pairs(
    cfg: &AppConfig,
    source_url: &str,
    table: &str,
    target: &IndexTarget,
    indices: &[String],
) -> Result<Vec<(String, String)>> {
    if target.alias_is_in_the_name() {
        let mut pairs = Vec::with_capacity(indices.len());
        for index in indices {
            if let Some(alias) = target.alias_from_name(index) {
                pairs.push((alias.map_err(|e| anyhow::anyhow!(e))?, index.clone()));
            }
        }
        return Ok(pairs);
    }
    let IndexTarget::Template { template, .. } = target else {
        bail!("{table} has no index template to render aliases from");
    };
    if cfg.source.flavor == "mysql" {
        bail!(
            "{} has more than one placeholder, so its index names cannot be split back into the \
             values that rendered them, and reading them from a MySQL source is not supported",
            target.spec()
        );
    }
    let client = crate::connect_pg(cfg, source_url).await?;
    let columns = crate::reconcile::template_columns(&client, table, &template.columns()).await?;
    let selected = columns
        .iter()
        .map(|(name, _)| format!("{}::text", crate::reconcile::quote_ident(name)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT DISTINCT {selected} FROM {}",
        crate::reconcile::qualify(table)
    );
    let rows = client
        .query(&sql, &[])
        .await
        .with_context(|| format!("cannot read the template's values from {table}"))?;
    let docs = rows
        .iter()
        .map(|row| {
            let values: Vec<Option<String>> = (0..columns.len()).map(|i| row.get(i)).collect();
            crate::reconcile::decode_row(&columns, &values)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(pairs_from_docs(target, &docs, indices))
}

/// The `(alias, index)` pairs the rows `docs` render, sorted and kept only
/// where the index is one of `indices`.
pub(crate) fn pairs_from_docs(
    target: &IndexTarget,
    docs: &[serde_json::Value],
    indices: &[String],
) -> Vec<(String, String)> {
    let listed: BTreeSet<&str> = indices.iter().map(String::as_str).collect();
    let mut pairs = BTreeSet::new();
    for doc in docs {
        // a row whose names cannot be rendered is one the stream halted on,
        // and its index, if any, holds nothing this rebuild wrote
        let (Ok(index), Ok(Some(alias))) = (target.render(doc), target.render_alias(doc)) else {
            continue;
        };
        if listed.contains(index.as_str()) {
            pairs.insert((alias, index));
        }
    }
    pairs.into_iter().collect()
}

/// The opening of what a templated switch reports: the aliases it moved and
/// the indices they now point at, out of the `listed` ones the template
/// matched. Counted apart because one alias can span several indices and
/// an index can be left with none, so the number of pairs is neither.
pub(crate) fn switch_summary(pairs: &[(String, String)], listed: usize) -> String {
    let aliases: BTreeSet<&str> = pairs.iter().map(|(alias, _)| alias.as_str()).collect();
    let indices: BTreeSet<&str> = pairs.iter().map(|(_, index)| index.as_str()).collect();
    let onto = if indices.len() == listed {
        format!("the {listed}")
    } else {
        format!("{} of the {listed}", indices.len())
    };
    format!("{} alias(es) now point at {onto} index(es)", aliases.len())
}

/// The `(alias, index)` pairs of `held` a switch to `pairs` does not touch
/// and that sit outside the generation `listed` names: the alias of a tenant
/// that generation has no index for, still on another generation.
pub(crate) fn left_behind(
    held: &[(String, String)],
    pairs: &[(String, String)],
    listed: &[String],
) -> Vec<(String, String)> {
    held.iter()
        .filter(|(alias, index)| {
            !pairs.iter().any(|(moved, _)| moved == alias) && !listed.contains(index)
        })
        .cloned()
        .collect()
}

/// The `listed` indices a switch to `pairs` would leave with no alias the
/// template claims: in no pair, and holding none of `held` that the switch
/// leaves alone. An alias the switch moves is removed from every index it is
/// not paired with, so holding one of those does not count.
pub(crate) fn unaliased(
    held: &[(String, String)],
    pairs: &[(String, String)],
    listed: &[String],
) -> Vec<String> {
    listed
        .iter()
        .filter(|index| {
            !pairs.iter().any(|(_, paired)| paired == *index)
                && !held.iter().any(|(alias, holder)| {
                    holder == *index && !pairs.iter().any(|(moved, _)| moved == alias)
                })
        })
        .cloned()
        .collect()
}

/// Point every alias in `pairs` at its index in one request, and return the
/// aliases the template renders that the switch leaves on another generation.
///
/// Looked up before the switch, which cannot change them, so a target that
/// cannot answer stops the command while nothing has moved yet. So does an
/// index the switch would leave with no alias, with `then` saying what to do
/// about it.
pub(crate) async fn switch_generation(
    sink: &dyn Sink,
    target: &IndexTarget,
    pairs: &[(String, String)],
    listed: &[String],
    then: &str,
) -> Result<Vec<(String, String)>> {
    let held = match target.alias_pattern() {
        Some(pattern) => sink.list_aliases(&pattern).await?,
        None => Vec::new(),
    };
    let bare = unaliased(&held, pairs, listed);
    if !bare.is_empty() {
        // the stream gives an alias only to an index it creates, so a row
        // written to one of these later would be one no reader sees
        let shown = bare.iter().take(10).cloned().collect::<Vec<_>>().join(", ");
        let more = if bare.len() > 10 {
            format!(" and {} more", bare.len() - 10)
        } else {
            String::new()
        };
        bail!(
            "{} index(es) {} matches would be left with no alias, because no row in the source \
             renders them: {shown}{more}. A row written to one later would land where no alias \
             reads it, since the stream only gives an alias to an index it creates. Nothing was \
             switched. {then}",
            bare.len(),
            target.pattern()
        );
    }
    sink.switch_aliases(pairs).await?;
    Ok(left_behind(&held, pairs, listed))
}

/// Say which aliases a switch left where they were, and what joins them to
/// the generation switched to.
pub(crate) fn report_left_behind(left: &[(String, String)]) {
    if left.is_empty() {
        return;
    }
    // the stream creates a tenant's missing index with its alias, and from
    // then on the alias reads two generations until a switch picks one
    println!(
        "{} alias(es) stayed where they were, because this generation has no index for their \
         tenant. Once the stream creates one, the alias reads both until switch-alias runs \
         again against this generation:",
        left.len()
    );
    for (alias, index) in left.iter().take(10) {
        println!("  {alias} -> {index}");
    }
    if left.len() > 10 {
        println!("  … and {} more", left.len() - 10);
    }
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
#[allow(clippy::too_many_arguments)]
pub async fn run_for(
    source: &Source,
    workspace: &Workspace,
    source_url: &str,
    admin_url: &str,
    target_password: Option<String>,
    qualified_table: &str,
    alias: &str,
    drop_old: bool,
) -> Result<()> {
    let cfg = &source.cfg;
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
    refuse_unsupported(cfg, &key, &tbl, alias, drop_old)?;
    if tbl.is_templated() {
        return run_templated(
            source,
            workspace,
            source_url,
            admin_url,
            target_password,
            &key,
            &tbl,
        )
        .await;
    }
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
    // The load asks the same question later, but by then the fresh index
    // exists and a refusal would leave it behind empty, one per retry.
    if cfg.source.flavor == "mysql" {
        resnapshot::mysql_version_base(sink.as_ref(), &cfg.stream_id()).await?;
    }
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

/// Rebuild a templated table as a fresh generation of its template, then
/// move every alias its section names onto that generation in one request.
async fn run_templated(
    source: &Source,
    workspace: &Workspace,
    source_url: &str,
    admin_url: &str,
    target_password: Option<String>,
    key: &str,
    tbl: &crate::config::TableSync,
) -> Result<()> {
    let cfg = &source.cfg;
    let qualified_table = tbl.table.as_str();
    let pk = run::pk_columns_for(tbl);
    let old = tbl
        .index_target(key, &pk)
        .map_err(|e| anyhow::anyhow!("[sync.{key}] {e}"))?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let fresh_spec =
        fresh_template_spec(old.spec(), now).map_err(|e| anyhow::anyhow!("[sync.{key}] {e}"))?;
    let mut fresh_section = tbl.clone();
    fresh_section.index = Some(fresh_spec.clone());
    let fresh = fresh_section.index_target(key, &pk).map_err(|e| {
        anyhow::anyhow!("[sync.{key}] the rebuilt template would be {fresh_spec:?}, which {e}")
    })?;
    let (old_pattern, fresh_pattern) = (old.pattern(), fresh.pattern());
    if index_patterns_overlap(&old_pattern, &fresh_pattern) {
        bail!(
            "[sync.{key}] the rebuilt template {fresh_spec:?} claims names {old_pattern:?} \
             claims too, so the configuration being replaced would treat the rebuild as its own"
        );
    }

    next_config(cfg, key, &fresh_spec, workspace, &source.name)?;

    let sink = run::build_sink(cfg, target_password)?;
    refuse_a_live_stream(cfg, source_url, sink.as_ref()).await?;
    let existing = sink.list_indices(&fresh_pattern).await?;
    if !existing.is_empty() {
        bail!(
            "{fresh_pattern} already matches {}; a rebuild will not write into indices it did \
             not create. Delete them, or wait a second and run this again",
            existing.join(", ")
        );
    }

    let before = source_count(cfg, source_url, tbl).await?;

    // The load writes without the alias: an index it creates must not join a
    // live alias before the count says the generation is whole.
    let mut rebuilt = cfg.clone();
    match rebuilt.sync.get_mut(key) {
        Some(section) => {
            section.index = Some(fresh_spec.clone());
            section.alias = None;
        }
        None => bail!("[sync.{key}] is not in this config"),
    }
    let specs: Vec<IndexSpec> = run::index_specs(&rebuilt)?
        .into_iter()
        .filter(|spec| spec.name == fresh_pattern)
        .collect();
    sink.ensure_ready(&specs).await?;

    println!(
        "rebuilding {old_pattern} as {fresh_pattern} from {qualified_table} ({before} row(s))"
    );
    let started = std::time::Instant::now();
    // No bulk-load settings to suspend: the indices do not exist until the
    // rows create them, and one created mid-load takes the target's defaults.
    let scope = LoadScope::resnapshot(qualified_table, None)
        .with_table_filters(run::table_filters(&rebuilt)?);
    resnapshot::load_one(&rebuilt, source_url, admin_url, sink.clone(), &scope).await?;

    let indices = sink.list_indices(&fresh_pattern).await?;
    sink.refresh(&indices).await?;
    let documents = count_all(sink.as_ref(), &indices).await?;
    let after = source_count(cfg, source_url, tbl).await?;
    match verdict(before, after, documents, tbl.append_only) {
        Verdict::Exact => {}
        Verdict::WithinDrift => println!(
            "{qualified_table} moved under the load: {before} row(s) before, {after} after, \
             {documents} document(s) written. The restart below replays everything committed \
             since, which closes the difference."
        ),
        Verdict::Mismatch => bail!(
            "the {} index(es) {fresh_pattern} matches hold {documents} document(s), which \
             {qualified_table} does not explain: {before} row(s) before the load, {after} after. \
             No alias was switched and {old_pattern} is untouched. Investigate, then delete the \
             rebuilt indices: {}",
            indices.len(),
            indices.join(", ")
        ),
    }

    let pairs = alias_pairs(cfg, source_url, qualified_table, &fresh, &indices).await?;
    let then = format!(
        "{fresh_pattern} is left as the load filled it and {old_pattern} is untouched. Delete \
         those indices: they hold only rows the source has since deleted, which the restart's \
         replay would delete anyway. Then set index = \"{fresh_spec}\" in [sync.{key}], run \
         switch-alias with that config, and start the pipeline again; the checkpoint did not \
         move, so it replays everything committed since the rebuild started"
    );
    let left = switch_generation(sink.as_ref(), &fresh, &pairs, &indices, &then).await?;
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "{} {fresh_pattern} matches ({documents} documents, {elapsed:.1}s)",
        switch_summary(&pairs, indices.len())
    );
    report_left_behind(&left);
    println!("Next:");
    println!("  1. set index = \"{fresh_spec}\" in [sync.{key}]");
    println!(
        "  2. start the pipeline again; the checkpoint did not move, so it replays everything \
         committed since the rebuild started"
    );
    println!(
        "  3. once satisfied, delete the previous generation: the indices {old_pattern} matches"
    );
    Ok(())
}

/// The config the operator is told to write once the switch is done, checked
/// as its load would check it.
///
/// Asked before anything is created: the aliases end on the new generation,
/// so a config that cannot load it leaves a pipeline that cannot start until
/// switch-alias moves them back. Under a config directory the other files
/// are asked as well, since one of them can claim the new generation's names
/// without overlapping the old ones.
fn next_config(
    cfg: &AppConfig,
    key: &str,
    fresh_spec: &str,
    workspace: &Workspace,
    source: &str,
) -> Result<AppConfig> {
    let mut next = cfg.clone();
    match next.sync.get_mut(key) {
        Some(section) => section.index = Some(fresh_spec.to_string()),
        None => bail!("[sync.{key}] is not in this config"),
    }
    next.validate().map_err(|e| {
        anyhow::anyhow!(
            "[sync.{key}] the rebuild would end on index = {fresh_spec:?}, and a config with \
             it does not load: {e}. Nothing was created"
        )
    })?;
    workspace.admits(source, &next).map_err(|e| {
        anyhow::anyhow!(
            "[sync.{key}] the rebuild would end on index = {fresh_spec:?}, and the configs \
             beside this one do not load with it: {e:#}. Nothing was created"
        )
    })?;
    Ok(next)
}

/// How many documents a generation holds, over every index in it. Counted
/// per listed index rather than over the glob, which an alias's name could
/// widen to indices the template never wrote.
async fn count_all(sink: &dyn Sink, indices: &[String]) -> Result<u64> {
    let mut total = 0;
    for index in indices {
        total += sink.count_documents(index).await?.with_context(|| {
            format!("{index} is gone; something removed it while the rebuild was counting")
        })?;
    }
    Ok(total)
}

/// Everything a rebuild cannot do, said before anything is created.
fn refuse_unsupported(
    cfg: &AppConfig,
    key: &str,
    tbl: &crate::config::TableSync,
    alias: &str,
    drop_old: bool,
) -> Result<()> {
    let index = tbl.index_name(key);
    let index = index.as_str();
    // A rebuild ends in an atomic switch of the name readers use, and this
    // target has neither an alias namespace to move a pointer inside nor a
    // swap that exchanges two names in one step. Refused rather than
    // approximated with a rename: the window where the name resolves to
    // nothing is exactly what a zero-downtime rebuild exists to avoid.
    if cfg.target.flavor == "postgres" {
        bail!(
            "a rebuild switches the name readers use onto a freshly filled one in a single \
             step, and a PostgreSQL target has no such step. Build the new table beside this \
             one with a second instance of the whole config, then point your readers at it"
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
    if tbl.is_templated() {
        return refuse_templated(cfg, key, tbl, alias, drop_old);
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

/// What a templated rebuild cannot do: anything without an alias namespace,
/// a section with no alias of its own to move, and a drop of indices a
/// pattern inferred.
fn refuse_templated(
    cfg: &AppConfig,
    key: &str,
    tbl: &crate::config::TableSync,
    alias: &str,
    drop_old: bool,
) -> Result<()> {
    if !matches!(cfg.target.flavor.as_str(), "opensearch" | "elasticsearch") {
        bail!(
            "[sync.{key}] chooses its index per row, which only OpenSearch and Elasticsearch \
             targets support"
        );
    }
    let Some(configured) = &tbl.alias else {
        bail!(
            "[sync.{key}] chooses its index per row and has no alias: a rebuild fills a fresh \
             generation and moves each index's alias onto it, and an index created after that \
             only carries one if the section names it. Add alias = \"{alias}\" to the section"
        );
    };
    // one source of truth: the aliases moved now and the ones the stream
    // creates for a new tenant afterwards have to be the same names
    if alias != configured {
        bail!(
            "--alias {alias} is not [sync.{key}]'s alias {configured:?}; a rebuild moves the \
             aliases the stream creates, so the two have to agree"
        );
    }
    if drop_old {
        bail!(
            "--drop-old with [sync.{key}]'s template would delete every index the old pattern \
             matches, which is a list inferred from a pattern rather than one anybody wrote down. \
             Delete the previous generation by name once you are satisfied"
        );
    }
    let target = tbl
        .index_target(key, &run::pk_columns_for(tbl))
        .map_err(|e| anyhow::anyhow!("[sync.{key}] {e}"))?;
    if !target.alias_is_in_the_name() && cfg.source.flavor == "mysql" {
        bail!(
            "[sync.{key}] index {:?} has more than one placeholder, so an index name cannot be \
             split back into the values that rendered it, and reading those values from a MySQL \
             source is not supported",
            target.spec()
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
    use crate::workspace::fixtures::TempDir;
    use serde_json::json;

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
        let err = refuse_unsupported(&templated, "events", &templated.sync["events"], "e", false)
            .expect_err("refused");
        assert!(err.to_string().contains("per row"), "{err}");

        let shared = config(
            "[sync.users]\ntable = \"public.users\"\nindex = \"search\"\nid = \"u-{id}\"\n\
             [sync.orders]\ntable = \"public.orders\"\nindex = \"search\"\nid = \"o-{id}\"\n",
        );
        let err = refuse_unsupported(&shared, "users", &shared.sync["users"], "s", false)
            .expect_err("refused");
        assert!(err.to_string().contains("more than one table"), "{err}");

        let joined = config(
            "[sync.customers]\ntable = \"public.customers\"\nindex = \"shop\"\n\
             [sync.customers.join]\nfield = \"rel\"\nname = \"customer\"\n\
             [sync.orders]\ntable = \"public.orders\"\nindex = \"shop\"\nid = \"o-{id}\"\n\
             [sync.orders.join]\nfield = \"rel\"\nname = \"order\"\nparent = \"customer_id\"\n",
        );
        let err = refuse_unsupported(&joined, "orders", &joined.sync["orders"], "s", false)
            .expect_err("refused");
        assert!(err.to_string().contains("join pair"), "{err}");

        let fanned = config(
            "[sync.docs]\ntable = \"public.docs\"\nindex = \"docs\"\n\
             [sync.docs.fan_out]\nfield = \"tags\"\nid = \"{id}-{tag}\"\n",
        );
        let err = refuse_unsupported(&fanned, "docs", &fanned.sync["docs"], "d", false)
            .expect_err("refused");
        assert!(err.to_string().contains("fan_out"), "{err}");
    }

    fn config_text(source_extra: &str, sections: &str) -> String {
        format!(
            "[source]\nurl = \"postgres://u@h/db\"\n{source_extra}\n\
             [target]\nurl = \"http://t\"\n{sections}"
        )
    }

    #[test]
    fn a_generation_the_config_could_not_load_is_refused_before_the_load() {
        let dir = TempDir::new("reindex-alone");
        let path = dir.write(
            "events.toml",
            &config_text(
                "",
                "[sync.events]\ntable = \"public.events\"\nindex = \"events-{tenant}\"\n\
                 alias = \"{tenant}-events-live\"\n\
                 [sync.groups]\ntable = \"public.groups\"\nindex = \"g{x}\"\n",
            ),
        );
        let ws = Workspace::load_file(&path).expect("g* and events-* cannot overlap");
        let cfg = &ws.sources[0].cfg;
        let err = next_config(cfg, "events", "g1764000000-events-{tenant}", &ws, "events")
            .expect_err("g* overlaps g1764000000-events-*");
        assert!(
            err.to_string().contains("g1764000000-events-{tenant}"),
            "{err}"
        );

        let next = next_config(cfg, "events", "events-{tenant}-g1764000000", &ws, "events")
            .expect("nothing else claims events-*-g1764000000");
        assert_eq!(
            next.sync["events"].index.as_deref(),
            Some("events-{tenant}-g1764000000")
        );
        assert_eq!(
            next.sync["events"].alias.as_deref(),
            Some("{tenant}-events-live"),
            "the config to write keeps its alias; only the load drops it"
        );
    }

    #[test]
    fn a_generation_a_sibling_file_claims_is_refused_before_the_load() {
        let dir = TempDir::new("reindex-siblings");
        dir.write(
            "events.toml",
            &config_text(
                "slot_name = \"events\"",
                "[sync.events]\ntable = \"public.events\"\nindex = \"events-{tenant}\"\n\
                 alias = \"{tenant}-events-live\"\n",
            ),
        );
        dir.write(
            "groups.toml",
            &config_text(
                "slot_name = \"groups\"",
                "[sync.groups]\ntable = \"public.groups\"\nindex = \"g{x}\"\n",
            ),
        );
        let ws = Workspace::load_dir(dir.path()).expect("g* and events-* cannot overlap");
        let events = ws
            .sources
            .iter()
            .find(|s| s.name == "events")
            .expect("events.toml");
        events
            .cfg
            .validate()
            .expect("the file alone has nothing that claims g1764000000-events-*");

        let err = next_config(
            &events.cfg,
            "events",
            "g1764000000-events-{tenant}",
            &ws,
            "events",
        )
        .expect_err("groups.toml claims g*");
        let why = format!("{err:#}");
        assert!(
            why.contains("g1764000000-events-{tenant}") && why.contains("groups.toml"),
            "the refusal has to name the generation and the file that claims it: {why}"
        );

        next_config(
            &events.cfg,
            "events",
            "events-{tenant}-g1764000000",
            &ws,
            "events",
        )
        .expect("no file claims events-*-g1764000000");
    }

    #[test]
    fn a_generation_is_marked_at_the_literal_end_and_replaced_on_the_next_rebuild() {
        let now = 1_764_000_000;
        assert_eq!(
            fresh_template_spec("events-{tenant}", now).expect("a leading literal"),
            "g1764000000-events-{tenant}"
        );
        assert_eq!(
            fresh_template_spec("g1756512345-events-{tenant}", now).expect("a second rebuild"),
            "g1764000000-events-{tenant}"
        );
        assert_eq!(
            fresh_template_spec("{tenant}-events", now).expect("a trailing literal"),
            "{tenant}-events-g1764000000"
        );
        assert_eq!(
            fresh_template_spec("{tenant}-events-g1756512345", now).expect("a second rebuild"),
            "{tenant}-events-g1764000000"
        );
        // a literal that merely looks like a short number is kept
        assert_eq!(
            fresh_template_spec("g2-events-{tenant}", now).expect("not a generation"),
            "g1764000000-g2-events-{tenant}"
        );
        let err = fresh_template_spec("{tenant}-events-{year}", now)
            .expect_err("no literal at either end");
        assert!(err.contains("both ends"), "{err}");
    }

    #[test]
    fn a_fresh_generation_never_shares_a_name_with_the_one_it_replaces() {
        for spec in [
            "events-{tenant}",
            "{tenant}-events",
            "events-{tenant}-{year}",
            "e{tenant}",
            "{tenant}x",
            "g1756512345-events-{tenant}",
            "{tenant}-events-g1756512345",
        ] {
            let fresh = fresh_template_spec(spec, 1_764_000_000).expect("a fresh spec");
            let pattern = |spec: &str| {
                pg2osync_engine::mapping::IndexTarget::Template {
                    spec: spec.into(),
                    template: pg2osync_engine::mapping::IdTemplate::parse(spec, &[])
                        .expect("a template"),
                    alias: None,
                }
                .pattern()
            };
            assert!(
                !pg2osync_core::sink::index_patterns_overlap(&pattern(spec), &pattern(&fresh)),
                "{spec} -> {fresh}"
            );
        }
    }

    fn templated(extra: &str) -> AppConfig {
        config(&format!(
            "[sync.events]\ntable = \"public.events\"\nindex = \"events-{{tenant}}\"\n{extra}"
        ))
    }

    #[test]
    fn a_templated_rebuild_moves_the_sections_own_alias_and_keeps_the_old_generation() {
        let cfg = templated("alias = \"{tenant}-live\"\n");
        refuse_unsupported(&cfg, "events", &cfg.sync["events"], "{tenant}-live", false)
            .expect("the section's alias");
        let err = refuse_unsupported(&cfg, "events", &cfg.sync["events"], "live", false)
            .expect_err("another alias");
        assert!(err.to_string().contains("{tenant}-live"), "{err}");
        let err = refuse_unsupported(&cfg, "events", &cfg.sync["events"], "{tenant}-live", true)
            .expect_err("--drop-old");
        assert!(err.to_string().contains("--drop-old"), "{err}");
        let bare = templated("");
        let err = refuse_unsupported(
            &bare,
            "events",
            &bare.sync["events"],
            "{tenant}-live",
            false,
        )
        .expect_err("no alias to move");
        assert!(err.to_string().contains("alias"), "{err}");
    }

    #[test]
    fn a_templated_fanned_section_is_refused_like_a_fixed_one() {
        // one row is many documents, so the row count proves nothing about
        // a generation either
        let cfg = templated(
            "alias = \"{tenant}-live\"\n[sync.events.fan_out]\nfield = \"tags\"\n\
             id = \"{id}-{tag}\"\n",
        );
        let err = refuse_unsupported(&cfg, "events", &cfg.sync["events"], "{tenant}-live", false)
            .expect_err("refused");
        assert!(err.to_string().contains("fan_out"), "{err}");
    }

    #[test]
    fn a_name_that_cannot_say_its_values_needs_a_source_that_can() {
        let mysql = toml::from_str::<AppConfig>(
            "[source]\nurl = \"mysql://u@h/db\"\nflavor = \"mysql\"\n[target]\nurl = \"http://t\"\n\
             [sync.events]\ntable = \"app.events\"\nindex = \"events-{tenant}-{year}\"\n\
             alias = \"{tenant}-live\"\n",
        )
        .expect("parses");
        let err = refuse_unsupported(
            &mysql,
            "events",
            &mysql.sync["events"],
            "{tenant}-live",
            false,
        )
        .expect_err("two placeholders on MySQL");
        assert!(err.to_string().contains("MySQL"), "{err}");
        let pg = templated("alias = \"{tenant}-live\"\n");
        refuse_unsupported(&pg, "events", &pg.sync["events"], "{tenant}-live", false)
            .expect("one placeholder reads its value from the name on any source");
    }

    #[test]
    fn where_the_alias_is_the_index_only_that_name_is_accepted() {
        let plain = "[sync.users]\ntable = \"public.users\"\nindex = \"users\"\n";
        let meili = toml::from_str::<AppConfig>(&format!(
            "[source]\nurl = \"postgres://u@h/db\"\n\
             [target]\nurl = \"http://t\"\nflavor = \"meilisearch\"\n{plain}"
        ))
        .expect("parses");
        let err = refuse_unsupported(&meili, "users", &meili.sync["users"], "live", false)
            .expect_err("refused");
        assert!(err.to_string().contains("nothing reads"), "{err}");
        // and the value the other targets refuse is the only one this one takes
        refuse_unsupported(&meili, "users", &meili.sync["users"], "users", false)
            .expect("the live index uid is the name a swap moves the documents into");
        assert_eq!(delete_hint(true, "users-1"), "DELETE /indexes/users-1");
        assert_eq!(delete_hint(false, "users-1"), "DELETE /users-1");
    }

    #[test]
    fn an_alias_that_is_the_index_is_refused() {
        let plain = "[sync.users]\ntable = \"public.users\"\nindex = \"users\"\n";
        let cfg = config(plain);
        let err = refuse_unsupported(&cfg, "users", &cfg.sync["users"], "users", false)
            .expect_err("refused");
        assert!(err.to_string().contains("cannot share a name"), "{err}");
        refuse_unsupported(&cfg, "users", &cfg.sync["users"], "live", false)
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
        let err = refuse_unsupported(
            &requiring,
            "users",
            &requiring.sync["users"],
            "users_live",
            false,
        )
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

    fn two_placeholders() -> IndexTarget {
        let cfg = config(
            "[sync.events]\ntable = \"public.events\"\nindex = \"events-{tenant}-{year}\"\n\
             alias = \"{tenant}-live\"\n",
        );
        cfg.sync["events"]
            .index_target("events", &[])
            .expect("a usable template")
    }

    fn owned(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(a, i)| (a.to_string(), i.to_string()))
            .collect()
    }

    #[test]
    fn rows_render_each_listed_index_with_its_alias() {
        let listed = [
            "events-acme-2024",
            "events-acme-2025",
            "events-big-co-2024",
            // its rows are all gone, so nothing renders a pair for it
            "events-globex-2023",
        ]
        .map(String::from);
        let docs = [
            json!({"tenant": "acme", "year": 2024}),
            json!({"tenant": "acme", "year": 2025}),
            // a value holding the separator, which is why the name cannot be
            // split back into the values
            json!({"tenant": "big-co", "year": 2024}),
            // a tenant with rows and no index here yet
            json!({"tenant": "initech", "year": 2026}),
            // a row the stream would halt on renders nothing
            json!({"tenant": "acme"}),
        ];
        assert_eq!(
            pairs_from_docs(&two_placeholders(), &docs, &listed),
            owned(&[
                ("acme-live", "events-acme-2024"),
                ("acme-live", "events-acme-2025"),
                ("big-co-live", "events-big-co-2024"),
            ])
        );
    }

    #[test]
    fn a_switch_counts_aliases_and_the_indices_that_got_one_not_pairs() {
        // one alias over two years is one alias, and an emptied index that
        // keeps an alias the switch does not move is not among the ones the
        // moved aliases point at
        let pairs = owned(&[
            ("acme-live", "events-acme-2024"),
            ("acme-live", "events-acme-2025"),
            ("big-co-live", "events-big-co-2024"),
        ]);
        assert_eq!(
            switch_summary(&pairs, 4),
            "2 alias(es) now point at 3 of the 4 index(es)"
        );
        let every = owned(&[
            ("acme-live", "e2e_rt-acme"),
            ("globex-live", "e2e_rt-globex"),
        ]);
        assert_eq!(
            switch_summary(&every, 2),
            "2 alias(es) now point at the 2 index(es)"
        );
    }

    #[test]
    fn an_alias_the_switch_does_not_move_is_named_with_the_index_holding_it() {
        let listed = ["e2e_rt-acme", "e2e_rt-globex"].map(String::from);
        let pairs = owned(&[
            ("acme-live", "e2e_rt-acme"),
            ("globex-live", "e2e_rt-globex"),
        ]);
        let held = owned(&[
            // moved by the switch, wherever it is now
            ("acme-live", "g9-e2e_rt-acme"),
            ("globex-live", "g9-e2e_rt-globex"),
            // only the newer generation has an index for this tenant
            ("initech-live", "g9-e2e_rt-initech"),
            // on an index this generation lists: a tenant with no rows left,
            // whose alias stays where it is
            ("hooli-live", "e2e_rt-acme"),
        ]);
        assert_eq!(
            left_behind(&held, &pairs, &listed),
            owned(&[("initech-live", "g9-e2e_rt-initech")])
        );
        assert!(left_behind(&[], &pairs, &listed).is_empty());
    }

    #[test]
    fn an_index_the_switch_would_leave_with_no_alias_is_named() {
        let listed = [
            "events-acme-2024",
            "events-big-co-2024",
            "events-globex-2023",
            "events-globex-2024",
            "events-hooli-2022",
            "events-initech-2021",
        ]
        .map(String::from);
        let pairs = owned(&[
            ("acme-live", "events-acme-2024"),
            ("big-co-live", "events-big-co-2024"),
            ("globex-live", "events-globex-2024"),
        ]);
        let held = owned(&[
            // the stream created the emptied 2023 index with its alias, and the
            // switch moves globex-live onto 2024 alone, so it would be stripped
            ("globex-live", "events-globex-2023"),
            // an alias the switch does not move stays where it is
            ("hooli-live", "events-hooli-2022"),
        ]);
        // initech-2021 never had an alias and nothing renders one for it
        assert_eq!(
            unaliased(&held, &pairs, &listed),
            ["events-globex-2023", "events-initech-2021"].map(String::from)
        );
        assert!(unaliased(&[], &pairs, &listed[..2]).is_empty());
    }

    #[test]
    fn a_templated_rebuild_needs_a_target_that_creates_indices_on_demand() {
        let meili = toml::from_str::<AppConfig>(
            "[source]\nurl = \"postgres://u@h/db\"\n\
             [target]\nurl = \"http://t\"\nflavor = \"meilisearch\"\n\
             [sync.events]\ntable = \"public.events\"\nindex = \"events-{tenant}\"\n\
             alias = \"{tenant}-live\"\n",
        )
        .expect("parses");
        let err = refuse_unsupported(
            &meili,
            "events",
            &meili.sync["events"],
            "{tenant}-live",
            false,
        )
        .expect_err("refused");
        assert!(
            err.to_string()
                .contains("only OpenSearch and Elasticsearch"),
            "{err}"
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
