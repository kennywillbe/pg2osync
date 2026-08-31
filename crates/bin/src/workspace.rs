//! A directory of configs, and the questions that are about the set.
//!
//! One file is still one source: nothing about a config changes because it has
//! neighbours. What a directory adds is everything two files can disagree
//! about — a name, a replication slot, an index, the port a listener binds —
//! and none of that is visible from inside either file.

use crate::config::{self, ApiConfig, AppConfig, Label, LogConfig, MetricsConfig, TableSync};
use anyhow::{Context, Result, bail};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// One config file, and the name everything else calls it by.
pub struct Source {
    pub name: String,
    pub path: PathBuf,
    pub cfg: AppConfig,
}

/// Every source one invocation was given, and what they share by being one
/// process: two listeners and a log subscriber.
pub struct Workspace {
    pub sources: Vec<Source>,
    pub metrics: MetricsConfig,
    pub api: ApiConfig,
    pub log: LogConfig,
}

impl Workspace {
    /// One file, or every file in one directory. `--config` keeps its default,
    /// so the directory is what decides which of the two this is.
    pub fn load(config: &Path, config_dir: Option<&Path>) -> Result<Self> {
        match config_dir {
            Some(dir) => Self::load_dir(dir),
            None => Self::load_file(config),
        }
    }

    pub fn load_file(path: &Path) -> Result<Self> {
        let cfg = AppConfig::load(path)?;
        Self::assemble(vec![source_of(path, cfg)])
    }

    /// Every `*.toml` directly in `dir`, in name order.
    ///
    /// A Kubernetes ConfigMap mounts as a directory of symlinks beside a
    /// `..data` symlink to a timestamped directory holding the files
    /// themselves, so anything that recursed, or followed an entry whose name
    /// begins with a dot, would load every config twice.
    pub fn load_dir(dir: &Path) -> Result<Self> {
        let entries = std::fs::read_dir(dir)
            .with_context(|| format!("cannot read config directory {}", dir.display()))?;
        let mut paths: Vec<PathBuf> = Vec::new();
        for entry in entries {
            let path = entry
                .with_context(|| format!("cannot read config directory {}", dir.display()))?
                .path();
            let hidden = path
                .file_name()
                .and_then(|name| name.to_str())
                .is_none_or(|name| name.starts_with('.'));
            // through the symlink rather than at it: the entries of a mounted
            // ConfigMap are symlinks, and every one of them is a file
            let is_file = path.metadata().is_ok_and(|meta| meta.is_file());
            let is_toml = path.extension().and_then(|ext| ext.to_str()) == Some("toml");
            if !hidden && is_file && is_toml {
                paths.push(path);
            }
        }
        // read_dir hands them over in the filesystem's order; a report that
        // reads differently on two machines is a report nobody can compare
        paths.sort();
        if paths.is_empty() {
            bail!(
                "no *.toml files in {}: a config directory is the list of sources to run, and \
                 an empty one lists none",
                dir.display()
            );
        }

        let mut sources = Vec::with_capacity(paths.len());
        let mut failures: Vec<String> = Vec::new();
        for path in &paths {
            // every file, not the first one that fails: an operator fixing a
            // directory of configs wants the whole list in one pass
            match AppConfig::load(path).map(|cfg| source_of(path, cfg)) {
                Ok(source) => sources.push(source),
                Err(e) => failures.push(format!("  {}: {e:#}", file_name(path))),
            }
        }
        if !failures.is_empty() {
            bail!(
                "{} of {} config file(s) in {} are invalid:\n{}",
                failures.len(),
                paths.len(),
                dir.display(),
                failures.join("\n")
            );
        }
        Self::assemble(sources)
    }

    /// Narrow the workspace to the one source a command names.
    pub fn only(self, name: &str) -> Result<Self> {
        let Self {
            sources,
            metrics,
            api,
            log,
        } = self;
        let kept: Vec<Source> = sources.into_iter().filter(|s| s.name == name).collect();
        if kept.is_empty() {
            bail!("no source is called {name:?}");
        }
        Ok(Self {
            sources: kept,
            metrics,
            api,
            log,
        })
    }

    fn assemble(sources: Vec<Source>) -> Result<Self> {
        validate_across(&sources)?;
        // the sections are per file but the listeners are per process, so the
        // agreement is what lets one answer stand for the whole workspace
        let metrics = agree(&sources, "metrics", |cfg| &cfg.metrics)?;
        let api = agree(&sources, "api", |cfg| &cfg.api)?;
        let log = agree(&sources, "log", |cfg| &cfg.log)?;
        Ok(Self {
            sources,
            metrics,
            api,
            log,
        })
    }
}

/// The name a source answers to: what `[source] name` says, or the file's
/// stem. The stem is what an operator already uses for the file, and it is
/// unique within a directory for free.
fn source_of(path: &Path, cfg: AppConfig) -> Source {
    let name = match &cfg.source.name {
        // the grammar of an explicit name is `AppConfig::validate`'s: a name
        // an operator typed is a name they can be asked to correct
        Some(name) => name.clone(),
        None => name_from_stem(
            path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default(),
        ),
    };
    Source {
        name,
        path: path.to_path_buf(),
        cfg,
    }
}

/// The file's stem, in the grammar the name has to survive a metrics label and
/// a command line in.
///
/// Fitted rather than refused, because nobody chose this name: a config with
/// nothing wrong in it must not fail to load over the path it was handed
/// under, and paths hold dots — a `mktemp` file, a `config.toml.bak`, a
/// downloaded copy.
fn name_from_stem(stem: &str) -> String {
    let mut name = String::with_capacity(stem.len());
    for c in stem.chars() {
        if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
            name.push(c);
        } else if !name.ends_with('-') {
            name.push('-');
        }
    }
    let trimmed = name.trim_matches('-');
    if trimmed.is_empty() {
        // a stem of nothing the grammar keeps still has to answer to something
        String::from("pg2osync")
    } else {
        trimmed.to_string()
    }
}

/// A config is addressed by its file name: the directory is the same for all
/// of them, and a full path buries the part that differs.
fn file_name(path: &Path) -> &str {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("<config>")
}

/// What two files must not disagree about.
fn validate_across(sources: &[Source]) -> Result<()> {
    let mut names: BTreeMap<&str, &Source> = BTreeMap::new();
    for source in sources {
        if let Some(first) = names.insert(&source.name, source) {
            bail!(
                "two sources are called {:?}: {} and {}. The name is how a message, a metric \
                 and a command line tell them apart, so set [source] name in one of them",
                source.name,
                file_name(&first.path),
                file_name(&source.path)
            );
        }
    }

    // The stream is identified by its slot or its server id, and by neither
    // the host it reads nor the file it was written in, so configs copied from
    // one template are one stream however many databases they name.
    let mut streams: BTreeMap<String, &Source> = BTreeMap::new();
    for source in sources {
        let document = pg2osync_sink::checkpoint_doc_id(&source.cfg.stream_id());
        if let Some(first) = streams.insert(document.clone(), source) {
            let (names_it, fix) = if source.cfg.source.flavor == "mysql" {
                (
                    format!("MySQL server_id {}", source.cfg.source.server_id),
                    "server_id",
                )
            } else {
                (
                    format!("replication slot {:?}", source.cfg.source.slot_name),
                    "slot_name",
                )
            };
            bail!(
                "duplicate stream identity: {} and {} both name {names_it}, so both would keep \
                 their position in the one checkpoint document {}/{document} and each would \
                 resume from the other's. Give every source a [source] {fix} of its own",
                file_name(&first.path),
                file_name(&source.path),
                pg2osync_sink::META_INDEX
            );
        }
    }

    // Two files write one index exactly as two sections of one file do, and
    // the index does not know which file a document came from.
    let sections: Vec<(Label, &TableSync)> = sources
        .iter()
        .flat_map(|source| {
            let file = file_name(&source.path);
            source
                .cfg
                .sync
                .iter()
                .map(move |(key, table)| (Label::in_file(file, key), table))
        })
        .collect();
    config::check_index_overlap(&sections)
}

/// A section that describes the process, not a source. Files that leave it out
/// take whatever the ones that declare it say; files that declare it
/// differently are asking for two listeners, and there is one process.
fn agree<T: PartialEq + Default + Clone>(
    sources: &[Source],
    section: &str,
    of: impl Fn(&AppConfig) -> &T,
) -> Result<T> {
    let default = T::default();
    let mut declared: Option<(&Source, &T)> = None;
    for source in sources {
        let value = of(&source.cfg);
        if *value == default {
            continue;
        }
        match declared {
            Some((first, chosen)) if chosen != value => bail!(
                "[{section}] differs between {} and {}: the section describes the process \
                 rather than one source, so the files that declare it must declare the same one",
                file_name(&first.path),
                file_name(&source.path)
            ),
            _ => declared = Some((source, value)),
        }
    }
    Ok(declared.map_or(default, |(_, value)| value.clone()))
}

/// Configs on disk, for the tests of anything that reads a directory of them.
#[cfg(test)]
pub(crate) mod fixtures {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A directory of the test's own, removed with it: what a workspace does
    /// is read a mount, so the fixtures are on disk.
    pub(crate) struct TempDir(PathBuf);

    impl TempDir {
        pub(crate) fn new(tag: &str) -> Self {
            static NEXT: AtomicU32 = AtomicU32::new(0);
            let path = std::env::temp_dir().join(format!(
                "pg2osync-workspace-{}-{tag}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&path).expect("a temporary directory");
            Self(path)
        }

        pub(crate) fn write(&self, name: &str, body: &str) -> PathBuf {
            let path = self.0.join(name);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("a fixture directory");
            }
            std::fs::write(&path, body).expect("a fixture file");
            path
        }

        pub(crate) fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// The smallest config that loads, with whatever the test is about
    /// appended to `[source]` and a table of its own.
    pub(crate) fn config(source_extra: &str, table: &str) -> String {
        format!(
            "[source]\nurl_env = \"PG2OSYNC_SOURCE_URL\"\n{source_extra}\n\
             [target]\nurl = \"http://localhost:9200\"\n\
             [sync.{table}]\ntable = \"public.{table}\"\n"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::{TempDir, config};
    use super::*;

    fn names(ws: &Workspace) -> Vec<&str> {
        ws.sources.iter().map(|s| s.name.as_str()).collect()
    }

    fn refused(result: Result<Workspace>) -> String {
        match result {
            Ok(ws) => panic!("expected a refusal, loaded {:?}", names(&ws)),
            Err(e) => format!("{e:#}"),
        }
    }

    #[test]
    fn every_toml_is_loaded_in_name_order_and_nothing_else_is() {
        let dir = TempDir::new("order");
        dir.write("orders.toml", &config("", "orders"));
        dir.write("accounts.toml", &config("slot_name = \"a\"", "accounts"));
        dir.write("mapping.json", "{\"properties\":{}}");
        dir.write("notes.txt", "not a config");
        let ws = Workspace::load_dir(dir.path()).expect("two sources");
        assert_eq!(names(&ws), ["accounts", "orders"]);
    }

    #[test]
    fn a_config_map_mount_is_read_once() {
        let dir = TempDir::new("configmap");
        let data = dir.path().join("..2026_08_30_12_00_00.1234");
        std::fs::create_dir_all(&data).expect("the timestamped directory");
        std::fs::write(data.join("orders.toml"), config("", "orders")).expect("the real file");
        std::os::unix::fs::symlink(&data, dir.path().join("..data")).expect("the ..data symlink");
        std::os::unix::fs::symlink(data.join("orders.toml"), dir.path().join("orders.toml"))
            .expect("the entry symlink");
        dir.write(".hidden.toml", &config("slot_name = \"h\"", "hidden"));
        dir.write("nested/deep.toml", &config("slot_name = \"d\"", "deep"));

        let ws = Workspace::load_dir(dir.path()).expect("one source");
        assert_eq!(names(&ws), ["orders"]);
    }

    #[test]
    fn a_directory_with_no_config_in_it_is_refused() {
        let dir = TempDir::new("empty");
        dir.write("readme.md", "nothing here");
        assert!(refused(Workspace::load_dir(dir.path())).contains("no *.toml files"));
    }

    #[test]
    fn a_source_is_named_by_its_file_unless_it_names_itself() {
        let dir = TempDir::new("naming");
        dir.write("orders.toml", &config("", "orders"));
        dir.write(
            "second.toml",
            &config("name = \"billing\"\nslot_name = \"b\"", "invoices"),
        );
        let ws = Workspace::load_dir(dir.path()).expect("two sources");
        assert_eq!(names(&ws), ["orders", "billing"]);

        let single = Workspace::load_file(&dir.path().join("orders.toml")).expect("one source");
        assert_eq!(names(&single), ["orders"]);
    }

    #[test]
    fn a_stem_a_metrics_label_could_not_carry_is_fitted_to_it() {
        let dir = TempDir::new("badname");
        let path = dir.write("orders v2.beta.toml", &config("", "orders"));
        let ws = Workspace::load_file(&path).expect("a derived name never fails to load");
        assert_eq!(names(&ws), ["orders-v2-beta"]);
        config::check_source_name(&ws.sources[0].name).expect("a name in the grammar");
    }

    #[test]
    fn a_name_a_metrics_label_could_not_carry_is_refused_when_it_was_chosen() {
        let dir = TempDir::new("badexplicitname");
        let named = dir.write("ok.toml", &config("name = \"orders v2\"", "orders"));
        assert!(refused(Workspace::load_file(&named)).contains("[source] name \"orders v2\""));
    }

    #[test]
    fn a_stem_with_nothing_the_grammar_keeps_still_names_the_source() {
        assert_eq!(
            name_from_stem("pg2osync-e2e-require-alias.TQMFWk"),
            "pg2osync-e2e-require-alias-TQMFWk"
        );
        assert_eq!(name_from_stem(".. ..."), "pg2osync");
        assert_eq!(name_from_stem(""), "pg2osync");
    }

    #[test]
    fn two_names_the_same_are_refused() {
        let dir = TempDir::new("dupname");
        dir.write("a.toml", &config("name = \"orders\"", "orders"));
        dir.write(
            "b.toml",
            &config("name = \"orders\"\nslot_name = \"b\"", "lines"),
        );
        let why = refused(Workspace::load_dir(dir.path()));
        assert!(why.contains("two sources are called \"orders\""), "{why}");
        assert!(why.contains("a.toml") && why.contains("b.toml"), "{why}");
    }

    #[test]
    fn two_files_sharing_one_checkpoint_document_are_refused_naming_it() {
        let dir = TempDir::new("slot");
        dir.write("tenant-a.toml", &config("", "orders"));
        dir.write("tenant-b.toml", &config("", "invoices"));
        let why = refused(Workspace::load_dir(dir.path()));
        assert!(why.contains("duplicate stream identity"), "{why}");
        assert!(why.contains(".pg2osync_meta/postgres-pg2osync"), "{why}");
        assert!(why.contains("slot_name"), "{why}");
    }

    #[test]
    fn two_mysql_files_sharing_a_server_id_are_refused() {
        let dir = TempDir::new("serverid");
        let mysql = "flavor = \"mysql\"\n";
        dir.write("a.toml", &config(mysql, "orders"));
        dir.write("b.toml", &config(mysql, "invoices"));
        let why = refused(Workspace::load_dir(dir.path()));
        assert!(why.contains("MySQL server_id 424242"), "{why}");
        assert!(why.contains("[source] server_id"), "{why}");
    }

    #[test]
    fn an_index_two_files_share_needs_an_explicit_id_in_each() {
        let dir = TempDir::new("sharedindex");
        let shared = |slot: &str, key: &str| {
            format!(
                "[source]\nurl_env = \"U\"\nslot_name = \"{slot}\"\n\
                 [target]\nurl = \"http://localhost:9200\"\n\
                 [sync.{key}]\ntable = \"public.{key}\"\nindex = \"people\"\n"
            )
        };
        dir.write("a.toml", &shared("a", "users"));
        dir.write("b.toml", &shared("b", "staff"));
        let why = refused(Workspace::load_dir(dir.path()));
        assert!(
            why.contains("two tables map to the same index \"people\""),
            "{why}"
        );
        assert!(
            why.contains("a.toml [sync.users]") || why.contains("b.toml [sync.staff]"),
            "{why}"
        );

        let with_ids = |slot: &str, key: &str| {
            format!(
                "[source]\nurl_env = \"U\"\nslot_name = \"{slot}\"\n\
                 [target]\nurl = \"http://localhost:9200\"\n\
                 [sync.{key}]\ntable = \"public.{key}\"\nindex = \"people\"\nid = \"{key}-{{id}}\"\n"
            )
        };
        let second = TempDir::new("sharedindex-ok");
        second.write("a.toml", &with_ids("a", "users"));
        second.write("b.toml", &with_ids("b", "staff"));
        Workspace::load_dir(second.path()).expect("each section declares its own id");
    }

    #[test]
    fn a_fixed_index_inside_another_files_template_is_refused() {
        let dir = TempDir::new("template");
        dir.write(
            "events.toml",
            "[source]\nurl_env = \"U\"\n\
             [target]\nurl = \"http://localhost:9200\"\n\
             [sync.events]\ntable = \"public.events\"\nindex = \"events-{tenant}\"\n\
             primary_key = \"id\"\n",
        );
        dir.write(
            "legacy.toml",
            "[source]\nurl_env = \"U\"\nslot_name = \"legacy\"\n\
             [target]\nurl = \"http://localhost:9200\"\n\
             [sync.old]\ntable = \"public.old\"\nindex = \"events-2024\"\n",
        );
        let why = refused(Workspace::load_dir(dir.path()));
        assert!(why.contains("events.toml [sync.events]"), "{why}");
        assert!(why.contains("legacy.toml [sync.old]"), "{why}");
        assert!(why.contains("TRUNCATE"), "{why}");
    }

    #[test]
    fn a_listener_two_files_describe_differently_is_refused_naming_both() {
        let dir = TempDir::new("metrics");
        dir.write(
            "a.toml",
            &format!(
                "{}\n[metrics]\nbind = \"0.0.0.0:9100\"\n",
                config("", "orders")
            ),
        );
        dir.write(
            "b.toml",
            &format!(
                "{}\n[metrics]\nbind = \"0.0.0.0:9200\"\n",
                config("slot_name = \"b\"", "invoices")
            ),
        );
        let why = refused(Workspace::load_dir(dir.path()));
        assert!(
            why.contains("[metrics] differs between a.toml and b.toml"),
            "{why}"
        );
    }

    #[test]
    fn one_file_declaring_a_listener_is_not_a_disagreement() {
        let dir = TempDir::new("agree");
        dir.write(
            "a.toml",
            &format!(
                "{}\n[metrics]\nbind = \"0.0.0.0:9100\"\n",
                config("", "orders")
            ),
        );
        dir.write("b.toml", &config("slot_name = \"b\"", "invoices"));
        let ws = Workspace::load_dir(dir.path()).expect("one declaration, no disagreement");
        assert_eq!(names(&ws), ["a", "b"]);
        assert_eq!(ws.metrics.bind, "0.0.0.0:9100");
    }

    #[test]
    fn two_files_asking_for_different_log_filters_are_refused() {
        // one process, one subscriber: the last file read would otherwise
        // decide what every source logs at
        let dir = TempDir::new("log");
        dir.write(
            "a.toml",
            &format!("{}\n[log]\nfilter = \"debug\"\n", config("", "orders")),
        );
        dir.write(
            "b.toml",
            &format!(
                "{}\n[log]\nfilter = \"warn\"\n",
                config("slot_name = \"b\"", "invoices")
            ),
        );
        let why = refused(Workspace::load_dir(dir.path()));
        assert!(
            why.contains("[log] differs between a.toml and b.toml"),
            "{why}"
        );
    }

    #[test]
    fn every_invalid_file_is_reported_not_only_the_first() {
        let dir = TempDir::new("invalid");
        dir.write("good.toml", &config("", "orders"));
        dir.write("broken.toml", "[source]\nurl_env = \"U\"\n");
        dir.write(
            "unqualified.toml",
            &config("slot_name = \"u\"", "users").replace("public.users", "users"),
        );
        let why = refused(Workspace::load_dir(dir.path()));
        assert!(why.contains("2 of 3 config file(s)"), "{why}");
        assert!(why.contains("broken.toml"), "{why}");
        assert!(why.contains("unqualified.toml"), "{why}");
        assert!(!why.contains("good.toml"), "{why}");
    }
}
