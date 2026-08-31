# Migrating from Logstash

## Who this is for

You run a Logstash pipeline with a `jdbc` input and an `opensearch` or
`elasticsearch` output: a `SELECT` re-run on a schedule, `sql_last_value`
carrying the watermark, a few filters in between. It works, and it has two
properties you have probably already worked around — it cannot see a deleted
row, and the index is only as fresh as the polling interval.

pg2osync replaces that pipeline with one binary reading the replication log.
This page maps what you already wrote onto what you would write instead, states
plainly what has no counterpart, and gives the cutover sequence that does not
take the index down.

It does not replace Logstash where Logstash is a log shipper. If the input is
files, Beats or Kafka, nothing here applies: the input pg2osync has is a
database table.

## What replaces what

Every row links to the section that documents the option, because the option's
own section is where the caveats live.

| In the Logstash pipeline | In pg2osync |
|---|---|
| `jdbc` input, `schedule`, `sql_last_value` | the replication stream — PostgreSQL's WAL or MySQL's binlog, read continuously. The watermark query's equivalent is [poll mode](../configuration.md#poll-mode), which exists as a documented fallback and not as the default |
| `statement` naming which columns to select | [`columns` / `exclude_columns`](../configuration.md#synckey) — projection, applied on every path |
| `mutate { rename => … }` | [`fields`](../configuration.md#field-names), the last shaping step |
| `mutate { add_field => … }` | [`constants`](../configuration.md#constant-fields) — scalars, plus `{schema}`/`{table}` |
| `mutate { convert => "integer" }` | the [`number`](../configuration.md#transforms) transform |
| `mutate { split => … }` — a string into an array | the [`split`](../configuration.md#transforms) transform |
| the `split` filter — one event into many | [`fan_out`](../configuration.md#fan-out), which also diffs the array, so an element that leaves takes its document with it |
| `json` filter | the [`json`](../configuration.md#transforms) transform |
| `date` filter | the [`date`](../configuration.md#transforms) transform: a `strptime` format into ISO 8601. It converts the column in place; there is no `@timestamp` to promote it to |
| `translate` filter | the [`lookup`](../configuration.md#transforms) transform, with `default` for a miss |
| `fingerprint` filter | [`hash`](../configuration.md#transforms) when nobody needs the value back, [`pseudonym`](../configuration.md#pseudonym) when joins have to survive |
| `mutate { replace => "***" }` over a secret | the [`redact`](../configuration.md#transforms) transform |
| `if [status] != "active" { drop {} }` | [`where`](../configuration.md#row-filters) — and a row that *leaves* the predicate is deleted from the index rather than merely skipped |
| a correlated subquery counting related rows | an [`aggregates`](../configuration.md#aggregates) entry with `op = "count"` |
| `jdbc_streaming` filter, or a `LEFT JOIN` flattened by hand | a [`children`](../configuration.md#nested-children) entry, with [`through`](../configuration.md#many-to-many-through-a-junction-table) for a junction table and [`single`/`flatten`](../configuration.md#a-one-to-one-relation) for a 1:1 |
| `document_id => "%{id}"` | [`id`](../configuration.md#document-ids), a template of literals and `{column}` placeholders |
| `routing => "%{tenant_id}"` | [`routing`](../configuration.md#routing) |
| `index => "logstash-%{+YYYY.MM.dd}"` | [per-row indices](../configuration.md#per-row-indices) — `index = "events-{created_month}"`, rendered from a column rather than from the clock |
| `action => "delete"` branching on a column | nothing to write: the engine derives index and delete from the change itself. A column that *marks* a row deleted is [`where`](../configuration.md#row-filters), or [`soft_delete`](../configuration.md#soft-deletes) in poll mode |
| a join field maintained by hand | [`join`](../configuration.md#join-fields) — parent and child each a document of its own, on one shard |
| an index template or a mapping applied out of band | [`mapping_file`](../configuration.md#index-mappings), read at startup and compared against an existing index |
| an `inference` / `text_embedding` ingest processor named in the output | [`pipeline`](../configuration.md#ingest-pipelines), carried on every bulk action of the section |
| the dead letter queue | [quarantine](../operations.md#carrying-on-past-one-refused-document) — `on_permanent_rejection = "quarantine"`, then `pg2osync rejects --replay` |
| a separate full-reload job for a mapping change | [`resnapshot`, `reindex`, or a second instance](choosing-a-rebuild.md) |

## The same pipeline, both ways

A Logstash pipeline keeping a `users` index warm:

```ruby
input {
  jdbc {
    jdbc_connection_string => "jdbc:postgresql://db-host/appdb"
    statement => "SELECT id, usr_nm, email, prefs, tenant_id, status
                    FROM users
                   WHERE updated_at > :sql_last_value"
    use_column_value => true
    tracking_column => "updated_at"
    tracking_column_type => "timestamp"
    schedule => "*/1 * * * *"
  }
}
filter {
  if [status] != "active" { drop {} }
  mutate { rename => { "usr_nm" => "username" } }
  json   { source => "prefs" target => "prefs" }
  fingerprint { source => "email" target => "email" method => "SHA256" }
  mutate { add_field => { "entity" => "user" } }
}
output {
  opensearch {
    index => "users"
    document_id => "%{id}"
    routing => "%{tenant_id}"
  }
}
```

The same thing as a `[sync]` section:

```toml
[source]
flavor = "postgres"
url_env = "PG2OSYNC_SOURCE_URL"

[target]
url = "http://opensearch:9200"

[sync.users]
table   = "public.users"
index   = "users"
where   = "status = 'active'"
routing = "tenant_id"

[sync.users.transform]
prefs = "json"
email = "hash"

[sync.users.fields]
usr_nm = "username"

[sync.users.constants]
entity = "user"
```

Three differences are worth naming rather than skimming past.

- There is no schedule, because there is no query. Changes arrive as the
  database commits them.
- There is no `id` either: a document's `_id` is already its row's primary key,
  and `document_id => "%{id}"` is what that means. `id` is for the shapes a key
  does not cover, such as `"tenant-{tenant_id}-{id}"`.
- `where` is not `drop`. A row that stops matching is **deleted** from the
  index; a `drop` filter simply never emits it, so the document it wrote last
  week stays there.

## The two arguments for making the move

**Polling cannot see a delete.** Nothing a removed row can match appears in
`WHERE updated_at > :sql_last_value`, so its document lives in the index until
something else removes it — a nightly rebuild, or a person. The replication log
carries the delete like any other change. That is why poll mode, which is the
same technique, opens by saying [deletes are
invisible](../configuration.md#poll-mode), and why
[`reconcile`](../operations.md#reconciling-an-index-against-its-source) exists
as the sweeper for the pipelines that cannot avoid it.

**A count in a subquery is only as fresh as its parent row.** If the Logstash
statement counts related rows —
`(SELECT count(*) FROM deals WHERE contact_id = c.id) AS open_deals` — that
number is recomputed when the *parent* row is selected again, which happens
when the parent's `updated_at` moves. A deal opening does not touch the
contact, so the count is wrong until something else does. An
[aggregate](../configuration.md#aggregates) watches the counted table instead:
an INSERT, an UPDATE or a DELETE there names the parent to count again, and a
row moving between parents names both.

## What has no counterpart

Stated plainly, because finding these out during a cutover is worse than
reading them now.

- **No expression or scripting language.** There is no `ruby` filter and no
  `grok`. `transform` is [eight fixed named
  ops](../configuration.md#transforms) and nothing else, which is a
  [deliberate refusal](../decisions.md#correctness) rather than a gap waiting
  to be filled. Anything that needs computing is computed in the database (a
  view, a generated column) or in the target (an [ingest
  pipeline](../configuration.md#ingest-pipelines)).
- **No fan-out to several consumers, and no replay from a broker.** The output
  is the configured sink. Where more than one system needs the stream, that is
  what Kafka is for.
- **One-way only.** No bidirectional sync and no conflict resolution — see
  [Scope](../decisions.md#scope).
- **No DDL propagation.** A schema change is reported, never applied: the index
  keeps holding documents in the old shape until a rebuild, and
  `pg2osync_schema_drift_total` is the metric that says so.
- **No pattern-matched table discovery.** Tables are named one by one; there is
  no regex, so a new table is a config change — and one that needs [no
  restart](add-a-source-table-without-a-restart.md).
- **No data streams.** OpenSearch and Elasticsearch data streams are
  append-only, and this pipeline updates and deletes documents.
- **`count` is the only aggregate.** `sum`, `min` and `max` are refused at
  startup, naming what is supported.
- **Poll mode is a fallback, not a mode to choose.** PostgreSQL only, no
  deletes, and every start re-runs the initial load.

## The cutover

Do not point pg2osync at the index Logstash is writing. Run the two beside each
other and move an alias, which is the same shape as the [zero-downtime
re-index](../operations.md#zero-downtime-re-index).

1. **Write the config against a new index.** `pg2osync init --table users`
   writes a starter file from the database; set `index` to a name Logstash does
   not own, and bring the mapping you already use across as
   [`mapping_file`](../configuration.md#index-mappings) so the new index is
   created with it.
2. **Check both ends before starting.** `pg2osync validate -c pg2osync.toml`
   checks the config, the connectivity, the server settings, the nullable
   columns an `id` or a `routing` names, and the ingest pipeline where one is
   configured.
3. **Run it beside Logstash.** `pg2osync run` does the initial load and then
   streams. Logstash keeps serving the live index throughout; the two write to
   different indices and never meet.
4. **Wait for it to catch up.**
   `pg2osync status -c pg2osync.toml --caught-up --timeout 300` is an exit code
   rather than a metric to watch.
5. **Compare the two sides.** `pg2osync reconcile -c pg2osync.toml` names
   documents whose row is gone, which — on an index Logstash filled — is
   exactly the deletes it could never see. Run it caught up, or rows still on
   their way look like orphans.
6. **Move the alias.** `pg2osync switch-alias -c pg2osync.toml --alias users`
   is one atomic request; a reader resolving the alias never sees it point
   nowhere.
7. **Stop the Logstash pipeline,** and keep the old index until you are sure —
   it is the rollback, one alias flip away.

Afterwards,
[`require_alias = true`](../configuration.md#keeping-every-write-behind-the-alias)
is worth setting: it turns a section that writes to a raw index instead of the
alias into a refused write, rather than an index that quietly goes stale.
