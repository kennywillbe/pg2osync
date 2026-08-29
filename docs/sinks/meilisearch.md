# Meilisearch sink

Meilisearch support for search use cases that don't need the full OpenSearch
query DSL.

```toml
[target]
flavor = "meilisearch"
url = "http://localhost:7700"
# api_key_env = "MEILI_API_KEY"    # optional
state_dir = "/var/lib/pg2osync"    # checkpoint fallback directory
```

## Key difference: file-based checkpoints

Meilisearch has no arbitrary document storage, so the hidden `.pg2osync_meta`
checkpoint index can't live there. pg2osync persists its checkpoint to a local
JSON file in `state_dir` instead.

Operational consequences:

- **The checkpoint is tied to one machine.** Don't run two pg2osync instances
  against the same Meilisearch target with a shared state dir.
- Persist `state_dir` (a volume mount in Docker, `persistence.enabled` in the
  Helm chart). Losing it triggers a full re-sync, which is safe but expensive.
- The file is written to a temporary name and renamed into place, so a crash
  mid-write cannot leave a truncated checkpoint that silently restarts the
  pipeline from zero.
- Deletes still propagate — pg2osync issues explicit delete calls; nothing
  depends on tombstones inside Meilisearch.

## Behavior

- Documents are upserted with the primary key as Meilisearch's document id.
- Writes are asynchronous server-side tasks; the sink waits for each task to
  complete before acknowledging, so the checkpoint only advances after durable
  acceptance.
- TRUNCATE deletes all documents of the index and waits for that task too.
- Meilisearch has no mappings; searchable and filterable attributes are yours to
  configure on the index.

## Version targeting

`dev/e2e-meili-smoke.sh` runs nightly against Meilisearch v1.53.1 (see
[compatibility](../compatibility.md)). It is a smoke suite rather than the
full `dev/e2e-test.sh`, because that suite asserts over mappings, join fields
and per-row indices — the three things this target does not have. What it does
cover is the initial load, live INSERT/UPDATE/DELETE and the file checkpoint
resuming after a restart.
