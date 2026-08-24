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
- Back up or persist `state_dir` alongside the process (volume mount in
  Docker). Losing it triggers a safe full re-sync (upsert semantics make this
  idempotent).
- Deletes still propagate — pg2osync issues explicit delete calls; nothing
  depends on tombstones inside Meilisearch.

## Behavior

- Documents are upserted with the primary key as Meilisearch's document id.
- Batches wait for task completion (`wait_task`) so checkpoints only advance
  after durable acceptance.
