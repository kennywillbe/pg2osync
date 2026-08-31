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

- On startup pg2osync creates each configured index with `id` as its primary
  key, and leaves an index that already exists alone — so a restart, a resume
  or a second pipeline over the same index starts normally.
- Documents are upserted with the primary key as Meilisearch's document id.
- Writes are asynchronous server-side tasks; the sink waits for each task to
  complete before acknowledging, so the checkpoint only advances after durable
  acceptance.
- TRUNCATE deletes all documents of the index and waits for that task too.
- Meilisearch has no mappings; searchable and filterable attributes are yours to
  configure on the index.

## Rebuilding an index

There are no mappings here, so a rebuild is never about a field type. What it is
for is an index settings change that only applies to documents indexed after it,
or a decoding bug whose wrong values are already written — cases where the
documents have to be built again while the live name keeps answering.

```sh
pg2osync reindex -c pg2osync.toml --table public.users --alias users
```

- **`--alias` must be the index the section already writes to.** There is no
  alias namespace on this target: the name readers use *is* an index uid, so
  there is nothing to point somewhere else. Any other value is refused rather
  than filling an index nothing reads.
- **The switch is `POST /swap-indexes`,** which exchanges the contents of two
  uids in a single task — atomic in the same sense an alias move is, and the
  reason a rebuild is possible here at all.
- **Afterwards `<index>-<unix seconds>` holds the *previous* documents,** not
  the new ones: the swap runs both ways. That index is the rollback — swap it
  back to undo the rebuild — and `--drop-old` deletes it instead.
- **No config edit follows,** unlike the other targets: the section's `index`
  never changed, so the only step left is starting the pipeline again.
- **Stop the pipeline first;** the command refuses to run beside it. The
  checkpoint file in `state_dir` does not move, so the restart replays
  everything committed since the rebuild started, which is what proves the
  contents — the count only proves how many.
- The checkpoint lives outside the uid namespace entirely, so no swap can touch
  it.

## Version targeting

`dev/e2e-meili-smoke.sh` runs nightly against Meilisearch v1.53.1 (see
[compatibility](../compatibility.md)). It is a smoke suite rather than the
full `dev/e2e-test.sh`, because that suite asserts over mappings, join fields
and per-row indices — the three things this target does not have. What it does
cover is the initial load, live INSERT/UPDATE/DELETE, the file checkpoint
resuming after a restart, and a rebuild swapping a fresh index into the live
name.

The `sink conformance kit` job runs the shared kit against the same version.
Three of its five checks pass here — read-back, idempotent replay and
checkpoint durability, the last against the state file rather than a document
in the target. The other two are reported as skipped rather than faked: this
target keeps no document versions, so a truncate cannot happen *at* a
position, and it has no schema, so there is no document it refuses for the
partial-batch check to work with.
