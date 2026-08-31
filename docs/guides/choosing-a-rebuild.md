# Choosing a rebuild

You changed something in the config. The question this page answers is the one
that comes next: does the index have to be rebuilt, and if so, how much of it.

There are four answers, and the reload itself tells you which one you are in —
a change it cannot apply is refused in place, with one `ERROR` line naming the
field, both values and what it would take. The
[Reloading table](../configuration.md#reloading) is the authority option by
option; this page is what to do about each class.

## The four classes

**Nothing.** `[engine] batch_size`, `batch_max_bytes`, `txn_buffer_cap_mb`,
`load_max_rows_per_sec`, `checkpoint_interval_ms`, the retry budget, and
`[log] filter` are read by the next batch. So is a plain `[sync]` section
[added or removed](add-a-source-table-without-a-restart.md): the table joins
the publication and the stream while everything else keeps running.

**A restart, and nothing more.** `[source]` and `[target]` name the stream and
the checkpoint is bound to it; `[metrics]`, `[api]` and any `*_env` are fixed at
exec; `write_concurrency`, `on_permanent_rejection` and `max_rejects` build the
sink task; `poll_column` builds the poll query. None of these changes what a
document *is*, so the documents already in the index stay correct and the
restart replays from the checkpoint.

**A re-snapshot.** `columns`, `exclude_columns`, `transform`, `fields`,
`constants`, `where`, `pipeline`, `soft_delete` and `mapping_file` change the
**shape** of the document. Documents written before the change keep the old
shape, and nothing in the index records which is which — so the index would
hold a mixture. Restart, then read the table again:

```sh
pg2osync resnapshot -c pg2osync.toml --table public.users
pg2osync resnapshot -c pg2osync.toml --table public.users --where "tenant_id = 42"
```

It is safe beside the stream and never moves the checkpoint: its rows carry the
position they were read at, so a change committed after that still wins. Three
things it does not do — it does not delete (that is
[`reconcile`](../operations.md#reconciling-an-index-against-its-source)), it
does not resume, and it does not overwrite a document whose version is above
the position it read at. [Recovery](../operations.md#recovery) has the detail.

**A rebuild.** `table`, `index`, `id`, `primary_key`, `append_only`, `routing`,
`fan_out`, `join` and `children` change what a row is **filed as**. Every
document the section already wrote is filed the old way, and re-writing the
rows would leave the old documents behind under their old ids, routing or
index. The fix is a fresh index with the alias moved onto it.

## The one that hides in the re-snapshot class

`mapping_file` is listed as a re-snapshot, and that is right for what the
reload does with it. But a mapping **applies only when the index does not
exist**: a target refuses to change an existing field's type, which is what a
reindex is for. So the class depends on what changed inside the file.

- A field the mapping did not previously name, which dynamic mapping has been
  inferring — a re-snapshot is enough once the mapping is right for the *next*
  index, and the existing index keeps the inferred type until it is rebuilt.
- **A field's type,** an analyzer, or anything else the existing index has
  already committed to — a rebuild. There is no in-place path, on any target.

Startup is where this surfaces: an existing index is compared against the
mapping, a field mapped to a different type is an error (every document
carrying it would be rejected, and a permanent rejection halts the pipeline),
and a field the index does not declare is a warning.

## Which rebuild

Three paths, in increasing order of what they cost you and decreasing order of
the gap they leave.

| | What it is | Freshness gap | Needs |
|---|---|---|---|
| `resnapshot` into a new index name | a copy of the config pointing at `users_v2`, filled from the source, then `switch-alias` | the new index is static from the moment the re-snapshot finished | no second slot |
| [`reindex`](../configuration.md#rebuilding-an-index) | one command: create `users-<unix seconds>` from the section's `mapping_file`, load it, check the count, move the alias | the length of the load — the pipeline is stopped for it | the pipeline stopped; it refuses to run beside a live stream, and there is no `--force` |
| a second instance | a second config with its own `index` and `slot_name`, run to catch-up, then the alias moved | none | a second slot, and two lots of retained WAL until the old one is dropped |

`reindex` is the usual answer. It refuses a
[templated](../configuration.md#per-row-indices) index, a
[shared](../configuration.md#sharing-an-index) index or a join pair, a
[fanned](../configuration.md#fan-out) table, and an `--alias` equal to the
index the section already writes to; for those, the second instance is the way.
The checkpoint does not move during it, so everything committed while the
pipeline was stopped is still in the log and the restart replays it into the
new index. Two follow-ups are yours and the command prints both: set `index` to
the new name, and start the pipeline again.

With
[`require_alias = true`](../configuration.md#keeping-every-write-behind-the-alias)
set — which is what stops a section quietly writing past its alias — unset it
for the duration of a `reindex`, since the command needs a fresh index to fill
and a separate name to point at it. Afterwards leave `index` naming the alias
and set the option again.

The old index is kept unless `--drop-old` says otherwise, because it is the
rollback: one alias flip away.

## The full sequence

The live cutover with no freshness gap at all has been rehearsed end to end
against the dev stack, with a reader polling the alias throughout, and the four
things the rehearsal turned up are written down beside it. Rather than restate
it here: [Zero-downtime re-index](../operations.md#zero-downtime-re-index).
