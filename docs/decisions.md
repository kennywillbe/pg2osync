# Design decisions

Why pg2osync is built the way it is. Code that contradicts a decision here is a
bug: change this document first, then the code.

## Change capture

**Read the replication log, not triggers or timestamps.** Logical replication
(PostgreSQL) and the binlog (MySQL) see every change including deletes, add no
write overhead to the source, and are available on managed databases. Triggers
tax every transaction; timestamp polling cannot see deletes at all — which is
why poll mode exists only as a documented fallback.

**pgoutput, not a plugin.** PostgreSQL's built-in output plugin needs nothing
installed server-side, unlike `wal2json` or `decoderbufs`.

## Protocol code is ours

**Transport is a dependency, decoding is not.** PostgreSQL replication uses the
`pgwire-replication` crate purely as a transport that hands us raw `XLogData`
frames; `tokio-postgres` does not support replication. For MySQL the whole wire
protocol is implemented in-house, because the only Rust binlog client is
unmaintained, blocking, and cannot handle split packets.

Everything above the transport — decoding, slot and publication management,
transaction buffering, checkpointing — is ours, behind our own boundary. No CDC
framework, and the transport stays swappable.

## Boundaries

**`core` depends on nothing.** It holds `ChangeEvent`, the `Sink` trait,
checkpoint types and the error taxonomy. Everything else depends on it and not
on each other, so the compiler enforces the architecture instead of discipline.

**The `Sink` trait lives in `core`, not in the sink crate.** The engine must
never import a sink implementation; the contract has to sit next to the shared
types for that to hold.

**The engine is source-agnostic.** It knows `ChangeEvent` and an opaque `u64`
position token. Whether that token is a WAL LSN or a packed binlog coordinate is
the source's business, and the binary injects a closure that renders it back to
text for the checkpoint. This is what made adding MySQL a small change rather
than a fork.

**New targets implement `Sink`.** No `match sink_kind` anywhere in the engine.

## Correctness

**At-least-once with idempotent writes.** Exactly-once across two systems
without a shared transaction is a fiction. Every document's `_id` is its row's
primary key, so a replay overwrites to the same value. Duplicates are therefore
invisible, which is what makes the snapshot-then-stream overlap safe.

**Never acknowledge a position before it is durable.** The value reported to the
source is clamped to the persisted checkpoint. Acknowledging further lets the
database recycle history for rows that are not indexed yet — the classic way a
CDC pipeline loses data on crash-restart.

**Buffer until commit.** Rows are held until their commit boundary so a partial
transaction is never presented as complete. A transaction that exceeds the batch
limits is split, which is a deliberate, documented exception: an unbounded
buffer is a worse failure than briefly observable partial state.

**Writes and truncates share one ordered channel.** Executing a truncate
directly would let writes still queued ahead of it land afterwards and
resurrect rows the source has already dropped. The target is also refreshed
before the truncate, because `delete_by_query` only removes documents a search
can see.

**Stop on permanent rejection, unless told to quarantine.** A document the target
will never accept halts the pipeline by default. Skipping it would be silent data
loss, and every later batch would widen the divergence. Halting means making no
progress, not exiting: the attempt fails and is retried, so the position never
passes the document and a mapping fix is picked up without a restart.

The cost of that default is that one malformed row stops replication for every
table until someone edits a mapping, so `on_permanent_rejection = "quarantine"`
records the refused document — with its position and the operation itself, in a
hidden `.pg2osync_rejects` index — and carries on. What it must never become is
Airbyte's Elasticsearch destination, which dead-letters a document while the
offset advances anyway: the rule is that a position may be acknowledged only once
the document behind it was written *or* durably recorded as refused, which is why
quarantining happens before the acknowledgement and a failure to quarantine halts.

**Quarantining a document is a partial transaction, and that is the trade.** "No
partial transactions" is otherwise an invariant here; skipping one row while its
siblings land breaks it for that transaction. It is why the option is off by
default and why it is named after what it does rather than after being resilient.

**Quarantine is bounded.** `max_rejects` (default 100) counts what the store
actually holds, read at startup rather than kept in memory, so a crash loop cannot
hand the budget back. One bad row is worth carrying on past; a mapping that
refuses a whole table is not, and the pipeline halts naming the limit. Nothing is
lost either way: the batch that reaches the limit has its refusals recorded first,
and a batch arriving once the limit is already spent is left unacknowledged, so
the source sends it again when the mapping is fixed.

**A rejected document is replayed through the ordinary write path.** `pg2osync
rejects --replay` submits it again with its original position as its version, so a
row the source has since changed loses to the newer value by the same rule that
orders everything else, and the record is cleared only once the target has taken
it.

**A checkpoint is bound to its stream.** It records the source kind, the slot or
`server_id`, and the publication. A checkpoint from another stream is rejected
rather than used to resume into an unrelated position space.

## Initial load

**No exported snapshot; short transactions and replay instead.** The obvious
design exports one transaction snapshot and reads every chunk from it, which
keeps a read view open for the whole load: `VACUUM` cannot then clean anything
that died after it started, and on MySQL a long read view makes purge block
rather than lag. What makes our load safe is not snapshot
consistency but that streaming resumes from a position that predates every
chunk: on PostgreSQL because the slot exists before the first range and nothing
advances it during the load, on MySQL because the binlog coordinate is read
before the first chunk. Anything a chunk missed or read stale is still in the
log and replays onto an idempotent write. Two conditions that argument rests on: writes are
whole-document upserts keyed by the row's primary key, and an update whose
unchanged TOASTed columns arrive as markers is completed from the stored
document — without that, a replayed update would erase a value a range read
correctly.

**The table is cut the way the storage engine reads it.** PostgreSQL's heap
order says nothing about the key, so ranges are sampled in advance and read
unordered: `ORDER BY key LIMIT n` forbids a bitmap heap scan, and index order
costs random heap access on any key that is not physically correlated. InnoDB's
clustered index *is* the table, so MySQL does the opposite — `WHERE key > cursor
ORDER BY key LIMIT n` walks the rows themselves, nothing is sampled, and each
chunk's last key is the next chunk's cursor. That also makes the MySQL resume
point exact, where PostgreSQL has to store its boundaries because a second
sample would cut the table elsewhere.

**The cursor comparison is never a row constructor.** `(a, b) > (x, y)` says
exactly what the expanded `(a > x) OR (a = x AND b > y)` says, and MySQL plans
it as `type: index` with no usable key while the expansion plans as
`type: range`. Measured on a composite key, for 1000 rows returned: 1000 rows
read expanded, 2000 read as a row constructor, restarting at the head of the
index every chunk — so the multiplier grows with how far the cursor has
travelled. MySQL bug #111952, closed as not-a-bug with a worklog in its place.
No `IS NOT NULL` guard accompanies the comparison even though MySQL sorts NULLs
first: a `PRIMARY KEY` column is `NOT NULL` whether it was declared so or not.

**Every document carries the position it became visible at, as a target
document version.** Streamed rows carry their commit position, copied rows the
position read before their range. A copied row that is already stale therefore
loses to the streamed change at the target, whichever order the two arrive in,
and a version conflict is success rather than a rejection. It is deliberately
separate from the checkpoint token: a copied row needs a version and must never
advance a position. Poll mode, which has no position at all, writes no version
and relies on ordering alone.

**MySQL versions by its binlog coordinate, not by a GTID.** `(file index << 32)
| offset` is monotonic across rotation and was already the ordering token, and a
transaction's events are written to the binlog as one group at commit — so no
position inside a group can predate a coordinate a reader saw earlier, which is
what makes an event's own offset a sound version. A GTID could not be one: it is
`source_uuid:N` with `N` restarting at 1 for each UUID, so a GTID set has no
order as an integer. MariaDB is the exception that proves the rule — its
sequence number is one monotonic 64-bit counter per replication domain — and one
version scheme for both servers is worth more than exploiting that.

**The version carries a generation, so the coordinate space can change.** The
version is `base + ((file index << 32) | offset)`, with `base` persisted beside
the checkpoint. That space is per server and per binlog history, and a failover
moves to a different one: the new coordinate may be *lower* than what the target
already holds, and `external_gte` would then refuse every write and leave the
index quietly stale. So when the source is behind the checkpoint and there is a
GTID position to resume the stream from, a new generation opens at
`stored token + 2^40` instead, and every later version outranks everything
written under the old numbering.

The margin has to exceed the highest version written but not yet acknowledged.
That gap is bounded by how much binlog one unacknowledged transaction can span —
a few file rotations, so a few multiples of `2^32`. `2^40` is a thousand
rotations of headroom and still leaves room for millions of generations in a
`u64`.

Without a GTID position the refusal stands: a coordinate behind the checkpoint
then means we can neither continue the stream nor trust the numbering, and
reloading into silence is the one outcome worth refusing.

**GTID is the resume position, never the version.** Binlog file names and
offsets are per server, which is why MySQL's own `GTID_ONLY` exists to stop
persisting them; a checkpoint holding only a coordinate cannot resume anywhere
but the server it came from. So the checkpoint carries a GTID position as well,
inside the source's own position text — `core` says that text is the source's
business and nothing else parses it.

The set is accumulated from the stream, one GTID per commit, and never read from
`@@GLOBAL.gtid_executed`: that describes what the *server* holds, including
transactions we have not consumed, so resuming from it would skip data.

The two servers share no mechanism for asking. MySQL has `COM_BINLOG_DUMP_GTID`
carrying the set in binary; MariaDB has no such command at all and switches into
GTID mode on the presence of `@slave_connect_state` alone. Both are implemented
rather than one being emulated, because the difference is in the server and
neither is a dialect of the other.

Anything that would leave the set incomplete refuses to use it rather than
checkpointing a lie: a tagged GTID event, which MySQL 8.4 gives a type of its
own, and an anonymous transaction under `gtid_mode = ON_PERMISSIVE`, which has
no GTID to record at all.

**A write the stream has already removed is dropped, not offered.** Versioning
alone does not make the overlap safe, and this is the one place it does not. A
versioned delete leaves a tombstone carrying the delete's version, the target
keeps that tombstone only for `index.gc_deletes` — 60s by default — and once it
is gone `external_gte` accepts *any* version, including one below the delete's.
So a copied row starved behind a busy stream for longer than that would put the
document back. Measured against a real target at `gc_deletes = 1s`: the same
write is refused with a 409 immediately and accepted two seconds later. `TRUNCATE`
has the same shape, since it clears an index with versioned deletes.

The engine therefore remembers what the stream removed and drops a copied row
that is older than the removal, rather than asking the target for a comparison it
cannot make. This is the move DBLog makes for the same problem — it buffers a
chunk and removes every key the log touched between two watermarks — except that
watermarks exist there to substitute for versions, and versions already order
everything else here, so only the case they cannot express needs it.

What bounds the state is the load's own protocol: a chunk's rows, then a mark,
then a *wait* for that mark to be written. When a mark arrives, every row of its
chunk has been handed over and none of the next chunk can exist yet, so the
window closes and the memory is one chunk's worth of deletes rather than the
load's. A loader that sent the next chunk before its mark was confirmed would
reopen the hole silently, which is why the ordering is stated in both places.

Raising `index.gc_deletes` for the duration of the load was the alternative and
is rejected: it moves the window instead of closing it, costs target heap for
every tombstone it holds, and an interrupted load would leave the setting raised
the way one already leaves `refresh_interval` at `-1`.

**The load runs beside the stream, not before it.** Loading first means nothing
acknowledges a position for the load's whole duration, so retained WAL grows
monotonically and a large enough table invalidates the slot — which forces the
full reload the load was trying to finish. Alternating copy and catch-up phases
does not fix it either: on PostgreSQL a paused consumer freezes `restart_lsn`
whether it detaches or merely stops reading, so the only thing that releases WAL
is continuing to consume it. Document versioning is what makes the overlap safe;
change events take strict priority over copy rows, and the copy pauses between
ranges while the slot's `wal_status` is anything but `reserved`.

**On MySQL the load overlaps the stream but never pauses for it.** The hazard
runs the other way there: a slot retains WAL until it is consumed, so a slow
consumer is what invalidates it, while MySQL purges binlogs on its own time and
space policy and ignores consumers entirely. Nothing accumulates because of us,
and what can go wrong — the file we still need being purged — is made likelier
by holding the load back, not less. So there is no `wal_status` analogue to
watch and deliberately no pause.

**Load progress is recorded per range, in the target, behind a durability
barrier.** The order is strict: rows, then a mark the sink reports once they are
written, then the progress document. A crash can therefore lose forward
progress and redo a range, which idempotent writes make free, but can never
claim a range that was not written. What the progress says depends on how the
table was cut: PostgreSQL stores its sampled boundaries alongside a count of
finished ranges, because recomputing them would cut the table elsewhere and the
count would name a different span of rows; MySQL stores the last key written,
which needs nothing else to be exact. A checkpoint alone is not proof the
load finished — the two are separate facts, and conflating them is what
silently skips a load.

**The load reads in waves, and only for the tables where reading is the cost.**
Parallel readers were the obvious answer to a slow load and are worth almost
nothing on an ordinary table: measured, four readers buy 8% on a narrow table
and 5% on a wide one, because the target is what the pipeline waits for. On a
table with a nested collection they buy **53%** — there the `COPY` runs an
aggregate subquery per parent row, so the *server* is doing per-row work and
more backends do it in parallel. That is the whole justification, and
`[source] load_workers` stays at 1 because outside that case it multiplies the
read load on someone's production database for single digits.

Waves rather than a free-running pool, and that is not a matter of taste. The
engine forgets its record of stream-removed keys on every load mark, which is
only safe while nothing from before that mark is still in flight; with a pool,
one worker's confirmed mark says nothing about the others. And progress is a
count of *leading* ranges written, which out-of-order completion cannot advance.
A wave satisfies both by construction: it is contiguous, and it is finished
before its mark is sent. The cost is the skew inside a wave, which sampled
ranges of equal row counts keep small.

The tombstone window is therefore bounded by a wave instead of a chunk — the
same argument, `load_workers` times wider.

**The load is made faster on the write side, because that is where the limit
is.** The obvious move is parallel readers, and it would have bought nothing.
Measured on an 8-core laptop against the dev stack, 2M rows: one `COPY` hands
over rows at ~1,050,000 a second, while the whole pipeline ran at 43,000 and
spent 63% of its wall clock idle. The target was the reason — one bulk request
open at a time tops out near 52,000 documents a second, and its size makes no
difference at all (50,500 at 500 documents a request, 52,100 at 20,000), while
four requests open at once reach 114,000. Refresh and replicas are already
suspended for the load, so concurrency was the only variable left.

Opening more requests is therefore the whole change, and it delivers: 43,000
rows a second at one, 67,000 at two, 87,000 at four, 96,000 at eight. The
process's CPU share over the same runs went from 37% of wall clock to 102%,
which says plainly what happened — it stopped waiting and started working. At
10M rows the same shape holds and the numbers barely move — 42,700 at one,
90,100 at four — so this is not an artefact of a table that fits in cache.

Wide rows do not change the answer. A TOAST-heavy table reads at 11,200 rows a
second through a client and gets *slower* with parallel readers, because what
saturates is transporting the data, not the backend producing it; server-side the
same read scales to 141,000 with four readers, so PostgreSQL is not the problem
and neither is our connection count.

**Write requests are open concurrently and completed in order.** Concurrency
that reordered completions would break three things at once, so it does not: a
position is acknowledged only after every batch sent before it is durable, a
refused document is filed before the position covering it passes, and a failure
acknowledges nothing behind it. Load marks, truncates and bare positions are
barriers that wait for the open writes to finish — for a load mark that is
required rather than tidy, because the engine forgets its record of
stream-removed keys on one, and that is only safe while the mark still means
every copy row before it is durable.

It stays at one request by default. Raising it multiplies the load placed on
someone's production target, which is not a default anyone should inherit
unmeasured, and it needs a target that decides between two writes by their
version: Meilisearch keeps whichever landed last, so it refuses the setting
outright rather than reordering writes quietly.

**A re-snapshot is a subcommand, not a signal table.** Debezium triggers an
ad-hoc snapshot by writing to a table in the user's database. pg2osync will not
write to the source, and the CLI is already where operator actions live, so
`pg2osync resnapshot --table` reads one table again into the index it is mapped
to. It is the initial load's chunked reader with a scope, going through the whole
ordinary write path — mapping, projections, transforms, children, id derivation —
because a document it writes has to be indistinguishable from one the load wrote,
and a second write path would drift immediately.

It cannot move the checkpoint by construction rather than by care: its rows carry
position `0`, so nothing acknowledges a position and the checkpoint task has
nothing to persist. That is what makes it safe beside a running pipeline, together
with the versioning that already orders a copied row against a streamed change.

It records no progress. An interruption means running it again; the alternative is
bookkeeping under the key the initial load uses, which the next pipeline start
would read as an unfinished load — the silent skip the load's own progress
documents exist to prevent. It also leaves `refresh_interval` alone, unlike the
initial load: it repairs an index that is in use, so hiding new writes for its
duration would be the wrong trade.

It adds and updates but never deletes. `reconcile` is the other half, and keeping
them apart is what keeps each one explainable.

**Children resolve in the source, once per transaction.** The engine is
source-agnostic and runs no SQL against the source, so the only place that can
group child lookups is the source's own decode loop — which already knows where a
transaction begins and ends. Rows of tables with no children go straight out; the
rest are held, and at the commit the distinct parents they affect are read in one
query per collection. A child row holds nothing but the parent key it names, so a
transaction touching a thousand children of one parent holds one key rather than a
thousand rows, and writes one document rather than a thousand identical ones.

Measured on 2,000 child rows across 20 parents in one transaction: 2,000 parent
re-reads and 2,001 child fetches became 1 and 2, documents written fell from 2,000
to 20, and throughput went from 845 to 2,829 rows/s. Every competitor breaks on
this cost model — PGSync asks the *index* which documents a child row affects and
was measured at 108s per batch; asking the source, once per batch, is the whole
difference.

**A MySQL child array is aggregated in Rust, not by `JSON_ARRAYAGG`.** The
obvious tool is the wrong one, measured on both servers: `JSON_OBJECT` renders a
`varbinary` as `"base64:type15:AP8Q"` on MySQL and as raw escaped bytes on
MariaDB where the pipeline says `"AP8Q"`, a `set` as `"a,b"` where the pipeline
says `["a","b"]`, a `decimal` as a JSON number where the pipeline keeps the
string so the precision survives, and a `bit` as base64 on MySQL and as *invalid
JSON* on MariaDB — its own `JSON_VALID` returns 0 for it.

Casting each column (`TO_BASE64`, `CAST(… AS CHAR)`, `CAST(… AS UNSIGNED)`) gets
closer and still fails: `TO_BASE64` wraps at 76 characters, so any value over 57
bytes disagrees with the pipeline's base64, and a `set` cannot become an array
without `JSON_TABLE` per row. And where it does work it means writing the type
mapping a second time, in SQL, for the two to agree.

So child rows come back as ordinary rows and go through the same
`build_document` that builds a parent. A value inside an array is then identical
to the same value as a document because it is the same code, and the cost stays
one query per collection per batch — the server still does the ordering, the cap
and the count.

**The child aggregation is built in one place.** The initial load's `COPY` and the
streaming re-fetch use the same subquery, so the array's contents, order and cap
cannot drift between them. Two builders would disagree the moment either changed,
and the disagreement is invisible until someone re-snapshots.

**The array is ordered by the child's primary key.** Without an order it is a set
in arbitrary order, so the two paths could embed the same children differently and
a re-snapshot would rewrite documents for no reason. With a cap it decides *which*
children are kept, so the same subset has to come back every time.

**No cap on an embedded collection by default, and truncation says so.** A cap
loses data, and the bound that matters is already the target's: past
`index.mapping.nested_objects.limit` (10,000) OpenSearch refuses a `nested`
document outright, which is reported and quarantined rather than lost. So the
default embeds everything and logs an array past that limit, naming the parent.
Where `max_rows` is set, the document carries `<field>_truncated` and
`<field>_total` — a consumer cannot otherwise tell a short array from a complete
one, and handing over part of a collection as if it were all of it is worse than
either extreme. Data Prepper's equivalent defaults to 1000 and documents no
overflow behaviour at all, which is the version of this not worth copying.

## Checkpoints

**State lives in the target.** A hidden `.pg2osync_meta` index holds one
document per stream; per-document atomicity gives the crash safety for free,
with no compare-and-swap. Per stream rather than one shared document, because
two pipelines writing to the same target otherwise overwrite each other's
position — which is what a zero-downtime re-index runs, and what splitting
tables across instances means. A local file breaks on ephemeral containers, and a table in
the source database pollutes the user's schema and risks replicating itself.
Meilisearch has nowhere to put an arbitrary document, so it uses a
write-then-rename state file — the documented exception.

**One position format for every source.** The document stores an ordering token
plus the source's own textual position. Documents written by earlier versions,
which stored only an LSN, are still readable: refusing them would force a full
re-index on upgrade.

## Types

**`numeric` and `decimal` become JSON strings.** A float round-trip loses
precision, and these columns are usually money. MySQL decimals keep their
declared scale so a streamed value matches what the initial load read.

**Unknown types become strings.** Domains, ranges and composites are passed
through as text rather than guessed at.

**`bytea`, blobs, binary and geometry become base64.** Binary cannot go into JSON
any other way.

**On MySQL both readers decide from the declared type, not from the wire.**
Neither format is self-describing where it matters. A binlog row image gives a
string column no charset, so `char` and `binary` share a type code and so do
`text` and `blob`; it gives an enum an ordinal and a set a bitmask with the
labels nowhere. The text protocol has the opposite gap: every value is bytes and
only the declared type says whether they are characters. So the shape is resolved
from `information_schema` once — `column_type` alongside `data_type`, because
that is where the labels live — and both decoders consult it. Deciding per format
instead is what made `text` arrive as base64 from the stream and as a string from
the load, and `varbinary` arrive as mangled text from both.

**A MySQL `enum` is its label, a `set` is an array of its labels, and a `bit` is
a number.** The alternatives are what the wire happens to carry — an ordinal, a
bitmask, a byte string — and none of them is searchable, which is the only reason
the document exists. A `set` is an array rather than a joined string so each
label matches on its own. `bit` fits a number because MySQL caps it at 64 bits.

## Operating limits

**Retention is reported, never capped by us.** A slot nothing is reading pins WAL
until the disk fills, and `max_slot_wal_keep_size` is the one setting that turns
that into a recoverable incident. pg2osync still will not set it: it is a
server-wide setting, and writing to the source's configuration is the same
refusal as not running DDL and not writing a signal table. What is owed instead
is that the number cannot be missed — `pg2osync_slot_retained_bytes` for every
slot on the server, the server's own `wal_status` beside it, and a startup
warning naming what an idle slot already holds.

Measured, so the risk is a number rather than a caution: a 110-byte row retains
238 bytes of WAL, which is ~820 MB an hour at a thousand writes a second.

**The check has to work while the pipeline is down.** That is the case that takes
a database out — a process stopped on Friday, nobody reading logs for something
that is not running, metrics unscraped because nothing is serving them. So
`pg2osync status --max-retained-mb` exits non-zero over a limit, which makes it
something a cron job can own, and it looks at every slot rather than the
configured one: an orphan from a former `slot_name` fills the same disk.

## Scope

**One-way replication only.** No bidirectional sync, no conflict resolution.

**Schema drift is reported, never applied.** pg2osync will not run DDL on the
target's behalf. A publication that does not match the configuration is an
error, not something to silently fix. A table whose columns change under a
running pipeline is logged, naming what was added, removed or retyped — the
index and the database now disagree about what a row looks like, and only a
rebuild closes that. Which is why the index name is configuration.

**No relational sources beyond PostgreSQL and MySQL/MariaDB**, and no
non-relational sources. The value is depth on these, not breadth.

**Nested children stay one level deep.** Deeper denormalization belongs in a
view or in the application.

## Implementation choices

**Hand-rolled metrics endpoint.** Six counters and one summary do not justify a
Prometheus client plus an HTTP framework in a binary that advertises having no
dependencies.

**Batch reads with `COPY … (FORMAT text)`.** Text parsing measured fast enough
(~21k docs/s end to end) that binary format's complexity is not yet justified.

**Secrets from the environment.** Every secret has an `*_env` form. Inline
values still work but warn, because config files end up in version control.

**Errors: `thiserror` in libraries, `anyhow` only in the binary.** Callers get
matchable variants; the CLI gets readable messages.

**YAGNI on configuration.** An option that does nothing is worse than a missing
one, because it implies a guarantee. Options that had no effect were removed
rather than documented.
