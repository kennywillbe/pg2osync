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

**The id is configurable; the default is still the primary key.** (#62.)
`[sync.x] id = "tenant-{tenant_id}-{id}"` renders an id from literals and
placeholders; a table that configures nothing is filed under its key
byte-for-byte as before, so no existing index needs a rebuild. Three rules
make that safe, and they are the reason the feature was slow to arrive:
identity renders from the row's **raw** values — before projections and
before transforms, because identity is a property of the row, not of the
projected document; exactly one place mints ids (`materialize` and
`completion_key` in the engine), so the stream, the load, the re-snapshot and
poll can never disagree; and a NULL in any column an id names halts the
pipeline rather than inventing a name. An id that references columns outside
the key additionally needs the row's before-image to delete and move its
documents, which is why `run` refuses such a table unless PostgreSQL reports
`REPLICA IDENTITY FULL` — on MySQL `binlog_row_image = FULL` already
guarantees it.

**One row can fan out into many documents.** (`fan_out`, #62.) A JSON-array
column can be indexed as one document per element — each the parent-minus-array
document merged with the element — so a search can match a single tag without
the whole parent. The documents of one row then share a key, and identity has
to say something about that too, which is why the element `id` is a second
template rendered from the merged document. Deletes and update-diffs are
computed from the row's before-image and issued as ordinary per-document
versioned writes: `delete_by_query` was rejected because it cannot carry an
external version and would have to act as a barrier, breaking
`write_concurrency`. The `Sink` trait is unchanged. Reconcile and re-snapshot
refuse fanned tables for now — both page by key, and one row is no longer one
document.

**Parent-child can be a join field, not only an embedded array.** (`join`, #60.)
An embedded array is one document and one write, and the right answer nearly
always; a join field is for children that are many, change far more often than
the parent, or must be searched in their own right — re-fetching a 50,000-row
collection because one child changed is the cost it avoids. What it costs: the
two tables share an index and a shard, so routing rides on every operation that
touches a child — bulk actions, the `_mget` behind TOAST completion, reconcile's
deletes, a quarantined document's replay — and a parent delete cascades through
a search, refreshed first, rather than an id list the engine could have built,
because the engine does not know which children the target holds. A join child
needs `REPLICA IDENTITY FULL` unless its parent column is part of its key:
routing comes from the same place identity does. Exactly one parent, one shared
field, no fan-out on a join table, and a parent id naming anything outside its
key is refused at config load — the child holds one column and computes the
parent's id from it alone. Ids must be unique across the shared index, which
config cannot see, and `TRUNCATE` on either table clears its relation only —
the join field is what tells the halves apart. A table that is both an embedded child
and a section of its own is warned about at startup rather than refused — a
load-once index is a legitimate thing to want — because the runner reads its
rows only as a re-fetch of the owner, so its own index receives the initial
load and no streamed change.

**A column can route a document; the rule is the id rule.** (`routing`, #109.)
Shard co-location arrived as a by-product of `join`, where a child has to live
on its parent's shard. It is worth having on its own: hundreds of small tenants
in one index, each query reading one shard. An index per tenant is the
alternative, and it is the wrong one at that shape — every index costs shards,
and shards cost memory whether they hold ten documents or ten million. A bare
column name, not a template: a routing value has no grammar to check, nothing
downstream parses it, and a composite routing key is a use case nobody has.
The value is read from the raw row, before projection and transforms, because
a projection must not be able to move a document to another shard — and NULL,
missing or empty halts, since the target rejects an empty routing and a silent
fallback to the default shard would hide the document from every routed query.
A routing column can change, which makes it identity's twin: the document is
written under the new routing and deleted under the old, through the same
comparison a changed `id` or a changed index template goes through, and a
non-key routing column therefore needs `REPLICA IDENTITY FULL` for the same
reason a non-key `id` does. Refused together with `join`, which already owns
the child's shard; refused on Meilisearch, which ignores routing. `reconcile`
is not refused: it never derives a routing, it reads each hit's `_routing`, so
the only thing it cannot see is a duplicate left under a stale routing — and
that document's row is still there, which is precisely what reconcile does not
collect.

**Several tables can feed one index once each declares its identity.** (#61.)
An index built before pg2osync is usually a union of several tables, and what
the old refusal protected against was never the union: it was `_id` inherited
from each table's own key, where two tables with a row `1` become one document
by accident. An explicit `id` on every section sharing the index is the
declaration that removes the accident — `user-{id}` on both sections still
collides, but now it is something the operator wrote down, and nothing checks
the values because nothing can see them. Two things cannot be recovered:
`reconcile` pages an index by one table's key column, so every other table's
document would look like an orphan, and it refuses; and `TRUNCATE` clears an
index, which would wipe tables the source never truncated. It is not halted
on — a halt would replay the same event from the slot at every restart with
nothing the operator could change — but skipped, logged and counted, and the
truncated table's documents stay until cleared by hand. A join pair escapes
that: its relation name is exactly the set of documents to clear.

**A row can choose its index; the rule is the id rule.** (#69.)
`index = "events-{tenant}"` renders the index from a column, and a name
derived from a column is the same problem as an id derived from one: the
column can change, and the document is then in the old index. So it is the
same template, rendered from the same raw row, through the same ladder — the
row, else the before-image, else the bare key, else halt — and
`(old index, old id) != (new index, new id)` is the move `id` already handles.
The index is created on demand, at the first document that needs it, rather
than ahead of time: pre-creating means enumerating a column's values, which
nothing can do without querying the source for a search concern, and
recording the glob and creating on first use keeps the mapping the operator
configured. `reconcile` is refused because it pages one index by its key
column, and a templated table's documents are spread over every index the
template renders — there is no single index to page. And a template must
carry a literal, because `TRUNCATE` clears what the template claims, and a
claim of `*` is a claim on the cluster.

**Vectors are the target's to compute.** (`pipeline`, #68.) Semantic search
needs an embedding per document, and the obvious design — an embedding client
inside pg2osync — puts a network hop and a rate limit inside every batch, adds
a second failure mode that backpressure would have to respect on top of the
target's, and makes a model choice that is not this project's to make. An
ingest pipeline gets the same result for one config field: the section names
the pipeline, every document it writes carries `"pipeline": "<name>"` on its
bulk action, and the target — which already owns the model, the plugin and
the `knn_vector` field — computes the vector on the way in. The document still
travels the one write path, so a quarantined document replayed later goes
through the pipeline again instead of landing without its vector. It is per
section, not per index, because the pipeline rides on the operation rather
than on the index: two tables feeding one index can embed different columns.
A delete carries none, since an ingest pipeline runs on index actions only.
Meilisearch has no ingest pipelines, and refuses the option at config load
rather than ignoring it.

**A table without a key syncs as insert-only, under a content hash.**
(`append_only`, #70.) The key requirement was right for a mutable row — an
update or a delete has to find the document the row already owns, and only a
key says which — and wrong for an event log or an audit trail, which never
updates or deletes and was refused for a case that never arises. Declared
`append_only`, a table files each row under a sha256 of its raw values as
canonical JSON. Not the source position: the initial load has no per-row
position, and the same row has to hash the same on every path — COPY, WAL,
poll, MySQL load and binlog — which key-sorted JSON of the raw row gives and a
position never could. Two identical rows are therefore one document, and that
is the at-least-once guarantee restated: a replayed row is the same document,
and a duplicate the source itself cannot tell apart is not one the index
should invent a difference for. An UPDATE or DELETE that arrives anyway halts
the pipeline, naming the table, rather than being missed: nothing can say
which document it addresses. `init` writes the flag for a keyless table
instead of refusing it, so the smallest config still runs and the declaration
sits where the operator will read it.

**A column can be renamed in the target; the rename is the last step.**
(`fields`, #66.) The source name is the one namespace the operator already
knows, and the one every other check — projection, `transform`, `id`,
`primary_key`, `soft_delete`, `poll_column` — is written against, so it stays
the name those options use. Renaming last, after identity, projection and
transforms, means none of them has to know a rename exists. The one place the
target name leaks back in is TOAST completion, which reads the stored document
and so translates the column through the same map. A rename onto a name
another surviving column already has is refused wherever it can be seen — at
config load and by `validate` against the live catalogue; at write time the
renamed value wins.

**A document can carry fields that come from no column.** (`constants`, #67.)
The alternative is a generated column: DDL on someone else's production table
for a search concern, the same objection this project raises against event
triggers and signal tables. Constants only, no expressions: the README promises
no transformation language, and a language is a parser, an evaluator and a
null semantics to own forever, where an entity tag or a `{schema}.{table}`
origin marker is the whole of what was asked. The two placeholders render once
at startup, so the engine inserts literal JSON and stays source-agnostic.
Constants are added last because `columns` would otherwise strip a field that
is not a column; written last, a constant wins a collision — so every collision
the configuration can see is refused at load, and the one only the catalogue
can see is refused by `validate`.

**Transforms are fixed, named reshapes, not a language.** (#63.) Eight ops —
`hash`, `redact`, `json`, `split`, `number`, `date`, `lookup`, `pseudonym` —
and the README's promise holds: a closed set, one literal parameter at most, no
chaining, nothing evaluated at run time, for the same reason constants carry no
expressions. A value a *reshaping* op cannot convert is indexed as it arrived
and counted, neither halted on nor nulled: halting turns a data-quality problem
into an availability problem, and a NULL is silent loss. The target's mapping is
the arbiter of what a field holds, quarantine already catches what it refuses,
and `pg2osync_transform_unconverted_total` plus one warning per (table, column)
keeps the rest visible. A *protective* op is the exception: `pseudonym` writes
`***` instead, because indexing what the op was asked to hide is the one outcome
worse than losing it. A value already in the target shape is not a failure,
so every op is idempotent under at-least-once replay. `split` cannot feed
`fan_out`, because fan-out reads the raw row — identity is a property of the
row, as the id paragraph says. `chrono`, already in the build through the
OpenSearch client, parses the dates: a strptime calendar is not protocol code.

A seventh op, `lookup` (#142), maps a status code to the label it is searched
by. A dictionary is data, not a language: the table is closed, written out in
the configuration, and evaluated as one hash lookup on the value's text form —
nothing is parsed, nothing is computed, and no key can name another column, so
there is still no expression to evaluate at run time. The alternative is a view
or an application-side label column, the same DDL-on-someone-else's-table
objection `constants` answers. A value the dictionary does not know is counted
under `pg2osync_transform_unconverted_total` rather than halting the pipeline,
for the reason the paragraph above gives: a code nobody declared a label for is
a data-quality problem, and halting would make it an availability one. It keeps
the original value, or `default` when the configuration would rather every
document carry a label; either way it is counted, because the counter measures
what the dictionary failed to cover, not what was written.

**A pseudonym is keyed and deterministic, so joins survive.** (`pseudonym`,
#143.) `hash` is one-way and truncated: two values can collide, so it is unsafe
on a unique or a foreign-key column, and nobody can get the value back. AES-SIV
(RFC 5297) is deterministic for the same key, plaintext and associated data, so
equal values give equal tokens across tables and across runs — a join on the
pseudonymised column still joins, uniqueness survives, and the key holder can
decrypt. It is chosen over a keyed hash because it is invertible; over a
random-nonce cipher because that breaks equality, which is the whole point.

The associated data is a *scope*, and it defaults to the column's own
`schema.table.column`, so a token cannot be replayed into another context. A
foreign key must therefore name its parent's scope explicitly
(`scope = "public.users.id"`): the default is fully qualified, and two columns
that must join have to spell the same label.

The key arrives only as `key_env` — 128 hex characters, a 64-byte AES-256-SIV
key — never inline, never in a log, an error or a `Debug`. A transform whose
`key_env` has no value is refused when the rules are built rather than at the
row, because a missing key at run time could only fall back to publishing the
plaintext. An unconvertible value is redacted for the same reason, and so is the
(unreachable) encryption failure: this op fails closed.

There is no `decrypt` subcommand. It would want the key on a command line, it
would want the scope too, and the construction — base64url of
`AES-SIV-Encrypt(K, headers = [scope], plaintext)`, documented in
[configuration.md](configuration.md) — is four lines with any RFC 5297 library.
What this buys is pseudonymisation, not anonymisation: it is reversible by the
key holder, so under GDPR the index still holds personal data, and because it is
deterministic a low-cardinality column is de-anonymised by counting. `hash`
stays for the cases that want no key and no way back.

**A row filter is SQL the database also runs, evaluated once more in the
engine.** (`where`, #64.) A subset, because a stream has no query to push a
predicate into: the engine evaluates it on every WAL, binlog and polled row, and
everything it accepts is valid on both sources, so the load pushes it down
unchanged. Three-valued logic is SQL's — NULL is unknown, only TRUE matches — so
the two evaluations agree. Strings compare byte-wise, exact for equality and for
ASCII and ISO 8601 order, so `created_at >= '2024-01-01'` holds against a
textual timestamp; a number against a string holding one compares numerically,
because `numeric`/`DECIMAL` arrive as strings on purpose. A row that leaves the
filter is deleted the way a moved id is, the before-image naming a fanned row's
element documents. An insert that never matched still costs one idempotent
not-found delete; the alternative is remembering what was written. Poll mode
does not push the predicate down: a row that left the filter has to keep
arriving to become the delete it now is. It travels the ordinary op path, so the
guard against a load resurrecting a removed document holds. A filter selects
rows and computes no values: no transformation language.

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
correctly. A completed value is copied as it is stored, already transformed,
and is not put through the transforms again: a hash of a hash would drift from
what a fresh write of the same row produces.

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

**The load can be capped, the stream cannot.** `[engine] load_max_rows_per_sec`
is a ceiling on how many load rows a second the pipeline takes; unset, which is
the default, means none. It stands beside the reactive pause on `wal_status`
rather than replacing it, because the two protect different things. The pause
protects the replication slot and only fires once the server is already
straining. The cap protects the source's CPU and IO before anything strains, for
the case no measurement of ours can see: a DBA who has decided the load may not
cost more than n rows a second while the business day is on. Nothing here can
work that number out, so there is no default — it exists only when an operator
names it.

Rows a second, not bytes a second. Rows are the unit the load is already watched
in — `pg_stat_statements` and `SHOW PROCESSLIST` count them, and so does our own
load summary — and a byte ceiling would be a number nobody could convert into
what they are looking at. Wide rows make a row ceiling conservative, which is the
direction worth being wrong in.

It is enforced in one place: the engine's intake of the copy channel, where both
sources' loaders and all three commands that run a load — `run`, `resnapshot`,
`reindex` — converge. A token bucket refilled per second, holding at most one
second of allowance so an idle stretch cannot be spent as a burst later. That
channel is bounded, so a throttled intake reaches back and stops the reads
filling it without either loader having to know. The stream keeps its own channel
and is never counted: on PostgreSQL, holding the stream back is precisely what
fills the slot, so a cap that covered it would turn a load that is merely slow
into a slot that is lost.

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

**A rebuild is a fresh index and an alias flip, not an in-place rewrite.** (#107.)
A mapping cannot be changed on a live index for anything that matters, so
rebuilding one means writing a new one; rewriting in place would leave an index
that is half old and half new searchable throughout, which is the outage the
exercise exists to avoid. `pg2osync reindex --table T --alias A` therefore
creates `<index>-<unix seconds>` with the section's mapping, loads the table
into it, checks the count against the source, and moves the alias onto it in
one atomic request.

It refuses to run while the stream is live, which is the one place it differs
from a re-snapshot. A re-snapshot is safe beside the stream because a copied
row and a streamed change meet at the target and the higher position wins — a
comparison between two documents in *one* index. A fresh index the stream is
not writing to has no second operand: a row updated during the load would be
permanently wrong there and the count would still match. So a rebuild closes
the window the way the initial load does — its rows carry position `0`, the
checkpoint does not move, and restarting the pipeline against the new index
replays everything committed since. The refusal is positive evidence rather
than a flag: an active replication slot on PostgreSQL, and on either source a
checkpoint that moves while it is watched. There is no `--force`.

The alias is flipped before the restart because it costs nothing to: the old
index is exactly as stale as the new one at that moment, and both catch up
from the same replay. A live cutover with no freshness gap at all is still two
instances, as [operations.md](https://github.com/kennywillbe/pg2osync/blob/main/docs/operations.md)
describes — dual-writing from one process is not on the table.

`refresh_interval` *is* suspended for the duration here, unlike a re-snapshot's:
nothing searches an index no alias points at yet, so there is no visibility to
trade away. The old index is kept unless `--drop-old` says otherwise, because
it is the rollback — one alias flip back — and a `--keep-old` that defaults to
on would be an option that does nothing. The count proves the number of rows,
not their contents; what proves the contents is the replay the restart runs.

The config edit the rebuild leaves to the operator is the step that fails
silently, so `[target] require_alias` (#150) lets a deployment refuse a write
whose target is an index rather than an alias — the flag both engines carry on
a bulk action. Opt-in, because writing to an index directly is a perfectly good
deployment and not one to break; a claim about how a deployment is run is the
operator's to make. It is a *permanent* rejection when it fires: a name that is
an index and not an alias is not going to become one on a retry, and quietly
retrying would be the drift the option exists to stop. `validate` checks every
fixed index against `/_alias/<name>` for the same reason the ingest-pipeline
check is there — the flag would otherwise only report the mistake one refused
batch at a time, after the pipeline had already been configured wrong for as
long as it took to notice. A templated index is refused with it rather than
enforced against: the index a row names is created on demand and no alias
stands in front of a name that did not exist a moment ago. A rebuild is refused
with it for the same shape of reason — the section names the alias, and
`reindex` needs a fresh index *and* a separate name to point at it — so the
option comes off for the rebuild and goes back on afterwards, with the section
left naming the alias rather than the new index. That is the config edit the
option exists to make unnecessary, so there is nothing to forget.

**An alias is a contract, not an API call.** (#108.) `Sink::switch_alias` names
an outcome — after it returns, the name readers use resolves to the documents
the rebuild wrote, and it never resolved to nothing in between — and leaves the
mechanism to the target. OpenSearch and Elasticsearch have an alias namespace
and move a pointer inside it. Meilisearch has none: the name readers use *is*
an index uid, and its atomic operation is `POST /swap-indexes`, which exchanges
the contents of two uids in one task. Modelling that as "this target has no
aliases" was the easy reading and the wrong one; the contract holds there, so
the sink implements it.

Two things follow from the swap, and the command says both out loud.
`--alias` on Meilisearch has to be the index the section already writes to,
because that is the only name a reader is using; any other value is refused
rather than quietly creating an index nobody reads. And the exchange runs both
ways, so once it is done the timestamped `<index>-<unix seconds>` holds the
*previous* documents, not the new ones — it is the rollback, `--drop-old`
deletes it, and the kept-index message says which name it is. No config edit
follows a rebuild here, only the restart, because the section's `index` never
changed. The checkpoint is a file in `state_dir` rather than a document in the
target, so it sits outside the uid namespace entirely and no swap can touch it.

A rebuild on this target is therefore never about a mapping — Meilisearch has
no field types to declare, and `ensure_ready` refuses a spec that carries a
mapping. What it is for is an index settings change that only applies to
documents indexed after it, or a decoding bug whose wrong values are already
written: both need the documents built again, and both want the live name to
keep answering until they are.

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

**A one-to-one child is an object, and a second row is a warning, not a choice.**
`single = true` unwraps the collection *after* the aggregation, in core, rather
than reading it with a `LIMIT 1` of its own: each source keeps exactly one
aggregation builder, so the initial load and the per-transaction re-fetch cannot
embed different shapes, and the ordering, counting and capping machinery is
untouched. A second matching row does not fail the run — a duplicate that exists
for the length of a migration must not halt an index — and it is not silently
resolved either: the batch logs one line naming the collection, how many parents
matched twice and the worst of them. The row that stands is the lowest-keyed one,
not the newest: primary-key order is what both the load and the re-fetch already
promise, so a re-snapshot embeds the same row rather than rewriting the document.
No metric counts it: neither source crate holds a `Metrics` handle, and a warning
already names what to fix.

**A many-to-many child is one more join inside the same builder.** `through`
names a junction table and changes nothing else: the aggregation gains
`JOIN junction j ON j.<through_key> = t.<child primary key>` and keys on
`j.<foreign_key>` instead of on a column of the child. The cap, the count taken
before the cap, the order by the child's primary key, `_truncated`/`_total`, the
projection and `single` are the same expressions they already were, in the one
builder each source has — a second builder for the many-to-many case is what
would let the initial load and a streamed re-fetch embed different arrays. It is
not a second level of nesting either: the document shape is unchanged and the
junction contributes no field of its own.

Both the junction and the child are streamed, because each carries half the
answer. A junction row *is* the relation, so it names the parent through its
`foreign_key` and takes the path a child row already takes today. A child row
does not name a parent at all — the row that would is one table away — so the
transaction's distinct child keys are turned into parent keys at commit by one
`SELECT DISTINCT j.<foreign_key> … WHERE j.<through_key> = ANY(…)` per
collection, and merged into the same named set. Everything downstream is
unchanged, so one parent read and one aggregation per collection per transaction
still hold; folding that lookup into the parent re-fetch instead would read a
parent twice whenever a junction row and one of its child rows changed together.

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
`transform = "number"` is the operator's explicit opt-out — an index that sorts
or range-queries on money asks for it, and accepts the double.

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

**No Amazon OpenSearch Serverless.** It looks like one more OpenSearch endpoint
and is a different target: SigV4 is the only authentication a collection
accepts, a custom document id works only on a *search* collection, and the
service owns refresh and index settings. The first rules out talking to it at
all without a signing implementation, the second would make the `_id`-is-the-
primary-key rule fail on two of the three collection types, and the third
removes the load's refresh suspension and `/synced`.

A `serverless = true` flag existed from the first commit and was never run
against the service. That is a support claim nobody could stand behind, so it is
gone and the url is refused instead. Nothing in the competitive set advertises
Serverless either — the tools that do are log shippers and AWS's own ingestion
pipeline, not database-to-index replication — so this closes no gap.

**Sources share a process, never a slot or a checkpoint.** (#146.) One process
was one source database, so thirty tenant databases were thirty processes, with
thirty sets of limits, scrape targets and probes for pipelines that are
individually almost idle. What they can share is the process. What they must
never share is anything the stream is identified by.

The form is a directory of configs — `--config-dir /etc/pg2osync` — rather than
a `[[sources]]` array inside one file. A file is what an operator already
templates, mounts and moves: one ConfigMap, one file per tenant, one `*_env`
per file. It also needs no schema migration and invalidates no example, because
every config that exists today stays a whole file. The accepted cost is that
`[metrics]` and `[api]` describe the process while sitting in a per-source
file, which is a validation rule rather than a redesign: the files that declare
one of those sections must declare the same one, and a file that leaves it out
takes what the others say.

A source is named by `[source] name`, or by its file's stem when the key is
left out. The grammar is `A-Z a-z 0-9 _ -`, because the name has to survive a
metrics label and a command line unescaped.

What the loader refuses is the point of it, and the slot is the one that
matters. A checkpoint is bound to its stream, and a stream is identified by its
slot name or its server id and by neither the host nor the file — which is
exactly why configs copied from one template are one stream, sharing one
`.pg2osync_meta` document and resuming from each other's position, however many
different databases they name. So two files sharing a slot, a server id or a
source name are refused before anything connects, and the message names the
checkpoint document they would have collided in. The
[shared-index](#correctness) and templated-index rules are the same rules as
within one file; they simply run over the whole directory as well, because an
index does not know which file a document came from.

`run --config-dir` then runs all of them, and `--source <name>` narrows it to
one. What the process shares is itself and its two listeners; what it does not
share is anything a pipeline is made of. Each source gets its own sink, engine,
checkpoint, retry policy and initial load, which is why a target outage on one
source is not one on another and why the sum of the configurations is what the
process costs. There is deliberately no global write budget: a shared semaphore
would make every source wait on every other one's slowest write, which is
precisely the coupling that running them apart avoided and that running them
together must not reintroduce. The sum of `[engine] write_concurrency` and
`batch_size` across the directory is the operator's to size.

Concatenating one exposition per source is not valid exposition — a family's
`# HELP` and `# TYPE` may appear once and its series must be contiguous — so
the sources write into one shared exposition and every series, including the
`pg2osync_slot_*` gauges, carries `source="<name>"`. A single `--config` run
gets the label too, so a dashboard written for one source keeps working when a
second one is added rather than silently summing the two. New with it is
`pg2osync_source_state{source,state}`, a state set over
starting/loading/streaming/reconnecting/halted/stopped: with several pipelines
in one process, "is it up" stopped being a question with one answer.

Health follows from that. `/healthz` stays an unconditional liveness answer:
returning 503 because one source of thirty is halted would have the kubelet
restart the twenty-nine that are working, in a loop that cannot fix a
configuration the source will keep rejecting. Readiness per source is
`/healthz/<name>` — 200 while starting, loading, streaming or reconnecting, 503
once halted or stopped, 404 with the known names for anything else — and
aggregation is the alert's job, not the probe's. `/synced` takes `?source=`:
one source and no parameter is exactly today's behaviour, several and no
parameter is a 400 naming them, because answering for whichever pipeline came
first would tell a caller its write is visible when the pipeline carrying it
has written nothing. Sources register with the endpoint as they connect rather
than before the listener opens, since MySQL cannot render a position until it
has read the binlog prefix off the server; one that has not registered yet
answers 503 rather than being mistaken for a name that does not exist.

The subcommands split in two on the same question. `run`, `bootstrap`,
`validate` and `status` describe or act on the whole set, so a directory is
every source in it and `--source` narrows to one. Everything else —
`resnapshot`, `reindex`, `switch-alias`, `reconcile`, `drop-slot`, `setup-sql`,
`rejects` — changes what one source owns, and a directory does not say which,
so it is refused until one is named. Acting on the first file would be a guess,
and looping over all of them would make `reconcile --delete` and `drop-slot`
thirty times as destructive as the operator asked for. `init` takes no
directory at all: it writes the first config, and there is no set yet to write
into.

A halted source is a state, not an exit. One source failing — a permanent
rejection, an exhausted quarantine, a `reconnect_max` run out — is logged
against its name, sets its state and leaves the others streaming; the process
exits non-zero only when every source has halted, which for a single source is
bit-identical to what it always did. What is not retried is the setup before
streaming begins, so a source unreachable at boot halts rather than waiting for
its database to come back; that is [#172](https://github.com/kennywillbe/pg2osync/issues/172),
not this change.

## Scope

**One-way replication only.** No bidirectional sync, no conflict resolution.

**Schema drift is reported, never applied.** pg2osync will not run DDL on the
target's behalf. A publication that does not match the configuration is an
error, not something to silently fix. A table whose columns change under a
running pipeline is logged, naming what was added, removed or retyped — the
index and the database now disagree about what a row looks like, and only a
rebuild closes that. Which is why the index name is configuration.

It is also counted, as `pg2osync_schema_drift_total{table}`. A log line is not
alertable: an operator who does not read logs never learns the index and the
table stopped agreeing, and "reported" that nobody can be paged on is barely
reported at all. The report reaches the counter through the change-event
channel, as a positionless `SchemaDrift` event the engine counts and drops —
the same path rows and truncates already take, so both sources report drift the
same way, neither of them holds a `Metrics` handle, and nothing PostgreSQL- or
MySQL-specific reaches the engine. Carrying no position is what keeps it inert:
a drift event can never flush a batch, acknowledge a position or move a
checkpoint. On MySQL the comparison is between the catalog's answer before a
DDL and its answer after, since the binlog says a statement ran but not what it
did to a column layout.

**A reload changes settings; everything else is refused with the reason.** On
SIGHUP the file is read again and validated whole, so a file with a mistake
anywhere in it changes nothing at all — `pg2osync validate && kill -HUP $(pidof
pg2osync)` is the workflow, and `validate` checks exactly what the reload
checks. What a reload may change is the handful of `[engine]` settings a batch
consults each time round — `batch_size`, `batch_max_bytes`, `txn_buffer_cap_mb`,
`checkpoint_interval_ms`, `load_max_rows_per_sec` — plus the retry budget
(`retry_max`, `retry_backoff_ms`, `retry_max_elapsed_ms`) and `[log] filter`.
Those are hot because nothing is built from them: the engine copies them at the
top of every turn, the retry budget lives in atomics behind the sink, and the
subscriber's filter is reloadable. Everything else is refused
in place, naming the field, both values and what it would take. `[source]` and
`[target]` name the stream and the checkpoint bound to it; a listener is bound
once and an environment variable is fixed when the process is executed; the sink
task is built with `write_concurrency`, `on_permanent_rejection` and
`max_rejects` when an attempt starts. Inside `[sync.*]` there are two further
classes and the difference is what it costs to put right: changing `table`,
`index`, `id`, `primary_key`, `append_only`, `routing`, `fan_out`, `join` or
`children` changes what a row *is filed as*, so every document already written
is filed the old way and the answer is a re-index behind an alias; changing
`columns`, `transform`, `fields`, `constants`, `where`, `pipeline`,
`soft_delete` or `mapping_file` only reshapes the document, so the index would
hold a mixture with nothing recording which, and the answer is a re-snapshot.
`poll_column` is a third class of one: it decides which rows the poll query
reads rather than what a document is or how it is shaped, so neither answer
applies and the fix is a plain restart.
Adding or removing a section is refused too: a table joins a running pipeline
only once its rows are loaded beside the stream and — on PostgreSQL — its name
is in the publication, and the ordering that makes that safe is the initial
load's argument, not something to approximate. Refusing loudly is the point. A
reload that silently ignored half the file would be worse than no reload,
because the file would stop describing the process. Every outcome is counted as
`pg2osync_config_reloads_total{result}`, for the same reason schema drift is:
a log line nobody can be paged on is barely a report. SIGHUP and nothing else —
a `pg2osync reload` subcommand would need a pidfile to find the running process,
which is state outside the target, and under a container runtime `kill -HUP 1`
is the same step. Nothing a reload does moves the checkpoint, by construction.

**A binlog shape the catalog cannot match is skipped, not fatal.** MySQL's
TABLE_MAP describes the table as it was when the row was written, and
`information_schema` only ever answers for now. A crash-restart resumes from the
last durable checkpoint, so any DDL that committed after that checkpoint is
replayed: the rows before it carry a column count the catalog no longer has, and
re-reading the catalog cannot bring the old layout back. Refusing to continue
there looks safe and is not — the reconnect resumes from the same checkpoint,
reaches the same event and fails again, so the pipeline stops replicating
*everything* rather than the handful of rows it cannot decode. Those rows are
therefore counted as drift, named in the log and left undecoded, which is the
same bargain the rest of this section makes: the index and the table disagree
about a shape that changed, and only a rebuild closes that.

**No event trigger in the user's database.** The attractive version of DDL
detection puts a `CREATE EVENT TRIGGER` in the source, which writes each schema
change into the WAL as a logical message so it arrives inline and correctly
ordered ahead of the data that depends on it. pgstream does this, and the
machinery is already here — `/synced` emits logical messages and the decoder
already advances on them.

It is still refused, for two reasons that were measured rather than assumed.

The first is that pgoutput already does the ordering. PostgreSQL re-sends a
RELATION message whenever a replicated table's shape changes, before the first
row event that depends on it, and `column_drift` reports exactly what changed.
Verified against a live database: an `ALTER TABLE ... ADD COLUMN` between two row
events logs `added later_col` and the next document carries the new column, in
order, with no trigger involved. The problem the trigger exists to solve is not
one we have.

The second is the cost. `CREATE EVENT TRIGGER` needs superuser — PostgreSQL's
own documentation says so plainly — which many managed providers do not grant,
and it would put an object of ours inside the user's database. That is the same
refusal as not running DDL on the source and not writing a signal table into it,
and the refusal is itself something people choose this tool for.

What the trigger would add over what pgoutput gives us is the DDL text, earlier
notice on a table nobody is writing to, and DDL that does not touch a replicated
table's shape at all. None of those change what a document looks like, which is
the only thing the index can disagree with the database about.

Worth revisiting only if pg2osync ever *applies* schema changes to the target —
a different product than this one, and the point at which knowing the statement
rather than the resulting shape starts to matter.

**No relational sources beyond PostgreSQL and MySQL/MariaDB**, and no
non-relational sources. The value is depth on these, not breadth.

**Nested children stay one level deep.** Anything deeper is the application's
to shape before it reaches the database, and there is no view route around
that: only base tables are eligible (`relkind = 'r'` on PostgreSQL,
`table_type = 'BASE TABLE'` on MySQL) because the WAL and the binlog carry
base-table rows, and a view has none to stream.

**Tables are named one by one; there is no pattern.** Debezium takes a regex in
`table.include.list` and Airbyte discovers its streams, so a table created
tomorrow that matches starts syncing on its own. Here a section names an index,
an id rule, a mapping and what to exclude, and a table that arrived by pattern
would take the default for all four — which is precisely the index that quietly
disagrees with the configuration that the rest of these options exist to
prevent. It is also a silent action on someone's data at three in the morning,
by a process whose whole argument is that it does nothing the operator did not
write down.

Adding a table is one `[sync.<key>]` section and a `pg2osync resnapshot --table`
— [configuration.md](configuration.md) has both. The cost people mean when they
ask for a pattern is the restart, and that is #145's to remove; the
explicitness is not what is expensive.

**No data streams.** OpenSearch and Elasticsearch data streams give an
append-only time series automatic rollover and a lifecycle policy attached to
the stream rather than to each index, which is a real convenience. They also
require `op_type: create`: a write that already landed comes back as a conflict
instead of overwriting itself, which is the opposite of the versioned upsert
[every write here relies on](#correctness), and a replay is the ordinary case
rather than the exception.

The one table shape that would fit is `append_only`, and it already has
retention — a time-bucketed index template plus an ILM or ISM policy the
operator PUTs once, as [the Retention section of
configuration.md](configuration.md#retention) describes (#120). The same
outcome, with one write path instead of two. Worth revisiting only if something
a data stream alone can do turns up in practice: a `logs-*` convention that has
to be honoured, or a managed service that refuses a plain index.

**Sinks are search targets, and nothing else.** pgstream and Conduit fan the
same change stream out to webhooks, and the request here was a `webhook` sink
for cache invalidation and integrations. It is the first step toward the
general CDC bus the README declines to be — "no fan-out to multiple consumers,
no message replay … you want Kafka" — and the step is hard to take back once a
second kind of consumer exists.

The correctness argument is the stronger one. Every non-search sink brings its
own delivery semantics, and they are not this one's: a 5xx from an HTTP
endpoint is not a versioned write that a replay can repeat harmlessly, so
at-least-once would have to mean something different per sink and the engine
would be the place that knew the difference. What keeps this tool
explainable is that a target is a document store with versions. Someone who
needs a side effect per change has the database's own tools — LISTEN/NOTIFY, a
trigger — or a CDC platform built to fan out.

## Implementation choices

**Hand-rolled metrics endpoint.** Six counters and one summary do not justify a
Prometheus client plus an HTTP framework in a binary that advertises having no
dependencies.

**Traces are opt-in and live in the binary.** (#152.) Metrics answer "how much
and how far behind"; nothing answered "what happened to *this* change" — read
from the log, transformed, written in which request, visible when — or tied it
to the request an application was already tracing. `PG2OSYNC_OTLP_ENDPOINT`
turns on an OTLP exporter over the spans the pipeline emits, and `/synced`
continues an incoming `traceparent` into the wait, so a caller's trace and the
batch it is waiting for are one timeline.

Behind an environment variable, and unset by default, for the reason the log
format is one: where telemetry goes belongs to whoever runs the deployment, not
to the config file that describes a pipeline. With no endpoint nothing is
constructed — no exporter, no batch thread, no connection — so an operator with
no tracing backend pays for none of it, and one whose collector is down loses
spans and one log line rather than throughput: an export failure is reported
once and never reaches the write path.

This does not change the metrics decision above. The two answer different
questions, and a Prometheus endpoint is still six counters that need no client
library.

**The engine emits `tracing` spans and nothing else.** The whole OpenTelemetry
dependency — exporter, sampler, propagator — is confined to `crates/bin`, which
is also the only place that knows the log format. The engine and the sources
name a span and its fields; what collects them, and whether anything does, is
the binary's business. Anything else would make the source-agnostic engine
depend on a telemetry vendor's types, and put a second consumer of them behind
every `Sink`. Where the engine needs something only the binary can do — linking
a caller's `traceparent` to a span — it takes a closure, the way it already
takes the position parser and the stream nudge.

**Batch reads with `COPY … (FORMAT text)`.** Text parsing measured fast enough
(~21k docs/s end to end) that binary format's complexity is not yet justified.

**Secrets from the environment.** Every secret has an `*_env` form. Inline
values still work but warn, because config files end up in version control.

**Errors: `thiserror` in libraries, `anyhow` only in the binary.** Callers get
matchable variants; the CLI gets readable messages.

**YAGNI on configuration.** An option that does nothing is worse than a missing
one, because it implies a guarantee. Options that had no effect were removed
rather than documented.

**Advisories are reviewed, not muted.** `cargo audit` runs in CI when the
dependencies move, and every entry in [`.cargo/audit.toml`](https://github.com/kennywillbe/pg2osync/blob/main/.cargo/audit.toml)
carries the argument for why the advisory does not reach this binary — for the
`rsa` sidechannel, that the process holds no private key to leak. An advisory
with no such argument is a bug to fix, not a line to add there.
