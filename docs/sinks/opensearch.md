# OpenSearch sink (default)

The primary, most battle-tested target. The full `dev/e2e-test.sh` suite runs
against OpenSearch 2.19.6 on every pull request (see
[compatibility](../compatibility.md)).

```toml
[target]
url = "http://localhost:9200"
# username = "admin"                # basic auth
# password_env = "OS_PASSWORD"
# tls_verify = true                 # set false only for self-signed dev certs
```

## Behavior

- Writes batches via `_bulk` with `index`/`delete` operations; document `_id`
  is the row's primary key → idempotent replay-safe writes.
- The checkpoint is one document per stream in a hidden **`.pg2osync_meta`**
  index, named `<source>-<slot_name>` or `<source>-<server_id>`. Deleting it
  forces a full initial load on the next start, which is safe but expensive.
- TRUNCATE runs as `_delete_by_query` with a refresh first, so a write that has
  not been refreshed yet cannot survive the truncate.
- Unmodified TOASTed columns are completed before the write — from the old
  tuple under `REPLICA IDENTITY FULL`, otherwise by reading the current
  document back — so a document is never written with a hole in it.
- Transient failures (429, 5xx, connection resets) are retried with exponential
  backoff per `[engine] retry_max` and `retry_backoff_ms`. A permanent
  rejection stops the pipeline instead of skipping the document.

## Amazon OpenSearch Serverless

**Not supported, deliberately.** A provisioned OpenSearch domain works —
including the AWS-managed kind — but a Serverless *collection* does not, and a
url ending in `.aoss.amazonaws.com` is refused at startup rather than left to
answer 403 to everything.

Three things would have to change, and each is a real piece of work:

- **SigV4 is the only authentication a collection accepts.** There is no
  basic-auth path, so pg2osync would have to sign every request with service
  name `aoss` — an AWS credential chain and a signing implementation, for one
  target.
- **A custom document id works only on a *search* collection.** Every document
  here carries its row's primary key as `_id`, because that is what makes a
  replay overwrite instead of duplicate. Time-series and vector collections
  reject it outright, so they could never work at all.
- **The service owns refresh and index settings.** Suspending refresh for an
  initial load, refreshing before a `TRUNCATE`, and `/synced` all depend on
  calls the service rejects. Each would need a documented degradation rather
  than a silent skip.

None of that is impossible; it simply has never been asked for, and carrying a
flag that had never been run against the service was a support claim nobody
could stand behind. If you need it, open an issue — with SigV4 done properly
rather than a proxy the operator has to run, and verified against a real
collection before the matrix says anything.

## Index naming rules

Enforced at config load (fails fast):

- lowercase letters, digits, `_`, `-` only
- must start with a lowercase letter
- must not start with `_` or `.` (reserved)

Two tables may map to the same index as a join pair, where every document
carries its own routing (see [Join fields](../configuration.md#join-fields)),
or once every section feeding it declares an `id` (see
[Sharing an index](../configuration.md#sharing-an-index)).

`index` may also carry `{column}` placeholders — `events-{tenant}`, see
[Per-row indices](../configuration.md#per-row-indices) — and the rules above
then apply to the rendered name: a row that renders an uppercase letter, an
empty value or a NULL halts the pipeline. Two things are different on this
side of a templated table:

- An index a row chooses is created on demand, when the first document for
  it is written, with the section's `mapping_file` if one is set — the same
  mapping a fixed index gets at startup, applied later because the name is
  not known until the row is.
- A `TRUNCATE` of a templated table clears the glob the template claims
  (`events-*` for `events-{tenant}`): one search over the pattern, then one
  versioned bulk delete per hit under the hit's own `_index`, so a row
  committed after the truncate survives it. That is why a template must have
  a literal part: `{tenant}` alone would claim `*`.

## Health & monitoring

- `pg2osync validate` checks reachability and version before you commit to a
  run.
- Watch `pg2osync_sink_errors_total` and `pg2osync_position_confirmed` on
  `/metrics`. Errors with a stalled confirmed position mean the target is
  unhappy: disk full, a mapping conflict, or expired credentials.

## Mappings

Indices are created with dynamic mapping if they do not exist. For anything
beyond the defaults — analyzers, `keyword` subfields, explicit date formats —
create the index (or an index template) yourself before running; pg2osync only
creates what is missing and never modifies an existing mapping.

A document that conflicts with an existing mapping is a permanent rejection and
stops the pipeline, by design.

A join field is compared like any other field at startup, `relations`
included: an index whose relation names disagree with the `mapping_file` is
reported as a conflict, because a wrong name produces documents no
`has_child` query can find rather than documents the target refuses. Dynamic
mapping cannot invent a join field, so an index a join pair writes to has to
be created by pg2osync from the parent's `mapping_file`, or by an index
template that declares the join, before the first document lands.

## Retention

Deleting old indices is the cluster's job, not pg2osync's: an ISM policy whose
`ism_template` matches the index names attaches itself to every index created
after it, a `{column}` template's indices included, with nothing to configure
here. See [Retention](../configuration.md#retention).

## Ingest pipelines

Semantic and hybrid search on OpenSearch go through the neural-search plugin:
a `text_embedding` processor in an ingest pipeline reads a text field and
writes the vector into a `knn_vector` field the mapping declares. pg2osync
computes no embedding of its own; a section names the pipeline
(`pipeline = "embed-products"`) and every document it writes carries that
name on its bulk action, so the target runs the pipeline on the way in.
`validate` refuses a pipeline the target does not have. See
[Ingest pipelines](../configuration.md#ingest-pipelines) for the mapping the
pipeline fills and what follows from the pipeline being per section.
