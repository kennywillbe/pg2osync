# OpenSearch sink (default)

The primary, most battle-tested target. Live-verified against OpenSearch 2.19.

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

Two tables may not map to the same index — document identity would be
ambiguous.

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
