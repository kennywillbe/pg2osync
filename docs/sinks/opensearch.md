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

```toml
[target]
url = "https://<collection-id>.<region>.aoss.amazonaws.com"
serverless = true
```

Serverless mode skips operations that AOSS rejects (`_refresh`, settings
changes) and expects requests to be IAM-signed — deploy behind a SigV4
signing proxy/gateway (or an AWS-side auth bridge) and point pg2osync at it.

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
