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
- Checkpoints live in a hidden **`.pg2osync_meta`** index (one doc per
  slot/publication pair). Don't delete it while syncing; deleting it triggers
  a safe full backfill on next start.
- TRUNCATE is applied as an index operation on the target.
- Partial updates: unmodified TOASTed columns are omitted from update docs;
  combined with partial `_doc` merges this keeps large fields intact.

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
- Watch `pg2osync_sink_errors_total` and `pg2osync_batches_flushed` on
  `/metrics`; sustained errors with stalled `lsn_confirmed` mean the sink is
  unhappy (disk full, mapping conflicts, auth expiry).
