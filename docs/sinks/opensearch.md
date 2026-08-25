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

**This has never been run against a real collection.** The profile exists and
skips the calls AOSS is documented to reject, but nobody has pointed it at
`*.aoss.amazonaws.com`. Read the rest of this section before planning around
it.

```toml
[target]
url = "https://<collection-id>.<region>.aoss.amazonaws.com"
serverless = true
```

What the AWS documentation says, and what each point means here:

- **SigV4 is the only authentication.** Every request to a collection endpoint
  must be signed with service name `aoss`; there is no basic-auth path, and no
  AWS-supported proxy that provides one. `aws-sigv4-proxy` is an AWS Labs
  sample rather than a documented AOSS component. Whatever you put in front,
  it is yours to run.
- **Your collection must be a SEARCH collection.** Time-series and vector
  collections reject a custom document id
  (`illegal_argument_exception: Document ID is not supported in create/index
  operation request`), and this tool writes the primary key as `_id` — that is
  what makes replay idempotent. There is no version of this that works without
  it.
- **`TRUNCATE` cannot work.** It runs as `_delete_by_query`, which is not in
  the supported-operations list. A truncate on the source would fail against a
  collection.
- **`/synced` cannot work.** The refresh interval is roughly ten seconds,
  `_refresh` is unsupported, and `refresh=true`/`wait_for` are rejected. There
  is no way to make a write visible on demand, so read-your-writes is not
  available on Serverless — only on a provisioned domain.
- **Unchanged-TOAST completion may not work.** It reads documents back with
  `_mget`, which AOSS lists only as a GET; the client here sends it as a POST.
- **The checkpoint index is `.pg2osync_meta`.** A leading dot is not documented
  as forbidden, but it is the convention for system indices, which AOSS manages
  itself. This is the least tested corner of an untested path.

None of that is fatal to the idea — a search collection, a signing proxy, no
truncates and no `/synced` is a real configuration. It is simply not one that
has been demonstrated, and the matrix in the README now says so.

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
