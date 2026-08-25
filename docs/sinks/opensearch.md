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

  pg2osync refuses to start when `[target] url` is an `*.aoss.amazonaws.com`
  host, because it cannot sign anything and every request would come back 403.
  Point the url at your proxy and keep `serverless = true`; the proxy is
  addressed by its own host, so the supported arrangement is unaffected.
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

### What a verification run would have to cover

Written down so whoever has an AWS account can do it without rediscovering the
list. Each item is a claim above that nobody has tested:

1. A **SEARCH** collection, a data access policy granting the index and document
   permissions, and a SigV4 proxy in front. Anything else fails at step 3.
2. `pg2osync bootstrap` — does creating `.pg2osync_meta` succeed, and does a
   dot-prefixed index name behave like an ordinary one?
3. An initial load — do writes with an explicit `_id` land? This is the one that
   decides whether a non-search collection is usable at all.
4. Streaming an update and a delete, then a restart — does the checkpoint
   round-trip through `.pg2osync_meta`?
5. A `TRUNCATE` on the source — `_delete_by_query` is not listed as supported,
   so the expected outcome is a clear failure rather than a silent no-op.
6. An update to a row whose TOASTed column did not change — the completion path
   reads documents back with `_mget` as a POST, which AOSS lists only as a GET.
7. `GET /synced` — refresh is the service's business, so the honest expectation
   is that read-your-writes cannot be offered here.

Until that has been run, the matrix says unverified. Guessing at it from the
documentation is how a support claim becomes a lie.

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
