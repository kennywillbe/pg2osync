# Configuration reference

Full example: [examples/pg2osync.example.toml](../examples/pg2osync.example.toml).
Unknown keys are rejected at load time (`deny_unknown_fields`), so typos fail
fast instead of silently misconfiguring.

## `[source]`

| Option | Default | Description |
|---|---|---|
| `flavor` | `"postgres"` | `"postgres"` or `"mysql"` |
| `mode` | `"wal"` | `"wal"` (logical replication) or `"poll"`. Ignored for MySQL. |
| `url_env` | — | Environment variable holding the connection URL (**recommended**) |
| `url` | — | Plain-text URL; works but warns as deprecated for secrets hygiene |
| `admin_url_env` | falls back to `url` | Dedicated connection for child-collection queries |
| `slot_name` | `"pg2osync"` | PostgreSQL replication slot name |
| `publication` | `"pg2osync_pub"` | PostgreSQL publication name |
| `server_id` | `424242` | MySQL only: unique replica server id |
| `poll_column` | `"updated_at"` | Poll mode: timestamp column |
| `poll_interval_secs` | `30` | Poll mode: interval |

URL formats:

```
postgres://user:pass@host:5432/dbname
mysql://user:pass@host:3306/dbname
```

## `[target]`

| Option | Default | Description |
|---|---|---|
| `flavor` | `"opensearch"` | `"opensearch"`, `"elasticsearch"`, `"meilisearch"` |
| `url` | *(required)* | Base URL, e.g. `http://localhost:9200` |
| `username` | — | Basic auth username |
| `password_env` | — | Env var with the password (**recommended** over `password`) |
| `api_key_env` | — | Elasticsearch API key |
| `tls_verify` | `true` | Set `false` only for self-signed dev certs |
| `serverless` | `false` | Amazon OpenSearch Serverless profile |
| `state_dir` | `./.pg2osync-state` | Meilisearch checkpoint directory |

## `[sync.<key>]`

One section per table. `<key>` is the default index name if `index` is omitted.

| Option | Description |
|---|---|
| `table` | Schema-qualified name, e.g. `"public.users"` (**required**) |
| `index` | Target index/collection name; must be lowercase `[a-z0-9_-]` |
| `primary_key` | Override PK detection (rarely needed) |
| `columns` | Whitelist of columns to index |
| `exclude_columns` | Blacklist; mutually exclusive with `columns` |
| `replica_identity_full` | Documents intent that the table uses `REPLICA IDENTITY FULL` |
| `transform` | Map of column → `"hash"` \| `"redact"` |

### Nested children

Embed one-to-many children as JSON arrays on the parent document (one level):

```toml
[[sync.customers.children]]
table = "public.orders"      # child table
field = "orders"             # array field on the parent doc
foreign_key = "customer_id"  # FK column on the CHILD table
```

Children are re-fetched and re-embedded whenever the parent row changes.
Child changes alone do not currently trigger parent re-indexing.

## `[engine]`

Sane defaults; tune only under measurable load.

| Option | Default | Description |
|---|---|---|
| `batch_size` | `500` | Max rows per `_bulk` request |
| `batch_max_bytes` | 10 MB | Max bytes per batch |
| `flush_interval_ms` | `1000` | Flush even when batch isn't full |
| `txn_buffer_cap_mb` | `256` | Cap for buffering an open transaction |
| `retry_max` | `10` | Retries per failed batch (exponential backoff) |
| `retry_backoff_ms` | `500` | Initial backoff |
| `checkpoint_interval_ms` | `500` | Checkpoint write frequency |

## `[metrics]`

| Option | Default | Description |
|---|---|---|
| `enabled` | `true` | Serve Prometheus text exposition |
| `bind` | `127.0.0.1:9100` | Listen address |

Exposed metrics:

```
pg2osync_events_total{type="insert|update|delete|truncate"}
pg2osync_batches_flushed
pg2osync_sink_errors_total
pg2osync_reconnects_total
pg2osync_latency_ms{quantile="0.5|0.9|0.99"}   # commit → indexed
pg2osync_lsn_confirmed                          # highest durably indexed LSN
```
