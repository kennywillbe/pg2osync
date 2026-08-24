# Elasticsearch sink

Same pipeline, Elasticsearch REST dialect. Select with:

```toml
[target]
flavor = "elasticsearch"
url = "http://localhost:9200"
username = "elastic"
password_env = "ES_PASSWORD"
# api_key_env = "ES_API_KEY"       # alternative to user/password
```

## Behavior

Identical contract to the [OpenSearch sink](opensearch.md):

- `_bulk` writes keyed by primary key (at-least-once + idempotent).
- Checkpoints in a hidden `.pg2osync_meta` index.
- TRUNCATE mapped to index operations.

Differences from OpenSearch are limited to REST dialect details (error
response shapes, refresh semantics) that the sink abstracts away — config and
operational behavior are the same.

## Version targeting

Developed against Elasticsearch 8.x. If you run 7.x and hit mapping or bulk
incompatibilities, open an issue — the fix is usually small.

## Security notes

- Prefer `api_key_env` over basic auth for long-running deployments.
- `tls_verify = false` is for local development only; never in production.
