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

- `_bulk` writes keyed by primary key (at-least-once and idempotent).
- The checkpoint is one document per stream in a hidden `.pg2osync_meta` index,
  in the same format the OpenSearch sink writes.
- TRUNCATE runs as `_delete_by_query?refresh=true&conflicts=proceed`, after an
  explicit refresh so unrefreshed writes cannot outlive it.
- Retries follow `[engine] retry_max` and `retry_backoff_ms`.
- `reconcile` walks an index with `search_after` in primary-key order, keeping
  each hit's `_routing` so a stray join child is deleted from the shard it
  lives on.
- `switch-alias` reads the alias's current holders and swaps them in a single
  `_aliases` request, so the alias never resolves to nothing mid-swap.

Differences from OpenSearch are limited to REST dialect details (error
response shapes, refresh semantics) that the sink abstracts away — config and
operational behavior are the same.

## Retention

Deleting old indices is the cluster's job, not pg2osync's: name an ILM policy
in `mapping_file`'s `settings` block as `index.lifecycle.name` and every index
created from that mapping — including one a `{column}` template renders on
demand — is managed from creation. See
[Retention](../configuration.md#retention).

## Version targeting

The full `dev/e2e-test.sh` suite runs nightly against Elasticsearch 8.19.20
with security disabled (see [compatibility](../compatibility.md)). The sink
speaks raw REST (`_bulk`, `_mget`, `_delete_by_query`, `_refresh`, `_doc`)
rather than using the official client, which keeps a second HTTP stack out of
the binary.

7.x is untested. If you hit a bulk or mapping incompatibility there, open an
issue — the fix is usually small.

## Security notes

- Prefer `api_key_env` over basic auth for long-running deployments.
- `tls_verify = false` is for local development only; never in production.
