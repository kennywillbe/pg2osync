# Deployment

pg2osync is a single process with no local state (except the Meilisearch
checkpoint file). Run one instance per replication slot.

> **One instance per slot.** Two processes streaming the same PostgreSQL slot
> fight over its position and undo each other's progress. To scale, split
> tables across instances, each with its own `slot_name` and `publication`.

## Container image

```sh
docker run --rm \
  -e PG2OSYNC_SOURCE_URL="postgres://user:pass@db:5432/appdb" \
  -e PG2OSYNC_TARGET_PASSWORD="…" \
  -v "$PWD/pg2osync.toml:/etc/pg2osync/pg2osync.toml:ro" \
  -p 9100:9100 \
  ghcr.io/kennywillbe/pg2osync:0.6.0
```

The image runs as UID 10001 with a read-only root filesystem and no
capabilities. The default command is `run -c /etc/pg2osync/pg2osync.toml`;
override it to use another subcommand:

```sh
docker run --rm … ghcr.io/kennywillbe/pg2osync:0.6.0 \
  validate -c /etc/pg2osync/pg2osync.toml
```

Build it yourself with `docker build -t pg2osync:local .`.

A compose example lives in [deploy/docker-compose.yml](../deploy/docker-compose.yml).

## Kubernetes with Helm

The chart lives in [deploy/helm/pg2osync](../deploy/helm/pg2osync).

```sh
helm install pg2osync deploy/helm/pg2osync \
  --namespace pg2osync --create-namespace \
  -f my-values.yaml
```

A minimal `my-values.yaml`:

```yaml
config:
  source:
    url_env: PG2OSYNC_SOURCE_URL
    slot_name: pg2osync
    publication: pg2osync_pub
  target:
    url: http://opensearch.search.svc:9200
    username: pg2osync
    password_env: PG2OSYNC_TARGET_PASSWORD
  metrics:
    bind: 0.0.0.0:9100          # 127.0.0.1 is unreachable for probes
  sync:
    users:
      table: public.users
      index: users
      exclude_columns: ["password_hash"]

# Production: create this Secret with External Secrets / Vault / SOPS and
# reference it instead of putting credentials in values.
existingSecret: pg2osync-credentials
```

Key values:

| Value | Default | Notes |
|---|---|---|
| `config` | see `values.yaml` | Rendered into `pg2osync.toml` in a ConfigMap |
| `extraConfig` | `""` | Raw TOML appended — use it for `[[sync.x.children]]` |
| `secrets` | `{}` | Rendered into a Secret; dev convenience only |
| `existingSecret` | `""` | Name of a Secret you manage; wins over `secrets` |
| `persistence.enabled` | `false` | Enable for Meilisearch, whose checkpoint is a file |
| `metrics.serviceMonitor.enabled` | `false` | Needs the Prometheus Operator CRDs |
| `probes.startup.failureThreshold` | `60` | 10 minutes of initial load headroom |

`config.sync` is intentionally empty in the chart defaults: Helm merges maps, so
a default table would survive your override and sync a table you never asked
for.

The chart hashes the rendered config into a pod annotation, so
`helm upgrade` restarts the pod when the configuration changes.

Repeated TOML tables (nested children) cannot be expressed in the values tree;
put them in `extraConfig`:

```yaml
extraConfig: |
  [[sync.customers.children]]
  table = "public.orders"
  field = "orders"
  foreign_key = "customer_id"
```

Verify before installing:

```sh
helm lint deploy/helm/pg2osync
helm template pg2osync deploy/helm/pg2osync -f my-values.yaml
```

## Kubernetes without Helm

Plain manifests with a Kustomization are in
[deploy/kubernetes](../deploy/kubernetes):

```sh
kubectl apply -k deploy/kubernetes
```

What they set up:

| File | Purpose |
|---|---|
| `namespace.yaml` | `pg2osync` namespace |
| `secret.yaml` | connection URL and target password as environment variables |
| `configmap.yaml` | `pg2osync.toml`, mounted read-only |
| `deployment.yaml` | one replica, `Recreate` strategy, non-root, read-only rootfs |
| `service.yaml` | headless service exposing the metrics port |
| `servicemonitor.yaml` | Prometheus Operator scrape config (optional) |

Before applying either variant:

1. Replace the credentials in `secret.yaml`, or delete the file and have
   External Secrets, Vault Agent or SOPS create a Secret named
   `pg2osync-credentials` with the same keys.
2. Point `[target] url` in the ConfigMap at your search cluster and set the
   `[sync.*]` sections for your tables.
3. Set `[metrics] bind = "0.0.0.0:9100"` — the default `127.0.0.1` is not
   reachable by kubelet probes or Prometheus.
4. Pin the image tag to a release, not `latest`, so a restart cannot silently
   change versions.

### Probes

`replicas: 1` plus `strategy: Recreate` prevents two instances from briefly
overlapping during a rollout.

The startup probe allows up to ten minutes because the initial load of a large
table happens before the pipeline reaches its steady state. Raise
`failureThreshold` if your initial load takes longer — a liveness probe alone
would restart the pod mid-load, forever.

### Verifying a rollout

```sh
kubectl -n pg2osync logs deploy/pg2osync -f
kubectl -n pg2osync port-forward svc/pg2osync-metrics 9100:9100
curl -s localhost:9100/metrics | grep pg2osync_position
```

`pg2osync_position_lag` staying near zero means the pipeline is keeping up. A
lag that grows steadily means the sink cannot absorb the write rate, and
PostgreSQL will retain WAL until it catches up.

## systemd

```ini
[Unit]
Description=pg2osync
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=pg2osync
# credentials live in a root-owned 0600 file, not in the unit
EnvironmentFile=/etc/pg2osync/env
ExecStart=/usr/local/bin/pg2osync run -c /etc/pg2osync/pg2osync.toml
Restart=always
RestartSec=5
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
# only needed for the Meilisearch state directory
ReadWritePaths=/var/lib/pg2osync

[Install]
WantedBy=multi-user.target
```

## Upgrades and rollbacks

The checkpoint format is forward-compatible: a newer version reads checkpoints
written by an older one. Rolling *back* across a format change is not
supported — an older binary that cannot parse the checkpoint ignores it and
starts a full initial load, which is safe but expensive.

Stopping for a while is safe as long as the source retains its history:
PostgreSQL keeps WAL for an inactive slot (watch disk usage with
`pg2osync status`), and MySQL keeps binlogs for `binlog_expire_logs_seconds`.
Past that window the position is gone and the next start does a full initial
load.

## Operational checklist

- [ ] One instance per slot, and the slot name is unique per environment
- [ ] Credentials come from the environment, not from the config file
- [ ] `[metrics] bind` reachable by your scraper, and lag is alerted on
- [ ] Disk alert on the source: an inactive slot retains WAL indefinitely
- [ ] `pg2osync drop-slot` runs when an instance is decommissioned for good
