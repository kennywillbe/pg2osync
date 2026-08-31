# Deployment

pg2osync is a single process with no local state (except the Meilisearch
checkpoint file). One process can serve one source database or thirty; what it
must never serve twice is one replication slot.

> **One slot, one stream, one checkpoint.** Two processes streaming the same
> PostgreSQL slot fight over its position and undo each other's progress, and
> two configs naming the same `slot_name` share one checkpoint document
> wherever they run. Everything below is about which process runs what;
> nothing below changes that line.

## Scaling: one process, many sources, or many processes

There are two ways to grow, and they answer different questions.

**More sources, one process.** Put one config file per source database in a
directory and run `pg2osync run --config-dir /etc/pg2osync`. Each file gets a
pipeline of its own — its own slot, sink, engine, checkpoint, retry policy and
initial load — and what they share is the process: one metrics endpoint, one
`/synced`, one log stream, one set of limits, one thing to probe and page on.
Thirty tenant databases that are individually almost idle are the case this
exists for: thirty processes cost thirty scrape targets, thirty probes and
thirty pod-sized floors of memory for work that fits in one.

What they still do not share is anything a stream is identified by. Every file
needs its own `slot_name` (PostgreSQL) or `server_id` (MySQL); files copied
from one template are refused at startup, naming the checkpoint document they
would have collided in. One source failing is a state, not an exit: it is
logged against its name, `/healthz/<name>` turns 503 and
`pg2osync_source_state{state="halted"}` goes to 1 while the others keep
streaming — see [operations](operations.md#health). The process exits non-zero
only when every source has halted.

The cost is what a shared process always costs. The sum of every `[engine]
write_concurrency` and `batch_size` in the directory is what the pod has to be
sized for, and there is deliberately no cap across the files: a shared budget
would make every source wait on the slowest target of any other. One
`terminationGracePeriodSeconds` covers the whole drain, not each source. A
restart — a node drain, a rollout, an OOM — restarts all of them, and each
resumes from its own checkpoint.

**More processes.** Split the files across releases when you want failure or
scheduling to be separate: a source whose target is on a different cluster, a
tenant that must not share a memory limit with the others, or simply more CPU
than one pod is given. Nothing changes in the configs — the same files, in a
different directory — because a file is a source wherever it runs.

**Not: more replicas.** `replicas: 2` is not twice the throughput, it is two
processes on one slot. Where one pipeline is genuinely too slow for one table,
split the tables across configs with a `slot_name` each; the source's write
rate, not pg2osync, is what that trades against.

One source, one file, `run -c pg2osync.toml`, remains exactly what it was: the
directory is an option, not a migration.

## Container image

<!-- x-release-please-start-version -->
```sh
docker run --rm \
  -e PG2OSYNC_SOURCE_URL="postgres://user:pass@db:5432/appdb" \
  -e PG2OSYNC_TARGET_PASSWORD="…" \
  -v "$PWD/pg2osync.toml:/etc/pg2osync/pg2osync.toml:ro" \
  -p 9100:9100 \
  ghcr.io/kennywillbe/pg2osync:1.5.0
```
<!-- x-release-please-end -->

The image runs as UID 10001 with a read-only root filesystem and no
capabilities. The default command is `run -c /etc/pg2osync/pg2osync.toml`;
override it to use another subcommand:

<!-- x-release-please-start-version -->
```sh
docker run --rm … ghcr.io/kennywillbe/pg2osync:1.5.0 \
  validate -c /etc/pg2osync/pg2osync.toml
```
<!-- x-release-please-end -->

Build it yourself with `docker build -t pg2osync:local .`.

A compose example lives in [deploy/docker-compose.yml](https://github.com/kennywillbe/pg2osync/blob/main/deploy/docker-compose.yml),
which brings up the tool alone against databases you already run. For a stack
that needs nothing of yours — PostgreSQL, OpenSearch and pg2osync together,
seeded, with a searchable row at the end of it — start from
[examples/docker-compose.yml](https://github.com/kennywillbe/pg2osync/blob/main/examples/docker-compose.yml).

## Kubernetes with Helm

The chart lives in [deploy/helm/pg2osync](https://github.com/kennywillbe/pg2osync/tree/main/deploy/helm/pg2osync).

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

# one JSON object per log line, for the cluster's log collector
logFormat: json

# Production: create this Secret with External Secrets / Vault / SOPS and
# reference it instead of putting credentials in values.
existingSecret: pg2osync-credentials
```

Key values:

| Value | Default | Notes |
|---|---|---|
| `config` | see `values.yaml` | Rendered into `pg2osync.toml` in a ConfigMap |
| `configs` | `{}` | One entry per source database, each a whole `config` tree; rendered as `<name>.toml` and run with `--config-dir` |
| `extraConfig` | `""` | Raw TOML appended — use it for `[[sync.x.children]]` |
| `secrets` | `{}` | Rendered into a Secret; dev convenience only |
| `existingSecret` | `""` | Name of a Secret you manage; wins over `secrets` |
| `command` / `args` | `[]` | Override the entrypoint, e.g. to source a file a Vault Agent rendered |
| `extraVolumes` / `extraVolumeMounts` | `[]` | Extra mounts, e.g. a Secrets Store CSI volume |
| `logFormat` | `text` | `json` sets `PG2OSYNC_LOG_FORMAT` on the pod, for Loki, Datadog or CloudWatch |
| `persistence.enabled` | `false` | Enable for Meilisearch, whose checkpoint is a file |
| `metrics.serviceMonitor.enabled` | `false` | Needs the Prometheus Operator CRDs |
| `grafanaDashboard.enabled` | `false` | Ships `deploy/grafana/pg2osync.json` as a ConfigMap the Grafana sidecar picks up |
| `probes.startup.failureThreshold` | `60` | 10 minutes of initial load headroom |
| `probes.readiness.enabled` | `true` | Readiness on `/healthz`, same port as liveness |
| `podDisruptionBudget.enabled` | `false` | With `maxUnavailable: 0`, blocks voluntary evictions |

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

### Several sources in one release

`configs` replaces `config` with a map of them. Each entry is rendered into the
same ConfigMap as `<name>.toml`, and the container runs
`run --config-dir /etc/pg2osync`:

```yaml
configs:
  orders:
    source:
      url_env: PG2OSYNC_ORDERS_URL
      slot_name: pg2osync_orders      # one slot per file, always
      publication: pg2osync_pub
    target:
      url: http://opensearch.search.svc:9200
    metrics:
      bind: 0.0.0.0:9100              # the process's, not this source's
    sync:
      orders:
        table: public.orders
        index: orders
  billing:
    source:
      url_env: PG2OSYNC_BILLING_URL
      slot_name: pg2osync_billing
      publication: pg2osync_pub
    target:
      url: http://opensearch.search.svc:9200
    sync:
      invoices:
        table: public.invoices
        index: invoices

existingSecret: pg2osync-credentials  # one Secret, one key per url_env
```

The entry's name is the source's name: it labels every metric, answers
`/healthz/orders`, and is what `--source orders` takes. `[metrics]`, `[api]`
and `[log]` describe the process rather than a source, so put them in one entry
or the same in all of them — a file that leaves them out takes what the others
declare, and two files declaring them differently are refused at startup.
Per-entry raw TOML goes under that entry's own `extraConfig` key.

`config` and `configs` are alternatives: setting tables under both fails the
render rather than quietly ignoring one. The checksum that rolls the pod is
taken over the whole set, so adding a file is a rollout.

Verify before installing:

```sh
helm lint deploy/helm/pg2osync
helm template pg2osync deploy/helm/pg2osync -f my-values.yaml
```

## Kubernetes without Helm

Plain manifests with a Kustomization are in
[deploy/kubernetes](https://github.com/kennywillbe/pg2osync/tree/main/deploy/kubernetes):

```sh
kubectl apply -k deploy/kubernetes
```

What they set up:

| File | Purpose |
|---|---|
| `namespace.yaml` | `pg2osync` namespace |
| `secret.yaml` | connection URL and target password as environment variables |
| `configmap.yaml` | `pg2osync.toml`, mounted read-only — one key per source, with `run --config-dir` for several |
| `deployment.yaml` | one replica, `Recreate` strategy, non-root, read-only rootfs |
| `service.yaml` | headless service exposing the metrics port |
| `servicemonitor.yaml` | Prometheus Operator scrape config (optional) |
| `poddisruptionbudget.yaml` | Blocks voluntary evictions (optional) |

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

### With an operator instead

Both variants above describe one deployment, edited by hand. Where there are
many source databases — a tenant per database, each arriving and leaving on its
own schedule — the [Kubernetes operator](operator.md) makes adding one a
`Pg2osync` object, and reconciles the ConfigMap, Deployment and ServiceMonitor
this page assembles. It renders the same files from the same tree, so a chart
release moves onto it by copying `configs:`.

### Probes

`replicas: 1` plus `strategy: Recreate` prevents two instances from briefly
overlapping during a rollout.

The startup probe allows up to ten minutes because the initial load of a large
table happens before the pipeline reaches its steady state. Raise
`failureThreshold` if your initial load takes longer — a liveness probe alone
would restart the pod mid-load, forever.

The readiness probe hits the same endpoint. Be honest about what it proves:
`/healthz` answers `200` as soon as the metrics listener binds, and that happens
before the initial load runs, so readiness means "the process is up and its port
answers", not "the checkpoint is loaded". It is still worth having. The pod
stays out of the Service endpoints while it is starting, failing or shutting
down, so a Prometheus scrape or a `port-forward` never resolves to a socket
nobody is listening on, and a rollout does not report itself complete until the
new pod answers. Set `probes.readiness.enabled: false` if you would rather the
endpoint exist unconditionally.

### PodDisruptionBudget

`podDisruptionBudget.enabled` (default `false`) renders a `policy/v1`
PodDisruptionBudget selecting the same pod as the Deployment, with
`maxUnavailable: 0` — or `minAvailable`, if you set that instead. With a single
replica, `maxUnavailable: 0` refuses every voluntary eviction: a node drain
stops and waits rather than moving a pipeline mid-transaction. That is the
point, and it is also the cost — the drain hangs until somebody deletes the pod
or disables the budget, so enable it only where an operator is watching. The
plain manifests ship the same object in `poddisruptionbudget.yaml`, left out of
the Kustomization until you uncomment it.

### Reloading the configuration

A change to `config` in values re-renders the ConfigMap. What that does to the
running pod is `reloadOnChange`:

| Value | What happens |
|---|---|
| `restart` (default) | The pod carries a checksum of the rendered config, so `helm upgrade` replaces it. Every option takes effect, at the cost of a drain, a reconnect and a replay from the checkpoint |
| `signal` | No checksum, so the pod stays. The kubelet updates the ConfigMap volume in place and a sidecar sends SIGHUP when the file changes |

```sh
helm upgrade pg2osync deploy/helm/pg2osync -n pg2osync --set reloadOnChange=signal
```

Under `signal` the pipeline applies the settings a batch reads — batch sizes,
the checkpoint interval, the load's rate ceiling, the retry budget, `[log]
filter` — and a plain `[sync.*]` section added to or removed from the file,
which means a `helm upgrade` that adds a table starts syncing it without
restarting the pod. It refuses everything else *in place*, with one `ERROR` line naming
the field and what it would take. [Reloading](configuration.md#reloading) has
the full table. So `signal` does not mean "every change is picked up"; it means
the changes that can be applied without a restart are, and the rest are named
rather than silently dropped. Watch
`pg2osync_config_reloads_total{result="refused"}` for the difference.

Three things it needs, and one it cannot do:

- `shareProcessNamespace: true`, which the chart sets. Without it the sidecar
  cannot see the pipeline process to signal it.
- Time. The kubelet refreshes a mounted ConfigMap on its own sync period, so
  the change reaches the file up to about a minute after `helm upgrade`
  returns, and the sidecar notices it within `reloadSidecar.intervalSeconds`.
- A whole-volume mount. A `subPath` mount of the config is **never** updated in
  place by the kubelet, so the sidecar would watch a file that never changes;
  the chart refuses that combination at template time rather than shipping a
  reload that silently does nothing.
- It cannot be a `postStart` hook or an init container: both run once, and the
  file changes long after.

To reload by hand at any time, under either value:

```sh
kubectl -n pg2osync exec deploy/pg2osync -c pg2osync -- kill -HUP 1
```

### Verifying a rollout

```sh
kubectl -n pg2osync logs deploy/pg2osync -f
kubectl -n pg2osync port-forward svc/pg2osync-metrics 9100:9100
curl -s localhost:9100/metrics | grep pg2osync_position
```

`pg2osync_position_lag` staying near zero means the pipeline is keeping up. A
lag that grows steadily means the sink cannot absorb the write rate, and
PostgreSQL will retain WAL until it catches up.

## Secrets

Every credential is an `*_env` option: the config file names an environment
variable, the process reads it at startup. That leaves one job for the cluster —
put the right value in the right variable — and three common ways to do it.

The Helm chart mounts the Secret with `envFrom`, so **the Secret's keys are the
variable names**, verbatim:

| Secret key | Config option |
|---|---|
| `PG2OSYNC_SOURCE_URL` | `[source] url_env` |
| `PG2OSYNC_ADMIN_URL` | `[source] admin_url_env` |
| `PG2OSYNC_TARGET_PASSWORD` | `[target] password_env` |
| `PG2OSYNC_TARGET_API_KEY` | `[target] api_key_env` |
| `PG2OSYNC_API_TOKEN` | `[api] token_env` |
| `PG2OSYNC_METRICS_TOKEN` | `[metrics] token_env` |

The names are yours — the `*_env` option decides them — but the Secret key and
that option's value have to be the same string.

`[source] sslrootcert`, `sslcert` and `sslkey` are the exception: they are
**file paths**, not variables. A secrets manager that delivers them has to
deliver a *file* the container can read, which is a volume mount, not a Secret
key read through `envFrom`.

### HashiCorp Vault, Agent Injector

The injector renders secrets to files under `/vault/secrets`, and a file is not
an environment variable. So the shape depends on which kind of option you are
filling.

**Certificates — annotations only, no chart change.** `sslrootcert`, `sslcert`
and `sslkey` already want a path, and the injector already writes one:

```yaml
# my-values.yaml
podAnnotations:
  vault.hashicorp.com/agent-inject: "true"
  vault.hashicorp.com/role: "pg2osync"
  vault.hashicorp.com/agent-inject-secret-client.crt: "secret/data/pg2osync"
  vault.hashicorp.com/agent-inject-template-client.crt: |
    {{- with secret "secret/data/pg2osync" -}}
    {{ .Data.data.client_cert }}
    {{- end }}
  vault.hashicorp.com/agent-inject-secret-client.key: "secret/data/pg2osync"
  vault.hashicorp.com/agent-inject-template-client.key: |
    {{- with secret "secret/data/pg2osync" -}}
    {{ .Data.data.client_key }}
    {{- end }}

config:
  source:
    sslmode: verify-full
    sslcert: /vault/secrets/client.crt
    sslkey: /vault/secrets/client.key
```

The suffix after `agent-inject-secret-` is the file name, so `client.crt`
becomes `/vault/secrets/client.crt`.

**Environment values — render an env file and source it.** Nothing turns a
rendered file into an environment variable except a shell, so the container's
entrypoint becomes one:

```yaml
# my-values.yaml
podAnnotations:
  vault.hashicorp.com/agent-inject: "true"
  vault.hashicorp.com/role: "pg2osync"
  vault.hashicorp.com/agent-inject-secret-env: "secret/data/pg2osync"
  vault.hashicorp.com/agent-inject-template-env: |
    {{- with secret "secret/data/pg2osync" -}}
    export PG2OSYNC_SOURCE_URL="{{ .Data.data.source_url }}"
    export PG2OSYNC_TARGET_PASSWORD="{{ .Data.data.target_password }}"
    export PG2OSYNC_METRICS_TOKEN="{{ .Data.data.metrics_token }}"
    {{- end }}

command: ["sh", "-c"]
args: ["source /vault/secrets/env && exec pg2osync run -c /etc/pg2osync/pg2osync.toml"]
```

`exec` matters: without it the shell stays PID 1 and swallows the `SIGTERM`
that makes pg2osync write its final checkpoint. The image is Alpine-based, so
`sh` is there.

Of the two, the certificate shape needs no chart change at all and the env
shape needs the smallest one the chart can offer — `command` and `args`, which
`values.yaml` exposes for exactly this. There is no third option that fills a
variable with fewer moving parts: `extraEnv` can carry a literal or a reference
to a Secret, and the Agent Injector writes files into a pod, never Kubernetes
Secrets. To fill `existingSecret` from Vault instead, use the Vault Secrets
Operator or External Secrets' Vault provider and follow the recipe below.

**Rotation.** Vault Agent re-renders the file when the lease renews, but
pg2osync reads the environment once, at startup, so a rotated secret does not
reach the running process. Restart the pod:
`kubectl -n pg2osync rollout restart deploy/pg2osync`. Reloading credentials in
place is [issue #145](https://github.com/kennywillbe/pg2osync/issues/145).

### AWS Secrets Manager, through the CSI driver

Install the [Secrets Store CSI driver](https://secrets-store-csi-driver.sigs.k8s.io/)
with `syncSecret.enabled=true`, plus the AWS provider (ASCP). The
`SecretProviderClass` pulls one JSON secret, splits it with JMESPath and syncs
the pieces into the Secret that `existingSecret` names:

```yaml
apiVersion: secrets-store.csi.x-k8s.io/v1
kind: SecretProviderClass
metadata:
  name: pg2osync-aws-secrets
  namespace: pg2osync
spec:
  provider: aws
  parameters:
    region: eu-central-1
    objects: |
      - objectName: "prod/pg2osync"
        objectType: "secretsmanager"
        jmesPath:
          - path: source_url
            objectAlias: PG2OSYNC_SOURCE_URL
          - path: target_password
            objectAlias: PG2OSYNC_TARGET_PASSWORD
          - path: api_token
            objectAlias: PG2OSYNC_API_TOKEN
          - path: metrics_token
            objectAlias: PG2OSYNC_METRICS_TOKEN
  # the keys of this Secret are the environment variable names
  secretObjects:
    - secretName: pg2osync-credentials
      type: Opaque
      data:
        - objectName: PG2OSYNC_SOURCE_URL
          key: PG2OSYNC_SOURCE_URL
        - objectName: PG2OSYNC_TARGET_PASSWORD
          key: PG2OSYNC_TARGET_PASSWORD
        - objectName: PG2OSYNC_API_TOKEN
          key: PG2OSYNC_API_TOKEN
        - objectName: PG2OSYNC_METRICS_TOKEN
          key: PG2OSYNC_METRICS_TOKEN
```

The driver syncs only while a pod mounts the volume, so the pod has to mount it
even though it reads the values from the environment:

```yaml
# my-values.yaml
existingSecret: pg2osync-credentials

serviceAccount:
  create: true
  name: pg2osync
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::111122223333:role/pg2osync

extraVolumes:
  - name: secrets-store
    csi:
      driver: secrets-store.csi.k8s.io
      readOnly: true
      volumeAttributes:
        secretProviderClass: pg2osync-aws-secrets

extraVolumeMounts:
  - name: secrets-store
    mountPath: /mnt/secrets-store
    readOnly: true
```

The IAM role needs `secretsmanager:GetSecretValue` on that secret, and its trust
policy has to name this service account. On the first rollout the Secret does
not exist yet: the volume mounts first and creates it, so a brief
`CreateContainerConfigError` before the container starts is expected, not a
misconfiguration. The file options skip the Secret entirely — leave them out of
`secretObjects` and point `sslcert`, `sslkey` or `sslrootcert` at
`/mnt/secrets-store/<objectAlias>`.

**Rotation.** With `enableSecretRotation=true` the driver refreshes the mounted
files and the synced Secret on its poll interval, but an environment variable a
Secret populated is fixed for the life of the container. A rotated secret needs
a restart — `kubectl -n pg2osync rollout restart deploy/pg2osync` — and so does
a rotated certificate, which pg2osync reads once, when it opens the connection.
Tracked as [issue #145](https://github.com/kennywillbe/pg2osync/issues/145).

### External Secrets Operator

The operator writes the Secret itself, so the pod needs nothing but
`existingSecret`. This is the least intrusive of the three, and the same shape
covers Vault, GCP Secret Manager or Azure Key Vault by swapping the provider in
the `SecretStore`:

```yaml
apiVersion: external-secrets.io/v1
kind: SecretStore
metadata:
  name: aws-secrets-manager
  namespace: pg2osync
spec:
  provider:
    aws:
      service: SecretsManager
      region: eu-central-1
      auth:
        jwt:
          serviceAccountRef:
            name: pg2osync
---
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: pg2osync-credentials
  namespace: pg2osync
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: aws-secrets-manager
    kind: SecretStore
  target:
    name: pg2osync-credentials
    creationPolicy: Owner
  data:
    # secretKey is the key of the created Secret, so it is the variable name
    - secretKey: PG2OSYNC_SOURCE_URL
      remoteRef:
        key: prod/pg2osync
        property: source_url
    - secretKey: PG2OSYNC_TARGET_PASSWORD
      remoteRef:
        key: prod/pg2osync
        property: target_password
    - secretKey: PG2OSYNC_API_TOKEN
      remoteRef:
        key: prod/pg2osync
        property: api_token
    - secretKey: PG2OSYNC_METRICS_TOKEN
      remoteRef:
        key: prod/pg2osync
        property: metrics_token
    # PEM material: sslcert and sslkey are paths, so these two keys are mounted
    # as files below instead of being read as variables
    - secretKey: client.crt
      remoteRef:
        key: prod/pg2osync
        property: client_cert
    - secretKey: client.key
      remoteRef:
        key: prod/pg2osync
        property: client_key
```

```yaml
# my-values.yaml
existingSecret: pg2osync-credentials

serviceAccount:
  create: true
  name: pg2osync

# only for the certificate keys above; drop this block and the two below when
# the source needs no client certificate. The path stays outside /etc/pg2osync,
# which the config ConfigMap already owns.
config:
  source:
    sslmode: verify-full
    sslcert: /etc/pg2osync-tls/client.crt
    sslkey: /etc/pg2osync-tls/client.key

extraVolumes:
  - name: source-tls
    secret:
      secretName: pg2osync-credentials
      defaultMode: 0440
      items:
        - key: client.crt
          path: client.crt
        - key: client.key
          path: client.key

extraVolumeMounts:
  - name: source-tls
    mountPath: /etc/pg2osync-tls
    readOnly: true
```

Every key of that Secret still arrives as an environment variable through
`envFrom`, `client.crt` included. That is harmless — a variable no `*_env`
option names is ignored — but put the PEM material in a second `ExternalSecret`
and a second Secret if you would rather it never reached the environment at all.

**Rotation.** The operator rewrites the Secret every `refreshInterval`, and
Kubernetes updates a Secret projected as a *volume* in place, but never one
consumed through `envFrom`. So the mounted certificate changes on disk while
the environment the process started with does not — and pg2osync reads both
only at startup. Restart after a rotation:
`kubectl -n pg2osync rollout restart deploy/pg2osync`. Reloading without a
restart is [issue #145](https://github.com/kennywillbe/pg2osync/issues/145).

### Why the binary has no secrets-manager client

[Design decisions](decisions.md) settle it: secrets come from the environment,
and that is the whole contract. Every recipe above already ends in an
environment variable or a file, because that is what Kubernetes, systemd and
Docker all know how to fill. A built-in client would add an SDK per provider, a
second credential needed to reach the first one, and an authentication path that
only fails in production — to arrive at the value the platform was going to hand
over anyway.

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
# `systemctl reload pg2osync` after editing the file; the process validates it
# whole and refuses in place anything that needs a restart
ExecReload=/usr/local/bin/pg2osync validate -c /etc/pg2osync/pg2osync.toml
ExecReload=/bin/kill -HUP $MAINPID
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

A stop — `SIGTERM` from `docker stop` or Kubernetes, `SIGINT` from a terminal —
finishes the requests already sent to the target and writes a final checkpoint
before exiting, which takes well under a second normally and at most one target
request timeout (30 s) when the target has stopped answering; the Kubernetes
default `terminationGracePeriodSeconds` of 30 s covers that, `docker stop`'s
default of 10 s does not, so pass `-t 30` if the target may be unhealthy.

Stopping for a while is safe as long as the source retains its history:
PostgreSQL keeps WAL for an inactive slot (watch disk usage with
`pg2osync status`), and MySQL keeps binlogs for `binlog_expire_logs_seconds`.
Past that window the position is gone and the next start does a full initial
load.

## Operational checklist

- [ ] One process per slot, and the slot name is unique per environment — a
      directory of configs is fine, two of them naming one slot is not
- [ ] Credentials come from the environment, not from the config file
- [ ] `[metrics] bind` reachable by your scraper, and lag is alerted on
- [ ] Disk alert on the source: an inactive slot retains WAL indefinitely
- [ ] `pg2osync drop-slot` runs when an instance is decommissioned for good
