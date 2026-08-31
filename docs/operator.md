# Kubernetes operator

The [Helm chart](deployment.md#kubernetes-with-helm) is one release per
database, upgraded by hand. With many source databases that becomes a values
file per tenant and a `helm upgrade` per change. The operator turns the same
deployment into one object:

```yaml
apiVersion: pg2osync.io/v1alpha1
kind: Pg2osync
metadata:
  name: tenant-a
  namespace: pg2osync
spec:
  secretRefs: [pg2osync-credentials]
  configs:
    orders:
      source:
        url_env: PG2OSYNC_ORDERS_URL
        slot_name: pg2osync_orders
        publication: pg2osync_pub
      target:
        url: http://opensearch:9200
      metrics:
        bind: 0.0.0.0:9100
      sync:
        orders:
          table: public.orders
          index: orders
```

`spec.configs` is the chart's `configs:` map, and the entries under it are the
config tree [Configuration](configuration.md) documents — so moving a release
onto the operator is a copy, not a rewrite. The operator renders each entry to
`<name>.toml` in one ConfigMap and runs `pg2osync run --config-dir
/etc/pg2osync` over it, exactly as the chart does.

`v1alpha1` means the API may change before `v1`. What it will not change is the
config tree inside it, which is the file format.

## Installing it

```sh
kubectl apply -k deploy/operator
```

| File | Purpose |
|---|---|
| `namespace.yaml` | the `pg2osync` namespace the operator and its resources live in |
| `crd.yaml` | the `Pg2osync` definition, cluster-scoped |
| `rbac.yaml` | ServiceAccount, Role and RoleBinding |
| `deployment.yaml` | the operator, one replica, with the pipeline image it deploys pinned in its args |

Two images: `pg2osync-operator` is the controller,
`pg2osync` is the pipeline it deploys. The operator has no default for the
second — `--pipeline-image` is pinned in `deployment.yaml`, so upgrading every
pipeline in the namespace is one reviewed edit. A single resource can pin
another version with `spec.image`.

## What the resource says

| Field | Meaning |
|---|---|
| `config` | one source database, rendered as `pg2osync.toml` |
| `configs` | several, rendered as `<name>.toml` each — the alternative to `config`, never both |
| `secretRefs` | Secrets in the same namespace whose keys become environment variables on the pod |
| `env` | plain environment variables, such as `RUST_LOG` |
| `image` | overrides the operator's pipeline image for this resource |
| `serviceMonitor` | ask Prometheus to scrape it |
| `reloadOnChange` | `restart` (default) or `signal` |

**Credentials are never in the spec.** A `[source] url`, a `[target] password`,
an API key or a metrics token written into a `Pg2osync` is refused — the
reason lands in `status.message` and nothing is deployed. Everyone with `get`
on the resource can read its spec, and specs end up in git; put it in a Secret,
list the Secret under `secretRefs`, and name the variable with the matching
`*_env` option. The operator itself has no access to secrets at all: the
kubelet resolves `envFrom`, so a bug in the controller cannot leak one.

A spec is also refused when two entries end up as the same source name — the
name labels every metric and answers `/healthz/<name>` — or when an entry's
key cannot be a file name.

### Reloading

`reloadOnChange` is the chart's story, unchanged. `restart` puts a checksum of
the rendered files on the pod, so an edit replaces it: every option takes
effect, at the cost of a drain and a replay from the checkpoint. `signal`
leaves the pod alone and runs the same small sidecar the chart runs, which
sends `SIGHUP` when the mounted directory changes; pg2osync then applies what a
running process can apply — including a table added to or removed from a
`[sync]` section — and refuses the rest in place, naming the field. See
[Reloading the configuration](deployment.md#reloading-the-configuration) for
which is which.

The checksum is taken over the rendered files, not over the spec, so an edit
that renders the same configuration does not drain a pipeline.

The two modes are not equivalent for a table added to an existing `[sync]` set.
A reload puts it into the publication and reads its rows while the pipeline
runs; a restart finds a publication that no longer matches the file and halts
the source naming the drift, because startup never rewrites a publication it
did not create. So a resource whose table set grows wants `signal` — or an
`ALTER PUBLICATION … ADD TABLE` before the pod comes back. Adding a whole
*source* — a second entry under `configs`, with a slot and publication of its
own — is unaffected: it is a new file, and it bootstraps itself.

### Metrics

`serviceMonitor: true` creates a ServiceMonitor next to the headless Service
the operator always creates. In a cluster without prometheus-operator there is
no such kind: the operator logs that it skipped it and carries on, because a
missing monitoring stack should leave a pipeline unscraped, not undeployed. It
resolves the kind once at startup, so a cluster that installs
prometheus-operator later needs the operator restarted.

### Status

```console
$ kubectl -n pg2osync get pg2osync
NAME       READY   SOURCES   AGE
tenant-a   true    2         6m
```

`status.ready` is the Deployment's readiness and nothing more, and
`observedGeneration` says which spec that describes. It deliberately does not
summarise each source: `/healthz/<name>` and
`pg2osync_source_state{source="<name>"}` already answer that, from the process
that knows, and an operator polling every pod to copy those answers into an
object would be a second, slower, staler source of truth for them. Ask the
pipeline — see [Operations](operations.md#health).

## What the operator does not do

Every object it creates carries an owner reference, so deleting a `Pg2osync`
deletes its ConfigMap, Service, Deployment and ServiceMonitor. That is the
whole cleanup path, and it is deliberate.

**It does not drop the replication slot.** A slot lives in the source database,
which the operator has no credentials for and no reason to be given any. It
also outlives the pipeline on purpose: a resource deleted by accident, or
deleted to be recreated, resumes from where it stopped instead of replaying the
whole table. So there is no finalizer, and deleting a resource for good is two
steps:

```sh
kubectl -n pg2osync delete pg2osync tenant-a
# or, from psql: SELECT pg_drop_replication_slot('pg2osync_orders');
pg2osync drop-slot -c orders.toml
```

A slot nobody reads holds WAL on the source until its disk fills. `pg2osync
status --max-retained-mb` is the check that catches it; see
[Operations](operations.md).

Also not in `v1alpha1`, each for the same reason — it would be a guarantee
nobody has asked for yet:

- **No leader election.** One operator replica. While it is down the pipelines
  it created keep streaming; only reconciliation pauses, and a restart re-lists
  everything.
- **No admission webhook.** The CRD schema and the refusals above are the whole
  validation. A webhook would be a certificate, a Service and a failure mode
  that can block writes to the API server.
- **No cross-namespace secrets.** `secretRefs` names Secrets in the resource's
  own namespace.
- **No operator-managed upgrades.** Changing `--pipeline-image` rolls the pods;
  when to do that is a decision, not a reconcile.

## Namespaces and RBAC

The operator reconciles the namespace it runs in, through a `Role` rather than
a `ClusterRole`: a compromised controller reaches one namespace's objects
instead of every namespace's, and it can be handed to a team that owns one
namespace without cluster-admin agreeing to it.

The cost is that a second namespace needs a second operator — the manifests
with `namespace:` changed. A cluster-wide install is a `ClusterRole` and a
`ClusterRoleBinding` with the same rules, and a controller that watches all
namespaces; that is a supported shape of Kubernetes, not of these manifests,
and running one controller for every tenant's pipelines makes it a
cluster-wide blast radius. Prefer one operator per namespace until that stops
being practical.

## Changing the definition

`deploy/operator/crd.yaml` is generated from the Rust types, and a unit test
fails when the two drift:

```sh
cargo run -p pg2osync-operator -- crd > deploy/operator/crd.yaml
```
