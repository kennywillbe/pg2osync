# Add a source table without a restart

A table added to a running pipeline is one of the few config changes that is
applied in place. The section joins the stream, the table joins the
publication, and its existing rows are read beside the live changes — no drain,
no reconnect, no replay from the checkpoint for the tables that were already
there.

This page is the one walkthrough for all three ways of delivering that change:
a bare process, systemd, and Kubernetes. What differs is only how the signal
gets sent.

## 1. Add the section

```toml
[sync.orders]
table = "public.orders"
index = "orders"
```

Keep it plain for a reload. A section added with
[`children`](../configuration.md#nested-children),
[`join`](../configuration.md#join-fields), [`fan_out`](../configuration.md#fan-out),
[`aggregates`](../configuration.md#aggregates) or a
[per-row index](../configuration.md#per-row-indices) needs a **restart**: the
source is built watching those extra tables, and the endpoints are told the
index pattern once. So is any section added while `mode = "poll"`, whose query
names its tables when the attempt is built. The
[Reloading table](../configuration.md#reloading) is the full list of which
change lands where.

## 2. Check the file before signalling

```sh
pg2osync validate -c pg2osync.toml
```

`validate` checks exactly what a reload checks, which is what makes this the
step rather than a courtesy. The whole file is validated first, so a mistake
anywhere in it changes nothing at all.

## 3. Signal the process

**A bare process:**

```sh
pg2osync validate -c pg2osync.toml && kill -HUP "$(pidof pg2osync)"
```

There is no `pg2osync reload` subcommand: a second process would have to find
the first one, and the only portable way is a pidfile — state outside the
target, which is the one place pg2osync keeps state.

**systemd:** the unit in [Deployment](../deployment.md#systemd) already has both
`ExecReload` lines, so the edit is followed by

```sh
systemctl reload pg2osync
```

**Kubernetes, by hand,** under either `reloadOnChange` value:

```sh
kubectl -n pg2osync exec deploy/pg2osync -c pg2osync -- kill -HUP 1
```

**Kubernetes, automatically:** set `reloadOnChange=signal`. The pod then
carries no config checksum, so a `helm upgrade` that changes `config` leaves it
running; the kubelet updates the mounted ConfigMap in place and a sidecar sends
SIGHUP when the file changes. Two things this costs, both covered in
[Reloading the configuration](../deployment.md#reloading-the-configuration):
the kubelet refreshes a mounted ConfigMap on its own sync period, so the change
reaches the file up to about a minute later, and the config has to be a
whole-volume mount — a `subPath` mount is never updated in place, and the chart
refuses that combination rather than shipping a reload that does nothing.

The [operator](../operator.md#reloading) renders the same thing from a
`Pg2osync` resource and runs the same sidecar. There it matters more than
anywhere else: on `restart`, a pod coming back finds a publication that no
longer matches the file and **halts the source naming the drift**, because
startup never rewrites a publication it did not create. A resource whose table
set grows wants `signal`.

Note that installing a handler changes what the signal does. SIGHUP's default
disposition is to terminate, so any tooling that sends it as a blunt restart
now gets a reload instead.

## 4. Watch it join

The reload runs in one order and no other. On PostgreSQL the table goes into
the publication first — the same statement `bootstrap` would have run — then
the reload waits until the pipeline has acknowledged a position past that
statement, because a walsender picks up the new membership at a transaction
boundary. The startup checks for that table run next, then its index is
created, then the stream begins admitting it, and only then are its existing
rows read, on the same channel the initial load uses and at the same rate
ceiling. A row written during any of this arrives exactly once, which is what
the ordering buys. The log says when the table is synced.

Two things it deliberately does not relax: the per-index bulk-load settings
stay as they are, because those indices are serving searches; and progress is
recorded, so a crash halfway through finishes the job on the next start rather
than leaving the table half indexed.

`pg2osync_config_reloads_total{result}` counts the outcome as `applied`,
`refused`, `invalid` (the file did not load, so nothing changed) or `failed` —
so a config pushed by a deployment tool is something an alert can watch rather
than something to read logs for. Nothing a reload does moves the checkpoint.

## When the role does not own the publication

Putting a table into a publication requires owning both the publication and the
table, and that is a PostgreSQL restriction a grant cannot work around. Where
the role does not, **the reload is refused and prints the exact
`ALTER PUBLICATION … ADD TABLE` for a DBA to run.** The section keeps running
as it was; nothing is half-applied.

Have the privileged role run the statement, then signal again — the reload
finds the table already published and carries on from there. The same applies
in reverse: removing a section takes the table out of the publication where the
role may, and otherwise logs one `WARN` naming the statement to run before the
next restart, or the publication and the file will disagree.

Removing a section stops its rows being routed. The index is left exactly as it
is and named in the log; nothing here deletes documents.

## What the reload refuses

Everything else is refused **in place**, with one `ERROR` line naming the
field, both values and what it would take. A refusal elsewhere does not hold
back the settings that can change, and nothing is ever half-applied.

Roughly: batch sizes, the checkpoint interval, the load's rate ceiling, the
retry budget and `[log] filter` are applied; `[source]`, `[target]`,
`[metrics]`, `[api]`, any `*_env` and the sink's own settings need a restart;
and an existing `[sync]` section's shape needs either a
[re-snapshot or a re-index](choosing-a-rebuild.md) as well. The
[Reloading table](../configuration.md#reloading) is the authority, option by
option.
