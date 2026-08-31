# Rotating a pseudonym key

[`pseudonym`](../configuration.md#pseudonym) is deterministic on purpose: equal
values give equal tokens, across tables and across runs, so a join on a
pseudonymised column still joins. The key is what makes that true.

**So changing the key re-tokenises everything.** Every value encrypts to a
different token under the new key, and a document written before the rotation
is unjoinable to one written after it — the same person's `users.id` and
`orders.user_id` no longer match, and any saved query or downstream system
holding a token stops finding anything. The index is only consistent again once
**every** section that pseudonymises anything has been read from the source
again.

There is no dual-key overlap. A transform names one `key_env`, the process
reads that variable once at exec, and there is no second key tried on a miss —
so there is no window in which both the old and the new token resolve. Plan the
rotation as a rebuild of the pseudonymised data, not as a credential swap.

## Before you start

List the sections in scope. It is every `[sync.<key>].transform` entry with
`op = "pseudonym"`, *including* the ones on other tables that carry an explicit
`scope` to join with one of them — for example an `orders.user_id` scoped to
`public.users.id`. Rotating one half of such a pair and not the other leaves
the join broken permanently rather than temporarily.

Note also what the tokens are *not* protecting, because a rotation is a good
moment to check it: the token appears in the document, but `_id`, `where` and
`fan_out` all read the raw row before transforms, so a pseudonymised column
named by an `id` template is still in the document id in plaintext.

## The sequence

1. **Generate the new key.** 128 hex characters, which is a 64-byte
   AES-256-SIV key:

   ```sh
   openssl rand -hex 64
   ```

2. **Put it in a new variable rather than overwriting the old one.**

   ```toml
   [sync.users.transform]
   email = { op = "pseudonym", key_env = "PG2OSYNC_PSEUDONYM_KEY_2" }
   ```

   Keeping the old variable in place is what makes step 6 a decision rather
   than a hope: until every section is re-snapshotted, the old key is the only
   thing that can read the tokens still in the index.

3. **Stop the pipeline, set the variable, start it again.** A key lives only in
   the environment, and the environment is fixed at exec — no reload picks up a
   new value, and `*_env` is a restart in the
   [Reloading table](../configuration.md#reloading) for exactly that reason.
   `pg2osync validate` checks the key before the pipeline does:

   ```sh
   pg2osync validate -c pg2osync.toml
   # ✓ pseudonym key present (64 bytes) from PG2OSYNC_PSEUDONYM_KEY_2
   ```

   A `key_env` naming a variable that is unset or not a well-formed key is
   refused at startup, so a mistake here stops the process rather than writing
   `***` into every document.

4. **Re-snapshot every section in the list, one at a time.**

   ```sh
   pg2osync resnapshot -c pg2osync.toml --table public.users
   pg2osync resnapshot -c pg2osync.toml --table public.orders
   ```

   This is safe beside the stream and never moves the checkpoint. Because the
   document id renders from the row's raw values — before transforms — each
   document is **overwritten in place** under the id it already had; a
   rotation leaves no orphans to reconcile. What it does leave, until the last
   table finishes, is an index holding both old and new tokens, so expect
   cross-table token joins to be wrong for the length of the run.

5. **Verify before retiring anything.** Two checks worth doing:

   - a token join across a scoped pair returns matches again — the query that
     was broken between step 3 and the end of step 4;
   - a known value re-encrypts to the token in the document. The construction
     is four lines in [Pseudonym](../configuration.md#pseudonym) and any RFC
     5297 library implements it; there is deliberately no `decrypt`
     subcommand, because it would want the key on a command line.

   Also check `pg2osync_transform_unconverted_total` has not moved. A value
   `pseudonym` cannot render is replaced with `***` rather than indexed, and
   counted there — a spike means the rotation wrote redactions, not tokens.

6. **Retire the old key.** Remove the old variable from wherever it is stored —
   the systemd `EnvironmentFile`, the Secret, the secrets manager entry — and
   only then, if you want the config tidy, rename the new variable back. That
   rename is another restart, and no re-snapshot: the key's value is what the
   tokens depend on, not the variable's name.

## Limitations, stated plainly

- **No dual-key overlap, and no re-encryption in the target.** The only way to
  move a document from the old key to the new one is to read its row again.
- **A re-snapshot does not resume.** An interruption means running it again,
  which costs the read and nothing else.
- **A big pseudonymised table is a long window.** Where that window is not
  acceptable, the rotation is a
  [zero-downtime re-index](../operations.md#zero-downtime-re-index) instead: a
  second instance with the new key builds a second index, and the alias moves
  when it is complete and consistent. That trades the window for a second slot
  and two lots of retained WAL.
- **Rotation does not un-publish the old tokens.** Anything that copied them
  out of the index still holds them, and they still resolve under the old key.
  Retiring the key is what ends that, which is why step 6 is a step.
