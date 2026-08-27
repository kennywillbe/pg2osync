# Repository rules, as files

The branch protection on `main` and the tag rule for releases are configured on
GitHub, which means they are a set of checkboxes nobody can review. These are
the same rules exported, so a change to them shows up in a diff like anything
else.

They are **documentation, not the source of truth** — GitHub is. Re-export after
changing anything:

```sh
gh api repos/kennywillbe/pg2osync/rulesets --jq '.[].id' | while read id; do
  gh api "repos/kennywillbe/pg2osync/rulesets/$id" > ".github/rulesets/release-tags.json"
done
gh api repos/kennywillbe/pg2osync/branches/main/protection > /tmp/protection.json
```

## What they say, and why

`main-branch-protection.json`

- **Every change arrives by pull request.** No direct pushes, and the required
  checks have to be green — `strict` means the branch must also be up to date,
  so a passing check on a stale base does not count. The **Docs** and **Audit**
  workflows are deliberately not required: both only run when the files they
  care about change, and a required check that never starts blocks the merge
  for ever.
- **The pull request title is a required check.** Merges are squash-only, so
  the title becomes the commit subject on `main`, and release-please reads that
  subject to pick the next version and write the changelog. A title that does
  not parse is a change that never shows up in a release.
- **Linear history.** A merge commit's subject is `Merge pull request #52 …`,
  which release-please cannot parse — it did exactly that once, and considered
  zero commits. Squash-only merges keep every commit on `main` a conventional
  one.
- **No force pushes and no deletion.** The history was rewritten once by force;
  after this it cannot be.
- **`enforce_admins` is true, the owner included.** The first version of this
  file argued for leaving the owner a way in at three in the morning. The
  stronger argument won: if the owner can push straight to `main`, then "who
  changed this and what did it pass" has no reliable answer, and the pull
  request list stops being the history of the project. An emergency is still
  possible — turn the setting off, do the thing, turn it back on — and that
  leaves a trace, which is the point.
- **Approvals required: zero.** GitHub does not let anyone approve their own
  pull request, so a single-maintainer repository that demands one approval
  cannot merge at all. The gate here is the checks; the review is the
  maintainer reading their own diff.

`release-tags.json`

- **Only an admin can create, move or delete a `v*` tag.** A tag is what
  publishes binaries and a container image, so it is the one action that reaches
  the outside world — it belongs to whoever owns the release, not to CI and not
  to a contributor.

  release-please tags through `RELEASE_PLEASE_TOKEN`, a fine-grained token owned
  by an admin, which is what satisfies this rule — the Actions app cannot be
  given a bypass on a personal repository at all, and GitHub also refuses to
  start a workflow from an event its own `GITHUB_TOKEN` created, so a bot tag
  would publish nothing. `docs/releasing.md` has both traps written out.
