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

- **Every change arrives by pull request.** No direct pushes, and the six CI
  jobs have to be green — `strict` means the branch must also be up to date, so
  a passing check on a stale base does not count.
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
