# Cutting a release

Three commands, and a pull request you merge in between. Nothing here happens
automatically: the version, the changelog and the tag are each an explicit act.

## How it works

Every push to `main` runs [release-please](https://github.com/googleapis/release-please),
which reads the [Conventional Commits](https://www.conventionalcommits.org/)
since the last release and keeps **one open pull request** up to date — the
release PR. It bumps every crate in the workspace to the same version and writes
the `CHANGELOG.md` entry.

```
merge a feature PR   →  release PR updated. Nothing released.
merge a feature PR   →  release PR updated. Nothing released.
merge the release PR →  versions bumped, changelog written, tag pushed,
                        binaries and the container image published.
```

The version comes from the commit types: `fix:` bumps the patch, `feat:` the
minor, and either a `!` after the type or a `BREAKING CHANGE:` trailer bumps the
major. To force a number regardless, put `Release-As: 1.3.0` in a commit body.

A batch release is the normal mode rather than a special case: whatever
accumulated since the last release goes out as one version.

## What you have to do

Merge the open pull request titled `chore: release …`. That is the whole
procedure.

Every other pull request is **squash-merged**, and its title becomes the commit
subject on `main`. That is what release-please reads, so the title has to be a
conventional commit — a required check enforces it. Merge commits are switched
off: the one that did land (`Merge pull request #52 …`) made release-please
consider zero commits and propose nothing.

Watch the **Release** workflow afterwards. It builds static binaries for Linux
and macOS on x86-64 and arm64, attaches them with checksums, and pushes
`ghcr.io/kennywillbe/pg2osync` tagged with the version and with `major.minor`.

## The one thing to set up

A fine-grained personal access token belonging to a repository admin, stored as
the repository secret **`RELEASE_PLEASE_TOKEN`**:

- **Repository access:** only this repository.
- **Permissions:** *Contents* read and write, *Pull requests* read and write, and
  *Issues* read and write. The last one is not obvious and is required: the
  labels release-please puts on its own pull request (`autorelease: pending`)
  go through the issues API, which is how GitHub models labels on a pull
  request. *Metadata* read-only is added for you and is mandatory.
- **Account permissions:** none.
- **Expiry:** whatever you are willing to renew. When it expires, release pull
  requests silently stop appearing — the workflow keeps passing because there is
  nothing for it to do. Put the expiry date somewhere you will see it.

The default `GITHUB_TOKEN` cannot do this job, and the workflow fails loudly
rather than half-releasing if the secret is missing. Two independent reasons:

- The `v*` tag rule allows only a repository admin to create a tag, and a
  personal repository cannot grant that bypass to the Actions app — GitHub
  refuses with *"Actor GitHub Actions integration must be part of the ruleset
  source or owner organization"*. A token owned by an admin passes.
- GitHub does not start a workflow from an event created by its own
  `GITHUB_TOKEN`. A tag pushed that way would appear with **nothing built behind
  it** — no binaries, no image, a release that looks finished and is empty. A tag
  pushed with a personal access token triggers the build normally.

Because the token is yours, the tag and the release are attributed to you rather
than to a bot.

## Conventional Commits, briefly

Only the subject line is constrained. The body stays whatever the change needs,
which for anything non-obvious here means saying why.

```
feat(mysql): resume from a GTID position

A binlog coordinate only means something on the server it was read from, so a
failover could not resume …
```

| Type | Effect | For |
|---|---|---|
| `feat:` | minor | new behaviour |
| `fix:` | patch | a defect |
| `perf:` `refactor:` | patch | same behaviour, different shape |
| `docs:` `test:` `chore:` `ci:` | none | no released change |
| `feat!:` or `BREAKING CHANGE:` | **major** | a config that stops loading, or a checkpoint that forces a reload |

The last row is the one that matters most: what a major version promises here is
that a `pg2osync.toml` keeps loading and a running pipeline does not re-read a
table from the start. Breaking either is a major, whatever else the change looks
like.

## The documentation site

The **Docs** workflow builds this book out of `docs/` with mdBook and publishes
it to GitHub Pages on every push to `main` that touches the documentation. Pull
requests build it without publishing, so a broken page or a chapter missing from
`SUMMARY.md` fails there rather than on the live site.

Publishing is switched off while the repository is private, because Pages on this
plan is public whatever the repository is, and the docs must not lead the code
out. Once the repository is public, one setting starts it:

```sh
gh api -X POST repos/kennywillbe/pg2osync/pages -f build_type=workflow
```

Or **Settings → Pages → Source: GitHub Actions**. The site is then
<https://kennywillbe.github.io/pg2osync/>, and nothing else has to be done again.
