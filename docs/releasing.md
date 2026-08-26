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
merge the release PR →  versions bumped, changelog written. Still nothing released.
push the tag         →  binaries for four targets, and the container image.
```

The version comes from the commit types: `fix:` bumps the patch, `feat:` the
minor, and either a `!` after the type or a `BREAKING CHANGE:` trailer bumps the
major. To force a number regardless, put `Release-As: 1.3.0` in a commit body.

## Doing it

```sh
# 1. merge the open release PR (it is titled "chore: release …")
# 2. then, on main:
git pull
git tag v1.3.0            # the version release-please just wrote
git push origin v1.3.0
```

Watch the **Release** workflow. It builds static binaries for Linux and macOS on
x86-64 and arm64, attaches them with checksums, and pushes
`ghcr.io/kennywillbe/pg2osync` tagged with the version and with `major.minor`.

## Why a person pushes the tag

release-please can create the tag and the GitHub release itself, and here it is
told not to (`skip-github-release`). Two reasons, both concrete:

- The `v*` tag rule allows only a repository admin to create one. A personal
  repository cannot grant that bypass to the Actions app — GitHub refuses with
  *"Actor GitHub Actions integration must be part of the ruleset source or owner
  organization"* — so the bot would fail on every release.
- The way around that is a long-lived personal access token stored in the
  repository. That is a standing credential with write access, kept so a bot can
  do what `git push origin v1.3.0` does. Not worth it.

There is a second reason to know about even if the first is ever fixed: GitHub
does not start a workflow from an event created by its own `GITHUB_TOKEN`. A tag
pushed by the bot would appear and **nothing would build** — no binaries, no
image, a release that looks finished and is empty. A tag pushed by a person
triggers the workflow normally.

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
