## What and why

<!-- What changes, and what problem it solves. Link the issue if there is one. -->

## How it was verified

<!-- CONTRIBUTING.md: push on a green RESULT line; CI should never be the
     first to tell you a change is red. -->

- [ ] `./dev/ci-local.sh` ended with a green `RESULT` line
- [ ] Regression test added that fails without this change

## Checklist

- [ ] Comments explain *why*, not what
- [ ] No architecture boundary crossed (see CONTRIBUTING.md); if a recorded
      decision changed, `docs/decisions.md` is updated here
- [ ] Docs updated for behaviour or configuration changes
- [ ] No secrets in code, tests, logs or fixtures
