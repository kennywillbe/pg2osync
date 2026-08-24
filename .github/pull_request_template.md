## What and why

<!-- What changes, and what problem it solves. Link the issue if there is one. -->

## How it was verified

<!-- Delete what does not apply; paste output for what you ran. -->

- [ ] `cargo fmt --all` and `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo test --workspace`
- [ ] `./dev/e2e-test.sh` (pipeline changes)
- [ ] `./dev/e2e-mysql-test.sh` (MySQL source changes)
- [ ] Regression test added that fails without this change

## Checklist

- [ ] Comments explain *why*, not what
- [ ] No architecture boundary crossed (see CONTRIBUTING.md); if a recorded
      decision changed, `docs/decisions.md` is updated here
- [ ] Docs updated for behaviour or configuration changes
- [ ] No secrets in code, tests, logs or fixtures
