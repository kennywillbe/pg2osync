# Security Policy

## Supported versions

`1.0.x` — latest patch is supported. There are no other versions.

## Reporting a vulnerability

Please **do not open a public issue** for security problems.

Use GitHub's [private vulnerability reporting](https://github.com/kennywillbe/pg2osync/security/advisories/new),
or email the maintainers at `security@pg2osync.dev`. Include:

- affected version (`pg2osync --version`) and deployment shape
- what an attacker can achieve, and the smallest reproduction you have
- logs or configuration with credentials redacted

You can expect an acknowledgement within 5 working days and a fix or mitigation
plan within 30 days for confirmed issues. We will credit you in the release
notes unless you prefer otherwise.

## Scope

In scope: credential handling, log/error leakage of secrets, TLS verification,
SQL and query injection through configuration values, privilege requirements
that are wider than documented, and anything that causes silent data loss.

Out of scope: findings that require a source or target database already under
attacker control, and denial of service caused by deliberately misconfigured
resource limits.

## Handling secrets

pg2osync reads credentials from environment variables (`url_env`,
`password_env`, `api_key_env`) and never logs them. Plain-text secrets in the
config file are accepted but warn on startup. If you find a code path that
prints a secret, treat it as a vulnerability and report it.
