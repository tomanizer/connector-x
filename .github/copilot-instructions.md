# Copilot Instructions

ConnectorX is a performance-sensitive Rust data-loading engine with Python bindings. Preserve the existing zero-copy/low-copy design, feature-gated source and destination modules, and explicit transport conversions.

## Review Guidance

- Prioritize correctness, memory safety, data corruption, SQL/connection-string injection, credential leakage, portability, and performance regressions.
- Keep review comments actionable and tied to changed lines. Do not request broad refactors unless the diff creates real risk.
- For new database sources, verify the full path: Cargo feature flag, source module, type system, dispatcher/router registration, destination transports, tests, docs, and Python exposure when applicable.
- Check that connection URLs and ODBC/JDBC strings escape user-controlled values. Never suggest logging full URLs, DSNs, passwords, tokens, or database contents.
- Treat live database tests as optional and env-gated. Unit tests must pass without real credentials or network databases.
- Be skeptical of changes that add extra row copies, per-cell allocations, repeated schema queries, unnecessary string parsing in hot loops, or global locks in fetch paths.
- For CI changes in this fork, Ubuntu connector CI is the default gate. macOS and Windows connector checks are manual opt-in unless a PR is being prepared for upstream.

## Coding Agent Guidance

- Keep changes narrow and consistent with existing module patterns under `connectorx/src/sources`, `connectorx/src/destinations`, and `connectorx/src/transports`.
- Prefer feature-gated Rust code over unconditional dependencies. New optional sources should compile with `--no-default-features` plus their exact source and destination features.
- Do not introduce new public behavior without focused tests. For connectors, add parser/URL tests and env-gated live tests that skip clearly when variables are unset.
- Use structured APIs for URLs, SQL metadata, ODBC/JDBC values, and Arrow arrays. Avoid ad hoc string manipulation where an existing parser or helper is available.
- Do not hardcode local paths, credentials, driver names, database hosts, or user-specific environment assumptions.
- Keep benchmark and profiling dependencies out of Windows builds unless they are known to compile there.

## Useful Commands

- Format: `cargo fmt --all`
- Rust unit tests: `cargo test --features all -- --nocapture`
- Focused connector check: `cargo check -p connectorx --no-default-features --features "<src_feature> dst_arrow fptr"`
- Focused connector test: `cargo test -p connectorx --no-default-features --features "<src_feature> dst_arrow fptr" --test <test_name> -- --nocapture`
- Python setup/tests: `just setup-python` and `just test-python`

## Repository Map

- Rust crate: `connectorx`
- Python package and bindings: `connectorx-python`
- Source implementations: `connectorx/src/sources`
- Destination implementations: `connectorx/src/destinations`
- Source-to-destination conversions: `connectorx/src/transports`
- Rust connector tests: `connectorx/tests`
- Python tests: `connectorx-python/connectorx/tests`
- Docs: `docs`
