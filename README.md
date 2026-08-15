# pgtask

`pgtask` is a PostgreSQL-native durable task and workflow engine written in Rust.

It is under active development. The design and delivery gates are documented in [`PLAN.md`](PLAN.md). The first supported release will provide transactional enqueueing, leased at-least-once execution, delayed and recurring tasks, durable checkpoints, native OpenTelemetry telemetry, a Python SDK, a Helm chart, and an optional UI.

Workers require session-capable PostgreSQL connections. `LISTEN` and `NOTIFY` are the normal dispatch path; low-frequency database polling only reconciles notifications missed during disconnects.

PostgreSQL is the only required service. PostgreSQL extensions are never required for correctness.

## Development

Start PostgreSQL:

```console
docker compose up -d postgres
```

Run the Rust checks:

```console
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Run the Python checks:

```console
uv sync --group dev
uv run ruff format --check python tests
uv run ruff check python tests
uv run mypy python tests
uv run pytest --cov=pgtask --cov=tests
```

Run the Helm lifecycle suite with `./scripts/test-kind-lifecycle.sh`. If registry access is unavailable, cross-build the Linux binaries, run `./scripts/build-prebuilt-image.sh`, and set `PGTASK_KIND_SKIP_BUILD=true`. Set `PGTASK_KIND_NODE_IMAGE` to use a locally cached `kind` node image.

The project is not ready for production use.

## Documentation

- [`PLAN.md`](PLAN.md) defines the roadmap and release gates.
- [`docs/failure-model.md`](docs/failure-model.md) defines crash and recovery behavior.
- [`docs/public-contracts.md`](docs/public-contracts.md) defines the versioned Rust, Python, SQL, CLI, and telemetry surfaces.
- [`docs/python.md`](docs/python.md) defines the typed Python API and ARQ migration approach.
- [`docs/sql-protocol.md`](docs/sql-protocol.md) defines the cross-language PostgreSQL API.
- [`docs/telemetry.md`](docs/telemetry.md) defines OpenTelemetry setup and instruments.
- [`docs/ui.md`](docs/ui.md) defines the observer service and opt-in audited administrator mode.
- Migration guides describe adopter-neutral rollout and rollback gates.
- [`docs/operations.md`](docs/operations.md) covers backup, restore, retention, draining, and incident response.
- [`docs/schema-compatibility.md`](docs/schema-compatibility.md) defines forward migrations, rolling compatibility, and rollback.
- [`docs/security-review.md`](docs/security-review.md) records trust boundaries, privilege checks, and accepted risks.
- [`docs/releases.md`](docs/releases.md) defines supported artifacts, publishing, and signature verification.
- [`docs/migrate-from-arq.md`](docs/migrate-from-arq.md) and [`docs/migrate-from-absurd.md`](docs/migrate-from-absurd.md) define explicit migration and rollback paths.

## License

This project is licensed under either the Apache License 2.0 or the MIT License, at your option.
