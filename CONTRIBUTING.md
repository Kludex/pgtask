# Contributing

## Setup

Install the pinned Rust toolchain, Docker, and Docker Compose. Then start PostgreSQL:

```console
docker compose up -d postgres
```

## Checks

Run these commands before submitting a change:

```console
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Tests that exercise persistence must use the public API and a real PostgreSQL database. Do not replace PostgreSQL behavior with mocks.

## Changes

Keep changes focused. Update `PLAN.md` when architecture or scope changes. Update `TODO.local.md` in the same change that completes or re-scopes roadmap work.
