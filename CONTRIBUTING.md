# Contributing

## Setup

Install the pinned Rust toolchain, Docker, Kubernetes, Helm, and Tilt. Select a local Kubernetes context, then start the
development environment:

```console
tilt up
export PGTASK_DATABASE_URL=postgresql://pgtask:pgtask@localhost:54329/pgtask
```

Tilt installs `charts/pgtask` with disposable PostgreSQL storage and forwards PostgreSQL to port `54329`. Run
`tilt down` to remove the Helm release.

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
