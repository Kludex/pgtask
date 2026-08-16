# Contributing

## Setup

Install the pinned Rust toolchain, Docker, Kubernetes, Helm, and Tilt. Select a local Kubernetes context that advertises
a local registry. Without one Tilt pushes the development image to Docker Hub and the build fails with
`denied: requested access to the resource is denied`. Create a suitable cluster with `k3d`:

```console
k3d cluster create pgtask --registry-create pgtask-registry:0.0.0.0:5111
```

Then start the development environment:

```console
tilt up
export PGTASK_DATABASE_URL=postgresql://pgtask:pgtask@localhost:54329/pgtask
```

Tilt installs `charts/pgtask` with disposable PostgreSQL storage and forwards PostgreSQL to port `54329`. Run
`tilt down` to remove the Helm release.

## Documentation

The site is built with [Zensical](https://zensical.org). Preview it with a live-reloading server:

```console
uvx zensical serve
```

`uvx zensical build` writes the static site to `site/`. Content lives in `docs/` and the navigation is declared in
`zensical.toml`. A heading is always followed by a sentence before any code block, so a reader never lands on a snippet
with no idea what it does.

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
