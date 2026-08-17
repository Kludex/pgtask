# Releases

## Supported artifacts

A release tag publishes the same version in each artifact:

- Rust crates for the public engine, storage, worker, CLI, and web packages.
- One CPython stable-ABI wheel for Python 3.10 or newer on each supported platform.
- A source distribution for Python installations that intentionally build locally.
- One public `@pgtask/client` package for Node.js 20 or newer.
- One Go module at `github.com/Kludex/pgtask/sdks/go` for Go 1.25 or newer.
- One multi-architecture container image for `linux/amd64` and `linux/arm64`.
- One Helm chart in an OCI registry.

Wheel targets are `manylinux_2_28` on x86-64 and ARM64, macOS 11 or newer on x86-64 and Apple Silicon, and 64-bit Windows. Python 3.10 through 3.14 run the SDK test suite. The stable ABI avoids publishing five copies of the same native extension per platform.

## The release tag sets the version

You do not edit a version before releasing. The tag on the release carries it, and every artifact
takes its version from that tag.

Each job that builds an artifact runs `scripts/set-release-version.sh` with the version from the tag,
which writes it into the workspace `Cargo.toml`, the path dependencies that carry a version for
crates.io, `package.json` and its lock file, and the chart's `version` and `appVersion`. It then
refreshes `Cargo.lock` so a `--locked` build still resolves, and finally runs
`scripts/check-release-version.sh` to confirm every manifest agrees.

The Python wheel needs no separate handling. `sdks/python/pyproject.toml` declares
`dynamic = ["version"]` and `sdks/python/Cargo.toml` inherits the workspace version, so maturin
builds `pgtask-0.2.0` from the same source.

!!! note "Why not uv-dynamic-versioning"

    It derives a Python version straight from the tag, which is what you want, but it requires
    hatchling as the build backend. This SDK needs maturin to compile the `_native` extension, so the
    version arrives through the Cargo workspace instead and reaches the wheel the same way.

The version committed in the repository is the development version. It is not what a release
publishes, and it does not have to be bumped before tagging. CI still checks that the committed
manifests agree with each other, so they never drift apart.

## Configure publishing

Configure the GitHub `pypi` environment as a trusted publisher for the PyPI project. Configure `@pgtask/client` on npm
with this repository and `release.yml` as its trusted publisher. Add `CARGO_REGISTRY_TOKEN` as a repository secret. Allow
GitHub Actions to write packages so it can publish to `ghcr.io`.

The three registries do not need the same preparation before a first release.

| Registry | Before the first release |
| --- | --- |
| PyPI | Nothing. Register a pending publisher from your account sidebar; it becomes a normal publisher on first use |
| crates.io | Nothing. `CARGO_REGISTRY_TOKEN` can publish a crate that does not exist yet |
| npm | **Publish once by hand.** A trusted publisher is configured in the package's settings, which requires the package to exist |

So the npm package is the one gate on a first release:

```console
cd sdks/typescript
npm run build
npm publish --access public
```

Configure the trusted publisher afterwards and every later release goes through OIDC. Publishing a
placeholder such as `0.0.1` works too, if you would rather the first real version came from the
workflow like the others.

The workflow uses GitHub OIDC for keyless Sigstore signatures. It does not store a signing key. Rust crates publish in
dependency order and wait for crates.io indexing before publishing a dependent crate, so a partial workflow can be rerun
without republishing completed artifacts.

## Create a release

Publishing a GitHub Release is what releases. Draft one, let GitHub create the `v1.0.0` tag on the
commit you are releasing, write the notes, and publish it:

```console
gh release create v1.0.0 --draft --generate-notes
```

Publishing the draft starts the workflow. Nothing happens while it stays a draft, so the notes can be
written and reviewed before anything is published to a registry.

The workflow reruns the full Rust suite on PostgreSQL 17 and 18 before any artifact is published,
then attaches the wheels, source distribution, npm package, and chart to the release you published.
It also creates the matching `sdks/go/v1.0.0` tag at the same commit so the Go module proxy resolves
the release.

!!! warning "A published release cannot be unpublished cleanly"

    Registries do not accept a version twice. If a release fails partway, fix forward with a new
    patch version rather than retrying the same one.

## Test local artifacts

Build and verify every artifact locally before tagging a release:

```console
./scripts/build-release-artifacts.sh
./scripts/test-release-artifacts.sh
```

The build packages and verifies every Cargo crate, release binary, stable-ABI wheel, Python source distribution, Helm chart, and local container image. The test installs the wheel on Python 3.10 and 3.14, builds the source distribution in a clean environment, renders the packaged chart, and runs the image as a non-root user.

Set `PGTASK_BUILD_IMAGE=false` or `PGTASK_TEST_IMAGE=false` only on a host without Docker. Set `PGTASK_RELEASE_PYTHONS` to a space-separated Python version list when you need a broader local compatibility check. Release CI remains authoritative for every supported operating system and architecture.

## Verify artifacts

Verify the image with the immutable digest shown by the release:

```console
cosign verify \
  --certificate-identity-regexp '^https://github.com/Kludex/pgtask/.github/workflows/release.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/kludex/pgtask@sha256:DIGEST
```

Verify the chart before installing it:

```console
cosign verify \
  --certificate-identity-regexp '^https://github.com/Kludex/pgtask/.github/workflows/release.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/kludex/charts/pgtask:1.0.0

helm install pgtask oci://ghcr.io/kludex/charts/pgtask --version 1.0.0
```

The final GitHub release is created only after PyPI, npm, crates.io, the signed image, and the signed chart succeed.
