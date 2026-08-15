#!/usr/bin/env bash
set -eu

version=$(./scripts/check-release-version.sh)
artifact_test_dir=$(mktemp -d)
trap 'rm -rf "$artifact_test_dir"' EXIT

wheels=(dist/"pgtask-$version"-cp310-abi3-*.whl)
if test "${#wheels[@]}" != 1 || test ! -f "${wheels[0]}"; then
    echo "expected one local stable-ABI wheel for pgtask $version" >&2
    exit 1
fi
wheel=$PWD/${wheels[0]}
sdist=$PWD/dist/pgtask-$version.tar.gz
chart=$PWD/dist/pgtask-$version.tgz

for python_version in ${PGTASK_RELEASE_PYTHONS:-3.10 3.14}; do
    environment=$artifact_test_dir/python-$python_version
    uv venv --python "$python_version" "$environment" >/dev/null
    uv pip install --python "$environment/bin/python" opentelemetry-api 'psycopg[binary]' >/dev/null
    uv pip install --python "$environment/bin/python" --no-deps "$wheel" >/dev/null
    "$environment/bin/python" -c \
        "import importlib.metadata, pgtask; assert importlib.metadata.version('pgtask') == '$version'; assert pgtask.Client and pgtask.Worker"
done

uv venv --python 3.14 "$artifact_test_dir/source" >/dev/null
uv pip install --python "$artifact_test_dir/source/bin/python" "$sdist" >/dev/null
"$artifact_test_dir/source/bin/python" -c \
    "import importlib.metadata, pgtask; assert importlib.metadata.version('pgtask') == '$version'; assert pgtask.TaskRegistry"

test "$(target/release/pgtask --version)" = "pgtask $version"
helm show chart "$chart" | rg --fixed-strings "appVersion: $version" >/dev/null
helm template artifact-test "$chart" >/dev/null

if test "${PGTASK_TEST_IMAGE:-true}" = true; then
    test "$(docker run --rm "pgtask:$version" pgtask --version)" = "pgtask $version"
    docker run --rm --entrypoint /bin/sh "pgtask:$version" -c \
        'test -x /usr/local/bin/pgtask && test -x /usr/local/bin/pgtask-bench && test -x /usr/local/bin/pgtask-smoke && test -x /usr/local/bin/pgtask-web'
fi
