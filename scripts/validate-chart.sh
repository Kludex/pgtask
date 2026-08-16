#!/bin/sh
set -eu

validation_dir=$(mktemp -d)
trap 'rm -rf "$validation_dir"' EXIT

case "$(uname -s)-$(uname -m)" in
    Darwin-arm64)
        kube_archive=kubeconform-darwin-arm64.tar.gz
        kube_checksum=f84f4dfbebf4a6b0b230385fa065a39ea35e02608c2b50d025dcf64775a69d67
        ct_archive=chart-testing_3.14.0_darwin_arm64.tar.gz
        ct_checksum=db10dbbb42b110c7a5da5a3202908f32ad2ca6ad600d423426dc7886d09aad07
        ;;
    Linux-x86_64)
        kube_archive=kubeconform-linux-amd64.tar.gz
        kube_checksum=9bc2bffbf71f261128533edaf912153948b7ff238f9a531ae6d34466ec287883
        ct_archive=chart-testing_3.14.0_linux_amd64.tar.gz
        ct_checksum=d16f0583616885423826241164ce1f6589c6fe5332fa74f374ebd2bd3cb3fe1f
        ;;
    *)
        echo "unsupported chart-validation platform: $(uname -s)-$(uname -m)" >&2
        exit 1
        ;;
esac

curl -fsSL "https://github.com/yannh/kubeconform/releases/download/v0.8.0/$kube_archive" \
    -o "$validation_dir/$kube_archive"
curl -fsSL "https://github.com/helm/chart-testing/releases/download/v3.14.0/$ct_archive" \
    -o "$validation_dir/$ct_archive"

actual_kube_checksum=$(shasum -a 256 "$validation_dir/$kube_archive" | cut -d ' ' -f 1)
actual_ct_checksum=$(shasum -a 256 "$validation_dir/$ct_archive" | cut -d ' ' -f 1)
test "$actual_kube_checksum" = "$kube_checksum"
test "$actual_ct_checksum" = "$ct_checksum"

tar -xzf "$validation_dir/$kube_archive" -C "$validation_dir"
tar -xzf "$validation_dir/$ct_archive" -C "$validation_dir"
uv venv "$validation_dir/venv" >/dev/null
uv pip install --python "$validation_dir/venv/bin/python" yamale yamllint >/dev/null

helm lint charts/pgtask
helm lint charts/pgtask --values charts/pgtask/values-development.yaml
helm template test charts/pgtask \
    | "$validation_dir/kubeconform" -strict -summary -ignore-missing-schemas
helm template test charts/pgtask --values charts/pgtask/values-development.yaml \
    | "$validation_dir/kubeconform" -strict -summary -ignore-missing-schemas
helm template test charts/pgtask \
    --set ui.enabled=true \
    --set ui.administrator.enabled=true \
    --set ui.administrator.actorHeader=x-authenticated-user \
    --set ui.ingress.enabled=true \
    --set serviceMonitor.enabled=true \
    --set workers.default.enabled=true \
    --set workers.default.image.repository=example.invalid/pgtask-worker \
    --set-json 'workers.default.command=["/app/worker"]' \
    --set workers.default.autoscaling.enabled=true \
    --set workers.default.autoscaling.queueDemand.enabled=true \
    --set networkPolicy.enabled=true \
    | "$validation_dir/kubeconform" -strict -summary -ignore-missing-schemas

if helm template test charts/pgtask \
    --set workers.default.enabled=true \
    --set workers.default.image.repository=example.invalid/pgtask-worker \
    --set-json 'workers.default.command=["/app/worker"]' \
    --set workers.default.replicas=0 >/dev/null 2>&1; then
    echo "scheduler-enabled workers must reject scale to zero" >&2
    exit 1
fi

if helm template test charts/pgtask --set workers.default.enabled=true >/dev/null 2>&1; then
    echo "enabled workers must require an application image and command" >&2
    exit 1
fi

PATH="$validation_dir/venv/bin:$PATH" "$validation_dir/ct" lint \
    --charts charts/pgtask \
    --validate-maintainers=false \
    --check-version-increment=false \
    --chart-yaml-schema "$validation_dir/etc/chart_schema.yaml" \
    --lint-conf "$validation_dir/etc/lintconf.yaml"
