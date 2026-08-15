#!/bin/sh
set -eu

cluster_name="pgtask-roadmap-$$"
namespace="pgtask"
release="pgtask"
image="pgtask-kind:test"
postgres_service="pgtask-pgtask-postgres"
worker_deployment="pgtask-pgtask-default"

cleanup() {
    trap - EXIT INT TERM
    kind delete cluster --name "$cluster_name"
}
trap cleanup EXIT INT TERM

if test "${PGTASK_KIND_SKIP_BUILD:-false}" != true; then
    docker build --tag "$image" .
fi
if test -n "${PGTASK_KIND_NODE_IMAGE:-}"; then
    kind create cluster --name "$cluster_name" --image "$PGTASK_KIND_NODE_IMAGE" --wait 120s
else
    kind create cluster --name "$cluster_name" --wait 120s
fi
kind load docker-image "$image" --name "$cluster_name"
kind load docker-image postgres:17 --name "$cluster_name"
kubectl create namespace "$namespace"

helm template "$release" ./charts/pgtask \
    --namespace "$namespace" \
    --values ./charts/pgtask/values-development.yaml \
    --set developmentPostgres.image=postgres:17 \
    --set migrations.enabled=false \
    --set workers.default.enabled=false \
    --show-only templates/development-postgres.yaml \
    | kubectl apply --namespace "$namespace" --filename -
kubectl rollout status "deployment/$postgres_service" --namespace "$namespace" --timeout=120s

helm upgrade --install "$release" ./charts/pgtask \
    --namespace "$namespace" \
    --values ./charts/pgtask/values-development.yaml \
    --values ./charts/pgtask/values-kind.yaml \
    --wait \
    --timeout 120s

kubectl run initial-drain \
    --namespace "$namespace" \
    --image "$image" \
    --image-pull-policy Never \
    --restart Never \
    --env "PGTASK_DATABASE_URL=postgresql://pgtask:pgtask@$postgres_service:5432/pgtask" \
    --env PGTASK_QUEUE=default \
    --env PGTASK_SMOKE_TASKS=20 \
    --command -- pgtask-smoke enqueue
kubectl wait pod/initial-drain --namespace "$namespace" --for=jsonpath='{.status.phase}'=Succeeded --timeout=120s
kubectl logs pod/initial-drain --namespace "$namespace" | grep -q 'drained 20 tasks'

kubectl rollout restart "deployment/$worker_deployment" --namespace "$namespace"
kubectl rollout status "deployment/$worker_deployment" --namespace "$namespace" --timeout=120s

kubectl run forced-recovery \
    --namespace "$namespace" \
    --image "$image" \
    --image-pull-policy Never \
    --restart Never \
    --env "PGTASK_DATABASE_URL=postgresql://pgtask:pgtask@$postgres_service:5432/pgtask" \
    --env PGTASK_QUEUE=default \
    --env PGTASK_SMOKE_TASKS=40 \
    --command -- pgtask-smoke enqueue
sleep 1
worker_pod="$(kubectl get pods --namespace "$namespace" --selector app.kubernetes.io/component=worker -o jsonpath='{.items[0].metadata.name}')"
case "$worker_pod" in
    pgtask-pgtask-default-*) ;;
    *) exit 1 ;;
esac
kubectl delete pod "$worker_pod" --namespace "$namespace" --grace-period=0 --force
kubectl rollout status "deployment/$worker_deployment" --namespace "$namespace" --timeout=120s
kubectl wait pod/forced-recovery --namespace "$namespace" --for=jsonpath='{.status.phase}'=Succeeded --timeout=120s
kubectl logs pod/forced-recovery --namespace "$namespace" | grep -q 'drained 40 tasks'

helm upgrade "$release" ./charts/pgtask \
    --namespace "$namespace" \
    --values ./charts/pgtask/values-development.yaml \
    --values ./charts/pgtask/values-kind.yaml \
    --set workers.default.concurrency=5 \
    --wait \
    --timeout 120s
helm rollback "$release" 1 --namespace "$namespace" --wait --timeout 120s

kubectl get "deployment/$worker_deployment" --namespace "$namespace"
