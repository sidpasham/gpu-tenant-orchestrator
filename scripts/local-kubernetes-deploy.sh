#!/bin/bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-gpu-tenant-orchestrator}"
MANIFEST_DIR="deploy/kubernetes/rancher-desktop"
MANIFEST_FILES=(
    "$MANIFEST_DIR/00-namespace.yaml"
    "$MANIFEST_DIR/10-configmaps.yaml"
    "$MANIFEST_DIR/20-serviceaccounts.yaml"
    "$MANIFEST_DIR/30-roles.yaml"
    "$MANIFEST_DIR/31-rolebindings.yaml"
    "$MANIFEST_DIR/35-services.yaml"
    "$MANIFEST_DIR/40-deployments.yaml"
    "$MANIFEST_DIR/70-ingress.yaml"
)
IMAGE_TAG="${IMAGE_TAG:-$(date +%Y%m%d%H%M%S)}"
API_IMAGE="${API_IMAGE:-gpu-tenant-orchestrator-api:${IMAGE_TAG}}"
WORKER_IMAGE="${WORKER_IMAGE:-gpu-tenant-orchestrator-worker:${IMAGE_TAG}}"
GRAFANA_DASHBOARD_DIR="deploy/monitoring/grafana"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_status() {
    echo -e "${BLUE}[*] $1${NC}"
}

print_success() {
    echo -e "${GREEN}[✔] $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}[!] $1${NC}"
}

show_help() {
    echo "Usage: ./scripts/local-kubernetes-deploy.sh [up|down|status|logs-api|logs-worker|logs-kafka]"
    echo "  up           - Build images and deploy all services to Rancher Desktop Kubernetes"
    echo "  down         - Delete the Kubernetes namespace and all local services"
    echo "  status       - Show Kubernetes resources and ingress endpoints"
    echo "  logs-api     - Follow API pod logs"
    echo "  logs-worker  - Follow worker pod logs"
    echo "  logs-kafka   - Follow Kafka pod logs"
}

require_rancher_desktop() {
    local context
    context="$(kubectl config current-context)"
    if [ "$context" != "rancher-desktop" ]; then
        echo "Expected kubectl context 'rancher-desktop', got '$context'." >&2
        exit 1
    fi

    local docker_context
    docker_context="$(docker context show)"
    if [ "$docker_context" != "rancher-desktop" ]; then
        echo "Expected Docker context 'rancher-desktop', got '$docker_context'." >&2
        echo "Run: docker context use rancher-desktop" >&2
        exit 1
    fi
}

build_images() {
    print_status "Building API image for Rancher Desktop Kubernetes: $API_IMAGE"
    docker build -f Dockerfile.api -t "$API_IMAGE" .

    print_status "Building worker image for Rancher Desktop Kubernetes: $WORKER_IMAGE"
    docker build -f Dockerfile.worker -t "$WORKER_IMAGE" .
}

apply_grafana_configmaps() {
    print_status "Applying Grafana dashboard configuration..."
    kubectl -n "$NAMESPACE" create configmap grafana-datasource-config \
        --from-file=datasource.yaml="$GRAFANA_DASHBOARD_DIR/provisioning/datasources/datasource.yaml" \
        --dry-run=client \
        -o yaml | kubectl apply -f -
    kubectl -n "$NAMESPACE" create configmap grafana-dashboard-provider \
        --from-file=dashboards.yaml="$GRAFANA_DASHBOARD_DIR/provisioning/dashboards/dashboards.yaml" \
        --dry-run=client \
        -o yaml | kubectl apply -f -
    kubectl -n "$NAMESPACE" create configmap grafana-dashboard-config \
        --from-file=gpu-tenant-metrics.json="$GRAFANA_DASHBOARD_DIR/dashboards/gpu-tenant-metrics.json" \
        --dry-run=client \
        -o yaml | kubectl apply -f -
    kubectl -n "$NAMESPACE" rollout restart deployment/grafana
}

deploy_stack() {
    require_rancher_desktop
    build_images

    print_status "Applying Kubernetes manifests..."
    if kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
        kubectl -n "$NAMESPACE" delete job/kafka-topic-init --ignore-not-found
    fi
    local apply_args=()
    local manifest_file
    for manifest_file in "${MANIFEST_FILES[@]}"; do
        apply_args+=("-f" "$manifest_file")
    done
    kubectl apply "${apply_args[@]}"
    apply_grafana_configmaps

    print_status "Promoting freshly built images into the API and worker deployments..."
    kubectl -n "$NAMESPACE" set image deployment/api api="$API_IMAGE"
    kubectl -n "$NAMESPACE" set image deployment/worker worker="$WORKER_IMAGE"

    print_status "Waiting for core services..."
    kubectl -n "$NAMESPACE" rollout status deployment/kafka --timeout=180s
    kubectl -n "$NAMESPACE" rollout status deployment/temporal --timeout=180s
    kubectl -n "$NAMESPACE" rollout status deployment/api --timeout=180s
    kubectl -n "$NAMESPACE" rollout status deployment/worker --timeout=180s
    kubectl -n "$NAMESPACE" rollout status deployment/prometheus --timeout=180s
    kubectl -n "$NAMESPACE" rollout status deployment/grafana --timeout=180s

    print_success "Rancher Desktop Kubernetes environment is ready."
    echo "--------------------------------------------------------"
    echo -e "API:          ${GREEN}http://gpu-tenant.localhost${NC}"
    echo -e "Temporal UI:  ${GREEN}http://temporal.gpu-tenant.localhost${NC}"
    echo -e "Grafana:      ${GREEN}http://grafana.gpu-tenant.localhost${NC} (admin/admin)"
    echo -e "Prometheus:   ${GREEN}http://prometheus.gpu-tenant.localhost${NC}"
    echo "--------------------------------------------------------"
    print_warning "Worker uses in-cluster RBAC and performs real Helm installs into tenant namespaces."
}

delete_stack() {
    require_rancher_desktop
    print_status "Deleting tenant namespaces created by local Helm releases..."
    tenant_namespaces="$(kubectl get namespaces -o name | sed -n 's#^namespace/\(tenant-.*\)#\1#p')"
    if [ -n "$tenant_namespaces" ]; then
        for tenant_namespace in $tenant_namespaces; do
            kubectl delete namespace "$tenant_namespace" --ignore-not-found
        done
    fi
    print_status "Deleting namespace '$NAMESPACE' and all local Kubernetes services..."
    kubectl delete namespace "$NAMESPACE" --ignore-not-found
    print_status "Deleting cluster-scoped RBAC for the local worker..."
    kubectl delete clusterrolebinding gpu-tenant-worker-helm --ignore-not-found
    kubectl delete clusterrole gpu-tenant-worker-helm --ignore-not-found
    print_success "Rancher Desktop Kubernetes environment deleted."
}

show_status() {
    require_rancher_desktop
    kubectl -n "$NAMESPACE" get pods,deployments,services,jobs,ingress
}

case "${1:-}" in
    up)
        deploy_stack
        ;;
    down)
        delete_stack
        ;;
    status)
        show_status
        ;;
    logs-api)
        require_rancher_desktop
        kubectl -n "$NAMESPACE" logs -f deployment/api
        ;;
    logs-worker)
        require_rancher_desktop
        kubectl -n "$NAMESPACE" logs -f deployment/worker
        ;;
    logs-kafka)
        require_rancher_desktop
        kubectl -n "$NAMESPACE" logs -f deployment/kafka
        ;;
    *)
        show_help
        exit 1
        ;;
esac
