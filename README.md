# GPU Tenant Orchestrator

## Status

GPU Tenant Orchestrator is a local-development prototype for accepting GPU
allocation requests, queueing them through Kafka, and executing tenant workload
deployments through Temporal and Helm.

The repository is suitable for local experimentation and unit-test driven
iteration. It is not production-ready until the production readiness checklist
in this document is completed.

## Problem Statement

GPU tenants need a simple ingress path for requesting standard allocation
profiles. The orchestrator provides an API boundary, durable event handoff, and
workflow execution layer so the request path is separated from the Kubernetes
deployment path.

## Goals

- Accept validated tenant GPU allocation requests over HTTP.
- Normalize request data before publishing it to Kafka.
- Consume allocation events and start a Temporal workflow per tenant request.
- Choose a deterministic GPU pool using region, latency, health, GPU type, and
  available capacity.
- Execute a Helm upgrade/install for the tenant workload chart.
- Support local development with Rancher Desktop Kubernetes, unit tests, and
  mock GPU mode.

## Non-Goals

- GPU cluster autoscaling or cloud capacity procurement.
- Billing, quota enforcement, or approval workflows.
- Multi-cloud placement decisions beyond the configured pool inventory.
- Production identity, authorization, and audit systems.
- Full Slurm or Kubernetes operator lifecycle management.

## Architecture

```mermaid
flowchart LR
    Client[Client] -->|POST /api/v1/tenant/allocate| API[FastAPI API]
    API -->|produce confirmed allocation event| Kafka["Kafka topic<br/>gpu-allocations"]
    Kafka --> Dispatcher["Worker<br/>KafkaWorkflowDispatcher"]
    Dispatcher -->|start workflow| Temporal["Temporal<br/>GPUAllocationWorkflow"]
    Temporal --> Scheduler["plan_gpu_allocation activity<br/>region-aware reservation"]
    Scheduler -->|reserved| Activity[run_helm_deploy activity]
    Scheduler -->|no acceptable capacity| Pending["PENDING_CAPACITY<br/>no Helm deploy"]
    Activity --> Helm[Helm upgrade/install]
    Helm --> Namespace["Tenant namespace<br/>tenant-customer-id"]
    Namespace --> ConfigMap["Tenant ConfigMap<br/>submit_job.sh"]
    Namespace --> Job[Tenant submitter Job]
    Dispatcher -->|invalid event or workflow-start failure| DLQ["Kafka topic<br/>gpu-allocations-dlq"]
    API -->|/metrics| Prometheus[Prometheus]
    Dispatcher -->|metrics: customer, tier, GPU, duration| Prometheus
    Prometheus --> Grafana[Grafana dashboard]
```

```mermaid
flowchart TB
    subgraph RD[Rancher Desktop Kubernetes]
        subgraph PlatformNS[gpu-tenant-orchestrator namespace]
            ApiIngress["API Ingress<br/>gpu-tenant.localhost"] --> ApiSvc[api Service]
            ApiSvc --> ApiDeploy[api Deployment x2]
            ApiDeploy --> KafkaSvc[kafka Service]

            WorkerSvc["worker Service<br/>:8081 health/metrics"] --> WorkerDeploy[worker Deployment]
            WorkerDeploy --> KafkaSvc
            WorkerDeploy --> TemporalSvc[temporal Service]
            WorkerDeploy --> KubeApi[Kubernetes API]

            PromIngress[Prometheus Ingress] --> PromSvc[prometheus Service]
            PromSvc --> PromDeploy[prometheus Deployment]
            PromDeploy --> ApiSvc
            PromDeploy --> WorkerSvc
            GrafanaIngress[Grafana Ingress] --> GrafanaSvc[grafana Service]
            GrafanaSvc --> GrafanaDeploy[grafana Deployment]
            GrafanaDeploy --> GrafanaConfig["Provisioned dashboards<br/>and datasource ConfigMaps"]
            TemporalIngress[Temporal UI Ingress] --> TemporalSvc
        end

        KubeApi --> TenantNS["Tenant namespaces<br/>tenant-team-a, tenant-team-b"]
        TenantNS --> TenantJobs[Tenant Helm Jobs and ConfigMaps]
    end
```

## Component Design

| Component | Path | Responsibility |
| --- | --- | --- |
| API gateway | `src/api/main.py` | Validates tenant input and publishes allocation events to Kafka. |
| Shared config | `src/shared/config.py` | Reads runtime configuration from environment variables. |
| Kafka dispatcher and worker | `src/temporal/worker.py` | Owns Kafka consumer lifecycle, starts Temporal workflows on the worker event loop, exposes worker health/metrics, and routes failed events to the DLQ. |
| Workflow | `src/temporal/workflows.py` | Defines the `GPUAllocationWorkflow` orchestration boundary. |
| Placement scheduler | `src/placement/scheduler.py` | Scores configured GPU pools, reserves available capacity, and returns pending capacity when no allowed pool fits. |
| Activity | `src/temporal/activities.py` | Validates placement/deployment data, reserves/releases GPU capacity, and runs Helm. |
| Tenant chart | `helm/tenant-workload` | Creates the workload submitter ConfigMap and Job. |
| Rancher Desktop Kubernetes stack | `deploy/kubernetes/rancher-desktop` | Runs the production-like local environment in Kubernetes with in-cluster RBAC. |
| Local deployment helper | `scripts/local-kubernetes-deploy.sh` | Builds images and starts, stops, checks, or tails the local Kubernetes stack. |

## Kubernetes Manifest Layout

The Rancher Desktop stack is split by service/domain. Each folder owns the
Kubernetes resources for that component:

| Folder | Resource group |
| --- | --- |
| `deploy/kubernetes/rancher-desktop/shared` | Platform namespace and shared runtime ConfigMap. |
| `deploy/kubernetes/rancher-desktop/api` | API ServiceAccount, Deployment, Service, and Ingress. |
| `deploy/kubernetes/rancher-desktop/worker` | Worker ServiceAccount, RBAC, Deployment, and Service. |
| `deploy/kubernetes/rancher-desktop/kafka` | Kafka Deployment and Service. |
| `deploy/kubernetes/rancher-desktop/temporal` | Temporal Deployment, Service, and UI Ingress. |
| `deploy/kubernetes/rancher-desktop/monitoring/prometheus` | Prometheus ServiceAccount, RBAC, ConfigMap, Deployment, Service, and Ingress. |
| `deploy/kubernetes/rancher-desktop/monitoring/grafana` | Grafana Deployment, Service, and Ingress. |
| `deploy/kubernetes/rancher-desktop/database` | Optional Postgres secret example. Not applied by the local deploy script. |

`scripts/local-kubernetes-deploy.sh` owns the apply order so dependencies such
as namespace, shared config, service accounts, RBAC, services, deployments, and
ingresses are created in a stable sequence.

## Request Flow

1. A client sends a tenant allocation request to the FastAPI service.
2. The API validates `customer_id`, `tier`, and optional placement preferences.
3. The API creates an `allocation_id`, returns it to the caller, publishes a
   normalized event to Kafka, and waits for delivery
   confirmation before returning `202`.
4. The worker dispatcher ensures the primary and DLQ Kafka topics exist before subscribing.
5. The worker dispatcher consumes the Kafka event and validates the message shape.
6. The worker starts a Temporal workflow with ID `gpu-alloc-{allocation_id}` and
   waits for the workflow result before committing the Kafka offset.
7. The workflow runs the GPU placement activity.
8. The placement activity maps the tenant tier to a GPU count, filters configured
   pools by GPU type, allowed regions, latency budget, and health, then reserves
   capacity if available.
9. If no acceptable pool has capacity, the workflow completes as
   `PENDING_CAPACITY` and does not run Helm.
10. If capacity is reserved, the workflow runs the Helm deployment activity with
    the selected region, cluster, pool, GPU type, reservation ID, and latency.
11. The activity runs `helm upgrade --install`, optionally creating the namespace.
12. The Helm chart creates a tenant-specific Kubernetes Job and ConfigMap.
13. If Helm fails after reservation, the workflow releases the GPU reservation.
14. Invalid events and workflow-start failures are published to `KAFKA_DLQ_TOPIC`.

## Placement And Capacity Model

The placement scheduler supports two reservation stores:

| Backend | Use case |
| --- | --- |
| `memory` | Local development and unit tests. Reservations live in the worker process. |
| `postgres` | Durable reservations across worker restarts and multiple worker processes. |

The checked-in local stack defaults to `memory`. Set
`PLACEMENT_STORE_BACKEND=postgres` and `GPU_PLACEMENT_DATABASE_URL` to use
PostgreSQL, including Neon.

The placement rules are the same for both stores:

1. Normalize the requested tier into a GPU count.
2. Use the requested `gpu_type` or the profile's default `gpu_type`.
3. Use `preferred_region` or `DEFAULT_CUSTOMER_REGION`.
4. If `allowed_regions` is omitted, only the preferred region is allowed.
5. If `allowed_regions` is provided, the preferred region is included first.
6. Filter pools by health, GPU type, allowed region, and `max_latency_ms`.
7. Rank candidates by preferred region, lower latency, best-fit remaining
   capacity, higher priority, and stable pool ID.
8. Reserve atomically or return `PENDING_CAPACITY`.

The orchestrator does not silently move a tenant to a distant region. A fallback
region is eligible only when the request or default policy allows that region and
the latency budget is satisfied.

When using Postgres, the store creates these tables automatically if they do not
exist:

- `gpu_pool_locks`: one row per pool, locked with `SELECT ... FOR UPDATE`
  during reservation.
- `gpu_reservations`: durable reservation rows with `RESERVED`, `ACTIVE`, and
  `RELEASED` status.

The pool lock prevents two worker transactions from reserving the same free GPUs
at the same time. The pool inventory itself still comes from
`GPU_POOLS_CONFIG_PATH` or `GPU_POOLS_CONFIG`; Postgres stores reservation state,
not the pool catalog.

## Versioned Catalog Config

Catalog-style config is stored as JSON under `config/`:

| File | Purpose |
| --- | --- |
| `config/gpu-profiles.json` | Product tier catalog: GPU count, default GPU type, and default latency budget. |
| `config/gpu-pools.local.json` | Local Rancher Desktop pool seed used by the placement scheduler. |

These are versioned because they describe product and placement policy. Runtime
endpoints, credentials, TLS options, and deployment environment flags stay in
environment variables or Kubernetes Secrets.

## API Contract

### Allocate Tenant GPU Workload

```http
POST /api/v1/tenant/allocate
Content-Type: application/json
```

Request:

```json
{
  "customer_id": "team-a",
  "tier": "premium",
  "preferred_region": "us-phoenix-1",
  "allowed_regions": ["us-phoenix-1", "us-ashburn-1"],
  "max_latency_ms": 80,
  "gpu_type": "mock"
}
```

Only `customer_id` and `tier` are required. If placement fields are omitted, the
worker uses `DEFAULT_CUSTOMER_REGION` plus the tier defaults from
`config/gpu-profiles.json`; fallback to another region is not allowed unless
`allowed_regions` is provided.

The API creates `allocation_id`, returns it in the `202` response, and includes
the same value in the Kafka event. Clients do not send it; the worker uses it
for Temporal workflow identity and reservation identity.

Accepted tiers:

| Tier | GPU count |
| --- | ---: |
| `premium` | 2 |
| `standard` | 1 |

Optional placement fields:

| Field | Description |
| --- | --- |
| `preferred_region` | Customer's preferred GPU region. Defaults to `DEFAULT_CUSTOMER_REGION`. |
| `allowed_regions` | Explicit region fallback list. If omitted, only the preferred region is eligible. |
| `max_latency_ms` | Maximum acceptable latency from preferred customer region to GPU region. Defaults from the selected tier profile. |
| `gpu_type` | Requested GPU type. Defaults from the selected tier profile; local development uses `mock`. |

`customer_id` must be Kubernetes-safe:

- 1 to 38 characters.
- Lowercase letters, numbers, and internal hyphens.
- Must start and end with a lowercase letter or number.

Success response:

```json
{
  "allocation_id": "alloc-0b7f5f0d83f94bb7bf52c7bcf34f30e8",
  "status": "Accepted",
  "message": "GPU Allocation event sent to queue."
}
```

### Poll Allocation Status

```http
GET /api/v1/tenant/allocations/{allocation_id}
```

The API looks up the Temporal workflow `gpu-alloc-{allocation_id}`. If the
Kafka event has been accepted but the worker has not started the workflow yet,
the endpoint returns `queued`.

Running response:

```json
{
  "allocation_id": "alloc-0b7f5f0d83f94bb7bf52c7bcf34f30e8",
  "workflow_id": "gpu-alloc-alloc-0b7f5f0d83f94bb7bf52c7bcf34f30e8",
  "status": "running",
  "workflow_status": "running",
  "run_id": "run-id",
  "task_queue": "gpu-allocation-tasks",
  "started_at": "2026-06-29T12:00:00+00:00",
  "closed_at": null
}
```

Completed workflows include the workflow result. Successful allocations return
`status: "active"` and capacity failures return `status: "pending_capacity"`.

Common error responses:

| Status | Reason |
| --- | --- |
| `400` | Unsupported tier. |
| `422` | Invalid request shape or invalid `customer_id`. |
| `500` | Kafka publish failure. |
| `503` | Temporal is unavailable during allocation status lookup. |

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `APP_ENV` | `local` | Runtime environment. Set to `production` or `prod` to enable production config validation. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers used by the API producer and worker consumer. |
| `KAFKA_TOPIC` | `gpu-allocations` | Kafka topic for allocation events. |
| `KAFKA_DLQ_TOPIC` | `gpu-allocations-dlq` | Dead-letter topic for invalid events and workflow-start failures. |
| `KAFKA_TOPIC_PARTITIONS` | `1` | Partition count used when the worker creates missing Kafka topics. |
| `KAFKA_TOPIC_REPLICATION_FACTOR` | `1` | Replication factor used when the worker creates missing Kafka topics. |
| `KAFKA_STARTUP_RETRY_SECONDS` | `5` | Backoff between worker Kafka initialization retries. |
| `KAFKA_PRODUCER_FLUSH_TIMEOUT_SECONDS` | `10` | API timeout while waiting for Kafka delivery confirmation before returning `202`. |
| `TEMPORAL_HOST` | `localhost:7233` | Temporal frontend address used by the worker. |
| `TEMPORAL_CONNECT_TIMEOUT_SECONDS` | `10` | Timeout for each worker Temporal connection attempt. |
| `TEMPORAL_STARTUP_RETRY_SECONDS` | `5` | Backoff between worker Temporal connection retries. |
| `HELM_DRY_RUN` | `false` | Render Helm manifests with `helm template` instead of installing them. The local Kubernetes stack sets this to `false`. |
| `HELM_NAMESPACE` | `default` | Fallback Kubernetes namespace for tenant Helm releases. |
| `HELM_NAMESPACE_TEMPLATE` | empty | Optional namespace template. The local stack uses `tenant-{customer_id}`. |
| `HELM_CREATE_NAMESPACE` | `false` | Adds `--create-namespace` to Helm install/upgrade when true. |
| `HELM_MOCK_GPU` | `true` | Requests CPU instead of `nvidia.com/gpu` in the tenant chart when true. |
| `HELM_KUBE_APISERVER` | empty | Optional explicit Kubernetes API server passed to Helm. The Rancher Desktop stack sets this to its reachable in-pod API endpoint. |
| `HELM_KUBE_TLS_SERVER_NAME` | empty | Optional Kubernetes API TLS server name passed to Helm. |
| `HELM_KUBE_CA_FILE` | empty | Optional Kubernetes API CA file passed to Helm. |
| `HELM_KUBE_TOKEN_FILE` | empty | Optional service-account token file read by the worker and passed to Helm. |
| `HELM_KUBE_INSECURE_SKIP_TLS_VERIFY` | `false` | Optional Helm TLS verification bypass. Keep this disabled outside short-lived local troubleshooting. |
| `DEFAULT_CUSTOMER_REGION` | `us-phoenix-1` | Region used when an allocation request omits `preferred_region`. |
| `GPU_PROFILES_CONFIG_PATH` | `config/gpu-profiles.json` | JSON file path for tier/profile catalog config. |
| `GPU_PROFILES_CONFIG` | empty | Inline JSON override for the tier/profile catalog. Prefer file paths or mounted ConfigMaps for normal use. |
| `PLACEMENT_STORE_BACKEND` | `memory` | Reservation backend. Use `postgres` for durable reservations. |
| `GPU_PLACEMENT_DATABASE_URL` | empty | PostgreSQL connection URL used when `PLACEMENT_STORE_BACKEND=postgres`. Falls back to `DATABASE_URL` if set. |
| `GPU_POOLS_CONFIG_PATH` | `config/gpu-pools.local.json` | JSON/YAML file path for placement pool seed config. |
| `GPU_POOLS_CONFIG` | empty | Inline JSON/YAML override for pool seed config. Prefer file paths or mounted ConfigMaps for normal use. |
| `WORKER_HEALTH_HOST` | `0.0.0.0` | Worker health server bind address. |
| `WORKER_HEALTH_PORT` | `8081` | Worker health and metrics server port. |

When `APP_ENV=production`, startup validation rejects local-only settings. The
API must not point Kafka at localhost. The worker must use Postgres
reservations, non-mock profile GPU types, `HELM_MOCK_GPU=false`, verified Helm
TLS, and non-local Kafka/Temporal endpoints.

Values that are good candidates for versioned JSON config:

- GPU tier/profile catalog.
- Local or seeded GPU pool inventory.
- Region latency matrices if they grow beyond per-pool latency maps.
- Tenant policy templates, such as allowed fallback region groups.
- Helm value presets that are product policy, not secrets.

Values that should remain environment variables or Kubernetes Secrets:

- Kafka, Temporal, database, and Kubernetes API endpoints.
- Database URLs, tokens, certificates, and service-account paths.
- `APP_ENV`, dry-run flags, TLS verification flags, and operational timeouts.
- Dynamic reservation state, which belongs in Postgres.

## Local Prerequisites

- Python 3.11 or newer.
- Rancher Desktop with Kubernetes enabled.
- Docker CLI using the Rancher Desktop Docker context.
- `kubectl` context set to `rancher-desktop`.
- Docker context set to `rancher-desktop`.
- Helm, if running Helm commands directly from the host.

For local development without a GPU cluster, the Helm chart defaults to
`mockGpu: true`, which requests CPU instead of `nvidia.com/gpu` and validates
the submitter path without requiring a Slurm cluster.

## Run Locally With Rancher Desktop Kubernetes

This is the only supported local runtime path for the project. API, worker,
Kafka, Temporal, Prometheus, and Grafana run as Kubernetes workloads in the
`gpu-tenant-orchestrator` namespace. Tenant allocation requests create real
Helm releases in `tenant-*` namespaces with `mockGpu=true`, so no physical GPU
or Slurm cluster is required.

### 1. Start Rancher Desktop

In Rancher Desktop settings:

- Enable Kubernetes.
- Enable Traefik.
- Use the `rancher-desktop` Docker context.
- Use the `rancher-desktop` Kubernetes context.

Verify the contexts:

```bash
docker context show
kubectl config current-context
```

Both commands should print `rancher-desktop`.

### 2. Start The Full Stack

Run one command from the repository root:

```bash
./scripts/local-kubernetes-deploy.sh up
```

The command builds fresh local API and worker images, applies the Kubernetes
manifests, rolls the deployments, and waits for the stack to become ready.
Set `IMAGE_TAG=dev` if you want deterministic image tags while iterating:

```bash
IMAGE_TAG=dev ./scripts/local-kubernetes-deploy.sh up
```

### 3. Open The Local URLs

Rancher Desktop exposes the Traefik Ingress through localhost. After `up`
finishes, these URLs should work directly:

| Service | URL |
| --- | --- |
| API | `http://gpu-tenant.localhost` |
| Temporal UI | `http://temporal.gpu-tenant.localhost` |
| Grafana | `http://grafana.gpu-tenant.localhost` with `admin/admin` |
| Prometheus | `http://prometheus.gpu-tenant.localhost` |

Health and metrics:

| Component | URL |
| --- | --- |
| API health | `http://gpu-tenant.localhost/healthz` |
| API readiness | `http://gpu-tenant.localhost/readyz` |
| API metrics | `http://gpu-tenant.localhost/metrics` |
| Worker health | `http://worker:8081/healthz` inside the cluster |
| Worker readiness | `http://worker:8081/readyz` inside the cluster |
| Worker metrics | `http://worker:8081/metrics` inside the cluster |

### 4. Send A Tenant Allocation Request

Submit a premium allocation:

```bash
curl -i -X POST http://gpu-tenant.localhost/api/v1/tenant/allocate \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"team-a","tier":"premium"}'
```

Submit a standard allocation:

```bash
curl -i -X POST http://gpu-tenant.localhost/api/v1/tenant/allocate \
  -H "Content-Type: application/json" \
  -d '{"customer_id":"team-b","tier":"standard"}'
```

Expected success response:

```json
{
  "allocation_id": "alloc-0b7f5f0d83f94bb7bf52c7bcf34f30e8",
  "status": "Accepted",
  "message": "GPU Allocation event sent to queue."
}
```

### 5. Verify The Workflow Result

Poll the allocation returned by the API:

```bash
curl -s http://gpu-tenant.localhost/api/v1/tenant/allocations/alloc-0b7f5f0d83f94bb7bf52c7bcf34f30e8
```

Typical completed response:

```json
{
  "allocation_id": "alloc-0b7f5f0d83f94bb7bf52c7bcf34f30e8",
  "workflow_id": "gpu-alloc-alloc-0b7f5f0d83f94bb7bf52c7bcf34f30e8",
  "status": "active",
  "workflow_status": "completed",
  "result": {
    "status": "ACTIVE"
  }
}
```

Check platform status:

```bash
./scripts/local-kubernetes-deploy.sh status
```

Check the tenant namespace and mock submitter job:

```bash
kubectl get namespace tenant-team-a
kubectl -n tenant-team-a get configmaps,jobs,pods
```

Check Kafka topics:

```bash
kubectl -n gpu-tenant-orchestrator exec deployment/kafka -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 127.0.0.1:29092 \
  --list
```

Read the DLQ topic:

```bash
kubectl -n gpu-tenant-orchestrator exec deployment/kafka -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server 127.0.0.1:29092 \
  --topic gpu-allocations-dlq \
  --from-beginning \
  --timeout-ms 5000
```

### Optional: Use Postgres For Reservations

The local stack runs with `PLACEMENT_STORE_BACKEND=memory` by default. To test
durable reservations with Neon or another PostgreSQL database, create the
optional worker secret after the platform namespace exists:

```bash
kubectl -n gpu-tenant-orchestrator create secret generic gpu-tenant-postgres \
  --from-literal=PLACEMENT_STORE_BACKEND=postgres \
  --from-literal=GPU_PLACEMENT_DATABASE_URL='postgresql://USER:PASSWORD@HOST/DB?sslmode=require'

kubectl -n gpu-tenant-orchestrator rollout restart deployment/worker
```

The worker reads this secret if present. The API does not need database access.

### 6. Follow Logs

```bash
./scripts/local-kubernetes-deploy.sh logs-api
./scripts/local-kubernetes-deploy.sh logs-worker
./scripts/local-kubernetes-deploy.sh logs-kafka
```

### 7. Stop And Clean Up

```bash
./scripts/local-kubernetes-deploy.sh down
```

The `down` command deletes the platform namespace and local `tenant-*`
namespaces created by Helm during allocation tests.

### Troubleshooting Local URLs

`http://gpu-tenant.localhost` and the dashboard URLs depend on Rancher
Desktop's Traefik Ingress being exposed on host port `80`.

If the browser or curl returns plain text `404 page not found`, the request
reached an HTTP router but did not reach this FastAPI application. FastAPI 404
responses are JSON and look like `{"detail":"Not Found"}`.

Check what owns host port `80`:

```bash
lsof -nP -iTCP:80 -sTCP:LISTEN
```

On Rancher Desktop for macOS, the listener may show up as `ssh`; that can be
Rancher Desktop's own port-forwarding process. Do not kill it unless you are
intentionally resetting Rancher Desktop, because it can also carry the
Kubernetes API tunnel used by `kubectl`.

If the URLs do not route after `up`:

```bash
kubectl -n gpu-tenant-orchestrator get pods,svc,ingress
kubectl -n gpu-tenant-orchestrator describe ingress api temporal-ui grafana prometheus
curl -i -H "Host: gpu-tenant.localhost" http://127.0.0.1/readyz
```

If `kubectl` reports `127.0.0.1:6443 refused`, restart Rancher Desktop from the
UI, wait for Kubernetes to become ready, then run:

```bash
./scripts/local-kubernetes-deploy.sh up
```

As a temporary fallback only, bypass Ingress and call the API service directly:

```bash
kubectl -n gpu-tenant-orchestrator port-forward svc/api 18000:8000
curl -i http://127.0.0.1:18000/readyz
```

## Uninstall Local Environment

Stop all local Kubernetes services:

```bash
./scripts/local-kubernetes-deploy.sh down
```

The `down` command deletes the platform namespace and local `tenant-*`
namespaces created by Helm during allocation tests.

Remove the local Python environment:

```bash
rm -rf venv
```

Optional cleanup if you built local API or worker images manually:

```bash
docker image rm gpu-tenant-orchestrator-api gpu-tenant-orchestrator-worker
```

## Tests

Install dev dependencies:

```bash
venv/bin/python -m pip install -r requirements-dev.txt
```

Run unit tests:

```bash
venv/bin/python -m pytest
```

The tests mock Kafka publishing and Helm execution, so they do not require
Kafka, Temporal, Kubernetes, or Helm to be running.

Current coverage focus:

- FastAPI request validation and Kafka publish behavior.
- API metrics counters.
- GPU profile catalog loading and validation.
- Tier-to-GPU allocation rules.
- Region-aware scheduler reservation, fallback, latency filtering, release, and
  activation behavior.
- Postgres reservation-store SQL flow, pool locking, capacity checks, and status
  updates using a fake connection.
- Helm command construction.
- Tenant namespace resolution.
- Worker metrics and DLQ routing.
- Invalid deployment input handling.
- Helm failure propagation.

## Docker Images

Runtime dependencies are split by process:

| File | Used by | Notes |
| --- | --- | --- |
| `requirements-common.txt` | API and worker | Shared Kafka client dependency. |
| `requirements-api.txt` | API image | FastAPI, Uvicorn, API validation, and Temporal status polling dependencies. |
| `requirements-worker.txt` | Worker image | Temporal, Kubernetes, Helm integration helpers, YAML parsing, and `psycopg`. |
| `requirements.txt` | Local aggregate | Installs both API and worker dependencies for development. |

The worker image installs Debian `libpq5` and uses `psycopg==3.2.3` rather than
`psycopg[binary]`, so the production runtime depends on the OS libpq package
instead of a bundled binary wheel.

Build the API image:

```bash
docker build -f Dockerfile.api -t gpu-tenant-orchestrator-api .
```

Build the worker image:

```bash
docker build -f Dockerfile.worker -t gpu-tenant-orchestrator-worker .
```

The worker image includes Helm. The API image does not. Both application images
run as a non-root user, and base images are pinned by digest in both
Dockerfiles.

## Helm Deployment Design

The chart at `helm/tenant-workload` creates:

- A tenant-specific ConfigMap containing `submit_job.sh`.
- A tenant-specific Kubernetes Job that runs the Slurm client container.

Important values:

| Value | Default | Description |
| --- | --- | --- |
| `customerId` | `default-user` | Tenant identifier used in resource names and labels. |
| `tier` | `standard` | Tenant tier label. |
| `gpuCount` | `1` | Requested GPU count. |
| `image` | `alpine:3.20` | Local mock workload image. Override with the approved Slurm client image for production. |
| `imagePullPolicy` | `IfNotPresent` | Pull policy for the tenant workload image. |
| `mockGpu` | `true` | Uses CPU requests and a no-op submit path when true; uses `nvidia.com/gpu` and submits to Slurm when false. |
| `mockCpu` | `250m` | CPU request and limit used for local mock GPU mode. |
| `gpuType` | `mock` | GPU type selected by placement. |
| `assignedRegion` | `unassigned` | Region selected by placement. |
| `assignedCluster` | `unassigned` | Cluster selected by placement. |
| `gpuPoolId` | `unassigned` | GPU pool selected by placement. |
| `reservationId` | `unassigned` | Reservation ID created by placement. |
| `latencyMs` | `0` | Latency estimate for the selected placement. |
| `nodeSelector` | `{}` | Optional strict node labels for tenant workload placement. |
| `affinity` | `{}` | Optional Kubernetes affinity rules for tenant workload placement. |
| `tolerations` | `[]` | Optional tolerations for tainted GPU nodes. |

Production GPU node scheduling can be configured through Helm values without
changing application code. Example:

```yaml
mockGpu: false
nodeSelector:
  accelerator: nvidia
affinity:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
        - matchExpressions:
            - key: gpu.oracle.com/type
              operator: In
              values: ["nvidia-a100"]
            - key: topology.kubernetes.io/region
              operator: In
              values: ["us-phoenix-1"]
tolerations:
  - key: nvidia.com/gpu
    operator: Exists
    effect: NoSchedule
```

The local API and worker manifests use preferred pod anti-affinity only. These
soft rules spread replicas on multi-node clusters but still allow scheduling on
Rancher Desktop's single-node Kubernetes cluster.

## Observability

The local stack includes Prometheus and Grafana configuration under
`deploy/monitoring`.

Current state:

- Prometheus scrapes itself and discovers annotated API and worker pods in the
  `gpu-tenant-orchestrator` namespace.
- API metrics include request, publish success, and publish failure counters.
- Worker metrics include dispatcher readiness, Temporal worker readiness, Kafka
  processed message count, invalid message count, DLQ count, workflow-start
  failure count, per-customer allocation counts, per-customer GPU count,
  allocation duration, and completion status including `pending_capacity`.
- Grafana provisions the `GPU Tenant Orchestrator Metrics` dashboard during
  `./scripts/local-kubernetes-deploy.sh up`. The local deploy script restarts
  Prometheus after scrape ConfigMap changes and restarts Grafana after dashboard
  ConfigMap changes.
- Dashboard PromQL aggregates across pod instances, which avoids stale-looking
  API counters when multiple API replicas are running.
- API and worker logs are plain stdout/stderr logs.

Useful Prometheus queries:

```promql
sum(gpu_tenant_api_allocation_requests_total)
sum(gpu_tenant_api_allocation_publish_success_total)
sum(gpu_tenant_worker_messages_consumed_total)
gpu_tenant_worker_customer_allocations_total
gpu_tenant_worker_customer_allocation_gpu_count
gpu_tenant_worker_customer_allocation_duration_seconds
gpu_tenant_worker_customer_allocations_completed_total
sum(gpu_tenant_worker_dlq_messages_total)
```

The Grafana dashboard includes:

- API accepted allocations.
- Kafka messages processed by the worker.
- DLQ event count.
- Last successful allocation duration.
- Total customers.
- Total GPUs allocated.
- Average allocation duration.
- Allocations by tier and completion status.
- Customer allocation GPU count table.
- Customer allocation duration table by customer, tier, and completion status.

## Production Readiness

The current repository should be treated as a prototype. Before production use,
complete the following checklist.

| Area | Current state | Required before production |
| --- | --- | --- |
| Authentication | No API auth. | Add tenant authentication and service-to-service authorization. |
| Authorization | No tenant-level access control. | Enforce tenant ownership and allowed tiers. |
| Kafka security | Local PLAINTEXT config. | Enable TLS/SASL, authenticated producers and consumers, ACLs, and topic retention policy. |
| Temporal security | Local dev server defaults. | Use managed or hardened Temporal with TLS, namespaces, retention, and worker identity. |
| Kubernetes access | Rancher Desktop path uses a worker service account and ClusterRole to create tenant namespaces and Helm resources. | Tighten RBAC with namespace admission controls, tenant namespace ownership, and least-privilege production roles. |
| Idempotency | Workflow ID is tenant-based. | Define duplicate request behavior, retries, and replay-safe semantics. |
| Failure handling | Invalid Kafka messages and workflow-start failures are routed to a DLQ; Helm failures raise to Temporal. | Add retry policy tuning, DLQ replay tooling, operator alerts, and failure reconciliation. |
| Observability | Local Prometheus scrapes API and worker metrics; Grafana is present. | Add structured logs, traces, production dashboards, SLOs, and alerting. |
| Health checks | API and worker expose readiness/liveness endpoints. | Add deeper dependency-aware readiness checks for Kafka and Temporal. |
| Deployment | Dockerfiles and a Rancher Desktop Kubernetes manifest exist. | Split production overlays from local manifests, publish images to a registry, version releases, and define rollout/rollback strategy. |
| Secrets | No secret management. | Use a secrets manager or Kubernetes Secrets with rotation policy. |
| Supply chain | Dependencies and Docker app base images are pinned; app containers run as non-root. | Add vulnerability scanning, SBOM generation, signed images, and base image patch policy. |
| Testing | Unit tests cover core behavior. | Add integration tests with Kafka/Temporal, Helm template tests, and end-to-end deployment tests. |
| Data governance | Minimal event payload. | Define retention, audit fields, PII policy, and request tracing IDs. |

## Public Repository Readiness

This repository includes a public baseline: `LICENSE`, `SECURITY.md`,
`CONTRIBUTING.md`, `.dockerignore`, and a GitHub Actions workflow at
`.github/workflows/ci.yml`.

Before making this repository public, complete this checklist.

| Area | Required action |
| --- | --- |
| Secrets | Run a full secret scan across history before publishing. |
| Internal references | Remove private hostnames, cluster names, credentials, tickets, and organization-only context. |
| License | Confirm the MIT license and copyright holder are correct. |
| Security policy | Replace the placeholder reporting channel in `SECURITY.md` with the real private contact path. |
| Contributions | Review `CONTRIBUTING.md` and adjust workflow expectations for the target repository. |
| Code of conduct | Add a `CODE_OF_CONDUCT.md` if accepting external contributions. |
| CI | GitHub Actions runs tests and static validation. Add dependency checks, linting, and Docker builds when publishing images. |
| Dependency posture | Add dependency update automation and vulnerability alerts. |
| Documentation | Keep local run, teardown, test, API, metrics, and architecture sections current. |
| Sample config | Provide safe example environment files only; do not commit real credentials. |
| Generated files | Ensure caches, local virtual environments, logs, and build artifacts are ignored. |
| Images | Avoid publishing unscanned images; document supported tags and architectures. |

Suggested pre-publication commands:

```bash
git status --short
venv/bin/python -m pytest -q
venv/bin/python -m py_compile \
  src/api/main.py \
  src/shared/config.py \
  src/temporal/activities.py \
  src/temporal/worker.py \
  src/temporal/workflows.py
bash -n scripts/local-kubernetes-deploy.sh
jq empty deploy/monitoring/grafana/dashboards/gpu-tenant-metrics.json
./scripts/local-kubernetes-deploy.sh validate
docker build -f Dockerfile.api -t gpu-tenant-orchestrator-api .
docker build -f Dockerfile.worker -t gpu-tenant-orchestrator-worker .
```

Use a dedicated secret scanning tool, such as `gitleaks` or `detect-secrets`,
before publishing the repository or its history.
