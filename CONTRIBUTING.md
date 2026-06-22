# Contributing

## Development Setup

Use Python 3.11 or newer.

```bash
python3 -m venv venv
venv/bin/python -m pip install --upgrade pip
venv/bin/python -m pip install -r requirements-dev.txt
```

## Local Runtime

The supported local runtime is Rancher Desktop Kubernetes:

```bash
./scripts/local-kubernetes-deploy.sh up
```

Clean up local Kubernetes resources with:

```bash
./scripts/local-kubernetes-deploy.sh down
```

## Validation

Run these checks before opening a pull request:

```bash
venv/bin/python -m pytest -q
venv/bin/python -m py_compile \
  src/api/main.py \
  src/shared/config.py \
  src/temporal/activities.py \
  src/temporal/worker.py \
  src/temporal/workflows.py
bash -n scripts/local-kubernetes-deploy.sh
jq empty deploy/monitoring/grafana/dashboards/gpu-tenant-metrics.json
kubectl apply --dry-run=client -f deploy/kubernetes/rancher-desktop
```

## Pull Request Expectations

- Keep changes focused and explain the behavior being changed.
- Add or update tests for API, worker, Helm, and script behavior when relevant.
- Update `README.md` when commands, architecture, metrics, or local setup
  change.
- Do not commit generated caches, local virtual environments, logs, or secrets.
