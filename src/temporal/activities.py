import re
import subprocess
from typing import Any

from temporalio import activity

from src.shared.config import (
    HELM_CREATE_NAMESPACE,
    HELM_DRY_RUN,
    HELM_KUBE_APISERVER,
    HELM_KUBE_CA_FILE,
    HELM_KUBE_INSECURE_SKIP_TLS_VERIFY,
    HELM_KUBE_TLS_SERVER_NAME,
    HELM_KUBE_TOKEN_FILE,
    HELM_MOCK_GPU,
    HELM_NAMESPACE,
    HELM_NAMESPACE_TEMPLATE,
)

VALID_TIERS = {"premium": 2, "standard": 1}
CUSTOMER_ID_PATTERN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
KUBERNETES_NAME_PATTERN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


def get_allocation_rules(tier: str) -> int:
    if not isinstance(tier, str):
        return 0
    return VALID_TIERS.get(tier.lower(), 0)


def resolve_helm_namespace(customer_id: str) -> str:
    namespace = (
        HELM_NAMESPACE_TEMPLATE.format(customer_id=customer_id)
        if HELM_NAMESPACE_TEMPLATE
        else HELM_NAMESPACE
    )
    if (
        not namespace
        or len(namespace) > 63
        or not KUBERNETES_NAME_PATTERN.fullmatch(namespace)
    ):
        raise ValueError("Resolved Helm namespace must be Kubernetes-safe")
    return namespace


def build_helm_kube_flags() -> list[str]:
    flags = []
    if HELM_KUBE_APISERVER:
        flags.extend(["--kube-apiserver", HELM_KUBE_APISERVER])
    if HELM_KUBE_TLS_SERVER_NAME:
        flags.extend(["--kube-tls-server-name", HELM_KUBE_TLS_SERVER_NAME])
    if HELM_KUBE_CA_FILE:
        flags.extend(["--kube-ca-file", HELM_KUBE_CA_FILE])
    if HELM_KUBE_TOKEN_FILE:
        with open(HELM_KUBE_TOKEN_FILE, encoding="utf-8") as token_file:
            flags.extend(["--kube-token", token_file.read().strip()])
    if HELM_KUBE_INSECURE_SKIP_TLS_VERIFY:
        flags.append("--kube-insecure-skip-tls-verify")
    return flags


@activity.defn(name="run_helm_deploy")
async def run_helm_deploy(data: dict[str, Any]) -> str:
    customer_id = data.get("customer_id")
    tier = data.get("tier")
    gpu_count = get_allocation_rules(tier)

    if (
        not isinstance(customer_id, str)
        or not customer_id
        or len(customer_id) > 38
        or not CUSTOMER_ID_PATTERN.fullmatch(customer_id)
        or not isinstance(tier, str)
        or gpu_count == 0
    ):
        raise ValueError("Deployment input must include customer_id and a valid tier")

    release_name = f"tenant-{customer_id}"
    namespace = resolve_helm_namespace(customer_id)
    chart_path = "./helm/tenant-workload"

    activity.heartbeat("Initiating Helm deployment execution context")

    helm_values = [
        "--set", f"customerId={customer_id}",
        "--set", f"tier={tier}",
        "--set", f"gpuCount={gpu_count}",
        "--set", f"mockGpu={str(HELM_MOCK_GPU).lower()}",
        "--namespace", namespace,
    ]
    if HELM_DRY_RUN:
        helm_command = ["helm", "template", release_name, chart_path, *helm_values]
    else:
        helm_command = [
            "helm",
            "upgrade",
            "--install",
            release_name,
            chart_path,
            *helm_values,
        ]
        if HELM_CREATE_NAMESPACE:
            helm_command.append("--create-namespace")
    helm_command.extend(build_helm_kube_flags())

    result = subprocess.run(helm_command, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(f"Helm execution failed: {result.stderr}")

    if HELM_DRY_RUN:
        return f"Successfully rendered resources via Helm for release: {release_name}"
    return f"Successfully provisioned resources via Helm for release: {release_name}"
