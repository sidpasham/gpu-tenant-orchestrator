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
    DEFAULT_CUSTOMER_REGION,
    GPU_PLACEMENT_DATABASE_URL,
    GPU_POOLS_CONFIG,
    GPU_PROFILES_CONFIG,
    PLACEMENT_STORE_BACKEND,
)
from src.placement.scheduler import (
    ACTIVE_STATUS,
    GpuPlacementScheduler,
    PENDING_CAPACITY_STATUS,
    PlacementRequest,
    RESERVED_STATUS,
    build_reservation_store,
    load_gpu_pools,
)
from src.shared.gpu_profiles import GpuProfile, load_gpu_profiles

GPU_PROFILE_CATALOG = load_gpu_profiles(GPU_PROFILES_CONFIG)
CUSTOMER_ID_PATTERN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
KUBERNETES_NAME_PATTERN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
PLACEMENT_NAME_PATTERN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
ALLOCATION_ID_PATTERN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
PLACEMENT_SCHEDULER: GpuPlacementScheduler | None = None


def get_allocation_rules(tier: str) -> int:
    profile = GPU_PROFILE_CATALOG.get(tier)
    if profile is None:
        return 0
    return profile.gpu_count


def get_placement_scheduler() -> GpuPlacementScheduler:
    global PLACEMENT_SCHEDULER

    if PLACEMENT_SCHEDULER is None:
        PLACEMENT_SCHEDULER = GpuPlacementScheduler(
            load_gpu_pools(GPU_POOLS_CONFIG),
            build_reservation_store(
                PLACEMENT_STORE_BACKEND,
                GPU_PLACEMENT_DATABASE_URL,
            ),
        )
    return PLACEMENT_SCHEDULER


def normalize_placement_name(value: Any, default: str, field_name: str) -> str:
    candidate = default if value is None or value == "" else value
    if not isinstance(candidate, str):
        raise ValueError(f"{field_name} must be a string")
    normalized_candidate = candidate.lower()
    if not PLACEMENT_NAME_PATTERN.fullmatch(normalized_candidate):
        raise ValueError(
            f"{field_name} must contain lowercase letters, numbers, and hyphens"
        )
    return normalized_candidate


def normalize_allowed_regions(
    raw_allowed_regions: Any,
    preferred_region: str,
) -> tuple[str, ...]:
    if raw_allowed_regions is None or raw_allowed_regions == "":
        return (preferred_region,)
    if not isinstance(raw_allowed_regions, list) or not raw_allowed_regions:
        raise ValueError("allowed_regions must be a non-empty list when provided")

    allowed_regions = []
    for region in raw_allowed_regions:
        normalized_region = normalize_placement_name(
            region,
            preferred_region,
            "allowed_regions",
        )
        if normalized_region not in allowed_regions:
            allowed_regions.append(normalized_region)

    if preferred_region not in allowed_regions:
        allowed_regions.insert(0, preferred_region)
    return tuple(allowed_regions)


def normalize_max_latency_ms(value: Any, default: int) -> int:
    candidate = default if value is None or value == "" else value
    try:
        max_latency_ms = int(candidate)
    except (TypeError, ValueError) as exc:
        raise ValueError("max_latency_ms must be an integer") from exc
    if max_latency_ms <= 0:
        raise ValueError("max_latency_ms must be positive")
    return max_latency_ms


def build_placement_request(
    data: dict[str, Any],
    profile: GpuProfile,
) -> PlacementRequest:
    customer_id = data["customer_id"]
    tier = data["tier"].lower()
    allocation_id = data.get("allocation_id")
    if allocation_id is not None:
        if not isinstance(allocation_id, str) or not ALLOCATION_ID_PATTERN.fullmatch(
            allocation_id
        ):
            raise ValueError("allocation_id must be Kubernetes-safe when provided")

    preferred_region = normalize_placement_name(
        data.get("preferred_region"),
        DEFAULT_CUSTOMER_REGION,
        "preferred_region",
    )
    gpu_type = normalize_placement_name(
        data.get("gpu_type"),
        profile.gpu_type,
        "gpu_type",
    )
    return PlacementRequest(
        customer_id=customer_id,
        tier=tier,
        gpu_count=profile.gpu_count,
        gpu_type=gpu_type,
        preferred_region=preferred_region,
        allowed_regions=normalize_allowed_regions(
            data.get("allowed_regions"),
            preferred_region,
        ),
        max_latency_ms=normalize_max_latency_ms(
            data.get("max_latency_ms"),
            profile.default_max_latency_ms,
        ),
        allocation_id=allocation_id,
    )


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


@activity.defn(name="plan_gpu_allocation")
async def plan_gpu_allocation(data: dict[str, Any]) -> dict[str, Any]:
    customer_id = data.get("customer_id")
    tier = data.get("tier")
    profile = GPU_PROFILE_CATALOG.get(tier)

    if (
        not isinstance(customer_id, str)
        or not customer_id
        or len(customer_id) > 38
        or not CUSTOMER_ID_PATTERN.fullmatch(customer_id)
        or not isinstance(tier, str)
        or profile is None
    ):
        raise ValueError("Allocation input must include customer_id and a valid tier")

    activity.heartbeat("Planning GPU placement and reserving capacity")
    placement_request = build_placement_request(data, profile)
    return get_placement_scheduler().reserve(placement_request)


@activity.defn(name="activate_gpu_reservation")
async def activate_gpu_reservation(data: dict[str, Any]) -> str:
    reservation_id = data.get("reservation_id")
    if not isinstance(reservation_id, str) or not reservation_id:
        raise ValueError("reservation_id is required")

    marked_active = get_placement_scheduler().mark_active(reservation_id)
    if not marked_active:
        raise ValueError(f"Unknown GPU reservation: {reservation_id}")
    return f"Activated GPU reservation: {reservation_id}"


@activity.defn(name="release_gpu_reservation")
async def release_gpu_reservation(data: dict[str, Any]) -> str:
    reservation_id = data.get("reservation_id")
    if not isinstance(reservation_id, str) or not reservation_id:
        return "No GPU reservation to release"

    released = get_placement_scheduler().release(reservation_id)
    if released:
        return f"Released GPU reservation: {reservation_id}"
    return f"GPU reservation was already absent: {reservation_id}"


@activity.defn(name="run_helm_deploy")
async def run_helm_deploy(data: dict[str, Any]) -> str:
    customer_id = data.get("customer_id")
    tier = data.get("tier")
    placement = data.get("placement") if isinstance(data.get("placement"), dict) else {}
    gpu_count = int(placement.get("gpu_count") or get_allocation_rules(tier))

    if (
        not isinstance(customer_id, str)
        or not customer_id
        or len(customer_id) > 38
        or not CUSTOMER_ID_PATTERN.fullmatch(customer_id)
        or not isinstance(tier, str)
        or gpu_count <= 0
    ):
        raise ValueError("Deployment input must include customer_id and a valid tier")
    if placement and placement.get("status") not in {RESERVED_STATUS, ACTIVE_STATUS}:
        raise ValueError("Deployment input must include a reserved GPU placement")

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
    placement_values = {
        "assignedRegion": placement.get("assigned_region"),
        "assignedCluster": placement.get("assigned_cluster"),
        "gpuPoolId": placement.get("gpu_pool_id"),
        "gpuType": placement.get("gpu_type"),
        "reservationId": placement.get("reservation_id"),
        "latencyMs": placement.get("latency_ms"),
    }
    for value_name, value in placement_values.items():
        if value is not None and value != "":
            helm_values.extend(["--set", f"{value_name}={value}"])

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
