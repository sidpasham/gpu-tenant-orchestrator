import os
from pathlib import Path

TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"0", "false", "no", "off"}
PRODUCTION_ENVIRONMENTS = {"prod", "production"}
APP_ROOT = Path(__file__).resolve().parents[2]


def parse_bool(name: str, default: str) -> bool:
    raw_value = os.getenv(name, default).strip().lower()
    if raw_value in TRUE_VALUES:
        return True
    if raw_value in FALSE_VALUES:
        return False
    raise ValueError(f"{name} must be a boolean value")


def parse_int(name: str, default: str, minimum: int | None = None) -> int:
    raw_value = os.getenv(name, default)
    try:
        parsed_value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if minimum is not None and parsed_value < minimum:
        raise ValueError(f"{name} must be greater than or equal to {minimum}")
    return parsed_value


def parse_float(name: str, default: str, minimum: float | None = None) -> float:
    raw_value = os.getenv(name, default)
    try:
        parsed_value = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if minimum is not None and parsed_value < minimum:
        raise ValueError(f"{name} must be greater than or equal to {minimum:g}")
    return parsed_value


def resolve_app_environment() -> str:
    return os.getenv("APP_ENV", os.getenv("ENVIRONMENT", "local")).strip().lower()


def is_production_environment(environment: str | None = None) -> bool:
    return (environment or APP_ENV).lower() in PRODUCTION_ENVIRONMENTS


def read_config_file(path: str) -> str:
    config_path = Path(path)
    if not config_path.is_absolute():
        config_path = APP_ROOT / config_path
    with config_path.open(encoding="utf-8") as config_file:
        return config_file.read()


def resolve_text_config(
    inline_env_name: str,
    path_env_name: str,
    default_path: str,
) -> tuple[str, str]:
    inline_value = os.getenv(inline_env_name)
    configured_path = os.getenv(path_env_name, default_path)
    if inline_value:
        return inline_value, configured_path
    return read_config_file(configured_path), configured_path


def resolve_kafka_bootstrap_servers() -> str:
    configured_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    service_host = os.getenv("KAFKA_SERVICE_HOST")

    if configured_servers == "kafka:29092" and service_host:
        service_port = (
            os.getenv("KAFKA_SERVICE_PORT_BROKER")
            or os.getenv("KAFKA_SERVICE_PORT")
            or "29092"
        )
        return f"{service_host}:{service_port}"

    return configured_servers


KAFKA_BOOTSTRAP_SERVERS = resolve_kafka_bootstrap_servers()


def resolve_temporal_host() -> str:
    configured_host = os.getenv("TEMPORAL_HOST", "localhost:7233")
    host_name, separator, configured_port = configured_host.partition(":")
    service_host = os.getenv("TEMPORAL_SERVICE_HOST")

    if host_name == "temporal" and service_host:
        service_port = (
            os.getenv("TEMPORAL_SERVICE_PORT_FRONTEND")
            or os.getenv("TEMPORAL_SERVICE_PORT")
            or configured_port
            or "7233"
        )
        return f"{service_host}:{service_port}"

    return configured_host


APP_ENV = resolve_app_environment()
TEMPORAL_HOST = resolve_temporal_host()
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "gpu-allocations")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "gpu-allocations-dlq")
KAFKA_TOPIC_PARTITIONS = parse_int("KAFKA_TOPIC_PARTITIONS", "1", minimum=1)
KAFKA_TOPIC_REPLICATION_FACTOR = parse_int(
    "KAFKA_TOPIC_REPLICATION_FACTOR",
    "1",
    minimum=1,
)
KAFKA_STARTUP_RETRY_SECONDS = parse_float(
    "KAFKA_STARTUP_RETRY_SECONDS",
    "5",
    minimum=0,
)
KAFKA_PRODUCER_FLUSH_TIMEOUT_SECONDS = parse_float(
    "KAFKA_PRODUCER_FLUSH_TIMEOUT_SECONDS",
    "10",
    minimum=0,
)
TEMPORAL_CONNECT_TIMEOUT_SECONDS = parse_float(
    "TEMPORAL_CONNECT_TIMEOUT_SECONDS",
    "10",
    minimum=0,
)
TEMPORAL_STARTUP_RETRY_SECONDS = parse_float(
    "TEMPORAL_STARTUP_RETRY_SECONDS",
    "5",
    minimum=0,
)
WORKFLOW_RESULT_TIMEOUT_SECONDS = parse_float(
    "WORKFLOW_RESULT_TIMEOUT_SECONDS",
    "360",
    minimum=1,
)
HELM_DRY_RUN = parse_bool("HELM_DRY_RUN", "false")
HELM_NAMESPACE = os.getenv("HELM_NAMESPACE", "default")
HELM_NAMESPACE_TEMPLATE = os.getenv("HELM_NAMESPACE_TEMPLATE", "")
HELM_CREATE_NAMESPACE = parse_bool("HELM_CREATE_NAMESPACE", "false")
HELM_MOCK_GPU = parse_bool("HELM_MOCK_GPU", "true")
HELM_KUBE_APISERVER = os.getenv("HELM_KUBE_APISERVER", "")
HELM_KUBE_TLS_SERVER_NAME = os.getenv("HELM_KUBE_TLS_SERVER_NAME", "")
HELM_KUBE_CA_FILE = os.getenv("HELM_KUBE_CA_FILE", "")
HELM_KUBE_TOKEN_FILE = os.getenv("HELM_KUBE_TOKEN_FILE", "")
HELM_KUBE_INSECURE_SKIP_TLS_VERIFY = parse_bool(
    "HELM_KUBE_INSECURE_SKIP_TLS_VERIFY",
    "false",
)
DEFAULT_CUSTOMER_REGION = os.getenv("DEFAULT_CUSTOMER_REGION", "us-phoenix-1")
PLACEMENT_STORE_BACKEND = os.getenv("PLACEMENT_STORE_BACKEND", "memory").lower()
GPU_PLACEMENT_DATABASE_URL = os.getenv(
    "GPU_PLACEMENT_DATABASE_URL",
    os.getenv("DATABASE_URL", ""),
)
GPU_PROFILES_CONFIG, GPU_PROFILES_CONFIG_PATH = resolve_text_config(
    "GPU_PROFILES_CONFIG",
    "GPU_PROFILES_CONFIG_PATH",
    "config/gpu-profiles.json",
)
GPU_POOLS_CONFIG, GPU_POOLS_CONFIG_PATH = resolve_text_config(
    "GPU_POOLS_CONFIG",
    "GPU_POOLS_CONFIG_PATH",
    "config/gpu-pools.local.json",
)
WORKER_HEALTH_HOST = os.getenv("WORKER_HEALTH_HOST", "0.0.0.0")
WORKER_HEALTH_PORT = parse_int("WORKER_HEALTH_PORT", "8081", minimum=1)


def _uses_local_endpoint(endpoint: str) -> bool:
    host = endpoint.partition(":")[0].lower()
    return host in {"localhost", "127.0.0.1", "::1"}


def validate_runtime_config(component: str) -> None:
    errors = []
    normalized_component = component.lower()

    if PLACEMENT_STORE_BACKEND not in {"memory", "postgres"}:
        errors.append("PLACEMENT_STORE_BACKEND must be 'memory' or 'postgres'")

    if not is_production_environment():
        if PLACEMENT_STORE_BACKEND == "postgres" and not GPU_PLACEMENT_DATABASE_URL:
            errors.append(
                "GPU_PLACEMENT_DATABASE_URL or DATABASE_URL is required "
                "when PLACEMENT_STORE_BACKEND=postgres"
            )
    else:
        if _uses_local_endpoint(KAFKA_BOOTSTRAP_SERVERS):
            errors.append(
                "KAFKA_BOOTSTRAP_SERVERS must not point to localhost in production"
            )
        if normalized_component == "worker":
            if _uses_local_endpoint(TEMPORAL_HOST):
                errors.append("TEMPORAL_HOST must not point to localhost in production")
            if HELM_DRY_RUN:
                errors.append("HELM_DRY_RUN must be false in production")
            if HELM_KUBE_INSECURE_SKIP_TLS_VERIFY:
                errors.append(
                    "HELM_KUBE_INSECURE_SKIP_TLS_VERIFY must be false in production"
                )
            if PLACEMENT_STORE_BACKEND != "postgres":
                errors.append(
                    "PLACEMENT_STORE_BACKEND must be postgres for the production worker"
                )
            if not GPU_PLACEMENT_DATABASE_URL:
                errors.append(
                    "GPU_PLACEMENT_DATABASE_URL or DATABASE_URL is required "
                    "for the production worker"
                )
            if HELM_MOCK_GPU:
                errors.append("HELM_MOCK_GPU must be false for the production worker")
            try:
                from src.shared.gpu_profiles import load_gpu_profiles

                if load_gpu_profiles(GPU_PROFILES_CONFIG).has_mock_gpu_type():
                    errors.append(
                        "GPU profile gpu_type must not be mock for the "
                        "production worker"
                    )
            except ValueError as exc:
                errors.append(f"GPU profile config is invalid: {exc}")

    if errors:
        raise ValueError("Invalid runtime configuration: " + "; ".join(errors))
