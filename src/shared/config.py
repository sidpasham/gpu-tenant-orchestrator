import os


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


TEMPORAL_HOST = resolve_temporal_host()
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "gpu-allocations")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "gpu-allocations-dlq")
KAFKA_TOPIC_PARTITIONS = int(os.getenv("KAFKA_TOPIC_PARTITIONS", "1"))
KAFKA_TOPIC_REPLICATION_FACTOR = int(os.getenv("KAFKA_TOPIC_REPLICATION_FACTOR", "1"))
KAFKA_STARTUP_RETRY_SECONDS = float(os.getenv("KAFKA_STARTUP_RETRY_SECONDS", "5"))
KAFKA_PRODUCER_FLUSH_TIMEOUT_SECONDS = float(
    os.getenv("KAFKA_PRODUCER_FLUSH_TIMEOUT_SECONDS", "10")
)
TEMPORAL_CONNECT_TIMEOUT_SECONDS = float(os.getenv("TEMPORAL_CONNECT_TIMEOUT_SECONDS", "10"))
TEMPORAL_STARTUP_RETRY_SECONDS = float(os.getenv("TEMPORAL_STARTUP_RETRY_SECONDS", "5"))
WORKFLOW_RESULT_TIMEOUT_SECONDS = float(os.getenv("WORKFLOW_RESULT_TIMEOUT_SECONDS", "360"))
HELM_DRY_RUN = os.getenv("HELM_DRY_RUN", "false").lower() in {"1", "true", "yes", "on"}
HELM_NAMESPACE = os.getenv("HELM_NAMESPACE", "default")
HELM_NAMESPACE_TEMPLATE = os.getenv("HELM_NAMESPACE_TEMPLATE", "")
HELM_CREATE_NAMESPACE = os.getenv("HELM_CREATE_NAMESPACE", "false").lower() in {"1", "true", "yes", "on"}
HELM_MOCK_GPU = os.getenv("HELM_MOCK_GPU", "true").lower() in {"1", "true", "yes", "on"}
HELM_KUBE_APISERVER = os.getenv("HELM_KUBE_APISERVER", "")
HELM_KUBE_TLS_SERVER_NAME = os.getenv("HELM_KUBE_TLS_SERVER_NAME", "")
HELM_KUBE_CA_FILE = os.getenv("HELM_KUBE_CA_FILE", "")
HELM_KUBE_TOKEN_FILE = os.getenv("HELM_KUBE_TOKEN_FILE", "")
HELM_KUBE_INSECURE_SKIP_TLS_VERIFY = os.getenv(
    "HELM_KUBE_INSECURE_SKIP_TLS_VERIFY", "false"
).lower() in {"1", "true", "yes", "on"}
WORKER_HEALTH_HOST = os.getenv("WORKER_HEALTH_HOST", "0.0.0.0")
WORKER_HEALTH_PORT = int(os.getenv("WORKER_HEALTH_PORT", "8081"))
