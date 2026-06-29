import pytest

from src.shared import config


def test_resolve_kafka_bootstrap_servers_uses_kubernetes_service_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    monkeypatch.setenv("KAFKA_SERVICE_HOST", "10.43.138.152")
    monkeypatch.setenv("KAFKA_SERVICE_PORT_BROKER", "29092")

    assert config.resolve_kafka_bootstrap_servers() == "10.43.138.152:29092"


def test_resolve_kafka_bootstrap_servers_preserves_explicit_external_host(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka.example.com:9093")
    monkeypatch.setenv("KAFKA_SERVICE_HOST", "10.43.138.152")
    monkeypatch.setenv("KAFKA_SERVICE_PORT_BROKER", "29092")

    assert config.resolve_kafka_bootstrap_servers() == "kafka.example.com:9093"


def test_resolve_temporal_host_uses_kubernetes_service_env_for_short_name(monkeypatch):
    monkeypatch.setenv("TEMPORAL_HOST", "temporal:7233")
    monkeypatch.setenv("TEMPORAL_SERVICE_HOST", "10.43.171.43")
    monkeypatch.setenv("TEMPORAL_SERVICE_PORT_FRONTEND", "7233")

    assert config.resolve_temporal_host() == "10.43.171.43:7233"


def test_resolve_temporal_host_preserves_explicit_external_host(monkeypatch):
    monkeypatch.setenv("TEMPORAL_HOST", "temporal.example.com:7233")
    monkeypatch.setenv("TEMPORAL_SERVICE_HOST", "10.43.171.43")
    monkeypatch.setenv("TEMPORAL_SERVICE_PORT_FRONTEND", "7233")

    assert config.resolve_temporal_host() == "temporal.example.com:7233"


def test_kafka_producer_flush_timeout_defaults_to_ten_seconds():
    assert config.KAFKA_PRODUCER_FLUSH_TIMEOUT_SECONDS == 10.0


def test_default_placement_config_is_available():
    assert config.DEFAULT_CUSTOMER_REGION == "us-phoenix-1"
    assert config.PLACEMENT_STORE_BACKEND == "memory"
    assert isinstance(config.GPU_PLACEMENT_DATABASE_URL, str)
    assert config.GPU_PROFILES_CONFIG_PATH == "config/gpu-profiles.json"
    assert config.GPU_POOLS_CONFIG_PATH == "config/gpu-pools.local.json"
    assert '"profiles"' in config.GPU_PROFILES_CONFIG
    assert "phoenix-mock-gpu-pool" in config.GPU_POOLS_CONFIG


def test_parse_bool_rejects_invalid_values(monkeypatch):
    monkeypatch.setenv("FEATURE_FLAG", "maybe")

    with pytest.raises(ValueError, match="FEATURE_FLAG must be a boolean"):
        config.parse_bool("FEATURE_FLAG", "false")


def test_parse_int_rejects_values_below_minimum(monkeypatch):
    monkeypatch.setenv("WORKER_HEALTH_PORT", "0")

    with pytest.raises(ValueError, match="greater than or equal to 1"):
        config.parse_int("WORKER_HEALTH_PORT", "8081", minimum=1)


def test_local_worker_config_allows_memory_and_mock_defaults(monkeypatch):
    monkeypatch.setattr(config, "APP_ENV", "local")
    monkeypatch.setattr(config, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setattr(config, "TEMPORAL_HOST", "localhost:7233")
    monkeypatch.setattr(config, "PLACEMENT_STORE_BACKEND", "memory")
    monkeypatch.setattr(config, "GPU_PLACEMENT_DATABASE_URL", "")
    monkeypatch.setattr(config, "HELM_MOCK_GPU", True)

    config.validate_runtime_config("worker")


def test_postgres_backend_requires_database_url(monkeypatch):
    monkeypatch.setattr(config, "APP_ENV", "local")
    monkeypatch.setattr(config, "PLACEMENT_STORE_BACKEND", "postgres")
    monkeypatch.setattr(config, "GPU_PLACEMENT_DATABASE_URL", "")

    with pytest.raises(ValueError, match="GPU_PLACEMENT_DATABASE_URL"):
        config.validate_runtime_config("worker")


def test_production_api_rejects_localhost_kafka(monkeypatch):
    monkeypatch.setattr(config, "APP_ENV", "production")
    monkeypatch.setattr(config, "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setattr(config, "TEMPORAL_HOST", "temporal.example.com:7233")

    with pytest.raises(ValueError, match="KAFKA_BOOTSTRAP_SERVERS"):
        config.validate_runtime_config("api")


def test_production_api_rejects_localhost_temporal(monkeypatch):
    monkeypatch.setattr(config, "APP_ENV", "production")
    monkeypatch.setattr(config, "KAFKA_BOOTSTRAP_SERVERS", "kafka.example.com:9093")
    monkeypatch.setattr(config, "TEMPORAL_HOST", "localhost:7233")

    with pytest.raises(ValueError, match="TEMPORAL_HOST"):
        config.validate_runtime_config("api")


def test_production_worker_requires_durable_real_gpu_config(monkeypatch):
    monkeypatch.setattr(config, "APP_ENV", "production")
    monkeypatch.setattr(config, "KAFKA_BOOTSTRAP_SERVERS", "kafka.example.com:9093")
    monkeypatch.setattr(config, "TEMPORAL_HOST", "temporal.example.com:7233")
    monkeypatch.setattr(config, "PLACEMENT_STORE_BACKEND", "memory")
    monkeypatch.setattr(config, "GPU_PLACEMENT_DATABASE_URL", "")
    monkeypatch.setattr(config, "HELM_DRY_RUN", False)
    monkeypatch.setattr(config, "HELM_KUBE_INSECURE_SKIP_TLS_VERIFY", False)
    monkeypatch.setattr(config, "HELM_MOCK_GPU", True)
    monkeypatch.setattr(
        config,
        "GPU_PROFILES_CONFIG",
        '{"profiles":{"premium":{"gpu_count":2,"gpu_type":"mock",'
        '"default_max_latency_ms":80}}}',
    )

    with pytest.raises(ValueError) as exc_info:
        config.validate_runtime_config("worker")

    error = str(exc_info.value)
    assert "PLACEMENT_STORE_BACKEND must be postgres" in error
    assert "GPU_PLACEMENT_DATABASE_URL" in error
    assert "HELM_MOCK_GPU must be false" in error
    assert "GPU profile gpu_type must not be mock" in error


def test_production_worker_accepts_postgres_real_gpu_config(monkeypatch):
    monkeypatch.setattr(config, "APP_ENV", "production")
    monkeypatch.setattr(config, "KAFKA_BOOTSTRAP_SERVERS", "kafka.example.com:9093")
    monkeypatch.setattr(config, "TEMPORAL_HOST", "temporal.example.com:7233")
    monkeypatch.setattr(config, "PLACEMENT_STORE_BACKEND", "postgres")
    monkeypatch.setattr(
        config,
        "GPU_PLACEMENT_DATABASE_URL",
        "postgresql://user:pass@db.example.com/gpu",
    )
    monkeypatch.setattr(config, "HELM_DRY_RUN", False)
    monkeypatch.setattr(config, "HELM_KUBE_INSECURE_SKIP_TLS_VERIFY", False)
    monkeypatch.setattr(config, "HELM_MOCK_GPU", False)
    monkeypatch.setattr(
        config,
        "GPU_PROFILES_CONFIG",
        '{"profiles":{"premium":{"gpu_count":2,"gpu_type":"nvidia-a100",'
        '"default_max_latency_ms":80}}}',
    )

    config.validate_runtime_config("worker")
