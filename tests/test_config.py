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
