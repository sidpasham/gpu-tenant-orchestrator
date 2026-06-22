import json

import pytest
from fastapi.testclient import TestClient

from src.api import main as api


class DummyProducer:
    def __init__(self, fail: bool = False, flush_result: int = 0, delivery_error=None):
        self.fail = fail
        self.flush_result = flush_result
        self.delivery_error = delivery_error
        self.records = []
        self.flush_calls = []

    def produce(self, topic, key, value, callback):
        if self.fail:
            raise RuntimeError("broker unavailable")
        self.records.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
                "callback": callback,
            }
        )

    def flush(self, timeout):
        self.flush_calls.append(timeout)
        for record in self.records:
            record["callback"](self.delivery_error, DummyKafkaMessage(record["topic"]))
        return self.flush_result


class DummyKafkaMessage:
    def __init__(self, topic):
        self._topic = topic

    def topic(self):
        return self._topic

    def partition(self):
        return 0


@pytest.fixture(autouse=True)
def reset_metrics():
    for key in api.metrics:
        api.metrics[key] = 0
    yield


def test_allocate_gpu_accepts_valid_request_and_publishes_event(monkeypatch):
    producer = DummyProducer()
    monkeypatch.setattr(api, "get_producer", lambda: producer)
    client = TestClient(api.app)

    response = client.post(
        "/api/v1/tenant/allocate",
        json={"customer_id": "team-a", "tier": "Premium"},
    )

    assert response.status_code == 202
    assert response.json() == {
        "status": "Accepted",
        "message": "GPU Allocation event sent to queue.",
    }
    assert producer.flush_calls == [api.KAFKA_PRODUCER_FLUSH_TIMEOUT_SECONDS]
    assert len(producer.records) == 1

    record = producer.records[0]
    assert record["topic"] == api.KAFKA_TOPIC
    assert record["key"] == "team-a"
    assert json.loads(record["value"]) == {
        "customer_id": "team-a",
        "tier": "premium",
    }
    assert callable(record["callback"])


def test_health_and_readiness_endpoints_do_not_require_kafka():
    client = TestClient(api.app)

    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").json() == {"status": "ready"}


def test_metrics_endpoint_reports_api_counters(monkeypatch):
    producer = DummyProducer()
    monkeypatch.setattr(api, "get_producer", lambda: producer)
    client = TestClient(api.app)

    client.post(
        "/api/v1/tenant/allocate",
        json={"customer_id": "team-a", "tier": "premium"},
    )

    response = client.get("/metrics")

    assert response.status_code == 200
    assert "gpu_tenant_api_allocation_requests_total 1" in response.text
    assert "gpu_tenant_api_allocation_publish_success_total 1" in response.text
    assert "gpu_tenant_api_allocation_publish_failures_total 0" in response.text


def test_allocate_gpu_rejects_unknown_tier_without_publishing(monkeypatch):
    producer = DummyProducer()
    monkeypatch.setattr(api, "get_producer", lambda: producer)
    client = TestClient(api.app)

    response = client.post(
        "/api/v1/tenant/allocate",
        json={"customer_id": "team-a", "tier": "gold"},
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "Invalid tier. Choose 'premium' or 'standard'."
    }
    assert producer.records == []
    assert producer.flush_calls == []


def test_allocate_gpu_rejects_invalid_customer_id(monkeypatch):
    producer = DummyProducer()
    monkeypatch.setattr(api, "get_producer", lambda: producer)
    client = TestClient(api.app)

    response = client.post(
        "/api/v1/tenant/allocate",
        json={"customer_id": "Team A", "tier": "standard"},
    )

    assert response.status_code == 422
    assert producer.records == []
    assert producer.flush_calls == []


def test_allocate_gpu_reports_publish_failures(monkeypatch):
    producer = DummyProducer(fail=True)
    monkeypatch.setattr(api, "get_producer", lambda: producer)
    client = TestClient(api.app)

    response = client.post(
        "/api/v1/tenant/allocate",
        json={"customer_id": "team-a", "tier": "standard"},
    )

    assert response.status_code == 500
    assert response.json() == {
        "detail": "Failed to publish event to Kafka: broker unavailable"
    }
    assert producer.flush_calls == []


def test_allocate_gpu_reports_delivery_timeout(monkeypatch):
    producer = DummyProducer(flush_result=1)
    monkeypatch.setattr(api, "get_producer", lambda: producer)
    client = TestClient(api.app)

    response = client.post(
        "/api/v1/tenant/allocate",
        json={"customer_id": "team-a", "tier": "standard"},
    )

    assert response.status_code == 500
    assert response.json() == {
        "detail": (
            "Failed to publish event to Kafka: Timed out waiting for Kafka "
            "delivery confirmation for 1 message(s)"
        )
    }


def test_allocate_gpu_reports_delivery_callback_failure(monkeypatch):
    producer = DummyProducer(delivery_error="broker rejected message")
    monkeypatch.setattr(api, "get_producer", lambda: producer)
    client = TestClient(api.app)

    response = client.post(
        "/api/v1/tenant/allocate",
        json={"customer_id": "team-a", "tier": "standard"},
    )

    assert response.status_code == 500
    assert response.json() == {
        "detail": (
            "Failed to publish event to Kafka: "
            "Kafka delivery failed: broker rejected message"
        )
    }
