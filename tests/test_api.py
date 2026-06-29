import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from temporalio.client import RPCError, RPCStatusCode, WorkflowExecutionStatus

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


class DummyWorkflowHandle:
    def __init__(self, status, result=None, error=None):
        self.status = status
        self.result_payload = result
        self.error = error
        self.describe_calls = 0
        self.result_calls = 0

    async def describe(self, rpc_timeout):
        self.describe_calls += 1
        if self.error is not None:
            raise self.error
        return SimpleNamespace(
            status=self.status,
            run_id="run-123",
            task_queue="gpu-allocation-tasks",
            start_time=datetime(2026, 6, 29, 12, 0, tzinfo=timezone.utc),
            close_time=(
                datetime(2026, 6, 29, 12, 1, tzinfo=timezone.utc)
                if self.status == WorkflowExecutionStatus.COMPLETED
                else None
            ),
        )

    async def result(self, rpc_timeout):
        self.result_calls += 1
        return self.result_payload


class DummyTemporalClient:
    def __init__(self, handle):
        self.handle = handle
        self.workflow_ids = []

    def get_workflow_handle(self, workflow_id):
        self.workflow_ids.append(workflow_id)
        return self.handle


@pytest.fixture(autouse=True)
def reset_metrics():
    for key in api.metrics:
        api.metrics[key] = 0
    api.temporal_client = None
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
    response_body = response.json()
    assert response_body["allocation_id"].startswith("alloc-")
    assert response_body == {
        "allocation_id": response_body["allocation_id"],
        "status": "Accepted",
        "message": "GPU Allocation event sent to queue.",
    }
    assert producer.flush_calls == [api.KAFKA_PRODUCER_FLUSH_TIMEOUT_SECONDS]
    assert len(producer.records) == 1

    record = producer.records[0]
    assert record["topic"] == api.KAFKA_TOPIC
    assert record["key"] == "team-a"
    payload = json.loads(record["value"])
    assert payload.pop("allocation_id") == response_body["allocation_id"]
    assert payload == {
        "customer_id": "team-a",
        "tier": "premium",
    }
    assert callable(record["callback"])


def test_allocate_gpu_publishes_optional_placement_preferences(monkeypatch):
    producer = DummyProducer()
    monkeypatch.setattr(api, "get_producer", lambda: producer)
    client = TestClient(api.app)

    response = client.post(
        "/api/v1/tenant/allocate",
        json={
            "customer_id": "team-a",
            "tier": "Premium",
            "preferred_region": "US-PHOENIX-1",
            "allowed_regions": ["US-PHOENIX-1", "us-ashburn-1", "us-ashburn-1"],
            "max_latency_ms": 80,
            "gpu_type": "MOCK",
        },
    )

    assert response.status_code == 202
    payload = json.loads(producer.records[0]["value"])
    assert payload.pop("allocation_id").startswith("alloc-")
    assert payload == {
        "customer_id": "team-a",
        "tier": "premium",
        "preferred_region": "us-phoenix-1",
        "allowed_regions": ["us-phoenix-1", "us-ashburn-1"],
        "max_latency_ms": 80,
        "gpu_type": "mock",
    }


def test_health_and_readiness_endpoints_do_not_require_kafka():
    client = TestClient(api.app)

    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").json() == {"status": "ready"}


def test_get_allocation_status_reports_running_workflow(monkeypatch):
    handle = DummyWorkflowHandle(WorkflowExecutionStatus.RUNNING)
    temporal_client = DummyTemporalClient(handle)

    async def fake_get_temporal_client():
        return temporal_client

    monkeypatch.setattr(api, "get_temporal_client", fake_get_temporal_client)
    client = TestClient(api.app)

    response = client.get("/api/v1/tenant/allocations/alloc-123")

    assert response.status_code == 200
    assert response.json() == {
        "allocation_id": "alloc-123",
        "workflow_id": "gpu-alloc-alloc-123",
        "status": "running",
        "workflow_status": "running",
        "run_id": "run-123",
        "task_queue": "gpu-allocation-tasks",
        "started_at": "2026-06-29T12:00:00+00:00",
        "closed_at": None,
    }
    assert temporal_client.workflow_ids == ["gpu-alloc-alloc-123"]
    assert handle.describe_calls == 1
    assert handle.result_calls == 0


def test_get_allocation_status_returns_completed_workflow_result(monkeypatch):
    handle = DummyWorkflowHandle(
        WorkflowExecutionStatus.COMPLETED,
        result={"status": "ACTIVE", "message": "done"},
    )

    async def fake_get_temporal_client():
        return DummyTemporalClient(handle)

    monkeypatch.setattr(api, "get_temporal_client", fake_get_temporal_client)
    client = TestClient(api.app)

    response = client.get("/api/v1/tenant/allocations/alloc-123")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "active"
    assert body["workflow_status"] == "completed"
    assert body["closed_at"] == "2026-06-29T12:01:00+00:00"
    assert body["result"] == {"status": "ACTIVE", "message": "done"}
    assert handle.result_calls == 1


def test_get_allocation_status_reports_queued_when_workflow_has_not_started(
    monkeypatch,
):
    handle = DummyWorkflowHandle(
        WorkflowExecutionStatus.RUNNING,
        error=RPCError("not found", RPCStatusCode.NOT_FOUND, b""),
    )

    async def fake_get_temporal_client():
        return DummyTemporalClient(handle)

    monkeypatch.setattr(api, "get_temporal_client", fake_get_temporal_client)
    client = TestClient(api.app)

    response = client.get("/api/v1/tenant/allocations/alloc-123")

    assert response.status_code == 200
    assert response.json() == {
        "allocation_id": "alloc-123",
        "workflow_id": "gpu-alloc-alloc-123",
        "status": "queued",
        "message": "Allocation event accepted but workflow has not started yet.",
    }


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


def test_allocate_gpu_rejects_invalid_allowed_region(monkeypatch):
    producer = DummyProducer()
    monkeypatch.setattr(api, "get_producer", lambda: producer)
    client = TestClient(api.app)

    response = client.post(
        "/api/v1/tenant/allocate",
        json={
            "customer_id": "team-a",
            "tier": "standard",
            "allowed_regions": ["us_phoenix_1"],
        },
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
