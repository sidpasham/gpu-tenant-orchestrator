import json
import re
import uuid
from typing import Any

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field, field_validator
from src.shared.config import (
    GPU_PROFILES_CONFIG,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_PRODUCER_FLUSH_TIMEOUT_SECONDS,
    KAFKA_TOPIC,
    validate_runtime_config,
)
from src.shared.gpu_profiles import load_gpu_profiles

validate_runtime_config("api")

app = FastAPI(title="Distributed GPU Tenant Ingress Gateway")

GPU_PROFILE_CATALOG = load_gpu_profiles(GPU_PROFILES_CONFIG)
PLACEMENT_NAME_PATTERN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
producer: Producer | None = None
metrics = {
    "allocation_requests_total": 0,
    "allocation_publish_success_total": 0,
    "allocation_publish_failures_total": 0,
}


class AllocationRequest(BaseModel):
    customer_id: str = Field(
        min_length=1,
        max_length=38,
        pattern=r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$",
        description="Kubernetes-safe tenant identifier used in Helm resource names.",
    )
    tier: str
    preferred_region: str | None = Field(
        default=None,
        min_length=1,
        max_length=32,
        description="Preferred customer region for GPU placement.",
    )
    allowed_regions: list[str] | None = Field(
        default=None,
        min_length=1,
        max_length=10,
        description="Regions the customer allows as placement fallbacks.",
    )
    max_latency_ms: int | None = Field(
        default=None,
        ge=1,
        le=10_000,
        description="Maximum accepted latency between customer and GPU region.",
    )
    gpu_type: str | None = Field(
        default=None,
        min_length=1,
        max_length=32,
        description="Requested GPU type, such as mock or nvidia-a100.",
    )

    @field_validator("tier", "preferred_region", "gpu_type", mode="before")
    @classmethod
    def normalize_optional_name(cls, value):
        if value is None:
            return value
        if not isinstance(value, str):
            return value
        return value.lower()

    @field_validator("preferred_region", "gpu_type")
    @classmethod
    def validate_optional_placement_name(cls, value):
        if value is None:
            return value
        if not PLACEMENT_NAME_PATTERN.fullmatch(value):
            raise ValueError("must contain lowercase letters, numbers, and hyphens")
        return value

    @field_validator("allowed_regions", mode="before")
    @classmethod
    def normalize_allowed_regions(cls, value):
        if value is None:
            return value
        if not isinstance(value, list):
            return value
        return [item.lower() if isinstance(item, str) else item for item in value]

    @field_validator("allowed_regions")
    @classmethod
    def validate_allowed_regions(cls, value):
        if value is None:
            return value
        normalized_regions = []
        for region in value:
            if not isinstance(region, str) or not PLACEMENT_NAME_PATTERN.fullmatch(
                region
            ):
                raise ValueError(
                    "regions must contain lowercase letters, numbers, and hyphens"
                )
            if region not in normalized_regions:
                normalized_regions.append(region)
        return normalized_regions


def format_accepted_tiers() -> str:
    tiers = sorted(GPU_PROFILE_CATALOG.valid_tiers())
    if len(tiers) == 1:
        return f"'{tiers[0]}'"
    return ", ".join(f"'{tier}'" for tier in tiers[:-1]) + f" or '{tiers[-1]}'"


def get_producer() -> Producer:
    global producer

    if producer is None:
        producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    return producer


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def publish_allocation_event(payload: dict[str, Any]) -> None:
    delivery_error = None
    delivery_confirmed = False

    def track_delivery(err, msg):
        nonlocal delivery_confirmed, delivery_error
        delivery_confirmed = True
        delivery_error = err
        delivery_report(err, msg)

    kafka_producer = get_producer()
    kafka_producer.produce(
        KAFKA_TOPIC,
        key=payload["customer_id"],
        value=json.dumps(payload, separators=(",", ":"), sort_keys=True),
        callback=track_delivery,
    )

    remaining_messages = kafka_producer.flush(KAFKA_PRODUCER_FLUSH_TIMEOUT_SECONDS)
    if remaining_messages > 0:
        raise RuntimeError(
            "Timed out waiting for Kafka delivery confirmation "
            f"for {remaining_messages} message(s)"
        )
    if not delivery_confirmed:
        raise RuntimeError("Kafka delivery callback did not run")
    if delivery_error is not None:
        raise RuntimeError(f"Kafka delivery failed: {delivery_error}")


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    return {"status": "ready"}


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics_endpoint():
    return "\n".join(
        [
            "# HELP gpu_tenant_api_allocation_requests_total Allocation requests received.",
            "# TYPE gpu_tenant_api_allocation_requests_total counter",
            f"gpu_tenant_api_allocation_requests_total {metrics['allocation_requests_total']}",
            "# HELP gpu_tenant_api_allocation_publish_success_total Allocation events delivered to Kafka.",
            "# TYPE gpu_tenant_api_allocation_publish_success_total counter",
            f"gpu_tenant_api_allocation_publish_success_total {metrics['allocation_publish_success_total']}",
            "# HELP gpu_tenant_api_allocation_publish_failures_total Allocation events that failed Kafka publish.",
            "# TYPE gpu_tenant_api_allocation_publish_failures_total counter",
            f"gpu_tenant_api_allocation_publish_failures_total {metrics['allocation_publish_failures_total']}",
            "",
        ]
    )


@app.post("/api/v1/tenant/allocate", status_code=202)
def allocate_gpu(request: AllocationRequest):
    metrics["allocation_requests_total"] += 1
    tier = request.tier.lower()
    if GPU_PROFILE_CATALOG.get(tier) is None:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid tier. Choose {format_accepted_tiers()}.",
        )

    payload = {
        "allocation_id": f"alloc-{uuid.uuid4().hex}",
        "customer_id": request.customer_id,
        "tier": tier,
    }
    if request.preferred_region:
        payload["preferred_region"] = request.preferred_region
    if request.allowed_regions:
        payload["allowed_regions"] = request.allowed_regions
    if request.max_latency_ms is not None:
        payload["max_latency_ms"] = request.max_latency_ms
    if request.gpu_type:
        payload["gpu_type"] = request.gpu_type

    try:
        publish_allocation_event(payload)
        metrics["allocation_publish_success_total"] += 1
        return {"status": "Accepted", "message": "GPU Allocation event sent to queue."}
    except Exception as e:
        metrics["allocation_publish_failures_total"] += 1
        raise HTTPException(
            status_code=500,
            detail=f"Failed to publish event to Kafka: {str(e)}",
        )
