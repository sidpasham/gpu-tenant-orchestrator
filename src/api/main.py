import json
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from confluent_kafka import Producer
from src.shared.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_PRODUCER_FLUSH_TIMEOUT_SECONDS,
    KAFKA_TOPIC,
)

app = FastAPI(title="Distributed GPU Tenant Ingress Gateway")

VALID_TIERS = {"premium", "standard"}
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


def publish_allocation_event(payload: dict[str, str]) -> None:
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
    if tier not in VALID_TIERS:
        raise HTTPException(
            status_code=400,
            detail="Invalid tier. Choose 'premium' or 'standard'.",
        )

    payload = {
        "customer_id": request.customer_id,
        "tier": tier,
    }

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
