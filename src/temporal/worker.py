import asyncio
import json
import threading
import time
from concurrent.futures import Future
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from temporalio.client import Client
from temporalio.worker import Worker

from src.shared.config import (
    GPU_PROFILES_CONFIG,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_DLQ_TOPIC,
    KAFKA_STARTUP_RETRY_SECONDS,
    KAFKA_TOPIC,
    KAFKA_TOPIC_PARTITIONS,
    KAFKA_TOPIC_REPLICATION_FACTOR,
    TEMPORAL_CONNECT_TIMEOUT_SECONDS,
    TEMPORAL_HOST,
    TEMPORAL_STARTUP_RETRY_SECONDS,
    WORKER_HEALTH_HOST,
    WORKER_HEALTH_PORT,
    WORKFLOW_RESULT_TIMEOUT_SECONDS,
    validate_runtime_config,
)
from src.shared.gpu_profiles import load_gpu_profiles
from src.temporal.activities import (
    activate_gpu_reservation,
    plan_gpu_allocation,
    release_gpu_reservation,
    run_helm_deploy,
)
from src.temporal.workflows import GPUAllocationWorkflow


RETRYABLE_KAFKA_ERRORS = {
    KafkaError._PARTITION_EOF,
    KafkaError.UNKNOWN_TOPIC_OR_PART,
    KafkaError.LEADER_NOT_AVAILABLE,
    KafkaError._TRANSPORT,
    KafkaError._ALL_BROKERS_DOWN,
}
GPU_COUNT_BY_TIER = load_gpu_profiles(GPU_PROFILES_CONFIG).gpu_counts_by_tier()


def is_retryable_kafka_error(error: KafkaError) -> bool:
    return error.code() in RETRYABLE_KAFKA_ERRORS


def is_topic_already_exists_error(exc: Exception) -> bool:
    if isinstance(exc, KafkaException) and exc.args:
        kafka_error = exc.args[0]
        topic_exists_code = getattr(KafkaError, "TOPIC_ALREADY_EXISTS", None)
        if (
            isinstance(kafka_error, KafkaError)
            and topic_exists_code is not None
            and kafka_error.code() == topic_exists_code
        ):
            return True
    return "already exists" in str(exc).lower() or "TOPIC_ALREADY_EXISTS" in str(exc)


def prometheus_label_value(value: Any) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def workflow_completion_status(result: Any) -> str:
    if not isinstance(result, dict):
        return "success"

    status = str(result.get("status", "success")).lower()
    if status == "active":
        return "success"
    return status


@dataclass
class WorkerHealthState:
    kafka_consumer_running: bool = False
    temporal_worker_running: bool = False
    messages_consumed_total: int = 0
    kafka_dispatcher_failures_total: int = 0
    temporal_connection_failures_total: int = 0
    invalid_messages_total: int = 0
    dlq_messages_total: int = 0
    workflow_start_failures_total: int = 0
    last_kafka_error: str = ""
    last_temporal_error: str = ""
    last_dlq_error: str = ""
    customer_allocations: dict[tuple[str, str], int] = field(default_factory=dict)
    customer_allocation_gpu_counts: dict[tuple[str, str], int] = field(default_factory=dict)
    customer_allocation_durations: dict[tuple[str, str, str], float] = field(default_factory=dict)
    customer_allocation_completions: dict[tuple[str, str, str], int] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def set_kafka_consumer_running(self, running: bool) -> None:
        with self._lock:
            self.kafka_consumer_running = running

    def set_temporal_worker_running(self, running: bool) -> None:
        with self._lock:
            self.temporal_worker_running = running

    def record_consumed_message(self) -> None:
        with self._lock:
            self.messages_consumed_total += 1

    def record_customer_allocation(self, customer_id: str, tier: str) -> None:
        normalized_tier = tier.lower()
        allocation_key = (customer_id, normalized_tier)
        with self._lock:
            self.customer_allocations[allocation_key] = (
                self.customer_allocations.get(allocation_key, 0) + 1
            )
            self.customer_allocation_gpu_counts[allocation_key] = GPU_COUNT_BY_TIER.get(
                normalized_tier, 0
            )

    def record_customer_allocation_completion(
        self,
        customer_id: str,
        tier: str,
        status: str,
        duration_seconds: float,
    ) -> None:
        completion_key = (customer_id, tier.lower(), status)
        with self._lock:
            self.customer_allocation_durations[completion_key] = duration_seconds
            self.customer_allocation_completions[completion_key] = (
                self.customer_allocation_completions.get(completion_key, 0) + 1
            )

    def record_kafka_dispatcher_failure(self) -> None:
        with self._lock:
            self.kafka_dispatcher_failures_total += 1

    def record_temporal_connection_failure(self, error: str) -> None:
        with self._lock:
            self.temporal_connection_failures_total += 1
            self.last_temporal_error = error

    def record_temporal_connection_success(self) -> None:
        with self._lock:
            self.last_temporal_error = ""

    def record_invalid_message(self) -> None:
        with self._lock:
            self.invalid_messages_total += 1

    def record_dlq_message(self) -> None:
        with self._lock:
            self.dlq_messages_total += 1
            self.last_dlq_error = ""

    def record_workflow_start_failure(self) -> None:
        with self._lock:
            self.workflow_start_failures_total += 1

    def record_kafka_error(self, error: str) -> None:
        with self._lock:
            self.last_kafka_error = error

    def record_dlq_error(self, error: str) -> None:
        with self._lock:
            self.last_dlq_error = error

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "kafka_consumer_running": self.kafka_consumer_running,
                "temporal_worker_running": self.temporal_worker_running,
                "messages_consumed_total": self.messages_consumed_total,
                "kafka_dispatcher_failures_total": self.kafka_dispatcher_failures_total,
                "temporal_connection_failures_total": self.temporal_connection_failures_total,
                "invalid_messages_total": self.invalid_messages_total,
                "dlq_messages_total": self.dlq_messages_total,
                "workflow_start_failures_total": self.workflow_start_failures_total,
                "last_kafka_error": self.last_kafka_error,
                "last_temporal_error": self.last_temporal_error,
                "last_dlq_error": self.last_dlq_error,
                "customer_allocations": dict(self.customer_allocations),
                "customer_allocation_gpu_counts": dict(
                    self.customer_allocation_gpu_counts
                ),
                "customer_allocation_durations": dict(
                    self.customer_allocation_durations
                ),
                "customer_allocation_completions": dict(
                    self.customer_allocation_completions
                ),
            }


HEALTH_STATE = WorkerHealthState()


def render_worker_metrics(state: WorkerHealthState) -> str:
    snapshot = state.snapshot()
    lines = [
        "# HELP gpu_tenant_worker_kafka_consumer_running Whether the Kafka consumer loop is running.",
        "# TYPE gpu_tenant_worker_kafka_consumer_running gauge",
        f"gpu_tenant_worker_kafka_consumer_running {int(snapshot['kafka_consumer_running'])}",
        "# HELP gpu_tenant_worker_temporal_worker_running Whether the Temporal worker is running.",
        "# TYPE gpu_tenant_worker_temporal_worker_running gauge",
        f"gpu_tenant_worker_temporal_worker_running {int(snapshot['temporal_worker_running'])}",
        "# HELP gpu_tenant_worker_messages_consumed_total Valid Kafka messages dispatched to Temporal.",
        "# TYPE gpu_tenant_worker_messages_consumed_total counter",
        f"gpu_tenant_worker_messages_consumed_total {snapshot['messages_consumed_total']}",
        "# HELP gpu_tenant_worker_kafka_dispatcher_failures_total Kafka dispatcher initialization or runtime failures that triggered retry.",
        "# TYPE gpu_tenant_worker_kafka_dispatcher_failures_total counter",
        f"gpu_tenant_worker_kafka_dispatcher_failures_total {snapshot['kafka_dispatcher_failures_total']}",
        "# HELP gpu_tenant_worker_temporal_connection_failures_total Temporal connection attempts that failed before worker startup.",
        "# TYPE gpu_tenant_worker_temporal_connection_failures_total counter",
        f"gpu_tenant_worker_temporal_connection_failures_total {snapshot['temporal_connection_failures_total']}",
        "# HELP gpu_tenant_worker_invalid_messages_total Invalid Kafka messages observed.",
        "# TYPE gpu_tenant_worker_invalid_messages_total counter",
        f"gpu_tenant_worker_invalid_messages_total {snapshot['invalid_messages_total']}",
        "# HELP gpu_tenant_worker_dlq_messages_total Messages written to the dead-letter topic.",
        "# TYPE gpu_tenant_worker_dlq_messages_total counter",
        f"gpu_tenant_worker_dlq_messages_total {snapshot['dlq_messages_total']}",
        "# HELP gpu_tenant_worker_workflow_start_failures_total Kafka events that failed Temporal workflow start.",
        "# TYPE gpu_tenant_worker_workflow_start_failures_total counter",
        f"gpu_tenant_worker_workflow_start_failures_total {snapshot['workflow_start_failures_total']}",
        "# HELP gpu_tenant_worker_customer_allocations_total Allocation events processed by customer and tier.",
        "# TYPE gpu_tenant_worker_customer_allocations_total counter",
    ]
    for (customer_id, tier), count in sorted(snapshot["customer_allocations"].items()):
        lines.append(
            'gpu_tenant_worker_customer_allocations_total'
            f'{{customer_id="{prometheus_label_value(customer_id)}",'
            f'tier="{prometheus_label_value(tier)}"}} {count}'
        )
    lines.extend(
        [
            "# HELP gpu_tenant_worker_customer_allocation_gpu_count Last requested GPU count by customer and tier.",
            "# TYPE gpu_tenant_worker_customer_allocation_gpu_count gauge",
        ]
    )
    for (customer_id, tier), gpu_count in sorted(
        snapshot["customer_allocation_gpu_counts"].items()
    ):
        lines.append(
            'gpu_tenant_worker_customer_allocation_gpu_count'
            f'{{customer_id="{prometheus_label_value(customer_id)}",'
            f'tier="{prometheus_label_value(tier)}"}} {gpu_count}'
        )
    lines.extend(
        [
            "# HELP gpu_tenant_worker_customer_allocation_duration_seconds Last end-to-end allocation duration by customer, tier, and status.",
            "# TYPE gpu_tenant_worker_customer_allocation_duration_seconds gauge",
        ]
    )
    for (customer_id, tier, status), duration_seconds in sorted(
        snapshot["customer_allocation_durations"].items()
    ):
        lines.append(
            "gpu_tenant_worker_customer_allocation_duration_seconds"
            f'{{customer_id="{prometheus_label_value(customer_id)}",'
            f'tier="{prometheus_label_value(tier)}",'
            f'status="{prometheus_label_value(status)}"}} {duration_seconds:.6f}'
        )
    lines.extend(
        [
            "# HELP gpu_tenant_worker_customer_allocations_completed_total End-to-end allocation completions by customer, tier, and status.",
            "# TYPE gpu_tenant_worker_customer_allocations_completed_total counter",
        ]
    )
    for (customer_id, tier, status), count in sorted(
        snapshot["customer_allocation_completions"].items()
    ):
        lines.append(
            "gpu_tenant_worker_customer_allocations_completed_total"
            f'{{customer_id="{prometheus_label_value(customer_id)}",'
            f'tier="{prometheus_label_value(tier)}",'
            f'status="{prometheus_label_value(status)}"}} {count}'
        )
    lines.append("")
    return "\n".join(lines)


class WorkerHealthHandler(BaseHTTPRequestHandler):
    state = HEALTH_STATE

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self._write_json(200, {"status": "ok"})
            return

        if self.path == "/readyz":
            snapshot = self.state.snapshot()
            ready = (
                snapshot["kafka_consumer_running"]
                and snapshot["temporal_worker_running"]
            )
            status_code = 200 if ready else 503
            body = {
                "status": "ready" if ready else "not_ready",
                "kafka_consumer_running": snapshot["kafka_consumer_running"],
                "temporal_worker_running": snapshot["temporal_worker_running"],
            }
            self._write_json(status_code, body)
            return

        if self.path == "/metrics":
            self._write_text(
                200,
                render_worker_metrics(self.state),
                "text/plain; version=0.0.4",
            )
            return

        self._write_json(404, {"detail": "not found"})

    def log_message(self, _format: str, *_args: Any) -> None:
        return

    def _write_json(self, status_code: int, body: dict[str, Any]) -> None:
        self._write_text(status_code, json.dumps(body), "application/json")

    def _write_text(self, status_code: int, body: str, content_type: str) -> None:
        data = body.encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def start_health_server(state: WorkerHealthState) -> ThreadingHTTPServer:
    handler = type("BoundWorkerHealthHandler", (WorkerHealthHandler,), {"state": state})
    server = ThreadingHTTPServer((WORKER_HEALTH_HOST, WORKER_HEALTH_PORT), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"Worker health server listening on {WORKER_HEALTH_HOST}:{WORKER_HEALTH_PORT}")
    return server


class KafkaWorkflowDispatcher:
    def __init__(
        self,
        temporal_client: Client,
        temporal_loop: asyncio.AbstractEventLoop,
        state: WorkerHealthState | None = None,
        admin_factory=AdminClient,
        consumer_factory=Consumer,
        producer_factory=Producer,
        kafka_topic: str = KAFKA_TOPIC,
        dlq_topic: str = KAFKA_DLQ_TOPIC,
        topic_partitions: int = KAFKA_TOPIC_PARTITIONS,
        topic_replication_factor: int = KAFKA_TOPIC_REPLICATION_FACTOR,
        startup_retry_seconds: float = KAFKA_STARTUP_RETRY_SECONDS,
        workflow_result_timeout_seconds: float = WORKFLOW_RESULT_TIMEOUT_SECONDS,
    ) -> None:
        self.temporal_client = temporal_client
        self.temporal_loop = temporal_loop
        self.state = state or HEALTH_STATE
        self.admin_factory = admin_factory
        self.consumer_factory = consumer_factory
        self.producer_factory = producer_factory
        self.kafka_topic = kafka_topic
        self.dlq_topic = dlq_topic
        self.topic_partitions = topic_partitions
        self.topic_replication_factor = topic_replication_factor
        self.startup_retry_seconds = startup_retry_seconds
        self.workflow_result_timeout_seconds = workflow_result_timeout_seconds
        self.consumer = None
        self.dlq_producer = None
        self._running = threading.Event()

    def run_forever(self) -> None:
        self._running.set()
        while self._running.is_set():
            try:
                self.initialize_kafka_clients()
                print("Kafka consumer dispatcher running...")
                self.state.set_kafka_consumer_running(True)
                while self._running.is_set():
                    self.poll_once()
            except Exception as exc:
                self.state.record_kafka_dispatcher_failure()
                self.state.record_kafka_error(str(exc))
                print(
                    "Kafka dispatcher unavailable; "
                    f"retrying in {self.startup_retry_seconds:g}s: {exc}"
                )
            finally:
                self.state.set_kafka_consumer_running(False)
                self.close_kafka_clients()

            if self._running.is_set():
                time.sleep(self.startup_retry_seconds)

    def initialize_kafka_clients(self) -> None:
        admin_client = self.admin_factory({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        self.ensure_topics(admin_client)

        self.consumer = self.consumer_factory(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "group.id": "temporal-orchestrator-group",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "allow.auto.create.topics": True,
            }
        )
        self.dlq_producer = self.producer_factory(
            {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
        )
        self.consumer.subscribe([self.kafka_topic])

    def ensure_topics(self, admin_client) -> None:
        topics = [
            NewTopic(
                self.kafka_topic,
                num_partitions=self.topic_partitions,
                replication_factor=self.topic_replication_factor,
            ),
            NewTopic(
                self.dlq_topic,
                num_partitions=self.topic_partitions,
                replication_factor=self.topic_replication_factor,
            ),
        ]
        futures = admin_client.create_topics(topics)
        for topic_name, future in futures.items():
            try:
                future.result(timeout=30)
                print(f"Kafka topic ready: {topic_name}")
            except Exception as exc:
                if is_topic_already_exists_error(exc):
                    print(f"Kafka topic already exists: {topic_name}")
                    continue
                raise RuntimeError(
                    f"Failed to ensure Kafka topic {topic_name}: {exc}"
                ) from exc

    def close_kafka_clients(self) -> None:
        if self.consumer is not None:
            try:
                self.consumer.close()
            except Exception as exc:
                print(f"Failed to close Kafka consumer cleanly: {exc}")
            finally:
                self.consumer = None

        if self.dlq_producer is not None:
            try:
                self.dlq_producer.flush(5)
            except Exception as exc:
                print(f"Failed to flush DLQ producer cleanly: {exc}")
            finally:
                self.dlq_producer = None

    def stop(self) -> None:
        self._running.clear()

    def poll_once(self) -> None:
        msg = self.consumer.poll(1.0)
        if msg is None:
            return
        if msg.error():
            self.handle_kafka_error(msg.error())
            return

        try:
            data = self.decode_message(msg)
        except ValueError as exc:
            self.state.record_invalid_message()
            self.send_to_dlq("invalid_message", str(exc), self.message_value(msg), msg)
            self.commit_message(msg)
            return

        print(f"Pulled event from Kafka: {data}, spinning up Temporal Workflow...")
        allocation_start_time = time.monotonic()
        try:
            workflow_result = self.start_workflow(data)
            completion_status = workflow_completion_status(workflow_result)
            allocation_duration_seconds = time.monotonic() - allocation_start_time
            self.state.record_consumed_message()
            self.state.record_customer_allocation(
                str(data["customer_id"]),
                str(data["tier"]),
            )
            self.state.record_customer_allocation_completion(
                str(data["customer_id"]),
                str(data["tier"]),
                completion_status,
                allocation_duration_seconds,
            )
        except Exception as exc:
            allocation_duration_seconds = time.monotonic() - allocation_start_time
            self.state.record_workflow_start_failure()
            self.state.record_customer_allocation_completion(
                str(data.get("customer_id", "unknown")),
                str(data.get("tier", "unknown")),
                "failure",
                allocation_duration_seconds,
            )
            self.send_to_dlq("workflow_start_failed", str(exc), data, msg)
        finally:
            self.commit_message(msg)

    def handle_kafka_error(self, error: KafkaError) -> None:
        self.state.record_kafka_error(str(error))
        if is_retryable_kafka_error(error):
            print(f"Kafka is not ready yet or has no new records: {error}")
            return
        raise RuntimeError(f"Kafka error: {error}")

    def decode_message(self, msg) -> dict[str, Any]:
        raw_value = self.message_value(msg)
        try:
            data = json.loads(raw_value)
        except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
            raise ValueError(f"message must be valid JSON: {exc}") from exc

        if not isinstance(data, dict) or "customer_id" not in data or "tier" not in data:
            raise ValueError("message must be a JSON object with customer_id and tier")
        return data

    def start_workflow(self, data: dict[str, Any]) -> Any:
        workflow_future: Future = asyncio.run_coroutine_threadsafe(
            self.execute_workflow(data),
            self.temporal_loop,
        )
        return workflow_future.result(timeout=self.workflow_result_timeout_seconds)

    async def execute_workflow(self, data: dict[str, Any]) -> Any:
        workflow_key = data.get("allocation_id") or data["customer_id"]
        workflow_handle = await self.temporal_client.start_workflow(
            GPUAllocationWorkflow.run,
            data,
            id=f"gpu-alloc-{workflow_key}",
            task_queue="gpu-allocation-tasks",
        )
        return await workflow_handle.result()

    def send_to_dlq(self, reason: str, error: str, payload: Any, msg) -> None:
        dlq_payload = {
            "reason": reason,
            "error": error,
            "source_topic": self.kafka_topic,
            "payload": payload,
        }
        try:
            self.dlq_producer.produce(
                self.dlq_topic,
                key=self.message_key(msg),
                value=json.dumps(dlq_payload, sort_keys=True),
            )
            self.dlq_producer.flush(5)
            self.state.record_dlq_message()
            print(f"Routed Kafka message to DLQ topic {self.dlq_topic}: {reason}")
        except Exception as exc:
            self.state.record_dlq_error(str(exc))
            print(f"Failed to publish message to DLQ topic {self.dlq_topic}: {exc}")

    def commit_message(self, msg) -> None:
        try:
            self.consumer.commit(message=msg, asynchronous=False)
        except Exception as exc:
            self.state.record_kafka_error(f"commit failed: {exc}")
            print(f"Kafka commit failed: {exc}")

    @staticmethod
    def message_key(msg) -> str | None:
        key = msg.key()
        if key is None:
            return None
        if isinstance(key, bytes):
            return key.decode("utf-8", errors="replace")
        return str(key)

    @staticmethod
    def message_value(msg) -> str:
        value = msg.value()
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)


def start_kafka_consumer(
    temporal_client: Client,
    temporal_loop: asyncio.AbstractEventLoop,
    state: WorkerHealthState | None = None,
) -> None:
    dispatcher = KafkaWorkflowDispatcher(temporal_client, temporal_loop, state)
    dispatcher.run_forever()


async def connect_temporal_with_retry(
    state: WorkerHealthState,
    connect=Client.connect,
    temporal_host: str = TEMPORAL_HOST,
    connect_timeout_seconds: float = TEMPORAL_CONNECT_TIMEOUT_SECONDS,
    retry_seconds: float = TEMPORAL_STARTUP_RETRY_SECONDS,
) -> Client:
    while True:
        try:
            client = await asyncio.wait_for(
                connect(temporal_host),
                timeout=connect_timeout_seconds,
            )
            state.record_temporal_connection_success()
            return client
        except Exception as exc:
            state.record_temporal_connection_failure(str(exc))
            print(
                "Temporal is unavailable; "
                f"retrying in {retry_seconds:g}s: {exc}"
            )
            await asyncio.sleep(retry_seconds)


async def main():
    validate_runtime_config("worker")
    health_server = start_health_server(HEALTH_STATE)
    client = await connect_temporal_with_retry(HEALTH_STATE)
    loop = asyncio.get_running_loop()
    dispatcher = KafkaWorkflowDispatcher(client, loop, HEALTH_STATE)

    kafka_thread = threading.Thread(target=dispatcher.run_forever, daemon=True)
    kafka_thread.start()

    worker = Worker(
        client,
        task_queue="gpu-allocation-tasks",
        workflows=[GPUAllocationWorkflow],
        activities=[
            plan_gpu_allocation,
            run_helm_deploy,
            activate_gpu_reservation,
            release_gpu_reservation,
        ],
    )
    print("Temporal Activity and Workflow Worker successfully active.")
    HEALTH_STATE.set_temporal_worker_running(True)
    try:
        await worker.run()
    finally:
        HEALTH_STATE.set_temporal_worker_running(False)
        dispatcher.stop()
        health_server.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
