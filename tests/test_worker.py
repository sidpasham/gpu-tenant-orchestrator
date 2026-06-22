import asyncio

from src.temporal import worker


class FakeKafkaError:
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code


class FakeMessage:
    def __init__(self, value=b"", key=b"team-a", error=None):
        self._value = value
        self._key = key
        self._error = error

    def error(self):
        return self._error

    def value(self):
        return self._value

    def key(self):
        return self._key


class FakeConsumer:
    def __init__(self, messages):
        self.messages = list(messages)
        self.commits = []

    def poll(self, _timeout):
        if not self.messages:
            return None
        return self.messages.pop(0)

    def commit(self, message, asynchronous):
        self.commits.append({"message": message, "asynchronous": asynchronous})


class FakeProducer:
    def __init__(self):
        self.records = []
        self.flush_calls = []

    def produce(self, topic, key, value):
        self.records.append({"topic": topic, "key": key, "value": value})

    def flush(self, timeout):
        self.flush_calls.append(timeout)


class FakeTopicFuture:
    def __init__(self, error=None):
        self.error = error

    def result(self, timeout):
        if self.error is not None:
            raise self.error
        return None


class FakeAdmin:
    def __init__(self, futures_by_topic=None):
        self.created_topics = []
        self.futures_by_topic = futures_by_topic or {}

    def create_topics(self, topics):
        self.created_topics = [
            {
                "topic": topic.topic,
                "partitions": topic.num_partitions,
                "replication_factor": topic.replication_factor,
            }
            for topic in topics
        ]
        return {
            topic["topic"]: self.futures_by_topic.get(
                topic["topic"], FakeTopicFuture()
            )
            for topic in self.created_topics
        }


def test_is_retryable_kafka_error_accepts_startup_and_empty_partition_errors():
    assert worker.is_retryable_kafka_error(
        FakeKafkaError(worker.KafkaError.UNKNOWN_TOPIC_OR_PART)
    )
    assert worker.is_retryable_kafka_error(
        FakeKafkaError(worker.KafkaError.LEADER_NOT_AVAILABLE)
    )
    assert worker.is_retryable_kafka_error(
        FakeKafkaError(worker.KafkaError._PARTITION_EOF)
    )


def test_is_retryable_kafka_error_rejects_non_retryable_errors():
    assert not worker.is_retryable_kafka_error(
        FakeKafkaError(worker.KafkaError._FAIL)
    )


def test_worker_metrics_include_health_and_dlq_counters():
    state = worker.WorkerHealthState()
    state.set_kafka_consumer_running(True)
    state.set_temporal_worker_running(True)
    state.record_consumed_message()
    state.record_customer_allocation("team-a", "premium")
    state.record_customer_allocation_completion("team-a", "premium", "success", 1.25)
    state.record_kafka_dispatcher_failure()
    state.record_temporal_connection_failure("temporal down")
    state.record_invalid_message()
    state.record_dlq_message()

    metrics = worker.render_worker_metrics(state)

    assert "gpu_tenant_worker_kafka_consumer_running 1" in metrics
    assert "gpu_tenant_worker_temporal_worker_running 1" in metrics
    assert "gpu_tenant_worker_messages_consumed_total 1" in metrics
    assert "gpu_tenant_worker_kafka_dispatcher_failures_total 1" in metrics
    assert "gpu_tenant_worker_temporal_connection_failures_total 1" in metrics
    assert "gpu_tenant_worker_invalid_messages_total 1" in metrics
    assert "gpu_tenant_worker_dlq_messages_total 1" in metrics
    assert (
        'gpu_tenant_worker_customer_allocations_total'
        '{customer_id="team-a",tier="premium"} 1'
    ) in metrics
    assert (
        'gpu_tenant_worker_customer_allocation_gpu_count'
        '{customer_id="team-a",tier="premium"} 2'
    ) in metrics
    assert (
        'gpu_tenant_worker_customer_allocation_duration_seconds'
        '{customer_id="team-a",tier="premium",status="success"} 1.250000'
    ) in metrics
    assert (
        'gpu_tenant_worker_customer_allocations_completed_total'
        '{customer_id="team-a",tier="premium",status="success"} 1'
    ) in metrics


def test_connect_temporal_with_retry_records_failures(monkeypatch):
    attempts = []
    sleeps = []
    state = worker.WorkerHealthState()

    async def fake_connect(host):
        attempts.append(host)
        if len(attempts) == 1:
            raise RuntimeError("temporal unavailable")
        return "temporal-client"

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(worker.asyncio, "sleep", fake_sleep)

    result = asyncio.run(
        worker.connect_temporal_with_retry(
            state,
            connect=fake_connect,
            temporal_host="temporal:7233",
            retry_seconds=3,
        )
    )

    assert result == "temporal-client"
    assert attempts == ["temporal:7233", "temporal:7233"]
    assert sleeps == [3]
    assert state.snapshot()["temporal_connection_failures_total"] == 1
    assert state.snapshot()["last_temporal_error"] == ""


def test_dispatcher_ensures_primary_and_dlq_topics():
    admin = FakeAdmin()
    dispatcher = worker.KafkaWorkflowDispatcher(
        temporal_client=None,
        temporal_loop=None,
        kafka_topic="gpu-allocations",
        dlq_topic="gpu-allocations-dlq",
        topic_partitions=3,
        topic_replication_factor=2,
    )

    dispatcher.ensure_topics(admin)

    assert admin.created_topics == [
        {
            "topic": "gpu-allocations",
            "partitions": 3,
            "replication_factor": 2,
        },
        {
            "topic": "gpu-allocations-dlq",
            "partitions": 3,
            "replication_factor": 2,
        },
    ]


def test_dispatcher_treats_existing_topics_as_ready():
    admin = FakeAdmin(
        {
            "gpu-allocations": FakeTopicFuture(RuntimeError("topic already exists")),
            "gpu-allocations-dlq": FakeTopicFuture(),
        }
    )
    dispatcher = worker.KafkaWorkflowDispatcher(
        temporal_client=None,
        temporal_loop=None,
        kafka_topic="gpu-allocations",
        dlq_topic="gpu-allocations-dlq",
    )

    dispatcher.ensure_topics(admin)


def test_dispatcher_retries_kafka_initialization(monkeypatch):
    state = worker.WorkerHealthState()

    class RetryDispatcher(worker.KafkaWorkflowDispatcher):
        def __init__(self):
            super().__init__(
                temporal_client=None,
                temporal_loop=None,
                state=state,
                startup_retry_seconds=0,
            )
            self.initialize_calls = 0

        def initialize_kafka_clients(self):
            self.initialize_calls += 1
            if self.initialize_calls == 1:
                raise RuntimeError("kafka unavailable")
            self.stop()

    monkeypatch.setattr(worker.time, "sleep", lambda _seconds: None)
    dispatcher = RetryDispatcher()

    dispatcher.run_forever()

    assert dispatcher.initialize_calls == 2
    assert state.snapshot()["kafka_dispatcher_failures_total"] == 1
    assert state.snapshot()["last_kafka_error"] == "kafka unavailable"


def test_dispatcher_routes_invalid_messages_to_dlq_and_commits():
    state = worker.WorkerHealthState()
    consumer = FakeConsumer([FakeMessage(value=b"not-json")])
    producer = FakeProducer()
    dispatcher = worker.KafkaWorkflowDispatcher(
        temporal_client=None,
        temporal_loop=None,
        state=state,
        kafka_topic="gpu-allocations",
        dlq_topic="gpu-allocations-dlq",
    )
    dispatcher.consumer = consumer
    dispatcher.dlq_producer = producer

    dispatcher.poll_once()

    assert len(producer.records) == 1
    assert producer.records[0]["topic"] == "gpu-allocations-dlq"
    assert '"reason": "invalid_message"' in producer.records[0]["value"]
    assert len(consumer.commits) == 1
    assert consumer.commits[0]["asynchronous"] is False
    assert state.snapshot()["invalid_messages_total"] == 1
    assert state.snapshot()["dlq_messages_total"] == 1


def test_dispatcher_records_customer_allocation_after_workflow_start(monkeypatch):
    state = worker.WorkerHealthState()
    consumer = FakeConsumer(
        [FakeMessage(value=b'{"customer_id":"team-a","tier":"premium"}')]
    )
    dispatcher = worker.KafkaWorkflowDispatcher(
        temporal_client=None,
        temporal_loop=None,
        state=state,
        kafka_topic="gpu-allocations",
        dlq_topic="gpu-allocations-dlq",
    )
    dispatcher.consumer = consumer
    dispatcher.dlq_producer = FakeProducer()
    monkeypatch.setattr(dispatcher, "start_workflow", lambda _data: None)

    dispatcher.poll_once()

    snapshot = state.snapshot()
    assert snapshot["messages_consumed_total"] == 1
    assert snapshot["customer_allocations"] == {("team-a", "premium"): 1}
    assert snapshot["customer_allocation_gpu_counts"] == {("team-a", "premium"): 2}
    assert set(snapshot["customer_allocation_durations"]) == {
        ("team-a", "premium", "success")
    }
    assert snapshot["customer_allocation_completions"] == {
        ("team-a", "premium", "success"): 1
    }
    assert len(consumer.commits) == 1


def test_dispatcher_routes_workflow_start_failures_to_dlq_and_commits(monkeypatch):
    state = worker.WorkerHealthState()
    consumer = FakeConsumer(
        [FakeMessage(value=b'{"customer_id":"team-a","tier":"premium"}')]
    )
    producer = FakeProducer()
    dispatcher = worker.KafkaWorkflowDispatcher(
        temporal_client=None,
        temporal_loop=None,
        state=state,
        kafka_topic="gpu-allocations",
        dlq_topic="gpu-allocations-dlq",
    )
    dispatcher.consumer = consumer
    dispatcher.dlq_producer = producer
    monkeypatch.setattr(
        dispatcher,
        "start_workflow",
        lambda _data: (_ for _ in ()).throw(RuntimeError("temporal unavailable")),
    )

    dispatcher.poll_once()

    assert len(producer.records) == 1
    assert '"reason": "workflow_start_failed"' in producer.records[0]["value"]
    snapshot = state.snapshot()
    assert snapshot["workflow_start_failures_total"] == 1
    assert snapshot["dlq_messages_total"] == 1
    assert set(snapshot["customer_allocation_durations"]) == {
        ("team-a", "premium", "failure")
    }
    assert snapshot["customer_allocation_completions"] == {
        ("team-a", "premium", "failure"): 1
    }
    assert len(consumer.commits) == 1
