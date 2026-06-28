from src.placement.scheduler import (
    ACTIVE_STATUS,
    GpuPlacementScheduler,
    GpuPool,
    PENDING_CAPACITY_STATUS,
    PlacementRequest,
    PostgresReservationStore,
    build_reservation_store,
)


def make_request(
    customer_id="team-a",
    gpu_count=2,
    preferred_region="us-phoenix-1",
    allowed_regions=("us-phoenix-1",),
    max_latency_ms=100,
    allocation_id=None,
):
    return PlacementRequest(
        customer_id=customer_id,
        tier="premium",
        gpu_count=gpu_count,
        gpu_type="mock",
        preferred_region=preferred_region,
        allowed_regions=allowed_regions,
        max_latency_ms=max_latency_ms,
        allocation_id=allocation_id,
    )


def make_scheduler():
    return GpuPlacementScheduler(
        [
            GpuPool(
                pool_id="phoenix",
                region="us-phoenix-1",
                cluster="cluster-a",
                gpu_type="mock",
                total_gpus=2,
                priority=100,
                latency_ms_by_region={"us-phoenix-1": 5},
            ),
            GpuPool(
                pool_id="ashburn",
                region="us-ashburn-1",
                cluster="cluster-b",
                gpu_type="mock",
                total_gpus=4,
                priority=80,
                latency_ms_by_region={"us-phoenix-1": 64},
            ),
        ]
    )


def test_scheduler_reserves_preferred_region_capacity_first():
    scheduler = make_scheduler()

    decision = scheduler.reserve(make_request())

    assert decision["status"] == "RESERVED"
    assert decision["assigned_region"] == "us-phoenix-1"
    assert decision["assigned_cluster"] == "cluster-a"
    assert decision["gpu_pool_id"] == "phoenix"
    assert decision["gpu_count"] == 2


def test_scheduler_uses_allowed_fallback_region_when_preferred_pool_is_full():
    scheduler = make_scheduler()

    scheduler.reserve(make_request(customer_id="team-a"))
    decision = scheduler.reserve(
        make_request(
            customer_id="team-b",
            allowed_regions=("us-phoenix-1", "us-ashburn-1"),
        )
    )

    assert decision["status"] == "RESERVED"
    assert decision["assigned_region"] == "us-ashburn-1"
    assert decision["latency_ms"] == 64


def test_scheduler_returns_pending_capacity_when_allowed_pools_are_full():
    scheduler = make_scheduler()

    scheduler.reserve(make_request(customer_id="team-a"))
    decision = scheduler.reserve(make_request(customer_id="team-b"))

    assert decision["status"] == PENDING_CAPACITY_STATUS
    assert decision["reason"] == (
        "Matching GPU pools exist, but none have enough free GPUs right now."
    )


def test_scheduler_respects_latency_budget_before_using_fallback_region():
    scheduler = make_scheduler()

    scheduler.reserve(make_request(customer_id="team-a"))
    decision = scheduler.reserve(
        make_request(
            customer_id="team-b",
            allowed_regions=("us-phoenix-1", "us-ashburn-1"),
            max_latency_ms=20,
        )
    )

    assert decision["status"] == PENDING_CAPACITY_STATUS
    assert decision["allowed_regions"] == ["us-phoenix-1", "us-ashburn-1"]


def test_scheduler_release_frees_capacity_for_later_requests():
    scheduler = make_scheduler()

    first_decision = scheduler.reserve(make_request(customer_id="team-a"))
    assert scheduler.release(first_decision["reservation_id"])
    second_decision = scheduler.reserve(make_request(customer_id="team-b"))

    assert second_decision["status"] == "RESERVED"
    assert second_decision["gpu_pool_id"] == "phoenix"


def test_scheduler_can_mark_reservation_active():
    scheduler = make_scheduler()

    decision = scheduler.reserve(make_request(customer_id="team-a"))

    assert scheduler.mark_active(decision["reservation_id"])
    assert (
        scheduler.reservation_store.get(decision["reservation_id"]).status
        == ACTIVE_STATUS
    )


def test_scheduler_uses_allocation_id_for_reservation_identity():
    scheduler = make_scheduler()

    decision = scheduler.reserve(make_request(allocation_id="alloc-123"))

    assert decision["reservation_id"] == "resv-alloc-123"


class FakePostgresDatabase:
    def __init__(self):
        self.reservations = {}
        self.pool_locks = set()
        self.executed_queries = []

    def connection_factory(self):
        return FakePostgresConnection(self)

    def reservation_row(self, reservation_id):
        reservation = self.reservations.get(reservation_id)
        if reservation is None:
            return None
        return (
            reservation["reservation_id"],
            reservation["customer_id"],
            reservation["tier"],
            reservation["pool_id"],
            reservation["region"],
            reservation["cluster"],
            reservation["gpu_type"],
            reservation["gpu_count"],
            reservation["latency_ms"],
            reservation["status"],
        )


class FakePostgresConnection:
    def __init__(self, database):
        self.database = database

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self):
        return FakePostgresCursor(self.database)

    def transaction(self):
        return self


class FakePostgresCursor:
    def __init__(self, database):
        self.database = database
        self._row = None
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params=None):
        normalized_query = " ".join(query.split())
        self.database.executed_queries.append(normalized_query)
        params = params or ()
        self._row = None
        self._rows = []

        if normalized_query.startswith("CREATE ") or normalized_query.startswith(
            "CREATE INDEX"
        ):
            return

        if (
            "SELECT reservation_id," in normalized_query
            and "FROM gpu_reservations" in normalized_query
            and "WHERE reservation_id = %s" in normalized_query
        ):
            self._row = self.database.reservation_row(params[0])
            return

        if normalized_query.startswith("INSERT INTO gpu_pool_locks"):
            self.database.pool_locks.add(params[0])
            return

        if (
            normalized_query.startswith("SELECT pool_id FROM gpu_pool_locks")
            and "FOR UPDATE" in normalized_query
        ):
            self._row = (params[0],)
            return

        if normalized_query.startswith("SELECT COALESCE(SUM(gpu_count), 0)"):
            pool_id = params[0]
            self._row = (
                sum(
                    reservation["gpu_count"]
                    for reservation in self.database.reservations.values()
                    if reservation["pool_id"] == pool_id
                    and reservation["status"] in {"RESERVED", "ACTIVE"}
                ),
            )
            return

        if normalized_query.startswith("INSERT INTO gpu_reservations"):
            reservation = {
                "reservation_id": params[0],
                "customer_id": params[1],
                "tier": params[2],
                "pool_id": params[3],
                "region": params[4],
                "cluster": params[5],
                "gpu_type": params[6],
                "gpu_count": params[7],
                "latency_ms": params[8],
                "status": params[9],
            }
            self.database.reservations[params[0]] = reservation
            self._row = self.database.reservation_row(params[0])
            return

        if normalized_query.startswith("SELECT pool_id, COALESCE(SUM(gpu_count), 0)"):
            reserved_by_pool = {}
            for reservation in self.database.reservations.values():
                if reservation["status"] not in {"RESERVED", "ACTIVE"}:
                    continue
                reserved_by_pool[reservation["pool_id"]] = (
                    reserved_by_pool.get(reservation["pool_id"], 0)
                    + reservation["gpu_count"]
                )
            self._rows = list(reserved_by_pool.items())
            return

        if normalized_query.startswith("UPDATE gpu_reservations"):
            status, reservation_id = params
            reservation = self.database.reservations.get(reservation_id)
            if reservation is not None:
                reservation["status"] = status
                self._row = (reservation_id,)
            return

        raise AssertionError(f"Unexpected SQL: {normalized_query}")

    def fetchone(self):
        return self._row

    def fetchall(self):
        return self._rows


def test_build_reservation_store_can_create_postgres_store():
    store = build_reservation_store("postgres", "postgresql://example")

    assert isinstance(store, PostgresReservationStore)


def test_postgres_store_reserves_with_pool_row_lock():
    database = FakePostgresDatabase()
    store = PostgresReservationStore(
        "postgresql://example",
        connection_factory=database.connection_factory,
    )
    pool = make_scheduler().pools[0]

    reservation = store.reserve(make_request(allocation_id="alloc-123"), pool)

    assert reservation.reservation_id == "resv-alloc-123"
    assert reservation.pool_id == "phoenix"
    assert reservation.status == "RESERVED"
    assert database.pool_locks == {"phoenix"}
    assert any("gpu_pool_locks" in query and "FOR UPDATE" in query for query in database.executed_queries)


def test_postgres_store_capacity_check_blocks_overbooking():
    database = FakePostgresDatabase()
    store = PostgresReservationStore(
        "postgresql://example",
        connection_factory=database.connection_factory,
    )
    pool = make_scheduler().pools[0]

    store.reserve(make_request(customer_id="team-a", allocation_id="alloc-a"), pool)

    try:
        store.reserve(make_request(customer_id="team-b", allocation_id="alloc-b"), pool)
        assert False, "expected capacity failure"
    except ValueError as exc:
        assert "free capacity" in str(exc)


def test_postgres_store_updates_reservation_status_and_totals():
    database = FakePostgresDatabase()
    store = PostgresReservationStore(
        "postgresql://example",
        connection_factory=database.connection_factory,
    )
    pool = make_scheduler().pools[0]
    reservation = store.reserve(make_request(allocation_id="alloc-123"), pool)

    assert store.reserved_gpus_by_pool() == {"phoenix": 2}
    assert store.mark_active(reservation.reservation_id)
    assert store.get(reservation.reservation_id).status == ACTIVE_STATUS
    assert store.release(reservation.reservation_id)
    assert store.reserved_gpus_by_pool() == {}
