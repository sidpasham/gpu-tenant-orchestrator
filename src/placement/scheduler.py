import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

import yaml


RESERVED_STATUS = "RESERVED"
PENDING_CAPACITY_STATUS = "PENDING_CAPACITY"
RELEASED_STATUS = "RELEASED"
ACTIVE_STATUS = "ACTIVE"
UNKNOWN_LATENCY_MS = 1_000_000
ACTIVE_RESERVATION_STATUSES = {RESERVED_STATUS, ACTIVE_STATUS}
RESERVATION_COLUMNS = """
    reservation_id,
    customer_id,
    tier,
    pool_id,
    region,
    cluster,
    gpu_type,
    gpu_count,
    latency_ms,
    status
"""


class ReservationStore(Protocol):
    def get(self, reservation_id: str) -> "CapacityReservation | None":
        ...

    def reserve(
        self,
        request: "PlacementRequest",
        pool: "GpuPool",
    ) -> "CapacityReservation":
        ...

    def mark_active(self, reservation_id: str) -> bool:
        ...

    def release(self, reservation_id: str) -> bool:
        ...

    def reserved_gpus_by_pool(self) -> dict[str, int]:
        ...

    @staticmethod
    def reservation_id_for(request: "PlacementRequest") -> str:
        ...


@dataclass(frozen=True)
class GpuPool:
    pool_id: str
    region: str
    cluster: str
    gpu_type: str
    total_gpus: int
    healthy: bool = True
    priority: int = 0
    latency_ms_by_region: dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, raw_pool: dict[str, Any]) -> "GpuPool":
        required_fields = ["pool_id", "region", "cluster", "gpu_type", "total_gpus"]
        missing_fields = [
            field_name for field_name in required_fields if field_name not in raw_pool
        ]
        if missing_fields:
            raise ValueError(
                "GPU pool is missing required fields: "
                + ", ".join(sorted(missing_fields))
            )

        total_gpus = int(raw_pool["total_gpus"])
        if total_gpus < 0:
            raise ValueError("GPU pool total_gpus must be non-negative")

        latency_ms_by_region = {
            str(region).lower(): int(latency_ms)
            for region, latency_ms in raw_pool.get(
                "latency_ms_by_region", {}
            ).items()
        }

        return cls(
            pool_id=str(raw_pool["pool_id"]).lower(),
            region=str(raw_pool["region"]).lower(),
            cluster=str(raw_pool["cluster"]).lower(),
            gpu_type=str(raw_pool["gpu_type"]).lower(),
            total_gpus=total_gpus,
            healthy=bool(raw_pool.get("healthy", True)),
            priority=int(raw_pool.get("priority", 0)),
            latency_ms_by_region=latency_ms_by_region,
        )

    def latency_to(self, customer_region: str) -> int:
        normalized_region = customer_region.lower()
        if normalized_region in self.latency_ms_by_region:
            return self.latency_ms_by_region[normalized_region]
        if normalized_region == self.region:
            return 5
        return UNKNOWN_LATENCY_MS


@dataclass(frozen=True)
class PlacementRequest:
    customer_id: str
    tier: str
    gpu_count: int
    gpu_type: str
    preferred_region: str
    allowed_regions: tuple[str, ...]
    max_latency_ms: int
    allocation_id: str | None = None


@dataclass
class CapacityReservation:
    reservation_id: str
    customer_id: str
    tier: str
    pool_id: str
    region: str
    cluster: str
    gpu_type: str
    gpu_count: int
    latency_ms: int
    status: str = RESERVED_STATUS

    def to_decision(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reservation_id": self.reservation_id,
            "customer_id": self.customer_id,
            "tier": self.tier,
            "gpu_count": self.gpu_count,
            "gpu_type": self.gpu_type,
            "assigned_region": self.region,
            "assigned_cluster": self.cluster,
            "gpu_pool_id": self.pool_id,
            "latency_ms": self.latency_ms,
        }

    @classmethod
    def from_row(cls, row: tuple[Any, ...]) -> "CapacityReservation":
        return cls(
            reservation_id=str(row[0]),
            customer_id=str(row[1]),
            tier=str(row[2]),
            pool_id=str(row[3]),
            region=str(row[4]),
            cluster=str(row[5]),
            gpu_type=str(row[6]),
            gpu_count=int(row[7]),
            latency_ms=int(row[8]),
            status=str(row[9]),
        )


class InMemoryReservationStore:
    def __init__(self) -> None:
        self._reservations: dict[str, CapacityReservation] = {}
        self._lock = threading.Lock()

    def get(self, reservation_id: str) -> CapacityReservation | None:
        with self._lock:
            return self._reservations.get(reservation_id)

    def reserve(
        self,
        request: PlacementRequest,
        pool: GpuPool,
    ) -> CapacityReservation:
        reservation_id = self.reservation_id_for(request)
        with self._lock:
            existing_reservation = self._reservations.get(reservation_id)
            if (
                existing_reservation
                and existing_reservation.status in ACTIVE_RESERVATION_STATUSES
            ):
                return existing_reservation

            used_gpus = sum(
                reservation.gpu_count
                for reservation in self._reservations.values()
                if reservation.pool_id == pool.pool_id
                and reservation.status in ACTIVE_RESERVATION_STATUSES
            )
            if pool.total_gpus - used_gpus < request.gpu_count:
                raise ValueError("GPU pool no longer has enough free capacity")

            reservation = CapacityReservation(
                reservation_id=reservation_id,
                customer_id=request.customer_id,
                tier=request.tier,
                pool_id=pool.pool_id,
                region=pool.region,
                cluster=pool.cluster,
                gpu_type=pool.gpu_type,
                gpu_count=request.gpu_count,
                latency_ms=pool.latency_to(request.preferred_region),
            )
            self._reservations[reservation_id] = reservation
            return reservation

    def mark_active(self, reservation_id: str) -> bool:
        with self._lock:
            reservation = self._reservations.get(reservation_id)
            if reservation is None:
                return False
            reservation.status = ACTIVE_STATUS
            return True

    def release(self, reservation_id: str) -> bool:
        with self._lock:
            reservation = self._reservations.get(reservation_id)
            if reservation is None:
                return False
            reservation.status = RELEASED_STATUS
            return True

    def reserved_gpus_by_pool(self) -> dict[str, int]:
        with self._lock:
            reserved_gpus: dict[str, int] = {}
            for reservation in self._reservations.values():
                if reservation.status not in {RESERVED_STATUS, ACTIVE_STATUS}:
                    continue
                reserved_gpus[reservation.pool_id] = (
                    reserved_gpus.get(reservation.pool_id, 0)
                    + reservation.gpu_count
                )
            return reserved_gpus

    @staticmethod
    def reservation_id_for(request: PlacementRequest) -> str:
        if request.allocation_id:
            return f"resv-{request.allocation_id}"
        return (
            f"resv-{request.customer_id}-{request.tier}-"
            f"{request.gpu_type}-{request.gpu_count}"
        )


class PostgresReservationStore:
    def __init__(
        self,
        database_url: str,
        connection_factory: Callable[[], Any] | None = None,
        initialize_schema: bool = True,
    ) -> None:
        if not database_url and connection_factory is None:
            raise ValueError("database_url is required for postgres reservation store")
        self.database_url = database_url
        self.connection_factory = connection_factory or self._connect
        self.initialize_schema = initialize_schema
        self._initialized = False
        self._init_lock = threading.Lock()

    def _connect(self):
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError(
                "Postgres reservation store requires psycopg. "
                "Install worker dependencies from requirements-worker.txt."
            ) from exc
        return psycopg.connect(self.database_url)

    def initialize(self) -> None:
        if not self.initialize_schema or self._initialized:
            return

        with self._init_lock:
            if self._initialized:
                return
            with self.connection_factory() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS gpu_pool_locks (
                            pool_id TEXT PRIMARY KEY
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS gpu_reservations (
                            reservation_id TEXT PRIMARY KEY,
                            customer_id TEXT NOT NULL,
                            tier TEXT NOT NULL,
                            pool_id TEXT NOT NULL,
                            region TEXT NOT NULL,
                            cluster TEXT NOT NULL,
                            gpu_type TEXT NOT NULL,
                            gpu_count INTEGER NOT NULL CHECK (gpu_count > 0),
                            latency_ms INTEGER NOT NULL CHECK (latency_ms >= 0),
                            status TEXT NOT NULL CHECK (
                                status IN (
                                    'RESERVED',
                                    'ACTIVE',
                                    'RELEASED',
                                    'PENDING_CAPACITY'
                                )
                            ),
                            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                        )
                        """
                    )
                    cursor.execute(
                        """
                        CREATE INDEX IF NOT EXISTS
                            gpu_reservations_pool_status_idx
                        ON gpu_reservations (pool_id, status)
                        """
                    )
            self._initialized = True

    def get(self, reservation_id: str) -> CapacityReservation | None:
        self.initialize()
        with self.connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT {RESERVATION_COLUMNS}
                    FROM gpu_reservations
                    WHERE reservation_id = %s
                    """,
                    (reservation_id,),
                )
                row = cursor.fetchone()
        return CapacityReservation.from_row(row) if row else None

    def reserve(
        self,
        request: PlacementRequest,
        pool: GpuPool,
    ) -> CapacityReservation:
        self.initialize()
        reservation_id = self.reservation_id_for(request)
        with self.connection_factory() as connection:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        SELECT {RESERVATION_COLUMNS}
                        FROM gpu_reservations
                        WHERE reservation_id = %s
                        FOR UPDATE
                        """,
                        (reservation_id,),
                    )
                    existing_row = cursor.fetchone()
                    if existing_row:
                        existing_reservation = CapacityReservation.from_row(
                            existing_row
                        )
                        if existing_reservation.status in ACTIVE_RESERVATION_STATUSES:
                            return existing_reservation

                    cursor.execute(
                        """
                        INSERT INTO gpu_pool_locks (pool_id)
                        VALUES (%s)
                        ON CONFLICT (pool_id) DO NOTHING
                        """,
                        (pool.pool_id,),
                    )
                    cursor.execute(
                        """
                        SELECT pool_id
                        FROM gpu_pool_locks
                        WHERE pool_id = %s
                        FOR UPDATE
                        """,
                        (pool.pool_id,),
                    )
                    cursor.fetchone()

                    cursor.execute(
                        """
                        SELECT COALESCE(SUM(gpu_count), 0)
                        FROM gpu_reservations
                        WHERE pool_id = %s AND status IN (%s, %s)
                        """,
                        (pool.pool_id, RESERVED_STATUS, ACTIVE_STATUS),
                    )
                    used_gpus_row = cursor.fetchone()
                    used_gpus = int(used_gpus_row[0] if used_gpus_row else 0)
                    if pool.total_gpus - used_gpus < request.gpu_count:
                        raise ValueError("GPU pool no longer has enough free capacity")

                    cursor.execute(
                        f"""
                        INSERT INTO gpu_reservations (
                            reservation_id,
                            customer_id,
                            tier,
                            pool_id,
                            region,
                            cluster,
                            gpu_type,
                            gpu_count,
                            latency_ms,
                            status
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (reservation_id) DO UPDATE SET
                            customer_id = EXCLUDED.customer_id,
                            tier = EXCLUDED.tier,
                            pool_id = EXCLUDED.pool_id,
                            region = EXCLUDED.region,
                            cluster = EXCLUDED.cluster,
                            gpu_type = EXCLUDED.gpu_type,
                            gpu_count = EXCLUDED.gpu_count,
                            latency_ms = EXCLUDED.latency_ms,
                            status = EXCLUDED.status,
                            updated_at = now()
                        RETURNING {RESERVATION_COLUMNS}
                        """,
                        (
                            reservation_id,
                            request.customer_id,
                            request.tier,
                            pool.pool_id,
                            pool.region,
                            pool.cluster,
                            pool.gpu_type,
                            request.gpu_count,
                            pool.latency_to(request.preferred_region),
                            RESERVED_STATUS,
                        ),
                    )
                    reservation_row = cursor.fetchone()
                    if not reservation_row:
                        raise RuntimeError("Postgres did not return reservation row")
                    return CapacityReservation.from_row(reservation_row)

    def mark_active(self, reservation_id: str) -> bool:
        return self._update_status(reservation_id, ACTIVE_STATUS)

    def release(self, reservation_id: str) -> bool:
        return self._update_status(reservation_id, RELEASED_STATUS)

    def reserved_gpus_by_pool(self) -> dict[str, int]:
        self.initialize()
        with self.connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT pool_id, COALESCE(SUM(gpu_count), 0)
                    FROM gpu_reservations
                    WHERE status IN (%s, %s)
                    GROUP BY pool_id
                    """,
                    (RESERVED_STATUS, ACTIVE_STATUS),
                )
                rows = cursor.fetchall()
        return {str(pool_id): int(gpu_count) for pool_id, gpu_count in rows}

    def _update_status(self, reservation_id: str, status: str) -> bool:
        self.initialize()
        with self.connection_factory() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE gpu_reservations
                    SET status = %s, updated_at = now()
                    WHERE reservation_id = %s
                    RETURNING reservation_id
                    """,
                    (status, reservation_id),
                )
                return cursor.fetchone() is not None

    @staticmethod
    def reservation_id_for(request: PlacementRequest) -> str:
        return InMemoryReservationStore.reservation_id_for(request)


class GpuPlacementScheduler:
    def __init__(
        self,
        pools: list[GpuPool],
        reservation_store: ReservationStore | None = None,
    ) -> None:
        if not pools:
            raise ValueError("At least one GPU pool must be configured")
        self.pools = pools
        self.reservation_store = reservation_store or InMemoryReservationStore()

    def reserve(self, request: PlacementRequest) -> dict[str, Any]:
        existing_reservation = self.reservation_store.get(
            self.reservation_store.reservation_id_for(request)
        )
        if (
            existing_reservation
            and existing_reservation.status in ACTIVE_RESERVATION_STATUSES
        ):
            return existing_reservation.to_decision()

        candidate_pools = self._candidate_pools(request)
        if not candidate_pools:
            return self._pending_decision(
                request,
                "No healthy GPU pools match the requested type, region policy, and latency budget.",
            )

        reserved_gpus_by_pool = self.reservation_store.reserved_gpus_by_pool()
        for pool in self._rank_pools(request, candidate_pools, reserved_gpus_by_pool):
            free_gpus = pool.total_gpus - reserved_gpus_by_pool.get(pool.pool_id, 0)
            if free_gpus < request.gpu_count:
                continue
            try:
                reservation = self.reservation_store.reserve(request, pool)
                return reservation.to_decision()
            except ValueError:
                continue

        return self._pending_decision(
            request,
            "Matching GPU pools exist, but none have enough free GPUs right now.",
        )

    def mark_active(self, reservation_id: str) -> bool:
        return self.reservation_store.mark_active(reservation_id)

    def release(self, reservation_id: str) -> bool:
        return self.reservation_store.release(reservation_id)

    def _candidate_pools(self, request: PlacementRequest) -> list[GpuPool]:
        allowed_regions = set(request.allowed_regions)
        return [
            pool
            for pool in self.pools
            if pool.healthy
            and pool.gpu_type == request.gpu_type
            and pool.region in allowed_regions
            and pool.latency_to(request.preferred_region) <= request.max_latency_ms
        ]

    def _rank_pools(
        self,
        request: PlacementRequest,
        candidate_pools: list[GpuPool],
        reserved_gpus_by_pool: dict[str, int],
    ) -> list[GpuPool]:
        def pool_score(pool: GpuPool) -> tuple[int, int, int, int, str]:
            free_gpus = pool.total_gpus - reserved_gpus_by_pool.get(pool.pool_id, 0)
            leftover_after_request = free_gpus - request.gpu_count
            preferred_penalty = 0 if pool.region == request.preferred_region else 1
            return (
                preferred_penalty,
                pool.latency_to(request.preferred_region),
                leftover_after_request,
                -pool.priority,
                pool.pool_id,
            )

        return sorted(candidate_pools, key=pool_score)

    @staticmethod
    def _pending_decision(request: PlacementRequest, reason: str) -> dict[str, Any]:
        return {
            "status": PENDING_CAPACITY_STATUS,
            "reason": reason,
            "customer_id": request.customer_id,
            "tier": request.tier,
            "gpu_count": request.gpu_count,
            "gpu_type": request.gpu_type,
            "preferred_region": request.preferred_region,
            "allowed_regions": list(request.allowed_regions),
            "max_latency_ms": request.max_latency_ms,
        }


def load_gpu_pools(config_text: str) -> list[GpuPool]:
    try:
        raw_config = yaml.safe_load(config_text)
    except yaml.YAMLError as exc:
        raise ValueError(f"GPU pool config must be valid YAML or JSON: {exc}") from exc

    if not isinstance(raw_config, list):
        raise ValueError("GPU pool config must be a list of pool objects")

    pools = []
    pool_ids = set()
    for raw_pool in raw_config:
        if not isinstance(raw_pool, dict):
            raise ValueError("Each GPU pool config entry must be an object")
        pool = GpuPool.from_mapping(raw_pool)
        if pool.pool_id in pool_ids:
            raise ValueError(f"Duplicate GPU pool id: {pool.pool_id}")
        pool_ids.add(pool.pool_id)
        pools.append(pool)
    return pools


def build_reservation_store(
    backend: str,
    database_url: str = "",
) -> ReservationStore:
    normalized_backend = backend.lower()
    if normalized_backend == "memory":
        return InMemoryReservationStore()
    if normalized_backend == "postgres":
        return PostgresReservationStore(database_url)
    raise ValueError("PLACEMENT_STORE_BACKEND must be 'memory' or 'postgres'")
