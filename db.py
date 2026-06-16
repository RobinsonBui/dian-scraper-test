"""Postgres-backed job store for dian-scraper-test.

Used only when DB_MODE=postgres. The legacy in-memory RunState is still
available behind the same interface so we can roll the new backend out
without touching every caller in one step.

Design:

  - One pool, one process. asyncpg pool sized for the monolith (1 API
    + 1 worker). When we split into separate containers in Phase 2 the
    pool sizing can be tuned per container without changing the API.

  - Errors propagate. asyncpg already retries transient connection
    drops; anything that bubbles out of here ends as a 500 on the API
    side or as `status=failed` on the worker side. We don't wrap them
    in custom exception types — the call sites just need to know
    whether it worked, not the granular cause.

  - SQL lives here, callers don't write SQL. Centralizing the queries
    makes auditing tables easy (`grep INSERT db.py`) and prevents
    N+1 queries hidden in handlers.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, Optional

# `asyncpg` is only needed when DB_MODE=postgres. The InMemoryJobBackend
# (legacy default) must be able to import the dataclasses below without
# requiring the driver to be installed. Deferring the runtime import to
# the methods that actually open a connection keeps the legacy code path
# zero-deps from this module.
if TYPE_CHECKING:
    import asyncpg

logger = logging.getLogger("dian-scraper.db")


# ──────────────────────────────────────────────────────────────────────────
# DTOs returned by the store. Kept dataclasses (not pydantic) because the
# rows are internal to the scraper — pydantic adds overhead for fields we
# don't validate. The API converts these to JSON via dict(asdict(...)).
# ──────────────────────────────────────────────────────────────────────────


@dataclass
class JobRow:
    """A row out of the `jobs` table.

    Mirrors the schema 1:1. Optional fields are Optional[...] so the
    type checker catches accidental None usage. Dates and timestamps
    are kept as native Python types so callers can format them however
    they need (ISO for JSON, native for arithmetic).
    """

    id: str
    company_id: Optional[str]
    status: str
    auth_url: str
    start_date: date
    end_date: date
    max_invoices: int
    worker_id: Optional[str]
    worker_heartbeat: Optional[datetime]
    created_at: datetime
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    summary: Optional[dict[str, Any]]
    error: Optional[str]
    # Classified error kind from the engine. Stable string vocabulary
    # the consumer (NUVARA) maps to a context-aware UI message:
    # auth_expired / auth_expired_midrun / captcha_blocked / timeout /
    # engine_crash. None for successful runs and legacy rows.
    error_kind: Optional[str] = None


@dataclass
class JobFileRow:
    """A row out of the `job_files` table.

    The combination `r2_key is None` ↔ STORAGE_MODE=local is enforced
    at write time by the store. Readers should not have to defend
    against mixed-mode rows on the same job.
    """

    id: int
    job_id: str
    cufe: Optional[str]
    prefijo_folio: Optional[str]
    issuer_nit: Optional[str]
    issue_date: Optional[date]
    kind: str
    name: str
    size_bytes: int
    r2_key: Optional[str]
    r2_url: Optional[str]
    uploaded_at: datetime


@dataclass
class JobEventRow:
    """A row out of the `job_events` table.

    `event_index` is monotonic per job. NUVARA stores the highest seen
    value in its own `dian_scraper_alt_runs.scraperEventsCursor` field
    and passes it back as `since` on every poll.
    """

    job_id: str
    event_index: int
    source: str
    phase: str
    status: str
    message: Optional[str]
    payload: Optional[dict[str, Any]]
    occurred_at: datetime


# ──────────────────────────────────────────────────────────────────────────
# JobStore — the only public surface of this module.
# ──────────────────────────────────────────────────────────────────────────


class JobStore:
    """Async repository over the `jobs`, `job_files`, `job_events` tables.

    Lifetime: created once at FastAPI startup via `from_env`, closed at
    shutdown. All methods are coroutines and safe to call concurrently
    from any task — asyncpg manages connection checkout per call.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # ── construction / teardown ───────────────────────────────────────

    @classmethod
    async def from_env(cls) -> "JobStore":
        """Build a JobStore from the DATABASE_URL env var.

        Raises if the var is missing or unreachable so the container
        crash-loops instead of pretending to work and silently dropping
        writes.

        Import is deferred so the legacy in-memory backend can run
        without asyncpg installed at all.
        """
        import asyncpg  # noqa: PLC0415 — runtime-deferred on purpose

        dsn = os.environ.get("DATABASE_URL", "").strip()
        if not dsn:
            raise RuntimeError(
                "DB_MODE=postgres requires DATABASE_URL — got empty string"
            )

        # min=2 keeps a hot connection available for the API even when
        # the worker is busy; max=10 is plenty for 1 worker + occasional
        # NUVARA polls. Phase 2 will bump these per-container.
        pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=2,
            max_size=10,
            command_timeout=30,
            # init() is called once per connection — use it to install
            # the JSON codec so asyncpg returns dicts instead of strings.
            init=cls._init_connection,
        )
        logger.info("JobStore: postgres pool ready (min=2 max=10)")
        return cls(pool)

    @staticmethod
    async def _init_connection(conn: asyncpg.Connection) -> None:
        """Per-connection setup. Auto-decodes JSONB into Python dicts."""
        await conn.set_type_codec(
            "jsonb",
            encoder=json.dumps,
            decoder=json.loads,
            schema="pg_catalog",
        )

    async def close(self) -> None:
        await self._pool.close()
        logger.info("JobStore: pool closed")

    # ── jobs ──────────────────────────────────────────────────────────

    async def create_job(
        self,
        *,
        job_id: str,
        company_id: Optional[str],
        auth_url: str,
        start_date: date,
        end_date: date,
        max_invoices: int,
    ) -> JobRow:
        """Insert a fresh job in queued state. Idempotent on (id) PK
        — if the same id is replayed, raises asyncpg.UniqueViolationError
        and the caller can decide whether to surface 409 or treat as
        an already-accepted submission.
        """
        row = await self._pool.fetchrow(
            """
            INSERT INTO jobs (
                id, company_id, status, auth_url,
                start_date, end_date, max_invoices
            )
            VALUES ($1, $2, 'queued', $3, $4, $5, $6)
            RETURNING *
            """,
            job_id, company_id, auth_url, start_date, end_date, max_invoices,
        )
        return self._job_row(row)

    async def mark_running(
        self,
        *,
        job_id: str,
        worker_id: str,
    ) -> Optional[JobRow]:
        """Claim a queued job for a worker. Returns the row if the
        transition succeeded, None if the job was not queued (race
        with another worker, cancelled by user, or doesn't exist).

        The single UPDATE...WHERE status='queued' is the lock — no
        explicit SELECT FOR UPDATE needed because Postgres serializes
        the row-level update under READ COMMITTED.
        """
        row = await self._pool.fetchrow(
            """
            UPDATE jobs
            SET status = 'running',
                worker_id = $2,
                worker_heartbeat = now(),
                started_at = now()
            WHERE id = $1 AND status = 'queued'
            RETURNING *
            """,
            job_id, worker_id,
        )
        return self._job_row(row) if row else None

    async def heartbeat(self, *, job_id: str) -> bool:
        """Bump worker_heartbeat. Returns False if the job moved to
        a terminal state (cancelled by API, reaped by sweeper) so the
        worker can abort gracefully on next iteration.
        """
        row = await self._pool.fetchrow(
            """
            UPDATE jobs
            SET worker_heartbeat = now()
            WHERE id = $1 AND status = 'running'
            RETURNING id
            """,
            job_id,
        )
        return row is not None

    async def mark_completed(
        self,
        *,
        job_id: str,
        summary: dict[str, Any],
    ) -> None:
        """Terminal-success transition. Sets finished_at and clears
        worker_heartbeat so the reaper leaves the row alone.
        """
        await self._pool.execute(
            """
            UPDATE jobs
            SET status = 'completed',
                summary = $2,
                finished_at = now(),
                worker_heartbeat = NULL
            WHERE id = $1
            """,
            job_id, json.dumps(summary),
        )

    async def mark_failed(
        self,
        *,
        job_id: str,
        error: str,
        error_kind: Optional[str] = None,
    ) -> None:
        """Terminal-failure transition. The reason is stored verbatim
        so operators can copy it into a ticket without redaction.
        `error_kind` is the engine's classified label (auth_expired,
        captcha_blocked, etc.) and stays NULL for legacy crashes the
        engine couldn't classify.
        """
        await self._pool.execute(
            """
            UPDATE jobs
            SET status = 'failed',
                error = $2,
                error_kind = $3,
                finished_at = now(),
                worker_heartbeat = NULL
            WHERE id = $1
            """,
            job_id, error, error_kind,
        )

    async def mark_cancelled(
        self,
        *,
        job_id: str,
        reason: str,
    ) -> Optional[JobRow]:
        """Cancel from the API side. Only valid from queued/running.
        Returns the row if the transition succeeded; None if the job
        was already terminal.
        """
        row = await self._pool.fetchrow(
            """
            UPDATE jobs
            SET status = 'cancelled',
                error = $2,
                finished_at = now(),
                worker_heartbeat = NULL
            WHERE id = $1 AND status IN ('queued', 'running')
            RETURNING *
            """,
            job_id, reason,
        )
        return self._job_row(row) if row else None

    async def get_job(self, *, job_id: str) -> Optional[JobRow]:
        row = await self._pool.fetchrow(
            "SELECT * FROM jobs WHERE id = $1",
            job_id,
        )
        return self._job_row(row) if row else None

    async def list_recent_jobs(self, *, limit: int = 50) -> list[JobRow]:
        """For the UI legacy listing. Postgres-native ORDER BY DESC +
        LIMIT — cheap with the (company_id, created_at DESC) index.
        """
        rows = await self._pool.fetch(
            "SELECT * FROM jobs ORDER BY created_at DESC LIMIT $1",
            limit,
        )
        return [self._job_row(r) for r in rows]

    async def reap_orphans(self, *, max_idle_seconds: int = 180) -> int:
        """Flip jobs whose worker stopped heartbeating to `failed`.

        Returns the number of rows reaped. Called by a background
        coroutine in server.py on a fixed interval (default 60s). The
        threshold is conservative — under normal load the worker
        heartbeats every 30s, so 3 missed pings means we're sure
        something went wrong.
        """
        result = await self._pool.execute(
            """
            UPDATE jobs
            SET status = 'failed',
                error = 'Worker died without heartbeat',
                finished_at = now(),
                worker_heartbeat = NULL
            WHERE status = 'running'
              AND worker_heartbeat < now() - make_interval(secs => $1)
            """,
            max_idle_seconds,
        )
        # asyncpg returns "UPDATE N" — split off the count.
        return int(result.split()[1]) if result.startswith("UPDATE ") else 0

    # ── files ─────────────────────────────────────────────────────────

    async def add_file(
        self,
        *,
        job_id: str,
        cufe: Optional[str],
        prefijo_folio: Optional[str],
        issuer_nit: Optional[str],
        issue_date: Optional[date],
        kind: str,
        name: str,
        size_bytes: int,
        r2_key: Optional[str] = None,
        r2_url: Optional[str] = None,
    ) -> JobFileRow:
        row = await self._pool.fetchrow(
            """
            INSERT INTO job_files (
                job_id, cufe, prefijo_folio, issuer_nit, issue_date,
                kind, name, size_bytes, r2_key, r2_url
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING *
            """,
            job_id, cufe, prefijo_folio, issuer_nit, issue_date,
            kind, name, size_bytes, r2_key, r2_url,
        )
        return self._file_row(row)

    async def list_files(self, *, job_id: str) -> list[JobFileRow]:
        rows = await self._pool.fetch(
            "SELECT * FROM job_files WHERE job_id = $1 ORDER BY id",
            job_id,
        )
        return [self._file_row(r) for r in rows]

    # ── events ────────────────────────────────────────────────────────

    async def append_event(
        self,
        *,
        job_id: str,
        source: str,
        phase: str,
        status: str,
        message: Optional[str] = None,
        payload: Optional[dict[str, Any]] = None,
        occurred_at: Optional[datetime] = None,
    ) -> JobEventRow:
        """Append one event row.

        Concurrency: Postgres forbids `FOR UPDATE` together with
        aggregates, so we serialize per-job writers with a transaction
        advisory lock keyed on a stable hash of `job_id`. The lock is
        cheap (~microseconds), scoped to the current transaction, and
        invisible to readers in other backends.

        Under sustained contention this becomes a queue, which is
        exactly what we want — duplicate event_index values on the
        (job_id, event_index) PK would raise UniqueViolationError and
        force the caller to retry.
        """
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # hashtext() is a Postgres built-in that returns an
                # int4 from a string; perfect for the single-arg
                # pg_advisory_xact_lock signature. Negative values are
                # fine — the lock space is per-database, not per-table.
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtext($1))",
                    job_id,
                )
                next_idx = await conn.fetchval(
                    """
                    SELECT COALESCE(MAX(event_index), 0) + 1
                    FROM job_events
                    WHERE job_id = $1
                    """,
                    job_id,
                )
                row = await conn.fetchrow(
                    """
                    INSERT INTO job_events (
                        job_id, event_index, source, phase, status,
                        message, payload, occurred_at
                    )
                    VALUES (
                        $1, $2, $3, $4, $5,
                        $6, $7, COALESCE($8, now())
                    )
                    RETURNING *
                    """,
                    job_id, next_idx, source, phase, status,
                    message, json.dumps(payload) if payload else None,
                    occurred_at,
                )
                return self._event_row(row)

    async def list_events_since(
        self,
        *,
        job_id: str,
        since: int = 0,
        limit: int = 500,
    ) -> tuple[list[JobEventRow], int]:
        """Page through events with event_index > since. Returns the
        list and the new high-water mark so the caller can persist it
        as the next `since` value.
        """
        rows = await self._pool.fetch(
            """
            SELECT * FROM job_events
            WHERE job_id = $1 AND event_index > $2
            ORDER BY event_index ASC
            LIMIT $3
            """,
            job_id, since, limit,
        )
        events = [self._event_row(r) for r in rows]
        next_since = events[-1].event_index if events else since
        return events, next_since

    # ── row mappers ───────────────────────────────────────────────────

    @staticmethod
    def _job_row(row: asyncpg.Record) -> JobRow:
        # `error_kind` is optional in the schema (added by an additive
        # ALTER) so legacy rows return None via dict-style access on
        # the asyncpg Record. We use `.get`-style defensive access
        # because asyncpg raises KeyError when the column doesn't
        # exist — the bootstrap.sql now always includes it, but a
        # mid-deploy state could briefly lack it.
        try:
            error_kind = row["error_kind"]
        except (KeyError, IndexError):
            error_kind = None
        return JobRow(
            id=row["id"],
            company_id=row["company_id"],
            status=row["status"],
            auth_url=row["auth_url"],
            start_date=row["start_date"],
            end_date=row["end_date"],
            max_invoices=row["max_invoices"],
            worker_id=row["worker_id"],
            worker_heartbeat=row["worker_heartbeat"],
            created_at=row["created_at"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            summary=row["summary"],
            error=row["error"],
            error_kind=error_kind,
        )

    @staticmethod
    def _file_row(row: asyncpg.Record) -> JobFileRow:
        return JobFileRow(
            id=row["id"],
            job_id=row["job_id"],
            cufe=row["cufe"],
            prefijo_folio=row["prefijo_folio"],
            issuer_nit=row["issuer_nit"],
            issue_date=row["issue_date"],
            kind=row["kind"],
            name=row["name"],
            size_bytes=row["size_bytes"],
            r2_key=row["r2_key"],
            r2_url=row["r2_url"],
            uploaded_at=row["uploaded_at"],
        )

    @staticmethod
    def _event_row(row: asyncpg.Record) -> JobEventRow:
        return JobEventRow(
            job_id=row["job_id"],
            event_index=row["event_index"],
            source=row["source"],
            phase=row["phase"],
            status=row["status"],
            message=row["message"],
            payload=row["payload"],
            occurred_at=row["occurred_at"],
        )
