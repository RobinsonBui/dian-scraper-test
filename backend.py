"""Job-storage backend abstraction for dian-scraper-test.

Provides a single Protocol that the API and the scraper engine talk
to. Two implementations are wired by env at boot:

  * `InMemoryJobBackend` — legacy `RunState` behavior. Selected when
    `DB_MODE=memory` AND `STORAGE_MODE=local`. Preserves bit-for-bit
    the response shape NUVARA used before this refactor so we can
    deploy the new image without a coordinated NUVARA release.

  * `PostgresR2JobBackend` — persistent. Selected when `DB_MODE=postgres`
    and/or `STORAGE_MODE=r2`. Writes job state to Postgres via the
    `JobStore` repo and ZIPs to R2 via `R2Storage`. Survives container
    restarts; jobs that were running come back as `failed` via the
    reaper so NUVARA can decide to retry.

Why one interface, two impls:

  - The handler code in `server.py` reads the same way regardless of
    backend. No `if DB_MODE == 'postgres'` branches sprinkled across
    1000 lines.
  - The legacy in-memory path can be removed in a future PR without
    touching call sites.
  - Tests can use a fake backend without standing up Postgres + R2.

The interface uses dataclasses (defined in db.py) for return types so
the JSON shape is consistent across backends.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional, Protocol, runtime_checkable

from db import JobEventRow, JobFileRow, JobRow, JobStore
from r2 import R2Storage

logger = logging.getLogger("dian-scraper.backend")


# ──────────────────────────────────────────────────────────────────────────
# Public surface
# ──────────────────────────────────────────────────────────────────────────


@runtime_checkable
class JobBackend(Protocol):
    """Everything `server.py` and `core.py` need from a backend.

    Methods are coroutines. Implementations must be safe to call
    concurrently from multiple tasks — the in-memory one uses
    asyncio.Lock, the Postgres one relies on transactional isolation.
    """

    # ── lifecycle ────────────────────────────────────────────────────

    async def startup(self) -> None: ...
    async def shutdown(self) -> None: ...

    # ── jobs ─────────────────────────────────────────────────────────

    async def create_job(
        self,
        *,
        job_id: str,
        company_id: Optional[str],
        auth_url: str,
        start_date: date,
        end_date: date,
        max_invoices: int,
    ) -> JobRow: ...

    async def get_job(self, *, job_id: str) -> Optional[JobRow]: ...

    async def list_recent_jobs(self, *, limit: int = 50) -> list[JobRow]: ...

    async def claim_job(self, *, job_id: str, worker_id: str) -> Optional[JobRow]: ...

    async def heartbeat(self, *, job_id: str) -> bool: ...

    async def mark_completed(
        self, *, job_id: str, summary: dict[str, Any],
    ) -> None: ...

    async def mark_failed(self, *, job_id: str, error: str) -> None: ...

    async def mark_cancelled(
        self, *, job_id: str, reason: str,
    ) -> Optional[JobRow]: ...

    async def reap_orphans(self, *, max_idle_seconds: int = 180) -> int: ...

    # ── events ───────────────────────────────────────────────────────

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
    ) -> JobEventRow: ...

    async def list_events_since(
        self,
        *,
        job_id: str,
        since: int = 0,
        limit: int = 500,
    ) -> tuple[list[JobEventRow], int]: ...

    # ── files ────────────────────────────────────────────────────────

    async def save_file(
        self,
        *,
        job_id: str,
        company_id: Optional[str],
        cufe: Optional[str],
        prefijo_folio: Optional[str],
        issuer_nit: Optional[str],
        issue_date: Optional[date],
        filename: str,
        body: bytes,
        kind: str = "zip",
    ) -> JobFileRow:
        """Persist a downloaded file. The backend decides where:

        - InMemoryJobBackend → writes to local filesystem under the
          legacy `downloads/{job_id}/` tree and returns a row with
          `r2_key=None`. The server still serves it via /files/...

        - PostgresR2JobBackend → uploads to R2 and persists a row
          with `r2_key + r2_url`. The /files endpoint is unused for
          rows created this way.
        """
        ...

    async def list_files(self, *, job_id: str) -> list[JobFileRow]: ...

    async def file_local_path(
        self, *, job_id: str, name: str,
    ) -> Optional[Path]:
        """Resolve a filesystem path for the /files/{job_id}/{name}
        legacy endpoint. Returns None when files live in R2 — the
        server should refuse the request in that case.
        """
        ...


# ──────────────────────────────────────────────────────────────────────────
# Postgres + R2 implementation
# ──────────────────────────────────────────────────────────────────────────


class PostgresR2JobBackend:
    """Production-grade backend backed by Postgres (state) + R2 (files).

    Construction is async (creates the pool, validates R2 creds) so
    it must be built via `PostgresR2JobBackend.from_env()` not the
    bare constructor.

    Storage layout:
        Postgres: jobs, job_files, job_events tables (see bootstrap.sql)
        R2:       dian-scraper-alt/{companyId}/{jobId}/{cufe}.zip
    """

    def __init__(self, *, store: JobStore, storage: R2Storage) -> None:
        self._store = store
        self._storage = storage

    @classmethod
    async def from_env(cls) -> "PostgresR2JobBackend":
        store = await JobStore.from_env()
        storage = R2Storage.from_env()
        # Validate R2 connectivity at boot; log a warning rather than
        # crash so a misconfigured bucket doesn't break the API as
        # a whole — uploads will fail loudly on first attempt anyway.
        #
        # We wrap in try/except as defense in depth: R2Storage.health_check
        # is supposed to swallow boto errors and return False, but botocore
        # has surprised us before (e.g. EndpointConnectionError sneaking
        # past a narrow except). A crash here would defeat the whole point
        # of returning a bool — the API never gets to tell the operator
        # what's misconfigured because the container exits before binding
        # to the port.
        try:
            ok = await storage.health_check()
        except Exception:
            logger.exception(
                "R2 health check raised; treating as not-ok and continuing.",
            )
            ok = False
        if not ok:
            logger.warning(
                "R2 health check failed at startup — uploads will likely fail.",
            )
        return cls(store=store, storage=storage)

    async def startup(self) -> None:
        # Pool already initialised in from_env(); nothing extra here.
        return None

    async def shutdown(self) -> None:
        await self._store.close()

    # Job CRUD just delegates to the store. Kept as thin wrappers so
    # the protocol's signature wins over JobStore's (they happen to
    # match today but we don't want them to drift).

    async def create_job(self, **kw) -> JobRow:
        return await self._store.create_job(**kw)

    async def get_job(self, **kw) -> Optional[JobRow]:
        return await self._store.get_job(**kw)

    async def list_recent_jobs(self, **kw) -> list[JobRow]:
        return await self._store.list_recent_jobs(**kw)

    async def claim_job(self, **kw) -> Optional[JobRow]:
        # The store's method is called `mark_running` but in the
        # protocol it's `claim_job` — clearer semantics for the
        # call site (this transitions queued→running atomically).
        return await self._store.mark_running(**kw)

    async def heartbeat(self, **kw) -> bool:
        return await self._store.heartbeat(**kw)

    async def mark_completed(self, **kw) -> None:
        await self._store.mark_completed(**kw)

    async def mark_failed(self, **kw) -> None:
        await self._store.mark_failed(**kw)

    async def mark_cancelled(self, **kw) -> Optional[JobRow]:
        return await self._store.mark_cancelled(**kw)

    async def reap_orphans(self, **kw) -> int:
        return await self._store.reap_orphans(**kw)

    async def append_event(self, **kw) -> JobEventRow:
        return await self._store.append_event(**kw)

    async def list_events_since(self, **kw) -> tuple[list[JobEventRow], int]:
        return await self._store.list_events_since(**kw)

    # Files: upload to R2, then persist a row with r2_key + r2_url.

    async def save_file(
        self,
        *,
        job_id: str,
        company_id: Optional[str],
        cufe: Optional[str],
        prefijo_folio: Optional[str],
        issuer_nit: Optional[str],
        issue_date: Optional[date],
        filename: str,
        body: bytes,
        kind: str = "zip",
    ) -> JobFileRow:
        # When company_id is missing we still store the file but bucket
        # it under a sentinel so it doesn't collide with real tenants.
        company_for_key = company_id or "__no_company__"
        upload = await self._storage.upload_zip(
            company_id=company_for_key,
            job_id=job_id,
            cufe=cufe,
            filename=filename,
            body=body,
        )
        return await self._store.add_file(
            job_id=job_id,
            cufe=cufe,
            prefijo_folio=prefijo_folio,
            issuer_nit=issuer_nit,
            issue_date=issue_date,
            kind=kind,
            name=filename,
            size_bytes=upload.size_bytes,
            r2_key=upload.key,
            r2_url=upload.url,
        )

    async def list_files(self, **kw) -> list[JobFileRow]:
        return await self._store.list_files(**kw)

    async def file_local_path(self, **kw) -> Optional[Path]:
        # Files live in R2 in this backend — the legacy /files endpoint
        # is meaningless here. Caller should 410 Gone.
        return None


# ──────────────────────────────────────────────────────────────────────────
# In-memory + local filesystem implementation (legacy)
# ──────────────────────────────────────────────────────────────────────────


@dataclass
class _MemoryJobRecord:
    """Internal mutable counterpart of JobRow for the in-memory store."""

    row: JobRow
    events: list[JobEventRow]
    files: list[JobFileRow]


class InMemoryJobBackend:
    """Drop-in for the legacy `RunState` + `DOWNLOADS_DIR` behaviour.

    Lives entirely in process RAM + local disk. Loses everything on
    restart — exactly the brittleness we are trying to retire. Kept
    available for `STORAGE_MODE=local + DB_MODE=memory` so the new
    image can ship without coupling to Postgres/R2 from day one.
    """

    def __init__(self, *, downloads_dir: Path) -> None:
        self._jobs: dict[str, _MemoryJobRecord] = {}
        self._lock = asyncio.Lock()
        self._downloads_root = downloads_dir
        self._downloads_root.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_env(cls, *, downloads_dir: Path) -> "InMemoryJobBackend":
        return cls(downloads_dir=downloads_dir)

    async def startup(self) -> None:
        return None

    async def shutdown(self) -> None:
        return None

    # ── jobs ────────────────────────────────────────────────────────

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
        async with self._lock:
            if job_id in self._jobs:
                # Match asyncpg.UniqueViolationError semantics by
                # raising a generic ValueError; the API maps it to 409.
                raise ValueError(f"job {job_id} already exists")
            now = _utcnow()
            row = JobRow(
                id=job_id,
                company_id=company_id,
                status="queued",
                auth_url=auth_url,
                start_date=start_date,
                end_date=end_date,
                max_invoices=max_invoices,
                worker_id=None,
                worker_heartbeat=None,
                created_at=now,
                started_at=None,
                finished_at=None,
                summary=None,
                error=None,
            )
            self._jobs[job_id] = _MemoryJobRecord(row=row, events=[], files=[])
            return row

    async def get_job(self, *, job_id: str) -> Optional[JobRow]:
        async with self._lock:
            rec = self._jobs.get(job_id)
            return rec.row if rec else None

    async def list_recent_jobs(self, *, limit: int = 50) -> list[JobRow]:
        async with self._lock:
            rows = sorted(
                (r.row for r in self._jobs.values()),
                key=lambda r: r.created_at,
                reverse=True,
            )
            return rows[:limit]

    async def claim_job(
        self, *, job_id: str, worker_id: str,
    ) -> Optional[JobRow]:
        async with self._lock:
            rec = self._jobs.get(job_id)
            if rec is None or rec.row.status != "queued":
                return None
            now = _utcnow()
            rec.row = _replace(
                rec.row,
                status="running",
                worker_id=worker_id,
                worker_heartbeat=now,
                started_at=now,
            )
            return rec.row

    async def heartbeat(self, *, job_id: str) -> bool:
        async with self._lock:
            rec = self._jobs.get(job_id)
            if rec is None or rec.row.status != "running":
                return False
            rec.row = _replace(rec.row, worker_heartbeat=_utcnow())
            return True

    async def mark_completed(
        self, *, job_id: str, summary: dict[str, Any],
    ) -> None:
        async with self._lock:
            rec = self._jobs.get(job_id)
            if rec is None:
                return
            rec.row = _replace(
                rec.row,
                status="completed",
                summary=summary,
                finished_at=_utcnow(),
                worker_heartbeat=None,
            )

    async def mark_failed(self, *, job_id: str, error: str) -> None:
        async with self._lock:
            rec = self._jobs.get(job_id)
            if rec is None:
                return
            rec.row = _replace(
                rec.row,
                status="failed",
                error=error,
                finished_at=_utcnow(),
                worker_heartbeat=None,
            )

    async def mark_cancelled(
        self, *, job_id: str, reason: str,
    ) -> Optional[JobRow]:
        async with self._lock:
            rec = self._jobs.get(job_id)
            if rec is None or rec.row.status not in ("queued", "running"):
                return None
            rec.row = _replace(
                rec.row,
                status="cancelled",
                error=reason,
                finished_at=_utcnow(),
                worker_heartbeat=None,
            )
            return rec.row

    async def reap_orphans(self, *, max_idle_seconds: int = 180) -> int:
        # In-memory backend has no separate worker process, so by
        # definition there are no orphans. Return 0 to keep parity
        # with the Postgres backend's contract.
        return 0

    # ── events ──────────────────────────────────────────────────────

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
        async with self._lock:
            rec = self._jobs.get(job_id)
            if rec is None:
                raise ValueError(f"job {job_id} not found")
            idx = len(rec.events) + 1
            event = JobEventRow(
                job_id=job_id,
                event_index=idx,
                source=source,
                phase=phase,
                status=status,
                message=message,
                payload=payload,
                occurred_at=occurred_at or _utcnow(),
            )
            rec.events.append(event)
            return event

    async def list_events_since(
        self,
        *,
        job_id: str,
        since: int = 0,
        limit: int = 500,
    ) -> tuple[list[JobEventRow], int]:
        async with self._lock:
            rec = self._jobs.get(job_id)
            if rec is None:
                return [], since
            tail = [e for e in rec.events if e.event_index > since][:limit]
            next_since = tail[-1].event_index if tail else since
            return tail, next_since

    # ── files ───────────────────────────────────────────────────────

    async def save_file(
        self,
        *,
        job_id: str,
        company_id: Optional[str],
        cufe: Optional[str],
        prefijo_folio: Optional[str],
        issuer_nit: Optional[str],
        issue_date: Optional[date],
        filename: str,
        body: bytes,
        kind: str = "zip",
    ) -> JobFileRow:
        async with self._lock:
            rec = self._jobs.get(job_id)
            if rec is None:
                raise ValueError(f"job {job_id} not found")

        # Write to disk OUTSIDE the lock — the lock is for in-memory
        # state, not for blocking IO.
        job_dir = self._downloads_root / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        out_path = job_dir / filename
        await asyncio.to_thread(out_path.write_bytes, body)

        async with self._lock:
            rec = self._jobs[job_id]  # refresh under lock
            file_row = JobFileRow(
                id=len(rec.files) + 1,
                job_id=job_id,
                cufe=cufe,
                prefijo_folio=prefijo_folio,
                issuer_nit=issuer_nit,
                issue_date=issue_date,
                kind=kind,
                name=filename,
                size_bytes=len(body),
                r2_key=None,
                r2_url=None,
                uploaded_at=_utcnow(),
            )
            rec.files.append(file_row)
            return file_row

    async def list_files(self, *, job_id: str) -> list[JobFileRow]:
        async with self._lock:
            rec = self._jobs.get(job_id)
            return list(rec.files) if rec else []

    async def file_local_path(
        self, *, job_id: str, name: str,
    ) -> Optional[Path]:
        path = self._downloads_root / job_id / name
        return path if path.is_file() else None


# ──────────────────────────────────────────────────────────────────────────
# Factory — picks the impl from env at boot
# ──────────────────────────────────────────────────────────────────────────


async def build_backend(*, downloads_dir: Path) -> JobBackend:
    """Return the backend the env asked for.

    `STORAGE_MODE=r2` or `DB_MODE=postgres` → PostgresR2JobBackend.
    Otherwise → InMemoryJobBackend (legacy).

    We don't allow mixed modes (e.g. memory state + R2 files) on
    purpose — they exist but the migration story is muddier. Add
    them only when a concrete need shows up.
    """
    db_mode = os.environ.get("DB_MODE", "memory").strip().lower()
    storage_mode = os.environ.get("STORAGE_MODE", "local").strip().lower()

    if db_mode == "postgres" or storage_mode == "r2":
        if db_mode != "postgres" or storage_mode != "r2":
            logger.warning(
                "Mixed mode requested (DB_MODE=%s STORAGE_MODE=%s). "
                "Promoting both to postgres+r2 for consistency.",
                db_mode, storage_mode,
            )
        backend = await PostgresR2JobBackend.from_env()
        logger.info("Backend: postgres + R2")
        return backend

    backend = InMemoryJobBackend.from_env(downloads_dir=downloads_dir)
    logger.info("Backend: in-memory + local filesystem (legacy)")
    return backend


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────


def _utcnow() -> datetime:
    """Tz-aware UTC now. Matches what asyncpg returns from TIMESTAMPTZ."""
    return datetime.now(tz=timezone.utc)


def _replace(row: JobRow, **changes: Any) -> JobRow:
    """Like dataclasses.replace but kept here so the import surface
    of this module stays small.
    """
    from dataclasses import replace
    return replace(row, **changes)
