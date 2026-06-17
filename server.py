"""Web server for DIAN scraper test — UI tipo Nuvara + remote downloader.

Two consumption modes:

    1. Human UI (browser): open http://localhost:8765/, configure a run,
       watch the live log. No auth needed — the UI is gated by the same
       network reachability that gates the operator's access.

    2. M2M (nuvara): POST /api/jobs with X-API-Key header → poll
       /api/jobs/{job_id} → download files from /files/{job_id}/{name}.
       All /api/* and /files/* endpoints REQUIRE a valid X-API-Key when
       the SCRAPER_API_KEY env var is set.

Auth model:

    - SCRAPER_API_KEY env var holds the shared secret.
    - When unset, the server runs in "open" mode (legacy, local dev).
    - When set, every /api/* and /files/* request must carry either:
        - HTTP header:    X-API-Key: <secret>
        - WS query param: ?api_key=<secret>   (browsers can't set WS headers)
      Failed auth returns 401 with a structured error.

Run with:
    SCRAPER_API_KEY=somesecret python server.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import secrets
import uuid
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Awaitable, Literal, Optional

import uvicorn
from fastapi import (
    FastAPI,
    HTTPException,
    Request,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.middleware.base import BaseHTTPMiddleware

from backend import JobBackend, PostgresR2JobBackend, build_backend
from core import (
    ERROR_KIND_ENGINE_CRASH,
    DianTestScraper,
    DownloadEvent,
    FileSavedEvent,
    Logger,
    ScraperError,
)
from db import JobFileRow, JobRow

logger = logging.getLogger("dian-scraper.server")

BASE_DIR = Path(__file__).resolve().parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
LOGS_DIR = BASE_DIR / "logs"
STATIC_DIR = BASE_DIR / "static"

# Shared secret consumed by `/api/*` and `/files/*` endpoints.
# When set, every machine-to-machine request must carry it. When empty,
# the server runs unauthenticated (legacy single-operator local dev).
SCRAPER_API_KEY: str = os.environ.get("SCRAPER_API_KEY", "").strip()


def _env_int(name: str, default: int) -> int:
    """Read an int from env with a sane fallback.

    Returns the default when the env var is unset, empty, or doesn't
    parse cleanly. We don't want a typo in Dokploy → Environment to
    crash the container on boot.
    """
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except ValueError:
        return default


# Per-invoice random sleep window. The scraper picks a uniform value in
# [DEFAULT_DELAY_MIN_MS, DEFAULT_DELAY_MAX_MS] before each download to
# look like a human. Tighter window = faster runs but higher block
# risk; loosen these if Azure WAF starts 403'ing.
#
# Defaults: 1000..6000 ms — tuned empirically against the DIAN portal
# in june 2026. Override per-deploy via Dokploy → Environment.
DEFAULT_DELAY_MIN_MS: int = _env_int("DEFAULT_DELAY_MIN_MS", 1000)
DEFAULT_DELAY_MAX_MS: int = _env_int("DEFAULT_DELAY_MAX_MS", 6000)
# Every Nth download the scraper takes a longer pause (3..6× the
# normal window) to look less robotic. Set high (e.g. 1000) to disable
# de-facto.
DEFAULT_LONG_PAUSE_EVERY: int = _env_int("DEFAULT_LONG_PAUSE_EVERY", 30)

# How many scraping jobs can run concurrently inside this process. Each
# job opens its own Playwright browser, so the ceiling is RAM-bound,
# not CPU-bound. The Fase 1 plan caps this at 3 so we stay under the
# WAF rate-limit of the single egress IP; once we have a pool of IPs
# (Fase 4) this can grow. Set via env in Dokploy.
MAX_CONCURRENT_JOBS: int = _env_int("MAX_CONCURRENT_JOBS", 3)

# How often the worker pings the DB to prove it's still alive. Must be
# noticeably shorter than REAPER_MAX_IDLE_SECONDS so a long DIAN pause
# (~36s worst case) doesn't get a healthy worker reaped.
WORKER_HEARTBEAT_SECONDS: int = _env_int("WORKER_HEARTBEAT_SECONDS", 30)

# How often the reaper sweeps the jobs table looking for orphans. Only
# active in the PostgresR2JobBackend — InMemoryJobBackend has nothing
# to reap.
REAPER_INTERVAL_SECONDS: int = _env_int("REAPER_INTERVAL_SECONDS", 60)

# A job whose worker hasn't heartbeated in this many seconds is
# considered dead. The reaper transitions it to status='failed' with
# error='Worker died without heartbeat' so NUVARA can decide to retry.
REAPER_MAX_IDLE_SECONDS: int = _env_int("REAPER_MAX_IDLE_SECONDS", 180)

# Identifies this process in the `jobs.worker_id` column. Useful when
# we go multi-container in Fase 2 — every container picks a distinct
# id so the reaper logs say which one died.
WORKER_ID: str = (
    os.environ.get("WORKER_ID", "").strip()
    or f"server-{os.getpid()}-{secrets.token_hex(3)}"
)

# Paths that the middleware lets through without checking the API key:
# the UI HTML root, the static assets, FastAPI's own docs, and a
# liveness/readiness endpoint for Dokploy healthchecks.
PUBLIC_PATHS = frozenset(
    {
        "/",
        "/healthz",
        "/openapi.json",
        "/docs",
        "/redoc",
        "/favicon.ico",
    }
)


def _is_public(path: str) -> bool:
    if path in PUBLIC_PATHS:
        return True
    if path.startswith("/static/"):
        return True
    return False


class ApiKeyMiddleware(BaseHTTPMiddleware):
    """Reject /api/* and /files/* requests without a valid X-API-Key.

    Skipped entirely when SCRAPER_API_KEY is empty so local dev keeps
    working with the legacy "open" mode.
    """

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Any]],
    ) -> Any:
        if not SCRAPER_API_KEY:
            return await call_next(request)

        if _is_public(request.url.path):
            return await call_next(request)

        # Only protect API + files. UI and statics already exited above.
        if not (
            request.url.path.startswith("/api/")
            or request.url.path.startswith("/files/")
        ):
            return await call_next(request)

        provided = request.headers.get("x-api-key", "")
        if not provided or not secrets.compare_digest(provided, SCRAPER_API_KEY):
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={
                    "error": "unauthorized",
                    "message": (
                        "Missing or invalid X-API-Key header. "
                        "Set the SCRAPER_API_KEY env var on the server and "
                        "send the matching value on every /api/* and "
                        "/files/* request."
                    ),
                },
            )
        return await call_next(request)


def _assert_ws_authorized(websocket: WebSocket) -> bool:
    """WebSocket auth check.

    Browsers can't set custom headers on WS handshakes, so we accept the
    API key either via X-API-Key header (server-to-server WS clients) OR
    via `?api_key=` query param (browser fallback). Same constant-time
    comparison as the HTTP path.
    """
    if not SCRAPER_API_KEY:
        return True
    provided = (
        websocket.headers.get("x-api-key")
        or websocket.query_params.get("api_key")
        or ""
    )
    return bool(provided) and secrets.compare_digest(provided, SCRAPER_API_KEY)


DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
STATIC_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------------
# State — process-wide containers for concurrent jobs.
#
# Before this refactor we had a single global RunState with one task /
# one cancel_event / one is_running flag. That worked for the validation
# rail but it caps the scraper at one job at a time and loses every
# in-flight job when the container restarts.
#
# After the refactor:
#   - All durable state (jobs, events, files) lives in `backend`
#     (PostgresR2JobBackend in production, InMemoryJobBackend in legacy
#     mode). The server holds NO authoritative job state of its own.
#   - Per-job in-process resources (the asyncio.Task running the
#     scraper, its cancel_event) live in per-job dicts keyed by job_id.
#   - A semaphore bounds the number of concurrent browsers we open in
#     this process; extra jobs wait queued in the backend and get
#     picked up as soon as a slot frees.
#   - The WebSocket broadcast hub is shared across all jobs (UI shows
#     one combined live log). A short ring buffer keeps the last
#     events so a reconnecting client sees recent history.
# --------------------------------------------------------------------------


JobStatus = str  # "queued" | "running" | "completed" | "failed" | "cancelled"

# The backend is built in lifespan(). Code outside lifespan must never
# touch this before startup — use `require_backend()` for a friendlier
# error message if anyone accidentally does.
backend: JobBackend | None = None

# In-flight task per job_id. We keep a reference here so:
#   - shutdown can cancel everything cleanly
#   - /api/cancel can target a specific job
# Entries are removed in _run_job's finally block once the job reaches
# a terminal state.
running_tasks: dict[str, asyncio.Task] = {}

# Per-job cancel signal. The scraper engine reads this Event; the API
# sets it on /api/cancel. One Event per job so cancelling job A doesn't
# stop job B that happens to be running in parallel.
cancel_events: dict[str, asyncio.Event] = {}

# Connected WebSocket clients receiving the combined live log.
ws_clients: set[WebSocket] = set()

# Short replay buffer for WS reconnects. We don't try to replay the
# full job log over WS — that's what GET /api/jobs/{id}/events is for.
# This is purely so a refreshing browser tab doesn't see a blank UI
# for a few seconds. Capped to keep memory flat.
ws_recent_events: deque[dict[str, Any]] = deque(maxlen=200)

# Bound on concurrent Playwright browsers in this process. Created in
# lifespan() because asyncio.Semaphore wants a running loop. None
# before startup; require_backend()/_acquire_slot guard against that.
jobs_semaphore: asyncio.Semaphore | None = None

# Background tasks owned by the server lifecycle (the reaper loop).
# Kept here so lifespan can cancel them cleanly on shutdown.
_lifecycle_tasks: list[asyncio.Task] = []


def require_backend() -> JobBackend:
    """Return the live backend or raise a 503 with a clear message.

    Centralised so every endpoint that needs the backend gives the
    same error shape when called before startup completes (rare but
    happens during cold-boot health probes).
    """
    if backend is None:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Backend not ready yet — try again in a moment.",
        )
    return backend


def _is_r2_mode() -> bool:
    """True when the active backend ships files to R2 instead of disk.

    Useful for the legacy /files/* endpoints, which need to tell a
    misconfigured caller 'go read R2 directly' instead of pretending
    the file doesn't exist (404) when really it just lives elsewhere.
    """
    return isinstance(backend, PostgresR2JobBackend)


def _parse_iso_date(value: str) -> date:
    """Parse a YYYY-MM-DD string into a date.

    The HTTP layer always receives strings (Pydantic StartRequest
    keeps them as `str` for backwards compatibility) but the backend
    contract uses native `date` for type safety. Centralised here so
    error messages stay consistent when a caller sends garbage.
    """
    try:
        return date.fromisoformat(value)
    except ValueError as e:
        raise HTTPException(400, f"invalid date '{value}': {e}")


def _parse_dian_date(value: str | None) -> date | None:
    """Parse a date string emitted by the DIAN scraper.

    Two accepted shapes, in order:

      * ISO YYYY-MM-DD          when the engine already normalised it.
      * DIAN DD-MM-YYYY         the raw format DIAN's portal returns
                                 in the document listing (e.g. '08-06-2026').

    Returns None on empty/whitespace input or anything we can't parse,
    instead of raising. This is critical: the caller (on_file_saved)
    persists the file regardless of whether the date can be parsed;
    a bad date should NOT silently drop the entire file row, which is
    exactly what was happening before (HTTPException leaking into
    _emit_file's try/except).
    """
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        pass
    # DIAN's dd-mm-yyyy with dashes (or sometimes slashes)
    for sep in ("-", "/"):
        parts = s.split(sep)
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            try:
                d, m, y = (int(p) for p in parts)
                if y < 100:
                    y += 2000
                return date(y, m, d)
            except (ValueError, TypeError):
                continue
    return None


# --------------------------------------------------------------------------
# Request / Response models
# --------------------------------------------------------------------------


class StartRequest(BaseModel):
    auth_url: str
    start_date: str = Field(..., description="YYYY-MM-DD")
    end_date: str = Field(..., description="YYYY-MM-DD")
    # 300 is the new default (was 30). Covers a tenant with very heavy
    # B2B billing without hitting the cap, while still bounded enough
    # that a runaway run can't pin the worker for hours. NUVARA's UI
    # surfaces this so the operator can lower or raise it per-run when
    # a smaller / bigger window is needed.
    max_invoices: int = 300
    headless: bool = True
    # NUVARA tenant the job belongs to. Optional so the human UI on
    # the scraper itself (no tenant context) still works; M2M callers
    # MUST send it because the R2 key layout
    # `dian-scraper-alt/{company_id}/{job_id}/{cufe}.zip` depends on
    # it. When missing in postgres+R2 mode the backend buckets files
    # under a `__no_company__` sentinel so they don't collide with
    # real tenants but are also easy to spot in audits.
    company_id: Optional[str] = None
    # Delays default to the server-wide env tuning. M2M callers can
    # still override them per-request (e.g. when reproducing a block
    # locally) by sending explicit values in the POST body.
    delay_min_ms: int = Field(default_factory=lambda: DEFAULT_DELAY_MIN_MS)
    delay_max_ms: int = Field(default_factory=lambda: DEFAULT_DELAY_MAX_MS)
    long_pause_every: int = Field(default_factory=lambda: DEFAULT_LONG_PAUSE_EVERY)
    # CUFEs already known to the consumer (typically NUVARA) for this
    # company in the requested date range. The engine filters them out
    # right after the DOM listing so we never re-download invoices the
    # consumer already has. NUVARA scopes the list by issueDate to keep
    # the payload small (~50 KB for a 6-month range on an active tenant).
    # Empty/None disables the filter — the legacy "download everything"
    # behaviour an operator gets when driving the scraper from the
    # standalone UI without a NUVARA-side lookup.
    skip_cufes: Optional[list[str]] = None
    # Which DIAN bucket to scrape:
    #   - "purchase" → /Document/Received (compras, default).
    #   - "sale"     → /Document/Sent (ventas).
    # Default 'purchase' keeps old NUVARA images that don't send the
    # field working as before. The standalone UI of this scraper
    # always uses the default — it's M2M / NUVARA that flips it.
    direction: Literal["purchase", "sale"] = "purchase"
    # Optional docType filter applied to the listing AFTER pagination
    # but BEFORE the download queue. The only family modelled today is
    # "support", which keeps rows whose visible "Tipo" column contains
    # either "documento soporte" (DS) or "documento equivalente" (DE).
    # NUVARA uses it for the support rail (DS pulled from /Sent but
    # causada como compra). Older NUVARA images simply omit the field
    # and the scraper falls back to the legacy "all doctypes" path.
    doc_type_filter: Optional[Literal["support"]] = None


# --------------------------------------------------------------------------
# Lifespan — owns the backend lifetime and any background loops.
# --------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    global backend, jobs_semaphore

    # Build the backend per the env (memory + local fs OR postgres + R2).
    # Errors here crash-loop the container intentionally — running with
    # a half-initialised backend silently drops writes, which is worse
    # than not booting at all.
    backend = await build_backend(downloads_dir=DOWNLOADS_DIR)
    await backend.startup()

    jobs_semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)

    # The reaper only matters when the backend can outlive workers
    # (Postgres). For the in-memory backend the jobs vanish with the
    # process so there's nothing to reap.
    if isinstance(backend, PostgresR2JobBackend):
        _lifecycle_tasks.append(asyncio.create_task(_reaper_loop()))
        logger.info(
            "Reaper enabled: every %ds, kill jobs idle > %ds",
            REAPER_INTERVAL_SECONDS, REAPER_MAX_IDLE_SECONDS,
        )

    logger.info(
        "Server ready (worker_id=%s, max_concurrent=%d)",
        WORKER_ID, MAX_CONCURRENT_JOBS,
    )

    try:
        yield
    finally:
        # 1. Stop background loops first so they don't try to write
        #    after the pool closes.
        for t in _lifecycle_tasks:
            t.cancel()
        for t in _lifecycle_tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        _lifecycle_tasks.clear()

        # 2. Signal cancellation to every in-flight job and await
        #    them. We deliberately don't .cancel() the task — the
        #    job loop checks cancel_event and exits cleanly through
        #    mark_cancelled(), which lets the backend record the
        #    final state.
        for ev in cancel_events.values():
            ev.set()
        if running_tasks:
            await asyncio.gather(
                *running_tasks.values(), return_exceptions=True,
            )

        # 3. Close the backend (pool, R2 client cleanup, …)
        if backend is not None:
            await backend.shutdown()


app = FastAPI(title="DIAN Scraper Test", lifespan=lifespan)
app.add_middleware(ApiKeyMiddleware)


# --------------------------------------------------------------------------
# Reaper — only active in postgres mode.
# --------------------------------------------------------------------------


async def _reaper_loop() -> None:
    """Periodically reap jobs whose worker stopped heartbeating.

    Runs forever until cancelled by lifespan. Each iteration sleeps
    REAPER_INTERVAL_SECONDS, then asks the backend to flip any job
    whose `worker_heartbeat` is older than REAPER_MAX_IDLE_SECONDS
    to status='failed'.

    The reaper is safe to run on multiple containers at once — the
    underlying UPDATE filters by status='running', so once one
    container flips a row, the others skip it.
    """
    assert backend is not None
    while True:
        try:
            await asyncio.sleep(REAPER_INTERVAL_SECONDS)
            n = await backend.reap_orphans(
                max_idle_seconds=REAPER_MAX_IDLE_SECONDS,
            )
            if n:
                logger.warning("Reaper: flipped %d orphaned job(s) to failed", n)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Reaper iteration failed; continuing")


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    """Liveness probe — always 200 when the process is up.

    Public on purpose so Dokploy / load balancer healthchecks don't need
    the API key.

    Also surfaces the tuning defaults so an operator can verify from
    outside that the env vars actually landed in the running container
    (saves one round trip to `docker inspect`). The `backend` field
    tells you at a glance which storage path the process picked up —
    crucial when triaging "why didn't this job land in R2".
    """
    backend_name: str
    if backend is None:
        backend_name = "starting"
    elif isinstance(backend, PostgresR2JobBackend):
        backend_name = "postgres+r2"
    else:
        backend_name = "memory+local"
    return {
        "status": "ok",
        "auth_required": bool(SCRAPER_API_KEY),
        "backend": backend_name,
        "worker_id": WORKER_ID,
        "defaults": {
            "delay_min_ms": DEFAULT_DELAY_MIN_MS,
            "delay_max_ms": DEFAULT_DELAY_MAX_MS,
            "long_pause_every": DEFAULT_LONG_PAUSE_EVERY,
            "max_concurrent_jobs": MAX_CONCURRENT_JOBS,
            "worker_heartbeat_seconds": WORKER_HEARTBEAT_SECONDS,
            "reaper_interval_seconds": REAPER_INTERVAL_SECONDS,
            "reaper_max_idle_seconds": REAPER_MAX_IDLE_SECONDS,
        },
    }


# --------------------------------------------------------------------------
# Static + downloads
# --------------------------------------------------------------------------


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    html_path = STATIC_DIR / "index.html"
    if not html_path.exists():
        raise HTTPException(500, "static/index.html missing")
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


def _media_type_for(name: str) -> str:
    lower = name.lower()
    if lower.endswith(".pdf"):
        return "application/pdf"
    if lower.endswith(".xml"):
        return "application/xml"
    if lower.endswith(".zip"):
        return "application/zip"
    return "application/octet-stream"


def _gone_in_r2_mode(job_id: str | None, name: str) -> JSONResponse:
    """Build the 410 Gone payload for /files endpoints in R2 mode.

    Why 410 and not 404:
      - 410 ('Gone') means 'this resource USED to exist here, look
        somewhere else'. That's exactly what's happening — the bytes
        moved to R2.
      - 404 is ambiguous (typo? misconfigured client? job not found?)
        and tempts a poller into spinning forever 'waiting for the
        file to appear'.

    Body carries enough info for the operator (and the test suite)
    to find the file without guessing: the bucket layout key, and
    the r2_url when we can resolve the job_files row. NUVARA already
    reads r2_url from /api/jobs/{id} so it never hits this endpoint;
    this 410 is purely a guard-rail for misconfigured clients.
    """
    detail: dict[str, Any] = {
        "error": "gone",
        "message": (
            "Files are stored in R2 in this deployment. "
            "Read `r2_url` from GET /api/jobs/{job_id} and fetch "
            "from there instead."
        ),
    }
    if job_id is not None:
        detail["job_id"] = job_id
        detail["expected_r2_prefix"] = f"dian-scraper-alt/.../{job_id}/"
    detail["filename"] = name
    return JSONResponse(status_code=status.HTTP_410_GONE, content=detail)


@app.get("/files/{name}")
async def serve_file_legacy(name: str) -> Any:
    """Legacy flat file endpoint kept for the human UI.

    Resolution order, in local-filesystem mode:
      1. Most recent job's per-job subdir (matches what the engine
         writes today).
      2. The legacy flat layout (kept so old links keep working).

    In R2 mode we don't have either layout — return 410 Gone with a
    pointer to where the file actually lives.
    """
    if "/" in name or ".." in name:
        raise HTTPException(400, "invalid filename")

    be = require_backend()

    if _is_r2_mode():
        recent = await be.list_recent_jobs(limit=1)
        return _gone_in_r2_mode(
            job_id=recent[0].id if recent else None,
            name=name,
        )

    candidates: list[Path] = []
    recent = await be.list_recent_jobs(limit=1)
    if recent:
        candidates.append(DOWNLOADS_DIR / recent[0].id / name)
    candidates.append(DOWNLOADS_DIR / name)

    for path in candidates:
        if path.exists() and path.is_file():
            return FileResponse(
                path, media_type=_media_type_for(name), filename=name,
            )

    raise HTTPException(404, "file not found")


@app.get("/files/{job_id}/{name}")
async def serve_file_for_job(job_id: str, name: str) -> Any:
    """Explicit M2M file endpoint: scoped to a specific job's downloads.

    Filesystem-backed only — in postgres+R2 mode the bytes live in R2
    and the response is a 410 Gone whose body carries the r2_url for
    the matching job_files row (when we can find it) so a
    misconfigured caller has a clear next step.
    """
    if "/" in name or ".." in name or "/" in job_id or ".." in job_id:
        raise HTTPException(400, "invalid path")
    be = require_backend()
    if await be.get_job(job_id=job_id) is None:
        raise HTTPException(404, "job not found")

    if _is_r2_mode():
        files = await be.list_files(job_id=job_id)
        match = next((f for f in files if f.name == name), None)
        detail: dict[str, Any] = {
            "error": "gone",
            "message": (
                "Files are stored in R2 in this deployment. "
                "Use the r2_url below (or read it from "
                "GET /api/jobs/{job_id}.files[]) to fetch the bytes."
            ),
            "job_id": job_id,
            "filename": name,
        }
        if match is not None:
            detail["r2_key"] = match.r2_key
            detail["r2_url"] = match.r2_url
            detail["size_bytes"] = match.size_bytes
        return JSONResponse(status_code=status.HTTP_410_GONE, content=detail)

    file_path = DOWNLOADS_DIR / job_id / name
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(404, "file not found")
    return FileResponse(
        file_path,
        media_type=_media_type_for(name),
        filename=name,
    )


# --------------------------------------------------------------------------
# Run lifecycle
# --------------------------------------------------------------------------


async def _run_job(job_id: str, req: StartRequest) -> None:
    """Drive a single scraping job to completion against the backend.

    Owns the full lifecycle of one job:

      1. Wait on the global semaphore so we never run more than
         MAX_CONCURRENT_JOBS Playwright browsers in the same process.
      2. Claim the job atomically (queued → running) via the backend.
         If another worker beat us to it (or the job was cancelled
         before we got here), exit cleanly.
      3. Spin up the engine with two callbacks pointed at the backend:
         progress_callback → backend.append_event (also broadcast to WS)
         file_callback     → backend.save_file   (filesystem OR R2)
      4. Run a periodic heartbeat task on the side so the reaper
         doesn't murder this job during a long DIAN pause.
      5. On engine return / exception / cancel: mark the terminal
         state on the backend and broadcast the final lifecycle event.
      6. Clean up the per-job dict entries so old jobs don't leak.

    This function is the ONLY caller of backend.claim_job /
    mark_completed / mark_failed / mark_cancelled — keeping the state
    machine in one place makes it trivial to audit.
    """
    assert backend is not None
    assert jobs_semaphore is not None
    cancel_event = cancel_events[job_id]

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    log_path = LOGS_DIR / f"run-{timestamp}-{job_id[:8]}.jsonl"
    file_logger = Logger(log_path)

    # The engine no longer writes ZIPs directly to disk — the backend's
    # save_file is the single point of persistence. This way the legacy
    # InMemoryJobBackend (writes to downloads/{job_id}/{name}) and the
    # PostgresR2JobBackend (uploads to R2) share one code path and one
    # invariant: a file is on disk / in R2 iff it has a job_files row.
    #
    # Why we don't let core.py write too:
    #   - Double write in legacy mode (same bytes, same path) is benign
    #     but wastes IO and confuses anyone reading the code.
    #   - The audit trail would say "file written" twice for one ZIP.
    #   - The two paths could drift; this keeps them lockstep.
    write_to_disk = False
    # We still create the per-job downloads subdir up front in legacy
    # mode so InMemoryJobBackend.save_file's mkdir() never races with
    # concurrent jobs trying to mkdir the same parent. Skipped in R2
    # mode because we don't want to leave empty dirs lying around.
    if not isinstance(backend, PostgresR2JobBackend):
        (DOWNLOADS_DIR / job_id).mkdir(parents=True, exist_ok=True)

    # --- Helpers bound to this job ---------------------------------------

    async def emit_lifecycle(status: str, message: str) -> None:
        """Append a synthetic lifecycle event to the backend log + WS.

        Mirrors what the engine's per-CUFE events look like so the UI
        renders both kinds in a single timeline without special-casing.
        """
        await _emit_event(
            job_id=job_id,
            source="worker",
            phase="lifecycle",
            status=status,
            message=message,
        )

    async def on_engine_event(event: DownloadEvent) -> None:
        """progress_callback → backend + WS broadcast."""
        payload = asdict(event)
        await _emit_event(
            job_id=job_id,
            source="engine",
            phase=event.phase,
            status=event.status,
            message=event.notes,
            payload=payload,
        )

    async def on_file_saved(event: FileSavedEvent) -> None:
        """file_callback → backend.save_file (filesystem OR R2 upload).

        Errors raised here are caught by core._emit_file and would
        otherwise vanish silently. We explicitly log them so a future
        regression doesn't leave the operator staring at files: []
        without a clue why.

        The old version called _parse_iso_date which raises
        HTTPException on bad input. DIAN's listing returns dates as
        DD-MM-YYYY (e.g. '08-06-2026'), not ISO, so EVERY successful
        download was hitting that and the exception was being
        swallowed by core._emit_file. _parse_dian_date tolerates
        both shapes and falls back to None — the file row still
        lands, just without the issue_date field populated.
        """
        try:
            issue_date = _parse_dian_date(event.issue_date)
            await backend.save_file(
                job_id=job_id,
                company_id=req.company_id,
                cufe=event.cufe or None,
                prefijo_folio=event.prefijo_folio or None,
                issuer_nit=event.issuer_nit or None,
                issue_date=issue_date,
                filename=event.filename,
                body=event.body,
                kind="zip",
            )
        except Exception:
            logger.exception(
                "on_file_saved failed for job=%s file=%s — file will not "
                "appear in /api/jobs/{id}.files[]",
                job_id, event.filename,
            )
            # Re-raise so callers higher up can react. core._emit_file
            # still swallows it (its contract is best-effort) but the
            # log line above is what the operator needs.
            raise

    async def heartbeat_loop() -> None:
        """Bump worker_heartbeat every WORKER_HEARTBEAT_SECONDS.

        Stops as soon as the job leaves status='running' (the backend
        returns False from heartbeat()) so we don't keep pinging after
        cancellation or failure.
        """
        while True:
            try:
                await asyncio.sleep(WORKER_HEARTBEAT_SECONDS)
                alive = await backend.heartbeat(job_id=job_id)
                if not alive:
                    return
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception(
                    "heartbeat failed for job %s; will retry", job_id,
                )

    # --- Lifecycle -------------------------------------------------------

    async with jobs_semaphore:
        # Cancelled before we even got a slot? Honor it.
        if cancel_event.is_set():
            await backend.mark_cancelled(
                job_id=job_id, reason="Cancelled before worker pickup",
            )
            await emit_lifecycle("cancelled", "Cancelled before worker pickup.")
            return

        claimed = await backend.claim_job(job_id=job_id, worker_id=WORKER_ID)
        if claimed is None:
            # Someone else claimed it, or the job moved to a terminal
            # state while we waited on the semaphore. Either way: we
            # have nothing to do.
            logger.info(
                "Job %s could not be claimed (already running or terminal); skipping",
                job_id,
            )
            return

        await emit_lifecycle(
            "started", "Job aceptado por el scraper, abriendo browser.",
        )

        hb_task = asyncio.create_task(heartbeat_loop())
        terminal_status: str = "failed"
        terminal_error: str | None = None
        # When the scraper raises a ScraperError (auth_expired,
        # captcha_blocked, etc.) we forward its `kind` so NUVARA can
        # render an actionable message instead of a generic stack
        # trace. Defaults to None (legacy callers will see the same
        # opaque 'failed' status they always did).
        terminal_error_kind: str | None = None
        summary: dict[str, Any] | None = None
        # ScraperError instances need scraper.logger.summary() AFTER the
        # except branch picks them up — we keep a reference outside the
        # `async with` so the post-flight branch can read it without
        # touching the closed scraper.
        scraper_ref: DianTestScraper | None = None
        # When True we've already marked the job's terminal state and
        # emitted the lifecycle event. The `finally` block uses this to
        # avoid double-marking when the browser cleanup raises.
        marked_terminal: bool = False

        async def _mark_terminal_once() -> None:
            """Persist terminal status + emit lifecycle event exactly once.

            Why this exists: we used to do this work in the outer
            `finally` block, AFTER the `async with DianTestScraper(...)`
            had finished its `__aexit__` (browser teardown). On every
            run that did paginated DOM scraping, Playwright's
            `context.close()` / `browser.close()` calls in __aexit__
            could block for 30-120s waiting on in-flight requests
            (downloads, navigations, network idle handlers).

            During that window NUVARA's `pollJobUntilDone` saw `status =
            running` because we hadn't called `backend.mark_completed`
            yet — even though every invoice was already downloaded,
            persisted to R2, and emitted via `on_file_saved`. Operators
            reported "NUVARA quedó en `en curso` aunque el scraper ya
            terminó hace rato". This bug.

            We now mark terminal IMMEDIATELY after the engine returns
            from `.run()` (or raises), BEFORE the `async with` exits.
            Browser cleanup still runs in its own finally and is best-
            effort; if it crashes we don't undo the terminal mark.
            """
            nonlocal marked_terminal
            if marked_terminal:
                return
            marked_terminal = True
            try:
                if terminal_status == "completed" and summary is not None:
                    await backend.mark_completed(
                        job_id=job_id, summary=summary,
                    )
                elif terminal_status == "cancelled":
                    await backend.mark_cancelled(
                        job_id=job_id,
                        reason=terminal_error or "Cancelled by user",
                    )
                else:
                    await backend.mark_failed(
                        job_id=job_id,
                        error=terminal_error or "Unknown failure",
                        error_kind=terminal_error_kind,
                    )
            except Exception:
                logger.exception(
                    "Could not record terminal state for job %s", job_id,
                )
            try:
                await emit_lifecycle(
                    terminal_status, f"Estado final: {terminal_status}",
                )
            except Exception:
                logger.exception(
                    "Could not emit terminal lifecycle event for job %s",
                    job_id,
                )

        try:
            try:
                async with DianTestScraper(
                    auth_url=req.auth_url,
                    start_date=req.start_date,
                    end_date=req.end_date,
                    max_invoices=req.max_invoices,
                    downloads_dir=DOWNLOADS_DIR / job_id,
                    logger=file_logger,
                    progress_callback=on_engine_event,
                    file_callback=on_file_saved,
                    write_to_disk=write_to_disk,
                    headless=req.headless,
                    delay_min_ms=req.delay_min_ms,
                    delay_max_ms=req.delay_max_ms,
                    long_pause_every=req.long_pause_every,
                    # The consumer's known-CUFE list lands here. core
                    # filters them out at listing time so the per-CUFE
                    # download loop never sees them.
                    skip_cufes=req.skip_cufes,
                    # Which DIAN bucket to navigate to. core uses
                    # /Document/Received for 'purchase' (default) and
                    # /Document/Sent for 'sale'. The DataTables form
                    # layout is identical on both, so the rest of the
                    # engine is direction-agnostic.
                    direction=req.direction,
                    # Optional docType narrow-down. None == legacy
                    # "all doctypes after the always-on pre-filter"
                    # behaviour.
                    doc_type_filter=req.doc_type_filter,
                    cancel_event=cancel_event,
                ) as scraper:
                    scraper_ref = scraper
                    try:
                        summary = await scraper.run()
                        terminal_status = (
                            "cancelled" if cancel_event.is_set() else "completed"
                        )
                        await _emit_event(
                            job_id=job_id,
                            source="engine",
                            phase="summary",
                            status="info",
                            message=(
                                f"Engine summary: total={summary.get('total')} "
                                f"ok={summary.get('ok')} failed={summary.get('failed')}"
                            ),
                            payload=summary,
                        )
                    except asyncio.CancelledError:
                        terminal_status = "cancelled"
                        terminal_error = "Cancelled by server shutdown"
                        # Mark terminal NOW so NUVARA sees `cancelled`
                        # without waiting for browser teardown.
                        await _mark_terminal_once()
                        raise
                    except ScraperError as e:
                        terminal_status = "failed"
                        terminal_error = str(e)
                        terminal_error_kind = e.kind
                        try:
                            summary = scraper.logger.summary()
                        except Exception:
                            summary = None
                        try:
                            await emit_lifecycle(
                                "failed",
                                f"[{e.kind}] {terminal_error}",
                            )
                        except Exception:
                            logger.exception(
                                "Could not emit failure lifecycle event for job %s",
                                job_id,
                            )
                    except Exception as e:
                        terminal_status = "failed"
                        terminal_error = f"{type(e).__name__}: {e}"
                        terminal_error_kind = ERROR_KIND_ENGINE_CRASH
                        try:
                            await emit_lifecycle("failed", terminal_error)
                        except Exception:
                            logger.exception(
                                "Could not emit crash lifecycle event for job %s",
                                job_id,
                            )

                    # Mark terminal BEFORE leaving the `async with`. The
                    # `__aexit__` that follows can block on Playwright
                    # teardown for tens of seconds; NUVARA shouldn't
                    # wait for that to learn the job is done.
                    await _mark_terminal_once()
            except asyncio.CancelledError:
                # Re-raise so the cancellation propagates to the worker
                # task — _mark_terminal_once was already called inside
                # the inner try.
                raise
            except Exception:
                # The `async with` itself raised during __aexit__ (most
                # likely Playwright failing to close the browser). We
                # already marked terminal inside; just log so the
                # operator can spot a recurring teardown issue.
                logger.exception(
                    "Browser teardown raised after terminal mark for job %s",
                    job_id,
                )
        finally:
            hb_task.cancel()
            try:
                await hb_task
            except (asyncio.CancelledError, Exception):
                pass

            # Belt-and-suspenders: if we somehow never marked terminal
            # (e.g. an exception fired BEFORE entering the inner
            # try/except), do it now so the job never strands as
            # `running`. _mark_terminal_once guards re-entry.
            await _mark_terminal_once()

            file_logger.close()
            # Cleanup per-job in-process state. We do NOT delete from
            # the backend — that's the historical record.
            cancel_events.pop(job_id, None)
            running_tasks.pop(job_id, None)
            # Reference cleanup so the closed scraper doesn't pin the
            # browser context in memory.
            scraper_ref = None  # noqa: F841


async def _emit_event(
    *,
    job_id: str,
    source: str,
    phase: str,
    status: str,
    message: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    """Single point of truth for appending an event.

    Writes to the backend (durable log) and broadcasts to WebSocket
    listeners (live UI). The WS push shape carries the new `job_id`
    at the top level so per-job WS filters work, plus the legacy
    field aliases (timestamp, notes, cufe, prefijo_folio, sequence)
    that the existing static/index.html consumes — until that UI is
    rewritten, both consumers coexist with zero cost (every key is
    a single dict lookup).

    Backend write failures propagate. WS failures evict the failing
    client without breaking the write.
    """
    assert backend is not None
    event_row = await backend.append_event(
        job_id=job_id,
        source=source,
        phase=phase,
        status=status,
        message=message,
        payload=payload,
    )
    ws_message = {
        "type": "event",
        "job_id": job_id,
        "payload": {
            "event_index": event_row.event_index,
            "source": source,
            "phase": phase,
            "status": status,
            "message": message,
            "payload": payload,
            "occurred_at": event_row.occurred_at.isoformat(),
            # Legacy fields the existing static/index.html still reads.
            # Cheap to include; removed once the UI rewrite lands.
            "timestamp": event_row.occurred_at.isoformat(),
            "notes": message,
            "cufe": (payload or {}).get("cufe", ""),
            "prefijo_folio": (payload or {}).get("prefijo_folio", ""),
            "sequence": (payload or {}).get("sequence", 0),
        },
    }
    await _broadcast_ws(ws_message)


async def _enqueue_job(req: StartRequest) -> JobRow:
    """Create a job in the backend and schedule its worker task.

    Returns the JobRow as persisted by the backend so the caller can
    surface the same shape it does for GET /api/jobs/{id} (status,
    timestamps, etc.) instead of inventing a parallel view.

    No global lock: the backend is the source of truth, the semaphore
    bounds concurrency at the engine layer, and queued jobs are a
    first-class state. Submitting while at capacity just enqueues
    and the next-freed worker picks it up.
    """
    be = require_backend()
    job_id = uuid.uuid4().hex
    start_date = _parse_iso_date(req.start_date)
    end_date = _parse_iso_date(req.end_date)
    row = await be.create_job(
        job_id=job_id,
        company_id=req.company_id,
        auth_url=req.auth_url,
        start_date=start_date,
        end_date=end_date,
        max_invoices=req.max_invoices,
    )
    cancel_events[job_id] = asyncio.Event()
    running_tasks[job_id] = asyncio.create_task(_run_job(job_id, req))
    return row


# --------------------------------------------------------------------------
# Helpers — map backend rows to the legacy JSON shape so consumers
# (NUVARA's adapter, static/index.html) don't need to change in this
# commit. Commit 3 deprecates the legacy shape and exposes the richer
# JobRow fields directly.
# --------------------------------------------------------------------------


def _file_url_for(row: JobFileRow) -> str:
    """Where to fetch a file from.

    - Postgres+R2 backend  → the R2 URL the engine already persisted.
      NUVARA fetches the bytes directly from R2 (no second hop here).
    - Legacy local backend → the existing /files/{job_id}/{name} route.
    """
    if row.r2_url:
        return row.r2_url
    return f"/files/{row.job_id}/{row.name}"


def _file_to_dict(row: JobFileRow) -> dict[str, Any]:
    return {
        "name": row.name,
        "kind": row.kind,
        "size_bytes": row.size_bytes,
        "cufe": row.cufe,
        "prefijo_folio": row.prefijo_folio,
        "issuer_nit": row.issuer_nit,
        "issue_date": row.issue_date.isoformat() if row.issue_date else None,
        "url": _file_url_for(row),
        "r2_key": row.r2_key,
    }


def _job_to_dict(row: JobRow, files: list[JobFileRow]) -> dict[str, Any]:
    return {
        "job_id": row.id,
        "status": row.status,
        "company_id": row.company_id,
        "created_at": row.created_at.isoformat(),
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": (
            row.finished_at.isoformat() if row.finished_at else None
        ),
        "summary": row.summary,
        "error": row.error,
        # NUVARA reads this to render a contextual UI message instead
        # of a stack trace. Stable string vocabulary documented in
        # core.py (auth_expired / auth_expired_midrun / captcha_blocked
        # / timeout / engine_crash). Null on completed runs and on
        # rows the legacy engine couldn't classify.
        "error_kind": row.error_kind,
        "files": [_file_to_dict(f) for f in files],
    }


@app.post("/api/jobs")
async def create_job(req: StartRequest) -> dict[str, Any]:
    """M2M entrypoint. Returns the job_id the caller must poll on
    `/api/jobs/{job_id}` to retrieve final state + file list.

    Unlike the previous implementation this never returns 409: the
    backend can hold an unbounded queue of jobs and the semaphore
    drains them one MAX_CONCURRENT_JOBS slot at a time. NUVARA's
    fire-and-poll flow keeps working unchanged.
    """
    row = await _enqueue_job(req)
    return {
        "ok": True,
        "job_id": row.id,
        "status": row.status,
        "poll_url": f"/api/jobs/{row.id}",
    }


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str) -> dict[str, Any]:
    """Returns the full state of a job — status, summary, error and
    the list of files persisted so far (with metadata + fetch URL).

    Safe to poll: idempotent read, no side effects."""
    be = require_backend()
    row = await be.get_job(job_id=job_id)
    if row is None:
        raise HTTPException(404, "job not found")
    files = await be.list_files(job_id=job_id)
    return _job_to_dict(row, files)


@app.get("/api/jobs/{job_id}/events")
async def get_job_events(
    job_id: str,
    since: int = 0,
    limit: int = 200,
) -> dict[str, Any]:
    """Cursor-paged event log for a job.

    Returns events with `event_index > since`. The backend stores the
    monotonic index per job; NUVARA persists the highest seen value
    and replays it as `since` on every poll so this stays cheap.
    """
    be = require_backend()
    row = await be.get_job(job_id=job_id)
    if row is None:
        raise HTTPException(404, "job not found")

    if limit <= 0 or limit > 1000:
        limit = 200
    if since < 0:
        since = 0

    events, next_since = await be.list_events_since(
        job_id=job_id, since=since, limit=limit,
    )
    serialized = [
        {
            "event_index": e.event_index,
            "source": e.source,
            "phase": e.phase,
            "status": e.status,
            "message": e.message,
            "payload": e.payload,
            "occurred_at": e.occurred_at.isoformat(),
            # Legacy aliases used by the existing UI templates.
            "timestamp": e.occurred_at.isoformat(),
            "notes": e.message,
            "cufe": (e.payload or {}).get("cufe", ""),
            "prefijo_folio": (e.payload or {}).get("prefijo_folio", ""),
            "sequence": (e.payload or {}).get("sequence", 0),
        }
        for e in events
    ]
    return {
        "job_id": row.id,
        "status": row.status,
        "events": serialized,
        "next_since": next_since,
        # `total` is approximate — derived from next_since on this page.
        # Commit 2 will expose a proper count from the backend.
        "total": next_since,
    }


@app.post("/api/start")
async def start_run_legacy(req: StartRequest) -> dict[str, Any]:
    """Legacy entrypoint kept for the in-repo human UI.

    Same semantics as POST /api/jobs but with the response shape
    the original UI expects. M2M callers should always use /api/jobs.
    """
    row = await _enqueue_job(req)
    return {"ok": True, "message": "Run started.", "job_id": row.id}


class CancelRequest(BaseModel):
    job_id: str


@app.post("/api/cancel")
async def cancel_run(req: CancelRequest) -> dict[str, Any]:
    """Cancel a specific job by id.

    The previous global `/api/cancel` (no body, cancels "the" run)
    doesn't survive concurrency — there's no longer a single run to
    cancel. Callers MUST send `{ "job_id": "..." }`. The scraper UI
    in static/index.html is updated in the same commit.
    """
    be = require_backend()
    ev = cancel_events.get(req.job_id)
    if ev is not None:
        ev.set()
    row = await be.mark_cancelled(
        job_id=req.job_id, reason="Cancellation requested by user",
    )
    if row is None and ev is None:
        # Neither in-process nor in the backend — the id is bogus
        # or the job already reached a terminal state.
        return {"ok": False, "message": "No active job with that id."}
    await _emit_event(
        job_id=req.job_id,
        source="worker",
        phase="lifecycle",
        status="info",
        message="Cancellation requested by user.",
    )
    return {"ok": True, "message": "Cancellation signal sent."}


@app.get("/api/status")
async def get_status() -> dict[str, Any]:
    """Coarse status endpoint kept for the in-repo human UI.

    With concurrent jobs there is no single "current run", so we
    surface the most recent job and the count of in-flight tasks.
    The richer query surface lives on /api/jobs/{id} and the listing
    endpoint commit 2 will add.
    """
    be = require_backend()
    recent = await be.list_recent_jobs(limit=1)
    last = recent[0] if recent else None
    return {
        "is_running": bool(running_tasks),
        "running_count": len(running_tasks),
        "max_concurrent": MAX_CONCURRENT_JOBS,
        "current_job_id": last.id if last else None,
        "current_job_status": last.status if last else None,
    }


# --------------------------------------------------------------------------
# WebSocket
# --------------------------------------------------------------------------


def _ws_filter_matches(ws: WebSocket, message: dict[str, Any]) -> bool:
    """True if this WebSocket should receive this event.

    A client connecting with `?job_id=<id>` opts into a single job's
    stream — useful for the human UI when running multiple jobs in
    parallel. Without the query param the client gets every event
    (the legacy behaviour the existing UI assumes).
    """
    wanted = ws.query_params.get("job_id")
    if not wanted:
        return True
    return message.get("job_id") == wanted


async def _broadcast_ws(message: dict[str, Any]) -> None:
    """Push `message` to every WebSocket client that wants it.

    Snapshotting `ws_clients` before iterating because we mutate it
    on send failures. A slow client doesn't block other recipients —
    each send is awaited but the WS lib buffers internally; if a
    client truly stalls past its TCP window we'll catch the
    exception and evict it.
    """
    ws_recent_events.append(message)
    for ws in list(ws_clients):
        if not _ws_filter_matches(ws, message):
            continue
        try:
            await ws.send_json(message)
        except Exception:
            ws_clients.discard(ws)


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket) -> None:
    """Live event broadcast for in-flight jobs.

    Optional query param `?job_id=<id>` filters the stream to one
    job. Without it the client receives every event from every job —
    useful for the global operator dashboard, less useful when you
    have ten jobs running in parallel and only care about one.

    Reconnecting clients see the most recent ws_recent_events
    (respecting the same filter) so a refreshing browser tab
    doesn't go blank for a few seconds.
    """
    if not _assert_ws_authorized(websocket):
        # 1008 = policy violation. Close before accept so the client gets
        # the rejection on the handshake itself.
        await websocket.close(code=1008, reason="missing or invalid api key")
        return

    await websocket.accept()
    ws_clients.add(websocket)
    try:
        # Replay recent events the client cares about. Same filter
        # the live stream uses, applied to the ring buffer.
        for ev in list(ws_recent_events):
            if _ws_filter_matches(websocket, ev):
                await websocket.send_json(ev)
        while True:
            # Keep connection alive — we don't expect client messages
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        ws_clients.discard(websocket)


# --------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8765, reload=False)
