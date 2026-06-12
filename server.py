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
import json
import os
import secrets
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Awaitable

import uvicorn
from fastapi import (
    Depends,
    FastAPI,
    Header,
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

from core import (
    DianTestScraper,
    DownloadEvent,
    Logger,
)

BASE_DIR = Path(__file__).resolve().parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
LOGS_DIR = BASE_DIR / "logs"
STATIC_DIR = BASE_DIR / "static"

# Shared secret consumed by `/api/*` and `/files/*` endpoints.
# When set, every machine-to-machine request must carry it. When empty,
# the server runs unauthenticated (legacy single-operator local dev).
SCRAPER_API_KEY: str = os.environ.get("SCRAPER_API_KEY", "").strip()

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
# State (single run at a time — this is still a single-tenant tool, but the
# RunState tracks the current job by id so M2M consumers can correlate
# results without polling websockets)
# --------------------------------------------------------------------------


JobStatus = str  # "queued" | "running" | "completed" | "failed" | "cancelled"


class RunState:
    def __init__(self) -> None:
        self.task: asyncio.Task | None = None
        self.cancel_event: asyncio.Event = asyncio.Event()
        self.clients: set[WebSocket] = set()
        # Live broadcast buffer — flushed on every new job so the UI shows
        # only the current run.
        self.events: list[dict[str, Any]] = []
        self.is_running: bool = False
        self.lock = asyncio.Lock()
        # Current and historical jobs, keyed by job_id.
        self.jobs: dict[str, JobRecord] = {}
        # Most recent job (used by the legacy /api/status endpoint that
        # the UI still consumes).
        self.current_job_id: str | None = None

    async def broadcast(self, message: dict[str, Any]) -> None:
        self.events.append(message)
        # Snapshot to avoid mutation during iteration
        for ws in list(self.clients):
            try:
                await ws.send_json(message)
            except Exception:
                self.clients.discard(ws)


class JobRecord:
    """Bookkeeping for a single scraping job.

    Holds the input that started it, the current status, the per-CUFE
    download events emitted by the engine, and a `files` index that lists
    every file persisted under the job's dedicated downloads subdirectory.

    The shape is intentionally JSON-serializable so `GET /api/jobs/{id}`
    can stream it to the M2M consumer without extra mapping.
    """

    def __init__(self, job_id: str, request: "StartRequest") -> None:
        self.id = job_id
        self.request = request
        self.status: JobStatus = "queued"
        self.created_at = datetime.utcnow().isoformat()
        self.started_at: str | None = None
        self.finished_at: str | None = None
        self.log_file: str | None = None
        self.events: list[dict[str, Any]] = []
        self.summary: dict[str, Any] | None = None
        self.error: str | None = None

    @property
    def downloads_dir(self) -> Path:
        return DOWNLOADS_DIR / self.id

    def files(self) -> list[dict[str, Any]]:
        """List every file that lives under the job's downloads subdir.

        Pulls metadata (cufe, prefijo_folio, http_status) from the matching
        download event when available so the M2M caller can correlate the
        binary back to its DIAN invoice in a single read.
        """
        if not self.downloads_dir.exists():
            return []

        # Index events by safe_id prefix (cufe[:20]) — matches the
        # filename convention in core._download_invoice().
        by_safe_id: dict[str, dict[str, Any]] = {}
        for ev in self.events:
            if ev.get("phase") != "download" or ev.get("status") != "ok":
                continue
            cufe = ev.get("cufe") or ""
            safe_id = cufe[:20] or f"seq-{ev.get('sequence', 0)}"
            by_safe_id[safe_id] = ev

        out: list[dict[str, Any]] = []
        for path in sorted(self.downloads_dir.iterdir()):
            if not path.is_file():
                continue
            safe_id = path.stem
            ev = by_safe_id.get(safe_id, {})
            kind = path.suffix.lstrip(".").lower() or "bin"
            out.append(
                {
                    "name": path.name,
                    "kind": kind,
                    "size_bytes": path.stat().st_size,
                    "cufe": ev.get("cufe"),
                    "prefijo_folio": ev.get("prefijo_folio"),
                    "issuer_nit": ev.get("issuer_nit"),
                    "issue_date": ev.get("issue_date"),
                    "url": f"/files/{self.id}/{path.name}",
                }
            )
        return out

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.id,
            "status": self.status,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "log_file": self.log_file,
            "summary": self.summary,
            "error": self.error,
            "files": self.files(),
            "events_count": len(self.events),
        }


state = RunState()


# --------------------------------------------------------------------------
# Request / Response models
# --------------------------------------------------------------------------


class StartRequest(BaseModel):
    auth_url: str
    start_date: str = Field(..., description="YYYY-MM-DD")
    end_date: str = Field(..., description="YYYY-MM-DD")
    max_invoices: int = 30
    headless: bool = True
    delay_min_ms: int = 5000
    delay_max_ms: int = 13000
    long_pause_every: int = 30


# --------------------------------------------------------------------------
# Lifespan
# --------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # Cancel any pending run on shutdown
    if state.task and not state.task.done():
        state.cancel_event.set()
        state.task.cancel()


app = FastAPI(title="DIAN Scraper Test", lifespan=lifespan)
app.add_middleware(ApiKeyMiddleware)


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    """Liveness probe — always 200 when the process is up.

    Public on purpose so Dokploy / load balancer healthchecks don't need
    the API key.
    """
    return {
        "status": "ok",
        "auth_required": bool(SCRAPER_API_KEY),
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


@app.get("/files/{name}")
async def serve_file_legacy(name: str) -> FileResponse:
    """Legacy file endpoint kept for the human UI.

    The UI used to assume a flat downloads/ directory and a single live
    run. We keep this working by resolving against the CURRENT job's
    downloads subdir when it exists, falling back to the flat layout for
    historical compatibility.

    M2M consumers should use `/files/{job_id}/{name}` instead — it's
    explicit and isolates jobs.
    """
    if "/" in name or ".." in name:
        raise HTTPException(400, "invalid filename")

    candidates: list[Path] = []
    if state.current_job_id:
        candidates.append(DOWNLOADS_DIR / state.current_job_id / name)
    candidates.append(DOWNLOADS_DIR / name)

    for path in candidates:
        if path.exists() and path.is_file():
            return FileResponse(path, media_type=_media_type_for(name), filename=name)

    raise HTTPException(404, "file not found")


@app.get("/files/{job_id}/{name}")
async def serve_file_for_job(job_id: str, name: str) -> FileResponse:
    """Explicit M2M file endpoint: scoped to a specific job's downloads."""
    if "/" in name or ".." in name or "/" in job_id or ".." in job_id:
        raise HTTPException(400, "invalid path")
    if job_id not in state.jobs:
        raise HTTPException(404, "job not found")
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


async def _run_job(job: JobRecord) -> None:
    """Drive a single scraping job to completion.

    Pipes engine events into both the job record (for /api/jobs/{id}) and
    the broadcast channel (for the live UI). Persists files under the
    job's dedicated subdirectory so concurrent jobs never collide.
    """
    job.downloads_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    log_path = LOGS_DIR / f"run-{timestamp}-{job.id[:8]}.jsonl"
    logger = Logger(log_path)
    job.log_file = log_path.name

    job.status = "running"
    job.started_at = datetime.utcnow().isoformat()

    async def cb(event: DownloadEvent) -> None:
        payload = asdict(event)
        job.events.append(payload)
        await state.broadcast(
            {"type": "event", "payload": payload, "job_id": job.id}
        )

    await state.broadcast(
        {
            "type": "status",
            "payload": {"running": True, "log_file": log_path.name},
            "job_id": job.id,
        }
    )

    try:
        async with DianTestScraper(
            auth_url=job.request.auth_url,
            start_date=job.request.start_date,
            end_date=job.request.end_date,
            max_invoices=job.request.max_invoices,
            downloads_dir=job.downloads_dir,
            logger=logger,
            progress_callback=cb,
            headless=job.request.headless,
            delay_min_ms=job.request.delay_min_ms,
            delay_max_ms=job.request.delay_max_ms,
            long_pause_every=job.request.long_pause_every,
            cancel_event=state.cancel_event,
        ) as scraper:
            summary = await scraper.run()
            job.summary = summary
            job.status = (
                "cancelled" if state.cancel_event.is_set() else "completed"
            )
            await state.broadcast(
                {"type": "summary", "payload": summary, "job_id": job.id}
            )
    except Exception as e:
        job.status = "failed"
        job.error = f"{type(e).__name__}: {e}"
        await state.broadcast(
            {
                "type": "error",
                "payload": {"error": job.error},
                "job_id": job.id,
            }
        )
    finally:
        logger.close()
        state.is_running = False
        job.finished_at = datetime.utcnow().isoformat()
        await state.broadcast(
            {
                "type": "status",
                "payload": {"running": False, "final_status": job.status},
                "job_id": job.id,
            }
        )


async def _enqueue_job(req: StartRequest) -> JobRecord:
    async with state.lock:
        if state.is_running:
            raise HTTPException(409, "A run is already in progress.")
        job_id = uuid.uuid4().hex
        job = JobRecord(job_id, req)
        state.jobs[job_id] = job
        state.current_job_id = job_id
        state.is_running = True
        state.cancel_event = asyncio.Event()
        # Reset live event buffer so the UI only renders the current run.
        state.events = []
        state.task = asyncio.create_task(_run_job(job))
    return job


@app.post("/api/jobs")
async def create_job(req: StartRequest) -> dict[str, Any]:
    """M2M entrypoint. Returns the job_id the caller must poll on
    `/api/jobs/{job_id}` to retrieve final state + file list."""
    job = await _enqueue_job(req)
    return {
        "ok": True,
        "job_id": job.id,
        "status": job.status,
        "poll_url": f"/api/jobs/{job.id}",
    }


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str) -> dict[str, Any]:
    """Returns the full state of a job — status, summary, error and the
    list of files that have been persisted so far (with metadata and
    per-job download URLs).

    Safe to poll: idempotent read, no side effects."""
    job = state.jobs.get(job_id)
    if job is None:
        raise HTTPException(404, "job not found")
    return job.to_dict()


@app.post("/api/start")
async def start_run_legacy(req: StartRequest) -> dict[str, Any]:
    """Legacy endpoint kept so the existing UI keeps working.

    Internally creates a job_id and routes through the same lifecycle as
    `/api/jobs`. M2M callers should use `/api/jobs` directly so they get
    the job_id back in the same response shape they expect."""
    job = await _enqueue_job(req)
    return {"ok": True, "message": "Run started.", "job_id": job.id}


@app.post("/api/cancel")
async def cancel_run() -> dict[str, Any]:
    if not state.is_running:
        return {"ok": False, "message": "No run in progress."}
    state.cancel_event.set()
    await state.broadcast(
        {
            "type": "event",
            "payload": {
                "phase": "log",
                "status": "info",
                "notes": "Cancellation requested by user.",
                "timestamp": datetime.utcnow().isoformat(),
                "sequence": 0,
                "cufe": "",
                "prefijo_folio": "",
            },
            "job_id": state.current_job_id,
        }
    )
    return {"ok": True, "message": "Cancellation signal sent."}


@app.get("/api/status")
async def get_status() -> dict[str, Any]:
    """Legacy status endpoint for the UI.

    Returns the global "is a run in flight?" flag plus the current job's
    id so an M2M caller that already authenticated can short-circuit by
    polling /api/jobs/{job_id} instead of the websocket.
    """
    return {
        "is_running": state.is_running,
        "events_count": len(state.events),
        "current_job_id": state.current_job_id,
    }


# --------------------------------------------------------------------------
# WebSocket
# --------------------------------------------------------------------------


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket) -> None:
    if not _assert_ws_authorized(websocket):
        # 1008 = policy violation. Close before accept so the client gets
        # the rejection on the handshake itself.
        await websocket.close(code=1008, reason="missing or invalid api key")
        return

    await websocket.accept()
    state.clients.add(websocket)
    # Send buffered events so a re-connecting client sees history
    try:
        for ev in state.events[-200:]:
            await websocket.send_json(ev)
        while True:
            # Keep connection alive — we don't expect client messages
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        state.clients.discard(websocket)


# --------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8765, reload=False)
