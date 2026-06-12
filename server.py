"""Web server for DIAN scraper test — UI tipo Nuvara.

Run with:
    python server.py
    # Then open http://localhost:8765/
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from core import (
    DianTestScraper,
    DownloadEvent,
    Logger,
)

BASE_DIR = Path(__file__).resolve().parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
LOGS_DIR = BASE_DIR / "logs"
STATIC_DIR = BASE_DIR / "static"

DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
STATIC_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------------
# State (single run at a time — this is a local test tool, not multi-user)
# --------------------------------------------------------------------------


class RunState:
    def __init__(self) -> None:
        self.task: asyncio.Task | None = None
        self.cancel_event: asyncio.Event = asyncio.Event()
        self.clients: set[WebSocket] = set()
        self.events: list[dict[str, Any]] = []
        self.is_running: bool = False
        self.lock = asyncio.Lock()

    async def broadcast(self, message: dict[str, Any]) -> None:
        self.events.append(message)
        # Snapshot to avoid mutation during iteration
        for ws in list(self.clients):
            try:
                await ws.send_json(message)
            except Exception:
                self.clients.discard(ws)


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


@app.get("/files/{name}")
async def serve_file(name: str) -> FileResponse:
    """Serve a downloaded file (PDF/XML/ZIP) — sanitized."""
    if "/" in name or ".." in name:
        raise HTTPException(400, "invalid filename")
    file_path = DOWNLOADS_DIR / name
    if not file_path.exists():
        raise HTTPException(404, "file not found")
    media_type = "application/octet-stream"
    if name.lower().endswith(".pdf"):
        media_type = "application/pdf"
    elif name.lower().endswith(".xml"):
        media_type = "application/xml"
    elif name.lower().endswith(".zip"):
        media_type = "application/zip"
    return FileResponse(file_path, media_type=media_type, filename=name)


# --------------------------------------------------------------------------
# Run lifecycle
# --------------------------------------------------------------------------


async def _run_scraper(req: StartRequest) -> None:
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    log_path = LOGS_DIR / f"run-{timestamp}.jsonl"
    logger = Logger(log_path)

    async def cb(event: DownloadEvent) -> None:
        payload = asdict(event)
        await state.broadcast({"type": "event", "payload": payload})

    await state.broadcast(
        {
            "type": "status",
            "payload": {"running": True, "log_file": log_path.name},
        }
    )

    try:
        async with DianTestScraper(
            auth_url=req.auth_url,
            start_date=req.start_date,
            end_date=req.end_date,
            max_invoices=req.max_invoices,
            downloads_dir=DOWNLOADS_DIR,
            logger=logger,
            progress_callback=cb,
            headless=req.headless,
            delay_min_ms=req.delay_min_ms,
            delay_max_ms=req.delay_max_ms,
            long_pause_every=req.long_pause_every,
            cancel_event=state.cancel_event,
        ) as scraper:
            summary = await scraper.run()
            await state.broadcast({"type": "summary", "payload": summary})
    except Exception as e:
        await state.broadcast(
            {
                "type": "error",
                "payload": {"error": f"{type(e).__name__}: {e}"},
            }
        )
    finally:
        logger.close()
        state.is_running = False
        await state.broadcast(
            {"type": "status", "payload": {"running": False}}
        )


@app.post("/api/start")
async def start_run(req: StartRequest) -> dict[str, Any]:
    async with state.lock:
        if state.is_running:
            raise HTTPException(409, "A run is already in progress.")
        state.is_running = True
        state.cancel_event = asyncio.Event()
        # Reset event buffer for fresh run
        state.events = []
        state.task = asyncio.create_task(_run_scraper(req))
    return {"ok": True, "message": "Run started."}


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
        }
    )
    return {"ok": True, "message": "Cancellation signal sent."}


@app.get("/api/status")
async def get_status() -> dict[str, Any]:
    return {"is_running": state.is_running, "events_count": len(state.events)}


# --------------------------------------------------------------------------
# WebSocket
# --------------------------------------------------------------------------


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket) -> None:
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
