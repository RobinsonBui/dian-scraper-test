"""Core scraper logic — extracted from scraper.py to be reused by the web UI.

This module exposes:
- `DianTestScraper` — the same Playwright-based scraper, but with a
  `progress_callback` so the web server can stream updates over WebSocket.
- `DownloadEvent` — structured event for the live log.
- `FileSavedEvent` — structured event for "a ZIP was downloaded" — carries
  the actual bytes so the consumer (server.py) can persist them to its
  backend of choice (filesystem, R2, …) without re-reading from disk.
- `InvoiceRow` — row from DIAN listing.
"""

from __future__ import annotations

import asyncio
import io
import json
import os
import random
import time
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import unquote, urlparse

from playwright.async_api import (
    APIResponse,
    Browser,
    BrowserContext,
    Page,
    Playwright,
    async_playwright,
)


def _parse_proxy_url(url: str | None) -> dict[str, str] | None:
    """Parse PROXY_URL env var into Playwright proxy dict.

    Accepts: http://user:pass@host:port  /  https://...  /  socks5://...
    Returns None if url is empty/unset so we keep direct connection.

    Playwright wants {"server": "scheme://host:port", "username": "...", "password": "..."}.
    Embedded credentials in server URL are NOT supported by chromium — we have to split them out.
    """
    if not url:
        return None
    parsed = urlparse(url.strip())
    if not parsed.scheme or not parsed.hostname:
        return None
    server = f"{parsed.scheme}://{parsed.hostname}"
    if parsed.port:
        server += f":{parsed.port}"
    proxy: dict[str, str] = {"server": server}
    if parsed.username:
        proxy["username"] = unquote(parsed.username)
    if parsed.password:
        proxy["password"] = unquote(parsed.password)
    return proxy


def _env_float(name: str, default: float) -> float:
    """Read a float from env with a fallback.

    Mirrors `server._env_int`: empty / malformed / non-positive values
    fall back to the default so a typo in Dokploy → Environment can't
    silently shrink an important timeout to zero. Centralised here so
    `core.py` doesn't have to import from `server.py`.
    """
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
        return value if value > 0 else default
    except ValueError:
        return default


DIAN_BASE_URL = "https://catalogo-vpfe.dian.gov.co"
RECEIVED_URL = f"{DIAN_BASE_URL}/Document/Received"
GETFILE_PDF_URL = f"{DIAN_BASE_URL}/Document/GetFilePdf"
DOWNLOAD_ZIP_URL = f"{DIAN_BASE_URL}/Document/DownloadZipFiles"

# Human-like timing config
HUMAN_DELAY_MIN_MS = 5000
HUMAN_DELAY_MAX_MS = 13000
LONG_PAUSE_EVERY_N = 30
LONG_PAUSE_MIN_MS = 60000
LONG_PAUSE_MAX_MS = 120000

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64; rv:131.0) "
    "Gecko/20100101 Firefox/131.0"
)


# --------------------------------------------------------------------------
# Data structures
# --------------------------------------------------------------------------


@dataclass
class InvoiceRow:
    """Row from DIAN Received documents listing."""

    cufe: str
    track_id: str
    prefijo_folio: str
    issuer_nit: str
    issue_date: str
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class DownloadEvent:
    """Single download attempt logged as JSONL."""

    timestamp: str
    sequence: int
    cufe: str
    prefijo_folio: str
    phase: str  # "list" | "download" | "block_detected" | "reauth" | "summary" | "sleep" | "log"
    status: str  # "ok" | "fail" | "block" | "info"
    http_status: int | None = None
    elapsed_ms: int | None = None
    bytes_downloaded: int | None = None
    error: str | None = None
    response_headers: dict[str, str] = field(default_factory=dict)
    notes: str | None = None
    # Extra fields for UI
    pdf_filename: str | None = None
    xml_filename: str | None = None
    issuer_nit: str | None = None
    issue_date: str | None = None
    pdf_b64_size: int | None = None
    xml_preview: str | None = None


ProgressCallback = Callable[[DownloadEvent], Awaitable[None]]


# --------------------------------------------------------------------------
# File hook — "I just downloaded this ZIP, here are the bytes"
# --------------------------------------------------------------------------
#
# Why a separate event/callback instead of stuffing bytes into DownloadEvent:
#   - DownloadEvent is JSON-serialized into the JSONL log and broadcast over
#     WebSocket. Putting raw bytes there would either bloat the log or force
#     a base64 round-trip nobody actually consumes.
#   - The server.py path that needs the bytes (upload to R2 via JobBackend)
#     is a distinct concern from "render an event in the UI". Keeping them
#     split means the engine stays single-purpose and the consumer wires
#     each hook to the subsystem that cares.
#
# Lifecycle: emitted ONCE per successful download, right after the bytes
# have been verified (status=200, body starts with PK, optional fallback
# already swapped in). Never emitted on block/fail — those produce only
# a DownloadEvent.
#
# Failure mode: same as progress_callback — the scraper catches and
# swallows callback exceptions so a bug in the consumer cannot abort an
# otherwise healthy scraping job.


@dataclass
class FileSavedEvent:
    """A ZIP that the engine just downloaded successfully.

    Mirrors the metadata that DownloadEvent carries for the same CUFE so
    the consumer can persist the file row without a cross-event join.
    `body` is the raw ZIP — keep it small (DIAN ZIPs are ~80 KB average,
    <1 MB worst case) so we never need to stream.
    """

    cufe: str
    prefijo_folio: str
    issuer_nit: str | None
    issue_date: str | None
    filename: str
    body: bytes
    size_bytes: int
    sequence: int


FileCallback = Callable[[FileSavedEvent], Awaitable[None]]


# --------------------------------------------------------------------------
# Logger
# --------------------------------------------------------------------------


class Logger:
    def __init__(self, log_path: Path) -> None:
        self.log_path = log_path
        self.events: list[DownloadEvent] = []
        log_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = log_path.open("a", encoding="utf-8")

    def emit(self, event: DownloadEvent) -> None:
        self.events.append(event)
        line = json.dumps(asdict(event), ensure_ascii=False)
        self._fh.write(line + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()

    def summary(self) -> dict[str, Any]:
        downloads = [e for e in self.events if e.phase == "download"]
        ok = sum(1 for e in downloads if e.status == "ok")
        fail = sum(1 for e in downloads if e.status == "fail")
        blocks = sum(1 for e in downloads if e.status == "block")
        latencies = [e.elapsed_ms for e in downloads if e.elapsed_ms is not None]
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        p95_latency = (
            sorted(latencies)[int(len(latencies) * 0.95)] if len(latencies) > 5 else 0
        )
        first_block = next((e for e in downloads if e.status == "block"), None)
        first_fail = next((e for e in downloads if e.status == "fail"), None)
        return {
            "total": len(downloads),
            "ok": ok,
            "fail": fail,
            "blocks": blocks,
            "avg_latency_ms": int(avg_latency),
            "p95_latency_ms": p95_latency,
            "first_block_seq": first_block.sequence if first_block else None,
            "first_fail_seq": first_fail.sequence if first_fail else None,
        }


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def to_dian_date(iso_date: str, end: bool = False) -> str:
    """Convert YYYY-MM-DD to dd/MM/yyyy as DIAN expects."""
    dt = datetime.fromisoformat(iso_date)
    return dt.strftime("%d/%m/%Y")


def detect_block(response: APIResponse, body_preview: bytes) -> tuple[bool, str | None]:
    """Detect if response is a WAF block. Returns (is_blocked, reason)."""
    if response.status == 429:
        return True, "HTTP 429 Too Many Requests"
    if response.status == 403:
        body_text = body_preview.decode("utf-8", errors="replace").lower()
        if "azure" in body_text or "cloudflare" in body_text or "blocked" in body_text:
            return True, "HTTP 403 with WAF signature in body"
        return True, "HTTP 403 (suspected WAF)"
    headers = dict(response.headers)
    if "x-azure-ref" in headers and response.status >= 400:
        return True, f"Azure ref signaled: {headers.get('x-azure-ref')}"
    if "cf-mitigated" in headers:
        return True, f"Cloudflare mitigation: {headers.get('cf-mitigated')}"
    if response.status in (301, 302, 303, 307, 308):
        location = headers.get("location", "")
        if "login" in location.lower() or "/User/" in location:
            return True, f"Redirect to login: {location[:120]}"
    return False, None


def extract_pdf_xml_from_zip(zip_bytes: bytes) -> tuple[bytes | None, str | None, list[str]]:
    """Extract PDF bytes + XML text from a DIAN ZIP. Returns (pdf, xml, filenames).

    Kept for ad-hoc tooling and the legacy UI (if it ever needs it).
    The main scraping path no longer calls this — the ZIP is shipped
    to the consumer as-is so extraction happens at the consumer side,
    where it doesn't burn DIAN-facing time."""
    pdf_bytes: bytes | None = None
    xml_text: str | None = None
    filenames: list[str] = []
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for name in zf.namelist():
                filenames.append(name)
                lower = name.lower()
                if lower.endswith(".pdf") and pdf_bytes is None:
                    pdf_bytes = zf.read(name)
                elif lower.endswith(".xml") and xml_text is None:
                    raw = zf.read(name)
                    try:
                        xml_text = raw.decode("utf-8")
                    except UnicodeDecodeError:
                        xml_text = raw.decode("latin-1", errors="replace")
    except zipfile.BadZipFile:
        pass
    return pdf_bytes, xml_text, filenames


# --------------------------------------------------------------------------
# Scraper
# --------------------------------------------------------------------------


class DianTestScraper:
    def __init__(
        self,
        auth_url: str,
        start_date: str,
        end_date: str,
        max_invoices: int,
        downloads_dir: Path,
        logger: Logger,
        progress_callback: ProgressCallback | None = None,
        file_callback: FileCallback | None = None,
        write_to_disk: bool = True,
        headless: bool = True,
        delay_min_ms: int = HUMAN_DELAY_MIN_MS,
        delay_max_ms: int = HUMAN_DELAY_MAX_MS,
        long_pause_every: int = LONG_PAUSE_EVERY_N,
        cancel_event: asyncio.Event | None = None,
    ) -> None:
        # Two new params (both opt-in, defaults preserve legacy behaviour):
        #
        #   file_callback — invoked once per successful ZIP download with
        #   the raw bytes. server.py uses it to hand the file over to the
        #   active JobBackend (filesystem or R2). When None, the engine
        #   still writes to disk under `downloads_dir` so the CLI path
        #   and any old caller keep working untouched.
        #
        #   write_to_disk — turn off the legacy `downloads_dir/{cufe}.zip`
        #   write. Used when the consumer is responsible for persistence
        #   (e.g. STORAGE_MODE=r2) so the container's filesystem doesn't
        #   accumulate ZIPs we'd otherwise have to garbage-collect. The
        #   default is True so behaviour is unchanged for anyone who
        #   doesn't opt out explicitly.
        self.auth_url = auth_url
        self.start_date = start_date
        self.end_date = end_date
        self.max_invoices = max_invoices
        self.downloads_dir = downloads_dir
        self.logger = logger
        self.progress_callback = progress_callback
        self.file_callback = file_callback
        self.write_to_disk = write_to_disk
        self.headless = headless
        self.delay_min_ms = delay_min_ms
        self.delay_max_ms = delay_max_ms
        self.long_pause_every = long_pause_every
        self.cancel_event = cancel_event or asyncio.Event()

        self.pw: Playwright | None = None
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None
        self.page: Page | None = None
        # Populated by __aenter__: 'chromium' or 'camoufox'. Kept on
        # the instance for diagnostics / logging downstream.
        self.browser_engine: str = "chromium"
        # AsyncCamoufox context manager — only set when
        # BROWSER_ENGINE=camoufox. __aexit__ tears it down.
        self._camoufox_ctx: Any = None

    async def _emit(self, event: DownloadEvent) -> None:
        self.logger.emit(event)
        if self.progress_callback:
            try:
                await self.progress_callback(event)
            except Exception:
                pass

    async def _emit_file(self, event: FileSavedEvent) -> None:
        """Hand a freshly downloaded ZIP to the consumer.

        Mirrors `_emit` semantics: callback errors are caught so a
        misbehaving consumer cannot break an otherwise healthy scraping
        run. We DO surface the failure in the live event log though —
        the previous version swallowed exceptions silently and a
        regression in the server's on_file_saved (an HTTPException
        leaking from a date parser) left the operator with a job
        that downloaded 10 ZIPs successfully but reported files: [].
        Better to broadcast the symptom than to hide it.
        """
        if self.file_callback is None:
            return
        try:
            await self.file_callback(event)
        except Exception as e:
            await self._emit(
                DownloadEvent(
                    timestamp=datetime.utcnow().isoformat(),
                    sequence=event.sequence,
                    cufe=event.cufe,
                    prefijo_folio=event.prefijo_folio,
                    phase="persist",
                    status="fail",
                    error=f"{type(e).__name__}: {e}",
                    notes=(
                        f"file_callback raised while persisting "
                        f"{event.filename}; the ZIP will NOT appear in "
                        f"GET /api/jobs/{{id}}.files[]"
                    ),
                )
            )

    async def _snapshot_for_diagnostics(
        self, *, tag: str, landed_url: str,
    ) -> None:
        """Capture screenshot + HTML of the current page and ship them
        to the consumer as `<tag>.png` and `<tag>.html` files.

        Lives next to `_emit_file` because it uses the same channel:
        when the caller wired a `file_callback` we hand the bytes
        over there (server.py persists to the backend, which in
        legacy mode means the operator can grab them from
        /files/{job_id}/<tag>.png).

        Best-effort by design. If the browser is already dead or the
        callback throws, we log a single event and move on — the
        caller is in the middle of raising a more important error
        and we don't want to mask it.
        """
        # Emit a marker event first so the operator sees the
        # diagnostic was attempted even if the snapshot itself fails.
        await self._emit(
            DownloadEvent(
                timestamp=datetime.utcnow().isoformat(),
                sequence=0,
                cufe="",
                prefijo_folio="",
                phase="auth_diagnostics",
                status="info",
                notes=(
                    f"Capturing snapshot ({tag}). Landed URL: "
                    f"{landed_url[:200]}"
                ),
            )
        )

        if self.page is None or self.file_callback is None:
            return

        try:
            png_bytes = await self.page.screenshot(full_page=True)
        except Exception as e:
            await self._emit(
                DownloadEvent(
                    timestamp=datetime.utcnow().isoformat(),
                    sequence=0,
                    cufe="",
                    prefijo_folio="",
                    phase="auth_diagnostics",
                    status="fail",
                    notes=f"screenshot capture failed: {type(e).__name__}: {e}",
                )
            )
            png_bytes = None

        try:
            html_text = await self.page.content()
            html_bytes = html_text.encode("utf-8", errors="replace")
        except Exception as e:
            await self._emit(
                DownloadEvent(
                    timestamp=datetime.utcnow().isoformat(),
                    sequence=0,
                    cufe="",
                    prefijo_folio="",
                    phase="auth_diagnostics",
                    status="fail",
                    notes=f"html capture failed: {type(e).__name__}: {e}",
                )
            )
            html_bytes = None

        # Ship whatever we managed to capture. The cufe / folio fields
        # don't apply to a diagnostic; we feed empty strings so the
        # backend's row still has a stable shape.
        for kind, name, body in (
            ("png", f"{tag}.png", png_bytes),
            ("html", f"{tag}.html", html_bytes),
        ):
            if not body:
                continue
            try:
                await self.file_callback(
                    FileSavedEvent(
                        cufe="",
                        prefijo_folio="",
                        issuer_nit=None,
                        issue_date=None,
                        filename=name,
                        body=body,
                        size_bytes=len(body),
                        sequence=0,
                    )
                )
                await self._emit(
                    DownloadEvent(
                        timestamp=datetime.utcnow().isoformat(),
                        sequence=0,
                        cufe="",
                        prefijo_folio="",
                        phase="auth_diagnostics",
                        status="ok",
                        notes=(
                            f"snapshot saved: {name} ({len(body)} bytes). "
                            f"Fetch with GET /files/{{job_id}}/{name}."
                        ),
                    )
                )
            except Exception as e:
                await self._emit(
                    DownloadEvent(
                        timestamp=datetime.utcnow().isoformat(),
                        sequence=0,
                        cufe="",
                        prefijo_folio="",
                        phase="auth_diagnostics",
                        status="fail",
                        notes=f"save {name} failed: {type(e).__name__}: {e}",
                    )
                )

    async def __aenter__(self) -> "DianTestScraper":
        # Browser engine selection. Default stays `chromium` to keep the
        # legacy boot path zero-change for anyone who hasn't flipped the
        # env var, but `camoufox` is the recommended setting for DIAN
        # given Azure WAF's increasingly strict bot detection. The
        # causation rail in NUVARA already uses Camoufox for the same
        # reason — this brings the standalone scraper into parity.
        engine = os.environ.get("BROWSER_ENGINE", "chromium").strip().lower()
        if engine not in {"chromium", "camoufox"}:
            engine = "chromium"
        self.browser_engine = engine

        # Optional proxy via env var (e.g. exit-node in Colombia so DIAN
        # responds fast / doesn't geo-throttle our BR-hosted server).
        proxy_cfg = _parse_proxy_url(os.environ.get("PROXY_URL"))

        await self._emit(
            DownloadEvent(
                timestamp=datetime.utcnow().isoformat(),
                sequence=0,
                cufe="",
                prefijo_folio="",
                phase="log",
                status="info",
                notes=f"Browser engine: {engine}",
            )
        )

        if proxy_cfg:
            await self._emit(
                DownloadEvent(
                    timestamp=datetime.utcnow().isoformat(),
                    sequence=0,
                    cufe="",
                    prefijo_folio="",
                    phase="log",
                    status="info",
                    notes=f"Using proxy {proxy_cfg['server']}"
                    + (" (auth)" if proxy_cfg.get("username") else ""),
                )
            )

        if engine == "camoufox":
            await self._launch_camoufox(proxy_cfg)
        else:
            await self._launch_chromium(proxy_cfg)
        return self

    async def _launch_chromium(
        self, proxy_cfg: dict[str, str] | None,
    ) -> None:
        """Original Chromium path. Kept for parity / debugging."""
        self.pw = await async_playwright().start()
        launch_kwargs: dict[str, Any] = {
            "headless": self.headless,
            "args": ["--disable-blink-features=AutomationControlled"],
        }
        if proxy_cfg:
            launch_kwargs["proxy"] = proxy_cfg
        self.browser = await self.pw.chromium.launch(**launch_kwargs)
        self.context = await self.browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="es-CO",
            timezone_id="America/Bogota",
            extra_http_headers={
                "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
            },
        )
        await self.context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        self.page = await self.context.new_page()

    async def _launch_camoufox(
        self, proxy_cfg: dict[str, str] | None,
    ) -> None:
        """Camoufox path — Firefox-based anti-detect browser.

        Mirrors the kwargs that NUVARA's causation rail already uses
        successfully against DIAN (apps/causation config.py:503). The
        firefox_user_prefs disable GPU/IPC features that crash inside
        Docker containers without /dev/shm or a real GPU. Camoufox is
        imported lazily so the default chromium path doesn't pay the
        cost of loading the binary bundle.
        """
        # Lazy import — we don't want chromium-only deployments paying
        # the import cost (camoufox pulls a few MB of dependencies).
        from camoufox.async_api import AsyncCamoufox  # noqa: PLC0415
        from camoufox.addons import DefaultAddons  # noqa: PLC0415

        kwargs: dict[str, Any] = {
            "headless": self.headless,
            "block_webgl": True,
            "exclude_addons": [DefaultAddons.UBO],
            "i_know_what_im_doing": True,
            "firefox_user_prefs": {
                # No GPU/rendering acceleration inside a container.
                "gfx.webrender.all": False,
                "gfx.webrender.enabled": False,
                "layers.acceleration.disabled": True,
                "gfx.canvas.accelerated": False,
                "gfx.x11-egl.force-disabled": True,
                # Single-process to avoid IPC crashes on small /dev/shm.
                "browser.tabs.remote.autostart": False,
                "dom.ipc.processCount": 0,
            },
            # Locale + timezone via Camoufox so the fingerprint stays
            # internally consistent (UA, navigator.language, Intl, etc.).
            "locale": "es-CO",
            "geoip": True,
        }
        if proxy_cfg:
            kwargs["proxy"] = proxy_cfg

        self._camoufox_ctx = AsyncCamoufox(**kwargs)
        self.browser = await self._camoufox_ctx.__aenter__()
        # Camoufox already injects locale + UA at the engine level, so
        # we deliberately skip the new_context() override we use for
        # chromium — overriding here would create the kind of
        # internally inconsistent fingerprint Camoufox is meant to
        # avoid (e.g. UA says Firefox but navigator.userAgentData
        # still leaks Chromium hints).
        self.context = await self.browser.new_context()
        self.page = await self.context.new_page()

    async def __aexit__(self, *args: Any) -> None:
        try:
            if self.context:
                await self.context.close()
        except Exception:
            pass
        try:
            if self.browser:
                await self.browser.close()
        except Exception:
            pass
        # Tear down whichever engine driver we used. Both paths are
        # best-effort — failures during shutdown shouldn't mask the
        # original error that may have triggered the exit.
        try:
            if self.pw:
                await self.pw.stop()
        except Exception:
            pass
        try:
            ctx = getattr(self, "_camoufox_ctx", None)
            if ctx is not None:
                await ctx.__aexit__(None, None, None)
        except Exception:
            pass

    async def authenticate(self) -> None:
        assert self.page is not None
        await self._emit(
            DownloadEvent(
                timestamp=datetime.utcnow().isoformat(),
                sequence=0,
                cufe="",
                prefijo_folio="",
                phase="log",
                status="info",
                notes=f"Opening auth URL...",
            )
        )
        await self.page.goto(
            self.auth_url, wait_until="domcontentloaded", timeout=120000
        )
        # DIAN sits behind Azure WAF, which often serves a JS challenge
        # (title 'Azure WAF', body 'Un momento, estamos comprobando
        # que no sea un bot') before letting us through to the real
        # portal. The challenge resolves itself in 3-15 s when the
        # browser is realistic enough (Camoufox/Firefox passes it),
        # but during that window `page.url` keeps showing the
        # original /User/AuthToken URL — so a single check 2 s in
        # would always conclude "stuck on /User/Auth" even on
        # otherwise healthy runs.
        #
        # We poll instead: every second, check whether we left the
        # auth/login path. The moment the URL moves to the inbox we
        # break out; if the budget runs out we capture diagnostics
        # and raise. Two budgets so the operator can tune via env:
        #   AUTH_WAIT_TIMEOUT_S       hard ceiling (default 30 s)
        #   AUTH_WAIT_POLL_INTERVAL_S sleep between checks (default 1 s)
        timeout_s = _env_float("AUTH_WAIT_TIMEOUT_S", 30.0)
        poll_s = max(0.25, _env_float("AUTH_WAIT_POLL_INTERVAL_S", 1.0))
        deadline = asyncio.get_event_loop().time() + timeout_s
        loop = asyncio.get_event_loop()
        announced_wait = False
        current_url = self.page.url
        while True:
            current_url = self.page.url
            if not (
                "login" in current_url.lower()
                or "/User/Auth" in current_url
            ):
                break
            if not announced_wait:
                # Emit once so the UI shows "still waiting on the WAF
                # challenge" instead of looking frozen. We don't spam
                # an event per second because the log is already noisy.
                await self._emit(
                    DownloadEvent(
                        timestamp=datetime.utcnow().isoformat(),
                        sequence=0,
                        cufe="",
                        prefijo_folio="",
                        phase="log",
                        status="info",
                        notes=(
                            f"Still on auth path, waiting for WAF "
                            f"challenge to resolve (up to {timeout_s:.0f}s)..."
                        ),
                    )
                )
                announced_wait = True
            if loop.time() >= deadline:
                break
            await asyncio.sleep(poll_s)

        if "login" in current_url.lower() or "/User/Auth" in current_url:
            # Diagnostics: when DIAN rejects the auth URL we have no
            # idea WHAT they served the scraper without seeing it. We
            # snapshot the landing page (HTML + screenshot) and hand
            # them to the consumer via the file_callback so the
            # operator can open them after the fact. This is the
            # next-best thing to attaching a debugger and saves a
            # round-trip of "can you also paste the page source".
            #
            # If anything in the snapshot itself fails (Playwright
            # already torn down, callback raises) we swallow it: the
            # original RuntimeError is more important than perfect
            # diagnostics.
            await self._snapshot_for_diagnostics(
                tag="auth-failed",
                landed_url=current_url,
            )
            raise RuntimeError(
                "Auth URL expired or invalid. Re-login in DIAN and copy a fresh URL."
            )
        await self._emit(
            DownloadEvent(
                timestamp=datetime.utcnow().isoformat(),
                sequence=0,
                cufe="",
                prefijo_folio="",
                phase="reauth",
                status="ok",
                notes=f"authenticated, landed on {current_url[:200]}",
            )
        )

    async def list_invoices(self) -> list[InvoiceRow]:
        assert self.page is not None
        await self._emit(
            DownloadEvent(
                timestamp=datetime.utcnow().isoformat(),
                sequence=0,
                cufe="",
                prefijo_folio="",
                phase="log",
                status="info",
                notes=(
                    f"Listing invoices {self.start_date}..{self.end_date} "
                    f"(max {self.max_invoices})..."
                ),
            )
        )
        # Wait for `networkidle` (no more than 2 in-flight requests for
        # 500ms) instead of just `domcontentloaded`. The listing page
        # pulls jQuery + DataTables + the calendar widget AFTER initial
        # HTML, and we hit the timing window where those scripts
        # weren't done loading when we started querying for them.
        # Falls back to domcontentloaded if networkidle never settles
        # (e.g. portal has a long-polling XHR open).
        try:
            await self.page.goto(
                RECEIVED_URL, wait_until="networkidle", timeout=60000
            )
        except Exception:
            # networkidle can hang on long-polling endpoints; fall back
            # to the previous behaviour and let waitForDT below absorb
            # the remaining script load time.
            await self.page.goto(
                RECEIVED_URL, wait_until="domcontentloaded", timeout=120000
            )
        await asyncio.sleep(2)

        start_dian = to_dian_date(self.start_date, end=False)
        end_dian = to_dian_date(self.end_date, end=True)

        # DIAN uses HIDDEN inputs for the date range (#startDate, #endDate).
        # Playwright's .fill() refuses to act on hidden elements, so we set
        # the value via JS — same as what the portal's calendar widget does
        # when the user picks a date.
        try:
            applied = await self.page.evaluate(
                """({startVal, endVal}) => {
                    const setVal = (sel, val) => {
                        const el = document.querySelector(sel);
                        if (!el) return false;
                        const setter = Object.getOwnPropertyDescriptor(
                            window.HTMLInputElement.prototype, 'value'
                        ).set;
                        setter.call(el, val);
                        el.dispatchEvent(new Event('input', { bubbles: true }));
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                        return true;
                    };
                    const okStart = setVal('#startDate', startVal)
                        || setVal('[name="StartDate"]', startVal);
                    const okEnd = setVal('#endDate', endVal)
                        || setVal('[name="EndDate"]', endVal);
                    return { okStart, okEnd };
                }""",
                {"startVal": start_dian, "endVal": end_dian},
            )
            await self._emit(
                DownloadEvent(
                    timestamp=datetime.utcnow().isoformat(),
                    sequence=0,
                    cufe="",
                    prefijo_folio="",
                    phase="log",
                    status="info",
                    notes=(
                        f"date inputs set via JS: start={start_dian} ({applied.get('okStart')})"
                        f" end={end_dian} ({applied.get('okEnd')})"
                    ),
                )
            )
        except Exception as e:
            await self._emit(
                DownloadEvent(
                    timestamp=datetime.utcnow().isoformat(),
                    sequence=0,
                    cufe="",
                    prefijo_folio="",
                    phase="log",
                    status="info",
                    notes=f"⚠ Date inputs not set ({e}). Using portal defaults.",
                )
            )

        # Try submit — search button or fallback to form submission.
        submitted = False
        for sel in (
            "button[type='submit']",
            "#searchBtn",
            ".btn-search",
            "button:has-text('Buscar')",
            "button:has-text('Consultar')",
        ):
            try:
                await self.page.click(sel, timeout=2000)
                submitted = True
                break
            except Exception:
                continue
        if not submitted:
            # Fallback: trigger the form submit via JS
            try:
                await self.page.evaluate(
                    """() => {
                        const form = document.querySelector('form');
                        if (form) form.submit();
                    }"""
                )
            except Exception:
                pass

        try:
            await self.page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        await asyncio.sleep(2)

        # DOM-pure listing — no dependency on the page's jQuery/DataTables.
        #
        # Why: Camoufox sandboxes Playwright's page.evaluate() so it runs
        # in an isolated world that CANNOT see the page's `window.jQuery`,
        # `$`, or the live DataTable instance. The previous strategy
        # (waitForDT + dt.page.info() + dt.page(n).draw()) timed out
        # after 120 s every single time on Camoufox because `jQuery`
        # was undefined in evaluate's scope — even though the page
        # had it loaded.
        #
        # New strategy (Camoufox + Chromium friendly):
        #   1. Poll the DOM for `table.dataTable tbody tr.document-row`.
        #      DIAN pre-renders page 1 server-side, so as soon as the
        #      tbody has rows we can read them with no JS injection.
        #   2. For pagination, scrape the visible page, then click the
        #      DataTables 'Next' button via `.click()` (DOM-only,
        #      works in both engines). After the click, poll the
        #      tbody until the data-id of the first row changes — that's
        #      how we know the new page rendered.
        #   3. Stop on: no Next button (or it's disabled), or
        #      maxInvoices reached, or the budget runs out.
        result_json = await self.page.evaluate(
            """({ maxWaitMs, intervalMs, maxInvoices }) => new Promise((resolve) => {
                const startedAt = Date.now();
                const deadline = startedAt + maxWaitMs;
                const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

                const TABLE_SELECTOR = 'table.dataTable, table#tableDocuments';
                const ROW_SELECTOR = 'tbody tr.document-row, tbody tr[data-id]';

                const getTable = () => document.querySelector(TABLE_SELECTOR);

                const readCurrentPageRows = () => {
                    const table = getTable();
                    if (!table) return [];
                    const out = [];
                    table.querySelectorAll(ROW_SELECTOR).forEach((node) => {
                        const dataId = node.getAttribute('data-id') || '';
                        const cells = [...node.querySelectorAll('td')].map(
                            (c) => (c.innerText || c.textContent || '').trim()
                        );
                        if (cells.length === 0) return;
                        // Skip empty-state placeholder rows
                        // ('No se encontraron resultados', etc.)
                        if (cells.length === 1 && !dataId) return;
                        out.push({ dataId, cells });
                    });
                    return out;
                };

                const findNextButton = () => {
                    // DataTables' Next button changes class set across
                    // versions; we accept any of the known shapes:
                    //   - <button class="dt-paging-button ... next">
                    //   - <a class="paginate_button next">
                    // We exclude disabled buttons so we don't click into
                    // a non-existent next page.
                    const selectors = [
                        '.dt-paging button.next:not(.disabled):not([aria-disabled="true"])',
                        'a.paginate_button.next:not(.disabled)',
                        '.dt-paging button[aria-label="Next"]:not(.disabled):not([aria-disabled="true"])',
                        'button.dt-paging-button[data-dt-idx="next"]:not(.disabled):not([aria-disabled="true"])',
                    ];
                    for (const sel of selectors) {
                        const btn = document.querySelector(sel);
                        if (btn) return btn;
                    }
                    return null;
                };

                const waitForFirstRows = async () => {
                    // Poll up to maxWaitMs/2 for the tbody to be
                    // populated. We split the budget so a slow page-1
                    // load still leaves room to walk pagination.
                    const tableDeadline = Math.min(
                        Date.now() + Math.floor(maxWaitMs / 2),
                        deadline
                    );
                    while (Date.now() < tableDeadline) {
                        const rows = readCurrentPageRows();
                        if (rows.length > 0) return rows;
                        // Bail if we're at deadline OR if the table
                        // exists and explicitly says 'no results' so
                        // we don't waste the rest of the budget.
                        const table = getTable();
                        if (table) {
                            const txt = (table.innerText || '').toLowerCase();
                            if (
                                txt.includes('ningún documento disponible')
                                || txt.includes('no se encontraron resultados')
                            ) {
                                return [];
                            }
                        }
                        await sleep(intervalMs);
                    }
                    return readCurrentPageRows();
                };

                const waitForPageChange = async (prevSignature) => {
                    // After clicking Next we wait for either:
                    //   - the first row's data-id to differ from before, or
                    //   - the row count to change, or
                    //   - we hit the deadline.
                    while (Date.now() < deadline) {
                        const rows = readCurrentPageRows();
                        const sig = rows.length === 0
                            ? `EMPTY:${rows.length}`
                            : `${rows[0].dataId}|${rows.length}`;
                        if (sig !== prevSignature && rows.length > 0) {
                            return rows;
                        }
                        await sleep(intervalMs);
                    }
                    return readCurrentPageRows();
                };

                (async () => {
                    let allRows = await waitForFirstRows();

                    if (allRows.length === 0) {
                        resolve(JSON.stringify({
                            ok: true,
                            method: 'dom-zero-records',
                            recordsTotal: 0,
                            rows: [],
                            pages: 0,
                        }));
                        return;
                    }

                    let pageCount = 1;
                    while (Date.now() < deadline) {
                        if (maxInvoices > 0 && allRows.length >= maxInvoices) break;
                        const next = findNextButton();
                        if (!next) break;
                        const prevSig =
                            `${allRows[allRows.length - 1].dataId}|first`;
                        // Click the button. We deliberately use the DOM
                        // click() because dispatching a synthetic event
                        // would skip jQuery handlers DataTables wires up.
                        try {
                            next.click();
                        } catch (e) {
                            // If clicking fails (button gone, etc.) we
                            // just stop pagination — what we have is
                            // already a valid result.
                            break;
                        }
                        // After clicking, the current page rows are still
                        // showing for a tick. Wait for change.
                        const pageRows = await waitForPageChange(prevSig);
                        if (pageRows.length === 0) break;
                        const seenIds = new Set(
                            allRows.map((r) => r.dataId).filter(Boolean)
                        );
                        let added = 0;
                        for (const r of pageRows) {
                            if (r.dataId && seenIds.has(r.dataId)) continue;
                            allRows.push(r);
                            added++;
                            if (maxInvoices > 0 && allRows.length >= maxInvoices) {
                                break;
                            }
                        }
                        pageCount++;
                        // If we didn't add anything new (e.g. page didn't
                        // actually change) bail to avoid infinite loops.
                        if (added === 0) break;
                    }

                    resolve(JSON.stringify({
                        ok: true,
                        method: pageCount > 1 ? 'dom-pagination' : 'dom-single-page',
                        recordsTotal: allRows.length,
                        rows: allRows.slice(0, maxInvoices || allRows.length),
                        pages: pageCount,
                    }));
                })().catch((err) => {
                    resolve(JSON.stringify({
                        ok: false,
                        error: 'dom-strategy-failed: ' + (err && err.message),
                    }));
                });
            })""",
            {
                "maxWaitMs": 60000,
                "intervalMs": 500,
                "maxInvoices": self.max_invoices,
            },
        )

        try:
            result = json.loads(result_json)
        except Exception:
            result = {"ok": False, "error": "non-json-result", "raw": result_json[:200]}

        if not result.get("ok"):
            await self._emit(
                DownloadEvent(
                    timestamp=datetime.utcnow().isoformat(),
                    sequence=0,
                    cufe="",
                    prefijo_folio="",
                    phase="list",
                    status="fail",
                    error=result.get("error", "unknown"),
                    notes=f"hint: {result.get('hint', '')}",
                )
            )
            # Capture the page so we can see why DIAN's HTML didn't
            # match our DataTables expectations. Same channel as the
            # auth diagnostics — operator opens GET /files/{job_id}/
            # listing-failed.png to triage.
            try:
                await self._snapshot_for_diagnostics(
                    tag="listing-failed",
                    landed_url=self.page.url if self.page else "",
                )
            except Exception:
                pass
            return []

        rows_data = result.get("rows", [])
        records_total = result.get("recordsTotal", 0)
        method = result.get("method", "")
        pages = result.get("pages", 1)

        # Edge case: result.ok is True but we got zero rows AND DIAN
        # reports recordsTotal=0. Could be a legitimately empty range,
        # could be the same serverSide bug from a different angle.
        # Snapshot the page so the operator can decide.
        if records_total == 0 and not rows_data:
            try:
                await self._snapshot_for_diagnostics(
                    tag="listing-empty",
                    landed_url=self.page.url if self.page else "",
                )
            except Exception:
                pass

        invoices: list[InvoiceRow] = []
        for i, row in enumerate(rows_data):
            if i >= self.max_invoices:
                break
            cells = row.get("cells", [])
            data_id = row.get("dataId", "")
            invoice = InvoiceRow(
                cufe=data_id or (cells[0] if cells else ""),
                track_id=data_id,
                prefijo_folio=cells[1] if len(cells) > 1 else f"row-{i}",
                issuer_nit=cells[2] if len(cells) > 2 else "",
                issue_date=cells[3] if len(cells) > 3 else "",
                raw=row,
            )
            if invoice.cufe:
                invoices.append(invoice)

        await self._emit(
            DownloadEvent(
                timestamp=datetime.utcnow().isoformat(),
                sequence=0,
                cufe="",
                prefijo_folio="",
                phase="list",
                status="ok",
                notes=(
                    f"found {len(invoices)} invoices "
                    f"(recordsTotal={records_total}, method={method}, "
                    f"pages={pages}, max_cap={self.max_invoices})"
                ),
            )
        )
        return invoices

    async def human_delay(self, sequence: int) -> int:
        base = random.randint(self.delay_min_ms, self.delay_max_ms)
        long_pause = 0
        if (
            sequence > 0
            and self.long_pause_every > 0
            and sequence % self.long_pause_every == 0
        ):
            long_pause = random.randint(LONG_PAUSE_MIN_MS, LONG_PAUSE_MAX_MS)
            await self._emit(
                DownloadEvent(
                    timestamp=datetime.utcnow().isoformat(),
                    sequence=sequence,
                    cufe="",
                    prefijo_folio="",
                    phase="sleep",
                    status="info",
                    notes=f"long pause: {long_pause / 1000:.1f}s after {sequence} downloads",
                )
            )
        total = base + long_pause
        # Sleep in 0.5s chunks so we can respond to cancellation
        slept = 0
        while slept < total:
            if self.cancel_event.is_set():
                return slept
            chunk = min(500, total - slept)
            await asyncio.sleep(chunk / 1000.0)
            slept += chunk
        return total

    async def _single_download_attempt(
        self,
        invoice: InvoiceRow,
        sequence: int,
        attempt: int,
        endpoint: str = "getfilepdf",
    ) -> tuple[Any, bytes, dict[str, str], int]:
        """One HTTP attempt. Returns (response, body, headers, elapsed_ms).

        endpoint:
        - "getfilepdf" → /Document/GetFilePdf?cune=<cufe>  (rich PDF, default)
        - "downloadzip" → /Document/DownloadZipFiles?trackId=<cufe>  (legacy fallback)

        DIAN sometimes ships GetFilePdf ZIPs with a 0-byte PDF entry. When that
        happens, retry through the legacy endpoint which has the actual bytes.
        See INTEGRATION.md §6 "Empty PDF en ZIP válido".
        """
        assert self.context is not None
        start = time.monotonic()
        if endpoint == "downloadzip":
            url = f"{DOWNLOAD_ZIP_URL}?trackId={invoice.cufe.lower()}"
        else:
            url = f"{GETFILE_PDF_URL}?cune={invoice.cufe.lower()}"
        response = await self.context.request.get(
            url,
            headers={
                "Accept": "application/octet-stream, application/zip, */*",
                "Referer": RECEIVED_URL,
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            },
            timeout=60000,
        )
        elapsed_ms = int((time.monotonic() - start) * 1000)
        body = await response.body()
        headers = dict(response.headers)
        return response, body, headers, elapsed_ms

    @staticmethod
    def _zip_has_real_pdf(zip_bytes: bytes) -> bool:
        """True iff the zip contains at least one .pdf entry with non-zero size."""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                for info in zf.infolist():
                    if info.filename.lower().endswith(".pdf") and info.file_size > 0:
                        return True
        except zipfile.BadZipFile:
            pass
        return False

    async def download_invoice(
        self, invoice: InvoiceRow, sequence: int
    ) -> DownloadEvent:
        """Download with transparent retry on transient WAF challenges.

        Empirical finding: Azure Front Door sometimes returns a stochastic
        403/429 challenge that resolves on its own within 2-5s. We retry up
        to 2 times with backoff before giving up. Each retry is logged so
        you can see them in the UI.
        """
        assert self.context is not None
        MAX_RETRIES = 2
        last_event: DownloadEvent | None = None

        for attempt in range(1, MAX_RETRIES + 2):  # 1, 2, 3
            try:
                response, body, headers, elapsed_ms = await self._single_download_attempt(
                    invoice, sequence, attempt
                )
            except Exception as e:
                elapsed_ms = 0
                last_event = DownloadEvent(
                    timestamp=datetime.utcnow().isoformat(),
                    sequence=sequence,
                    cufe=invoice.cufe,
                    prefijo_folio=invoice.prefijo_folio,
                    phase="download",
                    status="fail",
                    elapsed_ms=elapsed_ms,
                    error=f"{type(e).__name__}: {e}",
                    issuer_nit=invoice.issuer_nit,
                    issue_date=invoice.issue_date,
                    notes=f"attempt {attempt}/{MAX_RETRIES + 1}",
                )
                if attempt <= MAX_RETRIES:
                    await self._emit(
                        DownloadEvent(
                            timestamp=datetime.utcnow().isoformat(),
                            sequence=sequence,
                            cufe=invoice.cufe,
                            prefijo_folio=invoice.prefijo_folio,
                            phase="log",
                            status="info",
                            notes=(
                                f"retry {attempt + 1}/{MAX_RETRIES + 1} after "
                                f"network error: {type(e).__name__}"
                            ),
                        )
                    )
                    await asyncio.sleep(2 + attempt * 2)
                    continue
                return last_event

            is_blocked, block_reason = detect_block(response, body[:1024])
            is_transient = (
                response.status in (403, 429, 503, 502, 504)
                or (response.status == 200 and len(body) > 0 and body[:2] != b"PK")
            )

            # If this is a transient block AND we have retries left → retry
            if is_blocked and is_transient and attempt <= MAX_RETRIES:
                await self._emit(
                    DownloadEvent(
                        timestamp=datetime.utcnow().isoformat(),
                        sequence=sequence,
                        cufe=invoice.cufe,
                        prefijo_folio=invoice.prefijo_folio,
                        phase="log",
                        status="info",
                        notes=(
                            f"transient {response.status} on attempt {attempt}/{MAX_RETRIES + 1}, "
                            f"retrying in {2 + attempt * 2}s... ({block_reason})"
                        ),
                    )
                )
                await asyncio.sleep(2 + attempt * 2)
                continue

            # No retry needed/possible — build the final event below
            if is_blocked:
                return DownloadEvent(
                    timestamp=datetime.utcnow().isoformat(),
                    sequence=sequence,
                    cufe=invoice.cufe,
                    prefijo_folio=invoice.prefijo_folio,
                    phase="download",
                    status="block",
                    http_status=response.status,
                    elapsed_ms=elapsed_ms,
                    bytes_downloaded=len(body),
                    error=block_reason,
                    issuer_nit=invoice.issuer_nit,
                    issue_date=invoice.issue_date,
                    response_headers={
                        k: v for k, v in headers.items()
                        if k.lower() in (
                            "content-type", "x-azure-ref", "cf-mitigated",
                            "location", "server", "retry-after",
                        )
                    },
                )

            if response.status == 200 and len(body) > 0:
                if body[:2] == b"PK":
                    # If the primary endpoint shipped a 0-byte PDF inside the
                    # zip, try the legacy DownloadZipFiles endpoint ONCE before
                    # giving up. Empirically ~80% of GetFilePdf zips have empty
                    # PDFs but DownloadZipFiles has the real bytes.
                    fallback_note = ""
                    if not self._zip_has_real_pdf(body):
                        try:
                            fb_resp, fb_body, fb_headers, fb_elapsed = (
                                await self._single_download_attempt(
                                    invoice, sequence, attempt,
                                    endpoint="downloadzip",
                                )
                            )
                            if (
                                fb_resp.status == 200
                                and len(fb_body) > 0
                                and fb_body[:2] == b"PK"
                                and self._zip_has_real_pdf(fb_body)
                            ):
                                # Swap: legacy zip is better
                                body = fb_body
                                headers = fb_headers
                                elapsed_ms += fb_elapsed
                                fallback_note = " (recovered PDF via DownloadZipFiles)"
                            else:
                                fallback_note = (
                                    f" (DownloadZipFiles fallback also empty,"
                                    f" status={fb_resp.status})"
                                )
                        except Exception as e:
                            fallback_note = f" (DownloadZipFiles fallback errored: {type(e).__name__})"

                    safe_id = invoice.cufe[:20] or f"seq-{sequence}"
                    filename = f"{safe_id}.zip"

                    # Two persistence paths, picked by the caller:
                    #
                    # 1. write_to_disk=True (default, legacy): drop the
                    #    ZIP under downloads_dir so the legacy /files
                    #    endpoint can serve it and the CLI keeps its
                    #    on-disk layout intact.
                    #
                    # 2. file_callback set (server-driven): hand the
                    #    bytes to the consumer right here. server.py
                    #    wires this to JobBackend.save_file which
                    #    uploads to R2 in the postgres+R2 mode. When
                    #    paired with write_to_disk=False this is the
                    #    full "no local files" path the Fase 1 plan
                    #    asks for.
                    #
                    # Both can be active at the same time (dual-write)
                    # which is exactly what we want during the rollout:
                    # disk for the legacy UI, R2 for NUVARA, no risk of
                    # losing files if either side breaks.
                    if self.write_to_disk:
                        zip_path = self.downloads_dir / filename
                        zip_path.write_bytes(body)
                    await self._emit_file(
                        FileSavedEvent(
                            cufe=invoice.cufe,
                            prefijo_folio=invoice.prefijo_folio,
                            issuer_nit=invoice.issuer_nit,
                            issue_date=invoice.issue_date,
                            filename=filename,
                            body=body,
                            size_bytes=len(body),
                            sequence=sequence,
                        )
                    )

                    # We intentionally DO NOT extract PDF/XML to disk
                    # here. The consumer (NUVARA) downloads only the
                    # ZIP and extracts the PDF + XML in-memory on its
                    # side. That cuts:
                    #   - ~30% off the per-invoice latency in the
                    #     scraper (no zip read + 2 disk writes)
                    #   - 2/3 of the scraper→NUVARA transfer
                    #   - 2/3 of the R2 upload volume
                    #
                    # _zip_has_real_pdf is still used above to decide
                    # if we should retry through DownloadZipFiles. That
                    # check reads the zip with BytesIO so it doesn't
                    # touch disk.
                    pdf_filename = None
                    xml_filename = None

                    return DownloadEvent(
                        timestamp=datetime.utcnow().isoformat(),
                        sequence=sequence,
                        cufe=invoice.cufe,
                        prefijo_folio=invoice.prefijo_folio,
                        phase="download",
                        status="ok",
                        http_status=200,
                        elapsed_ms=elapsed_ms,
                        bytes_downloaded=len(body),
                        notes=f"saved zip ({len(body)} bytes){fallback_note}",
                        pdf_filename=pdf_filename,
                        xml_filename=xml_filename,
                        issuer_nit=invoice.issuer_nit,
                        issue_date=invoice.issue_date,
                        # The scraper no longer extracts PDF/XML — the
                        # consumer does it. These fields stay None so
                        # the DownloadEvent shape is preserved without
                        # paying the extract cost here.
                        pdf_b64_size=None,
                        xml_preview=None,
                    )
                else:
                    return DownloadEvent(
                        timestamp=datetime.utcnow().isoformat(),
                        sequence=sequence,
                        cufe=invoice.cufe,
                        prefijo_folio=invoice.prefijo_folio,
                        phase="download",
                        status="block",
                        http_status=200,
                        elapsed_ms=elapsed_ms,
                        bytes_downloaded=len(body),
                        error="200 OK but body is not a ZIP (likely HTML challenge)",
                        issuer_nit=invoice.issuer_nit,
                        issue_date=invoice.issue_date,
                        response_headers={
                            "content-type": headers.get("content-type", ""),
                        },
                    )

            return DownloadEvent(
                timestamp=datetime.utcnow().isoformat(),
                sequence=sequence,
                cufe=invoice.cufe,
                prefijo_folio=invoice.prefijo_folio,
                phase="download",
                status="fail",
                http_status=response.status,
                elapsed_ms=elapsed_ms,
                bytes_downloaded=len(body),
                error=f"HTTP {response.status}",
                issuer_nit=invoice.issuer_nit,
                issue_date=invoice.issue_date,
            )

        # Exhausted retries — return the last failure we saw, or a generic one
        return last_event or DownloadEvent(
            timestamp=datetime.utcnow().isoformat(),
            sequence=sequence,
            cufe=invoice.cufe,
            prefijo_folio=invoice.prefijo_folio,
            phase="download",
            status="fail",
            error="exhausted retries without a final response",
            issuer_nit=invoice.issuer_nit,
            issue_date=invoice.issue_date,
        )

    async def run(self) -> dict[str, Any]:
        await self.authenticate()
        if self.cancel_event.is_set():
            return self.logger.summary()
        invoices = await self.list_invoices()
        if not invoices:
            await self._emit(
                DownloadEvent(
                    timestamp=datetime.utcnow().isoformat(),
                    sequence=0,
                    cufe="",
                    prefijo_folio="",
                    phase="summary",
                    status="info",
                    notes="No invoices found in range",
                )
            )
            return self.logger.summary()

        consecutive_blocks = 0
        for i, invoice in enumerate(invoices, start=1):
            if self.cancel_event.is_set():
                await self._emit(
                    DownloadEvent(
                        timestamp=datetime.utcnow().isoformat(),
                        sequence=i,
                        cufe="",
                        prefijo_folio="",
                        phase="summary",
                        status="info",
                        notes="cancelled by user",
                    )
                )
                break
            if i > 1:
                await self.human_delay(i - 1)
                if self.cancel_event.is_set():
                    continue

            event = await self.download_invoice(invoice, i)
            await self._emit(event)

            if event.status == "block":
                consecutive_blocks += 1
                if consecutive_blocks >= 3:
                    await self._emit(
                        DownloadEvent(
                            timestamp=datetime.utcnow().isoformat(),
                            sequence=i,
                            cufe="",
                            prefijo_folio="",
                            phase="summary",
                            status="block",
                            notes="stopped after 3 consecutive blocks",
                        )
                    )
                    break
            else:
                consecutive_blocks = 0

        summary = self.logger.summary()
        await self._emit(
            DownloadEvent(
                timestamp=datetime.utcnow().isoformat(),
                sequence=0,
                cufe="",
                prefijo_folio="",
                phase="summary",
                status="info",
                notes=json.dumps(summary),
            )
        )
        return summary
