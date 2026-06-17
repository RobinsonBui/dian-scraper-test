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
# DIAN files emitted invoices under /Document/Sent. The DataTables
# form is identical to /Received, which is why the rest of the engine
# is direction-agnostic — we just navigate to a different URL up
# front. NUVARA's main scraper (apps/causation) uses the same pair.
SENT_URL = f"{DIAN_BASE_URL}/Document/Sent"
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
# Error classification
# --------------------------------------------------------------------------
#
# Five mutually-exclusive kinds the consumer (NUVARA) can map to a
# context-aware UI message. The strings are deliberately stable
# (snake_case, no version suffix) so a NUVARA update isn't required
# every time we touch the scraper.
#
#   auth_expired       — DIAN's auth URL was rejected at the start of
#                        the run (single-use token, expired, etc.)
#   auth_expired_midrun — DIAN started redirecting to /login after
#                        downloads had already begun (session went
#                        away mid-job; partial results stay)
#   captcha_blocked    — Azure WAF served a JS challenge we couldn't
#                        get past inside the auth wait budget
#   timeout            — the global job budget elapsed
#   engine_crash       — unhandled exception in the Playwright engine
#                        (the only "we don't really know" bucket)

ERROR_KIND_AUTH_EXPIRED = "auth_expired"
ERROR_KIND_AUTH_EXPIRED_MIDRUN = "auth_expired_midrun"
ERROR_KIND_CAPTCHA_BLOCKED = "captcha_blocked"
ERROR_KIND_TIMEOUT = "timeout"
ERROR_KIND_ENGINE_CRASH = "engine_crash"


class ScraperError(RuntimeError):
    """Engine-side error with a classified `kind`.

    Subclass of RuntimeError so any existing `except RuntimeError`
    catch sites keep working — the new attribute is purely additive.
    server.py reads `kind` to populate the job row's `error_kind`
    column, which NUVARA then turns into an actionable UI message.
    """

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


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
        skip_cufes: list[str] | set[str] | None = None,
        direction: str = "purchase",
        doc_type_filter: str | None = None,
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
        # Consumer-provided CUFE skip list. Stored as a set for O(1)
        # `in` checks during the per-row filter in list_invoices().
        # We deliberately drop None/empty strings up front so a sloppy
        # caller can't silently fill the set with junk that never
        # matches anything.
        #
        # Casing: DIAN's listing always serves CUFEs lowercase, so we
        # normalize the skip set to lowercase too. Without this, a
        # consumer that canonicalizes CUFEs to UPPERCASE (NUVARA does,
        # see normalize-cufe.ts) sends a skip list that the
        # case-sensitive `in` check below would never match, and the
        # engine quietly re-downloads every known invoice.
        self.skip_cufes: set[str] = (
            {c.lower() for c in skip_cufes if c}
            if skip_cufes
            else set()
        )
        # Which DIAN bucket to scrape. We validate up-front so a typo
        # ('Purchase', 'compras', 'received') doesn't silently fall
        # back to the default and confuse the operator. The list URL
        # is decided here so the rest of the engine only touches
        # `self.list_url`.
        if direction not in ("purchase", "sale"):
            raise ValueError(
                f"direction must be 'purchase' or 'sale', got {direction!r}"
            )
        self.direction: str = direction
        self.list_url: str = SENT_URL if direction == "sale" else RECEIVED_URL

        # Optional docType narrow-down applied after pagination but
        # before the download queue. Today only 'support' is modelled
        # (DS + DE substring match on the visible Tipo column).
        # Unknown values raise so a NUVARA bug never silently falls
        # back to "no filter" and pulls every sale row.
        if doc_type_filter is not None and doc_type_filter not in ("support",):
            raise ValueError(
                f"doc_type_filter must be None or 'support', got {doc_type_filter!r}"
            )
        self.doc_type_filter: str | None = doc_type_filter

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
        """Best-effort browser teardown with per-step timeouts.

        Why each call is wrapped in `asyncio.wait_for`: Playwright's
        `.close()` methods can block indefinitely when there are
        in-flight requests, hanging downloads, or stuck network-idle
        handlers — patterns that happen routinely when a paginated
        listing was still draining at the moment the engine returned.
        Without a timeout here, `__aexit__` could pin the worker task
        for minutes, which used to delay the job's terminal mark on
        the server side (the consumer would see `status=running` long
        after every invoice had been processed).

        Timeouts are intentionally generous (10s each) — most clean
        shutdowns finish in under 1s, and aborting in the rare slow
        case is preferable to blocking forever. Each step swallows
        BOTH timeouts and exceptions so a stuck `context.close()`
        doesn't prevent `browser.close()` from being attempted.
        """
        teardown_timeout_s = 10.0

        async def _safe_close(coro: Awaitable[Any], label: str) -> None:
            try:
                await asyncio.wait_for(coro, timeout=teardown_timeout_s)
            except asyncio.TimeoutError:
                # Log so a recurring slow teardown is observable.
                # We don't fail the run — the scraper's work is done.
                try:
                    self.logger.warning(
                        "Browser teardown step %s timed out after %.1fs",
                        label,
                        teardown_timeout_s,
                    )
                except Exception:
                    pass
            except Exception:
                pass

        if self.context:
            await _safe_close(self.context.close(), "context.close")
        if self.browser:
            await _safe_close(self.browser.close(), "browser.close")
        # Tear down whichever engine driver we used. Both paths are
        # best-effort — failures during shutdown shouldn't mask the
        # original error that may have triggered the exit.
        if self.pw:
            await _safe_close(self.pw.stop(), "playwright.stop")
        ctx = getattr(self, "_camoufox_ctx", None)
        if ctx is not None:
            await _safe_close(ctx.__aexit__(None, None, None), "camoufox.__aexit__")

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
            # operator can open them after the fact.
            await self._snapshot_for_diagnostics(
                tag="auth-failed",
                landed_url=current_url,
            )

            # Try to distinguish "WAF challenge we couldn't pass"
            # (browser fingerprint trips Azure's bot detection) from
            # "expired token" (DIAN's auth_url is single-use and we
            # got it after it was already consumed). Both land on
            # /User/Auth, but the WAF case has very telltale HTML.
            error_kind = ERROR_KIND_AUTH_EXPIRED
            message = (
                "Auth URL expired or invalid. "
                "Re-login in DIAN and copy a fresh URL."
            )
            try:
                if self.page is not None:
                    html = await self.page.content()
                    html_lower = html.lower()
                    if (
                        "<title>azure waf</title>" in html_lower
                        or "comprobando que no sea un bot" in html_lower
                        or "/.azwaf/" in html
                    ):
                        error_kind = ERROR_KIND_CAPTCHA_BLOCKED
                        message = (
                            "Azure WAF blocked the session before we "
                            "could read the DIAN portal. "
                            "Wait a few minutes and retry."
                        )
            except Exception:
                # HTML probe is best-effort; the original auth_expired
                # classification is a safe default.
                pass

            raise ScraperError(error_kind, message)
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
                    f"direction={self.direction} (max {self.max_invoices})..."
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
                self.list_url, wait_until="networkidle", timeout=60000
            )
        except Exception:
            # networkidle can hang on long-polling endpoints; fall back
            # to the previous behaviour and let waitForDT below absorb
            # the remaining script load time.
            await self.page.goto(
                self.list_url, wait_until="domcontentloaded", timeout=120000
            )
        await asyncio.sleep(2)

        # DIAN's portal uses jQuery daterangepicker on
        # `#dashboard-report-range`. Its callback is the ONLY thing
        # that writes the hidden inputs in the format DIAN's backend
        # accepts (`YYYY/MM/DD`). Setting the hidden inputs directly
        # to `DD/MM/YYYY` (what to_dian_date returns) makes the form
        # POST land at /Document/Received with garbage in StartDate/
        # EndDate — the controller then falls back to its default
        # "last N invoices" window and returns May+June rows even
        # when we asked for April.
        #
        # Strategy:
        #   1. Set the hidden inputs to YYYY/MM/DD directly. That is
        #      what daterangepicker's callback writes, what DIAN's
        #      Buscar handler reads, and what the new
        #      /Document/GetDocumentsPageToken AJAX puts in its body.
        #   2. Drive the daterangepicker itself via its public API
        #      (`setStartDate` / `setEndDate`) so the visible widget
        #      and any internal state stay in sync with the hidden
        #      values. This is the same code path a real click would
        #      trigger.
        #   3. Fall back to plain hidden-input writes if (2) fails
        #      (e.g. the portal stripped daterangepicker on this page
        #      version) — still better than the old DD/MM/YYYY shape.
        from datetime import datetime as _dt
        _start = _dt.fromisoformat(self.start_date)
        _end = _dt.fromisoformat(self.end_date)
        start_dian = _start.strftime("%Y/%m/%d")
        end_dian = _end.strftime("%Y/%m/%d")
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
                    // 1. Hidden inputs — the canonical YYYY/MM/DD form.
                    const okStart = setVal('#startDate', startVal)
                        || setVal('[name="StartDate"]', startVal);
                    const okEnd = setVal('#endDate', endVal)
                        || setVal('[name="EndDate"]', endVal);

                    // 2. Drive the daterangepicker widget so its
                    //    internal state matches the hidden inputs.
                    //    Wrapped in try/catch because the widget may
                    //    not be initialised yet on slow pages.
                    let widgetOk = false;
                    try {
                        const jq = window.jQuery || window.$;
                        if (jq) {
                            const dp = jq('#dashboard-report-range')
                                .data('daterangepicker');
                            if (dp && typeof dp.setStartDate === 'function') {
                                dp.setStartDate(startVal);
                                dp.setEndDate(endVal);
                                widgetOk = true;
                            }
                        }
                    } catch (e) { /* keep widgetOk=false */ }

                    return { okStart, okEnd, widgetOk };
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
                        f"date inputs set: start={start_dian} "
                        f"end={end_dian} "
                        f"(hiddenStart={applied.get('okStart')}, "
                        f"hiddenEnd={applied.get('okEnd')}, "
                        f"widget={applied.get('widgetOk')})"
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

        # docType narrow-down at the SERVER-SIDE level: DIAN's
        # /Document/Sent has a <select id="DocumentTypeId"> that, when
        # set to "05" (Documento soporte con no obligados), tells
        # /Document/GetDocumentsPageToken to return ONLY DS rows.
        # That's far more reliable than scraping the full Sent
        # listing (which may span dozens of pages of regular sales)
        # and filtering in Python afterwards.
        #
        # We drive the select via both raw value AND the
        # bootstrap-select widget (which DIAN uses to render the
        # pretty dropdown) so the visible button label stays in sync
        # with the value the AJAX call reads on submit.
        if self.doc_type_filter == "support":
            dian_doc_type_code = "05"  # Documento soporte con no obligados
            try:
                doctype_applied = await self.page.evaluate(
                    """({code}) => {
                        const jq = window.jQuery || window.$;
                        let valueOk = false;
                        let widgetOk = false;
                        try {
                            const sel = document.querySelector(
                                '#DocumentTypeId'
                            );
                            if (sel) {
                                sel.value = code;
                                sel.dispatchEvent(
                                    new Event('change', { bubbles: true })
                                );
                                valueOk = sel.value === code;
                            }
                        } catch (e) { /* keep valueOk=false */ }
                        try {
                            if (jq) {
                                jq('#DocumentTypeId')
                                    .selectpicker('val', code)
                                    .selectpicker('refresh');
                                widgetOk = true;
                            }
                        } catch (e) { /* keep widgetOk=false */ }
                        return { valueOk, widgetOk };
                    }""",
                    {"code": dian_doc_type_code},
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
                            f"docType select set: code={dian_doc_type_code} "
                            f"(value={doctype_applied.get('valueOk')}, "
                            f"widget={doctype_applied.get('widgetOk')})"
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
                        notes=(
                            f"⚠ DocumentTypeId not set ({e}). Falling "
                            f"back to Python-side substring filter."
                        ),
                    )
                )

        # Submit. CRITICAL: DIAN's portal requires a real click on the
        # "Buscar" button — a naive form.submit() bypasses the jQuery
        # handler that writes the date range hidden inputs and fires
        # the AJAX call to /Document/GetDocumentsPageToken. Without
        # that handler running, DIAN ignores the date range we just
        # set and returns its default window (last N invoices),
        # producing the exact symptom of "asked for April, got June".
        #
        # We try several click strategies in order of how reliably
        # they dispatch the real DIAN handler:
        #
        #   1. .btn-radian-success — the actual class on DIAN's
        #      green Buscar button in the current layout (2026-06).
        #   2. button:has-text('Buscar') — text-based fallback that
        #      survives a class rename.
        #   3. .btn-search and #searchBtn — DIAN's legacy selectors
        #      that the click handler is still registered against,
        #      kept for older portal versions and as a safety net.
        #
        # We deliberately do NOT fall back to form.submit() any more.
        # If every click target failed it means DIAN's UI changed
        # again and we'd rather fail loudly here than silently send
        # the wrong range.
        submitted = False
        for sel in (
            ".btn-radian-success",
            "button:has-text('Buscar')",
            ".btn-search",
            "#searchBtn",
            "button:has-text('Consultar')",
        ):
            try:
                await self.page.click(sel, timeout=2000)
                submitted = True
                await self._emit(
                    DownloadEvent(
                        timestamp=datetime.utcnow().isoformat(),
                        sequence=0,
                        cufe="",
                        prefijo_folio="",
                        phase="log",
                        status="info",
                        notes=f"Clicked search button: {sel}",
                    )
                )
                break
            except Exception:
                continue
        if not submitted:
            # Loud failure with a snapshot so we can see WHAT DIAN
            # is serving when no known button is present. The list
            # phase will report empty results downstream, which is
            # the right outcome for a UI we can't drive.
            await self._emit(
                DownloadEvent(
                    timestamp=datetime.utcnow().isoformat(),
                    sequence=0,
                    cufe="",
                    prefijo_folio="",
                    phase="list",
                    status="fail",
                    error="no-search-button",
                    notes=(
                        "Could not find DIAN's 'Buscar' button. The "
                        "portal layout may have changed; check the "
                        "next listing-failed snapshot for the new "
                        "markup."
                    ),
                )
            )

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
                        // dataType is DIAN's numeric document type code
                        // (e.g. '01' Factura, '102' Nómina, '96' RADIAN).
                        // Populated server-side as `data-type` on the
                        // <tr> — far more reliable than parsing the
                        // visible 'Tipo' column text, which may be
                        // localized or truncated.
                        const dataType = node.getAttribute('data-type') || '';
                        const cells = [...node.querySelectorAll('td')].map(
                            (c) => (c.innerText || c.textContent || '').trim()
                        );
                        if (cells.length === 0) return;
                        // Skip empty-state placeholder rows
                        // ('No se encontraron resultados', etc.)
                        if (cells.length === 1 && !dataId) return;
                        out.push({ dataId, dataType, cells });
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
                        '.dt-paging li.paginate_button.next:not(.disabled) a',
                        '.dt-paging button[aria-label="Next"]:not(.disabled):not([aria-disabled="true"])',
                        'button.dt-paging-button[data-dt-idx="next"]:not(.disabled):not([aria-disabled="true"])',
                    ];
                    for (const sel of selectors) {
                        const btn = document.querySelector(sel);
                        if (btn) return btn;
                    }
                    return null;
                };

                /**
                 * Reads the DataTables "Mostrando registros del X al Y de
                 * Z" line and returns Z (recordsTotal). Returns null when
                 * the info line isn't present yet. This is the only
                 * RELIABLE way to know how many records DIAN's
                 * server-side endpoint says exist for the active filter
                 * — `allRows.length` only tells us what we've already
                 * pulled into memory.
                 *
                 * DataTables localises the line so we match by digits.
                 */
                const readDtRecordsTotal = () => {
                    const info = document.querySelector(
                        '.dataTables_info, .dt-info, #tableDocuments_info'
                    );
                    if (!info) return null;
                    const txt = (info.innerText || info.textContent || '').trim();
                    // Matches 'del 1 al 10 de 97 registros' /
                    // 'Showing 1 to 10 of 97 entries' / etc. We grab
                    // the LAST integer in the line as the total.
                    const matches = txt.match(/\d+/g);
                    if (!matches || matches.length === 0) return null;
                    const last = parseInt(matches[matches.length - 1], 10);
                    return Number.isFinite(last) ? last : null;
                };

                /**
                 * Try to switch the DataTable's page length to the
                 * largest available option (100 if present, else the max
                 * the dropdown offers). This collapses what would be 10
                 * "Next" clicks into 1 for tenants with heavy invoice
                 * volume. Falls back silently if the dropdown isn't
                 * there — old DIAN deployments without the selector are
                 * paginated 10-by-10 the slow way.
                 */
                const trySetPageLengthMax = async () => {
                    const sel = document.querySelector(
                        'select[name="tableDocuments_length"], '
                        + 'select.dataTables_length, '
                        + '.dataTables_length select, '
                        + '.dt-length select'
                    );
                    if (!sel) return false;
                    // Pick the highest option value <= 100. We cap at 100
                    // because DIAN's server-side endpoint may throttle
                    // or hard-cap larger pages with WAF rules; 100 is
                    // the standard DataTables ceiling.
                    let best = -1;
                    for (const opt of sel.options) {
                        const v = parseInt(opt.value, 10);
                        if (!Number.isFinite(v) || v <= 0) continue;
                        if (v > 100) continue;
                        if (v > best) best = v;
                    }
                    if (best <= 0) return false;
                    const currentRows = readCurrentPageRows();
                    const prevSig = currentRows.length > 0
                        ? `${currentRows[0].dataId}|${currentRows.length}`
                        : `EMPTY:0`;
                    sel.value = String(best);
                    sel.dispatchEvent(new Event('change', { bubbles: true }));
                    // Wait for the table to redraw with the new page
                    // size. We use the same change-detection signature
                    // we use for Next clicks.
                    const reloadDeadline = Math.min(
                        Date.now() + 8000,
                        deadline
                    );
                    while (Date.now() < reloadDeadline) {
                        const rows = readCurrentPageRows();
                        const sig = rows.length === 0
                            ? `EMPTY:0`
                            : `${rows[0].dataId}|${rows.length}`;
                        if (sig !== prevSig && rows.length > 0) return true;
                        await sleep(intervalMs);
                    }
                    return false;
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
                            dtRecordsTotal: readDtRecordsTotal(),
                            rows: [],
                            pages: 0,
                            pageLengthChanged: false,
                        }));
                        return;
                    }

                    // Bump the page length to the highest available
                    // option (typically 100) BEFORE walking pagination.
                    // For a tenant with 97 invoices on a single month,
                    // this collapses ~10 Next clicks into 1 — each click
                    // is a server-side draw against DIAN's listing
                    // endpoint, which is rate-limited and triggers WAF
                    // challenges when clicked too quickly.
                    const pageLengthChanged = await trySetPageLengthMax();
                    if (pageLengthChanged) {
                        // Re-read the (now larger) first page.
                        allRows = readCurrentPageRows();
                    }

                    // dtRecordsTotal is the authoritative server-side
                    // count; we use it to know when to stop paginating
                    // even if findNextButton() momentarily reports a
                    // disabled state during a redraw.
                    const dtRecordsTotal = readDtRecordsTotal();

                    let pageCount = 1;
                    let consecutiveEmptyClicks = 0;
                    while (Date.now() < deadline) {
                        if (maxInvoices > 0 && allRows.length >= maxInvoices) break;
                        if (
                            dtRecordsTotal !== null
                            && allRows.length >= dtRecordsTotal
                        ) {
                            // We've already pulled everything DataTables
                            // says exists. Don't click into a phantom
                            // page that would just show duplicates.
                            break;
                        }
                        const next = findNextButton();
                        if (!next) {
                            // The Next button can disappear briefly
                            // during a DataTables redraw. Wait a tick
                            // and look again — but only a small number
                            // of times before giving up.
                            consecutiveEmptyClicks++;
                            if (consecutiveEmptyClicks >= 3) break;
                            await sleep(intervalMs);
                            continue;
                        }
                        consecutiveEmptyClicks = 0;
                        // Signature based on FIRST visible row so we
                        // detect the page redraw correctly. Using
                        // allRows[allRows.length-1] as before compared
                        // the LAST row of the accumulated set against
                        // the FIRST row of the new page — which always
                        // differed and so never actually validated the
                        // change.
                        const visibleNow = readCurrentPageRows();
                        const prevSig = visibleNow.length === 0
                            ? `EMPTY:0`
                            : `${visibleNow[0].dataId}|${visibleNow.length}`;
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
                        dtRecordsTotal: dtRecordsTotal,
                        rows: allRows.slice(0, maxInvoices || allRows.length),
                        pages: pageCount,
                        pageLengthChanged: pageLengthChanged,
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
        # `dtRecordsTotal` is what the DataTables "info" line reports as
        # the server-side total, e.g. "del 1 al 100 de 97 registros".
        # `records_total` above is just len(rows_data) — what we DOWNloaded.
        # We log both so the operator can spot a mismatch (rows fell short
        # of the server-side total → pagination cut short).
        dt_records_total = result.get("dtRecordsTotal")
        method = result.get("method", "")
        pages = result.get("pages", 1)
        page_length_changed = bool(result.get("pageLengthChanged"))

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

        # Build candidates from DOM rows (no max_invoices cap yet — we
        # apply that AFTER skip_cufes so the cap protects against the
        # new-invoice budget, not the gross listing size).
        # DIAN's /Document/Received listing columns (as of 2026-06):
        #   [0]  download button cell (no text)
        #   [1]  Recepción           (delivery date)
        #   [2]  Fecha                (issue date)
        #   [3]  Prefijo
        #   [4]  N° documento
        #   [5]  Tipo                 (already exposed via dataType attr)
        #   [6]  NIT Emisor
        #   [7]  Emisor               (name)
        #   [8]  NIT Receptor
        #   [9]  Receptor
        #   [10] Resultado
        #   [11] Estado RADIAN
        #   [12] Valor Total
        #
        # The previous mapping (folio=cells[1], nit=cells[2], date=cells[3])
        # was inherited from an older DIAN UI and is silently broken on
        # the current portal: it ends up storing the delivery date as the
        # folio, the issue date as the NIT, and the prefijo as the issue
        # date. Symptom in the UI was every invoice showing '—' on the
        # Fecha column while the prefijo_folio cell looked like a date.
        candidates: list[InvoiceRow] = []
        for i, row in enumerate(rows_data):
            cells = row.get("cells", [])
            data_id = row.get("dataId", "")
            # Compose 'Prefijo + N° documento' for prefijo_folio so the
            # operator sees the same string DIAN renders (e.g. 'NE353',
            # 'FE-12345'). Falls back to either piece alone if one is
            # missing.
            prefix = cells[3] if len(cells) > 3 else ""
            number = cells[4] if len(cells) > 4 else ""
            if prefix and number:
                prefijo_folio = f"{prefix}{number}" if number.startswith(prefix) else f"{prefix}{number}"
            else:
                prefijo_folio = number or prefix or f"row-{i}"
            invoice = InvoiceRow(
                cufe=data_id or (cells[0] if cells else ""),
                track_id=data_id,
                prefijo_folio=prefijo_folio,
                issuer_nit=cells[6] if len(cells) > 6 else "",
                issue_date=cells[2] if len(cells) > 2 else "",
                raw=row,
            )
            if invoice.cufe:
                candidates.append(invoice)

        # Pre-download filter: drop rows that NUVARA's import pipeline
        # would always reject anyway. Three buckets, identified BEFORE
        # we spend download budget on them:
        #
        #   - Payroll (type 102 'Nomina individual electrónica' and 103
        #     'Nota de ajuste nómina'). NUVARA imports nómina through
        #     a separate flow, not via /Document/Received.
        #   - RADIAN event responses (type 96). Status messages, not
        #     invoices.
        #   - Total value parses to 0 (e.g. cancelled contingencies).
        #     Causation has nothing to book for them.
        #
        # The decision uses the <tr>'s data-type attribute when
        # present; we fall back to substring matching on the visible
        # 'Tipo' column ONLY when the attribute is missing, so a
        # DIAN markup regression can't silently start filtering
        # everything by mismatching the numeric codes.
        PAYROLL_TYPES = {"102", "103"}
        RADIAN_TYPES = {"96"}
        nomina_filtered = 0
        radian_filtered = 0
        zero_filtered = 0
        kept: list[InvoiceRow] = []
        for inv in candidates:
            raw = inv.raw if isinstance(inv.raw, dict) else {}
            data_type = str(raw.get("dataType") or "").strip()
            cells = raw.get("cells") or []
            tipo_text = cells[5].lower() if len(cells) > 5 else ""
            total_text = cells[12] if len(cells) > 12 else ""
            # Numeric type code from the row attribute is preferred.
            # If empty, sniff the visible 'Tipo' column for 'nomina'
            # or 'radian' as a defensive secondary check.
            is_payroll = data_type in PAYROLL_TYPES or (
                not data_type and "nomina" in tipo_text
            )
            is_radian = data_type in RADIAN_TYPES or (
                not data_type and "radian" in tipo_text
            )
            # Normalize total value: strip currency sign, NBSPs and
            # thousand/decimal separators. After the strip a 'zero'
            # in any locale (e.g. '0', '0.00' → '000', '0,00' → '000')
            # collapses to a non-empty string of nothing but '0' digits.
            # The empty-string case means an empty cell — we don't
            # treat that as zero because we can't tell whether DIAN
            # served an actual value of 0 or just left the cell blank.
            normalized_total = (
                total_text
                .replace("$", "")
                .replace("\u00a0", "")  # non-breaking space
                .replace(" ", "")
                .replace(".", "")
                .replace(",", "")
                .strip()
            )
            is_zero_value = (
                bool(total_text)
                and normalized_total != ""
                and all(c == "0" for c in normalized_total)
            )
            if is_payroll:
                nomina_filtered += 1
                continue
            if is_radian:
                radian_filtered += 1
                continue
            if is_zero_value:
                zero_filtered += 1
                continue
            kept.append(inv)

        total_pre_filtered = (
            nomina_filtered + radian_filtered + zero_filtered
        )
        if total_pre_filtered > 0:
            await self._emit(
                DownloadEvent(
                    timestamp=datetime.utcnow().isoformat(),
                    sequence=0,
                    cufe="",
                    prefijo_folio="",
                    phase="list",
                    status="info",
                    notes=(
                        f"Omitted {total_pre_filtered} rows pre-download: "
                        f"{nomina_filtered} nómina, "
                        f"{radian_filtered} RADIAN, "
                        f"{zero_filtered} valor cero "
                        f"— {len(kept)} candidates remain."
                    ),
                )
            )
        candidates = kept

        # docType narrow-down. Today only the `support` family is
        # modelled (DS / DE). We keep only rows whose document type
        # matches the family. Three-layer detection so a DIAN markup
        # tweak can't silently drop everything:
        #
        #   1. data-type attribute on the <tr> with a known DS / DE
        #      numeric code (preferred — most stable across UI
        #      revisions).
        #   2. The visible "Tipo" column (cells[5]) containing the
        #      short code "DS" or "DE" as a whole word.
        #   3. ANY cell containing the long Spanish phrase
        #      "documento soporte" / "documento equivalente".
        #
        # Any of the three matching keeps the row. NUVARA uses this
        # on `support` runs to pull DS / DE out of the Sent bucket
        # without downloading regular sales.
        if self.doc_type_filter == "support":
            SUPPORT_DATA_TYPES = {"05", "11", "5", "12"}
            support_kept: list[InvoiceRow] = []
            doc_type_filtered = 0
            for inv in candidates:
                raw = inv.raw if isinstance(inv.raw, dict) else {}
                cells = raw.get("cells") or []
                data_type = str(raw.get("dataType") or "").strip()
                tipo_text = (cells[5] if len(cells) > 5 else "").lower().strip()
                # Layer 1: numeric code on the <tr>.
                if data_type in SUPPORT_DATA_TYPES:
                    support_kept.append(inv)
                    continue
                # Layer 2: short code in the "Tipo" column. We match
                # the whole word so we don't false-positive on e.g.
                # 'DSF' or 'DEUDA'.
                if tipo_text in ("ds", "de"):
                    support_kept.append(inv)
                    continue
                # Layer 3: long phrase in ANY cell. This catches the
                # case where DIAN dropped a "Documento soporte con no
                # obligados" description into a column other than
                # Tipo (e.g. between Tipo and NIT Emisor). Match is
                # accent-insensitive lowercase substring.
                all_cells_text = " ".join(
                    str(c or "").lower() for c in cells
                )
                if (
                    "documento soporte" in all_cells_text
                    or "documento equivalente" in all_cells_text
                ):
                    support_kept.append(inv)
                    continue
                doc_type_filtered += 1
            if doc_type_filtered > 0 or len(support_kept) > 0:
                await self._emit(
                    DownloadEvent(
                        timestamp=datetime.utcnow().isoformat(),
                        sequence=0,
                        cufe="",
                        prefijo_folio="",
                        phase="list",
                        status="info",
                        notes=(
                            f"docType filter (support): kept "
                            f"{len(support_kept)} DS / DE, dropped "
                            f"{doc_type_filtered} other rows from the "
                            f"Sent bucket."
                        ),
                    )
                )
            candidates = support_kept

        # Skip CUFEs the consumer (NUVARA) already has. We log the
        # skipped count separately so an operator can see at a glance
        # that the engine isn't re-downloading already-imported
        # invoices. Skip BEFORE the max_invoices cap: re-downloading a
        # known invoice and counting it toward the cap would defeat
        # the purpose of telling us to skip it.
        skipped_count = 0
        if self.skip_cufes:
            filtered: list[InvoiceRow] = []
            for inv in candidates:
                # Defensive lowercasing in case DIAN ever ships mixed
                # case in the data-id attribute. The skip set is
                # already lowercase (see __init__).
                if (inv.cufe or "").lower() in self.skip_cufes:
                    skipped_count += 1
                    continue
                filtered.append(inv)
            candidates = filtered
            if skipped_count > 0:
                await self._emit(
                    DownloadEvent(
                        timestamp=datetime.utcnow().isoformat(),
                        sequence=0,
                        cufe="",
                        prefijo_folio="",
                        phase="list",
                        status="info",
                        notes=(
                            f"Skipped {skipped_count} CUFEs already known "
                            f"to the consumer; {len(candidates)} new to "
                            f"download (skip_cufes size={len(self.skip_cufes)})"
                        ),
                    )
                )

        # Apply max_invoices cap on the post-skip set.
        invoices = candidates[: self.max_invoices] if self.max_invoices > 0 else candidates

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
                    f"(rowsScraped={records_total}, "
                    f"dtServerTotal={dt_records_total}, "
                    f"method={method}, pages={pages}, "
                    f"pageLen100={page_length_changed}, "
                    f"max_cap={self.max_invoices}, "
                    f"skipped={skipped_count}, "
                    f"pre_filtered={total_pre_filtered})"
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
                # Referer matches the bucket we navigated to. DIAN
                # validates this on download endpoints — using the
                # wrong one yields a 403 on sale runs.
                "Referer": self.list_url,
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
        # Mid-run deauth detection: when DIAN's session quietly expires
        # mid-job we keep getting redirected to /login, which surfaces
        # as fail/block downloads. Counter wakes up the moment a
        # download isn't OK; we sniff the current page URL and if it's
        # on the auth path we raise a typed ScraperError after the
        # second consecutive non-ok. Partial successes already
        # collected via file_callback stay in the consumer's hands.
        consecutive_non_ok = 0
        DEAUTH_THRESHOLD = 2
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

            if event.status == "ok":
                consecutive_non_ok = 0
            else:
                consecutive_non_ok += 1
                if consecutive_non_ok >= DEAUTH_THRESHOLD:
                    # Sniff the live URL. If DIAN bumped us back to the
                    # login wall the rest of the run is doomed — abort
                    # with a typed error so the consumer can tell the
                    # operator "ask for a new token".
                    current_url = ""
                    try:
                        if self.page is not None:
                            current_url = self.page.url or ""
                    except Exception:
                        current_url = ""
                    if (
                        "login" in current_url.lower()
                        or "/User/Auth" in current_url
                    ):
                        await self._emit(
                            DownloadEvent(
                                timestamp=datetime.utcnow().isoformat(),
                                sequence=i,
                                cufe="",
                                prefijo_folio="",
                                phase="summary",
                                status="fail",
                                notes=(
                                    f"DIAN redirected to {current_url[:120]} "
                                    f"after {consecutive_non_ok} failed "
                                    f"downloads — session expired mid-run."
                                ),
                            )
                        )
                        # Snapshot so the operator can confirm in the
                        # UI exactly what DIAN was showing.
                        try:
                            await self._snapshot_for_diagnostics(
                                tag="midrun-deauth",
                                landed_url=current_url,
                            )
                        except Exception:
                            pass
                        raise ScraperError(
                            ERROR_KIND_AUTH_EXPIRED_MIDRUN,
                            (
                                "DIAN session expired mid-run. "
                                f"{len([e for e in self.logger.events if e.phase == 'download' and e.status == 'ok'])} "
                                "files were saved before the session went "
                                "away; the rest need a fresh token."
                            ),
                        )

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
