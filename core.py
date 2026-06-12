"""Core scraper logic — extracted from scraper.py to be reused by the web UI.

This module exposes:
- `DianTestScraper` — the same Playwright-based scraper, but with a
  `progress_callback` so the web server can stream updates over WebSocket.
- `DownloadEvent` — structured event.
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
        headless: bool = True,
        delay_min_ms: int = HUMAN_DELAY_MIN_MS,
        delay_max_ms: int = HUMAN_DELAY_MAX_MS,
        long_pause_every: int = LONG_PAUSE_EVERY_N,
        cancel_event: asyncio.Event | None = None,
    ) -> None:
        self.auth_url = auth_url
        self.start_date = start_date
        self.end_date = end_date
        self.max_invoices = max_invoices
        self.downloads_dir = downloads_dir
        self.logger = logger
        self.progress_callback = progress_callback
        self.headless = headless
        self.delay_min_ms = delay_min_ms
        self.delay_max_ms = delay_max_ms
        self.long_pause_every = long_pause_every
        self.cancel_event = cancel_event or asyncio.Event()

        self.pw: Playwright | None = None
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None
        self.page: Page | None = None

    async def _emit(self, event: DownloadEvent) -> None:
        self.logger.emit(event)
        if self.progress_callback:
            try:
                await self.progress_callback(event)
            except Exception:
                pass

    async def __aenter__(self) -> "DianTestScraper":
        self.pw = await async_playwright().start()

        # Optional proxy via env var (e.g. exit-node in Colombia so DIAN
        # responds fast / doesn't geo-throttle our BR-hosted server).
        proxy_cfg = _parse_proxy_url(os.environ.get("PROXY_URL"))

        launch_kwargs: dict[str, Any] = {
            "headless": self.headless,
            "args": ["--disable-blink-features=AutomationControlled"],
        }
        if proxy_cfg:
            launch_kwargs["proxy"] = proxy_cfg
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
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.pw:
            await self.pw.stop()

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
        await asyncio.sleep(2)
        current_url = self.page.url
        if "login" in current_url.lower() or "/User/Auth" in current_url:
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

        # DIAN renders the listing via jQuery DataTables with server-side
        # AJAX pagination. The DOM only contains the rows of the CURRENT
        # page (default pageLength=10), so a naive querySelectorAll only
        # returns the first 10 rows.
        #
        # Strategy (mirrors Nuvara's dian_invoice_scraper.py:88-267):
        #   1. Wait for DataTables to be ready.
        #   2. If recordsTotal <= length → all on one page.
        #   3. Otherwise: set page.len(100), then iterate dt.page(n).draw()
        #      using the draw.dt event to wait for each AJAX response.
        #   4. Concat all rows across pages.
        result_json = await self.page.evaluate(
            """({ maxWaitMs, intervalMs, maxInvoices }) => new Promise((resolve) => {
                let elapsed = 0;
                const getJQ = () => {
                    if (typeof jQuery !== 'undefined') return jQuery;
                    if (typeof $ !== 'undefined' && $.fn) return $;
                    return null;
                };

                const collectAllRows = (dt) => {
                    // dt.rows().data() returns the raw row data from the
                    // ajax payload (array per row). We also want the DOM
                    // node so we can read data-id and cell text reliably.
                    const out = [];
                    dt.rows().every(function (rowIdx) {
                        const node = this.node();
                        if (!node) return;
                        const dataId = node.getAttribute('data-id') || '';
                        const cells = [...node.querySelectorAll('td')].map(
                            (c) => (c.innerText || c.textContent || '').trim()
                        );
                        if (cells.length === 0) return;
                        out.push({ dataId, cells });
                        if (maxInvoices > 0 && out.length >= maxInvoices) {
                            return false; // break
                        }
                    });
                    return out;
                };

                const startPagination = (dt, info, remaining) => {
                    const pageSize = info.length || 100;
                    const totalPages = Math.ceil(info.recordsTotal / pageSize);
                    let allRows = collectAllRows(dt);
                    let done = false;

                    if (totalPages <= 1 || (maxInvoices > 0 && allRows.length >= maxInvoices)) {
                        resolve(JSON.stringify({
                            ok: true,
                            method: 'single-page-rows',
                            recordsTotal: info.recordsTotal,
                            rows: allRows.slice(0, maxInvoices || allRows.length),
                            pages: 1,
                        }));
                        return;
                    }

                    let nextPage = 1;
                    const safety = setTimeout(() => {
                        if (done) return;
                        done = true;
                        resolve(JSON.stringify({
                            ok: true,
                            method: 'pagination-timeout',
                            warning: 'safety timer fired',
                            recordsTotal: info.recordsTotal,
                            rows: allRows.slice(0, maxInvoices || allRows.length),
                            pages: nextPage,
                            totalPages,
                        }));
                    }, Math.max(remaining, 60000));

                    const collectNext = () => {
                        dt.one('draw.dt', () => {
                            if (done) return;
                            const pageRows = collectAllRows(dt);
                            // collectAllRows reads from CURRENT page only when
                            // using `.rows({page:'current'})`. With `.rows()`
                            // it returns everything DT has buffered which may
                            // re-include earlier pages — to be safe we use
                            // the DOM-restricted version below.
                            const tbody = document.querySelectorAll(
                                'table.dataTable tbody tr'
                            );
                            const onlyThisPage = [];
                            tbody.forEach((node) => {
                                const dataId = node.getAttribute('data-id') || '';
                                const cells = [...node.querySelectorAll('td')].map(
                                    (c) => (c.innerText || c.textContent || '').trim()
                                );
                                if (cells.length === 0) return;
                                onlyThisPage.push({ dataId, cells });
                            });
                            allRows = allRows.concat(onlyThisPage);
                            nextPage++;
                            const reachedCap =
                                maxInvoices > 0 && allRows.length >= maxInvoices;
                            if (nextPage < totalPages && !reachedCap) {
                                collectNext();
                            } else {
                                done = true;
                                clearTimeout(safety);
                                resolve(JSON.stringify({
                                    ok: true,
                                    method: 'pagination',
                                    recordsTotal: info.recordsTotal,
                                    rows: allRows.slice(0, maxInvoices || allRows.length),
                                    pages: nextPage,
                                    totalPages,
                                }));
                            }
                        });
                        dt.page(nextPage).draw(false);
                    };

                    // First page is already collected via collectAllRows above.
                    collectNext();
                };

                const waitForDT = () => {
                    const jq = getJQ();
                    if (!jq || !jq.fn || !jq.fn.dataTable) {
                        if (elapsed >= maxWaitMs) {
                            resolve(JSON.stringify({
                                ok: false,
                                error: 'timeout-no-jquery-or-datatables',
                                hint: 'DIAN may not be using jQuery DataTables on this page',
                            }));
                            return;
                        }
                        elapsed += intervalMs;
                        setTimeout(waitForDT, intervalMs);
                        return;
                    }

                    try {
                        const tables = jq.fn.dataTable.tables();
                        if (!tables || tables.length === 0) {
                            if (elapsed >= maxWaitMs) {
                                resolve(JSON.stringify({
                                    ok: false,
                                    error: 'no-datatable-instance',
                                }));
                                return;
                            }
                            elapsed += intervalMs;
                            setTimeout(waitForDT, intervalMs);
                            return;
                        }

                        const dt = jq(tables[0]).DataTable();
                        const info = dt.page.info();

                        if (info.recordsTotal === 0) {
                            if (elapsed >= maxWaitMs) {
                                resolve(JSON.stringify({
                                    ok: true,
                                    method: 'zero-records',
                                    recordsTotal: 0,
                                    rows: [],
                                }));
                                return;
                            }
                            elapsed += intervalMs;
                            setTimeout(waitForDT, intervalMs);
                            return;
                        }

                        if (info.recordsTotal <= info.length) {
                            // All records fit on one page
                            const rows = collectAllRows(dt);
                            resolve(JSON.stringify({
                                ok: true,
                                method: 'single-page',
                                recordsTotal: info.recordsTotal,
                                rows: rows.slice(0, maxInvoices || rows.length),
                                pages: 1,
                            }));
                            return;
                        }

                        // Bump page size to 100 (DIAN max) to reduce rounds
                        if (info.length < 100) {
                            dt.one('draw.dt', () => {
                                const newInfo = dt.page.info();
                                if (newInfo.recordsTotal <= newInfo.length) {
                                    const rows = collectAllRows(dt);
                                    resolve(JSON.stringify({
                                        ok: true,
                                        method: 'single-page-after-resize',
                                        recordsTotal: newInfo.recordsTotal,
                                        rows: rows.slice(0, maxInvoices || rows.length),
                                        pages: 1,
                                    }));
                                } else {
                                    startPagination(dt, newInfo, maxWaitMs - elapsed);
                                }
                            });
                            dt.page.len(100).draw();
                            return;
                        }

                        startPagination(dt, info, maxWaitMs - elapsed);
                    } catch (err) {
                        if (elapsed >= maxWaitMs) {
                            resolve(JSON.stringify({
                                ok: false,
                                error: 'datatables-error: ' + err.message,
                            }));
                            return;
                        }
                        elapsed += intervalMs;
                        setTimeout(waitForDT, intervalMs);
                    }
                };

                waitForDT();
            })""",
            {
                "maxWaitMs": 120000,
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
            return []

        rows_data = result.get("rows", [])
        records_total = result.get("recordsTotal", 0)
        method = result.get("method", "")
        pages = result.get("pages", 1)

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
                    zip_path = self.downloads_dir / f"{safe_id}.zip"
                    zip_path.write_bytes(body)

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
