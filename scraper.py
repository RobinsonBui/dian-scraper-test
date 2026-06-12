"""DIAN Scraper Test — standalone human-like scraper.

Tests the hypothesis that Nuvara's blocking issue is caused by switching from
Playwright (login) to Python `requests` (download). Here we do EVERYTHING in
the same browser context.

Usage:
    python scraper.py \\
        --auth-url "https://catalogo-vpfe.dian.gov.co/User/AuthToken?pk=..." \\
        --start-date 2026-05-01 \\
        --end-date 2026-05-31 \\
        --max-invoices 50
"""

from __future__ import annotations

import asyncio
import json
import random
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any

import click
from playwright.async_api import (
    APIResponse,
    Browser,
    BrowserContext,
    Page,
    Playwright,
    async_playwright,
)
from rich.console import Console
from rich.live import Live
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

console = Console()

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

# Modern Firefox UA — match Camoufox baseline
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
    phase: str  # "list" | "download" | "block_detected" | "reauth" | "summary"
    status: str  # "ok" | "fail" | "block" | "info"
    http_status: int | None = None
    elapsed_ms: int | None = None
    bytes_downloaded: int | None = None
    error: str | None = None
    response_headers: dict[str, str] = field(default_factory=dict)
    notes: str | None = None


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

    def summary(self) -> str:
        downloads = [e for e in self.events if e.phase == "download"]
        ok = sum(1 for e in downloads if e.status == "ok")
        fail = sum(1 for e in downloads if e.status == "fail")
        blocks = sum(1 for e in downloads if e.status == "block")
        latencies = [e.elapsed_ms for e in downloads if e.elapsed_ms is not None]

        avg_latency = (
            sum(latencies) / len(latencies) if latencies else 0
        )
        p95_latency = (
            sorted(latencies)[int(len(latencies) * 0.95)]
            if len(latencies) > 5
            else 0
        )

        # First block / first fail
        first_block = next(
            (e for e in downloads if e.status == "block"), None
        )
        first_fail = next(
            (e for e in downloads if e.status == "fail"), None
        )

        return (
            f"\n{'=' * 60}\n"
            f" DIAN SCRAPER TEST — SUMMARY\n"
            f"{'=' * 60}\n"
            f"Total invoices attempted: {len(downloads)}\n"
            f"  ✓ Successful: {ok}\n"
            f"  ✗ Failed:     {fail}\n"
            f"  🚫 Blocked:   {blocks}\n"
            f"\n"
            f"Latency:\n"
            f"  Average: {avg_latency:.0f} ms\n"
            f"  P95:     {p95_latency} ms\n"
            f"\n"
            f"First block: "
            f"{f'seq #{first_block.sequence} (cufe {first_block.cufe[:20]}...)' if first_block else 'NEVER ✓'}\n"
            f"First fail:  "
            f"{f'seq #{first_fail.sequence} cufe {first_fail.cufe[:20]}... error: {first_fail.error}' if first_fail else 'NEVER ✓'}\n"
            f"\n"
            f"Log file: {self.log_path}\n"
            f"{'=' * 60}\n"
        )


# --------------------------------------------------------------------------
# Human-like timing
# --------------------------------------------------------------------------


async def human_delay(sequence: int) -> int:
    """Sleep with realistic jitter. Returns ms slept."""
    base = random.randint(HUMAN_DELAY_MIN_MS, HUMAN_DELAY_MAX_MS)
    long_pause = 0
    if sequence > 0 and sequence % LONG_PAUSE_EVERY_N == 0:
        long_pause = random.randint(LONG_PAUSE_MIN_MS, LONG_PAUSE_MAX_MS)
        console.log(
            f"[yellow]💤 Long pause: sleeping {long_pause / 1000:.1f}s "
            f"(after {sequence} downloads)[/yellow]"
        )
    total = base + long_pause
    await asyncio.sleep(total / 1000.0)
    return total


# --------------------------------------------------------------------------
# Block detection
# --------------------------------------------------------------------------


def detect_block(response: APIResponse, body_preview: bytes) -> tuple[bool, str | None]:
    """Detect if response is a WAF block. Returns (is_blocked, reason)."""
    if response.status == 429:
        return True, "HTTP 429 Too Many Requests"
    if response.status == 403:
        # Could be Azure WAF or DIAN security rule
        body_text = body_preview.decode("utf-8", errors="replace").lower()
        if "azure" in body_text or "cloudflare" in body_text or "blocked" in body_text:
            return True, "HTTP 403 with WAF signature in body"
        return True, "HTTP 403 (suspected WAF)"

    # Azure-specific headers
    headers = dict(response.headers)
    if "x-azure-ref" in headers and response.status >= 400:
        return True, f"Azure ref signaled: {headers.get('x-azure-ref')}"
    if "cf-mitigated" in headers:
        return True, f"Cloudflare mitigation: {headers.get('cf-mitigated')}"

    # DIAN-specific: redirect to login = session blocked or expired
    if response.status in (301, 302, 303, 307, 308):
        location = headers.get("location", "")
        if "login" in location.lower() or "/User/" in location:
            return True, f"Redirect to login: {location[:120]}"

    return False, None


# --------------------------------------------------------------------------
# Main scraper class
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
        headless: bool = False,
    ) -> None:
        self.auth_url = auth_url
        self.start_date = start_date
        self.end_date = end_date
        self.max_invoices = max_invoices
        self.downloads_dir = downloads_dir
        self.logger = logger
        self.headless = headless

        self.pw: Playwright | None = None
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None
        self.page: Page | None = None

    async def __aenter__(self) -> "DianTestScraper":
        self.pw = await async_playwright().start()
        self.browser = await self.pw.chromium.launch(
            headless=self.headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        self.context = await self.browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 768},
            locale="es-CO",
            timezone_id="America/Bogota",
            extra_http_headers={
                "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
            },
        )
        # Hide webdriver flag
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
        """Open auth URL and verify we're inside the portal."""
        console.log(f"[cyan]🔐 Opening auth URL...[/cyan]")
        assert self.page is not None
        await self.page.goto(self.auth_url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(2)
        current_url = self.page.url
        console.log(f"[cyan]   landed on: {current_url}[/cyan]")

        if "login" in current_url.lower() or "/User/Auth" in current_url:
            raise RuntimeError(
                "Auth URL expired or invalid. Re-login in your real browser "
                "and copy a fresh URL from your DIAN email."
            )

        self.logger.emit(
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
        """List invoices in date range from /Document/Received."""
        console.log(
            f"[cyan]📋 Listing invoices {self.start_date}..{self.end_date} "
            f"(max {self.max_invoices})...[/cyan]"
        )
        assert self.page is not None and self.context is not None

        # Go to the Received documents page
        await self.page.goto(RECEIVED_URL, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(2)

        # DIAN uses a date picker form and a server-side table.
        # Strategy: fill the form, submit, then read rows via page.evaluate.

        # Format dates as DIAN expects: dd/MM/yyyy
        start_dian = _to_dian_date(self.start_date, end=False)
        end_dian = _to_dian_date(self.end_date, end=True)

        try:
            # Fill date range. Selectors are best-effort; portal may change.
            await self.page.fill("#startDate, [name='StartDate']", start_dian, timeout=5000)
            await self.page.fill("#endDate, [name='EndDate']", end_dian, timeout=5000)
            console.log(f"[dim]   form filled: {start_dian}..{end_dian}[/dim]")
        except Exception as e:
            console.log(
                f"[yellow]⚠ Could not fill date inputs ({e}). "
                f"Trying to use whatever portal returns by default.[/yellow]"
            )

        # Submit search button if present
        try:
            await self.page.click("button[type='submit'], #searchBtn, .btn-search", timeout=3000)
            await self.page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            console.log("[dim]   no obvious search button; assuming auto-load.[/dim]")

        await asyncio.sleep(2)

        # Try to read rows. Selectors are placeholders — adjust on real portal.
        rows_data = await self.page.evaluate(
            """() => {
                const rows = document.querySelectorAll('tr[data-id], tbody tr');
                const out = [];
                rows.forEach(row => {
                    const dataId = row.getAttribute('data-id') || '';
                    const cells = [...row.querySelectorAll('td')].map(c => c.innerText.trim());
                    if (cells.length === 0) return;
                    out.push({
                        dataId,
                        cells,
                    });
                });
                return out;
            }"""
        )

        invoices: list[InvoiceRow] = []
        for i, row in enumerate(rows_data):
            if i >= self.max_invoices:
                break
            cells = row.get("cells", [])
            data_id = row.get("dataId", "")
            # Heuristic mapping — adjust if portal columns differ
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

        console.log(
            f"[green]✓ Found {len(invoices)} invoices "
            f"(showing first {min(3, len(invoices))}):[/green]"
        )
        for inv in invoices[:3]:
            console.log(
                f"   {inv.prefijo_folio} | nit={inv.issuer_nit} | "
                f"cufe={inv.cufe[:30]}..."
            )

        self.logger.emit(
            DownloadEvent(
                timestamp=datetime.utcnow().isoformat(),
                sequence=0,
                cufe="",
                prefijo_folio="",
                phase="list",
                status="ok",
                notes=f"found {len(invoices)} invoices in range",
            )
        )

        return invoices

    async def download_invoice(
        self, invoice: InvoiceRow, sequence: int
    ) -> DownloadEvent:
        """Download a single invoice ZIP using THE SAME browser context.

        Critical: uses page.context.request (Playwright's HTTP client that
        shares cookies, TLS, headers with the browser session). NOT a separate
        `requests.Session()` like Nuvara does today.
        """
        assert self.context is not None

        start = time.monotonic()
        url = f"{GETFILE_PDF_URL}?cune={invoice.cufe.lower()}"

        try:
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

            # Check for blocking
            is_blocked, block_reason = detect_block(response, body[:1024])

            if is_blocked:
                event = DownloadEvent(
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
                    response_headers={
                        k: v for k, v in headers.items()
                        if k.lower() in (
                            "content-type", "x-azure-ref",
                            "cf-mitigated", "location", "server",
                            "retry-after",
                        )
                    },
                )
                return event

            if response.status == 200 and len(body) > 0:
                # Check magic bytes: ZIP starts with PK\x03\x04
                if body[:2] == b"PK":
                    out_path = self.downloads_dir / f"{invoice.cufe[:20]}.zip"
                    out_path.write_bytes(body)
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
                        notes=f"saved to {out_path.name}",
                    )
                else:
                    # 200 but not a ZIP → probably HTML challenge page
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
                        response_headers={
                            "content-type": headers.get("content-type", ""),
                        },
                    )

            # Other status
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
            )

        except Exception as e:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return DownloadEvent(
                timestamp=datetime.utcnow().isoformat(),
                sequence=sequence,
                cufe=invoice.cufe,
                prefijo_folio=invoice.prefijo_folio,
                phase="download",
                status="fail",
                elapsed_ms=elapsed_ms,
                error=f"{type(e).__name__}: {e}",
            )

    async def run(self) -> None:
        """Main loop: auth → list → download with human pacing."""
        await self.authenticate()
        invoices = await self.list_invoices()

        if not invoices:
            console.log("[red]No invoices found. Check date range or auth.[/red]")
            return

        console.print(f"\n[bold cyan]Starting downloads ({len(invoices)} invoices)...[/bold cyan]\n")

        consecutive_blocks = 0
        for i, invoice in enumerate(invoices, start=1):
            if i > 1:
                slept_ms = await human_delay(i - 1)
                console.log(f"[dim]   slept {slept_ms / 1000:.1f}s[/dim]")

            event = await self.download_invoice(invoice, i)
            self.logger.emit(event)

            status_emoji = {
                "ok": "✓",
                "fail": "✗",
                "block": "🚫",
            }.get(event.status, "?")
            status_color = {
                "ok": "green",
                "fail": "yellow",
                "block": "red",
            }.get(event.status, "white")

            console.log(
                f"[{status_color}]{status_emoji} #{i:3d}/{len(invoices)} "
                f"{invoice.prefijo_folio:20s} "
                f"status={event.http_status} "
                f"{event.elapsed_ms}ms "
                f"bytes={event.bytes_downloaded or 0}[/{status_color}] "
                f"{event.error or event.notes or ''}"
            )

            if event.status == "block":
                consecutive_blocks += 1
                if consecutive_blocks >= 3:
                    console.print(
                        f"\n[bold red]🛑 3 consecutive blocks detected. "
                        f"Stopping to preserve forensics.[/bold red]\n"
                    )
                    self.logger.emit(
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


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _to_dian_date(iso_date: str, end: bool) -> str:
    """Convert YYYY-MM-DD to dd/MM/yyyy as DIAN expects."""
    dt = datetime.fromisoformat(iso_date)
    return dt.strftime("%d/%m/%Y")


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


@click.command()
@click.option("--auth-url", required=True, help="DIAN auth URL from email.")
@click.option("--start-date", required=True, help="ISO date YYYY-MM-DD.")
@click.option("--end-date", required=True, help="ISO date YYYY-MM-DD.")
@click.option("--max-invoices", default=50, type=int, help="Max invoices to download.")
@click.option(
    "--headless/--no-headless",
    default=False,
    help="Run browser headless (default: visible so you can watch).",
)
@click.option(
    "--downloads-dir",
    default="downloads",
    type=click.Path(),
    help="Where to save downloaded ZIPs.",
)
def main(
    auth_url: str,
    start_date: str,
    end_date: str,
    max_invoices: int,
    headless: bool,
    downloads_dir: str,
) -> None:
    """Run the DIAN scraper test."""
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    base_dir = Path(__file__).resolve().parent
    log_path = base_dir / "logs" / f"run-{timestamp}.jsonl"
    downloads_path = base_dir / downloads_dir
    downloads_path.mkdir(parents=True, exist_ok=True)

    logger = Logger(log_path)

    console.print(
        f"\n[bold]DIAN Scraper Test[/bold]\n"
        f"  start_date: {start_date}\n"
        f"  end_date:   {end_date}\n"
        f"  max:        {max_invoices}\n"
        f"  headless:   {headless}\n"
        f"  log:        {log_path.relative_to(base_dir)}\n"
        f"  downloads:  {downloads_path.relative_to(base_dir)}\n"
    )

    async def runner() -> None:
        try:
            async with DianTestScraper(
                auth_url=auth_url,
                start_date=start_date,
                end_date=end_date,
                max_invoices=max_invoices,
                downloads_dir=downloads_path,
                logger=logger,
                headless=headless,
            ) as scraper:
                await scraper.run()
        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted by user.[/yellow]")
        except Exception as e:
            console.print(f"\n[bold red]Fatal: {type(e).__name__}: {e}[/bold red]")
            logger.emit(
                DownloadEvent(
                    timestamp=datetime.utcnow().isoformat(),
                    sequence=0,
                    cufe="",
                    prefijo_folio="",
                    phase="summary",
                    status="fail",
                    error=f"{type(e).__name__}: {e}",
                )
            )
            raise
        finally:
            summary = logger.summary()
            summary_path = log_path.with_name(log_path.stem + "-summary.txt")
            summary_path.write_text(summary, encoding="utf-8")
            console.print(summary)
            console.print(
                f"[bold green]Summary saved to {summary_path.relative_to(base_dir)}[/bold green]"
            )
            logger.close()

    asyncio.run(runner())


if __name__ == "__main__":
    main()
