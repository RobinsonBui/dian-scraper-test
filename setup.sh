#!/usr/bin/env bash
# Setup script — one-shot install for the DIAN scraper test.
set -euo pipefail

cd "$(dirname "$0")"

echo "==> Creating venv (using uv if available, else python -m venv)"
if command -v uv > /dev/null 2>&1; then
    uv venv
    source .venv/bin/activate
    # Install deps directly from requirements.txt — this is a script project,
    # not a package, so we skip `pip install -e .` (which trips on logs/, downloads/).
    uv pip install -r requirements.txt
else
    python3 -m venv .venv
    source .venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
fi

echo "==> Installing Chromium for Playwright (this may take a couple minutes)"
playwright install chromium

echo ""
echo "✓ Setup complete."
echo ""
echo "Activate the venv:"
echo "  source .venv/bin/activate"
echo ""
echo "Run a test:"
echo "  python scraper.py --auth-url '<paste-here>' \\"
echo "    --start-date 2026-05-01 --end-date 2026-05-31 --max-invoices 30"
echo ""
