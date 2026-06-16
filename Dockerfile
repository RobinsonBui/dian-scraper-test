# Dockerfile — DIAN scraper test (FastAPI + Playwright Chromium)
#
# Uses Microsoft's official Playwright image so Chromium + system libs
# (libnss3, libatk, fonts, etc.) are already wired up. Saves us from
# 30 lines of apt-get and weird "browser closed unexpectedly" errors.

FROM mcr.microsoft.com/playwright/python:v1.60.0-jammy

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install Python deps first so this layer caches across code changes
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# App code. Each Python module is listed explicitly so the image
# doesn't accidentally pick up local dev artefacts (downloads/,
# logs/, __pycache__, .env, …). When adding a new module remember
# to mirror it here — the CI image won't see it otherwise.
#
# Layout:
#   server.py            FastAPI app + endpoints + lifespan
#   core.py / scraper.py Playwright scrapers (core is the one the
#                        server imports; scraper is the standalone CLI)
#   backend.py           JobBackend Protocol + in-memory + postgres+R2
#                        implementations. Imported by server.py at boot.
#   db.py                asyncpg JobStore — used only when DB_MODE=postgres.
#   r2.py                boto3 R2 client — used only when STORAGE_MODE=r2.
#   db/bootstrap.sql     One-shot schema for the postgres container, mounted
#                        as /docker-entrypoint-initdb.d/ in compose. The
#                        image still ships it so an operator can re-apply
#                        it manually from inside the container if needed.
COPY core.py scraper.py server.py backend.py db.py r2.py ./
COPY db ./db
COPY static ./static

# Runtime dirs (also declared as volumes in compose)
RUN mkdir -p /app/downloads /app/logs

EXPOSE 8765

# Healthcheck — hits /healthz (public, no auth) so we don't need to bake
# the API key into the image just to verify the process is alive.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,sys; \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8765/healthz', timeout=3).status == 200 else 1)" \
    || exit 1

CMD ["python", "-m", "uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8765"]
