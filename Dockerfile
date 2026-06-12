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

# App code
COPY core.py scraper.py server.py ./
COPY static ./static

# Runtime dirs (also declared as volumes in compose)
RUN mkdir -p /app/downloads /app/logs

EXPOSE 8765

# Healthcheck — hits the index page; if FastAPI is up it'll return 200
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,sys; \
import urllib.error; \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8765/', timeout=3).status == 200 else 1)" \
    || exit 1

CMD ["python", "-m", "uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8765"]
