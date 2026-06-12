# DIAN Scraper — M2M Integration Contract

> Audience: NUVARA `apps/causation` integration. This document is the
> source of truth for the HTTP contract between NUVARA and the scraper
> when the scraper runs as a remote, authenticated downloader.

## Deployment

Image: `ghcr.io/robinsonbui/dian-scraper-test:<TAG>` (Dokploy raw compose).

The next deploy tag should be **v1.61.0** to signal the breaking shape
change (job-based results + mandatory auth in M2M mode).

### Environment variables

| Name              | Required | Default | Description                                                                                                    |
| ----------------- | -------- | ------- | -------------------------------------------------------------------------------------------------------------- |
| `SCRAPER_API_KEY` | yes\*    | empty   | Shared secret. Every `/api/*` and `/files/*` request must carry it via `X-API-Key`. \*Empty = legacy open mode. |
| `TZ`              | no       | UTC     | Set to `America/Bogota` so the timestamps in logs match the operator's perception.                             |
| `PROXY_URL`       | no       | empty   | Optional outbound proxy for browser + API traffic when the scraper VM is outside Colombia.                     |

Generate the API key once on the host that controls the deploy:

```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
# or
openssl rand -hex 48
```

Set the same value as `DIAN_SCRAPER_ALT_API_KEY` in NUVARA `apps/api`.

## Auth model

- When `SCRAPER_API_KEY` is set on the server, every request to `/api/*`
  and `/files/*` must include the header `X-API-Key: <secret>`. Failed
  auth returns `401` with `{"error": "unauthorized", "message": "..."}`.
- The WebSocket `/ws` accepts the key via `X-API-Key` header (server
  clients) or `?api_key=` query param (browsers, which can't set custom
  WS headers). Auth failure closes the handshake with `1008`.
- The following paths are **public on purpose** (no key required):
  - `GET /` — the HTML UI (still gated by network reachability).
  - `GET /healthz` — liveness probe for Dokploy / load balancer.
  - `GET /openapi.json`, `/docs`, `/redoc` — FastAPI's own docs.
  - `GET /static/*` — UI assets.

## M2M endpoints

### `POST /api/jobs`

Creates a new scraping job. Returns the `job_id` the caller must poll.

**Headers:** `X-API-Key: <secret>` (if auth is enabled).

**Body:**

```json
{
  "auth_url": "<URL given by the DIAN portal after cert login>",
  "start_date": "2026-01-01",
  "end_date": "2026-01-31",
  "max_invoices": 30,
  "headless": true,
  "delay_min_ms": 5000,
  "delay_max_ms": 13000,
  "long_pause_every": 30
}
```

**Response (201):**

```json
{
  "ok": true,
  "job_id": "a1b2c3d4e5f6...",
  "status": "queued",
  "poll_url": "/api/jobs/a1b2c3d4e5f6..."
}
```

**Errors:**

- `401` — missing/invalid `X-API-Key`.
- `409` — a job is already running. The scraper is **single-tenant**:
  one concurrent run at a time. Callers must serialize.

### `GET /api/jobs/{job_id}`

Returns the complete state of a job. Safe to poll — read-only and
idempotent.

**Headers:** `X-API-Key: <secret>` (if auth is enabled).

**Response (200):**

```json
{
  "job_id": "a1b2c3d4e5f6...",
  "status": "running",
  "created_at": "2026-06-12T16:30:00Z",
  "started_at": "2026-06-12T16:30:01Z",
  "finished_at": null,
  "log_file": "run-20260612T163001Z-a1b2c3d4.jsonl",
  "summary": null,
  "error": null,
  "files": [
    {
      "name": "abcdef1234.zip",
      "kind": "zip",
      "size_bytes": 18234,
      "cufe": "abcdef1234567890abcd...",
      "prefijo_folio": "SETP990000001",
      "issuer_nit": "900123456",
      "issue_date": "2026-01-15",
      "url": "/files/a1b2c3d4e5f6.../abcdef1234.zip"
    }
  ],
  "events_count": 42
}
```

**Status values:**

- `queued` — job created, scraper not started yet.
- `running` — scraper engine active.
- `completed` — engine finished without raising.
- `failed` — engine raised; `error` is populated.
- `cancelled` — `/api/cancel` was called.

**Polling strategy (NUVARA):**

```text
1. POST /api/jobs → get job_id
2. Loop with 5s interval:
     GET /api/jobs/{job_id}
     if status in {completed, failed, cancelled}: break
3. for f in response.files:
     GET /files/{job_id}/{f.name}
     stream binary into R2 with the matching cufe metadata
```

Cap the loop at e.g. 30 minutes; the scraper takes ~1s + `delay_min..max`
per invoice plus a longer pause every `long_pause_every`.

### `GET /files/{job_id}/{name}`

Downloads a single file produced by the given job. Returns the binary
with the appropriate `Content-Type` (`application/pdf`, `application/xml`,
`application/zip`).

**Headers:** `X-API-Key: <secret>` (if auth is enabled).

**Errors:**

- `400` — filename or job_id contains `..` or `/`.
- `404` — job unknown or file not produced.

### `POST /api/cancel`

Cancels the currently running job (whatever its id). Idempotent.

### `GET /api/status`

Legacy global status (still used by the UI). Now also includes
`current_job_id` so an M2M caller that already wrote down the job_id can
ignore it.

### `GET /healthz` (public)

```json
{ "status": "ok", "auth_required": true }
```

## File layout on disk

```
downloads/
├── <job_id_1>/
│   ├── <cufe[:20]>.zip   ← raw DIAN download
│   ├── <cufe[:20]>.pdf   ← extracted from the zip
│   └── <cufe[:20]>.xml   ← extracted from the zip
├── <job_id_2>/...
```

A job's files live under its own subdirectory so concurrent jobs (if we
ever lift the single-tenant lock) never collide.

The legacy `/files/{name}` endpoint resolves against the **current**
job's subdir first, then the flat root for historical compatibility.

## Backwards compatibility

- `/api/start` is preserved as an alias for `/api/jobs`. It still returns
  `{"ok": true, "message": "Run started.", "job_id": "..."}` so the UI
  HTML keeps working unchanged.
- `/files/{name}` (no job_id) is preserved for the UI's preview iframe.

Both are deprecated for M2M — new integrations must use the explicit
job-based endpoints so file isolation works.

## Failure modes

| Symptom                                | Likely cause                              | Fix                                                              |
| -------------------------------------- | ----------------------------------------- | ---------------------------------------------------------------- |
| `401 unauthorized`                     | Missing or wrong `X-API-Key`              | Set the matching secret on both sides.                           |
| `409 a run is already in progress`     | Another caller is currently scraping      | Serialize or wait for `/api/status.is_running == false`.         |
| `404 job not found` on `/api/jobs/{id}` | Container restarted (in-memory state lost) | Persist jobs in Redis if we ever need durability across restarts. |
| Empty `files: []` after `status=completed` | DIAN portal returned no invoices in range | Check `summary.total` in the response — engine succeeded, just nothing to download. |
| `summary` carries `total > 0` but `ok < total` | Some downloads got 403 or 5xx          | Check the JSONL log file referenced in `log_file` for details.   |
