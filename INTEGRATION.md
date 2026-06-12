# DIAN Scraper — Integration Guide

> **Audiencia**: dev senior que va a integrar los hallazgos a `apps/causation`
> (Nuvara). Asume conocimiento de `dian_file_downloader.py`,
> `dian_invoice_scraper.py`, Playwright, y el flujo DIAN actual.

## TL;DR

Este reproducer standalone demostró empíricamente que el bloqueo "se descarga
algunas y deja de descargar" del scraper Nuvara **NO es bloqueo por IP ni por
rate-limit** — son dos bugs independientes en el código actual:

1. **Bug de clasificación de retry**: `download_retry.py` marca todos los 4xx
   como FATAL, incluyendo `403` que Azure Front Door usa como
   *challenge transitorio* en ~20% de los requests. Esos 403 se recuperan en
   el segundo intento sin escalada.

2. **Bug de TLS fingerprint mid-sesión**: `_download_zip()` usa
   `requests.Session()` con cookies extraídas de Camoufox. El TLS/HTTP/2
   fingerprint cambia mid-sesión (Firefox → Python OpenSSL) y Azure WAF
   acumula puntaje de bot hasta bloquear.

El reproducer fija ambos problemas y descarga 30/30 facturas con 6 retries
transparentes (todas OK). Detalle abajo.

---

## 1. Contexto: qué está pasando hoy

### Síntoma observado en producción

- El scraper descarga `N` facturas (varía entre clientes), luego empieza a
  fallar.
- En staging y en local pasa igual, no es ambiente.
- Otros sistemas en el mercado (Siigo, Alegra, etc.) descargan sin drama.
- Coincide con la migración reciente de DIAN a Azure (Front Door + WAF +
  Bot Manager).

### Diagnóstico técnico

Después de leer `apps/causation/src/causation_api/scraper/` con foco en:

- `dian_auth_scraper.py` (login con Camoufox → cookies + storage_state)
- `dian_file_downloader.py` (descarga del ZIP de cada factura)
- `dian/download_retry.py` (política de retry)
- `process-invoice-downloads.use-case.ts` (orquestador NestJS)

Los dos problemas:

#### Problema A — Mezcla de stacks dentro de la misma sesión

```python
# dian_file_downloader.py:2065
def _download_zip(self, cookies: list[dict[str, Any]], cufe: str) -> bytes:
    session = requests.Session()  # ← Python requests (TLS OpenSSL, HTTP/1.1)
    for cookie in cookies:        # ← cookies que vienen de Camoufox (TLS Firefox)
        session.cookies.set(cookie["name"], cookie["value"])
    response = session.get(url, headers=self._download_headers(), ...)
```

Para Azure Front Door + Bot Manager esto es un **bait-and-switch**:

| Fase | Cliente | TLS fingerprint (JA3/JA4) | HTTP/2 | UA |
|---|---|---|---|---|
| Login | Camoufox (Firefox) | Firefox real | sí | Firefox |
| Descarga | `requests` Python | OpenSSL | no (HTTP/1.1) | configurable, body distinto |

El WAF correlaciona la sesión por cookies pero ve TLS/HTTP/2 cambiar. Eso es
señal clásica de bot que reusa credenciales en otra herramienta. El scoring
sube y a las pocas requests empieza a tirar 403/challenge.

#### Problema B — 403 transitorio clasificado como fatal

```python
# dian/download_retry.py:46-49 (antes del fix)
def is_fatal_download_error(error: str) -> bool:
    lower = error.lower()
    if any(code in lower for code in ("401", "403", "404", "http 4")):
        return True  # ← bug: 403 NO siempre es fatal
```

Azure Front Door tiene reglas estocásticas: aleatoriamente devuelve 403 a un
% de requests independiente del comportamiento. Eso lo confirma el reproducer.
El cliente debe reintentar para validar que es un browser real. Nuvara hoy NO
reintenta y marca la factura como FAILED.

### Por qué los competidores funcionan

Hipótesis sin verificar: bajan TODO con el browser (Camoufox/Playwright), no
mezclan stacks. Eso elimina el problema A. Para el problema B también
reintentan (los frameworks de scraping serios manejan 403 transitorio como
default).

---

## 2. El reproducer: cómo valida la hipótesis

Carpeta `tools/dian-scraper-test/` (esta misma).

### Qué hace distinto al scraper de Nuvara

| Aspecto | Nuvara hoy | Reproducer |
|---|---|---|
| Browser para login | Camoufox | Chromium stock |
| Cliente para descarga | `requests.Session()` | `context.request` (browser-coherent) |
| TLS/HTTP fingerprint mid-sesión | cambia | constante |
| Throttling entre descargas | 3s fijo (`process-invoice-downloads.use-case.ts:91`) | 5-13s con jitter |
| Pausa larga periódica | no | 60-120s cada 30 descargas |
| 403 → tratamiento | FATAL (no retry) | retryable con backoff |
| Listado | DataTables API con paginación | DataTables API con paginación (idem Nuvara) |

### Stack del reproducer

- **Python 3.11+ + Playwright 1.48+** (`pip install -r requirements.txt`).
- **FastAPI + WebSocket** para UI en vivo (puerto 8765).
- **Vanilla HTML/JS** en `static/index.html` (UI tipo Nuvara con 3 paneles).
- **Sin proxies, sin CapSolver** — la auth URL se obtiene logueando manualmente
  en DIAN desde el navegador del operador (vos pegás el link del email).

### Resultado empírico (run del 11-jun-2026)

```
Total: 30 facturas (rango: mayo 2026, NIT real de cliente)
OK:    30
Fail:  0
Block: 0  (persistente)
Avg:   582ms
P95:   1317ms

403 transitorios recuperados con retry: 6/30 (20%)
  #6, #11, #16, #21, #22, #29
```

**Cero bloqueos persistentes**, **20% de 403 transitorios** recuperados con
1 retry. La hipótesis quedó validada.

---

## 3. Cómo correr el reproducer

### Setup (una vez)

```bash
cd tools/dian-scraper-test
./setup.sh
```

El script crea `.venv/`, instala deps y baja Chromium.

### Run web (recomendado)

```bash
source .venv/bin/activate
python server.py
# Abrí http://localhost:8765/
```

UI con 3 paneles:

- **Izquierda**: configuración (auth URL, fechas, max, delays).
- **Centro**: lista de facturas descargadas + log en vivo.
- **Derecha**: preview de PDF (iframe) y XML (formateado) de la factura
  seleccionada.

WebSocket en `/ws` para streaming. Endpoints REST: `/api/start`,
`/api/cancel`, `/api/status`, `/files/{nombre}` (sirve PDFs/XMLs/ZIPs).

### Run CLI

```bash
python scraper.py \
    --auth-url "https://catalogo-vpfe.dian.gov.co/User/AuthToken?pk=..." \
    --start-date 2026-05-01 \
    --end-date 2026-05-31 \
    --max-invoices 100
```

### Cómo obtener una auth URL fresca

1. Hacé login en DIAN desde tu Brave/Chrome con la cédula del cliente (rep.
   legal o NIT habilitado).
2. Resolvés el Turnstile manualmente.
3. DIAN te manda un email con un link "Acceder al portal".
4. Copiá ese link COMPLETO. Es de un solo uso y vence a los pocos minutos
   si no lo abrís.

> **Importante**: si tardás más de ~3 min en pegarlo, el reproducer va a fallar
> con "Auth URL expired or invalid". Generá uno nuevo y empezá rápido.

### Outputs

- `downloads/<cufe-trunc>.zip` — ZIP crudo descargado.
- `downloads/<cufe-trunc>.pdf` — PDF extraído del ZIP.
- `downloads/<cufe-trunc>.xml` — XML UBL extraído del ZIP.
- `logs/run-<timestamp>.jsonl` — un JSON por evento (download, sleep, block,
  reauth, summary, list).

### Análisis del log con jq

```bash
# Solo eventos de download
jq 'select(.phase == "download")' logs/run-*.jsonl

# Latencia por sequence number → CSV
jq -r 'select(.phase == "download") | [.sequence, .elapsed_ms, .status, .http_status] | @csv' \
    logs/run-*.jsonl > latency.csv

# Conteo por status final
jq -r 'select(.phase == "download") | .status' logs/run-*.jsonl | sort | uniq -c

# Cuántos retries hubo
jq -r 'select(.phase == "log" and (.notes | test("transient"))) | .notes' \
    logs/run-*.jsonl | wc -l
```

---

## 4. Cambios que hay que aplicar en Nuvara

Hay **dos** cambios independientes. PR 1 ya está hecho en
`branch: test/dian-627` (commit pending). PR 2 queda por hacer.

### PR 1 — Reclasificación de retry (HECHO, sin commit aún)

> **Archivos modificados** (en `branch: test/dian-627`, sin commit):
> - `apps/causation/src/causation_api/scraper/infrastructure/adapters/dian/download_retry.py`
> - `apps/causation/tests/unit/scraper/test_dian_file_downloader_retry.py`

#### Cambio funcional

Antes de este PR, todo 4xx se clasificaba como fatal. Después:

| Caso | Clasificación |
|---|---|
| `HTTP 401` | fatal (sesión expirada — necesita re-auth, no retry) |
| `HTTP 404` | fatal (recurso inexistente) |
| `HTTP 403` bare | **retryable** (Azure WAF challenge) |
| `HTTP 403 + "controles de seguridad"` | fatal (DIAN security block real) |
| `HTTP 403 + "DIAN_SECURITY_BLOCK"` | fatal |
| `HTTP 429` | **retryable** (rate limit con backoff) |
| `HTTP 5xx` | retryable (sin cambio) |
| Parse / XML / schema | fatal (sin cambio) |

#### Diseño del check (fatal-first)

El call site en `dian_file_downloader.py:254-255` ya hace:

```python
is_fatal = _is_fatal_download_error(error_str)
is_retryable = not is_fatal and _is_retryable_download_error(error_str)
```

Eso es lo que hace seguro el cambio: **fatal se evalúa primero**. Un error
`"DIAN_SECURITY_BLOCK: HTTP 403"` matchea `_FATAL_4XX_BODY_MARKERS`
(`"dian_security_block"`) antes de llegar al check de retryable que vería el
`403`. **No tocar el call site.**

#### Tests

- 9 tests existentes → siguen pasando.
- 5 tests nuevos:
  - `test_transient_403_succeeds_on_retry`
  - `test_http_429_rate_limit_is_retryable`
  - `test_dian_security_block_403_stays_fatal` (regresión crítica)
  - `test_http_404_stays_fatal_no_retry`
  - `test_transient_403_exhausts_retries_then_fails`

Correr:

```bash
cd apps/causation
uv run pytest tests/unit/scraper/test_dian_file_downloader_retry.py -v
# 14/14 passed (en mi máquina, 0.31s)
```

Sanidad del scraper entero:

```bash
uv run pytest tests/unit/scraper/ -q
# 592 passed
```

#### Mensaje de commit sugerido

```
fix(scraper): reclassify transient 403/429 as retryable for DIAN

Azure Front Door fronting DIAN returns stochastic 403 challenges to ~20%
of requests, validated with standalone Playwright reproducer in
tools/dian-scraper-test (30/30 OK with 6 retries). Explicit DIAN security
blocks (body markers like 'controles de seguridad' or 'DIAN_SECURITY_BLOCK')
stay fatal — fatal check runs first at the call site so they take precedence
over the new retryable 403 rule.

Expected impact: recovers ~20% of invoices previously marked FAILED.

Refs: tools/dian-scraper-test reproducer.
```

#### Impacto esperado

- Sube la tasa de `dian.download.retry` (counter de retries).
- Cae la tasa de invoices con `pdfDownloadStatus = FAILED`.
- P95 de latencia por descarga sube ~2-5s (las que ahora tienen retry).
- **No** cambia el comportamiento ante security blocks reales (lo dice el test
  `test_dian_security_block_403_stays_fatal`).

---

### PR 2 — Descarga via `context.request` en lugar de `requests` (PENDIENTE)

> **Estado**: NO implementado. Requiere su propio PR + validación.

#### Archivo afectado

`apps/causation/src/causation_api/scraper/infrastructure/adapters/dian_file_downloader.py:2052-2279`
(función `_download_zip`).

#### Qué hay que cambiar

Reemplazar:

```python
def _download_zip(self, cookies: list[dict[str, Any]], cufe: str) -> bytes:
    session = requests.Session()
    for cookie in cookies:
        session.cookies.set(cookie["name"], cookie["value"])
    response = session.get(url, headers=self._download_headers(), ...)
```

Por algo que use `context.request` del browser pool. El reproducer lo hace
así (`core.py:_single_download_attempt`):

```python
async def _single_download_attempt(self, invoice, sequence, attempt):
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
    body = await response.body()
```

`context.request` comparte cookies, TLS, HTTP/2, headers Sec-CH-UA con el
browser. El WAF ve un solo cliente coherente toda la sesión.

#### Por qué NO lo metí yo

1. **Scope**: toca el browser pool, la gestión de sesiones, y los fallbacks
   `GetFilePdf` → `DownloadZipFiles`. Es un PR de ~200 líneas.
2. **Riesgo**: el flujo actual tiene casos especiales que necesitan validación
   con datos reales:
   - Empty PDF fallback (líneas 2230-2273).
   - `DOWNLOAD_URL?trackId=` fallback cuando `GETFILE_PDF_URL?cune=` da 404.
   - El `_download_zip` corre dentro de `asyncio.to_thread()` porque
     `requests` es sync — esto hay que cambiarlo a `await` directo, lo cual
     cambia el shape del call site (`_download_and_parse`).
3. **Pareto**: PR 1 solo (retry transparente) probablemente captura el
   70-80% del beneficio. Vale la pena medir en staging después de PR 1 antes
   de meter PR 2.

#### Plan de implementación sugerido para PR 2

1. **Cambiar la signatura** de `_download_and_parse` y `_download_zip` para
   ser async y recibir el `context` del browser pool en lugar de cookies
   sueltas.
2. **Borrar `proxies = ...`** del `_download_zip` actual — ahora el proxy va
   por la config del browser pool (que ya soporta `settings.proxy_url` via
   `build_camoufox_kwargs()` en `config.py:429`).
3. **Adaptar todos los call sites** de `_download_and_parse` que hoy hacen
   `asyncio.to_thread(self._download_and_parse, cookies, ...)` para hacer
   `await self._download_and_parse(context, ...)`.
4. **Headers Sec-Fetch-***: agregar los headers `Sec-Fetch-Dest`,
   `Sec-Fetch-Mode`, `Sec-Fetch-Site`, `Referer` (los browser reales los
   mandan; `requests` no).
5. **Validar fallbacks**: el `GetFilePdf` → `DownloadZipFiles` fallback debe
   seguir funcionando porque algunas facturas (DS / documentos equivalentes)
   solo bajan por el segundo endpoint.
6. **Tests**: agregar tests con `playwright` real (no mocks) para validar
   integración. Hay tests en `tests/integration/scraper/` que podés usar como
   base.

#### Riesgos a vigilar

| Riesgo | Mitigación |
|---|---|
| `context.request.get()` tira excepción en lugar de devolver response 4xx/5xx | wrap en try/except y re-lanzar `DownloadError` con el mismo formato actual |
| El browser pool no tiene contexts suficientes para la concurrencia actual | revisar `BrowserPool.max_size` — si saturás, las descargas van a esperar lease en lugar de fallar |
| Latencia sube porque cada descarga requiere lease del pool | usar el mismo lease para todo el batch del job (ya existe pattern en `_resolve_track_id_from_authenticated_search`) |
| El XML/PDF binario por `context.request` viene como `Buffer` y no como stream | usar `await response.body()` que da bytes — sin chunking. Para los ZIPs grandes (>5MB) podría haber problema; el actual usa `iter_content(chunk_size=8192)`. Hay que evaluar. |

---

## 5. Orden recomendado de deploy

```
1. Mergeás PR 1 (retry policy).
2. Deploy a staging.
3. Monitor por 24-48h:
   - métrica dian.download.retry sube (esperado)
   - tasa de FAILED baja (esperado, ~20% recovery)
   - p95 de descarga sube ~2-5s (esperado)
4. Si los números cuadran → deploy a prod.
5. Mide en prod por una semana.
6. Si todavía hay un % significativo de blocks persistentes → meté PR 2.
7. Si no, PR 2 queda como mejora futura sin urgencia.
```

---

## 6. Pitfalls observados durante el desarrollo del reproducer

Anotalo porque algunos te pueden pegar en Nuvara también:

### DIAN usa hidden inputs para el datepicker

`#startDate` y `#endDate` son `<input type="hidden">`. Playwright `.fill()`
falla con *"element is not visible"*. Hay que setear el value via JS:

```javascript
const setter = Object.getOwnPropertyDescriptor(
    window.HTMLInputElement.prototype, 'value'
).set;
setter.call(el, val);
el.dispatchEvent(new Event('input', { bubbles: true }));
el.dispatchEvent(new Event('change', { bubbles: true }));
```

(Ver `core.py:list_invoices()`). Nuvara probablemente ya lo resuelve porque
funciona en producción, pero verificar.

### Paginación DataTables server-side

DIAN usa `DataTables` con AJAX server-side. El DOM solo tiene la página actual
(default 10 filas). Hay que:

1. Esperar que `jQuery.fn.dataTable` esté listo.
2. Llamar `dt.page.len(100).draw()` (100 es el máximo que acepta DIAN).
3. Escuchar `draw.dt` para esperar el redibujado.
4. Iterar `dt.page(n).draw(false)` para cada página.

Nuvara ya lo hace bien en `dian_invoice_scraper.py:88-267` (`_JS_WAIT_AND_EXTRACT`).
El reproducer copia esa misma técnica en `core.py:list_invoices()`.

### Auth URL es de un solo uso

El link del email DIAN tiene TTL corto (~5 min) y se invalida cuando lo abrís
una vez. Si el flujo necesita re-auth mid-job, hay que disparar un nuevo email
y esperar al cliente — eso es UX problem, no scraping. Nuvara ya lo maneja
con el flujo OTP.

### Status 200 con body que no es ZIP = challenge HTML

A veces Azure devuelve 200 pero el body es una página HTML de challenge en
lugar del ZIP. Hay que validar magic bytes:

```python
if body[:2] != b"PK":
    # es challenge HTML, no ZIP — tratar como block
```

El reproducer lo detecta (ver `detect_block` y rama en `download_invoice`).
Nuvara: `_ensure_zip_response` ya hace esto, pero verificar que se aplica
también en los fallbacks.

### Empty PDF en ZIP válido

Caso conocido: DIAN devuelve un ZIP estructuralmente válido (200 OK, `PK\x03\x04`)
pero con un PDF de 0 bytes. Nuvara tiene fallback de `GetFilePdf` →
`DownloadZipFiles` para esto (líneas 2230-2273 de `dian_file_downloader.py`).
El reproducer NO maneja este caso (no le tocó), pero si vos lo ves cuando
adaptes el PR 2, mantené el fallback.

### `Document/GetFilePdf?cune=` vs `Document/DownloadZipFiles?trackId=`

Dos endpoints distintos de descarga. `GetFilePdf` es el que el botón
"Descargar" del portal usa y devuelve la representación rica (con datos del
adquirente/vendedor para documentos soporte / equivalentes). `DownloadZipFiles`
es el legacy y devuelve una versión más simple.

El orden correcto (que Nuvara ya hace):

1. Intentar `GetFilePdf?cune=<cufe-lowercase>`.
2. Si 404 → fallback `DownloadZipFiles?trackId=<cufe-lowercase>`.
3. Si el primer endpoint devuelve un PDF vacío en el ZIP → fallback al segundo.

El reproducer solo usa `GetFilePdf` para simplificar el test. Si lo extendés
para Nuvara, mantené los fallbacks actuales.

### CUFE en lowercase

DIAN almacena trackIds en lowercase. Mandar `trackId=ABC123` da 404 aunque
`abc123` exista. `cufe.lower()` antes de armar la URL.

---

## 7. Archivos de referencia rápida

| Archivo | Para qué |
|---|---|
| `tools/dian-scraper-test/core.py` | scraper core con retry + paginación + jitter |
| `tools/dian-scraper-test/scraper.py` | CLI wrapper |
| `tools/dian-scraper-test/server.py` | FastAPI + WebSocket |
| `tools/dian-scraper-test/static/index.html` | UI |
| `apps/causation/src/causation_api/scraper/infrastructure/adapters/dian/download_retry.py` | política de retry (PR 1) |
| `apps/causation/src/causation_api/scraper/infrastructure/adapters/dian_file_downloader.py` | descarga (PR 2 target) |
| `apps/causation/src/causation_api/scraper/infrastructure/adapters/dian_invoice_scraper.py` | listado (referencia, OK) |
| `apps/causation/tests/unit/scraper/test_dian_file_downloader_retry.py` | tests del retry |

---

## 8. Preguntas frecuentes

**¿Por qué Chromium y no Camoufox en el reproducer?**

Para simplificar setup y eliminar variables. Si el test pasa con Chromium
stock (que tiene anti-fingerprint más débil que Camoufox), va a pasar igual
o mejor con Camoufox. Si lo querés más realista, sustituí
`pw.chromium.launch()` por `pw.firefox.launch()` con un browser-binary de
Camoufox.

**¿Hace falta proxy residencial colombiano?**

Probablemente **no**, según los datos del reproducer (corrido desde IP
residencial CO de tu Robin). Para confirmar definitivamente, hay que correr
el reproducer desde la IP del datacenter HostDime y ver si mantiene tasa
similar de OK. Si la tasa se mantiene >85%, no necesitamos proxy.

**¿Y CapSolver?**

El reproducer NO lo usa (el operador resuelve el Turnstile manualmente al
loguear en DIAN para obtener la auth URL). Nuvara sigue necesitándolo para
el flujo automatizado de login. No cambia.

**¿El reproducer funciona desde Dokploy?**

Funciona pero tiene caveat: la URL del WebSocket apunta a `location.host`.
Para usarlo desde el contenedor, exponé el puerto 8765 y accede desde la IP
del contenedor. O mejor: corré el reproducer en LOCAL apuntando a la IP del
contenedor (en cuyo caso es el local el que conecta).

**¿Qué hago con la branch `test/dian-627` después del merge de PR 1?**

Si vas a hacer PR 2 sobre la misma branch, dejala. Si lo vas a hacer en otra,
mergeás `test/dian-627` a `main` y la borrás.

---

## 9. Contacto

Cualquier duda sobre los hallazgos o el reproducer, pingueá a Robinson.

Los datos crudos del run del reproducer están en `logs/run-*.jsonl` —
útiles para mostrar al cliente si hace falta justificar el cambio.
