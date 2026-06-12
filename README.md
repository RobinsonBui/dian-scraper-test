# DIAN Scraper Test

Mini app standalone para validar el patrón de scraping "humano" contra el
portal DIAN antes de tocar el código de Nuvara.

Dos modos de uso:

1. **Web UI (recomendado)** — tipo Nuvara, con paneles de configuración,
   lista de facturas descargadas, log en vivo y preview de PDF/XML.
2. **CLI** — para correr automatizado en terminal con output rich.

## Hipótesis que valida

El bloqueo de DIAN/Azure WAF se produce porque el scraper actual:

1. Hace login con Camoufox (browser real, TLS Firefox).
2. Descarga con `requests` de Python (TLS OpenSSL, HTTP/1.1).
3. El WAF detecta el cambio de fingerprint en la misma sesión y bloquea.

Esta app prueba lo opuesto: TODO el flujo (login + listado + descarga) corre
dentro del mismo browser Playwright (`context.request`), sin saltar a
`requests`. Si las descargas bajan completas sin bloqueo, la hipótesis queda
confirmada y sabemos qué cambiar en Nuvara.

## Setup (una sola vez)

```bash
cd /tmp/opencode/dian-scraper-test
./setup.sh
```

El script:

- Crea un venv local (usa `uv` si está disponible, si no `python -m venv`).
- Instala las deps de `requirements.txt`.
- Descarga Chromium para Playwright.

## Modo Web UI (recomendado)

```bash
source .venv/bin/activate
python server.py
# Abrí http://localhost:8765/ en tu Brave
```

Lo que vas a ver:

```
┌──────────────────┬──────────────────────────┬──────────────────────────┐
│ Configuración    │ Facturas descargadas     │ Vista previa             │
│                  │                          │                          │
│ Auth URL         │ ✓ FE12345  HTTP 200 1.2s │ [Tabs: PDF / XML / JSON] │
│ Start date       │ ✓ FE12346  HTTP 200 950ms│                          │
│ End date         │ ✗ FE12347  HTTP 403      │ <iframe del PDF>         │
│ Max invoices     │ 🚫 FE12348 BLOCK 290ms   │                          │
│ Delay min/max    │                          │                          │
│ Long pause       │                          │                          │
│                  │                          │                          │
│ [Iniciar]        │                          │                          │
│                  ├──────────────────────────┤                          │
│                  │ Log en vivo              │                          │
│                  │ 14:23:01 [list] found 47 │                          │
│                  │ 14:23:02 [download] #1 ✓ │                          │
└──────────────────┴──────────────────────────┴──────────────────────────┘
```

**Flujo de uso:**

1. Hacé login en DIAN desde tu Brave con la cédula del cliente.
2. Cuando llegue el email, copiá la auth URL.
3. Pegala en el panel izquierdo, ajustá fechas y dale **Iniciar**.
4. Vas viendo en vivo cada descarga; clickeá una factura para ver su
   PDF + XML en el panel derecho.

## Modo CLI

```bash
source .venv/bin/activate
python scraper.py \
    --auth-url "https://catalogo-vpfe.dian.gov.co/User/AuthToken?pk=..." \
    --start-date 2026-05-01 \
    --end-date 2026-05-31 \
    --max-invoices 30
```

## Outputs

- `downloads/` — ZIPs, PDFs y XMLs descargados (uno por factura).
- `logs/run-<timestamp>.jsonl` — log estructurado, una línea JSON por evento.
- `logs/run-<timestamp>-summary.txt` — resumen final (sólo en modo CLI).

## Patrón humano implementado

| Comportamiento | Implementación |
|---|---|
| Browser real | Playwright Chromium con headers de usuario real |
| Misma sesión para todo | UN context, UNA page, UNA cookie store |
| Throttling con jitter | 5-13s aleatorio entre descargas (configurable) |
| Pausa larga periódica | 60-120s cada 30 descargas (configurable) |
| Click en links (no fetch directo) | Navega via UI cuando es posible |
| Detección proactiva de bloqueo | `cf-mitigated`, `x-azure-ref`, redirect a login, 429/403 |
| Same fingerprint end-to-end | `context.request` reusa TLS/HTTP del browser |

## Cómo interpretar resultados

| Resultado | Lectura |
|---|---|
| Descarga las N sin bloqueo | **Hipótesis confirmada**: el switch a `requests` en Nuvara es el problema. Cambiar Nuvara para usar `context.request` resuelve el caso. |
| Bloqueo entre #30 y #50 | El TLS fingerprint ayuda pero no es suficiente. Hay que sumar throttling más agresivo. |
| Bloqueo en las primeras 10 | DIAN bloquea por **IP de origen** independiente del fingerprint. Ahí sí necesitamos proxy residencial CO. |
| Auth URL falla al abrir | Sesión muerta antes de empezar. Necesitas login fresco. |

## Caveats que tenés que saber

1. **Selectores del DOM**: hice las queries con selectores genéricos. Si el
   portal cambió, el `list_invoices()` puede no encontrar nada. Si pasa eso,
   decime y ajustamos cuando veamos el HTML real.

2. **Chromium ≠ Camoufox**: Camoufox tiene un anti-fingerprint más agresivo
   que Chromium stock. Si el test pasa con Chromium genérico, va a pasar
   **mejor** con Camoufox. Si falla con Chromium, vale probar con Camoufox
   antes de descartar la hipótesis.

3. **No usa proxies**: intencional. Probamos si el problema es comportamiento.
   Si querés probar con tu IP residencial colombiana, corré la app desde tu
   Robin (no desde Dokploy o el VPS).

## Análisis post-corrida

El log JSONL es fácil de analizar con `jq`:

```bash
# Solo bloqueos
jq 'select(.status == "block")' logs/run-*.jsonl

# Latencia por descarga, formato CSV
jq -r 'select(.phase == "download") | [.sequence, .elapsed_ms, .status] | @csv' \
    logs/run-*.jsonl > latency.csv

# Conteo por status
jq -r 'select(.phase == "download") | .status' logs/run-*.jsonl | sort | uniq -c
```
