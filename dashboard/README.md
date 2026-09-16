# Dashboard

Live traffic + forecasting dashboard for Aeroflow.

Reads data from S3 (raw flight states, hourly features, forecasts) and
serves a public web dashboard via FastAPI (deployed on Modal).

## Endpoints

- `GET /` — dashboard page
- `GET /api/live` — current aircraft, today vs yesterday curves
- `GET /api/patterns` — hour-of-day profile, weekday pattern, anomalies
- `GET /api/forecasts` — latest outlook, actual-vs-predicted, MAPE
- `GET /api/health` — data freshness, coverage gaps

## Theming

The dashboard ships a dark (default) and a light theme. A switch in the header
flips between them; the choice is saved to `localStorage` and otherwise follows
the visitor's OS `prefers-color-scheme` setting.

`style.css` is the single source of truth for colour: every colour is a custom
property on `:root` / `[data-theme="light"]`, and `app.js` reads those same
variables via `getComputedStyle`. That keeps the charts in step with the UI
instead of carrying their own hardcoded palette.

Two deliberate details, both worth preserving:

- The theme bootstrap lives in `static/theme.js`, loaded as a **blocking**
  `<script src>` in `<head>` (not `defer`), so it sets `data-theme` before first
  paint and a light-mode visitor never sees a flash of the dark palette. It is a
  separate file rather than an inline block so the CSP can be `script-src 'self'`
  with no `'unsafe-inline'`.
- Theme switching does **not** cross-fade. Transitioning foreground and
  background together makes them interpolate through mid-grey, collapsing text
  contrast to ~1.1:1 mid-switch. The swap is instant and therefore always
  legible.

On theme change, charts are re-rendered from the last good payload held in the
`CACHE` object rather than refetched, so a slow or failing request can't leave
some charts painted in the previous theme.

## Anomaly explanations

Days whose mean deviates >20% from the trailing window are flagged, and each is
given a **data-grounded** explanation rather than an external attribution:

- `incomplete ingestion` - fewer than 20 of 24 hours were ingested
- `baseline spans a data change` - a >25% step inside the comparison window
- `sustained level shift` - the new level holds on following days
- `transient deviation` - the level returns to the trailing range

This is deliberate. On 2026-09-07 the feed's capture rate halved overnight
(17 -> 8 aircraft per poll) and stayed there for a week, which made every
subsequent day look like a ~30% traffic shortfall against a baseline straddling
the change. Attributing such a day to weather or a real event would be
confidently wrong, so the panel reports the evidence and lets the reader judge.
External attribution (weather, events) would only be worth adding for deviations
that survive these checks - and would need an LLM/search key plus a cost budget,
which this project does not currently have.

## Security headers

Every response carries a strict CSP plus the usual hardening headers — see
`_SECURITY_HEADERS` in `app.py`: `script-src 'self'` with no `'unsafe-inline'`,
`frame-ancestors 'none'` and `X-Frame-Options: DENY`, `X-Content-Type-Options`,
`Referrer-Policy`, `Permissions-Policy`, HSTS and `Cross-Origin-Opener-Policy`.

Two constraints this imposes on the frontend, both enforced by tests:

- **No inline `<script>`** — a single inline block would force `'unsafe-inline'`
  back into `script-src`, which is why the theme bootstrap is its own file.
- **No inline `style="..."` attributes**, in markup *or* in templates rendered
  through `innerHTML`. CSP's `style-src-attr` blocks those and the failure is
  **silent**: the element still renders, just unstyled. Use classes instead
  (`.donut-center .dc-unit`, `.anomaly .pct.up/.down`). Setting `element.style.x`
  from JS is fine — CSSOM mutation is not covered by CSP.

The only permitted external origin is Google Fonts (`style-src` for the
stylesheet, `font-src` for the font files). If it is unreachable the page falls
back to system fonts.

## Cost model

This service is public and runs on Modal, where a request that rebuilds every
aggregate from S3 costs real money. Three things keep that bounded:

1. **Immutable objects are cached by key** (`_cached_object`). Raw parquet and
   forecast JSON are timestamped and never rewritten, so a warm container reads
   only objects created since its last request instead of re-reading the whole
   archive (~620 objects down to ~0).
2. **Completed days come from feature files, not raw.** One feature file holds a
   day of hourly counts; the raw path needs ~95 files. `hourly_day()` prefers
   features and only falls back to raw for today, which has no features yet.
3. **Aggregates are published to S3** (`dashboard/cache/*.json`, ~21 KiB total)
   and served from there. A container that just started would otherwise have to
   rebuild everything, so every visit paid the full cost. Publishing means total
   rebuild work is bounded by time, not by traffic, and containers can scale to
   zero between visits.

Measured effect on execution time per endpoint: forecasts 97.0s -> 0.65s,
patterns 41.2s -> 0.65s, live 48.4s -> 0.99s, health 3.3s -> 0.65s.

The frontend polls every 3 minutes, but only while the tab is **visible**
(`startPolling`/`stopPolling` wired to the Page Visibility API, with an
immediate catch-up refresh on return). It stops polling when the tab is
backgrounded: Modal bills container runtime and a backgrounded tab's poll pays
a full cold start for data nobody is looking at. Time-derived UI (the clock,
snapshot age) updates locally via `tickLocal()` without any network call.

`_PAYLOAD_TTL` (300s) is the main freshness/cost knob: it bounds how stale a
served payload may be.

## Local dev

```bash
uv sync && uv run uvicorn src.dashboard.app:app --reload --port 8000
```
