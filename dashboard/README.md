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

- An inline script in `<head>` sets `data-theme` before first paint, so a
  light-mode visitor never sees a flash of the dark palette.
- Theme switching does **not** cross-fade. Transitioning foreground and
  background together makes them interpolate through mid-grey, collapsing text
  contrast to ~1.1:1 mid-switch. The swap is instant and therefore always
  legible.

On theme change, charts are re-rendered from the last good payload held in the
`CACHE` object rather than refetched, so a slow or failing request can't leave
some charts painted in the previous theme.

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

The frontend polls every 3 minutes (`setInterval(refresh, 180000)`). Data is
only ingested every 15 minutes, so polling faster asks for changes that cannot
exist. Time-derived UI (the clock, snapshot age) updates locally via
`tickLocal()` without any network call.

`_PAYLOAD_TTL` (300s) is the main freshness/cost knob: it bounds how stale a
served payload may be.

## Local dev

```bash
uv sync && uv run uvicorn src.dashboard.app:app --reload --port 8000
```
