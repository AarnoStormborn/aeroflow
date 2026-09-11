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

## Local dev

```bash
uv sync && uv run uvicorn src.dashboard.app:app --reload --port 8000
```
