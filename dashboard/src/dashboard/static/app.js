/* Aeroflow dashboard client — charts, doughnuts, KPIs. */

/* Theme-aware colours, read from the CSS custom properties in style.css so the
   stylesheet stays the single source of truth. Re-read on every refresh so a
   theme switch re-colours the charts instead of leaving stale palette values. */
function readTheme() {
  const cs = getComputedStyle(document.documentElement);
  const v = (name) => cs.getPropertyValue(name).trim();
  return {
    palette: [
      v("--accent"), v("--accent2"), v("--green"), v("--pink"), v("--amber"),
      v("--cyan"), v("--red"), v("--violet"), v("--lime"), v("--orange"),
    ],
    muted: v("--muted"),
    grid: v("--grid"),
    accentRgb: v("--accent-rgb"),
    accent2Rgb: v("--accent2-rgb"),
    greenRgb: v("--green-rgb"),
  };
}
let THEME = readTheme();

function fmtClock(iso) {
  if (!iso) return "–";
  const d = new Date(iso);
  return d.toUTCString().slice(17, 25) + " UTC";
}
function fmtStamp(iso) {
  if (!iso) return "–";
  return iso.replace("T", " ").slice(0, 16);
}

// Keep Chart.js instances separate from DOM globals. Browsers expose an
// element with id="liveChart" as window.liveChart, so using window[id] here
// would find the canvas and call canvas.destroy(), which does not exist.
const charts = Object.create(null);

function makeChart(id, cfg) {
  const el = document.getElementById(id);
  if (!el || typeof Chart === "undefined") return null;
  const ctx = el.getContext("2d");
  // Chart.js keeps its own registry. Use it as the source of truth because
  // overlapping refreshes can outlive an entry in our local map.
  const existing = Chart.getChart(el);
  if (existing) existing.destroy();
  charts[id] = null;
  const c = new Chart(ctx, cfg);
  charts[id] = c;
  return c;
}

function baseScales(yTitle) {
  return {
    x: { ticks: { color: THEME.muted, maxRotation: 0, autoSkip: true, maxTicksLimit: 12 }, grid: { color: THEME.grid } },
    y: { ticks: { color: THEME.muted }, grid: { color: THEME.grid }, title: { display: !!yTitle, text: yTitle, color: THEME.muted } },
  };
}

/* ---------------- KPI + live chart ---------------- */

/* Last successful payload per endpoint. Theme switching re-renders from these
   instead of refetching, so a slow or failing request can never leave some
   charts painted in the previous theme. */
const CACHE = Object.create(null);

async function loadLive() {
  const d = await (await fetch("/api/live")).json();
  CACHE.live = d;
  renderLive(d);
}

function renderLive(d) {
  document.getElementById("live-sub").textContent = `as of ${fmtClock(d.now_utc)}`;
  document.getElementById("clock").textContent = fmtClock(d.now_utc);

  const today = d.today || [];
  const yest = d.yesterday || [];
  const peak = today.reduce((m, r) => Math.max(m, r.count), 0);
  const avgToday = today.length ? today.reduce((s, r) => s + r.count, 0) / today.length : 0;

  countUp("kpi-active", d.active_aircraft_now);
  countUp("today-peak", Math.round(peak));
  countUp("today-avg", Math.round(avgToday));

  // "Aircraft now" is a single OpenSky poll, not an hourly average, so state
  // the snapshot age explicitly instead of implying a running total.
  const cap = document.getElementById("active-caption");
  if (cap) {
    const at = d.active_captured_at ? new Date(d.active_captured_at) : null;
    const ageMin = at ? Math.round((Date.now() - at.getTime()) / 60000) : null;
    cap.innerHTML = (ageMin === null)
      ? "instant snapshot"
      : `in airspace · snapshot ${ageMin <= 0 ? "just now" : ageMin + "m ago"}`;
    if (at) cap.title = `Instantaneous count from the ${fmtClock(d.active_captured_at)} UTC satellite poll`;
  }

  // vs yesterday (compare same elapsed hours)
  const yestMap = Object.fromEntries(yest.map(r => [r.hour, r.count]));
  const pairs = today.filter(r => r.hour in yestMap && yestMap[r.hour] > 0);
  const pct = pairs.length
    ? (today.filter(r => r.hour in yestMap).reduce((s, r) => s + r.count, 0)
       - pairs.reduce((s, r) => s + yestMap[r.hour], 0))
      / pairs.reduce((s, r) => s + yestMap[r.hour], 0) * 100
    : 0;
  const el = document.getElementById("vs-yesterday");
  el.textContent = (pct >= 0 ? "+" : "") + pct.toFixed(1) + "%";
  el.style.color = Math.abs(pct) > 25 ? (pct < 0 ? "var(--red)" : "var(--amber)") : "var(--green)";
  document.getElementById("vs-yesterday-label").textContent =
    Math.abs(pct) > 25 ? (pct < 0 ? "⚠ down" : "⚠ up") : "steady";

  // gradient area: today vs yesterday
  const labels = today.map(r => `${String(r.hour).padStart(2, "0")}:00`);
  const todayVals = today.map(r => r.count);
  const yestVals = today.map(r => yestMap[r.hour] ?? null);

  const g = context => {
    const grad = context.chart.ctx.createLinearGradient(0, 0, 0, 280);
    grad.addColorStop(0, `rgba(${THEME.accentRgb},.35)`);
    grad.addColorStop(1, `rgba(${THEME.accentRgb},0)`);
    return grad;
  };
  makeChart("liveChart", {
    type: "line",
    data: { labels, datasets: [
      { label: "Today", data: todayVals, borderColor: THEME.palette[0], backgroundColor: g, fill: true, tension: .4, pointRadius: 0, borderWidth: 2.5 },
      { label: "Yesterday", data: yestVals, borderColor: THEME.muted, borderDash: [5, 5], fill: false, tension: .4, pointRadius: 0, borderWidth: 1.5 },
    ]},
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { labels: { color: THEME.muted, boxWidth: 10, usePointStyle: true } } },
      scales: baseScales("aircraft / hour") },
  });

  // breakdowns
  renderBreakdown(d.breakdown);
}

function renderBreakdown(b) {
  if (!b) return;
  doughnut("countryChart", "country-total", b.countries.map(c => c.label),
           b.countries.map(c => c.n), "aircraft");
  doughnut("airlineChart", "airline-total", b.airlines.map(c => c.label),
           b.airlines.map(c => c.n), "ac");
  doughnut("altChart", "alt-total", b.altitudes.map(c => c.label),
           b.altitudes.map(c => c.n), "ac");
}

function doughnut(id, centerId, labels, values, unit) {
  const center = document.getElementById(centerId);
  const total = values.reduce((a, b) => a + b, 0);
  if (center) center.innerHTML = `<div>${total}</div><div style="font-size:11px">${unit}</div>`;
  makeChart(id, {
    type: "doughnut",
    data: { labels, datasets: [{ data: values, backgroundColor: THEME.palette.slice(0, labels.length),
      borderWidth: 0, hoverOffset: 6 }] },
    options: { responsive: true, maintainAspectRatio: false, cutout: "68%",
      plugins: { legend: { position: "bottom", labels: { color: THEME.muted, boxWidth: 8, font: { size: 10 } } } } },
  });
}

/* ---------------- patterns ---------------- */

async function loadPatterns() {
  const d = await (await fetch("/api/patterns")).json();
  CACHE.patterns = d;
  renderPatterns(d);
}

function renderPatterns(d) {

  // 7-day overlay
  const overlay = d.overlay || [];
  const labels = Array.from({ length: 24 }, (_, i) => `${String(i).padStart(2, "0")}:00`);
  const datasets = overlay.map((day, i) => {
    const m = Object.fromEntries(day.hours.map(h => [h.hour, h.count]));
    return {
      label: day.date.slice(5) + " " + day.weekday,
      data: labels.map((_, h) => m[h] ?? null),
      borderColor: THEME.palette[i % THEME.palette.length],
      borderWidth: i === overlay.length - 1 ? 3 : 1.5,
      backgroundColor: i === overlay.length - 1 ? `rgba(${THEME.accentRgb},.08)` : "transparent",
      fill: i === overlay.length - 1,
      pointRadius: 0, tension: .35,
    };
  });
  makeChart("weekChart", {
    type: "line",
    data: { labels, datasets },
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { labels: { color: THEME.muted, boxWidth: 10, font: { size: 10 } } } },
      scales: baseScales("aircraft") },
  });

  // day-part doughnut
  const parts = d.day_parts || [];
  doughnut("daypartChart", null, parts.map(p => p.part), parts.map(p => p.total), "");

  // weekday bars (rounded, gradient)
  const wp = d.weekday_profile || [];
  const todayIdx = new Date().getUTCDay(); // 0=Sun
  const wkOrder = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
  const sorted = wkOrder.map((n, i) => {
    const f = wp.find(w => w.weekday === n);
    return { ...(f || { weekday: n, mean_total: 0 }), today: (i === (todayIdx + 6) % 7) };
  }).filter(x => x.mean_total > 0);
  makeChart("weekdayChart", {
    type: "bar",
    data: { labels: sorted.map(s => s.weekday + (s.today ? " •" : "")),
      datasets: [{ data: sorted.map(s => s.mean_total),
        backgroundColor: sorted.map(s => s.today ? THEME.palette[1] : `rgba(${THEME.accentRgb},.55)`),
        borderRadius: 6, borderSkipped: false }] },
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } }, scales: baseScales("flights/day") },
  });

  // hour profile (bar with highlight for current hour)
  const hp = d.hour_profile || [];
  const nowH = new Date().getUTCHours();
  makeChart("hourChart", {
    type: "bar",
    data: { labels: hp.map(r => r.hour),
      datasets: [{ data: hp.map(r => r.mean),
        backgroundColor: hp.map(r => r.hour === nowH ? `rgba(${THEME.greenRgb},.9)` : `rgba(${THEME.accent2Rgb},.45)`),
        borderRadius: 4, borderSkipped: false }] },
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } }, scales: baseScales("avg aircraft") },
  });

  // anomalies
  renderAnomalies(d.anomalies || []);
}

function renderAnomalies(anomalies) {
  const panel = document.getElementById("anomalies-panel");
  if (!anomalies.length) { panel.style.display = "none"; return; }
  panel.style.display = "";
  document.getElementById("anomalies-body").innerHTML = anomalies.map(a => {
    const up = a.deviation_pct > 0;
    return `<div class="anomaly ${up ? "up" : ""}">
      <span class="date">${a.date}</span>
      <span class="detail">avg <b>${a.mean}</b> flights vs trailing-week <b>${a.trail_mean}</b></span>
      <span class="pct" style="color:${up ? "var(--amber)" : "var(--red)"}">
        ${up ? "▲" : "▼"} ${Math.abs(a.deviation_pct).toFixed(1)}%</span>
    </div>`;
  }).join("");
}

/* ---------------- forecasts ---------------- */

async function loadForecasts() {
  const d = await (await fetch("/api/forecasts")).json();
  CACHE.forecasts = d;
  renderForecasts(d);
}

function renderForecasts(d) {

  // h=1 actual vs predicted
  const names = Object.keys(d.h1_history || {});
  const series = {};
  for (const n of names) series[n] = d.h1_history[n].slice(-60);
  const sample = names.length ? series[names[0]] : [];
  // Targets are ISO strings; show "MM-DD HH:00" (the T separator reads oddly)
  const labels = sample.map(p => p.target.slice(5, 10) + " " + p.target.slice(11, 16));
  const datasets = [];
  names.forEach((n, i) => {
    datasets.push({
      label: "pred · " + n.replace("flight-traffic-", ""),
      data: series[n].map(p => p.pred),
      borderColor: THEME.palette[i % THEME.palette.length], tension: .25, pointRadius: 0, borderWidth: 2,
    });
  });
  datasets.push({
    label: "actual", data: sample.map(p => p.actual ?? null),
    borderColor: THEME.palette[6], borderDash: [6, 4],
    pointRadius: 2.5, pointBackgroundColor: THEME.palette[6],
    spanGaps: false, tension: .3,
  });
  if (datasets.length) {
    makeChart("fcChart", {
      type: "line",
      data: { labels, datasets },
      options: { responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { labels: { color: THEME.muted, boxWidth: 10, font: { size: 10 } } },
          tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${c.parsed.y} aircraft` } },
        },
        scales: baseScales("aircraft") },
    });
  }

  // latest outlook: stacked lines for each model over next 6h
  const lm = d.latest_models || {};
  const hours = Object.values(lm)[0]?.hours || [];
  const oLabels = hours;
  const oDatasets = Object.entries(lm).map(([n, m], i) => ({
    label: n.replace("flight-traffic-", ""),
    data: m.series, borderColor: THEME.palette[i % THEME.palette.length],
    tension: .3, pointRadius: 3, borderWidth: 2.5,
    backgroundColor: `rgba(${THEME.accentRgb},.05)`, fill: i === 0,
  }));
  if (oDatasets.length) {
    makeChart("outlookChart", {
      type: "line",
      data: { labels: oLabels, datasets: oDatasets },
      options: { responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { labels: { color: THEME.muted, boxWidth: 10, font: { size: 10 } } },
          tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${c.parsed.y} aircraft` } },
        },
        scales: baseScales("predicted aircraft") },
    });
  }
  const meta = document.getElementById("outlook-meta");
  const versions = Object.entries(lm)
    .map(([n, m]) => `${n.replace("flight-traffic-", "")}${m.version ? " v" + m.version : ""}`)
    .join(", ");
  meta.innerHTML = d.latest_generated
    ? `model <span class="mono">${versions || "–"}</span> · generated <span class="mono">${fmtStamp(d.latest_generated)} UTC</span> · ${d.num_forecasts} forecasts stored`
    : "no forecasts yet";
}

/* ---------------- health ---------------- */

async function loadHealth() {
  const d = await (await fetch("/api/health")).json();
  CACHE.health = d;
  renderHealth(d);
}

function renderHealth(d) {
  const pill = document.getElementById("status-pill");
  const fresh = d.data_freshness_min;
  if (fresh === null) { pill.textContent = "no data today"; pill.className = "status-pill bad"; }
  else if (fresh <= 30) { pill.textContent = "● live · " + fresh + "m"; pill.className = "status-pill ok"; }
  else if (fresh <= 120) { pill.textContent = "stale · " + fresh + "m"; pill.className = "status-pill warn"; }
  else { pill.textContent = "● down? · " + fresh + "m"; pill.className = "status-pill bad"; }

  const cov = (d.coverage || []).slice().reverse();
  document.getElementById("health-body").innerHTML = `
    <div class="hgrid">
      <div class="hitem"><div class="hv">${d.data_freshness_min ?? "–"}m</div><div class="hl">data age</div></div>
      <div class="hitem"><div class="hv">${d.raw_files_today ?? 0}</div><div class="hl">raw files today</div></div>
      <div class="hitem"><div class="hv">${d.feature_days ?? 0}</div><div class="hl">feature days</div></div>
      <div class="hitem"><div class="hv">${d.forecast_count ?? 0}</div><div class="hl">forecasts stored</div></div>
    </div>
    <table class="coverage">
      <tr><th>date</th><th>raw data</th><th>features</th></tr>
      ${cov.map(r => `<tr><td>${r.date}</td>
        <td>${r.raw ? '<span class="dot yes"></span>' : '<span class="dot no"></span>'}</td>
        <td>${r.features ? '<span class="dot yes"></span>' : '<span class="dot no"></span>'}</td></tr>`).join("")}
    </table>`;
}

/* ---------------- helpers ---------------- */

function countUp(id, target) {
  const el = document.getElementById(id);
  const start = parseInt(el.textContent) || 0;
  if (target === start) return;
  const dur = 600, t0 = performance.now();
  function tick(t) {
    const p = Math.min((t - t0) / dur, 1);
    el.textContent = Math.round(start + (target - start) * (1 - Math.pow(1 - p, 3)));
    if (p < 1) requestAnimationFrame(tick);
  }
  requestAnimationFrame(tick);
}

/* ---------------- theme ---------------- */

const THEME_KEY = "aeroflow-theme";

function savedTheme() {
  try {
    const t = localStorage.getItem(THEME_KEY);
    return (t === "light" || t === "dark") ? t : null;
  } catch (e) {
    return null;  // storage blocked (private mode / embedded webview)
  }
}

/* The inline <head> script sets data-theme before first paint; this syncs the
   button to it and wires up switching. */
function applyTheme(name) {
  document.documentElement.setAttribute("data-theme", name);
  THEME = readTheme();
  const btn = document.getElementById("theme-toggle");
  if (btn) {
    // Icon shows the mode the button switches TO, not the current one
    btn.textContent = name === "light" ? "☾" : "☀";
    btn.setAttribute(
      "aria-label",
      name === "light" ? "Switch to dark theme" : "Switch to light theme",
    );
    btn.title = btn.getAttribute("aria-label");
  }
}

/* Re-render every chart from the last good payload, using the current theme.
   No network: a theme switch must always be visually complete. */
function rerenderFromCache() {
  if (CACHE.live) renderLive(CACHE.live);
  if (CACHE.patterns) renderPatterns(CACHE.patterns);
  if (CACHE.forecasts) renderForecasts(CACHE.forecasts);
  if (CACHE.health) renderHealth(CACHE.health);
}

function initTheme() {
  applyTheme(document.documentElement.getAttribute("data-theme") || "dark");

  const btn = document.getElementById("theme-toggle");
  if (btn) {
    btn.addEventListener("click", () => {
      const next =
        document.documentElement.getAttribute("data-theme") === "light" ? "dark" : "light";
      try { localStorage.setItem(THEME_KEY, next); } catch (e) { /* non-fatal */ }
      applyTheme(next);
      rerenderFromCache();  // instant, theme-correct repaint
      refresh();            // then pull fresh data
    });
  }

  // Track OS changes only while the visitor hasn't made an explicit choice
  const mq = window.matchMedia("(prefers-color-scheme: light)");
  const onSysChange = (e) => {
    if (savedTheme()) return;
    applyTheme(e.matches ? "light" : "dark");
    rerenderFromCache();
    refresh();
  };
  if (mq.addEventListener) mq.addEventListener("change", onSysChange);
  else if (mq.addListener) mq.addListener(onSysChange);  // older Safari
}

/* ---------------- loop ---------------- */

// Chart.js is self-hosted, but wait for the global to be defined before the
// first paint so the initial refresh can't race the script and blank the page.
function whenChartLib(timeoutMs = 15000) {
  return new Promise((resolve) => {
    if (typeof Chart !== "undefined") return resolve(true);
    const t0 = Date.now();
    const iv = setInterval(() => {
      if (typeof Chart !== "undefined") { clearInterval(iv); resolve(true); }
      else if (Date.now() - t0 > timeoutMs) { clearInterval(iv); resolve(false); }
    }, 150);
  });
}

let refreshInFlight = false;

async function refresh() {
  if (refreshInFlight) return;
  refreshInFlight = true;
  try {
    await whenChartLib();
    THEME = readTheme();
    // run each loader independently so a single failure doesn't blank all
    const results = await Promise.allSettled([
      loadLive(), loadPatterns(), loadForecasts(), loadHealth(),
    ]);
    for (const r of results) if (r.status === "rejected") console.error(r.reason);
    document.getElementById("foot-updated").textContent =
      "updated " + fmtClock(new Date().toISOString());
  } catch (e) {
    console.error(e);
    const pill = document.getElementById("status-pill");
    pill.textContent = "load error"; pill.className = "status-pill bad";
  } finally {
    refreshInFlight = false;
  }
}

initTheme();
refresh();
setInterval(refresh, 60000);
