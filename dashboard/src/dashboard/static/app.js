/* Aeroflow dashboard client: fetches JSON API and renders charts. */

const COLORS = {
  blue: "#58a6ff", green: "#3fb950", red: "#f85149",
  amber: "#d29922", purple: "#bc8cff", gray: "#8b949e",
};

function fmtTime(iso) {
  if (!iso) return "–";
  return iso.replace("T", " ").slice(0, 16);
}

/* ---------------- live strip ---------------- */

async function loadLive() {
  const d = await (await fetch("/api/live")).json();
  document.getElementById("gen-at").textContent = fmtTime(d.now_utc);
  document.getElementById("active-now").textContent = d.active_aircraft_now;

  const today = d.today || [];
  const peak = today.reduce((m, r) => Math.max(m, r.count), 0);
  document.getElementById("today-peak").textContent = peak ? Math.round(peak) : "–";

  const labels = today.map(r => `${r.hour}:00`);
  const todayVals = today.map(r => r.count);
  const yest = d.yesterday || [];
  const yestMap = Object.fromEntries(yest.map(r => [r.hour, r.count]));
  const yestVals = today.map(r => yestMap[r.hour] ?? null);

  const ctx = document.getElementById("liveChart").getContext("2d");
  if (window.liveChart) window.liveChart.destroy();
  window.liveChart = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [
        { label: "Today", data: todayVals, borderColor: COLORS.blue, backgroundColor: "rgba(88,166,255,.12)", fill: true, tension: .3, pointRadius: 0 },
        { label: "Yesterday", data: yestVals, borderColor: COLORS.gray, borderDash: [5,4], tension: .3, pointRadius: 0 },
      ],
    },
    options: chartOpts("Flight count"),
  });
}

/* ---------------- patterns ---------------- */

async function loadPatterns() {
  const d = await (await fetch("/api/patterns")).json();

  // hour profile
  const hp = d.hour_profile || [];
  const hctx = document.getElementById("hourChart").getContext("2d");
  if (window.hourChart) window.hourChart.destroy();
  window.hourChart = new Chart(hctx, {
    type: "bar",
    data: {
      labels: hp.map(r => `${r.hour}`),
      datasets: [{ label: "avg flights", data: hp.map(r => r.mean), backgroundColor: COLORS.green, borderRadius: 3 }],
    },
    options: chartOpts("avg count"),
  });

  // weekday
  const wp = d.weekday_profile || [];
  const wctx = document.getElementById("weekdayChart").getContext("2d");
  if (window.weekdayChart) window.weekdayChart.destroy();
  window.weekdayChart = new Chart(wctx, {
    type: "bar",
    data: {
      labels: wp.map(r => r.weekday),
      datasets: [{ label: "mean daily total", data: wp.map(r => r.mean_total), backgroundColor: COLORS.purple, borderRadius: 3 }],
    },
    options: chartOpts("total flights/day"),
  });

  // anomalies
  const anomalies = d.anomalies || [];
  const panel = document.getElementById("anomalies-panel");
  const body = document.getElementById("anomalies-body");
  if (anomalies.length) {
    panel.style.display = "";
    body.innerHTML = anomalies.map(a =>
      `<div class="anomaly">
        <span class="tag">${a.date}</span>
        <span>mean <b>${a.mean}</b> vs trailing <b>${a.trail_mean}</b></span>
        <span style="color:${a.deviation_pct < 0 ? COLORS.red : COLORS.amber}">
          (${a.deviation_pct > 0 ? "+" : ""}${a.deviation_pct}%)</span>
      </div>`).join("");
  } else {
    panel.style.display = "none";
  }
}

/* ---------------- forecasts ---------------- */

async function loadForecasts() {
  const d = await (await fetch("/api/forecasts")).json();

  const names = Object.keys(d.h1_history || {});
  const series = {};
  for (const n of names) {
    series[n] = d.h1_history[n].slice(-48);
  }
  const sample = names.length ? series[names[0]] : [];
  const labels = sample.map(p => p.target.slice(5));

  const ctx = document.getElementById("fcChart").getContext("2d");
  if (window.fcChart) window.fcChart.destroy();
  const datasets = [];
  const modelColors = [COLORS.blue, COLORS.green, COLORS.purple, COLORS.amber];
  names.forEach((n, i) => {
    const short = n.replace("flight-traffic-", "");
    datasets.push({
      label: `pred: ${short}`,
      data: series[n].map(p => p.pred),
      borderColor: modelColors[i % modelColors.length],
      tension: .25, pointRadius: 0,
    });
  });
  // actuals (from whichever series has them)
  datasets.push({
    label: "actual",
    data: sample.map(p => p.actual ?? null),
    borderColor: COLORS.red, borderDash: [6,4], tension: .3, pointRadius: 2,
  });
  if (window.fcChart) window.fcChart.destroy();
  window.fcChart = new Chart(ctx, {
    type: "line",
    data: { labels, datasets },
    options: chartOpts("flight count"),
  });

  // latest forecast block
  const latest = document.getElementById("latest-fc");
  if (d.latest_generated) {
    let html = `<div class="mono">generated: ${fmtTime(d.latest_generated)} UTC</div><br>`;
    for (const [name, val] of Object.entries(d.latest_models || {})) {
      html += `<div><span style="color:${COLORS.blue}">●</span> <b>${name.replace("flight-traffic-", "")}</b> next-hour: <span class="mono">${val}</span></div>`;
    }
    html += `<div style="color:var(--muted);margin-top:8px">${d.num_forecasts} forecasts stored · comparison via daily eval</div>`;
    latest.innerHTML = html;
  } else {
    latest.innerHTML = "No forecasts yet.";
  }
}

/* ---------------- health ---------------- */

async function loadHealth() {
  const d = await (await fetch("/api/health")).json();
  const pill = document.getElementById("status-pill");
  const fresh = d.data_freshness_min;
  if (fresh === null) {
    pill.textContent = "no data today";
    pill.className = "status-pill bad";
  } else if (fresh <= 30) {
    pill.textContent = `● live (${fresh} min ago)`;
    pill.className = "status-pill ok";
  } else if (fresh <= 120) {
    pill.textContent = `stale (${fresh} min)`;
    pill.className = "status-pill warn";
  } else {
    pill.textContent = `● down? (${fresh} min)`;
    pill.className = "status-pill bad";
  }

  document.getElementById("raw-today").textContent = d.raw_files_today ?? "–";
  document.getElementById("freshness").textContent =
    fresh === null ? "–" : `${fresh} min`;

  const cov = d.coverage || [];
  const rows = cov.slice().reverse().map(r => {
    return `<tr>
      <td>${r.date}</td>
      <td>${r.raw ? '<span class="dot yes"></span>' : '<span class="dot no"></span>'}</td>
      <td>${r.features ? '<span class="dot yes"></span>' : '<span class="dot no"></span>'}</td>
    </tr>`;
  }).join("");
  document.getElementById("health-body").innerHTML = `
    <div class="info-body">
      <span>feature days: <b>${d.feature_days}</b> &nbsp;·&nbsp;
      forecasts stored: <b>${d.forecast_count}</b></span>
      <div style="height:10px"></div>
    </div>
    <table class="coverage">
      <tr><th>date</th><th>raw</th><th>features</th></tr>
      ${rows}
    </table>`;
}

/* ---------------- reports ---------------- */

async function loadReports() {
  const d = await (await fetch("/api/reports")).json();
  const body = document.getElementById("reports-body");
  if (!d.reports || !d.reports.length) {
    body.innerHTML = '<span class="info-body">No reports yet.</span>';
    return;
  }
  body.innerHTML = d.reports.map(r => {
    const url = `/api/reports/file?key=${encodeURIComponent(r.key)}`;
    return `<a href="${url}" target="_blank">📄 ${r.date}</a>`;
  }).join("");
}

/* ---------------- helpers ---------------- */

function chartOpts(ylabel) {
  return {
    responsive: true, maintainAspectRatio: false,
    plugins: { legend: { labels: { color: "#8b949e", boxWidth: 12 } } },
    scales: {
      x: { ticks: { color: "#8b949e", maxRotation: 45 }, grid: { color: "#21262d" } },
      y: { ticks: { color: "#8b949e" }, grid: { color: "#21262d" }, title: { display: true, text: ylabel, color: "#8b949e" } },
    },
  };
}

/* ---------------- loop ---------------- */

async function refresh() {
  try {
    await Promise.all([loadLive(), loadPatterns(), loadForecasts(), loadHealth(), loadReports()]);
  } catch (e) {
    const pill = document.getElementById("status-pill");
    pill.textContent = "load error";
    pill.className = "status-pill bad";
  }
}

refresh();
setInterval(refresh, 60000);
