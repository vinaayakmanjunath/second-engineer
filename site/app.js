// Second Engineer dashboard: one module, one init function per page (selected by <body data-page>).
// All numbers come from data/*.json produced by the pipeline's export step.
import * as Plot from "https://cdn.jsdelivr.net/npm/@observablehq/plot@0.6.16/+esm";

const MERMAID_URL = "https://cdn.jsdelivr.net/npm/mermaid@10.9.1/dist/mermaid.esm.min.mjs";
const REPO_URL = "https://github.com/vinaayakmanjunath/second-engineer";
const HOUR = 3600e3;
const NAV = [["index.html", "Fleet"], ["vessel.html", "Vessel"], ["quality.html", "Data quality"], ["pipeline.html", "Pipeline"]];
const BANNER = "Virtual vessels derived from one real ship (ARC Gloria) · fault injected into GLORIA-C main engine from a test-bench signature · demonstration, not validated";
const FOOTER = "Data: Sensor-Based Operational Dataset of Main Ship Machinery (ARC Gloria), Zenodo, DOI 10.5281/zenodo.18330661, CC BY 4.0. Marine Engine Fault dataset, Zenodo, DOI 10.5281/zenodo.19857425, CC BY 4.0. Data transformed (resampled, mapped, fault-injected) for demonstration. Virtual vessels; injected fault; demonstration, not validated.";

// ---------- formatting & data helpers ----------
const $ = (sel) => document.querySelector(sel);
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
// Timestamps in the exports are UTC; some lack a zone suffix ("2026-09-01 21:53:12"), so force UTC parsing.
const toDate = (s) => (s == null ? null : new Date(/[zZ]|[+-]\d\d:?\d\d$/.test(s) ? s : s.replace(" ", "T") + "Z"));
const fmtTime = (d) => (d == null ? "—" : (d instanceof Date ? d : toDate(d)).toISOString().slice(0, 16).replace("T", " "));
const fmtNum = (n, digits = 0) => (n == null || Number.isNaN(n) ? "—" : Number(n).toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits }));
const fmtUsd = (n) => (n == null ? "—" : "$" + fmtNum(n));
const label = (s) => String(s).replaceAll("_", " ");
const num = (v) => (v == null ? NaN : v); // Plot breaks lines/areas at NaN
const sum = (rows, key) => rows.reduce((s, r) => s + (r[key] ?? 0), 0);
function groupBy(rows, key) {
  const m = new Map();
  for (const r of rows) m.has(key(r)) ? m.get(key(r)).push(r) : m.set(key(r), [r]);
  return m;
}
const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

async function load(name) {
  const res = await fetch(`data/${name}`);
  if (!res.ok) throw new Error(`data/${name}: HTTP ${res.status}`);
  return res.json();
}

function healthClass(h) {
  if (h == null) return ["", "no data"];
  return h >= 70 ? ["ok", "good"] : h >= 40 ? ["warn", "watch"] : ["bad", "poor"];
}

function likelyCause(alert, { full = false } = {}) {
  const m = alert.fault_match ?? [];
  // gold decides confidence (mart_fault_match.confident); the page only displays it
  if (!m.length || !m[0].confident) return "no confident match";
  if (!full) return `${esc(label(m[0].fault))} (${m[0].similarity.toFixed(2)})`;
  const margin = m[0].margin == null ? "" : ` <span class="muted">· lead over #2: ${m[0].margin.toFixed(3)}</span>`;
  return `<ol>${m.slice(0, 3).map((f) => `<li>${esc(label(f.fault))} <span class="muted">${f.similarity.toFixed(2)}</span></li>`).join("")}</ol>${margin}`;
}

// Columns: {label, text?: row => value (escaped), html?: row => trusted markup, num?: bool}
function table(cols, rows, { caption = "", empty = "None" } = {}) {
  const cell = (c, r) => (c.html ? c.html(r) : esc(c.text(r)));
  const cls = (c) => (c.num ? ' class="num"' : "");
  const body = rows.length
    ? rows.map((r) => `<tr>${cols.map((c) => `<td${cls(c)}>${cell(c, r)}</td>`).join("")}</tr>`).join("")
    : `<tr><td colspan="${cols.length}">${esc(empty)}</td></tr>`;
  return `<div class="table-wrap"><table>${caption ? `<caption>${esc(caption)}</caption>` : ""}
    <thead><tr>${cols.map((c) => `<th scope="col"${cls(c)}>${esc(c.label)}</th>`).join("")}</tr></thead>
    <tbody>${body}</tbody></table></div>`;
}

const kv = (pairs) => `<dl class="kv">${pairs.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`).join("")}</dl>`;
const settingsList = (obj) => kv(Object.entries(obj ?? {}).map(([k, v]) => [label(k), Array.isArray(v) ? v.join(", ") : v]));

// Give rows a segment id so lines break across gaps (engine off / no data) instead of bridging them.
function withSegments(rows, key) {
  let seg = 0;
  const prev = new Map();
  return rows.map((r) => {
    const k = key(r);
    if (!prev.has(k) || r.t - prev.get(k) > HOUR) seg++;
    prev.set(k, r.t);
    return { ...r, seg };
  });
}

// ---------- charts: re-render on resize and colour-scheme change ----------
const charts = new Map();
function chart(el, render) {
  charts.set(el, render);
  draw(el);
}
function draw(el) {
  el.replaceChildren(charts.get(el)(Math.max(280, el.clientWidth)));
}
let resizeTimer;
const redrawAll = () => charts.forEach((_, el) => draw(el));
addEventListener("resize", () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(redrawAll, 150);
});
matchMedia("(prefers-color-scheme: dark)").addEventListener("change", redrawAll);

const timeAxis = { type: "utc", label: "Time (UTC)" };

// Vertical markers for the injected fault's ramp start and declared failure (from run.results, when published).
function eventMarks(results, vessel) {
  const inj = results?.injection;
  if (!inj || (vessel && inj.vessel_id !== vessel)) return [];
  const ev = [
    { t: toDate(inj.ramp_start_utc), text: "ramp start", anchor: "end", dx: -3 },
    { t: toDate(inj.failure_utc), text: "declared failure", anchor: "start", dx: 3 },
  ];
  return [
    Plot.ruleX(ev, { x: "t", stroke: css("--bad"), strokeDasharray: "4,3" }),
    // label ramp start left of its line and failure right of its line so they never collide
    ...ev.map((e) => Plot.text([e], { x: "t", text: "text", frameAnchor: "top", textAnchor: e.anchor, dx: e.dx, dy: 4, fill: css("--bad"), fontSize: 10 })),
  ];
}

const alertSpans = (alerts) =>
  Plot.rect(alerts, { x1: (a) => toDate(a.first_flagged), x2: (a) => toDate(a.last_seen), fill: css("--span"), fillOpacity: 0.18 });

function healthPlot(width, { height = 220, marks = [], color } = {}) {
  return Plot.plot({
    width, height, marginLeft: 40,
    x: timeAxis,
    y: { domain: [0, 100], label: "Health (0–100)", grid: true },
    color,
    marks,
  });
}

// ---------- page shell ----------
function shell() {
  const page = location.pathname.split("/").pop() || "index.html";
  document.body.insertAdjacentHTML("afterbegin", `
    <a class="skip" href="#main">Skip to content</a>
    <header class="site-header"><div class="inner">
      <a class="brand" href="index.html">Second Engineer</a>
      <nav aria-label="Main"><ul>${NAV.map(([href, text]) =>
        `<li><a href="${href}"${href === page ? ' aria-current="page"' : ""}>${text}</a></li>`).join("")}</ul></nav>
    </div></header>
    <div class="banner" role="note"><div class="inner">${esc(BANNER)}</div></div>`);
  document.body.insertAdjacentHTML("beforeend", `<footer class="site-footer"><div class="inner"><p>${esc(FOOTER)}</p></div></footer>`);
}

// ---------- Fleet overview with replay ----------
async function initFleet() {
  const [fleet, alerts, health, run] = await Promise.all(["fleet.json", "alerts.json", "health.json", "run.json"].map(load));
  const { detection } = run.settings;
  const results = run.results;
  const vessels = fleet.map((f) => f.vessel_id);
  const rows = health.map((r) => ({ ...r, t: toDate(r.hour) }));
  const bySystem = groupBy(rows, (r) => `${r.vessel_id}|${r.system}`);
  const A = alerts.map((a) => ({ ...a, opened: toDate(a.opened_at), last: toDate(a.last_seen) }));
  const me = withSegments(rows.filter((r) => r.system === "ME"), (r) => r.vessel_id);

  const start = new Date(Math.min(...rows.map((r) => +r.t)));
  const end = new Date(Math.max(...fleet.map((f) => +toDate(f.as_of_utc))));
  const steps = Math.ceil((end - start) / HOUR);
  const slider = $("#replay");
  Object.assign(slider, { min: 0, max: steps, step: 1, value: steps });
  const at = (i) => (i >= steps ? end : new Date(+start + i * HOUR));

  // State as of T, reconstructed with the same rules gold uses (mart_fleet / mart_alerts):
  // an hour is known once it has ended; vessel health = min over systems of their latest hour;
  // an alert is open while its system's latest hour ends within alert_close_hours of its last flagged hour.
  function stateAt(T) {
    return fleet.map((f) => {
      const latest = new Map();
      for (const [key, rs] of bySystem) {
        if (!key.startsWith(f.vessel_id + "|")) continue;
        const r = rs.findLast((x) => +x.t + HOUR <= T);
        if (r) latest.set(r.system, r);
      }
      const hs = [...latest.values()];
      const va = A.filter((a) => a.vessel_id === f.vessel_id && a.opened <= T).map((a) => {
        const l = latest.get(a.system);
        return { ...a, open: !l || +l.t + HOUR - a.last <= detection.alert_close_hours * HOUR };
      });
      const lastRun = hs.length ? Math.max(...hs.map((r) => +r.t + HOUR)) : null;
      return {
        f, alerts: va,
        health: hs.length ? Math.min(...hs.map((r) => r.health)) : null,
        open: va.filter((a) => a.open).length,
        total: va.length,
        usd: sum(va.filter((a) => a.open), "usd_at_risk"),
        lagH: lastRun == null ? null : (T - lastRun) / HOUR,
      };
    });
  }

  function card(s, live) {
    // Live state is owned by gold (fleet.json); replay state is reconstructed client-side.
    const v = live
      ? { health: s.f.health_index, open: s.f.open_alerts, total: s.f.total_alerts, usd: s.f.usd_at_risk_open,
          lag: `${fmtNum(s.f.freshness_lag_hours, 1)} h`, lagNote: "behind the newest batch in the fleet" }
      : { ...s, lag: s.lagH == null ? "—" : `${fmtNum(s.lagH, 1)} h`, lagNote: "behind the replay time" };
    const [cls, word] = healthClass(v.health);
    return `<article class="card" data-vessel="${esc(s.f.vessel_id)}">
      <h3><a href="vessel.html?v=${encodeURIComponent(s.f.vessel_id)}">${esc(s.f.vessel_id)}</a></h3>
      <p class="meta">${esc(s.f.owner)} · logger ${esc(s.f.logger)}</p>
      <dl>
        <dt>Health index</dt><dd class="health ${cls}" data-k="health">${fmtNum(v.health, 1)} <small>${word}</small></dd>
        <dt>Open / total alerts</dt><dd data-k="alerts">${v.open} / ${v.total}</dd>
        <dt>$ at risk <small>(illustrative)</small></dt><dd data-k="usd">${fmtUsd(v.usd)}</dd>
        <dt>Data lag <small>${v.lagNote}</small></dt><dd data-k="lag">${v.lag}</dd>
      </dl></article>`;
  }

  const chartEl = $("#fleet-chart");
  let T = end;
  chart(chartEl, (width) => {
    const past = me.filter((r) => r.t <= T);
    return healthPlot(width, {
      height: 260,
      color: { domain: vessels, legend: true },
      marks: [
        ...eventMarks(results),
        Plot.line(me, { x: "t", y: "health", stroke: "vessel_id", z: "seg", strokeOpacity: 0.2 }),
        Plot.line(past, { x: "t", y: "health", stroke: "vessel_id", z: "seg", strokeWidth: 1.6 }),
        Plot.dot(past, { x: "t", y: "health", fill: "vessel_id", r: 1.2 }),
        Plot.ruleX([T], { stroke: css("--fg"), strokeWidth: 1.5 }),
        Plot.tip(past, Plot.pointer({ x: "t", y: "health", stroke: "vessel_id",
          title: (d) => `${d.vessel_id} ME\n${fmtTime(d.t)} UTC\nhealth ${d.health} · ${d.flagged} sensors flagged` })),
      ],
    });
  });

  function update() {
    T = at(+slider.value);
    const live = T >= end;
    $("#replay-time").value = `${fmtTime(T)} UTC`;
    $("#replay-mode").textContent = live ? "Live — latest pipeline state" : "Replay — state reconstructed as of this time";
    $("#live-btn").disabled = live;
    const state = stateAt(T);
    $("#cards").innerHTML = state.map((s) => card(s, live)).join("");
    const recent = state.flatMap((s) => s.alerts).sort((a, b) => b.opened - a.opened);
    $("#alerts").innerHTML = table([
      { label: "Vessel", html: (a) => `<a href="vessel.html?v=${encodeURIComponent(a.vessel_id)}">${esc(a.vessel_id)}</a>` },
      { label: "System", text: (a) => a.system },
      { label: "Opened (UTC)", text: (a) => fmtTime(a.opened) },
      { label: "Status", html: (a) => (a.open ? '<span class="status-open">open</span>' : "closed") },
      { label: "Severity", text: (a) => a.severity },
      { label: "Max |z|", num: true, text: (a) => fmtNum(a.max_abs_z, 1) },
      { label: "Likely cause", html: (a) => likelyCause(a) },
      { label: "$ at risk (illustrative)", num: true, text: (a) => fmtUsd(a.usd_at_risk) },
    ], recent.slice(0, 10), {
      empty: "No alerts opened yet at this time.",
      caption: `Showing ${Math.min(10, recent.length)} of ${recent.length} alerts opened by ${fmtTime(T)} UTC. Status is as of that time; severity, max |z|, likely cause and $ describe the whole alert window. $ at risk is illustrative (assumed off-hire event cost, not a pricing model).`,
    });
    draw(chartEl);
  }
  slider.addEventListener("input", update);
  $("#live-btn").addEventListener("click", () => { slider.value = steps; update(); });
  $("#replay-range").textContent = `${fmtTime(start)} → ${fmtTime(end)} UTC`;
  update();

  $("#results").innerHTML = results ? resultsSummary(results) : '<p class="muted">Results pending — the evaluation summary has not been published yet.</p>';
}

function resultsSummary(r) {
  const fc = r.fault_vs_control ?? {}, f = fc.with_fault ?? {}, c = fc.control ?? {}, inj = r.injection ?? {};
  const v = (x) => (x == null ? "—" : typeof x === "string" ? label(x) : x);
  const rows = [
    ["Alerts opened in ramp window", "alerts_opened_in_window"], ["First alert, hours before failure", "hours_before_failure"],
    ["Flagged hours in window", "flagged_hours_in_window"], ["Peak |z| in window", "peak_abs_z_in_window"],
    ["Strongest alert: top fault match", "top_fault"], ["Strongest alert: injected fault rank", "injected_fault_rank"],
    ["Alerts before the ramp (false alarms)", "alerts_before_ramp"]];
  return `<div class="grid2"><div>
    <h3>Injected fault</h3>${kv([
      ["Vessel / system", `${inj.vessel_id} / ${inj.system}`], ["Fault", label(inj.fault ?? "—")],
      ["Severity (× bench)", inj.severity_x_bench], ["Ramp", `${fmtTime(inj.ramp_start_utc)} → ${fmtTime(inj.failure_utc)} UTC (${inj.ramp_hours} h)`]])}
    </div><div>
    <h3>Fault vs no-fault control</h3>
    <p><strong>${fc.verdict ?? "—"}</strong></p>
    ${table([{ label: "Ramp window", text: (x) => x[0] }, { label: "With fault", num: true, text: (x) => v(f[x[1]]) },
      { label: "Control (no fault)", num: true, text: (x) => v(c[x[1]]) }], rows,
      { caption: "Same vessel and detector, re-run without the fault. Only differences from the control are effects of the fault." })}
    ${fc.paired_hours ? `<p>Hour by hour in the window: <strong>${fc.paired_hours.fault_only} hours flagged only with the fault</strong>,
      ${fc.paired_hours.control_only} only in the control, ${fc.paired_hours.both} in both. Before the ramp the control flags
      ${Math.round(fc.paired_hours.control_pre_ramp_flag_rate * 100)}% of healthy hours (natural drift).</p>` : ""}
    </div></div>
    <h3>False alerts</h3><p class="muted">Alerts on data with no fault: the healthy vessels from this run, and the injected vessel from its no-fault control, where every alert is false.</p>${table([
      { label: "Vessel", text: (x) => x.vessel_id }, { label: "Alerts", num: true, text: (x) => x.alerts },
      { label: "Running days", num: true, text: (x) => fmtNum(x.running_days, 1) },
      { label: "Per running day", num: true, text: (x) => fmtNum(x.per_running_day, 2) }], r.false_alerts ?? [])}
    <p class="muted">Results generated ${fmtTime(r.generated_utc)} UTC.</p>`;
}

// ---------- Vessel detail ----------
async function initVessel() {
  const [fleet, sensors, alerts, health, baselines, run] = await Promise.all(
    ["fleet.json", "sensors.json", "alerts.json", "health.json", "baselines.json", "run.json"].map(load));
  const zThr = run.settings.detection.z_threshold;
  const results = run.results;
  const vessels = fleet.map((f) => f.vessel_id);
  const sensorInfo = new Map(sensors.map((s) => [s.sensor_id, s]));
  const series = new Map(); // vessel -> Promise<series json>
  const getSeries = (v) => (series.has(v) || series.set(v, load(`series_${v}.json`)), series.get(v));
  const sel = { vessel: $("#sel-vessel"), system: $("#sel-system"), sensor: $("#sel-sensor") };
  const options = (el, values, current, text = (x) => x) => {
    el.innerHTML = values.map((x) => `<option value="${esc(x)}">${esc(text(x))}</option>`).join("");
    el.value = values.includes(current) ? current : values[0];
  };

  const q = new URLSearchParams(location.search);
  options(sel.vessel, vessels, vessels.includes(q.get("v")) ? q.get("v") : vessels.includes("GLORIA-C") ? "GLORIA-C" : vessels[0]);
  let want = { system: q.get("system") ?? "ME", sensor: q.get("sensor") ?? "me.exh_temp_cyl3_c" };

  async function refresh() {
    const v = sel.vessel.value;
    const data = await getSeries(v);
    if (v !== sel.vessel.value) return; // a newer selection superseded this one
    const ids = Object.keys(data).filter((id) => sensorInfo.has(id));
    const systems = [...new Set(ids.map((id) => sensorInfo.get(id).system))];
    options(sel.system, systems, want.system);
    const inSystem = ids.filter((id) => sensorInfo.get(id).system === sel.system.value);
    const fallback = inSystem.find((id) => sensorInfo.get(id).scored) ?? inSystem[0];
    options(sel.sensor, inSystem, inSystem.includes(want.sensor) ? want.sensor : fallback,
      (id) => `${id}${sensorInfo.get(id).scored ? "" : " (unscored)"}`);
    want = { system: sel.system.value, sensor: sel.sensor.value };
    history.replaceState(null, "", `?${new URLSearchParams({ v, system: want.system, sensor: want.sensor })}`);
    render(v, want.system, want.sensor, data[want.sensor]);
  }
  sel.vessel.addEventListener("change", refresh);
  sel.system.addEventListener("change", () => { want.system = sel.system.value; refresh(); });
  sel.sensor.addEventListener("change", () => { want.sensor = sel.sensor.value; refresh(); });

  function render(v, system, sensorId, s) {
    const info = sensorInfo.get(sensorId);
    const b = baselines.find((x) => x.vessel_id === v && x.sensor_id === sensorId);
    const sysAlerts = alerts.filter((a) => a.vessel_id === v && a.system === system);
    const unit = info.unit;
    $("#sensor-meta").innerHTML = b
      ? `${esc(label(info.measurement))} (${esc(unit)}) · baseline from ${b.n_hours} running hours, ${fmtTime(b.ref_start)} → ${fmtTime(b.ref_end)} UTC · ${
          b.speed_conditioned ? `straight line vs engine rpm (${fmtNum(b.rpm_min)}–${fmtNum(b.rpm_max)} rpm)` : "constant (not speed-dependent)"} · σ = ${fmtNum(b.sigma, 3)} ${esc(unit)}`
      : `${esc(label(info.measurement))} (${esc(unit)}) · ${info.scored ? "no baseline for this vessel" : "not scored (load proxy or sea-temperature driven)"}`;
    $("#value-title").textContent = `${sensorId} — hourly mean while running, ${v}`;

    const pts = withSegments(s.hour.map((h, i) => {
      const e = s.expected[i], sg = s.sigma[i];
      return { t: toDate(h), value: s.value[i], expected: num(e), lo: e == null || sg == null ? NaN : e - zThr * sg,
               hi: e == null || sg == null ? NaN : e + zThr * sg, z: num(s.z[i]) };
    }), () => 0);
    const scored = pts.some((p) => !Number.isNaN(p.z));

    chart($("#value-chart"), (width) => Plot.plot({
      width, height: 280, marginLeft: 50,
      x: timeAxis, y: { label: `${label(info.measurement)} (${unit})`, grid: true },
      marks: [
        alertSpans(sysAlerts),
        Plot.areaY(pts, { x: "t", y1: "lo", y2: "hi", z: "seg", fill: css("--band"), fillOpacity: 0.16 }),
        Plot.line(pts, { x: "t", y: "expected", z: "seg", stroke: css("--muted"), strokeDasharray: "5,3" }),
        Plot.line(pts, { x: "t", y: "value", z: "seg", stroke: css("--accent"), strokeWidth: 1.4 }),
        Plot.dot(pts, { x: "t", y: "value", fill: css("--accent"), r: 1.1 }),
        ...eventMarks(results, v),
        Plot.tip(pts, Plot.pointerX({ x: "t", y: "value",
          title: (d) => `${fmtTime(d.t)} UTC\nvalue ${fmtNum(d.value, 2)} ${unit}` +
            (Number.isNaN(d.expected) ? "\nnot scored" : `\nexpected ${fmtNum(d.expected, 2)} · z ${fmtNum(d.z, 2)}`) })),
      ],
    }));

    $("#z-section").hidden = !scored;
    if (scored) {
      chart($("#z-chart"), (width) => Plot.plot({
        width, height: 200, marginLeft: 50,
        x: timeAxis, y: { label: "Robust z", grid: true },
        marks: [
          alertSpans(sysAlerts),
          Plot.ruleY([0], { stroke: css("--muted") }),
          Plot.ruleY([-zThr, zThr], { stroke: css("--bad"), strokeDasharray: "4,3" }),
          Plot.line(pts, { x: "t", y: "z", z: "seg", stroke: css("--accent"), strokeOpacity: 0.6 }),
          Plot.dot(pts, { x: "t", y: "z", r: 1.6, fill: (d) => (Math.abs(d.z) >= zThr ? css("--bad") : css("--accent")) }),
          ...eventMarks(results, v),
        ],
      }));
    }

    const hs = withSegments(health.filter((r) => r.vessel_id === v && r.system === system).map((r) => ({ ...r, t: toDate(r.hour) })), () => 0);
    $("#health-title").textContent = `${system} health, ${v}`;
    chart($("#health-chart"), (width) => healthPlot(width, {
      marks: [
        alertSpans(sysAlerts),
        Plot.line(hs, { x: "t", y: "health", z: "seg", stroke: css("--accent") }),
        Plot.dot(hs, { x: "t", y: "health", r: 1.1, fill: css("--accent") }),
        ...eventMarks(results, v),
        Plot.tip(hs, Plot.pointerX({ x: "t", y: "health", title: (d) => `${fmtTime(d.t)} UTC\nhealth ${d.health} · ${d.flagged} sensors flagged` })),
      ],
    }));

    $("#vessel-alerts-title").textContent = `Alerts — ${v}`;
    $("#vessel-alerts").innerHTML = table([
      { label: "Opened (UTC)", text: (a) => fmtTime(a.opened_at) },
      { label: "System", text: (a) => a.system },
      { label: "Status", html: (a) => (a.status === "open" ? '<span class="status-open">open</span>' : "closed") },
      { label: "Flagged h", num: true, text: (a) => a.flagged_hours },
      { label: "Max |z|", num: true, text: (a) => fmtNum(a.max_abs_z, 1) },
      { label: "Severity", text: (a) => a.severity },
      { label: "Sensors", html: (a) => `<span title="${esc(a.sensors)}">${a.sensors ? a.sensors.split(",").length : 0}</span>` },
      { label: "Likely cause (similarity)", html: (a) => likelyCause(a, { full: true }) },
      { label: "$ at risk (illustrative)", num: true, text: (a) => fmtUsd(a.usd_at_risk) },
    ], alerts.filter((a) => a.vessel_id === v).sort((a, b) => toDate(b.opened_at) - toDate(a.opened_at)), {
      empty: "No alerts for this vessel.",
      caption: `Likely cause = top-3 bench fault signatures by cosine similarity; below ${run.settings.detection.match_min} shown as no confident match. A small lead over #2 means the top two faults look alike. $ at risk is illustrative (assumed off-hire event cost, not a pricing model). Shaded spans on the charts are alert windows.`,
    });
  }
  await refresh();
}

// ---------- Data quality ----------
const DQ_METRICS = [
  ["valid_pct", "Valid %"], ["coverage_pct", "Coverage %"], ["points_quarantined", "Quarantined points"],
  ["late_batches", "Late batches"], ["lateness_p95_min", "Lateness p95 (min)"], ["out_of_order_rows", "Out-of-order rows"],
  ["duplicate_files", "Duplicate files dropped"], ["duplicate_batches", "Duplicate batches dropped"],
  ["flatlined_sensor_hours", "Flatlined sensor-hours"], ["unmapped_points", "Unmapped points"],
];

async function initQuality() {
  const [dq, run] = await Promise.all(["dq.json", "run.json"].map(load));
  const vessels = [...new Set(dq.map((r) => r.vessel_id))];
  const totals = vessels.map((v) => {
    const rs = dq.filter((r) => r.vessel_id === v);
    const t = { vessel_id: v, days: rs.length };
    for (const k of Object.keys(rs[0])) if (typeof rs[0][k] === "number") t[k] = sum(rs, k);
    t.valid_pct = (100 * t.points_valid) / (t.points_valid + t.points_quarantined);
    t.coverage_pct = t.coverage_pct / rs.length;
    t.lateness_p95_max = Math.max(...rs.map((r) => r.lateness_p95_min ?? 0));
    return t;
  });
  const col = (lbl, k, d = 0) => ({ label: lbl, num: true, text: (r) => fmtNum(r[k], d) });
  const vcol = { label: "Vessel", text: (r) => r.vessel_id };
  $("#dq-volume").innerHTML = table([vcol, col("Days", "days"), col("Points valid", "points_valid"),
    col("Quarantined", "points_quarantined"), col("Valid %", "valid_pct", 2), col("Mean daily coverage %", "coverage_pct", 1),
    col("Missing value", "q_missing_value"), col("Bad timestamp", "q_bad_timestamp"), col("Future timestamp", "q_future_timestamp"),
    col("Out of range", "q_out_of_range"), col("Unknown sensor", "q_unknown_sensor"), col("Unmapped points", "unmapped_points")],
  totals, { caption: "Quarantine columns count points by reason. Coverage % = share of the day's seconds with data." });
  $("#dq-transport").innerHTML = table([vcol, col("Batches loaded", "batches_loaded"), col("Duplicate files dropped", "duplicate_files"),
    col("Duplicate batches dropped", "duplicate_batches"), col("Late batches (>1 h)", "late_batches"),
    col("Max daily lateness p95 (min)", "lateness_p95_max", 1), col("Out-of-order rows", "out_of_order_rows"),
    col("Flatlined sensor-hours", "flatlined_sensor_hours")], totals);

  const sim = run.simulator ?? {};
  const simVessels = Object.keys(sim);
  $("#sim-injected").innerHTML = simVessels.length
    ? table([{ label: "Simulator log counter", text: label },
        ...simVessels.map((v) => ({ label: v, num: true, text: (k) => fmtNum(sim[v][k]) }))],
      [...new Set(simVessels.flatMap((v) => Object.keys(sim[v]).filter((k) => typeof sim[v][k] === "number")))])
    : '<p class="muted">No simulator log in this run.</p>';
  const transport = run.results?.transport;
  $("#sim-handled").innerHTML = transport
    ? table([{ label: "Vessel", text: (x) => x.vessel_id }, { label: "Check", text: (x) => label(x.check) },
        { label: "Injected", num: true, text: (x) => fmtNum(x.injected) }, { label: "Handled", num: true, text: (x) => fmtNum(x.handled) },
        { label: "Match", html: (x) => (x.match == null ? '<span class="muted">not 1:1</span>' : x.match ? "yes" : '<span class="bad">no</span>') }], transport,
      { caption: "“not 1:1”: injected and handled counts measure different things (e.g. rows shuffled vs rows seen out of order)." })
    : '<p class="muted">Results pending — handled counts are published with the evaluation results.</p>';

  const metricSel = $("#dq-metric");
  metricSel.innerHTML = DQ_METRICS.map(([k, l]) => `<option value="${k}">${esc(l)}</option>`).join("");
  const days = dq.map((r) => ({ ...r, t0: toDate(r.day), t1: new Date(+toDate(r.day) + 24 * HOUR) }));
  const heat = $("#dq-heatmap");
  chart(heat, (width) => {
    const [k, l] = DQ_METRICS.find(([key]) => key === metricSel.value);
    return Plot.plot({
      width, height: 90 + 34 * vessels.length, marginLeft: 76,
      x: timeAxis, y: { label: null, domain: vessels },
      color: { label: l, legend: true, scheme: k.endsWith("pct") ? "blues" : "oranges", zero: true },
      marks: [
        Plot.barX(days, { x1: "t0", x2: "t1", y: "vessel_id", fill: k, inset: 0.5 }),
        Plot.tip(days, Plot.pointer({ x1: "t0", x2: "t1", y: "vessel_id", title: (d) => `${d.vessel_id} ${d.day}\n${l}: ${fmtNum(d[k], 2)}` })),
      ],
    });
  });
  metricSel.addEventListener("change", () => draw(heat));

  $("#dq-daily").innerHTML = table([vcol, { label: "Day (UTC)", text: (r) => r.day },
    ...DQ_METRICS.map(([k, l]) => col(l, k, k.endsWith("pct") ? 2 : k === "lateness_p95_min" ? 1 : 0))], dq);
}

// ---------- Pipeline ----------
async function initPipeline() {
  const run = await load("run.json");
  $("#run-meta").innerHTML = kv([
    ["Export generated (UTC)", fmtTime(run.generated_utc)],
    ["Git commit", run.git_sha ?? "not recorded"],
    ["Silver partitions rewritten", run.steps.filter((s) => s.step === "silver").map((s) => s.partitions_rewritten).join(", ") || "—"],
  ]);
  const details = (s) => Object.entries(s).filter(([k, v]) => !["step", "at_utc", "seconds"].includes(k) && !Array.isArray(v))
    .map(([k, v]) => `${label(k)}: ${typeof v === "number" ? fmtNum(v) : v}`).join(" · ");
  $("#run-steps").innerHTML = table([
    { label: "Step", text: (s) => s.step }, { label: "At (UTC)", text: (s) => fmtTime(s.at_utc) },
    { label: "Seconds", num: true, text: (s) => fmtNum(s.seconds, 1) }, { label: "Details", text: details }], run.steps,
  { caption: "Most recent bronze/silver/gold step logs from the pipeline run log." });
  const { detection, injection, simulator, cost_event_usd } = run.settings;
  $("#set-detection").innerHTML = settingsList(detection);
  $("#set-injection").innerHTML = settingsList(injection);
  $("#set-simulator").innerHTML = settingsList(simulator);
  $("#set-cost").textContent = `Illustrative off-hire event cost: ${fmtUsd(cost_event_usd)} (assumed: off-hire + OPEX for the off-hire days + off-hire bunkers), weighted by alert severity. Not a pricing model.`;
  $("#repo-link").href = REPO_URL;

  const { default: mermaid } = await import(MERMAID_URL);
  mermaid.initialize({ startOnLoad: false, securityLevel: "strict",
    theme: matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "neutral" });
  await mermaid.run({ querySelector: ".mermaid" });
}

// ---------- boot ----------
const PAGES_INIT = { fleet: initFleet, vessel: initVessel, quality: initQuality, pipeline: initPipeline };
shell();
PAGES_INIT[document.body.dataset.page]?.().catch((err) => {
  console.error(err);
  $("#main").insertAdjacentHTML("afterbegin", `<p class="error" role="alert">Could not load dashboard data: ${esc(err.message)}</p>`);
});
