function escapeHtml(s) {
  const d = document.createElement("div");
  d.textContent = s ?? "";
  return d.innerHTML;
}

function badge(status) {
  return { pass: "✓", fail: "✗", skip: "–", missing: "?" }[status] || "?";
}

function renderResult(suiteId, data) {
  const resultsEl = document.getElementById("results-" + suiteId);
  const card = document.getElementById("card-" + suiteId);
  if (!resultsEl || !card) return;

  if (data.error) {
    resultsEl.innerHTML = `<div class="msg-block error"><strong>error</strong><pre>${escapeHtml(data.error)}</pre></div>`;
    card.dataset.status = "error";
    updateEngineSummary(card.dataset.engine);
    return;
  }
  if (data.build_error) {
    resultsEl.innerHTML = `<div class="msg-block error"><strong>build/run failed</strong><pre>${escapeHtml(data.build_error)}</pre></div>`;
    card.dataset.status = "error";
    updateEngineSummary(card.dataset.engine);
    return;
  }

  const tests = data.tests || [];
  const passed = tests.filter((t) => t.status === "pass").length;
  const failed = tests.filter((t) => t.status === "fail").length;
  const other = tests.length - passed - failed;
  card.dataset.status = failed > 0 ? "fail" : "pass";

  let html = `<div class="summary ${failed ? "has-fail" : "all-pass"}">`;
  html += `${passed}/${tests.length} passed`;
  if (failed) html += `, <span class="fail-count">${failed} failed</span>`;
  if (other) html += `, ${other} skipped/missing`;
  html += ` — ${data.duration_s.toFixed(2)}s</div>`;

  html += '<ul class="test-list">';
  for (const t of tests) {
    html += `<li class="test-${t.status}"><span class="badge">${badge(t.status)}</span> <span class="tname">${escapeHtml(t.name)}</span>`;
    if (t.elapsed_s != null) html += ` <span class="elapsed">${t.elapsed_s.toFixed(3)}s</span>`;
    if (t.message) html += `<pre class="msg">${escapeHtml(t.message)}</pre>`;
    html += "</li>";
  }
  html += "</ul>";
  resultsEl.innerHTML = html;
  updateEngineSummary(card.dataset.engine);
}

function updateEngineSummary(engine) {
  const cards = document.querySelectorAll(`.card[data-engine="${engine}"]`);
  let ran = 0, clean = 0, failing = 0;
  cards.forEach((c) => {
    const status = c.dataset.status;
    if (status === "pass") { ran++; clean++; }
    else if (status === "fail" || status === "error") { ran++; failing++; }
  });
  const el = document.getElementById("summary-" + engine);
  if (!el) return;
  el.textContent = ran === 0
    ? `${cards.length} suites, none run yet`
    : `${ran}/${cards.length} suites run — ${clean} clean${failing ? `, ${failing} with failures` : ""}`;
}

async function runSuite(suiteId, btn) {
  const resultsEl = document.getElementById("results-" + suiteId);
  const original = btn.textContent;
  btn.disabled = true;
  btn.textContent = "running…";
  resultsEl.innerHTML = '<div class="pending">running…</div>';
  try {
    const res = await fetch(`/api/run/${encodeURIComponent(suiteId)}`, { method: "POST" });
    const data = await res.json();
    renderResult(suiteId, data);
  } catch (e) {
    resultsEl.innerHTML = `<div class="msg-block error">request failed: ${escapeHtml(String(e))}</div>`;
  } finally {
    btn.disabled = false;
    btn.textContent = original;
  }
}

async function runAll(engine, btn) {
  const original = btn.textContent;
  btn.disabled = true;
  btn.textContent = "running all…";
  const cards = document.querySelectorAll(`.card[data-engine="${engine}"]`);
  cards.forEach((c) => {
    const el = document.getElementById("results-" + c.dataset.suiteId);
    if (el) el.innerHTML = '<div class="pending">queued…</div>';
  });
  try {
    const res = await fetch(`/api/run-all/${engine}`, { method: "POST" });
    const data = await res.json();
    for (const r of data) renderResult(r.suite_id, r);
  } catch (e) {
    alert("run-all failed: " + e);
  } finally {
    btn.disabled = false;
    btn.textContent = original;
  }
}

document.addEventListener("DOMContentLoaded", () => {
  updateEngineSummary("rust");
  updateEngineSummary("go");
});
