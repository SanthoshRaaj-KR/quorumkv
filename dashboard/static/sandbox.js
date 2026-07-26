const API = "/api/sandbox";

function escapeHtml(s) {
  const d = document.createElement("div");
  d.textContent = s ?? "";
  return d.innerHTML;
}

async function post(path, body) {
  const res = await fetch(API + path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  return res.json();
}

async function get(path) {
  const res = await fetch(API + path);
  return res.json();
}

async function refresh() {
  const state = await get("/state");
  render(state);
}

function roleClass(role) {
  return { leader: "role-leader", candidate: "role-candidate", follower: "role-follower" }[role] || "";
}

function render(state) {
  const nodesEl = document.getElementById("nodes");
  if (state.error) {
    nodesEl.innerHTML = `<div class="msg-block error">${escapeHtml(state.error)}</div>`;
    return;
  }
  nodesEl.innerHTML = "";
  for (const n of state.nodes || []) {
    nodesEl.appendChild(renderNode(n));
  }
  const traceEl = document.getElementById("trace");
  const trace = state.trace || [];
  traceEl.innerHTML = trace.length
    ? trace.slice().reverse().map(renderTraceRow).join("")
    : '<div class="pending">no messages yet — tick the clock or propose something</div>';
}

function renderNode(n) {
  const div = document.createElement("div");
  div.className = "node-card " + roleClass(n.role);
  if (n.down) div.classList.add("node-down");
  else if (n.isolated) div.classList.add("node-isolated");

  let entriesHtml = "";
  for (const e of n.entries) {
    const compacted = e.index <= n.offset;
    entriesHtml += `<tr class="${compacted ? "compacted" : ""}"><td>${e.index}</td><td>${e.term}</td><td>${e.noOp ? "<em>no-op</em>" : escapeHtml(e.cmd)}</td></tr>`;
  }

  div.innerHTML = `
    <div class="node-head">
      <span class="node-id">node ${n.id}</span>
      <span class="role-badge">${n.role}</span>
      ${n.down ? '<span class="status-badge down">DOWN</span>' : ""}
      ${n.isolated ? '<span class="status-badge isolated">PARTITIONED</span>' : ""}
    </div>
    <div class="node-stats">
      term=${n.term}&nbsp; leader=${n.leaderId || "–"}<br>
      commit=${n.commitIndex}&nbsp; applied=${n.lastApplied}&nbsp; last=${n.lastIndex}&nbsp; offset=${n.offset}
    </div>
    <table class="entries">
      <thead><tr><th>idx</th><th>term</th><th>cmd</th></tr></thead>
      <tbody>${entriesHtml || '<tr><td colspan="3" class="empty">nothing above the boundary</td></tr>'}</tbody>
    </table>

    <div class="engine-box">
      <div class="engine-box-title">propose (real LSM engine via its sidecar)</div>
      <input type="text" class="key-input" placeholder="key">
      <input type="text" class="value-input" placeholder="value (blank + delete = tombstone)">
      <div class="node-actions">
        <button onclick="doPropose(${n.id}, this, 'put')">Put</button>
        <button onclick="doPropose(${n.id}, this, 'delete')">Delete</button>
      </div>
    </div>

    <div class="engine-box">
      <div class="engine-box-title">get <span class="hint">(reads this node's local engine directly — bypasses Raft, can lag a follower)</span></div>
      <input type="text" class="get-key-input" placeholder="key">
      <div class="node-actions">
        <button onclick="doGet(${n.id}, this)">Get</button>
      </div>
      <div class="get-result" id="get-result-${n.id}"></div>
    </div>

    <div class="node-actions">
      <button onclick="doAction('crash', ${n.id})" title="kill -9: stops ticking, drops in-flight messages, kills its sidecar too">Crash</button>
      <button onclick="doAction('restart', ${n.id})" title="respawns its sidecar over the same directory, then rebuilds the Raft driver">Restart</button>
      <button onclick="doAction('isolate', ${n.id})" title="partition: keeps ticking, but no message crosses">Partition</button>
      <button onclick="doAction('heal', ${n.id})">Heal</button>
    </div>
  `;
  return div;
}

function renderTraceRow(ev) {
  return `<div class="trace-row"><span class="trace-seq">#${ev.seq}</span><span class="trace-type">${ev.type}</span>${ev.from} → ${ev.to}<span class="trace-detail">${escapeHtml(ev.detail)}</span></div>`;
}

async function doPropose(id, btn, op) {
  const card = btn.closest(".node-card");
  const key = card.querySelector(".key-input").value;
  const value = card.querySelector(".value-input").value;
  if (!key) {
    alert("key is required");
    return;
  }
  const data = await post("/propose", { node: id, op, key, value });
  if (data.error) {
    alert(data.error);
    return;
  }
  refresh();
}

async function doGet(id, btn) {
  const card = btn.closest(".node-card");
  const key = card.querySelector(".get-key-input").value;
  const resultEl = document.getElementById("get-result-" + id);
  if (!key) {
    resultEl.textContent = "key is required";
    return;
  }
  resultEl.textContent = "…";
  const data = await post("/get", { node: id, key });
  if (data.error) {
    resultEl.innerHTML = `<span class="get-error">${escapeHtml(data.error)}</span>`;
    return;
  }
  resultEl.textContent = data.found ? `"${data.value}"` : "(absent)";
}

async function doAction(action, id) {
  await post(`/${action}`, { node: id });
  refresh();
}

async function doTick(n) {
  await fetch(`${API}/tick?n=${n}`, { method: "POST" });
  refresh();
}

async function doTickRandom() {
  const n = 5 + Math.floor(Math.random() * 196); // random in [5, 200]
  const info = document.getElementById("tick-random-info");
  if (info) info.textContent = `→ ticked ×${n}`;
  await doTick(n);
}

async function doReset() {
  const nodes = parseInt(document.getElementById("cfg-nodes").value, 10) || 3;
  const threshold = parseInt(document.getElementById("cfg-threshold").value, 10) || 0;
  await post("/reset", { nodes, snapshot_threshold: threshold });
  refresh();
}

document.addEventListener("DOMContentLoaded", refresh);
