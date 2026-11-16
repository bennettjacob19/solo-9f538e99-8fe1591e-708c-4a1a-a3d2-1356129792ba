/* Web UI for the directory-migration gateway. Vanilla JS, no dependencies. */

const $ = (id) => document.getElementById(id);

async function api(path, options = {}) {
  const resp = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  let body = null;
  try { body = await resp.json(); } catch { body = {}; }
  return { status: resp.status, body };
}

function toast(message, ok = true) {
  const box = $("toast");
  const item = document.createElement("div");
  item.className = `toast-item ${ok ? "ok" : "err"}`;
  item.textContent = message;
  box.appendChild(item);
  setTimeout(() => item.remove(), 6000);
}

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => (
    { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]
  ));
}

// ---- current config ------------------------------------------------------
async function refreshConfig() {
  const { body } = await api("/api/config");
  const rows = [
    ["配置版号", body.version],
    ["当前上游 URL", body.upstream_url],
    ["默认制品", body.artifact],
    ["最近切换时间", body.updated_at ? new Date(body.updated_at * 1000).toLocaleString() : "—"],
  ];
  $("configView").innerHTML = rows.map(([k, v]) =>
    `<div class="k">${esc(k)}</div><div><b>${esc(v)}</b></div>`).join("");
  // Keep the switch form's version field in sync (only when not focused).
  const versionInput = $("swVersion");
  if (document.activeElement !== versionInput) versionInput.value = body.version;
}

// ---- inflight ------------------------------------------------------------
async function refreshInflight() {
  const { body } = await api("/api/inflight");
  const jobs = body.inflight || [];
  if (!jobs.length) {
    $("inflightView").innerHTML =
      `<span class="muted">${body.workers} 个 worker 均空闲</span>`;
    return;
  }
  $("inflightView").innerHTML =
    `<table><thead><tr><th>Job</th><th>Worker</th><th>绑定 URL（受理时刻）</th>
     <th>制品</th><th>已耗时(s)</th></tr></thead><tbody>` +
    jobs.map((j) => `<tr>
      <td>#${j.job_id}</td><td>${esc(j.worker)}</td>
      <td>${esc(j.bound_url)} <span class="muted">(v${j.config_version})</span></td>
      <td>${esc(j.artifact)}</td><td>${j.age_seconds}</td></tr>`).join("") +
    "</tbody></table>";
}

// ---- breakers ------------------------------------------------------------
async function refreshBreakers() {
  const { body } = await api("/api/breakers");
  const breakers = body.breakers || [];
  if (!breakers.length) {
    $("breakerView").innerHTML = `<span class="muted">尚无熔断记录</span>`;
    return;
  }
  $("breakerView").innerHTML =
    `<table><thead><tr>
      <th>URL</th><th>状态</th><th>连续失败</th><th>累计请求/失败/拒绝</th>
      <th>最近事件</th><th>事件时间</th><th>Era / Generation</th>
    </tr></thead><tbody>` +
    breakers.map((b) => `<tr>
      <td>${esc(b.url)}</td>
      <td><span class="chip ${b.state}">${b.state}</span></td>
      <td>${b.consecutive_failures} / ${body.threshold}</td>
      <td>${b.requests} / ${b.failures} / ${b.rejected}</td>
      <td>${esc(b.last_event)}</td>
      <td>${b.last_event_at ? new Date(b.last_event_at * 1000).toLocaleTimeString() : "—"}</td>
      <td>${b.era} / ${b.generation}</td>
    </tr>`).join("") + "</tbody></table>" +
    `<p class="muted" style="margin-top:8px">冷却 ${body.cooldown}s；
     每个 URL 独立计数，切换/切回都不会清零。</p>`;
}

// ---- directory panels ----------------------------------------------------
async function refreshDirectories() {
  const { body } = await api("/api/directories");
  const panels = Object.entries(body).map(([name, item]) => {
    const state = item.state || {};
    const stats = item.stats || {};
    const modeBadge = {
      normal: '<span class="chip CLOSED">normal</span>',
      delay: '<span class="chip HALF_OPEN">delay</span>',
      "503": '<span class="chip OPEN">503</span>',
    }[state.mode] || `<span class="chip">${esc(state.mode ?? "未知")}</span>`;
    return `<div class="dirbox">
      <h3>目录实例 ${name.toUpperCase()}
        <span class="muted">${esc(item.url)}</span></h3>
      <div class="kv">
        <div class="k">目录标识</div><div><b>${esc(state.directory_id ?? "—")}</b></div>
        <div class="k">当前模式</div><div>${modeBadge}</div>
        <div class="k">延迟设置</div><div>${esc(state.delay ?? "—")} s</div>
        <div class="k">实际访问次数</div><div>${esc(stats.hits ?? "—")}</div>
      </div>
      <div class="row" style="margin-top:10px">
        <div class="narrow">
          <label>模式</label>
          <select id="mode-${name}">
            <option value="normal" ${state.mode === "normal" ? "selected" : ""}>normal</option>
            <option value="delay" ${state.mode === "delay" ? "selected" : ""}>delay</option>
            <option value="503" ${state.mode === "503" ? "selected" : ""}>503</option>
          </select>
        </div>
        <div class="narrow">
          <label>延迟秒数</label>
          <input id="delay-${name}" type="number" step="0.1" min="0" value="${state.delay ?? 2}">
        </div>
        <div class="narrow" style="align-self:end">
          <button data-dir="${name}" class="apply-mode">应用模式</button>
        </div>
        <div class="narrow" style="align-self:end">
          <button data-dir="${name}" class="reset-stats secondary">清零计数</button>
        </div>
      </div>
    </div>`;
  });
  $("dirPanels").innerHTML = panels.join("");
  document.querySelectorAll(".apply-mode").forEach((btn) => btn.addEventListener("click", async () => {
    const name = btn.dataset.dir;
    const { status, body: res } = await api("/api/directories/control", {
      method: "POST",
      body: JSON.stringify({
        target: name, action: "mode",
        mode: $(`mode-${name}`).value,
        delay: parseFloat($(`delay-${name}`).value) || 0,
      }),
    });
    toast(`实例 ${name.toUpperCase()} 模式更新: ${JSON.stringify(res)}`, status === 200);
    refreshDirectories();
  }));
  document.querySelectorAll(".reset-stats").forEach((btn) => btn.addEventListener("click", async () => {
    const name = btn.dataset.dir;
    await api("/api/directories/control", {
      method: "POST", body: JSON.stringify({ target: name, action: "stats-reset" }),
    });
    toast(`实例 ${name.toUpperCase()} 访问计数已清零`);
    refreshDirectories();
  }));
}

// ---- switch form ---------------------------------------------------------
$("btnPreflight").addEventListener("click", async () => {
  const { status, body } = await api("/api/preflight", {
    method: "POST",
    body: JSON.stringify({
      url: $("swUrl").value.trim(),
      expected_id: $("swId").value.trim(),
      artifact: $("swArtifact").value.trim() || undefined,
    }),
  });
  toast(`预检 ${body.ok ? "成功" : "失败"}: ${JSON.stringify(body)}`, body.ok && status === 200);
});

$("btnSwitch").addEventListener("click", async () => {
  const btn = $("btnSwitch");
  btn.disabled = true;
  try {
    const { status, body } = await api("/api/switch", {
      method: "POST",
      body: JSON.stringify({
        url: $("swUrl").value.trim(),
        expected_id: $("swId").value.trim(),
        version: parseInt($("swVersion").value, 10),
        artifact: $("swArtifact").value.trim() || undefined,
      }),
    });
    if (body.ok) {
      toast(`切换成功：版号 ${body.config.version}，上游 ${body.config.upstream_url}`);
    } else {
      toast(`切换被拒绝（配置未变）: ${body.error} ${body.reason ?? ""}`, false);
    }
  } finally {
    btn.disabled = false;
    refreshConfig();
    refreshBreakers();
  }
});

// ---- queries -------------------------------------------------------------
$("btnQuery").addEventListener("click", async () => {
  const artifact = $("qArtifact").value.trim() || "example-app.json";
  const n = Math.max(1, Math.min(20, parseInt($("qConcurrency").value, 10) || 1));
  const started = performance.now();
  const results = await Promise.all(Array.from({ length: n }, () =>
    api(`/api/query?artifact=${encodeURIComponent(artifact)}`)));
  const elapsed = ((performance.now() - started) / 1000).toFixed(3);
  $("queryResults").innerHTML =
    `<p class="muted">${n} 次查询完成，墙钟 ${elapsed}s</p>` +
    results.map(({ status, body: r }, i) => {
      const ok = status === 200;
      return `<details ${i === 0 ? "open" : ""} style="margin-bottom:6px">
        <summary class="${ok ? "ok" : "err"}">
          #${i + 1} → HTTP ${status} · worker ${esc(r.worker)} ·
          ${esc(r.upstream_url)} · 目录标识 ${esc(r.directory_id ?? "—")}
          ${r.stale_result_ignored ? '· <span class="warn">stale 结果已忽略</span>' : ""}
        </summary>
        <pre>${esc(JSON.stringify(r, null, 2))}</pre>
      </details>`;
    }).join("");
  refreshBreakers();
  refreshInflight();
  refreshDirectories();
});

// ---- reset breakers ------------------------------------------------------
$("btnResetBreakers").addEventListener("click", async () => {
  await api("/api/breakers/reset", { method: "POST" });
  toast("熔断状态已重置（各 URL 记录保留，计数清零）");
  refreshBreakers();
});

// ---- periodic refresh ----------------------------------------------------
async function refreshAll() {
  await Promise.all([refreshConfig(), refreshInflight(),
                     refreshBreakers(), refreshDirectories()]);
}
refreshAll();
setInterval(refreshAll, 2000);
