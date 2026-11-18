"use strict";

const STATE_LABELS = {
  pending: "等待中",
  running: "采集中",
  paused: "已暂停",
  done: "已完成",
  error: "出错",
};

const demoApiUrl = window.DEMO_API_URL || "";
const demoControlUrl = window.DEMO_CONTROL_URL || "";

const jobsEl = document.getElementById("jobs");
const emptyEl = document.getElementById("empty");
const summaryEl = document.getElementById("job-summary");
const template = document.getElementById("job-template");
const form = document.getElementById("create-form");
const urlInput = document.getElementById("url");
const createBtn = document.getElementById("create-btn");
const createError = document.getElementById("create-error");

/** jobId -> { el, nodes, snapshot } */
const cards = new Map();
let timer = null;

function api(path, options = {}) {
  return fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  }).then(async (res) => {
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `请求失败 (${res.status})`);
    return data;
  });
}

function fmtTime(seconds) {
  if (!seconds) return "";
  const d = new Date(seconds * 1000);
  return d.toLocaleTimeString("zh-CN", { hour12: false });
}

function buildCard(job) {
  const el = template.content.firstElementChild.cloneNode(true);
  const nodes = {
    stateText: el.querySelector(".state-text"),
    url: el.querySelector(".job-url"),
    pause: el.querySelector(".pause"),
    resume: el.querySelector(".resume"),
    download: el.querySelector(".download"),
    del: el.querySelector(".delete"),
    bar: el.querySelector(".bar"),
    recs: el.querySelector(".rec-count"),
    pages: el.querySelector(".pages"),
    dup: el.querySelector(".dup"),
    cursor: el.querySelector(".cursor"),
    stateLabel: el.querySelector(".state-label"),
    epoch: el.querySelector(".epoch"),
    rounds: el.querySelector(".rounds-line"),
    notice: el.querySelector(".notice"),
    log: el.querySelector(".log"),
  };
  nodes.url.textContent = job.source_url;
  nodes.url.title = job.source_url;
  nodes.download.href = `/api/jobs/${job.id}/download`;
  nodes.download.setAttribute("download", "");

  nodes.pause.addEventListener("click", () => act(job.id, "pause"));
  nodes.resume.addEventListener("click", () => act(job.id, "resume"));
  nodes.del.addEventListener("click", () => removeJob(job.id, job.source_url));

  const card = { el, nodes, snapshot: job };
  cards.set(job.id, card);
  jobsEl.appendChild(el);
  renderCard(card, job, 0);
  return card;
}

async function act(jobId, action) {
  const card = cards.get(jobId);
  if (!card) return;
  if (action === "pause") card.nodes.pause.disabled = true;
  if (action === "resume") card.nodes.resume.disabled = true;
  try {
    await api(`/api/jobs/${jobId}/${action}`, { method: "POST" });
  } catch (err) {
    showToast(err.message);
  }
  await refresh();
}

async function removeJob(jobId, url) {
  if (!window.confirm(`删除任务及已采集数据？\n${url}`)) return;
  try {
    await api(`/api/jobs/${jobId}`, { method: "DELETE" });
    const card = cards.get(jobId);
    if (card) card.el.remove();
    cards.delete(jobId);
    updateCounts();
  } catch (err) {
    showToast(err.message);
  }
}

function renderCard(card, job, recordCount) {
  const previous = card.snapshot || {};
  card.snapshot = job;
  card.el.dataset.state = job.state;

  card.nodes.stateText.textContent = STATE_LABELS[job.state] || job.state;
  card.nodes.stateLabel.textContent = STATE_LABELS[job.state] || job.state;
  card.nodes.recs.textContent = recordCount;
  card.nodes.pages.textContent = job.pages_done;
  card.nodes.dup.textContent = job.duplicates;
  let cursorLabel;
  if (job.next_cursor) cursorLabel = job.next_cursor;
  else if (job.state === "done") cursorLabel = "（结束）";
  else if (job.pages_done === 0) cursorLabel = "（首页）";
  else cursorLabel = "—";
  card.nodes.cursor.textContent = cursorLabel;
  if (job.run_epoch !== undefined) {
    card.nodes.epoch.textContent = `第 ${job.run_epoch} 轮`;
  }

  // Superseded rounds may finish their one in-flight request; show them so the
  // interleaving is visible (they must not issue further requests).
  const rounds = job.rounds || [];
  const extra = rounds.filter((r) => !r.current);
  if (extra.length) {
    const parts = extra.map(
      (r) => `旧轮次 #${r.epoch}${r.inflight ? "（在途响应收尾）" : "（退出中）"}`
    );
    card.nodes.rounds.hidden = false;
    card.nodes.rounds.textContent =
      `当前轮次 #${job.run_epoch}；${parts.join("、")}——旧轮次不再发请求、不写记录、不改状态`;
  } else {
    card.nodes.rounds.hidden = true;
  }

  const busy = job.state === "running" || job.state === "pending";
  card.nodes.pause.hidden = !busy;
  card.nodes.pause.disabled = false;
  card.nodes.resume.hidden = busy || job.state === "done";
  card.nodes.resume.disabled = false;

  // Progress: pages are open-ended, so use a soft asymptote plus a completed
  // state at 100%.  The bar communicates liveness, the numbers are truth.
  let pct = 0;
  if (job.state === "done") pct = 100;
  else if (job.pages_done > 0) pct = Math.min(92, 8 + job.pages_done * 9);
  else if (job.state === "running") pct = 4;
  card.nodes.bar.style.width = `${pct}%`;

  const notices = [];
  if (job.state === "paused") {
    notices.push({
      text: "已暂停：在途请求的结果会被丢弃，不会写入记录或推进游标。继续后将接续保存的游标。",
      error: false,
    });
  }
  if (extra.length) {
    notices.push({
      text: `继续已生效（当前第 ${job.run_epoch} 轮）。旧轮次只等已发请求结束，不再发请求、不写记录、不改状态；它的迟到响应与 429 等待不会影响本轮。`,
      error: false,
    });
  }
  if (job.state === "running" && job.retry_after_at) {
    // Use the server clock; browser clock may be skewed.
    const now = job.server_time || Date.now() / 1000;
    const secs = Math.max(0, Math.ceil(job.retry_after_at - now));
    notices.push({ text: `429 限流：正在按 Retry-After 等待，约 ${secs}s 后重试（仍可暂停）`, error: false });
  }
  if (job.state === "error" && job.last_error) {
    notices.push({ text: job.last_error, error: true });
  }
  if (notices.length) {
    const notice = notices[0];
    card.nodes.notice.hidden = false;
    card.nodes.notice.textContent = notice.text;
    card.nodes.notice.classList.toggle("error", notice.error);
  } else {
    card.nodes.notice.hidden = true;
  }

  // Only re-render log if it changed, to keep scrolling stable.
  const logKey = (job.events || []).map((e) => e.message).join("|");
  if (logKey !== card.logKey) {
    card.logKey = logKey;
    card.nodes.log.innerHTML = "";
    for (const event of job.events || []) {
      const li = document.createElement("li");
      li.className = event.level;
      const t = document.createElement("span");
      t.className = "t";
      t.textContent = fmtTime(event.at);
      li.appendChild(t);
      li.appendChild(document.createTextNode(event.message));
      card.nodes.log.appendChild(li);
    }
    card.nodes.log.scrollTop = card.nodes.log.scrollHeight;
  }

  // Flash numbers that changed.
  if (previous.records_seen !== undefined && job.records_seen !== previous.records_seen) {
    card.nodes.recs.animate(
      [{ color: "#58a6ff" }, { color: "" }],
      { duration: 700, easing: "ease-out" }
    );
  }
}

async function refresh() {
  let jobs;
  try {
    jobs = await api("/api/jobs");
  } catch (err) {
    summaryEl.textContent = "连接中断，重试中…";
    schedule();
    return;
  }

  const seen = new Set();
  for (const job of jobs) {
    seen.add(job.id);
    let card = cards.get(job.id);
    if (!card) card = buildCard(job);
    // Keep DOM order equal to the API's (newest first) order.
    jobsEl.appendChild(card.el);
    try {
      const detail = await api(`/api/jobs/${job.id}`);
      renderCard(card, detail, detail.record_count);
    } catch (err) {
      renderCard(card, job, card.snapshot.record_count || 0);
    }
  }
  for (const [id, card] of cards) {
    if (!seen.has(id)) {
      card.el.remove();
      cards.delete(id);
    }
  }
  updateCounts();
  schedule();
}

function updateCounts() {
  emptyEl.hidden = cards.size > 0;
  const running = [...cards.values()].filter((c) => c.snapshot.state === "running").length;
  const done = [...cards.values()].filter((c) => c.snapshot.state === "done").length;
  let text = `${cards.size} 个任务`;
  if (running) text += ` · ${running} 个进行中`;
  if (done) text += ` · ${done} 个已完成`;
  summaryEl.textContent = cards.size ? text : "—";
}

function schedule() {
  clearTimeout(timer);
  const fast = [...cards.values()].some(
    (c) => c.snapshot.state === "running" || c.snapshot.state === "pending"
  );
  timer = setTimeout(refresh, fast ? 700 : 2500);
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const url = urlInput.value.trim();
  if (!url) return;
  createError.hidden = true;
  createBtn.disabled = true;
  createBtn.textContent = "启动中…";
  try {
    await api("/api/jobs", { method: "POST", body: JSON.stringify({ url }) });
    clearTimeout(timer);
    await refresh();
  } catch (err) {
    createError.textContent = err.message;
    createError.hidden = false;
  } finally {
    createBtn.disabled = false;
    createBtn.textContent = "启动任务";
  }
});

/* ----------------------------- demo controls ---------------------------- */
const demoBar = document.querySelector(".demo-bar");

async function demoCall(path, body) {
  const res = await fetch(`${demoControlUrl}${path}`, {
    method: body === undefined ? "GET" : "POST",
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`演示源返回 ${res.status}`);
  return res.json();
}

function renderDemoState(config) {
  const parts = [];
  if (config.slow_ms > 0) parts.push(`慢响应 ${config.slow_ms}ms`);
  if (config.duplicates) parts.push("重复记录已开启");
  if (config.rate_limit_every > 0) parts.push(`每 ${config.rate_limit_every} 次请求 429（Retry-After ${config.retry_after}s）`);
  document.getElementById("demo-state").textContent =
    parts.length ? `当前：${parts.join(" · ")}` : "当前：无故障注入";

  const chips = {
    slow: config.slow_ms > 0,
    duplicates: !!config.duplicates,
    ratelimit: config.rate_limit_every > 0,
  };
  document.querySelectorAll(".chip[data-demo]").forEach((chip) => {
    const key = chip.dataset.demo;
    chip.classList.toggle("active", !!chips[key]);
  });
}

if (demoBar && demoControlUrl) {
  const interleaveBtn = document.getElementById("demo-interleave");
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  async function waitForInflight(timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const stats = await demoCall("/stats");
      if (stats.log.some((entry) => !entry.finished)) return true;
      await sleep(120);
    }
    return false;
  }

  if (interleaveBtn) {
    interleaveBtn.addEventListener("click", async () => {
      interleaveBtn.disabled = true;
      try {
        // Deterministic interleaving: a slow request is in flight when the
        // operator pauses and immediately resumes.
        await demoCall("/reset", {});
        await demoCall("/config", { slow_ms: 2000, rate_limit_every: 0 });
        const job = await api("/api/jobs", {
          method: "POST",
          body: JSON.stringify({ url: demoApiUrl }),
        });
        if (!(await waitForInflight(5000))) {
          throw new Error("没有观察到在途请求");
        }
        await api(`/api/jobs/${job.id}/pause`, { method: "POST" });
        await api(`/api/jobs/${job.id}/resume`, { method: "POST" });
        showToast("已暂停并立即继续：旧轮次只剩在途请求收尾，迟到响应会被丢弃");
        clearTimeout(timer);
        await refresh();
        // Drop the artificial latency so the resumed round finishes quickly.
        await sleep(2200);
        await demoCall("/config", { slow_ms: 0 });
      } catch (err) {
        showToast(`交错演示失败：${err.message}`);
      } finally {
        interleaveBtn.disabled = false;
      }
    });
  }

  demoBar.addEventListener("click", async (event) => {
    const chip = event.target.closest(".chip[data-demo]");
    if (!chip) return;
    const kind = chip.dataset.demo;
    try {
      if (kind === "reset") {
        const config = await demoCall("/reset", {});
        renderDemoState({ slow_ms: 0, duplicates: false, rate_limit_every: 0, retry_after: config.retry_after });
        return;
      }
      const current = await demoCall("/config");
      let patch;
      if (kind === "slow") patch = { slow_ms: current.slow_ms > 0 ? 0 : 1500 };
      if (kind === "duplicates") patch = { duplicates: !current.duplicates };
      if (kind === "ratelimit") {
        patch = { rate_limit_every: current.rate_limit_every > 0 ? 0 : 3, retry_after: 3 };
      }
      renderDemoState(await demoCall("/config", patch));
    } catch (err) {
      showToast(`演示源不可用：${err.message}`);
    }
  });

  fetch(`${demoControlUrl}/config`)
    .then((r) => r.json())
    .then(renderDemoState)
    .catch(() => {
      document.getElementById("demo-state").textContent = "演示源未连接（不影响其他接口地址）";
    });
}

/* -------------------------------- toast -------------------------------- */
let toastEl = null;
function showToast(message) {
  if (!toastEl) {
    toastEl = document.createElement("div");
    toastEl.className = "toast";
    document.body.appendChild(toastEl);
  }
  toastEl.textContent = message;
  toastEl.classList.add("show");
  clearTimeout(toastEl._t);
  toastEl._t = setTimeout(() => toastEl.classList.remove("show"), 3200);
}

refresh();
