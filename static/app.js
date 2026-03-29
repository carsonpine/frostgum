(function () {
  const API = "";
  const PAGE_SIZE = 50;

  let currentProgram = null;
  let currentIxName = null;
  let currentIxOffset = 0;
  let currentAcctType = null;
  let currentAcctOffset = 0;
  let currentPage = "home";

  async function fetchJson(path, opts) {
    const res = await fetch(API + path, opts);
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || res.statusText);
    return data;
  }

  function setStatus(state) {
    const badge = document.getElementById("status-badge");
    badge.className = "badge badge-" + state;
    const labels = { ok: "online", err: "error", loading: "connecting" };
    badge.textContent = labels[state] || state;
  }

  function escHtml(str) {
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function renderTable(rows) {
    if (!rows || rows.length === 0) return "";
    const keys = Object.keys(rows[0]);
    const thead = `<tr>${keys.map((k) => `<th>${escHtml(k)}</th>`).join("")}</tr>`;
    const tbody = rows
      .map(
        (row) =>
          `<tr>${keys
            .map((k) => {
              const v = row[k] === null || row[k] === undefined ? "" : String(row[k]);
              return `<td title="${escHtml(v)}">${escHtml(v)}</td>`;
            })
            .join("")}</tr>`
      )
      .join("");
    return `<div class="table-wrapper"><table><thead>${thead}</thead><tbody>${tbody}</tbody></table></div>`;
  }

  async function updateMeta() {
    try {
      const meta = await fetchJson("/api/meta");
      document.getElementById("nav-slot").textContent = Number(meta.current_slot).toLocaleString();
      document.getElementById("nav-tables").textContent = meta.table_count;
    } catch (_) {}
  }

  async function refreshStats() {
    if (!currentProgram) return;
    try {
      const stats = await fetchJson(`/programs/${currentProgram}/stats`);
      const counts = stats.instruction_counts || {};
      const grid = document.getElementById("stats-grid");
      const entries = Object.entries(counts);
      if (entries.length > 0) {
        grid.innerHTML = entries
          .map(
            ([name, count]) => `
            <div class="stat-card">
              <div class="stat-name">${escHtml(name)}</div>
              <div class="stat-value">${Number(count).toLocaleString()}</div>
            </div>`
          )
          .join("");
        document.getElementById("stats-section").classList.remove("hidden");
      }
    } catch (_) {}
  }

  function startPolling() {
    setInterval(async () => {
      await updateMeta();
      if (currentPage === "home") await refreshStats();
    }, 5000);
  }

  async function init() {
    try {
      await fetchJson("/health");
      setStatus("ok");
    } catch {
      setStatus("err");
    }

    await updateMeta();

    try {
      const { programs } = await fetchJson("/programs");
      if (programs.length > 0) {
        currentProgram = programs[0].program_id;
        renderProgramInfo(programs[0]);
        await loadStats();
        await loadInstructions();
        await loadAccounts();
        document.getElementById("main-tabs-section").classList.remove("hidden");
      }
    } catch (e) {
      console.error("init failed:", e);
    }

    await loadDbTables();
    startPolling();
    showLoader(false);
  }

  function showLoader(show) {
    const loader = document.getElementById("loader");
    const app = document.getElementById("app");
    if (show) {
      loader.classList.remove("fade-out");
      app.classList.add("hidden");
    } else {
      loader.classList.add("fade-out");
      app.classList.remove("hidden");
      setTimeout(() => loader.style.display = "none", 450);
    }
  }

  function renderProgramInfo(program) {
    document.getElementById("program-name").textContent = program.name || "—";
    document.getElementById("program-id").textContent = program.program_id;
    document.getElementById("program-info").classList.remove("hidden");
  }

  async function loadStats() {
    if (!currentProgram) return;
    try {
      const stats = await fetchJson(`/programs/${currentProgram}/stats`);
      const counts = stats.instruction_counts || {};
      const grid = document.getElementById("stats-grid");
      const entries = Object.entries(counts);
      if (entries.length === 0) return;
      grid.innerHTML = entries
        .map(
          ([name, count]) => `
          <div class="stat-card">
            <div class="stat-name">${escHtml(name)}</div>
            <div class="stat-value">${Number(count).toLocaleString()}</div>
          </div>`
        )
        .join("");
      document.getElementById("stats-section").classList.remove("hidden");
    } catch (_) {}
  }

  async function loadInstructions() {
    if (!currentProgram) return;
    try {
      const { instructions } = await fetchJson(`/programs/${currentProgram}/instructions`);
      if (!instructions || instructions.length === 0) return;

      const nav = document.getElementById("instructions-nav");
      nav.innerHTML = instructions
        .map(
          (ix, i) =>
            `<button class="sub-tab-btn${i === 0 ? " active" : ""}" data-ix="${escHtml(ix.name)}">${escHtml(ix.name)}</button>`
        )
        .join("");

      nav.addEventListener("click", async (e) => {
        const btn = e.target.closest(".sub-tab-btn");
        if (!btn) return;
        nav.querySelectorAll(".sub-tab-btn").forEach((b) => b.classList.remove("active"));
        btn.classList.add("active");
        currentIxName = btn.dataset.ix;
        currentIxOffset = 0;
        await loadIxData();
      });

      currentIxName = instructions[0].name;
      currentIxOffset = 0;
      await loadIxData();
    } catch (e) {
      console.error("instructions load failed:", e);
    }
  }

  async function loadIxData(append = false) {
    if (!currentProgram || !currentIxName) return;
    const content = document.getElementById("instructions-content");
    try {
      const data = await fetchJson(
        `/programs/${currentProgram}/instructions/${currentIxName}?limit=${PAGE_SIZE}&offset=${currentIxOffset}&order=desc`
      );
      const rows = data.data || [];

      if (!append) {
        if (rows.length === 0) {
          content.innerHTML = `<div class="empty-state">No data for <code>${escHtml(currentIxName)}</code></div>`;
          return;
        }
        content.innerHTML = renderTable(rows);
        if (rows.length === PAGE_SIZE) appendMoreBtn(content, "ix-more-btn", async () => {
          currentIxOffset += PAGE_SIZE;
          await loadIxData(true);
        });
      } else {
        const btn = document.getElementById("ix-more-btn");
        if (btn) btn.remove();
        if (rows.length > 0) {
          appendRowsToTable(content, rows);
          if (rows.length === PAGE_SIZE) appendMoreBtn(content, "ix-more-btn", async () => {
            currentIxOffset += PAGE_SIZE;
            await loadIxData(true);
          });
        }
      }
    } catch (e) {
      content.innerHTML = `<div class="error-msg">${escHtml(e.message)}</div>`;
    }
  }

  async function loadAccounts() {
    if (!currentProgram) return;
    try {
      const { account_types } = await fetchJson(`/programs/${currentProgram}/accounts`);
      if (!account_types || account_types.length === 0) return;

      const nav = document.getElementById("accounts-nav");
      nav.innerHTML = account_types
        .map(
          (a, i) =>
            `<button class="sub-tab-btn${i === 0 ? " active" : ""}" data-type="${escHtml(a.name)}">${escHtml(a.name)}</button>`
        )
        .join("");

      nav.addEventListener("click", async (e) => {
        const btn = e.target.closest(".sub-tab-btn");
        if (!btn) return;
        nav.querySelectorAll(".sub-tab-btn").forEach((b) => b.classList.remove("active"));
        btn.classList.add("active");
        currentAcctType = btn.dataset.type;
        currentAcctOffset = 0;
        await loadAcctData();
      });

      currentAcctType = account_types[0].name;
      currentAcctOffset = 0;
      await loadAcctData();
    } catch (e) {
      console.error("accounts load failed:", e);
    }
  }

  async function loadAcctData(append = false) {
    if (!currentProgram || !currentAcctType) return;
    const content = document.getElementById("accounts-content");
    try {
      const data = await fetchJson(
        `/programs/${currentProgram}/accounts/${currentAcctType}?limit=${PAGE_SIZE}&offset=${currentAcctOffset}`
      );
      const rows = data.data || [];

      if (!append) {
        if (rows.length === 0) {
          content.innerHTML = `<div class="empty-state">No accounts of type <code>${escHtml(currentAcctType)}</code></div>`;
          return;
        }
        content.innerHTML = renderTable(rows);
        if (rows.length === PAGE_SIZE) appendMoreBtn(content, "acct-more-btn", async () => {
          currentAcctOffset += PAGE_SIZE;
          await loadAcctData(true);
        });
      } else {
        const btn = document.getElementById("acct-more-btn");
        if (btn) btn.remove();
        if (rows.length > 0) {
          appendRowsToTable(content, rows);
          if (rows.length === PAGE_SIZE) appendMoreBtn(content, "acct-more-btn", async () => {
            currentAcctOffset += PAGE_SIZE;
            await loadAcctData(true);
          });
        }
      }
    } catch (e) {
      content.innerHTML = `<div class="error-msg">${escHtml(e.message)}</div>`;
    }
  }

  function appendMoreBtn(container, id, handler) {
    const btn = document.createElement("button");
    btn.className = "load-more-btn";
    btn.id = id;
    btn.textContent = "Load more";
    btn.addEventListener("click", handler);
    container.appendChild(btn);
  }

  function appendRowsToTable(container, rows) {
    const wrapper = container.querySelector(".table-wrapper");
    if (!wrapper) return;
    const tbody = wrapper.querySelector("tbody");
    if (!tbody) return;
    const keys = Object.keys(rows[0]);
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      tr.innerHTML = keys
        .map((k) => {
          const v = row[k] === null || row[k] === undefined ? "" : String(row[k]);
          return `<td title="${escHtml(v)}">${escHtml(v)}</td>`;
        })
        .join("");
      tbody.appendChild(tr);
    });
  }

  async function loadDbTables() {
    try {
      const data = await fetchJson("/api/sql", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sql: "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND (table_name LIKE 'ix_%' OR table_name LIKE 'acct_%') ORDER BY table_name"
        }),
      });
      const rows = data.data || [];
      const container = document.getElementById("db-tables-list");
      if (rows.length === 0) {
        container.innerHTML = `<div class="empty-state">No tables yet</div>`;
        return;
      }
      container.innerHTML = rows
        .map((r) => {
          const name = r.table_name;
          const type = name.startsWith("ix_") ? "instruction" : "account";
          return `<div class="db-table-item" data-table="${escHtml(name)}">
            <div class="db-table-name">${escHtml(name)}</div>
            <div class="db-table-type">${type}</div>
          </div>`;
        })
        .join("");

      container.addEventListener("click", (e) => {
        const item = e.target.closest(".db-table-item");
        if (!item) return;
        const table = item.dataset.table;
        const sqlInput = document.getElementById("sql-input");
        sqlInput.value = `SELECT * FROM ${table} ORDER BY id DESC LIMIT 50`;
        document.getElementById("sql-run-btn").click();
      });
    } catch (_) {}
  }

  document.getElementById("sql-run-btn").addEventListener("click", async () => {
    const sql = document.getElementById("sql-input").value.trim();
    const result = document.getElementById("sql-result");
    if (!sql) return;
    result.innerHTML = `<div class="empty-state">running…</div>`;
    try {
      const data = await fetchJson("/api/sql", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql }),
      });
      if (data.data && data.data.length > 0) {
        result.innerHTML =
          renderTable(data.data) +
          `<div class="empty-state" style="padding:0.5rem 0">${data.count} row(s)</div>`;
      } else {
        result.innerHTML = `<div class="empty-state">0 rows returned</div>`;
      }
    } catch (e) {
      result.innerHTML = `<div class="error-msg">${escHtml(e.message)}</div>`;
    }
  });

  document.querySelectorAll(".nav-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const page = btn.dataset.page;
      currentPage = page;
      document.querySelectorAll(".nav-btn").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      document.querySelectorAll(".page").forEach((p) => p.classList.add("hidden"));
      document.getElementById(`page-${page}`).classList.remove("hidden");
    });
  });

  document.querySelectorAll(".page-tab-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const tab = btn.dataset.tab;
      document.querySelectorAll(".page-tab-btn").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      document.querySelectorAll(".tab-pane").forEach((p) => p.classList.add("hidden"));
      document.getElementById(`tab-${tab}`).classList.remove("hidden");
    });
  });

  init();
})();
