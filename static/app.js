(function () {
  const API = "";

  let currentProgram = null;
  let currentIxName = null;
  let currentIxOffset = 0;
  let currentAcctType = null;
  let currentAcctOffset = 0;
  const PAGE_SIZE = 50;

  async function fetchJson(path, opts) {
    const res = await fetch(API + path, opts);
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || res.statusText);
    return data;
  }

  function setStatus(state) {
    const badge = document.getElementById("status-badge");
    badge.className = "badge badge-" + state;
    badge.textContent = state === "ok" ? "online" : state === "err" ? "error" : "connecting...";
  }

  async function init() {
    try {
      await fetchJson("/health");
      setStatus("ok");
    } catch {
      setStatus("err");
    }

    try {
      const { programs } = await fetchJson("/programs");
      if (programs.length > 0) {
        currentProgram = programs[0].program_id;
        renderProgramInfo(programs[0]);
        await loadStats();
        await loadInstructions();
        await loadAccounts();
      }
    } catch (e) {
      console.error("init failed:", e);
    }
  }

  function renderProgramInfo(program) {
    const section = document.getElementById("program-info");
    const details = document.getElementById("program-details");
    details.innerHTML = `
      <div class="program-name">${escHtml(program.name || "—")}</div>
      <div class="program-id-display">${escHtml(program.program_id)}</div>
    `;
    section.classList.remove("hidden");
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
        .map(([name, count]) => `
          <div class="stat-card">
            <div class="stat-name">${escHtml(name)}</div>
            <div class="stat-value">${Number(count).toLocaleString()}</div>
          </div>
        `)
        .join("");
      document.getElementById("stats-section").classList.remove("hidden");
    } catch (e) {
      console.error("stats failed:", e);
    }
  }

  async function loadInstructions() {
    if (!currentProgram) return;
    try {
      const { instructions } = await fetchJson(`/programs/${currentProgram}/instructions`);
      if (!instructions || instructions.length === 0) return;

      const nav = document.getElementById("instructions-nav");
      nav.innerHTML = instructions
        .map((ix, i) => `
          <button class="tab-btn${i === 0 ? " active" : ""}" data-ix="${escHtml(ix.name)}">
            ${escHtml(ix.name)}
          </button>
        `)
        .join("");

      nav.addEventListener("click", async (e) => {
        const btn = e.target.closest(".tab-btn");
        if (!btn) return;
        nav.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
        btn.classList.add("active");
        currentIxName = btn.dataset.ix;
        currentIxOffset = 0;
        await loadIxData();
      });

      currentIxName = instructions[0].name;
      currentIxOffset = 0;
      await loadIxData();
      document.getElementById("instructions-section").classList.remove("hidden");
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
        if (rows.length === PAGE_SIZE) {
          content.innerHTML += `<button class="load-more-btn" id="ix-more-btn">Load more</button>`;
          document.getElementById("ix-more-btn").addEventListener("click", async () => {
            currentIxOffset += PAGE_SIZE;
            await loadIxData(true);
          });
        }
      } else {
        const existingMoreBtn = document.getElementById("ix-more-btn");
        if (existingMoreBtn) existingMoreBtn.remove();
        if (rows.length > 0) {
          const existingWrapper = content.querySelector(".table-wrapper");
          if (existingWrapper) {
            const tbody = existingWrapper.querySelector("tbody");
            if (tbody) {
              rows.forEach((row) => {
                const keys = Object.keys(row);
                const tr = document.createElement("tr");
                tr.innerHTML = keys
                  .map((k) => `<td title="${escHtml(String(row[k]))}">${escHtml(String(row[k] ?? ""))}</td>`)
                  .join("");
                tbody.appendChild(tr);
              });
            }
          }
          if (rows.length === PAGE_SIZE) {
            const btn = document.createElement("button");
            btn.className = "load-more-btn";
            btn.id = "ix-more-btn";
            btn.textContent = "Load more";
            btn.addEventListener("click", async () => {
              currentIxOffset += PAGE_SIZE;
              await loadIxData(true);
            });
            content.appendChild(btn);
          }
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
        .map((a, i) => `
          <button class="tab-btn${i === 0 ? " active" : ""}" data-type="${escHtml(a.name)}">
            ${escHtml(a.name)}
          </button>
        `)
        .join("");

      nav.addEventListener("click", async (e) => {
        const btn = e.target.closest(".tab-btn");
        if (!btn) return;
        nav.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("active"));
        btn.classList.add("active");
        currentAcctType = btn.dataset.type;
        currentAcctOffset = 0;
        await loadAcctData();
      });

      currentAcctType = account_types[0].name;
      currentAcctOffset = 0;
      await loadAcctData();
      document.getElementById("accounts-section").classList.remove("hidden");
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
        if (rows.length === PAGE_SIZE) {
          content.innerHTML += `<button class="load-more-btn" id="acct-more-btn">Load more</button>`;
          document.getElementById("acct-more-btn").addEventListener("click", async () => {
            currentAcctOffset += PAGE_SIZE;
            await loadAcctData(true);
          });
        }
      } else {
        const existingMoreBtn = document.getElementById("acct-more-btn");
        if (existingMoreBtn) existingMoreBtn.remove();
        if (rows.length > 0) {
          const wrapper = content.querySelector(".table-wrapper");
          if (wrapper) {
            const tbody = wrapper.querySelector("tbody");
            if (tbody) {
              rows.forEach((row) => {
                const keys = Object.keys(row);
                const tr = document.createElement("tr");
                tr.innerHTML = keys
                  .map((k) => `<td title="${escHtml(String(row[k]))}">${escHtml(String(row[k] ?? ""))}</td>`)
                  .join("");
                tbody.appendChild(tr);
              });
            }
          }
          if (rows.length === PAGE_SIZE) {
            const btn = document.createElement("button");
            btn.className = "load-more-btn";
            btn.id = "acct-more-btn";
            btn.textContent = "Load more";
            btn.addEventListener("click", async () => {
              currentAcctOffset += PAGE_SIZE;
              await loadAcctData(true);
            });
            content.appendChild(btn);
          }
        }
      }
    } catch (e) {
      content.innerHTML = `<div class="error-msg">${escHtml(e.message)}</div>`;
    }
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

  document.getElementById("sql-run-btn").addEventListener("click", async () => {
    const sql = document.getElementById("sql-input").value.trim();
    const result = document.getElementById("sql-result");
    if (!sql) return;
    result.innerHTML = `<div class="empty-state">running...</div>`;
    try {
      const data = await fetchJson("/api/sql", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql }),
      });
      if (data.data && data.data.length > 0) {
        result.innerHTML = renderTable(data.data) + `<div class="empty-state" style="padding:0.5rem">${data.count} row(s)</div>`;
      } else {
        result.innerHTML = `<div class="empty-state">0 rows returned</div>`;
      }
    } catch (e) {
      result.innerHTML = `<div class="error-msg">${escHtml(e.message)}</div>`;
    }
  });

  function escHtml(str) {
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  init();
})();
