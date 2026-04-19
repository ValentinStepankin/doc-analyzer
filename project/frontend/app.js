'use strict';

// ─── Config ───────────────────────────────────────────────────────────────────
const API_BASE = '/api';

// ─── Custom select ────────────────────────────────────────────────────────────
function toggleCsel(e, id) {
  e.stopPropagation();
  const el = document.getElementById(id);
  const isOpen = el.classList.contains('open');
  document.querySelectorAll('.csel.open').forEach(c => c.classList.remove('open'));
  if (!isOpen) el.classList.add('open');
}

function pickCsel(id, value, label) {
  const el = document.getElementById(id);
  if (!el) return;
  el.dataset.value = value;
  el.querySelector('.csel-label').textContent = label;
  el.querySelectorAll('.csel-opt').forEach(o => o.classList.toggle('selected', o.dataset.value === value));
  el.classList.remove('open');
  applyFilters();
}

function getCsel(id) {
  return document.getElementById(id)?.dataset.value ?? '';
}

function setCsel(id, value) {
  const el = document.getElementById(id);
  if (!el) return;
  const opt = el.querySelector(`.csel-opt[data-value="${CSS.escape(value)}"]`)
           || el.querySelector(`.csel-opt[data-value="${value}"]`);
  if (!opt) return;
  el.dataset.value = value;
  el.querySelector('.csel-label').textContent = opt.textContent.trim();
  el.querySelectorAll('.csel-opt').forEach(o => o.classList.toggle('selected', o.dataset.value === value));
}

document.addEventListener('click', () => {
  document.querySelectorAll('.csel.open').forEach(c => c.classList.remove('open'));
});

// ─── State ────────────────────────────────────────────────────────────────────
const state = {
  view: 'overview',
  files: [],
  totalFiles: 0,
  page: 1,
  limit: 50,
  filters: { action: '', category: '', ext: '', minScore: 0, search: '', status: '' },
  sort: { col: 'value_score', order: 'desc' },
  pollInterval: null,
  scanActive: false,
};

// ─── API ──────────────────────────────────────────────────────────────────────
async function apiFetch(path, opts = {}) {
  const res = await fetch(API_BASE + path, {
    ...opts,
    headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) },
  });
  if (!res.ok) {
    let detail = `${res.status} ${res.statusText}`;
    try { const j = await res.json(); if (j.detail) detail = j.detail; } catch (_) {}
    const err = new Error(detail);
    throw err;
  }
  return res.json();
}

// ─── Utils ────────────────────────────────────────────────────────────────────
function showToast(message, type = 'error') {
  const color = type === 'error' ? '#f85149' : '#3fb950';
  const icon  = type === 'error' ? 'error_outline' : 'check_circle';
  const toast = document.createElement('div');
  toast.style.cssText = `position:fixed;bottom:24px;right:24px;z-index:9999;display:flex;align-items:center;gap:10px;padding:12px 18px;border-radius:10px;background:#1e2329;border:1px solid ${color}30;color:${color};font-size:13px;box-shadow:0 4px 20px rgba(0,0,0,0.4);transition:opacity 0.3s`;
  toast.innerHTML = `<span class="material-symbols-outlined" style="font-size:18px">${icon}</span>${message}`;
  document.body.appendChild(toast);
  setTimeout(() => { toast.style.opacity = '0'; setTimeout(() => toast.remove(), 300); }, 4000);
}

function filename(path) {
  return (path || '').split(/[\\/]/).pop();
}

function formatSize(bytes) {
  if (!bytes) return '—';
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

function formatDate(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: 'numeric' })
    + ', ' + d.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
}

function formatDateShort(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  const time = d.toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
  const date = d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: 'numeric' });
  return `${time}, ${date}`;
}

function actionBadge(action) {
  const map = {
    keep:            { color: '#3fb950', label: 'Хранить' },
    archive:         { color: '#7dd3fc', label: 'Архив' },
    review:          { color: '#f0883e', label: 'Проверка' },
    trash_candidate: { color: '#f85149', label: 'Мусор' },
  };
  const { color, label } = map[action] || { color: '#879484', label: action || '—' };
  return `<span style="background:${color}18;color:${color};border:1px solid ${color}30;border-radius:9999px" class="text-[10px] px-2.5 py-0.5 font-semibold uppercase tracking-wide whitespace-nowrap">${label}</span>`;
}

function scoreBar(score) {
  if (score == null) return '<span class="text-[10px] text-on-surface-variant/40">—</span>';
  return `
    <div class="flex items-center gap-2.5">
      <div class="w-20 h-1.5 bg-surface-container-highest rounded-full overflow-hidden flex-shrink-0">
        <div class="h-full bg-gradient-to-r from-[#f85149] via-[#fabc45] to-[#3fb950] rounded-full" style="width:${score}%"></div>
      </div>
      <span class="text-[10px] font-mono font-bold">${score}</span>
    </div>`;
}

function extBadge(ext) {
  return `<span class="bg-surface-container-highest text-[10px] px-2 py-0.5 rounded uppercase font-bold tracking-tighter">${(ext || '?').toUpperCase()}</span>`;
}

function esc(str) {
  return String(str || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// ─── Navigation ───────────────────────────────────────────────────────────────
function navigate(viewName) {
  document.querySelectorAll('.view').forEach(el => el.classList.remove('active'));
  const el = document.getElementById('view-' + viewName);
  if (el) el.classList.add('active');

  document.querySelectorAll('.nav-item').forEach(a => {
    const isActive = a.dataset.view === viewName;
    a.classList.toggle('text-primary',              isActive);
    a.classList.toggle('font-bold',                  isActive);
    a.classList.toggle('bg-surface-container-highest', isActive);
    a.classList.toggle('text-on-surface',            !isActive);
    a.classList.toggle('opacity-60',                 !isActive);
    a.classList.toggle('hover:opacity-100',          !isActive);
    a.classList.toggle('hover:bg-surface-container', !isActive);
  });

  state.view = viewName;

  if (viewName === 'overview') loadOverview();
  if (viewName === 'results')  loadResults();
  if (viewName === 'scan')     loadScanView();
  if (viewName === 'export')   loadExport();
}

// ─── Overview ─────────────────────────────────────────────────────────────────
async function loadOverview() {
  const [stats, status] = await Promise.all([
    apiFetch('/stats').catch(() => ({})),
    apiFetch('/status').catch(() => ({})),
  ]);

  // Stat cards
  document.getElementById('stat-cards').innerHTML = [
    statCard('Всего найдено',  stats.total     ?? status.total_found ?? '—', 'manage_search', '#dfe2eb',
      (stats.total ?? status.total_found ?? 0) > 0 ? "goToResults('all')" : null),
    statCard('Обработано',     stats.processed ?? status.processed  ?? '—', 'task_alt',       '#3fb950',
      (stats.processed ?? status.processed ?? 0) > 0 ? "goToResults('')" : null),
    statCard('Ошибок',         stats.errors    ?? status.errors     ?? '—', 'error_outline',  '#f85149',
      (stats.errors ?? status.errors ?? 0) > 0 ? 'goToErrors()' : null),
    statCard('Пропущено',      stats.skipped   ?? status.skipped    ?? '—', 'skip_next',      '#879484'),
  ].join('');

  // Active scan banner
  const banner = document.getElementById('scan-banner');
  if (status.current_file) {
    banner.classList.remove('hidden');
    banner.innerHTML = activeScanBannerHTML(status);
    startPolling();
  } else {
    banner.classList.add('hidden');
  }

  // Distributions
  document.getElementById('dist-categories').innerHTML =
    distributionHTML('По категориям', stats.by_category || {}, false);
  document.getElementById('dist-actions').innerHTML =
    distributionHTML('По рекомендациям', stats.by_action || {}, true);

  // Recent files
  document.getElementById('recent-files').innerHTML =
    recentFilesHTML(stats.recent || []);
}

function statCard(label, value, icon, color, onclick = null) {
  const clickable = onclick ? `onclick="${onclick}" role="button" tabindex="0"` : '';
  const hover     = onclick ? 'hover:bg-surface-container-high cursor-pointer transition-colors' : '';
  return `
    <div class="bg-surface-container rounded-xl p-5 flex flex-col gap-2 ${hover}" ${clickable}>
      <div class="flex items-center justify-between">
        <span class="text-[10px] uppercase tracking-widest text-on-surface-variant font-bold">${label}</span>
        <span class="material-symbols-outlined text-lg" style="color:${color}">${icon}</span>
      </div>
      <span class="text-3xl font-bold font-headline" style="color:${color}">${value}</span>
      ${onclick ? `<span class="text-[10px] text-on-surface-variant/60 mt-1">Нажмите, чтобы посмотреть →</span>` : ''}
    </div>`;
}

function goToResults(status) {
  navigate('results');
  setTimeout(() => {
    state.filters = { action: '', category: '', ext: '', minScore: 0, search: '', status };
    setCsel('filter-action', '');
    setCsel('filter-category', '');
    setCsel('filter-ext', '');
    setCsel('filter-status', status || '');
    const inp = document.getElementById('search-input'); if (inp) inp.value = '';
    const sf = document.getElementById('score-filter'); if (sf) sf.value = 0;
    const sv = document.getElementById('score-val'); if (sv) sv.textContent = '0';
    state.page = 1;
    fetchAndRenderFiles();
  }, 50);
}

function goToErrors() { goToResults('error'); }

function activeScanBannerHTML(status) {
  const done = (status.processed || 0) + (status.skipped || 0) + (status.errors || 0);
  const pct = status.total_found > 0
    ? Math.round(done / status.total_found * 100) : 0;
  return `
    <div class="bg-surface-container rounded-xl p-5 border border-primary/20">
      <div class="flex items-center justify-between mb-3">
        <div class="flex items-center gap-2.5">
          <span class="material-symbols-outlined text-primary text-xl spin-slow">progress_activity</span>
          <span class="text-sm font-bold text-primary uppercase tracking-wider">Идёт сканирование</span>
        </div>
        <button onclick="stopScan()" class="flex items-center gap-1 text-xs text-error hover:text-on-surface transition-colors">
          <span class="material-symbols-outlined text-sm">stop_circle</span> Остановить
        </button>
      </div>
      <div class="text-xs text-on-surface-variant mb-2 truncate font-mono">▸ ${status.current_file}</div>
      <div class="h-1.5 bg-surface-container-highest rounded-full overflow-hidden mb-3">
        <div id="banner-progress-bar" class="h-full bg-primary transition-all duration-500 rounded-full" style="width:${pct}%"></div>
      </div>
      <div class="flex gap-6 text-[11px] text-on-surface-variant">
        <span>Обработано: <strong class="text-on-surface">${status.processed}</strong></span>
        <span>Пропущено: <strong class="text-on-surface">${status.skipped}</strong></span>
        <span>Ошибок: <strong style="color:#f85149">${status.errors}</strong></span>
      </div>
    </div>`;
}

function distributionHTML(title, data, useActionColors) {
  const actionColors = { keep: '#3fb950', archive: '#7dd3fc', review: '#f0883e', trash_candidate: '#f85149' };
  const actionLabels = { keep: 'Хранить', archive: 'Архив', review: 'На проверку', trash_candidate: 'Мусор' };
  const entries = Object.entries(data).sort((a, b) => b[1] - a[1]);
  const max = entries[0]?.[1] || 1;

  return `
    <h2 class="text-[10px] uppercase tracking-widest text-on-surface-variant font-bold mb-4">${title}</h2>
    <div class="space-y-3">
      ${entries.map(([key, count]) => {
        const pct = Math.round(count / max * 100);
        const color = useActionColors ? (actionColors[key] || '#879484') : '#67df70';
        const label = useActionColors ? (actionLabels[key] || key) : key;
        return `
          <div>
            <div class="flex justify-between items-center mb-1">
              <span class="text-xs text-on-surface">${label}</span>
              <span class="text-[10px] font-mono text-on-surface-variant">${count}</span>
            </div>
            <div class="h-1.5 bg-surface-container-highest rounded-full overflow-hidden">
              <div class="h-full rounded-full" style="width:${pct}%;background:${color}"></div>
            </div>
          </div>`;
      }).join('')}
      ${entries.length === 0 ? '<p class="text-xs text-on-surface-variant">Нет данных</p>' : ''}
    </div>`;
}

function recentFilesHTML(files) {
  if (!files.length) {
    return `<div class="py-10 text-center text-on-surface-variant text-sm">Нет обработанных файлов</div>`;
  }
  return `
    <table class="w-full text-left">
      <tbody class="divide-y divide-outline-variant/5">
        ${files.map(f => `
          <tr class="hover:bg-surface-container-highest/40 cursor-pointer transition-colors" onclick="openDetail(${f.id})">
            <td class="py-3 px-4 text-xs font-semibold text-on-surface max-w-[220px]">
              <span title="${esc(f.path)}" class="block truncate">${filename(f.path)}</span>
            </td>
            <td class="py-3 px-4 text-[11px] text-on-surface-variant">${f.category || '—'}</td>
            <td class="py-3 px-4">${scoreBar(f.value_score)}</td>
            <td class="py-3 px-4">${actionBadge(f.suggested_action)}</td>
            <td class="py-3 px-4 text-[10px] font-mono text-on-surface-variant whitespace-nowrap">${formatDateShort(f.processed_at)}</td>
          </tr>`).join('')}
      </tbody>
    </table>`;
}

function updateOverviewBanner(status) {
  const banner = document.getElementById('scan-banner');
  if (!banner) return;
  if (!status.current_file) {
    banner.classList.add('hidden');
    loadOverview(); // обновить статистику после завершения сканирования
    return;
  }
  // Перестроить баннер если он был скрыт, иначе только обновить прогресс-бар
  if (banner.classList.contains('hidden')) {
    banner.classList.remove('hidden');
    banner.innerHTML = activeScanBannerHTML(status);
  } else {
    const _done = (status.processed || 0) + (status.skipped || 0) + (status.errors || 0);
    const pct = status.total_found > 0 ? Math.round(_done / status.total_found * 100) : 0;
    const bar = document.getElementById('banner-progress-bar');
    if (bar) bar.style.width = pct + '%';
  }
}

// ─── Results ──────────────────────────────────────────────────────────────────
function loadResults() {
  // search-input already in DOM, just bind Enter if not already bound
  const inp = document.getElementById('search-input');
  if (inp && !inp._bound) {
    inp._bound = true;
    inp.addEventListener('keydown', e => { if (e.key === 'Enter') doSearch(); });
  }
  fetchAndRenderFiles();
}

async function fetchAndRenderFiles() {
  const tbody = document.getElementById('files-tbody');
  if (!tbody) return;
  tbody.innerHTML = `<tr><td colspan="8" class="py-12 text-center text-on-surface-variant text-sm">Загрузка...</td></tr>`;

  let data;
  const q = state.filters.search;

  if (q) {
    data = await apiFetch(
      `/search?q=${encodeURIComponent(q)}&page=${state.page}&limit=${state.limit}`
    ).catch(() => ({ files: [], total: 0 }));
  } else {
    const p = new URLSearchParams({
      page: state.page,
      limit: state.limit,
      sort: state.sort.col,
      order: state.sort.order,
    });
    if (state.filters.action)   p.set('action',    state.filters.action);
    if (state.filters.category) p.set('category',  state.filters.category);
    if (state.filters.ext)      p.set('ext',        state.filters.ext);
    if (state.filters.minScore) p.set('min_score',  state.filters.minScore);
    if (state.filters.status)   p.set('status',     state.filters.status);

    data = await apiFetch(`/files?${p}`).catch(() => ({ files: [], total: 0 }));
  }

  state.files      = data.files  || [];
  state.totalFiles = data.total  || 0;

  // Update header count
  const countEl = document.getElementById('results-count');
  if (countEl) countEl.textContent = `${state.totalFiles} файл(ов)`;

  // Update sort indicators
  ['path', 'value_score', 'category', 'processed_at'].forEach(col => {
    const th = document.getElementById('th-' + col);
    if (!th) return;
    const base = th.textContent.replace(/ [↑↓]$/, '');
    if (col === state.sort.col) {
      th.textContent = base + (state.sort.order === 'desc' ? ' ↓' : ' ↑');
    } else {
      th.textContent = base;
    }
  });

  renderFilesTable(state.files);
  renderPagination(state.totalFiles);
}

function renderFilesTable(files) {
  const tbody = document.getElementById('files-tbody');
  if (!tbody) return;

  if (!files.length) {
    tbody.innerHTML = `
      <tr><td colspan="8" class="py-16 text-center">
        <div class="flex flex-col items-center gap-3 text-on-surface-variant">
          <span class="material-symbols-outlined text-4xl opacity-40">search_off</span>
          <div class="text-sm font-medium">Ничего не найдено</div>
          <div class="text-xs opacity-60">Попробуйте другой запрос или сбросьте фильтры</div>
        </div>
      </td></tr>`;
    const sa = document.getElementById('select-all');
    if (sa) sa.checked = false;
    updateBulkActions();
    return;
  }

  tbody.innerHTML = files.map(f => {
    const name = filename(f.path);
    const summaryRaw = f.summary || '';
    const summaryTrunc = summaryRaw.substring(0, 80) + (summaryRaw.length > 80 ? '…' : '');
    return `
      <tr class="group hover:bg-surface-container-highest/40 cursor-pointer transition-colors duration-100" onclick="openDetail(${f.id})">
        <td class="py-3 px-4" onclick="event.stopPropagation()">
          <input type="checkbox" data-id="${f.id}" onchange="updateBulkActions()" class="row-checkbox rounded-sm bg-surface-container-highest border-none focus:ring-primary cursor-pointer"/>
        </td>
        <td class="py-3 px-4 max-w-[180px]">
          <span title="${esc(f.path)}" class="block truncate text-xs font-semibold text-on-surface">${esc(name)}</span>
        </td>
        <td class="py-3 px-4">${extBadge(f.ext)}</td>
        <td class="py-3 px-4">${scoreBar(f.value_score)}</td>
        <td class="py-3 px-4 text-[11px] text-on-surface-variant whitespace-nowrap">${esc(f.category) || '—'}</td>
        <td class="py-3 px-4 text-center">${f.status === 'error'
          ? `<span style="background:#f8514918;color:#f85149;border:1px solid #f8514930;border-radius:9999px" class="text-[10px] px-2.5 py-0.5 font-semibold uppercase tracking-wide whitespace-nowrap">ошибка</span>`
          : actionBadge(f.suggested_action)}</td>
        <td class="py-3 px-4 max-w-[220px]">
          <span title="${esc(summaryRaw)}" class="block truncate text-xs text-on-surface-variant">${esc(summaryTrunc) || '—'}</span>
        </td>
        <td class="py-3 px-4 text-[10px] font-mono text-on-surface-variant whitespace-nowrap">${formatDateShort(f.processed_at)}</td>
      </tr>`;
  }).join('');

  const sa = document.getElementById('select-all');
  if (sa) sa.checked = false;
  updateBulkActions();
}

function renderPagination(total) {
  const info     = document.getElementById('pagination-info');
  const controls = document.getElementById('pagination-controls');
  if (!info || !controls) return;

  const pages = Math.ceil(total / state.limit);
  const from  = (state.page - 1) * state.limit + 1;
  const to    = Math.min(state.page * state.limit, total);

  if (total === 0) {
    info.innerHTML = 'Нет результатов';
    controls.innerHTML = '';
    return;
  }

  info.innerHTML = `Показано <span class="text-on-surface">${from}–${to}</span> из <span class="text-on-surface">${total}</span>`;

  if (pages <= 1) { controls.innerHTML = ''; return; }

  let nums = [];
  if (pages <= 7) {
    nums = Array.from({ length: pages }, (_, i) => i + 1);
  } else {
    nums = [1];
    if (state.page > 4) nums.push('…');
    for (let i = Math.max(2, state.page - 1); i <= Math.min(pages - 1, state.page + 1); i++) nums.push(i);
    if (state.page < pages - 3) nums.push('…');
    nums.push(pages);
  }

  controls.innerHTML = `
    <button onclick="goPage(${state.page - 1})" ${state.page === 1 ? 'disabled' : ''}
      class="w-8 h-8 flex items-center justify-center rounded hover:bg-surface-container transition-colors disabled:opacity-30">
      <span class="material-symbols-outlined text-lg">chevron_left</span>
    </button>
    ${nums.map(n => n === '…'
      ? `<span class="px-1 text-on-surface-variant text-sm">…</span>`
      : `<button onclick="goPage(${n})" class="w-8 h-8 flex items-center justify-center rounded ${n === state.page ? 'bg-primary text-on-primary' : 'hover:bg-surface-container'} text-[11px] font-bold transition-colors">${n}</button>`
    ).join('')}
    <button onclick="goPage(${state.page + 1})" ${state.page >= pages ? 'disabled' : ''}
      class="w-8 h-8 flex items-center justify-center rounded hover:bg-surface-container transition-colors disabled:opacity-30">
      <span class="material-symbols-outlined text-lg">chevron_right</span>
    </button>`;
}

function goPage(n) {
  const pages = Math.ceil(state.totalFiles / state.limit);
  if (n < 1 || n > pages) return;
  state.page = n;
  fetchAndRenderFiles();
}

function changeLimit(val) {
  state.limit = +val;
  state.page  = 1;
  fetchAndRenderFiles();
}

function pickLimitCsel(value) {
  const el = document.getElementById('limit-csel');
  if (!el) return;
  el.dataset.value = value;
  el.querySelector('.csel-label').textContent = value;
  el.querySelectorAll('.csel-opt').forEach(o => o.classList.toggle('selected', o.dataset.value === value));
  el.classList.remove('open');
  changeLimit(value);
}

function sortBy(col) {
  if (state.sort.col === col) {
    state.sort.order = state.sort.order === 'desc' ? 'asc' : 'desc';
  } else {
    state.sort.col   = col;
    state.sort.order = 'desc';
  }
  state.page = 1;
  fetchAndRenderFiles();
}

function applyFilters() {
  // Сбросить поиск при смене фильтра — они не должны работать одновременно
  state.filters.search = '';
  const inp = document.getElementById('search-input');
  if (inp) inp.value = '';
  state.filters.action   = getCsel('filter-action');
  state.filters.category = getCsel('filter-category');
  state.filters.ext      = getCsel('filter-ext');
  state.filters.status   = getCsel('filter-status');
  state.filters.minScore = +(document.getElementById('score-filter')?.value || 0);
  state.page = 1;
  fetchAndRenderFiles();
}

function clearFilters() {
  state.filters = { action: '', category: '', ext: '', minScore: 0, search: '', status: 'all' };
  state.page = 1;
  setCsel('filter-action', '');
  setCsel('filter-category', '');
  setCsel('filter-ext', '');
  setCsel('filter-status', 'all');
  const inp = document.getElementById('search-input'); if (inp) inp.value = '';
  const sf = document.getElementById('score-filter'); if (sf) sf.value = 0;
  const sv = document.getElementById('score-val'); if (sv) sv.textContent = '0';
  fetchAndRenderFiles();
}

function doSearch() {
  const q = document.getElementById('search-input')?.value.trim() || '';
  state.filters.search = q;
  state.page = 1;
  fetchAndRenderFiles();
}

function toggleSelectAll(cb) {
  document.querySelectorAll('.row-checkbox').forEach(c => { c.checked = cb.checked; });
  updateBulkActions();
}

function updateBulkActions() {
  const checked   = document.querySelectorAll('.row-checkbox:checked');
  const container = document.getElementById('bulk-actions');
  const countEl   = document.getElementById('bulk-count');
  if (checked.length > 0) {
    container?.classList.remove('hidden');
    container?.classList.add('flex');
    if (countEl) countEl.textContent = `Выбрано: ${checked.length}`;
  } else {
    container?.classList.add('hidden');
    container?.classList.remove('flex');
  }
}

async function reprocessSelected() {
  const ids = [...document.querySelectorAll('.row-checkbox:checked')].map(c => c.dataset.id);
  if (!ids.length) return;
  const results = await Promise.all(ids.map(id => apiFetch(`/files/${id}/reprocess`, { method: 'POST' }).catch(() => ({}))));
  const scanStarted = results.some(r => r.scan_started);
  if (scanStarted) {
    navigate('scan');
    startPolling();
  } else {
    fetchAndRenderFiles();
  }
}

// ─── Detail Panel ─────────────────────────────────────────────────────────────
async function openDetail(id) {
  const panel  = document.getElementById('detail-panel');
  const overlay = document.getElementById('panel-overlay');
  const content = document.getElementById('detail-content');

  content.innerHTML = `<div class="flex items-center justify-center h-full text-on-surface-variant text-sm py-20">Загрузка...</div>`;
  panel.classList.remove('translate-x-full');
  overlay.classList.remove('hidden');

  const file = await apiFetch(`/files/${id}`).catch(() => null);
  if (!file) {
    content.innerHTML = `<div class="p-8 text-on-surface-variant text-sm">Не удалось загрузить файл</div>`;
    return;
  }
  content.innerHTML = detailPanelHTML(file);
}

function closeDetail() {
  document.getElementById('detail-panel')?.classList.add('translate-x-full');
  document.getElementById('panel-overlay')?.classList.add('hidden');
}

function detailPanelHTML(f) {
  const chunks = f.chunks || [];
  return `
    <!-- Panel header -->
    <div class="flex items-center justify-between px-5 py-4 border-b border-outline-variant/10 bg-surface-container-low sticky top-0 z-10">
      <span class="text-sm font-semibold text-on-surface truncate max-w-[380px]" title="${esc(f.path)}">${esc(filename(f.path))}</span>
      <button onclick="closeDetail()" class="material-symbols-outlined text-on-surface-variant hover:text-on-surface transition-colors flex-shrink-0 ml-3">close</button>
    </div>

    <div class="px-5 py-5 space-y-5">

      <!-- Score + action -->
      <div class="flex items-start justify-between gap-4">
        <div class="flex flex-col gap-2.5 items-start">
          ${f.status === 'pending'
            ? `<span style="background:#879484
18;color:#879484;border:1px solid #87948430;border-radius:9999px" class="text-[10px] px-2.5 py-0.5 font-semibold uppercase tracking-wide">В очереди</span>`
            : f.status === 'error'
              ? `<span style="background:#f8514918;color:#f85149;border:1px solid #f8514930;border-radius:9999px" class="text-[10px] px-2.5 py-0.5 font-semibold uppercase tracking-wide">Ошибка</span>`
              : actionBadge(f.suggested_action)}
          ${f.status === 'ok' ? `
          <div class="flex items-center gap-3">
            <div class="w-44 h-2 bg-surface-container-highest rounded-full overflow-hidden">
              <div class="h-full bg-gradient-to-r from-[#f85149] via-[#fabc45] to-[#3fb950] rounded-full" style="width:${f.value_score ?? 0}%"></div>
            </div>
            <span class="text-2xl font-bold font-mono">${f.value_score ?? '—'}</span>
          </div>` : ''}
        </div>
        ${f.status === 'ok' ? `
        <button onclick="changeAction(${f.id}, '${f.suggested_action || ''}', '${(f.category || '').replace(/'/g, "\\'")}')" class="flex-shrink-0 flex items-center gap-1 text-xs bg-surface-container px-3 py-1.5 rounded hover:bg-surface-container-highest transition-colors text-on-surface-variant">
          <span class="material-symbols-outlined text-sm">edit</span> Изменить
        </button>` : ''}
      </div>

      <!-- Metadata -->
      <div class="bg-surface-container-low rounded-xl p-4 space-y-2 text-xs">
        ${f.status === 'ok' ? `<div class="flex gap-2"><span class="text-on-surface-variant w-24 shrink-0">Категория</span><span class="text-on-surface">${f.category || '—'}</span></div>` : ''}
        <div class="flex gap-2 items-center"><span class="text-on-surface-variant w-24 shrink-0">Тип / Размер</span><span class="text-on-surface uppercase font-bold">${f.ext}</span><span class="text-on-surface-variant mx-1">•</span><span class="text-on-surface-variant">${formatSize(f.size)}</span></div>
        <div class="flex gap-2"><span class="text-on-surface-variant w-24 shrink-0">${f.status === 'pending' ? 'В очереди с' : f.status === 'error' ? 'Ошибка' : 'Обработан'}</span><span class="text-on-surface">${formatDate(f.processed_at)}</span></div>
        <div class="flex gap-2 items-start"><span class="text-on-surface-variant w-24 shrink-0">Путь</span><span class="text-on-surface break-all font-mono text-[10px] leading-relaxed">${esc(f.path)}</span></div>
      </div>

      <!-- Summary -->
      <div>
        <h3 class="text-[10px] uppercase tracking-widest text-on-surface-variant font-bold mb-2">Сводка</h3>
        ${f.status === 'error'
          ? `<p class="text-sm text-on-surface-variant leading-relaxed italic">При обработке произошла ошибка. Нажмите «Переобработать файл» для повторной попытки.</p>`
          : f.status === 'pending'
            ? `<p class="text-sm text-on-surface-variant leading-relaxed italic">Файл ещё не обработан — данные появятся после завершения анализа.</p>`
            : `<p class="text-sm text-on-surface leading-relaxed">${esc(f.summary) || '—'}</p>`}
      </div>

      <!-- Why -->
      ${f.status === 'ok' && f.why ? `
        <div>
          <h3 class="text-[10px] uppercase tracking-widest text-on-surface-variant font-bold mb-2">Ценность</h3>
          <p class="text-sm text-on-surface-variant leading-relaxed">${esc(f.why)}</p>
        </div>` : ''}

      <!-- Chunks -->
      ${chunks.length > 0 ? `
        <div>
          <h3 class="text-[10px] uppercase tracking-widest text-on-surface-variant font-bold mb-3">Чанки (${chunks.length})</h3>
          <div class="space-y-2">
            ${chunks.map(c => `
              <div class="bg-surface-container-low rounded-xl p-4">
                <div class="flex items-center justify-between mb-2">
                  <span class="text-[10px] text-on-surface-variant">Чанк ${c.chunk_index + 1}</span>
                  <div class="flex items-center gap-2">
                    <span class="text-[10px] text-on-surface-variant">${c.category || ''}</span>
                    <span class="text-[10px] font-mono font-bold text-on-surface">${c.value_score}</span>
                  </div>
                </div>
                <p class="text-xs text-on-surface-variant leading-relaxed">${c.summary || (c.text || '').substring(0, 200) || '—'}</p>
              </div>`).join('')}
          </div>
        </div>` : ''}
    </div>

    <!-- Footer actions -->
    <div class="px-5 py-4 border-t border-outline-variant/10 bg-surface-container-low sticky bottom-0">
      <button onclick="reprocessFile(${f.id})" class="w-full flex items-center justify-center gap-2 bg-surface-container hover:bg-surface-container-highest text-on-surface text-xs font-medium py-2.5 rounded transition-colors">
        <span class="material-symbols-outlined text-sm">refresh</span> Переобработать файл
      </button>
    </div>`;
}

async function reprocessFile(id) {
  const res = await apiFetch(`/files/${id}/reprocess`, { method: 'POST' }).catch(() => ({}));
  closeDetail();
  if (res.scan_started) {
    navigate('scan');
    startPolling();
  } else if (state.view === 'results') {
    fetchAndRenderFiles();
  }
}

let _actionModalFileId = null;

function changeAction(id, currentAction, currentCategory) {
  _actionModalFileId = id;
  // Подсветить текущие значения
  document.querySelectorAll('#action-modal-options .action-modal-opt').forEach(btn => {
    btn.style.opacity = btn.dataset.action === currentAction ? '1' : '0.55';
    btn.style.fontWeight = btn.dataset.action === currentAction ? '600' : '400';
  });
  document.querySelectorAll('#category-modal-options .action-modal-opt').forEach(btn => {
    btn.style.opacity = btn.dataset.cat === currentCategory ? '1' : '0.55';
    btn.style.fontWeight = btn.dataset.cat === currentCategory ? '600' : '400';
  });
  document.getElementById('action-modal').classList.remove('hidden');
  document.getElementById('action-modal').classList.add('open');
}

function closeActionModal() {
  document.getElementById('action-modal').classList.add('hidden');
  document.getElementById('action-modal').classList.remove('open');
  _actionModalFileId = null;
}

async function submitActionModal(field, value) {
  const id = _actionModalFileId;
  closeActionModal();
  if (!id) return;
  const body = field === 'action' ? { suggested_action: value } : { category: value };
  await apiFetch(`/files/${id}`, { method: 'PATCH', body: JSON.stringify(body) }).catch(() => {});
  openDetail(id);
  if (state.view === 'results') fetchAndRenderFiles();
}

// ─── Browse & Recent paths ────────────────────────────────────────────────────
async function browsePath() {
  const btn = document.getElementById('browse-btn');
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="material-symbols-outlined text-sm spin-slow">progress_activity</span> Выбор...';
  }
  try {
    const res = await apiFetch('/browse/directory', { method: 'POST' });
    if (res.path) {
      document.getElementById('scan-dir').value = res.path;
    }
  } catch (_) {
    // Пользователь отменил диалог или диалог недоступен — поле остаётся редактируемым
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = '<span class="material-symbols-outlined text-sm">folder_open</span> Выбрать';
    }
  }
}

async function loadRecentPaths() {
  const container = document.getElementById('recent-paths');
  if (!container) return;
  try {
    const data = await apiFetch('/recent-paths');
    const paths = data.paths || [];
    if (!paths.length) { container.classList.add('hidden'); return; }
    container.classList.remove('hidden');
    container.innerHTML = `
      <div class="text-[10px] uppercase tracking-widest text-on-surface-variant font-bold mb-1.5">Недавние</div>
      <div class="space-y-0.5">
        ${paths.map(p => `
          <div class="flex items-center gap-1 group">
            <button data-path="${esc(p)}" onclick="selectRecentPath(this.dataset.path)"
              class="flex-1 min-w-0 text-left flex items-center gap-2 px-2 py-1.5 rounded hover:bg-surface-container-highest transition-colors">
              <span class="material-symbols-outlined text-sm text-on-surface-variant group-hover:text-primary transition-colors flex-shrink-0">history</span>
              <span class="text-xs text-on-surface-variant group-hover:text-on-surface font-mono truncate">${esc(p)}</span>
            </button>
            <button data-path="${esc(p)}" onclick="removeRecentPath(this.dataset.path)"
              class="flex-shrink-0 opacity-0 group-hover:opacity-100 transition-opacity p-1 rounded hover:bg-surface-container-highest text-on-surface-variant hover:text-error">
              <span class="material-symbols-outlined text-sm">close</span>
            </button>
          </div>`).join('')}
      </div>`;
  } catch (_) {
    container.classList.add('hidden');
  }
}

function selectRecentPath(path) {
  const input = document.getElementById('scan-dir');
  if (input) input.value = path;
}

async function removeRecentPath(path) {
  await apiFetch(`/recent-paths?path=${encodeURIComponent(path)}`, { method: 'DELETE' }).catch(() => {});
  loadRecentPaths();
}

// ─── Scan View ────────────────────────────────────────────────────────────────
async function loadScanView() {
  // Сбросить ошибку валидации при переходе на экран
  setScanDirError(false);

  // Подписка на ввод для сброса ошибки в реальном времени
  const scanDirInput = document.getElementById('scan-dir');
  if (scanDirInput && !scanDirInput._validationBound) {
    scanDirInput.addEventListener('input', () => { if (scanDirInput.value.trim()) setScanDirError(false); });
    scanDirInput._validationBound = true;
  }

  // Сбросить кнопку в исходное состояние перед проверкой статуса
  const btn = document.getElementById('start-scan-btn');
  const resetBtn = () => {
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = '<span class="material-symbols-outlined">play_arrow</span> Запустить сканирование';
    }
  };

  try {
    const status = await apiFetch('/status');
    if (status.current_file) {
      if (btn) { btn.disabled = true; btn.innerHTML = 'Идёт сканирование...'; }
      showActiveScanUI(status);
      startPolling();
    } else if (status.finished_at && status.processed > 0) {
      resetBtn();
      showScanDoneUI(status);
    } else {
      resetBtn();
    }
  } catch (_) { resetBtn(); }

  // Load model name
  try {
    const ollama = await apiFetch('/ollama/health');
    const el = document.getElementById('scan-model');
    if (el && ollama.model) el.textContent = ollama.model;
  } catch (_) {}

  loadRecentPaths();
}

function showActiveScanUI(status) {
  document.getElementById('scan-active-block')?.classList.remove('hidden');
  document.getElementById('scan-done-block')?.classList.add('hidden');
  updateScanProgress(status);
}

function showScanDoneUI(status) {
  document.getElementById('scan-active-block')?.classList.add('hidden');
  const btn = document.getElementById('start-scan-btn');
  if (btn) btn.classList.add('hidden');
  const footer = document.getElementById('scan-done-footer');
  if (footer) footer.classList.remove('hidden');
  const reprocessBtn = document.getElementById('reprocess-errors-btn');
  if (reprocessBtn) {
    if ((status.errors || 0) > 0) reprocessBtn.classList.remove('hidden');
    else reprocessBtn.classList.add('hidden');
  }
}

function resetScanForm() {
  document.getElementById('scan-done-footer')?.classList.add('hidden');
  const btn = document.getElementById('start-scan-btn');
  if (btn) {
    btn.classList.remove('hidden');
    btn.disabled = false;
    btn.innerHTML = '<span class="material-symbols-outlined">play_arrow</span> Запустить сканирование';
  }
}

function updateScanProgress(status) {
  const done = (status.processed || 0) + (status.skipped || 0) + (status.errors || 0);
  const pct = status.total_found > 0
    ? Math.round(done / status.total_found * 100) : 0;

  const bar = document.getElementById('scan-progress-bar');
  if (bar) bar.style.width = pct + '%';

  const curr = document.getElementById('scan-current-file');
  if (curr) curr.textContent = status.current_file || '—';

  const set = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
  set('scan-stat-processed', status.processed);
  set('scan-stat-skipped',   status.skipped);
  set('scan-stat-errors',    status.errors);
  set('scan-stat-total',     status.total_found || '—');

  if (!status.current_file && status.finished_at) {
    showScanDoneUI(status);
    stopPolling();
  }
}

function setScanDirError(show) {
  const input = document.getElementById('scan-dir');
  const error = document.getElementById('scan-dir-error');
  if (show) {
    input?.classList.add('border-red-500', 'focus:ring-red-500/30');
    input?.classList.remove('border-outline-variant/20', 'focus:ring-primary/30');
    error?.classList.remove('hidden');
  } else {
    input?.classList.remove('border-red-500', 'focus:ring-red-500/30');
    input?.classList.add('border-outline-variant/20', 'focus:ring-primary/30');
    error?.classList.add('hidden');
  }
}

async function startScan() {
  const dir = document.getElementById('scan-dir')?.value.trim();
  if (!dir) {
    setScanDirError(true);
    document.getElementById('scan-dir')?.focus();
    return;
  }
  setScanDirError(false);
  const opts = {
    directory:                  dir,
    process_standalone_images:  document.getElementById('opt-standalone-images')?.checked ?? true,
    process_embedded_images:    document.getElementById('opt-embedded-images')?.checked ?? true,
    reprocess_errors:           document.getElementById('opt-reprocess')?.checked ?? false,
    model_name:                 document.querySelector('input[name="opt-model"]:checked')?.value ?? 'qwen3-vl:8b',
  };
  const btn = document.getElementById('start-scan-btn');
  if (btn) { btn.disabled = true; btn.textContent = 'Запуск...'; }

  const showLaunchError = (msg) => {
    const el = document.getElementById('scan-launch-error');
    const txt = document.getElementById('scan-launch-error-text');
    if (el && txt) { txt.textContent = msg; el.classList.remove('hidden'); }
  };
  const hideLaunchError = () => document.getElementById('scan-launch-error')?.classList.add('hidden');

  hideLaunchError();
  try {
    await apiFetch('/scan/start', { method: 'POST', body: JSON.stringify(opts) });
    if (btn) { btn.disabled = true; btn.innerHTML = 'Идёт сканирование...'; }
    showActiveScanUI({ processed: 0, skipped: 0, errors: 0, total_found: 0, current_file: '...' });
    startPolling();
    loadRecentPaths();
  } catch (e) {
    showLaunchError(e.message || 'Не удалось запустить сканирование');
    if (btn) { btn.disabled = false; btn.innerHTML = '<span class="material-symbols-outlined">play_arrow</span> Запустить сканирование'; }
  }
}

async function stopScan() {
  try {
    const res = await apiFetch('/scan/stop', { method: 'POST' });
    if (res.ok) {
      stopPolling();
      document.getElementById('scan-active-block')?.classList.add('hidden');
      resetScanForm();
    } else {
      showToast('Не удалось остановить процесс: ' + (res.detail || 'процесс не найден'));
    }
  } catch (e) {
    showToast('Ошибка при остановке: ' + e.message);
  }
}

async function reprocessErrors() {
  await apiFetch('/scan/reprocess-errors', { method: 'POST' }).catch(() => {});
  document.getElementById('scan-done-footer')?.classList.add('hidden');
  const btn = document.getElementById('start-scan-btn');
  if (btn) btn.classList.remove('hidden');
  showActiveScanUI({ processed: 0, skipped: 0, errors: 0, total_found: 0, current_file: '...' });
  startPolling();
}

// ─── Export View ──────────────────────────────────────────────────────────────
function loadExport() {
  loadExportHistory();
  loadExportCounts();
}

async function loadExportCounts() {
  try {
    const stats = await apiFetch('/stats');
    const set = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val ?? '0'; };
    set('export-count-all',     stats.processed ?? 0);
    set('export-count-keep',    stats.by_action?.keep ?? 0);
    set('export-count-archive', stats.by_action?.archive ?? 0);
    set('export-count-review',  stats.by_action?.review ?? 0);
    set('export-count-trash',   stats.by_action?.trash_candidate ?? 0);
  } catch (_) {}
}

async function loadExportHistory() {
  const el = document.getElementById('export-history');
  if (!el) return;
  try {
    const data = await apiFetch('/export/history');
    if (!data.files?.length) {
      el.classList.add('hidden');
      return;
    }
    el.classList.remove('hidden');
    el.innerHTML = `
      <div class="text-[10px] uppercase tracking-widest text-on-surface-variant font-bold mb-3">История экспортов</div>
      ${data.files.map(f => `
        <div class="flex items-center justify-between py-2 border-b border-outline-variant/5 last:border-0">
          <span class="text-xs font-mono text-on-surface">${f.name}</span>
          <a href="${API_BASE}/export/download/${f.name}" class="text-xs text-primary hover:underline">Скачать</a>
        </div>`).join('')}`;
  } catch (_) {
    el.innerHTML = `
      <div class="text-[10px] uppercase tracking-widest text-on-surface-variant font-bold mb-3">История экспортов</div>
      <div class="text-xs text-on-surface-variant">Недоступно</div>`;
  }
}

function downloadExport() {
  const filter = document.querySelector('input[name="export-filter"]:checked')?.value || '';
  const url = `${API_BASE}/export/csv${filter ? '?filter=' + filter : ''}`;
  window.location.href = url;
}

// ─── Polling ──────────────────────────────────────────────────────────────────
function startPolling() {
  if (state.pollInterval) return;
  state.pollInterval = setInterval(async () => {
    try {
      const status = await apiFetch('/status');
      if (state.view === 'scan')     updateScanProgress(status);
      if (state.view === 'overview') updateOverviewBanner(status);
      if (!status.current_file)      stopPolling();
    } catch (_) {}
  }, 1000);
}

function stopPolling() {
  if (state.pollInterval) {
    clearInterval(state.pollInterval);
    state.pollInterval = null;
  }
}

// ─── Ollama Status ────────────────────────────────────────────────────────────
async function checkOllamaStatus() {
  try {
    const data = await apiFetch('/ollama/health');
    const online = data.online === true;
    const dot   = document.getElementById('ollama-dot');
    const label = document.getElementById('ollama-label');
    if (dot)   dot.style.color   = online ? '#3fb950' : '#f85149';
    if (label) label.textContent = online ? 'Ollama: online' : 'Ollama: offline';
  } catch (_) {
    const dot   = document.getElementById('ollama-dot');
    const label = document.getElementById('ollama-label');
    if (dot)   dot.style.color   = '#f85149';
    if (label) label.textContent = 'Ollama: offline';
  }
}

// ─── Init ─────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  navigate('overview');
  checkOllamaStatus();
  setInterval(checkOllamaStatus, 30_000);

  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') closeDetail();
  });
});
