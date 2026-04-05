const PANEL_TITLES = {
  pipeline: 'Расписание',
  story:    'Генерация сюжета',
  request:  'Генерация видео',
  publish:  'Публикация',
  service:  'Служебные',
  log:      'Монитор',
};

function openSidebar() {
  document.getElementById('sidebar').classList.add('open');
  var existing = document.getElementById('sidebar-overlay');
  if (existing) { existing.classList.add('open'); return; }
  var el = document.createElement('div');
  el.className = 'sidebar-overlay open';
  el.id = 'sidebar-overlay';
  el.addEventListener('click', closeSidebar);
  document.body.insertBefore(el, document.body.firstChild);
}

function closeSidebar() {
  document.getElementById('sidebar').classList.remove('open');
  var el = document.getElementById('sidebar-overlay');
  if (el) el.remove();
}

function switchPanel(name) {
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.sidebar-item').forEach(b => b.classList.remove('active'));
  const panel = document.getElementById('panel-' + name);
  if (panel) panel.classList.add('active');
  const btn = document.querySelector('.sidebar-item[data-panel="' + name + '"]');
  if (btn) btn.classList.add('active');
  const titleEl = document.getElementById('page-title');
  if (titleEl) titleEl.textContent = PANEL_TITLES[name] || name;
  closeSidebar();
  if (name === 'log') {
    monitorClockStart();
  } else {
    monitorClockStop();
  }
  if (name === 'request') {
    if (typeof loadModels === 'function') loadModels();
  }
  if (name === 'story') {
    if (typeof loadTextModels === 'function') loadTextModels();
  }
}

let _monitorClockTimer = null;
let _monitorClockOffset = 0;

function monitorClockTick() {
  const el = document.getElementById('monitor-clock');
  if (!el) return;
  const serverNow = Date.now() + _monitorClockOffset;
  const msk = new Date(serverNow + 3 * 60 * 60 * 1000);
  const pad = n => String(n).padStart(2, '0');
  el.textContent =
    pad(msk.getUTCDate()) + '.' +
    pad(msk.getUTCMonth() + 1) + '.' +
    msk.getUTCFullYear() + ' ' +
    pad(msk.getUTCHours()) + ':' +
    pad(msk.getUTCMinutes()) + ':' +
    pad(msk.getUTCSeconds());
}

function monitorClockStart() {
  const el = document.getElementById('monitor-clock');
  if (!el) return;
  const t0 = Date.now();
  fetch('/api/time').then(r => r.json()).then(d => {
    const t1 = Date.now();
    const serverMs = d.utc_ms;
    _monitorClockOffset = serverMs - Math.round((t0 + t1) / 2);
    monitorClockTick();
    el.style.display = 'block';
    if (!_monitorClockTimer) _monitorClockTimer = setInterval(monitorClockTick, 1000);
  }).catch(() => {
    monitorClockTick();
    el.style.display = 'block';
    if (!_monitorClockTimer) _monitorClockTimer = setInterval(monitorClockTick, 1000);
  });
}

function monitorClockStop() {
  if (_monitorClockTimer) { clearInterval(_monitorClockTimer); _monitorClockTimer = null; }
  const el = document.getElementById('monitor-clock');
  if (el) el.style.display = 'none';
}

(function() {
  const tab = new URLSearchParams(window.location.search).get('tab');
  if (tab && document.getElementById('panel-' + tab)) {
    switchPanel(tab);
    history.replaceState(null, '', window.location.pathname);
  }
})();
