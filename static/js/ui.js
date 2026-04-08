const PANEL_TITLES = {
  pipeline: 'Расписание',
  story:    'Генерация сюжета',
  request:  'Генерация видео',
  publish:  'Публикация',
  service:  'Служебные',
  log:      'Монитор',
  info:     'Информация',
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
    monitorClockStart('monitor-clock');
  } else if (name === 'pipeline') {
    monitorClockStart('schedule-clock');
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
let _monitorClockActiveId = null;

function monitorClockTick() {
  const el = _monitorClockActiveId ? document.getElementById(_monitorClockActiveId) : null;
  if (!el) return;
  const serverNow = Date.now() + _monitorClockOffset;
  const msk = new Date(serverNow + 3 * 60 * 60 * 1000);
  const pad = n => String(n).padStart(2, '0');
  const timeStr =
    pad(msk.getUTCDate()) + '.' +
    pad(msk.getUTCMonth() + 1) + '.' +
    msk.getUTCFullYear() + ' ' +
    pad(msk.getUTCHours()) + ':' +
    pad(msk.getUTCMinutes()) + ':' +
    pad(msk.getUTCSeconds());
  const timeEl = el.querySelector('[id$="-clock-time"]');
  if (timeEl) timeEl.textContent = timeStr;
  else el.textContent = timeStr;
}

function monitorClockStart(elId) {
  monitorClockStop();
  const el = document.getElementById(elId);
  if (!el) return;
  _monitorClockActiveId = elId;
  const t0 = Date.now();
  fetch('/api/time').then(r => r.json()).then(d => {
    const t1 = Date.now();
    _monitorClockOffset = d.utc_ms - Math.round((t0 + t1) / 2);
    monitorClockTick();
    el.style.display = 'flex';
    if (!_monitorClockTimer) _monitorClockTimer = setInterval(monitorClockTick, 1000);
  }).catch(() => {
    monitorClockTick();
    el.style.display = 'flex';
    if (!_monitorClockTimer) _monitorClockTimer = setInterval(monitorClockTick, 1000);
  });
}

function monitorClockStop() {
  if (_monitorClockTimer) { clearInterval(_monitorClockTimer); _monitorClockTimer = null; }
  if (_monitorClockActiveId) {
    const el = document.getElementById(_monitorClockActiveId);
    if (el) el.style.display = 'none';
    _monitorClockActiveId = null;
  }
}

(function() {
  const tab = new URLSearchParams(window.location.search).get('tab');
  if (tab && document.getElementById('panel-' + tab)) {
    switchPanel(tab);
    history.replaceState(null, '', window.location.pathname);
  } else {
    const active = document.querySelector('.tab-panel.active');
    if (active && active.id === 'panel-pipeline') monitorClockStart('schedule-clock');
    else if (active && active.id === 'panel-log') monitorClockStart('monitor-clock');
  }
})();

/* ── Тултип долгого нажатия для тач-устройств ── */
(function() {
  if (!('ontouchstart' in window)) return;

  let _timer   = null;
  let _tooltip = null;
  let _autoHide = null;

  function getTooltip() {
    if (!_tooltip) {
      _tooltip = document.createElement('div');
      _tooltip.id = 'touch-tooltip';
      _tooltip.classList.add('hidden');
      document.body.appendChild(_tooltip);
    }
    return _tooltip;
  }

  function showTooltip(text, targetEl) {
    const tip = getTooltip();
    tip.textContent = text;
    tip.classList.remove('hidden');

    const rect = targetEl.getBoundingClientRect();
    const tipW = 220;
    let x = rect.left + rect.width / 2 - tipW / 2;
    let y = rect.top - 10;

    x = Math.max(8, Math.min(x, window.innerWidth - tipW - 8));

    tip.style.width  = tipW + 'px';
    tip.style.left   = x + 'px';

    tip.style.top    = '0px';
    tip.style.bottom = '';
    const tipH = tip.offsetHeight || 34;
    if (y - tipH < 8) {
      tip.style.top  = (rect.bottom + 10) + 'px';
    } else {
      tip.style.top  = (y - tipH) + 'px';
    }

    clearTimeout(_autoHide);
    _autoHide = setTimeout(hideTooltip, 2500);
  }

  function hideTooltip() {
    clearTimeout(_autoHide);
    if (_tooltip) _tooltip.classList.add('hidden');
  }

  function findTitle(el) {
    while (el && el !== document.body) {
      if (el.title) return { text: el.title, el };
      el = el.parentElement;
    }
    return null;
  }

  document.addEventListener('touchstart', function(e) {
    clearTimeout(_timer);
    const found = findTitle(e.target);
    if (!found) return;
    _timer = setTimeout(function() {
      showTooltip(found.text, found.el);
    }, 500);
  }, { passive: true });

  document.addEventListener('touchend',    function() { clearTimeout(_timer); }, { passive: true });
  document.addEventListener('touchmove',   function() { clearTimeout(_timer); hideTooltip(); }, { passive: true });
  document.addEventListener('touchcancel', function() { clearTimeout(_timer); hideTooltip(); }, { passive: true });
})();
