const PANEL_TITLES = {
  screenwriter: 'Сценарист',
  director:     'Режиссер',
  workflow: 'Рабочий поток',
  story:    'Генерация сюжета',
  request:  'Генерация видео',
  publish:  'Публикация',
  log:      'Монитор',
  service:  'Служебные',
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

var _MONITOR_SCROLL_KEY = 'memo_pageScroll_panel-log';

function _activePanelStorageKey() {
  return 'vip_active_panel_' + window.location.pathname;
}

function _resolvePanelName(name) {
  if (name === 'pipeline') return 'workflow';
  return name;
}

function getInitialPanelName() {
  var tabParam = new URLSearchParams(window.location.search).get('tab');
  if (tabParam) {
    tabParam = _resolvePanelName(tabParam);
    history.replaceState(null, '', window.location.pathname);
    if (document.getElementById('panel-' + tabParam)) return tabParam;
  }
  var saved = localStorage.getItem(_activePanelStorageKey());
  if (!saved) return null;
  saved = _resolvePanelName(saved);
  return document.getElementById('panel-' + saved) ? saved : null;
}

function switchPanel(name) {
  name = _resolvePanelName(name);
  var panel = document.getElementById('panel-' + name);
  if (!panel) return;

  var activePanel = document.querySelector('.tab-panel.active');
  var alreadyActive = panel.classList.contains('active')
    && !!document.querySelector('.sidebar-item.active[data-panel="' + name + '"]');

  if (activePanel && activePanel.id === 'panel-log') {
    localStorage.setItem(_MONITOR_SCROLL_KEY, String(window.scrollY || 0));
    if (name !== 'log' && typeof window.monitorPausePolling === 'function') {
      window.monitorPausePolling();
    }
  }
  if (activePanel && activePanel.id === 'panel-publish' && name !== 'publish') {
    var registry = window._browserWidgetRegistry;
    if (registry) {
      Object.keys(registry).forEach(function(key) { registry[key](); });
    }
  }

  if (alreadyActive) {
    localStorage.setItem(_activePanelStorageKey(), name);
    closeSidebar();
    _runPanelEnterEffects(name);
    return;
  }

  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.sidebar-item').forEach(b => b.classList.remove('active'));
  panel.classList.add('active');
  localStorage.setItem(_activePanelStorageKey(), name);
  const btn = document.querySelector('.sidebar-item[data-panel="' + name + '"]');
  if (btn) btn.classList.add('active');
  const titleEl = document.getElementById('page-title');
  if (titleEl) titleEl.textContent = PANEL_TITLES[name] || name;
  closeSidebar();
  _runPanelEnterEffects(name);
}

function _runPanelEnterEffects(name) {
  if (name === 'log') {
    var _rawScroll = localStorage.getItem(_MONITOR_SCROLL_KEY);
    if (_rawScroll !== null) {
      var savedScroll = parseInt(_rawScroll, 10) || 0;
      requestAnimationFrame(function() { window.scrollTo(0, savedScroll); });
    }
    if (typeof window.monitorResumePolling === 'function') window.monitorResumePolling();
    else if (typeof window.monitorRefresh === 'function') window.monitorRefresh();
  }
  if (name === 'service') {
    if (typeof refreshWorkflowState === 'function') refreshWorkflowState();
  }
  if (typeof refreshDbOpStatus === 'function') refreshDbOpStatus();
  if (name === 'director' || name === 'workflow') {
    if (typeof refreshMoviePoolCount === 'function') refreshMoviePoolCount();
    refreshGoodPoolCount();
  }
  if (name === 'request') {
    if (typeof loadModels === 'function') loadModels();
  }
  if (name === 'story') {
    if (typeof loadTextModels === 'function') loadTextModels();
  }
  if (name === 'screenwriter') {
    var storyCard = document.getElementById('card-story-editor');
    if (storyCard) storyCard.classList.remove('card--editing-new', 'card--editing-existing');
    if (typeof loadStoryList === 'function') loadStoryList();
  }
}

let _monitorClockTimer = null;
let _monitorClockOffset = 0;

function monitorClockTick() {
  const el = document.getElementById('header-clock');
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
  el.textContent = timeStr;
}

function monitorClockStart() {
  if (_monitorClockTimer) return;
  const t0 = Date.now();
  fetch('/api/time').then(r => r.json()).then(d => {
    const t1 = Date.now();
    _monitorClockOffset = d.utc_ms - Math.round((t0 + t1) / 2);
    monitorClockTick();
    if (!_monitorClockTimer) _monitorClockTimer = setInterval(monitorClockTick, 200);
  }).catch(() => {
    monitorClockTick();
    if (!_monitorClockTimer) _monitorClockTimer = setInterval(monitorClockTick, 200);
  });
}

function loadGoodPoolCount() {
  var els = document.querySelectorAll('.pool-count-value');
  if (!els.length) return;
  fetch('/production/stories/good_pool_count')
    .then(function(r) { return r.ok ? r.json() : null; })
    .then(function(d) {
      if (d && d.count !== undefined) {
        els.forEach(function(el) { el.textContent = d.count; });
      }
    })
    .catch(function() {});
}

function _isStoryPoolPanelActive() {
  var el = document.querySelector('.pool-count-value');
  if (!el) return false;
  var panel = el.closest('.tab-panel');
  return panel ? panel.classList.contains('active') : false;
}

function refreshGoodPoolCount() {
  if (!_isStoryPoolPanelActive()) return;
  loadGoodPoolCount();
}

(function() {
  document.documentElement.classList.remove('vip-panels-pending');
  monitorClockStart();
  loadGoodPoolCount();
  setInterval(refreshGoodPoolCount, 200);
  var btnHamburger = document.getElementById('btn-hamburger');
  if (btnHamburger) btnHamburger.addEventListener('click', openSidebar);
})();
