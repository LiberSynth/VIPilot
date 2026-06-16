/* Фабрика виджетов браузерной авторизации для платформ Дзен / Рутьюб / VK Видео.
 *
 * Использование:
 *   createBrowserWidget('dzen');
 *   createBrowserWidget('rutube');
 *   createBrowserWidget('vkvideo');
 *
 * Для slug='dzen' регистрирует глобальные функции:
 *   window.dzenBrowserOpen()
 *   window.dzenBrowserSaveSession()
 */

window._browserWidgetRegistry = window._browserWidgetRegistry || {};

var BROWSER_SVG_FULLSCREEN = '<svg viewBox="0 0 16 16"><polyline points="6,2 2,2 2,6"/><polyline points="10,2 14,2 14,6"/><polyline points="10,14 14,14 14,10"/><polyline points="6,14 2,14 2,10"/></svg>';
var BROWSER_SVG_LOUPE      = '<svg viewBox="0 0 16 16"><circle cx="6.5" cy="6.5" r="3.5"/><line x1="9" y1="9" x2="13" y2="13"/></svg>';
var BROWSER_SVG_NORMAL     = '<svg viewBox="0 0 16 16"><polyline points="6,6 2,6 2,2"/><polyline points="10,6 14,6 14,2"/><polyline points="10,10 14,10 14,14"/><polyline points="6,10 2,10 2,14"/></svg>';

function createBrowserWidget(slug) {
  var VIEWPORT_W = 1280;
  var VIEWPORT_H = 720;

  var canvas   = document.getElementById(slug + '-browser-canvas');
  var overlay  = document.getElementById(slug + '-browser-overlay');
  var wrap     = document.getElementById(slug + '-browser-wrap');
  var wrapHome = document.getElementById(slug + '-browser-wrap-home');
  var btnStart = document.getElementById(slug + '-btn-start');
  var btnSave  = document.getElementById(slug + '-btn-save');
  var btnStop  = document.getElementById(slug + '-btn-stop');
  var btnFullscreen = document.getElementById(slug + '-btn-fullscreen');
  var btnLoupe = document.getElementById(slug + '-btn-loupe');

  if (!canvas || !wrap || !wrapHome) return;

  var ctx = canvas.getContext('2d');
  canvas.width  = VIEWPORT_W;
  canvas.height = VIEWPORT_H;

  var sse        = null;
  var active     = false;
  var firstFrame = false;

  var STATE = { IDLE: 'idle', STARTING: 'starting', OPEN: 'open', STOPPING: 'stopping' };
  var state = STATE.IDLE;
  var MOVE_THROTTLE_MS = 60;
  var _lastMoveSentAt = 0;
  var _pendingMove = null;
  var _moveTimer = null;
  var _viewMode = null;
  var _viewOverlay = null;

  var API = '/api/' + slug + '-browser/';

  function resetCanvasDisplay() {
    canvas.style.removeProperty('width');
    canvas.style.removeProperty('height');
    canvas.style.removeProperty('max-width');
  }

  function updateViewButtons() {
    var canView = (state === STATE.OPEN && !_viewMode);
    if (btnFullscreen) btnFullscreen.disabled = !canView;
    if (btnLoupe) btnLoupe.disabled = !canView;
  }

  function exitViewMode() {
    if (!_viewMode) return;
    if (_viewOverlay) {
      _viewOverlay.remove();
      _viewOverlay = null;
    }
    wrapHome.appendChild(wrap);
    resetCanvasDisplay();
    document.body.style.overflow = '';
    _viewMode = null;
    updateViewButtons();
  }

  function enterViewMode(mode) {
    if (state !== STATE.OPEN || _viewMode) return;
    if (mode !== 'fullscreen' && mode !== 'loupe') return;

    _viewMode = mode;
    _viewOverlay = document.createElement('div');
    _viewOverlay.className = 'browser-view-overlay';
    _viewOverlay.innerHTML =
      '<div class="browser-view-shell">' +
        '<div class="browser-view-head">' +
          '<span class="browser-view-title">' +
            (mode === 'fullscreen' ? 'Полный экран' : 'Лупа 100%') +
          '</span>' +
          '<div class="monitor-hdr-actions-always browser-auth-view-actions">' +
            '<button type="button" class="cycle-float-btn browser-view-exit" title="Обычный вид">' +
              BROWSER_SVG_NORMAL +
            '</button>' +
          '</div>' +
        '</div>' +
        '<div class="browser-view-body browser-view-body--' + mode + '"></div>' +
      '</div>';

    var body = _viewOverlay.querySelector('.browser-view-body');
    body.appendChild(wrap);

    if (mode === 'loupe') {
      canvas.style.width  = VIEWPORT_W + 'px';
      canvas.style.height = VIEWPORT_H + 'px';
    } else {
      canvas.style.width    = '100%';
      canvas.style.maxWidth = VIEWPORT_W + 'px';
      canvas.style.height   = 'auto';
    }

    _viewOverlay.querySelector('.browser-view-exit').addEventListener('click', exitViewMode);
    document.body.appendChild(_viewOverlay);
    document.body.style.overflow = 'hidden';
    updateViewButtons();
    canvas.focus();
  }

  if (btnFullscreen) {
    btnFullscreen.addEventListener('click', function () { enterViewMode('fullscreen'); });
  }
  if (btnLoupe) {
    btnLoupe.addEventListener('click', function () { enterViewMode('loupe'); });
  }

  function getTargetId() {
    var el = document.getElementById(slug + '_target_id');
    return el ? el.value : '';
  }

  function getStudioUrl() {
    var card = document.getElementById(slug + '-browser-card');
    return card ? (card.dataset.studioUrl || '') : '';
  }

  function sendEvent(ev) {
    if (!active) return;
    fetch(API + 'event', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(ev),
    }).catch(function () {});
  }

  function canvasCoords(e) {
    var rect = canvas.getBoundingClientRect();
    var scaleX = VIEWPORT_W / rect.width;
    var scaleY = VIEWPORT_H / rect.height;
    return {
      x: Math.round((e.clientX - rect.left) * scaleX),
      y: Math.round((e.clientY - rect.top)  * scaleY),
    };
  }

  function flushMouseMove() {
    if (_moveTimer) {
      clearTimeout(_moveTimer);
      _moveTimer = null;
    }
    if (!active || !_pendingMove) {
      _pendingMove = null;
      return;
    }
    sendEvent({ type: 'move', x: _pendingMove.x, y: _pendingMove.y });
    _pendingMove = null;
    _lastMoveSentAt = Date.now();
  }

  canvas.addEventListener('click', function (e) {
    if (!active) return;
    var c = canvasCoords(e);
    sendEvent({ type: 'click', x: c.x, y: c.y });
    canvas.focus();
    e.preventDefault();
  });

  canvas.addEventListener('mousemove', function (e) {
    if (!active) return;
    _pendingMove = canvasCoords(e);
    var now = Date.now();
    var wait = MOVE_THROTTLE_MS - (now - _lastMoveSentAt);
    if (wait <= 0) {
      flushMouseMove();
    } else if (!_moveTimer) {
      _moveTimer = setTimeout(flushMouseMove, wait);
    }
  });

  canvas.addEventListener('wheel', function (e) {
    if (!active) return;
    sendEvent({ type: 'scroll', dx: e.deltaX, dy: e.deltaY });
    e.preventDefault();
  }, { passive: false });

  var SPECIAL_KEYS = {
    'Enter': 'Enter', 'Backspace': 'Backspace', 'Delete': 'Delete',
    'Tab': 'Tab', 'Escape': 'Escape',
    'ArrowLeft': 'ArrowLeft', 'ArrowRight': 'ArrowRight',
    'ArrowUp': 'ArrowUp', 'ArrowDown': 'ArrowDown',
    'Home': 'Home', 'End': 'End',
    'F5': 'F5', 'F12': 'F12',
  };

  canvas.addEventListener('keydown', function (e) {
    if (!active) return;
    if (SPECIAL_KEYS[e.key]) {
      sendEvent({ type: 'keydown', key: SPECIAL_KEYS[e.key] });
      e.preventDefault();
    } else if (e.key.length === 1 && !e.ctrlKey && !e.metaKey) {
      sendEvent({ type: 'type', text: e.key });
      e.preventDefault();
    } else if (e.ctrlKey && e.key === 'a') {
      sendEvent({ type: 'keydown', key: 'Control+a' });
      e.preventDefault();
    } else if (e.ctrlKey && e.key === 'c') {
      sendEvent({ type: 'keydown', key: 'Control+c' });
      e.preventDefault();
    } else if (e.ctrlKey && e.key === 'v') {
      sendEvent({ type: 'keydown', key: 'Control+v' });
      e.preventDefault();
    }
  });

  function connectStream() {
    if (sse) { sse.close(); sse = null; }
    firstFrame = false;
    sse = new EventSource(API + 'stream');

    sse.onmessage = function (e) {
      var data = e.data;
      if (data === 'STOPPED') {
        handleStopped();
        return;
      }
      if (data === ':keepalive' || data.startsWith(':')) return;

      var img = new Image();
      img.onload = function () {
        ctx.drawImage(img, 0, 0);
        if (!firstFrame) {
          firstFrame = true;
          if (state === STATE.STARTING) {
            overlay.style.display = 'none';
            applyState(STATE.OPEN);
          }
        }
      };
      img.src = 'data:image/jpeg;base64,' + data;
    };

    sse.onerror = function () {};
  }

  function applyState(s) {
    state = s;
    if (btnStart) btnStart.disabled = (s !== STATE.IDLE);
    if (btnSave)  btnSave.disabled  = (s !== STATE.OPEN);
    if (btnStop)  btnStop.disabled  = (s === STATE.IDLE || s === STATE.STOPPING);
    updateViewButtons();
  }

  function handleStopped() {
    exitViewMode();
    active = false;
    firstFrame = false;
    if (_moveTimer) {
      clearTimeout(_moveTimer);
      _moveTimer = null;
    }
    _pendingMove = null;
    if (sse) { sse.close(); sse = null; }
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    overlay.style.display = 'none';
    overlay.textContent = '';
    applyState(STATE.IDLE);
  }

  function stopOthers() {
    var registry = window._browserWidgetRegistry;
    Object.keys(registry).forEach(function (key) {
      if (key !== slug) registry[key]();
    });
  }

  function browserOpen() {
    if (state !== STATE.IDLE) return;
    var tid = getTargetId();
    if (!tid) return;

    stopOthers();

    overlay.style.display = 'flex';
    overlay.textContent = 'Загрузка…';

    fetch(API + 'start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target_id: tid }),
    })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        if (!data.ok) {
          overlay.style.display = 'none';
          overlay.textContent = '';
          applyState(STATE.IDLE);
          return;
        }
        active = true;
        firstFrame = false;
        applyState(STATE.STARTING);
        connectStream();
        var url = getStudioUrl();
        if (url) {
          setTimeout(function () {
            sendEvent({ type: 'navigate', url: url });
          }, 1200);
        }
      })
      .catch(function () {
        overlay.style.display = 'none';
        overlay.textContent = '';
        applyState(STATE.IDLE);
      });
  }

  function browserStop() {
    if (state === STATE.IDLE || state === STATE.STOPPING) return;
    exitViewMode();
    active = false;
    applyState(STATE.STOPPING);
    overlay.style.display = 'flex';
    overlay.textContent = 'Закрытие…';
    fetch(API + 'stop', { method: 'POST' }).catch(function () {});
    // SSE остаётся открытым — дожидается сообщения STOPPED, после чего
    // handleStopped() скроет обёртку и сбросит состояние.
  }

  function saveSession() {
    if (state !== STATE.OPEN) return;
    var tid = getTargetId();
    if (!tid) return;
    applyState(STATE.STOPPING);
    overlay.style.display = 'flex';
    overlay.textContent = 'Сохранение сессии…';

    fetch(API + 'save-session', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target_id: tid }),
    })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        if (data.ok) {
          active = false;
          showToast('Сессия сохранена', 'success');
          fetch(API + 'stop', { method: 'POST' }).catch(function () {});
          // SSE дожидается STOPPED → handleStopped() сбросит состояние.
        } else {
          overlay.style.display = 'none';
          overlay.textContent = '';
          applyState(STATE.OPEN);
          showToast('Ошибка: ' + (data.error || 'Не удалось сохранить'), 'error');
        }
      })
      .catch(function () {
        overlay.style.display = 'none';
        overlay.textContent = '';
        applyState(STATE.OPEN);
        showToast('Ошибка соединения', 'error');
      });
  }

  window.addEventListener('beforeunload', function () {
    if (active) {
      fetch(API + 'stop', { method: 'POST', keepalive: true }).catch(function () {});
    }
  });

  window._browserWidgetRegistry[slug]  = browserStop;
  window[slug + 'BrowserOpen']        = browserOpen;
  window[slug + 'BrowserSaveSession'] = saveSession;
  window[slug + 'BrowserStop']        = browserStop;
}
