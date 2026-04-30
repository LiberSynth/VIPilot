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

function createBrowserWidget(slug) {
  var VIEWPORT_W = 1280;
  var VIEWPORT_H = 720;

  var canvas   = document.getElementById(slug + '-browser-canvas');
  var overlay  = document.getElementById(slug + '-browser-overlay');
  var btnStart = document.getElementById(slug + '-btn-start');
  var btnSave  = document.getElementById(slug + '-btn-save');
  var btnStop  = document.getElementById(slug + '-btn-stop');

  if (!canvas) return;

  var ctx = canvas.getContext('2d');
  canvas.width  = VIEWPORT_W;
  canvas.height = VIEWPORT_H;

  var sse        = null;
  var active     = false;
  var firstFrame = false;

  var STATE = { IDLE: 'idle', STARTING: 'starting', OPEN: 'open', STOPPING: 'stopping' };
  var state = STATE.IDLE;

  var API = '/api/' + slug + '-browser/';

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

  canvas.addEventListener('click', function (e) {
    if (!active) return;
    var c = canvasCoords(e);
    sendEvent({ type: 'click', x: c.x, y: c.y });
    canvas.focus();
    e.preventDefault();
  });

  canvas.addEventListener('mousemove', function (e) {
    if (!active) return;
    var c = canvasCoords(e);
    sendEvent({ type: 'move', x: c.x, y: c.y });
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
  }

  function handleStopped() {
    active = false;
    firstFrame = false;
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
