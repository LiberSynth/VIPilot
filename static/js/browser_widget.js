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

function createBrowserWidget(slug, opts) {
  opts = opts || {};
  var autoStart = opts.autoStart !== false;
  var VIEWPORT_W = 1280;
  var VIEWPORT_H = 720;

  var canvas  = document.getElementById(slug + '-browser-canvas');
  var overlay = document.getElementById(slug + '-browser-overlay');
  var btnSave = document.getElementById(slug + '-btn-save');

  if (!canvas) return;

  var ctx = canvas.getContext('2d');
  canvas.width  = VIEWPORT_W;
  canvas.height = VIEWPORT_H;

  var sse        = null;
  var active     = false;
  var firstFrame = false;

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
          overlay.style.display = 'none';
        }
      };
      img.src = 'data:image/jpeg;base64,' + data;
    };

    sse.onerror = function () {};
  }

  function handleStopped() {
    active = false;
    if (sse) { sse.close(); sse = null; }
  }

  function browserOpen() {
    if (active) return;
    var tid = getTargetId();
    if (!tid) return;

    overlay.style.display = 'flex';
    overlay.textContent = 'Загрузка…';

    fetch(API + 'start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target_id: tid }),
    })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        if (!data.ok) return;
        active = true;
        firstFrame = false;
        connectStream();
        var url = getStudioUrl();
        if (url) {
          setTimeout(function () {
            sendEvent({ type: 'navigate', url: url });
          }, 1200);
        }
      })
      .catch(function () {});
  }

  function saveSession() {
    var tid = getTargetId();
    if (!tid) return;
    if (btnSave) btnSave.disabled = true;

    fetch(API + 'save-session', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target_id: tid }),
    })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        if (btnSave) btnSave.disabled = false;
        if (data.ok) {
          showToast('Сессия сохранена', 'success');
        } else {
          showToast('Ошибка: ' + (data.error || 'Не удалось сохранить'), 'error');
        }
      })
      .catch(function () {
        if (btnSave) btnSave.disabled = false;
        showToast('Ошибка соединения', 'error');
      });
  }

  function _showPipelineWidget() {
    active = true;
    firstFrame = false;
    overlay.style.display = 'flex';
    overlay.textContent = 'Публикация…';
    connectStream();
  }

  setInterval(function () {
    if (active) return;
    var panel = document.getElementById('panel-publish');
    if (!panel || !panel.classList.contains('active')) return;
    var tid = getTargetId();
    if (!tid) return;
    fetch(API + 'status?target_id=' + encodeURIComponent(tid))
      .then(function (r) { return r.json(); })
      .then(function (data) {
        var st = data && data.browser && data.browser.status;
        if (st === 'running' && !active) {
          _showPipelineWidget();
        }
      })
      .catch(function () {});
  }, 2500);

  window.addEventListener('beforeunload', function () {
    if (active) {
      fetch(API + 'stop', { method: 'POST', keepalive: true }).catch(function () {});
    }
  });

  var _origSwitchPanel = window.switchPanel;
  window.switchPanel = function (name) {
    if (_origSwitchPanel) _origSwitchPanel(name);
    if (name === 'publish' && !active && autoStart) {
      browserOpen();
    }
  };

  document.addEventListener('DOMContentLoaded', function () {
    var panel = document.getElementById('panel-publish');
    if (panel && panel.classList.contains('active') && !active && autoStart) {
      browserOpen();
    }
  });

  window[slug + 'BrowserOpen']        = browserOpen;
  window[slug + 'BrowserSaveSession'] = saveSession;
}
