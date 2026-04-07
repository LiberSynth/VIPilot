/* Dzen browser widget — авторизация через встроенный браузер */

(function () {
  var VIEWPORT_W = 900;
  var VIEWPORT_H = 680;

  var canvas = document.getElementById('dzen-browser-canvas');
  var wrap   = document.getElementById('dzen-browser-wrap');
  var overlay = document.getElementById('dzen-browser-overlay');
  var hint   = document.getElementById('dzen-browser-hint');
  var statusText = document.getElementById('dzen-browser-status-text');
  var btnSave  = document.getElementById('dzen-btn-save');
  var btnClose = document.getElementById('dzen-btn-close');
  var sessionStatus = document.getElementById('dzen-session-status');

  if (!canvas) return;

  var ctx = canvas.getContext('2d');
  canvas.width  = VIEWPORT_W;
  canvas.height = VIEWPORT_H;

  var sse = null;
  var active = false;
  var firstFrame = false;

  function getTargetId() {
    var el = document.getElementById('dzen_target_id');
    return el ? el.value : '';
  }

  function setStatusText(msg, color) {
    if (!statusText) return;
    statusText.style.display = msg ? '' : 'none';
    statusText.style.color = color || '#aaa';
    statusText.textContent = msg;
  }

  function sendEvent(ev) {
    if (!active) return;
    fetch('/api/dzen-browser/event', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(ev),
    }).catch(function () {});
  }

  /* ── Mouse events ── */
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

  /* ── Keyboard events (captured while canvas focused) ── */
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

  /* ── SSE frame rendering ── */
  function connectStream() {
    if (sse) { sse.close(); sse = null; }
    firstFrame = false;
    sse = new EventSource('/api/dzen-browser/stream');

    sse.onmessage = function (e) {
      var data = e.data;
      if (data === 'STOPPED') {
        setStatusText('Браузер остановлен.', '#aaa');
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
          setStatusText('', '');
        }
      };
      img.src = 'data:image/jpeg;base64,' + data;
    };

    sse.onerror = function () {
      if (!active) return;
      setStatusText('Ошибка соединения с браузером.', '#ff6b6b');
    };
  }

  function handleStopped() {
    active = false;
    if (sse) { sse.close(); sse = null; }
    wrap.style.display = 'none';
    hint.style.display = 'none';
    btnSave.style.display = 'none';
    btnClose.style.display = 'none';
  }

  /* ── Public actions ── */
  window.dzenBrowserOpen = function () {
    if (active) return;
    var tid = getTargetId();
    if (!tid) {
      setStatusText('Таргет Дзен не найден.', '#ff6b6b');
      statusText.style.display = '';
      return;
    }
    setStatusText('Запускаю браузер…', '#aaa');
    statusText.style.display = '';

    fetch('/api/dzen-browser/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target_id: tid }),
    })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        if (data.ok) {
          active = true;
          firstFrame = false;
          overlay.style.display = 'flex';
          overlay.textContent = 'Загрузка…';
          wrap.style.display = '';
          hint.style.display = '';
          btnSave.style.display = '';
          btnClose.style.display = '';
          setStatusText('Браузер запускается…', '#aaa');
          connectStream();
        } else {
          setStatusText('Ошибка запуска браузера: ' + (data.error || ''), '#ff6b6b');
        }
      })
      .catch(function () {
        setStatusText('Сетевая ошибка.', '#ff6b6b');
      });
  };

  window.dzenBrowserSaveSession = function () {
    var tid = getTargetId();
    if (!tid) return;
    btnSave.disabled = true;
    setStatusText('Сохраняю сессию…', '#aaa');

    fetch('/api/dzen-browser/save-session', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target_id: tid }),
    })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        btnSave.disabled = false;
        if (data.ok) {
          setStatusText('', '');
          showToast('Сессия сохранена', 'success');
          if (sessionStatus) {
            var now = new Date().toISOString().slice(0, 16).replace('T', ' ');
            sessionStatus.style.color = '#69db7c';
            sessionStatus.textContent = 'Сессия сохранена: ' + now + ' UTC';
          }
        } else {
          showToast('Ошибка: ' + (data.error || 'Не удалось сохранить'), 'error');
        }
      })
      .catch(function () {
        btnSave.disabled = false;
        showToast('Ошибка соединения', 'error');
      });
  };

  window.dzenBrowserClose = function () {
    fetch('/api/dzen-browser/stop', { method: 'POST' }).catch(function () {});
    handleStopped();
  };

  /* ── Авто-открытие при переходе на вкладку Публикация ── */
  var _origSwitchPanel = window.switchPanel;
  window.switchPanel = function (name) {
    if (_origSwitchPanel) _origSwitchPanel(name);
    if (name === 'publish' && !active) {
      window.dzenBrowserOpen();
    }
  };

  /* Открыть сразу, если страница загружена уже на вкладке publish */
  document.addEventListener('DOMContentLoaded', function () {
    var panel = document.getElementById('panel-publish');
    if (panel && panel.classList.contains('active') && !active) {
      window.dzenBrowserOpen();
    }
  });
})();
