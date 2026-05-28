function wfUpdateUI(state, activeThreads) {
  var dot         = document.getElementById('wf-dot');
  var text        = document.getElementById('wf-state-text');
  var threadsText = document.getElementById('wf-threads-text');
  var btnStart    = document.getElementById('wf-btn-start');
  var btnPause    = document.getElementById('wf-btn-pause');
  if (state === 'running') {
    dot.className     = 'wf-dot running';
    text.textContent  = 'Работает';
    btnStart.disabled = true;
    btnPause.disabled = false;
  } else {
    dot.className     = 'wf-dot pause';
    text.textContent  = 'Приостановлен';
    btnStart.disabled = false;
    btnPause.disabled = true;
  }
  if (threadsText) {
    var n = (typeof activeThreads === 'number') ? activeThreads : 0;
    threadsText.textContent = '(выполняется потоков: ' + n + ')';
    threadsText.style.color = n > 0 ? '#c8ccff' : '#7878c8';
  }
}

function refreshWorkflowState() {
  var panel = document.getElementById('panel-service');
  if (!panel || !panel.classList.contains('active')) return;
  fetch('/api/workflow/state')
    .then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.state) wfUpdateUI(d.state, d.active_threads || 0);
    })
    .catch(function() {});
}

(function() {
  refreshWorkflowState();
  setInterval(refreshWorkflowState, 5000);
})();

function wfStart() {
  var btn = document.getElementById('wf-btn-start');
  btn.disabled = true;
  fetch('/api/workflow/start', { method: 'POST' })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.ok) { wfUpdateUI('running'); showToast('Движок запущен', 'success'); }
    })
    .catch(function() { btn.disabled = false; showToast('Ошибка соединения', 'error'); });
}

function wfPause() {
  var btn = document.getElementById('wf-btn-pause');
  btn.disabled = true;
  fetch('/api/workflow/pause', { method: 'POST' })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      if (d.ok) { wfUpdateUI('pause'); showToast('Движок приостановлен', 'success'); }
    })
    .catch(function() { btn.disabled = false; showToast('Ошибка соединения', 'error'); });
}

function wfDeepDebugging(checked) {
  fetch('/api/workflow/deep_debugging', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ enabled: checked ? '1' : '0' }),
  })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var badge = document.getElementById('deep-debugging-badge');
      var on    = d.deep_debugging === '1';
      if (badge) badge.style.display = on ? '' : 'none';
      showToast(on ? 'Глубокая отладка: включена' : 'Глубокая отладка: выключена', 'success');
    })
    .catch(function() { showToast('Ошибка соединения', 'error'); });
}

function wfUseDonor(checked) {
  fetch('/api/workflow/use_donor', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ enabled: checked ? '1' : '0' }),
  })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var badge = document.getElementById('use-donor-badge');
      var on    = d.use_donor === '1';
      if (badge) badge.style.display = on ? '' : 'none';
      showToast(on ? 'Подбирать видео из пула: включено' : 'Подбирать видео из пула: выключено', 'success');
    })
    .catch(function() { showToast('Ошибка соединения', 'error'); });
}

function openRestartDialog() {
  new ConfirmDialog({
    title: 'Перезапустить движок?',
    text:
      'Приложение будет остановлено и автоматически перезапущено.<br><br>' +
      'Текущие задачи будут прерваны. Состояние движка (работает/приостановлен) сохранится.',
    confirmLabel: 'Перезапустить',
    onConfirm: function(btn, dlg) {
      btn.disabled    = true;
      btn.textContent = 'Перезапуск…';
      fetch('/api/workflow/restart', { method: 'POST' })
        .then(function(r) { return r.json(); })
        .then(function() {
          dlg.close();
          showToast('Перезапуск… страница обновится автоматически', 'success');
          setTimeout(function() { location.reload(); }, 4000);
        })
        .catch(function() { dlg.close(); showToast('Ошибка соединения', 'error'); });
    },
  }).open();
}

function _isDonorPanelActive() {
  var el = document.getElementById('donor-count');
  if (!el) return false;
  var panel = el.closest('.tab-panel');
  return panel ? panel.classList.contains('active') : false;
}

function refreshDonorCount() {
  if (!_isDonorPanelActive()) return;
  fetch('/api/donors/count')
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var el = document.getElementById('donor-count');
      if (el) el.textContent = d.count;
    })
    .catch(function() {});
}

function refreshMoviePoolCount() {
  if (!_isDonorPanelActive()) return;
  fetch('/api/donors/count?good_only=1')
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var el = document.getElementById('movie-pool-count');
      if (el) el.textContent = d.count;
    })
    .catch(function() {});
}

(function() {
  refreshDonorCount();
  refreshMoviePoolCount();
  setInterval(refreshDonorCount, 10000);
  setInterval(refreshMoviePoolCount, 10000);
})();

function openClearHistoryDialog() {
  var PHRASE = 'Я осознанно подтверждаю действие';
  new ConfirmDialog({
    title: 'Очистить всю историю?',
    text:
      'Будут удалены все батчи (включая активные), все логи и записи лога. Сюжеты не затрагиваются. Действие нельзя отменить.<br>' +
      'Для подтверждения действия введите текст "Я осознанно подтверждаю действие" ниже и нажмите кнопку Очистить.<br>' +
      '<input type="text" id="_cd-guard-input" autocomplete="off">',
    confirmLabel: 'Очистить',
    confirmStyle: 'background:#b05820',
    onConfirm: function(btn, dlg) {
      btn.disabled    = true;
      btn.textContent = 'Удаляем…';
      fetch('/api/clear_history', { method: 'POST' })
        .then(function(r) { return r.json(); })
        .then(function(data) {
          dlg.close();
          if (data.ok) {
            var del   = data.deleted || {};
            var parts = [];
            if (del.logs)        parts.push('логов: '   + del.logs);
            if (del.log_entries) parts.push('записей: ' + del.log_entries);
            if (del.batches)     parts.push('батчей: '  + del.batches);
            if (del.stories)     parts.push('сюжетов: ' + del.stories);
            showToast('История очищена' + (parts.length ? ': ' + parts.join(', ') : ''), 'success');
          } else {
            showToast('Ошибка: ' + (data.error || 'неизвестная ошибка'), 'error');
          }
        })
        .catch(function() { dlg.close(); showToast('Ошибка соединения', 'error'); });
    },
  }).open();
  var inp        = document.getElementById('_cd-guard-input');
  var confirmBtn = document.getElementById('_cd-confirm');
  confirmBtn.disabled = true;
  inp.addEventListener('input', function() {
    confirmBtn.disabled = inp.value !== PHRASE;
  });
}


var VACUUM_DB_TIMEOUT_MS = 15 * 60 * 1000;

function fetchWithTimeout(url, options, timeoutMs) {
  var controller = typeof AbortController !== 'undefined' ? new AbortController() : null;
  var timer;
  var opts = options ? Object.assign({}, options) : {};
  if (controller) {
    opts.signal = controller.signal;
    timer = setTimeout(function() { controller.abort(); }, timeoutMs);
  }
  return fetch(url, opts).finally(function() {
    if (timer) clearTimeout(timer);
  });
}

function formatVacuumSummary(res) {
  var mb = ((res.freed_bytes || 0) / (1024 * 1024)).toFixed(1);
  var base = 'таблиц: ' + (res.tables_ok || 0) + '/' + (res.tables_total || 0)
           + ', освобождено: ' + mb + ' МБ';
  if (res.mode === 'vacuum_full_fallback') {
    return 'VACUUM FULL, ' + base;
  }
  if (res.mode === 'pg_repack') {
    return 'pg_repack, ' + base;
  }
  return base;
}

function openVacuumDbDialog() {
  new ConfirmDialog({
    title: 'Сжать базу данных?',
    text:
      'Сжатие всех таблиц схемы public. На Windows (VPS) — VACUUM FULL, таблицы кратковременно блокируются; '
      + 'при наличии pg_repack — онлайн. Может занять несколько минут, нужно свободное место на диске. '
      + 'Не закрывайте страницу до завершения. Не запускайте во время деплоя.',
    confirmLabel: 'Сжать',
    onConfirm: function(btn, dlg) {
      btn.disabled    = true;
      btn.textContent = 'Сжимаем…';
      fetchWithTimeout('/api/vacuum_db', { method: 'POST' }, VACUUM_DB_TIMEOUT_MS)
        .then(function(r) {
          if (!r.ok) {
            return r.text().then(function(body) {
              throw new Error('HTTP ' + r.status + (body ? ': ' + body.slice(0, 120) : ''));
            });
          }
          return r.json();
        })
        .then(function(data) {
          dlg.close();
          var res  = data.result || {};
          var base = formatVacuumSummary(res);
          if (data.ok) {
            showToast('БД сжата (' + base + ')', 'success');
          } else {
            showToast('Сжатие с ошибками: ' + (data.error || 'неизвестная ошибка') + ' (' + base + ')', 'error');
          }
        })
        .catch(function(err) {
          dlg.close();
          if (err && err.name === 'AbortError') {
            showToast(
              'Ответ в браузере не дождались (таймаут). Сжатие на сервере может ещё идти — '
              + 'проверьте лог: «Дефрагментация БД завершена».',
              'warn'
            );
            return;
          }
          var msg = (err && err.message) ? err.message : 'неизвестная ошибка';
          showToast('Ошибка запроса: ' + msg, 'error');
        });
    },
  }).open();
}
