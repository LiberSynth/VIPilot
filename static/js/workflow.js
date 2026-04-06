function showToast(msg, type) {
  var el = document.createElement('div');
  el.className = 'flash ' + (type || 'success');
  el.style.cssText = 'position:fixed;bottom:24px;left:50%;transform:translateX(-50%);z-index:2000;max-width:420px;width:90%;text-align:center;box-shadow:0 4px 24px rgba(0,0,0,.4);transition:opacity .3s';
  el.textContent = msg;
  document.body.appendChild(el);
  setTimeout(function() {
    el.style.opacity = '0';
    setTimeout(function() { el.remove(); }, 300);
  }, 3500);
}

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
    threadsText.style.color = n > 0 ? '#7878c8' : '#44445a';
  }
}

function _pluralThread(n) {
  var mod10  = n % 10;
  var mod100 = n % 100;
  if (mod10 === 1 && mod100 !== 11) return 'поток';
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 10 || mod100 >= 20)) return 'потока';
  return 'потоков';
}

function refreshWorkflowState() {
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
      showToast(on ? 'Использовать донора: включено' : 'Использовать донора: выключено', 'success');
    })
    .catch(function() { showToast('Ошибка соединения', 'error'); });
}

function wfEmulation(checked) {
  fetch('/api/workflow/emulation', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ enabled: checked ? '1' : '0' }),
  })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var badge = document.getElementById('emulation-badge');
      var card  = document.getElementById('wf-card');
      var on    = d.emulation_mode === '1';
      if (badge) badge.style.display = on ? '' : 'none';
      if (card)  card.classList.toggle('emulation-active', on);
      showToast(on ? 'Эмуляция включена' : 'Эмуляция выключена', on ? 'warn' : 'success');
    })
    .catch(function() { showToast('Ошибка соединения', 'error'); });
}

function _buildRestartOverlay() {
  var el = document.createElement('div');
  el.className = 'confirm-overlay open';
  el.id = 'restartOverlay';
  el.innerHTML =
    '<div class="confirm-box">' +
      '<div class="confirm-box-title">Перезапустить движок?</div>' +
      '<div class="confirm-box-text">' +
        'Приложение будет остановлено и автоматически перезапущено.<br><br>' +
        'Текущие задачи будут прерваны. Состояние движка (работает / приостановлен) сохранится.' +
      '</div>' +
      '<div class="confirm-box-btns">' +
        '<button class="confirm-cancel" onclick="closeRestartDialog()">Отмена</button>' +
        '<button class="confirm-confirm" id="restartConfirmBtn" onclick="confirmRestart()" style="background:#b05820">Перезапустить</button>' +
      '</div>' +
    '</div>';
  return el;
}

function openRestartDialog() {
  var existing = document.getElementById('restartOverlay');
  if (existing) existing.remove();
  document.body.appendChild(_buildRestartOverlay());
}

function closeRestartDialog() {
  var el = document.getElementById('restartOverlay');
  if (el) el.remove();
}

function confirmRestart() {
  var btn = document.getElementById('restartConfirmBtn');
  btn.disabled    = true;
  btn.textContent = 'Перезапуск…';
  fetch('/api/workflow/restart', { method: 'POST' })
    .then(function(r) { return r.json(); })
    .then(function() {
      closeRestartDialog();
      showToast('Перезапуск… страница обновится автоматически', 'success');
      setTimeout(function() { location.reload(); }, 4000);
    })
    .catch(function() { closeRestartDialog(); showToast('Ошибка соединения', 'error'); });
}

function refreshDonorCount() {
  fetch('/api/donors/count')
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var el = document.getElementById('donor-count');
      if (el) el.textContent = d.count;
    })
    .catch(function() {});
}

(function() {
  refreshDonorCount();
  setInterval(refreshDonorCount, 10000);
})();

function _buildClearHistoryOverlay() {
  var el = document.createElement('div');
  el.className = 'confirm-overlay open';
  el.id = 'clearHistoryOverlay';
  el.innerHTML =
    '<div class="confirm-box">' +
      '<div class="confirm-box-title">Очистить всю историю?</div>' +
      '<div class="confirm-box-text">' +
        'Будут удалены все логи (краткие и подробные) и все завершённые батчи со связанными сюжетами.<br><br>' +
        'Активные батчи (в очереди, в процессе генерации) не затрагиваются.<br><br>' +
        'Действие нельзя отменить.' +
      '</div>' +
      '<div class="confirm-box-btns">' +
        '<button class="confirm-cancel" onclick="closeClearHistoryDialog()">Отмена</button>' +
        '<button class="confirm-confirm" id="clearHistoryConfirmBtn" onclick="confirmClearHistory()">Очистить</button>' +
      '</div>' +
    '</div>';
  return el;
}

function openClearHistoryDialog() {
  var existing = document.getElementById('clearHistoryOverlay');
  if (existing) existing.remove();
  document.body.appendChild(_buildClearHistoryOverlay());
}

function closeClearHistoryDialog() {
  var el = document.getElementById('clearHistoryOverlay');
  if (el) el.remove();
}

function confirmClearHistory() {
  var btn = document.getElementById('clearHistoryConfirmBtn');
  btn.disabled    = true;
  btn.textContent = 'Удаляем…';
  fetch('/api/clear_history', { method: 'POST' })
    .then(function(r) { return r.json(); })
    .then(function(data) {
      closeClearHistoryDialog();
      if (data.ok) {
        var d     = data.deleted || {};
        var parts = [];
        if (d.logs)        parts.push('логов: '   + d.logs);
        if (d.log_entries) parts.push('записей: ' + d.log_entries);
        if (d.batches)     parts.push('батчей: '  + d.batches);
        if (d.stories)     parts.push('сюжетов: ' + d.stories);
        showToast('История очищена' + (parts.length ? ': ' + parts.join(', ') : ''), 'success');
      } else {
        showToast('Ошибка: ' + (data.error || 'неизвестная ошибка'), 'error');
      }
    })
    .catch(function() {
      closeClearHistoryDialog();
      showToast('Ошибка соединения', 'error');
    });
}
