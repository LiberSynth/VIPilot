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

function _syncUseDonorDisabled() {
  var approveMoviesChk = document.getElementById('approve_movies_check');
  var useDonorChk      = document.getElementById('use_donor_check');
  if (!approveMoviesChk || !useDonorChk) return;
  useDonorChk.disabled = approveMoviesChk.checked;
}

function wfApproveMovies(checked) {
  fetch('/api/workflow/approve_movies', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ enabled: checked ? '1' : '0' }),
  })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var badge = document.getElementById('approve-movies-badge');
      var on    = d.approve_movies === '1';
      if (badge) badge.style.display = on ? '' : 'none';
      if (on) {
        var useDonorChk   = document.getElementById('use_donor_check');
        var useDonorBadge = document.getElementById('use-donor-badge');
        if (useDonorChk && !useDonorChk.checked) {
          useDonorChk.checked = true;
          if (useDonorBadge) useDonorBadge.style.display = '';
          wfUseDonor(true);
        }
      }
      _syncUseDonorDisabled();
      showToast(on ? 'Утверждать видео: включено' : 'Утверждать видео: выключено', on ? 'warn' : 'success');
    })
    .catch(function() { showToast('Ошибка соединения', 'error'); });
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
  _syncUseDonorDisabled();
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

function openCreateBackupDialog(btn) {
  if (!btn || btn.disabled) return;
  var origText = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Создаём бэкап…';
  fetch('/api/db_backup', { method: 'GET' })
    .then(function(r) {
      if (!r.ok) {
        return r.json().then(
          function(d) { throw new Error(d.error || ('Ошибка ' + r.status)); },
          function()  { throw new Error('Ошибка ' + r.status); }
        );
      }
      var disp = r.headers.get('Content-Disposition') || '';
      var m = disp.match(/filename="?([^";]+)"?/);
      var fname = (m && m[1].trim()) || 'vipilot-backup.dump';
      return r.blob().then(function(b) { return { blob: b, fname: fname }; });
    })
    .then(function(res) {
      var url = URL.createObjectURL(res.blob);
      var a = document.createElement('a');
      a.href = url; a.download = res.fname;
      document.body.appendChild(a); a.click();
      setTimeout(function() { URL.revokeObjectURL(url); a.remove(); }, 200);
      var mb = (res.blob.size / (1024 * 1024)).toFixed(2);
      showToast('Бэкап создан: ' + res.fname + ' (' + mb + ' МБ)', 'success');
    })
    .catch(function(e) { showToast('Ошибка бэкапа: ' + (e.message || e), 'error'); })
    .finally(function() { btn.disabled = false; btn.textContent = origText; });
}

function refreshDbRestoreStatus() {
  var btn = document.getElementById('btn-db-restore');
  if (!btn) return;
  fetch('/api/db_restore/status')
    .then(function(r) { return r.json(); })
    .then(function(d) {
      if (d && d.in_progress) {
        btn.disabled = true;
        btn.textContent = 'Восстанавливаем…';
      } else if (!btn.dataset.busy) {
        btn.disabled = false;
        btn.textContent = 'Восстановить из бэкапа';
      }
    })
    .catch(function() {});
}

function _doRestoreUpload(file, btn, origText) {
  btn.disabled = true;
  btn.dataset.busy = '1';
  btn.textContent = 'Восстанавливаем…';
  fetch('/api/db_restore', {
    method: 'POST',
    headers: { 'Content-Type': 'application/octet-stream' },
    body:    file,
  })
    .then(function(r) { return r.json().then(function(d) { return { r: r, d: d }; }); })
    .then(function(res) {
      if (res.r.ok && res.d.ok) {
        showToast('База восстановлена из бэкапа', 'success');
      } else {
        showToast('Ошибка восстановления: ' + (res.d.error || ('код ' + res.r.status)), 'error');
      }
    })
    .catch(function(e) { showToast('Ошибка соединения: ' + (e.message || e), 'error'); })
    .finally(function() {
      delete btn.dataset.busy;
      btn.disabled = false;
      btn.textContent = origText;
      refreshDbRestoreStatus();
    });
}

function openRestoreBackupDialog(btn) {
  if (!btn || btn.disabled) return;
  var origText = btn.textContent;
  var picker = document.createElement('input');
  picker.type = 'file';
  picker.accept = '.dump,application/octet-stream';
  picker.style.display = 'none';
  document.body.appendChild(picker);
  picker.addEventListener('change', function() {
    var file = picker.files && picker.files[0];
    picker.remove();
    if (!file) return;
    var PHRASE = 'Я осознанно подтверждаю действие';
    new ConfirmDialog({
      title: 'Восстановить базу из бэкапа?',
      text:
        'Текущая база будет полностью заменена содержимым файла «' + file.name + '». ' +
        'Операция атомарна: при ошибке текущая база останется нетронутой, но успешное восстановление откатить нельзя. ' +
        'Движок будет автоматически приостановлен на время восстановления.<br>' +
        'Для подтверждения введите текст "Я осознанно подтверждаю действие" ниже и нажмите Восстановить.<br>' +
        '<input type="text" id="_cd-guard-input" autocomplete="off">',
      confirmLabel: 'Восстановить',
      confirmStyle: 'background:#b05820',
      onConfirm: function(cBtn, dlg) {
        dlg.close();
        _doRestoreUpload(file, btn, origText);
      },
      triggerBtn: btn,
    }).open();
    var inp        = document.getElementById('_cd-guard-input');
    var confirmBtn = document.getElementById('_cd-confirm');
    if (confirmBtn) confirmBtn.disabled = true;
    if (inp) inp.addEventListener('input', function() {
      if (confirmBtn) confirmBtn.disabled = inp.value !== PHRASE;
    });
  });
  picker.click();
}

function openVacuumDbDialog() {
  new ConfirmDialog({
    title: 'Сжать базу данных?',
    text:
      'Запустит pg_repack по всем таблицам схемы public. Операция онлайн — приложение не блокируется, но может занять несколько минут и временно требует свободного места на диске. Не запускайте во время деплоя.',
    confirmLabel: 'Сжать',
    onConfirm: function(btn, dlg) {
      btn.disabled    = true;
      btn.textContent = 'Сжимаем…';
      fetch('/api/vacuum_db', { method: 'POST' })
        .then(function(r) { return r.json(); })
        .then(function(data) {
          dlg.close();
          var res  = data.result || {};
          var mb   = ((res.freed_bytes || 0) / (1024 * 1024)).toFixed(1);
          var base = 'таблиц: ' + (res.tables_ok || 0) + '/' + (res.tables_total || 0)
                   + ', освобождено: ' + mb + ' МБ';
          if (data.ok) {
            showToast('БД сжата (' + base + ')', 'success');
          } else {
            showToast('Сжатие с ошибками: ' + (data.error || 'неизвестная ошибка') + ' (' + base + ')', 'error');
          }
        })
        .catch(function() { dlg.close(); showToast('Ошибка соединения', 'error'); });
    },
  }).open();
}
