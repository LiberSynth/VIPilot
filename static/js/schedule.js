(function() {
  function renderSchedule(times) {
    const list = document.getElementById('schedule-list');
    if (!list) return;
    list.innerHTML = '';
    if (!Array.isArray(times) || times.length === 0) {
      list.innerHTML = '<div style="font-size:13px;color:#555;padding:4px 0;">Нет добавленных времён</div>';
      return;
    }
    times.slice().sort(function(a, b) {
      return a.time_msk.localeCompare(b.time_msk);
    }).forEach(function(t) {
      const row = document.createElement('div');
      row.className = 'pt-item-row';
      row.dataset.id = t.id;
      row.innerHTML =
        '<div class="pt-val-box">' + t.time_msk + '</div>' +
        '<button type="button" class="pt-btn-remove" onclick="deleteScheduleSlot(this,\'' + t.id + '\')">Удалить</button>';
      list.appendChild(row);
    });
  }

  function loadSchedule() {
    const list = document.getElementById('schedule-list');
    if (list) list.innerHTML = '<div style="font-size:13px;color:#555;padding:4px 0;">Загрузка…</div>';
    return fetch('/api/schedule')
      .then(function(r) { return r.json(); })
      .then(renderSchedule)
      .catch(function() {
        if (list) list.innerHTML = '<div style="font-size:13px;color:#555;padding:4px 0;">Ошибка загрузки</div>';
      });
  }

  function loadScheduleSilent() {
    return fetch('/api/schedule')
      .then(function(r) { return r.json(); })
      .then(renderSchedule)
      .catch(function() {});
  }

  function updateSaveBtn() {
    const input = document.getElementById('new-schedule-time');
    const btn   = document.getElementById('pt-save-btn');
    if (btn) btn.disabled = !input || !input.value;
  }

  function runNow() {
    const btn = document.getElementById('btn-run-now');
    if (btn) { btn.disabled = true; btn.textContent = 'Запускается…'; }
    fetch('/api/run-now', { method: 'POST' })
      .then(r => r.json())
      .then(d => {
        if (d.ok) {
          if (btn) { btn.textContent = 'Запущено'; }
          setTimeout(() => {
            if (btn) { btn.disabled = false; btn.textContent = 'Запустить сейчас'; }
          }, 3000);
        } else {
          alert('Ошибка: ' + (d.error || 'неизвестная ошибка'));
          if (btn) { btn.disabled = false; btn.textContent = 'Запустить сейчас'; }
        }
      })
      .catch(() => {
        alert('Ошибка соединения');
        if (btn) { btn.disabled = false; btn.textContent = 'Запустить сейчас'; }
      });
  }

  function addScheduleSlot() {
    const input = document.getElementById('new-schedule-time');
    const btn   = document.getElementById('pt-save-btn');
    if (!input || !input.value) return;
    const val = input.value;
    input.value = '';
    if (btn) btn.disabled = true;
    fetch('/api/schedule', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({time: val})
    })
      .then(function(r) { return r.json(); })
      .then(function() { return loadSchedule(); })
      .catch(function() {});
  }

  window.runNow          = runNow;
  window.addScheduleSlot = addScheduleSlot;

  window.deleteScheduleSlot = function(btn, id) {
    var row = btn.closest('.pt-item-row');
    if (row) row.remove();
    fetch('/api/schedule/' + id, {method: 'DELETE'})
      .then(function(r) { return r.json(); })
      .then(function() { return loadScheduleSilent(); })
      .catch(function() {});
  };

  (function() {
    const input = document.getElementById('new-schedule-time');
    if (input) input.addEventListener('input', updateSaveBtn);
  })();

  (function() {
    const panel = document.getElementById('panel-pipeline');
    if (panel && panel.classList.contains('active')) loadSchedule();
  })();

  var _origSwitchPanel = window.switchPanel;
  window.switchPanel = function(name) {
    if (_origSwitchPanel) _origSwitchPanel(name);
    if (name === 'pipeline') loadSchedule();
  };
})();
