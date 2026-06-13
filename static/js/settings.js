const ta    = document.getElementById('ta');
const cc    = document.getElementById('charcount');
const taSys = document.getElementById('ta_formatprompt');
const ccSys = document.getElementById('charcount_formatprompt');

function updateCount()    { if (ta    && cc)    cc.textContent    = ta.value.length    + ' символов'; }
function updateSysCount() { if (taSys && ccSys) ccSys.textContent = taSys.value.length + ' символов'; }

if (ta)    { ta.addEventListener('input',    updateCount);    updateCount(); }
if (taSys) { taSys.addEventListener('input', updateSysCount); updateSysCount(); }

(function() {
  var KEY_SYS  = 'rbc_format_prompt_h';
  var KEY_META = 'rbc_text_prompt_h';

  function applyHeight(el, key, defaultPx) {
    var saved = parseInt(localStorage.getItem(key), 10);
    el.style.height = (saved > 0 ? saved : defaultPx) + 'px';
  }

  function watchHeight(el, key) {
    var saveTimer = null;
    new ResizeObserver(function() {
      clearTimeout(saveTimer);
      saveTimer = setTimeout(function() {
        var h = Math.round(el.offsetHeight);
        if (h > 30) localStorage.setItem(key, h);
      }, 250);
    }).observe(el);
  }

  if (taSys) { applyHeight(taSys, KEY_SYS,  100); watchHeight(taSys, KEY_SYS); }
  if (ta)    { applyHeight(ta,    KEY_META, 300); watchHeight(ta,    KEY_META); }
})();

function collectAllSettings(activeTab) {
  const data = new FormData();
  data.set('active_tab', activeTab || 'pipeline');
  const setIfExists = (key, id) => { const el = document.getElementById(id); if (el) data.set(key, el.value); };
  if (ta)    data.set('text_prompt',   ta.value);
  if (taSys) data.set('format_prompt', taSys.value);
  setIfExists('video_duration',      'video_duration');
  setIfExists('video_post_prompt',   'ta_postprompt');
  setIfExists('story_fails_to_next', 'story_fails_to_next');
  setIfExists('video_fails_to_next', 'video_fails_to_next');
  setIfExists('target_id',        'target_id');
  setIfExists('notify_email',     'notify_email');
  setIfExists('notify_phone',     'notify_phone');
  setIfExists('entries_lifetime', 'entries_lifetime');
  setIfExists('log_lifetime',     'log_lifetime');
  setIfExists('batch_lifetime',   'batch_lifetime');
  setIfExists('buffer_minutes',    'buffer_minutes');
  setIfExists('loop_interval',    'loop_interval');
  setIfExists('max_batch_threads', 'max_batch_threads');
  setIfExists('max_model_passes',  'max_model_passes');
  return data;
}

var _requestSaveTimer = null;
function saveRequestSettings() {
  fetch('/save', { method: 'POST', body: collectAllSettings('request') }).catch(() => {});
}
function scheduleRequestSave() {
  clearTimeout(_requestSaveTimer);
  _requestSaveTimer = setTimeout(saveRequestSettings, 800);
}

var _storySaveTimer = null;
function saveStorySettings() {
  fetch('/save', { method: 'POST', body: collectAllSettings('story') }).catch(() => {});
}
function scheduleStorySave() {
  clearTimeout(_storySaveTimer);
  _storySaveTimer = setTimeout(saveStorySettings, 800);
}

var _publishSaveTimer = null;
function savePublishSettings() {
  fetch('/save', { method: 'POST', body: collectAllSettings('publish') }).catch(() => {});
}
function schedulePublishSave() {
  clearTimeout(_publishSaveTimer);
  _publishSaveTimer = setTimeout(savePublishSettings, 600);
}

var _serviceSaveTimer = null;
function saveServiceSettings() {
  fetch('/save', { method: 'POST', body: collectAllSettings('service') }).catch(() => {});
}
function scheduleServiceSave() {
  clearTimeout(_serviceSaveTimer);
  _serviceSaveTimer = setTimeout(saveServiceSettings, 800);
}

function validateLifetimes() {
  const ll  = document.getElementById('entries_lifetime');
  const sll = document.getElementById('log_lifetime');
  const bl  = document.getElementById('batch_lifetime');
  if (!ll || !sll || !bl) return true;
  const v_ll  = parseInt(ll.value)  || 0;
  const v_sll = parseInt(sll.value) || 0;
  const v_bl  = parseInt(bl.value)  || 0;
  const llInvalid  = v_ll  > v_sll;
  const sllInvalid = v_sll < v_ll || v_sll > v_bl;
  const blInvalid  = v_bl  < v_sll;
  ll.style.outline  = llInvalid  ? '2px solid #ff6060' : '';
  sll.style.outline = sllInvalid ? '2px solid #ff6060' : '';
  bl.style.outline  = blInvalid  ? '2px solid #ff6060' : '';
  return !llInvalid && !sllInvalid && !blInvalid;
}

(function() {
  const requestFields = [
    document.getElementById('video_duration'),
    document.getElementById('video_fails_to_next'),
    document.getElementById('ta_postprompt'),
  ].filter(Boolean);
  requestFields.forEach(f => {
    f.addEventListener('input',  scheduleRequestSave);
    f.addEventListener('change', scheduleRequestSave);
  });

  const storyFields = [
    ta,
    taSys,
    document.getElementById('story_fails_to_next'),
  ].filter(Boolean);
  storyFields.forEach(f => {
    f.addEventListener('input',  scheduleStorySave);
    f.addEventListener('change', scheduleStorySave);
  });

  const serviceFields = [
    document.getElementById('notify_email'),
    document.getElementById('notify_phone'),
    document.getElementById('buffer_minutes'),
    document.getElementById('loop_interval'),
    document.getElementById('max_batch_threads'),
  ].filter(Boolean);
  serviceFields.forEach(f => {
    f.addEventListener('input',  scheduleServiceSave);
    f.addEventListener('change', scheduleServiceSave);
  });

  ['entries_lifetime', 'log_lifetime', 'batch_lifetime'].forEach(id => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener('input',  validateLifetimes);
    el.addEventListener('change', () => { if (validateLifetimes()) scheduleServiceSave(); });
  });
})();

var _pubCounterTimer = null;
function _savePublicationCounter() {
  const el = document.getElementById('publication_counter');
  if (!el) return;
  const val = parseInt(el.value);
  if (isNaN(val) || val < 0) return;
  fetch('/api/publication-counter/set', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ value: val }),
  })
    .then(r => r.json())
    .then(data => { if (!data.ok) showToast('Ошибка: ' + (data.error || 'неизвестная'), 'error'); })
    .catch(() => showToast('Ошибка запроса', 'error'));
}
function _schedulePubCounterSave() {
  clearTimeout(_pubCounterTimer);
  _pubCounterTimer = setTimeout(_savePublicationCounter, 800);
}
(function() {
  const el = document.getElementById('publication_counter');
  if (!el) return;
  el.addEventListener('input',  _schedulePubCounterSave);
  el.addEventListener('change', _schedulePubCounterSave);
})();

function _backupYamlBasename(file) {
  var raw = (file && (file.webkitRelativePath || file.name)) || '';
  var parts = raw.split(/[/\\]/);
  return parts[parts.length - 1] || raw;
}

function _backupTableFromFile(file) {
  return _backupYamlBasename(file).replace(/\.(yaml|yml)$/i, '');
}

async function downloadBackup(btn) {
  var tables;
  try {
    var rTables = await fetch('/api/export-backup/tables');
    if (!rTables.ok) throw new Error('tables: http ' + rTables.status);
    tables = await rTables.json();
  } catch (e) {
    if (typeof window.showToast === 'function') window.showToast('Ошибка получения списка файлов');
    return;
  }

  var total = tables.length;
  btn.disabled = true;
  var dlg = new ExportMoviesDialog({
    total: total,
    title: 'Выгрузка данных',
    onRetry: function(failedItems) { _retryDownloadBackup(btn, failedItems); },
  });
  dlg.open();

  var done = 0;
  var failedItems = [];

  for (var i = 0; i < tables.length; i++) {
    if (dlg.isCancelled()) break;
    var table = tables[i];
    var filename = table + '.yaml';
    dlg.setProgress(done, filename, i + 1);
    try {
      var tr = await fetch('/api/export-backup/' + encodeURIComponent(table));
      if (!tr.ok) throw new Error('http ' + tr.status);
      var blob = await tr.blob();
      var url = URL.createObjectURL(blob);
      var a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
      done++;
      dlg.setProgress(done, filename);
    } catch (e) {
      if (dlg.isCancelled()) break;
      failedItems.push({ filename: filename, reason: 'ошибка скачивания' });
    }
  }

  btn.disabled = false;
  dlg.finish(done, dlg.isCancelled(), failedItems);
}

function uploadBackup(btn) {
  new ConfirmDialog({
    title: 'Загрузить все данные?',
    text:
      'База будет синхронизирована с YAML-файлами из выбранной папки: ' +
      'записи обновятся, отсутствующие в папке — удалятся.<br><br>' +
      'Перед загрузкой рекомендуется остановить движок.',
    confirmLabel: 'Выбрать папку',
    triggerBtn: btn,
    onConfirm: function(_confirmBtn, dlg) {
      dlg.close();
      _openBackupUploadPicker(btn);
    },
  }).open();
}

function _openBackupUploadPicker(btn) {
  if (typeof window.showDirectoryPicker === 'function') {
    window.showDirectoryPicker({ mode: 'read' })
      .then(function(dirHandle) {
        var yamlFiles = [];
        return _collectYamlFromDirHandle(dirHandle, yamlFiles).then(function() {
          return _doUploadBackup(btn, yamlFiles);
        });
      })
      .catch(function(e) {
        if (e && e.name === 'AbortError') return;
        if (typeof window.showToast === 'function') {
          window.showToast('Не удалось прочитать папку: ' + ((e && e.message) || e), 'error');
        }
      });
    return;
  }

  var input = document.getElementById('_backup-upload-input');
  if (!input) return;
  input.value = '';
  input.onchange = function() { _doUploadBackup(btn, input.files); };
  input.click();
}

async function _collectYamlFromDirHandle(dirHandle, out) {
  for await (var entry of dirHandle.values()) {
    if (entry.kind === 'file') {
      if (/\.(yaml|yml)$/i.test(entry.name)) {
        out.push(await entry.getFile());
      }
    } else if (entry.kind === 'directory') {
      await _collectYamlFromDirHandle(entry, out);
    }
  }
}

async function _doUploadBackup(btn, files) {
  var yamlFiles = [];
  for (var i = 0; i < files.length; i++) {
    var f = files[i];
    var lower = (f.name || '').toLowerCase();
    if (lower.endsWith('.yaml') || lower.endsWith('.yml')) yamlFiles.push(f);
  }

  var total = yamlFiles.length;
  if (total === 0) {
    if (typeof window.showToast === 'function') window.showToast('Нет подходящих YAML-файлов');
    return;
  }

  btn.disabled = true;
  var dlg = new ExportMoviesDialog({
    total: total,
    title: 'Загрузка данных',
    doneVerb: 'Загружено',
    failedVerb: 'загрузить',
  });
  dlg.open();

  var done = 0;
  var failedItems = [];

  yamlFiles.sort(function(a, b) {
    return _backupTableFromFile(a).localeCompare(_backupTableFromFile(b));
  });

  for (var i = 0; i < yamlFiles.length; i++) {
    if (dlg.isCancelled()) break;
    var f = yamlFiles[i];
    var basename = _backupYamlBasename(f);
    var table = _backupTableFromFile(f);
    dlg.setProgress(done, basename, i + 1);
    try {
      var fd = new FormData();
      fd.append('table', table);
      fd.append('file', f);
      var r = await fetch('/api/import-backup/table', { method: 'POST', body: fd });
      if (!r.ok) {
        var errMsg = 'http ' + r.status;
        try {
          var errBody = await r.json();
          if (errBody && errBody.error) errMsg = errBody.error;
        } catch (ignore) {}
        throw new Error(errMsg);
      }
      done++;
      dlg.setProgress(done, basename);
    } catch (e) {
      if (dlg.isCancelled()) break;
      failedItems.push({
        filename: basename,
        reason: (e && e.message) ? e.message : 'ошибка загрузки',
      });
    }
  }

  btn.disabled = false;
  dlg.finish(done, dlg.isCancelled(), failedItems);
}

async function _retryDownloadBackup(btn, failedItems) {
  btn.disabled = true;
  var dlg = new ExportMoviesDialog({
    total: failedItems.length,
    title: 'Повтор ошибок',
    onRetry: function(items) { _retryDownloadBackup(btn, items); },
  });
  dlg.open();

  var done = 0;
  var newFailed = [];

  for (var i = 0; i < failedItems.length; i++) {
    if (dlg.isCancelled()) break;
    var filename = failedItems[i].filename;
    dlg.setProgress(done, filename, i + 1);
    try {
      var table = filename.replace(/\.(yaml|yml)$/i, '');
      if (!table) throw new Error('bad filename');
      var r = await fetch('/api/export-backup/' + encodeURIComponent(table));
      if (!r.ok) throw new Error('http ' + r.status);
      var blob = await r.blob();
      var url = URL.createObjectURL(blob);
      var a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
      done++;
      dlg.setProgress(done, filename);
    } catch (e) {
      if (dlg.isCancelled()) break;
      newFailed.push({ filename: filename, reason: 'ошибка скачивания' });
    }
  }

  btn.disabled = false;
  dlg.finish(done, dlg.isCancelled(), newFailed);
}

function downloadUpdatePackage(btn) {
  btn.disabled = true;
  const a = document.createElement('a');
  a.href = '/api/export-update-package';
  a.download = 'update_package.yaml';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  setTimeout(() => { btn.disabled = false; }, 2000);
}

function uploadUpdatePackage(btn) {
  const input = document.getElementById('upload-package-input');
  input.value = '';
  input.onchange = function() {
    const file = input.files[0];
    if (!file) return;
    btn.disabled = true;
    const fd = new FormData();
    fd.append('file', file);
    fetch('/api/import-update-package', { method: 'POST', body: fd })
      .then(r => r.json())
      .then(data => {
        if (data.ok) {
          const s = data.summary;
          const lines = Object.entries(s).map(([t, v]) =>
            `${t}: +${v.inserted} ~${v.updated} -${v.deleted}`
          );
          showToast('Пакет загружен:\n' + lines.join('\n'), 'success');
        } else {
          showToast('Ошибка: ' + (data.error || 'неизвестная'), 'error');
        }
      })
      .catch(() => showToast('Ошибка загрузки файла', 'error'))
      .finally(() => { btn.disabled = false; });
  };
  input.click();
}

(function() {
  var _bodyTimers = {};

  function _saveTargetBodyIfValid(areaEl, errorEl, targetId) {
    var val = areaEl.value;
    if (!val.trim()) {
      areaEl.classList.remove('input-error');
      if (errorEl) { errorEl.style.display = 'none'; errorEl.textContent = ''; }
      return;
    }
    var parsed;
    try {
      parsed = JSON.parse(val);
      if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) throw new Error('not an object');
    } catch (e) {
      areaEl.classList.add('input-error');
      if (errorEl) { errorEl.style.display = 'block'; errorEl.textContent = 'Невалидный JSON'; }
      return;
    }
    areaEl.classList.remove('input-error');
    if (errorEl) { errorEl.style.display = 'none'; errorEl.textContent = ''; }
    fetch('/api/targets/' + encodeURIComponent(targetId) + '/targets-config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ targets_config: parsed })
    });
  }

  function attachTargetBodyListener(areaId, errorId, targetIdElId) {
    var areaEl  = document.getElementById(areaId);
    var errorEl = document.getElementById(errorId);
    var tidEl   = document.getElementById(targetIdElId);
    if (!areaEl || !tidEl) return;
    var tid = tidEl.value;
    if (!tid) return;
    areaEl.addEventListener('input', function() {
      clearTimeout(_bodyTimers[areaId]);
      _bodyTimers[areaId] = setTimeout(function() {
        _saveTargetBodyIfValid(areaEl, errorEl, tid);
      }, 800);
    });
    areaEl.addEventListener('blur', function() {
      clearTimeout(_bodyTimers[areaId]);
      _saveTargetBodyIfValid(areaEl, errorEl, tid);
    });
  }

  attachTargetBodyListener('vk-target-body',     'vk-target-body-error',     'vk_target_id');
  attachTargetBodyListener('dzen-target-body',    'dzen-target-body-error',   'dzen_target_id');
  attachTargetBodyListener('rutube-target-body',  'rutube-target-body-error', 'rutube_target_id');
  attachTargetBodyListener('vkvideo-target-body', 'vkvideo-target-body-error','vkvideo_target_id');

  function attachTargetActiveToggle(toggleId, targetIdElId) {
    var toggle  = document.getElementById(toggleId);
    var tidEl   = document.getElementById(targetIdElId);
    if (!toggle || !tidEl) return;
    var tid = tidEl.value;
    if (!tid) return;
    toggle.addEventListener('change', function() {
      var active = toggle.checked;
      var card = toggle.closest('.card');
      fetch('/api/targets/' + encodeURIComponent(tid) + '/active', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ active: active })
      }).then(function(r) { return r.json(); }).then(function(data) {
        if (data.ok && card) {
          card.classList.toggle('target-disabled', !active);
        } else if (!data.ok) {
          toggle.checked = !active;
        }
      }).catch(function() {
        toggle.checked = !active;
      });
    });
  }

  attachTargetActiveToggle('vk-active-toggle',     'vk_target_id');
  attachTargetActiveToggle('dzen-active-toggle',    'dzen_target_id');
  attachTargetActiveToggle('rutube-active-toggle',  'rutube_target_id');
  attachTargetActiveToggle('vkvideo-active-toggle', 'vkvideo_target_id');
})();
