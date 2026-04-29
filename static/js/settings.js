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
  const chkApproveStories = document.getElementById('approve_stories_check');
  const hidApproveStories = document.getElementById('approve_stories_hidden');
  if (chkApproveStories || hidApproveStories) {
    const approveVal = chkApproveStories ? (chkApproveStories.checked ? '1' : '0') : (hidApproveStories ? hidApproveStories.value : '0');
    data.set('approve_stories', approveVal);
  }
  setIfExists('target_id',        'target_id');
  setIfExists('notify_email',     'notify_email');
  setIfExists('notify_phone',     'notify_phone');
  setIfExists('entries_lifetime', 'entries_lifetime');
  setIfExists('log_lifetime',     'log_lifetime');
  setIfExists('batch_lifetime',   'batch_lifetime');
  setIfExists('file_lifetime',    'file_lifetime');
  setIfExists('buffer_hours',     'buffer_hours');
  setIfExists('loop_interval',    'loop_interval');
  setIfExists('max_batch_threads', 'max_batch_threads');
  setIfExists('max_model_passes',  'max_model_passes');
  return data;
}

function onApproveStoriesToggle(chk) {
  const hid   = document.getElementById('approve_stories_hidden');
  const badge = document.getElementById('approve-stories-badge');
  if (hid)   hid.value            = chk.checked ? '1' : '0';
  if (badge) badge.style.display  = chk.checked ? ''  : 'none';
  saveStorySettings();
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
    document.getElementById('file_lifetime'),
    document.getElementById('buffer_hours'),
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

function openResetPublicationCounterDialog(btn) {
  new ConfirmDialog({
    title: 'Обнулить счётчик публикаций?',
    text: 'publication_counter будет сброшен в 0. Следующая публикация получит номер 1.',
    confirmLabel: 'Обнулить',
    cancelLabel: 'Отмена',
    confirmStyle: 'danger',
    triggerBtn: btn,
    onConfirm: function(confirmBtn, dlg) {
      confirmBtn.disabled = true;
      fetch('/api/publication-counter/reset', { method: 'POST' })
        .then(r => r.json())
        .then(data => {
          dlg.close();
          if (data.ok) {
            showToast('Счётчик обнулён', 'success');
          } else {
            showToast('Ошибка: ' + (data.error || 'неизвестная'), 'error');
          }
        })
        .catch(() => { dlg.close(); showToast('Ошибка запроса', 'error'); });
    },
  }).open();
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
})();
