const ta    = document.getElementById('ta');
const cc    = document.getElementById('charcount');
const taSys = document.getElementById('ta_sysprompt');
const ccSys = document.getElementById('charcount_sysprompt');

function updateCount()    { if (ta    && cc)    cc.textContent    = ta.value.length    + ' символов'; }
function updateSysCount() { if (taSys && ccSys) ccSys.textContent = taSys.value.length + ' символов'; }

if (ta)    { ta.addEventListener('input',    updateCount);    updateCount(); }
if (taSys) { taSys.addEventListener('input', updateSysCount); updateSysCount(); }

(function() {
  var KEY_SYS  = 'rbc_sysprompt_h';
  var KEY_META = 'rbc_metaprompt_h';

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
  const v = id => { const el = document.getElementById(id); return el ? el.value : ''; };
  data.set('metaprompt',      ta    ? ta.value    : '');
  data.set('system_prompt',   taSys ? taSys.value : '');
  data.set('video_duration',      v('video_duration'));
  data.set('video_post_prompt',   (function(){ var el = document.getElementById('ta_postprompt'); return el ? el.value : ''; })());
  data.set('story_fails_to_next', v('story_fails_to_next'));
  data.set('video_fails_to_next', v('video_fails_to_next'));
  const chkStory     = document.getElementById('vk_story_pub_check');
  const chkWall      = document.getElementById('vk_wall_pub_check');
  const chkTranscode = document.getElementById('vk_transcode_check');
  const storyVal     = chkStory     ? (chkStory.checked     ? '1' : '0') : v('vk_story_pub_hidden');
  const wallVal      = chkWall      ? (chkWall.checked      ? '1' : '0') : v('vk_wall_pub_hidden');
  const transcodeVal = chkTranscode ? (chkTranscode.checked ? '1' : '0') : v('vk_transcode_hidden');
  data.set('vk_publish_story', storyVal);
  data.set('vk_publish_wall',  wallVal);
  data.set('vk_transcode',     transcodeVal);
  data.set('aspect_ratio_x',   v('ar-x'));
  data.set('aspect_ratio_y',   v('ar-y'));
  data.set('target_id',        v('target_id'));
  data.set('notify_email',     v('notify_email'));
  data.set('notify_phone',     v('notify_phone'));
  data.set('entries_lifetime', v('entries_lifetime'));
  data.set('log_lifetime',     v('log_lifetime'));
  data.set('batch_lifetime',   v('batch_lifetime'));
  data.set('file_lifetime',    v('file_lifetime'));
  data.set('buffer_hours',      v('buffer_hours'));
  data.set('loop_interval',     v('loop_interval'));
  data.set('max_batch_threads', v('max_batch_threads'));
  return data;
}

function updateAR() {
  const xEl = document.getElementById('ar-x');
  const yEl = document.getElementById('ar-y');
  const rect = document.getElementById('ar-rect');
  if (!xEl || !yEl || !rect) return;
  const x = Math.max(1, parseInt(xEl.value) || 1);
  const y = Math.max(1, parseInt(yEl.value) || 1);
  const maxSide = 64;
  let w, h;
  if (x >= y) { w = maxSide; h = Math.round(maxSide * y / x); }
  else        { h = maxSide; w = Math.round(maxSide * x / y); }
  rect.style.width  = w + 'px';
  rect.style.height = h + 'px';
  document.querySelectorAll('.ar-preset').forEach(function(btn) {
    const parts = btn.textContent.split(':').map(Number);
    btn.classList.toggle('active', parts[0] === x && parts[1] === y);
  });
  savePublishSettings();
}

function setAR(x, y) {
  const xEl = document.getElementById('ar-x');
  const yEl = document.getElementById('ar-y');
  if (xEl) xEl.value = x;
  if (yEl) yEl.value = y;
  updateAR();
}
updateAR();

function onVkPublishToggle(changedChk, otherId, selfHidId, otherHidId) {
  const otherChk = document.getElementById(otherId);
  if (!changedChk.checked && !otherChk.checked) {
    changedChk.checked = true;
  }
  const selfVal  = changedChk.checked ? '1' : '0';
  const otherVal = otherChk.checked   ? '1' : '0';
  document.getElementById(selfHidId).value  = selfVal;
  document.getElementById(otherHidId).value = otherVal;
  savePublishSettings();
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

var _dzenCsrfSaveTimer = null;
function saveDzenCsrf() {
  const targetId = document.getElementById('dzen_target_id');
  const csrfInput = document.getElementById('dzen_csrf_token');
  const statusEl = document.getElementById('dzen_csrf_status');
  if (!targetId || !csrfInput) return;
  const fd = new FormData();
  fd.append('dzen_target_id', targetId.value);
  fd.append('dzen_csrf_token', csrfInput.value.trim());
  fetch('/save-dzen', { method: 'POST', body: fd })
    .then(r => r.json())
    .then(data => {
      if (!statusEl) return;
      if (data.ok) {
        statusEl.style.color = '#69db7c';
        statusEl.textContent = 'CSRF-токен сохранён (только что).';
      } else {
        statusEl.style.color = '#ff6b6b';
        statusEl.textContent = 'Ошибка сохранения токена.';
      }
    })
    .catch(() => {});
}
function scheduleDzenCsrfSave() {
  clearTimeout(_dzenCsrfSaveTimer);
  _dzenCsrfSaveTimer = setTimeout(saveDzenCsrf, 800);
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
