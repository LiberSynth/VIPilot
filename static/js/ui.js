/* ── Автосохранение черновика сюжета ── */
var resetDraftStoryId;
var setDraftStoryFromRecord;
(function() {
  var _draftStoryId = null;
  var _draftTimer = null;
  var _draftSaving = false;
  var _draftPendingRetry = false;

  function setDraftCardState(state) {
    var card = document.getElementById('card-story-draft');
    if (!card) return;
    card.classList.remove('card--editing-new', 'card--editing-existing');
    if (state === 'new') card.classList.add('card--editing-new');
    else if (state === 'existing') card.classList.add('card--editing-existing');
  }

  resetDraftStoryId = function() {
    _draftStoryId = null;
    _draftSaving = false;
    _draftPendingRetry = false;
    clearTimeout(_draftTimer);
    setDraftCardState(null);
  };

  setDraftStoryFromRecord = function(story) {
    var titleEl = document.getElementById('draft-story-title');
    var contentEl = document.getElementById('draft-story-content');
    if (titleEl) titleEl.value = story.title || '';
    if (contentEl) contentEl.value = story.content || '';
    _draftStoryId = story.id;
    _draftSaving = false;
    _draftPendingRetry = false;
    clearTimeout(_draftTimer);
    setDraftCardState('existing');
  };

  function saveDraft() {
    var titleEl = document.getElementById('draft-story-title');
    var contentEl = document.getElementById('draft-story-content');
    if (!titleEl || !contentEl) return;
    var title = titleEl.value;
    var content = contentEl.value;
    if (!_draftStoryId && !title && !content) return;
    if (_draftSaving && !_draftStoryId) {
      _draftPendingRetry = true;
      return;
    }
    _draftSaving = true;
    fetch('/producer/story/draft', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ story_id: _draftStoryId, title: title, content: content }),
    })
    .then(function(r) { return r.ok ? r.json() : null; })
    .then(function(d) {
      if (d && d.story_id) {
        var isNew = !_draftStoryId;
        _draftStoryId = d.story_id;
        if (isNew) {
          setDraftCardState('new');
        }
        if (typeof loadStoriesList === 'function') loadStoriesList();
      }
      _draftSaving = false;
      if (_draftPendingRetry) {
        _draftPendingRetry = false;
        saveDraft();
      }
    })
    .catch(function() {
      _draftSaving = false;
      if (_draftPendingRetry) {
        _draftPendingRetry = false;
        saveDraft();
      }
    });
  }

  function onDraftInput() {
    clearTimeout(_draftTimer);
    _draftTimer = setTimeout(saveDraft, 800);
  }

  function initDraftAutosave() {
    var titleEl = document.getElementById('draft-story-title');
    var contentEl = document.getElementById('draft-story-content');
    if (!titleEl || !contentEl) return;
    titleEl.addEventListener('input', onDraftInput);
    contentEl.addEventListener('input', onDraftInput);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initDraftAutosave);
  } else {
    initDraftAutosave();
  }
})();

/* ── Список сюжетов в панели Сценариста ── */
(function() {
  var GRADE_CYCLE = ['good', 'bad', null];
  var GRADE_LABELS = { good: 'хорошо', bad: 'плохо', 'null': 'не указано' };
  var GRADE_COLORS = {
    good: 'rgba(62,207,142,.18)',
    bad:  'rgba(255,80,80,.18)',
    'null': 'rgba(255,255,255,.1)',
  };
  var GRADE_TEXT_COLORS = {
    good: '#3ecf8e',
    bad:  '#ff6060',
    'null': '#888',
  };
  function gradeKey(g) { return g === null || g === undefined ? 'null' : g; }

  function renderStories(stories) {
    var container = document.getElementById('stories-list');
    if (!container) return;
    if (!stories || stories.length === 0) {
      container.innerHTML = '<div class="stories-empty">Нет сюжетов</div>';
      return;
    }
    var html = '';
    for (var i = 0; i < stories.length; i++) {
      var s = stories[i];
      var grade = s.grade !== undefined ? s.grade : null;
      var gk = gradeKey(grade);
      var label = GRADE_LABELS[gk] || gk;
      var bg = GRADE_COLORS[gk] || 'rgba(255,255,255,.07)';
      var tc = GRADE_TEXT_COLORS[gk] || '#aaa';
      var icons = '';
      if (s.used) {
        icons += '<span class="story-icon story-icon-used" title="Использован в производстве">' +
          '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">' +
          '<polyline points="2,8 6,12 14,4"/></svg></span>';
      }
      if (s.ai_generated && s.manual_changed) {
        icons += '<span class="story-icon story-icon-ai-manual" title="Сгенерировано AI, отредактировано вручную">' +
          '<svg viewBox="0 0 26 24" fill="none" stroke="none" stroke-linecap="round" stroke-linejoin="round">' +
          '<path d="M14 21v-1.5a3.5 3.5 0 0 0-3.5-3.5H3.5A3.5 3.5 0 0 0 0 19.5V21" stroke="#fbbf24" stroke-width="1.7"/>' +
          '<circle cx="8.5" cy="7.5" r="3.8" stroke="#fbbf24" stroke-width="1.7"/>' +
          '<path d="M21 1 L22.1 4.4 L25.5 5.5 L22.1 6.6 L21 10 L19.9 6.6 L16.5 5.5 L19.9 4.4 Z" fill="#ffffff"/>' +
          '</svg></span>';
      } else if (s.manual_changed) {
        icons += '<span class="story-icon story-icon-manual" title="Написано вручную">' +
          '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">' +
          '<path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg></span>';
      } else {
        icons += '<span class="story-icon story-icon-ai" title="Сгенерировано AI">' +
          '<svg viewBox="0 0 16 16" fill="currentColor" stroke="none">' +
          '<path d="M8 1 L9.3 6.7 L15 8 L9.3 9.3 L8 15 L6.7 9.3 L1 8 L6.7 6.7 Z"/></svg></span>';
      }
      var modelLabel = s.model_name ? ' <span class="story-model-name">(' + escapeHtml(s.model_name) + ')</span>' : '';
      var gradeBadge = '<button class="story-grade-badge" data-id="' + s.id + '" data-grade="' + gk + '" ' +
        'style="background:' + bg + ';color:' + tc + '" ' +
        'title="Оценка: ' + label + '. Нажмите для смены">' +
        label + '</button>';
      html += '<div class="story-row" data-id="' + s.id + '">' +
        '<div class="story-title">' + escapeHtml(s.title || '(без названия)') + modelLabel + ' ' + gradeBadge + '</div>' +
        '<div class="story-row-right">' + icons + '</div>' +
      '</div>';
    }
    container.innerHTML = html;
    container.querySelectorAll('.story-grade-badge').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        cycleGrade(btn);
      });
    });
    container.querySelectorAll('.story-row').forEach(function(row) {
      var storyId = row.getAttribute('data-id');
      var storyObj = null;
      for (var j = 0; j < stories.length; j++) {
        if (String(stories[j].id) === String(storyId)) { storyObj = stories[j]; break; }
      }
      if (!storyObj) return;
      row.addEventListener('click', function() {
        if (typeof setDraftStoryFromRecord === 'function') setDraftStoryFromRecord(storyObj);
      });
    });
  }

  function cycleGrade(btn) {
    var storyId = btn.getAttribute('data-id');
    var currentGradeAttr = btn.getAttribute('data-grade');
    var currentGrade = currentGradeAttr === 'null' ? null : currentGradeAttr;
    var idx = GRADE_CYCLE.indexOf(currentGrade);
    var nextGrade = GRADE_CYCLE[(idx + 1) % GRADE_CYCLE.length];
    btn.disabled = true;
    fetch('/producer/story/' + storyId + '/grade', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ grade: nextGrade }),
    })
    .then(function(r) { return r.ok ? r.json() : null; })
    .then(function(d) {
      btn.disabled = false;
      if (d && d.ok) {
        var grade = d.grade !== undefined ? d.grade : null;
        var gk = gradeKey(grade);
        btn.setAttribute('data-grade', gk);
        btn.style.background = GRADE_COLORS[gk] || 'rgba(255,255,255,.07)';
        btn.style.color = GRADE_TEXT_COLORS[gk] || '#aaa';
        btn.textContent = GRADE_LABELS[gk] || gk;
        btn.title = 'Оценка: ' + (GRADE_LABELS[gk] || gk) + '. Нажмите для смены';
      }
    })
    .catch(function() { btn.disabled = false; });
  }

  function escapeHtml(str) {
    return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  window.loadStoriesList = function() {
    var container = document.getElementById('stories-list');
    if (!container) return;
    var hasContent = container.querySelector('.story-row');
    if (!hasContent) {
      container.innerHTML = '<div class="stories-loading">Загрузка...</div>';
    }
    fetch('/producer/stories')
      .then(function(r) { return r.ok ? r.json() : []; })
      .then(renderStories)
      .catch(function() {
        if (container) container.innerHTML = '<div class="stories-empty">Ошибка загрузки</div>';
      });
  };

  function initNewStoryButton() {
    var btn = document.getElementById('btn-story-new');
    if (!btn) return;
    btn.addEventListener('click', function() {
      var titleEl = document.getElementById('draft-story-title');
      var contentEl = document.getElementById('draft-story-content');
      if (titleEl) titleEl.value = '';
      if (contentEl) contentEl.value = '';
      if (typeof resetDraftStoryId === 'function') resetDraftStoryId();
      if (titleEl) titleEl.focus();
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initNewStoryButton);
  } else {
    initNewStoryButton();
  }
})();

const PANEL_TITLES = {
  screenwriter: 'Сценарист',
  director:     'Режиссер',
  pipeline: 'Расписание',
  story:    'Генерация сюжета',
  request:  'Генерация видео',
  publish:  'Публикация',
  service:  'Служебные',
  log:      'Монитор',
  info:     'Информация',
};

function openSidebar() {
  document.getElementById('sidebar').classList.add('open');
  var existing = document.getElementById('sidebar-overlay');
  if (existing) { existing.classList.add('open'); return; }
  var el = document.createElement('div');
  el.className = 'sidebar-overlay open';
  el.id = 'sidebar-overlay';
  el.addEventListener('click', closeSidebar);
  document.body.insertBefore(el, document.body.firstChild);
}

function closeSidebar() {
  document.getElementById('sidebar').classList.remove('open');
  var el = document.getElementById('sidebar-overlay');
  if (el) el.remove();
}

function switchPanel(name) {
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.sidebar-item').forEach(b => b.classList.remove('active'));
  const panel = document.getElementById('panel-' + name);
  if (panel) panel.classList.add('active');
  const btn = document.querySelector('.sidebar-item[data-panel="' + name + '"]');
  if (btn) btn.classList.add('active');
  const titleEl = document.getElementById('page-title');
  if (titleEl) titleEl.textContent = PANEL_TITLES[name] || name;
  closeSidebar();
  if (name === 'request') {
    if (typeof loadModels === 'function') loadModels();
  }
  if (name === 'story') {
    if (typeof loadTextModels === 'function') loadTextModels();
    var draftTitle = document.getElementById('draft-story-title');
    var draftContent = document.getElementById('draft-story-content');
    if (draftTitle) draftTitle.value = '';
    if (draftContent) draftContent.value = '';
    if (typeof resetDraftStoryId === 'function') resetDraftStoryId();
  }
  if (name === 'screenwriter') {
    var draftCard = document.getElementById('card-story-draft');
    if (draftCard) draftCard.classList.remove('card--editing-new', 'card--editing-existing');
    if (typeof loadStoriesList === 'function') loadStoriesList();
  }
}

let _monitorClockTimer = null;
let _monitorClockOffset = 0;

function monitorClockTick() {
  const el = document.getElementById('header-clock');
  if (!el) return;
  const serverNow = Date.now() + _monitorClockOffset;
  const msk = new Date(serverNow + 3 * 60 * 60 * 1000);
  const pad = n => String(n).padStart(2, '0');
  const timeStr =
    pad(msk.getUTCDate()) + '.' +
    pad(msk.getUTCMonth() + 1) + '.' +
    msk.getUTCFullYear() + ' ' +
    pad(msk.getUTCHours()) + ':' +
    pad(msk.getUTCMinutes()) + ':' +
    pad(msk.getUTCSeconds());
  el.textContent = timeStr;
}

function monitorClockStart() {
  if (_monitorClockTimer) return;
  const t0 = Date.now();
  fetch('/api/time').then(r => r.json()).then(d => {
    const t1 = Date.now();
    _monitorClockOffset = d.utc_ms - Math.round((t0 + t1) / 2);
    monitorClockTick();
    if (!_monitorClockTimer) _monitorClockTimer = setInterval(monitorClockTick, 1000);
  }).catch(() => {
    monitorClockTick();
    if (!_monitorClockTimer) _monitorClockTimer = setInterval(monitorClockTick, 1000);
  });
}

(function() {
  monitorClockStart();
  const tab = new URLSearchParams(window.location.search).get('tab');
  if (tab && document.getElementById('panel-' + tab)) {
    switchPanel(tab);
    history.replaceState(null, '', window.location.pathname);
  }
})();

/* ── Тултип долгого нажатия для тач-устройств ── */
(function() {
  if (!('ontouchstart' in window)) return;

  let _timer   = null;
  let _tooltip = null;
  let _autoHide = null;

  function getTooltip() {
    if (!_tooltip) {
      _tooltip = document.createElement('div');
      _tooltip.id = 'touch-tooltip';
      _tooltip.classList.add('hidden');
      document.body.appendChild(_tooltip);
    }
    return _tooltip;
  }

  function showTooltip(text, targetEl) {
    const tip = getTooltip();
    tip.textContent = text;
    tip.classList.remove('hidden');

    const rect = targetEl.getBoundingClientRect();
    const tipW = 220;
    let x = rect.left + rect.width / 2 - tipW / 2;
    let y = rect.top - 10;

    x = Math.max(8, Math.min(x, window.innerWidth - tipW - 8));

    tip.style.width  = tipW + 'px';
    tip.style.left   = x + 'px';

    tip.style.top    = '0px';
    tip.style.bottom = '';
    const tipH = tip.offsetHeight || 34;
    if (y - tipH < 8) {
      tip.style.top  = (rect.bottom + 10) + 'px';
    } else {
      tip.style.top  = (y - tipH) + 'px';
    }

    clearTimeout(_autoHide);
    _autoHide = setTimeout(hideTooltip, 2500);
  }

  function hideTooltip() {
    clearTimeout(_autoHide);
    if (_tooltip) _tooltip.classList.add('hidden');
  }

  function findTitle(el) {
    while (el && el !== document.body) {
      if (el.title) return { text: el.title, el };
      el = el.parentElement;
    }
    return null;
  }

  document.addEventListener('touchstart', function(e) {
    clearTimeout(_timer);
    const found = findTitle(e.target);
    if (!found) return;
    _timer = setTimeout(function() {
      showTooltip(found.text, found.el);
    }, 500);
  }, { passive: true });

  document.addEventListener('touchend',    function() { clearTimeout(_timer); }, { passive: true });
  document.addEventListener('touchmove',   function() { clearTimeout(_timer); hideTooltip(); }, { passive: true });
  document.addEventListener('touchcancel', function() { clearTimeout(_timer); hideTooltip(); }, { passive: true });
})();

/* ── Кнопка «Сгенерировать» в панели Сценариста ── */
(function() {
  var _DEFAULT_HINT = 'Вы можете сгенерировать сюжет при помощи AI-модели';
  var _pollTimer = null;
  var _hintResetTimer = null;

  function setHint(text) {
    var el = document.getElementById('story-generate-hint');
    if (el) el.textContent = text || _DEFAULT_HINT;
  }

  function resetHint() { setHint(_DEFAULT_HINT); }

  function scheduleResetHint() {
    if (_hintResetTimer) { clearTimeout(_hintResetTimer); }
    _hintResetTimer = setTimeout(function() { _hintResetTimer = null; resetHint(); }, 2000);
  }

  function startPoll(batchId) {
    if (_pollTimer) { clearTimeout(_pollTimer); _pollTimer = null; }
    if (_hintResetTimer) { clearTimeout(_hintResetTimer); _hintResetTimer = null; }
    function poll() {
      fetch('/api/batch/' + batchId + '/logs')
        .then(function(r) { return r.json(); })
        .then(function(d) {
          if (d.error) { _pollTimer = setTimeout(poll, 700); return; }
          var entries = [];
          if (d.logs && d.logs.length) {
            var lastLog = d.logs[d.logs.length - 1];
            if (lastLog.entries && lastLog.entries.length) {
              entries = lastLog.entries;
            }
          }
          if (entries.length) {
            setHint(entries[entries.length - 1].message);
          }
          var status = d.batch_status;
          if (status === 'story_probe') {
            var storyId = d.story_id;
            var btn = document.getElementById('btn-story-generate');
            if (btn) btn.disabled = false;
            if (storyId) {
              fetch('/api/story/' + encodeURIComponent(storyId))
                .then(function(r) { return r.json(); })
                .then(function(s) {
                  if (typeof setDraftStoryFromRecord === 'function') {
                    setDraftStoryFromRecord({
                      id:      storyId,
                      title:   s.title || '',
                      content: s.text  || '',
                    });
                  }
                  if (typeof loadStoriesList === 'function') {
                    loadStoriesList();
                    setTimeout(function() {
                      var container = document.getElementById('stories-list');
                      if (container) {
                        var row = container.querySelector('.story-row[data-id="' + storyId + '"]');
                        if (row) row.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
                      }
                    }, 400);
                  }
                  setHint('Сюжет сгенерирован');
                  scheduleResetHint();
                })
                .catch(function() { scheduleResetHint(); });
            } else {
              setHint('Сюжет сгенерирован');
              scheduleResetHint();
            }
          } else if (status === 'error' || status === 'cancelled' || status === 'fatal_error') {
            var btn = document.getElementById('btn-story-generate');
            if (btn) btn.disabled = false;
            scheduleResetHint();
          } else {
            _pollTimer = setTimeout(poll, 700);
          }
        })
        .catch(function() { _pollTimer = setTimeout(poll, 700); });
    }
    poll();
  }

  function initGenerateButton() {
    var btn = document.getElementById('btn-story-generate');
    if (!btn) return;
    btn.addEventListener('click', function() {
      if (btn.disabled) return;
      btn.disabled = true;
      setHint('Запускаю генерацию…');
      fetch('/producer/story/generate', { method: 'POST' })
        .then(function(r) { return r.json(); })
        .then(function(d) {
          if (d.error) {
            btn.disabled = false;
            setHint('Ошибка: ' + d.error);
            setTimeout(resetHint, 4000);
            return;
          }
          startPoll(d.batch_id);
        })
        .catch(function(e) {
          btn.disabled = false;
          setHint('Ошибка запроса');
          setTimeout(resetHint, 4000);
        });
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initGenerateButton);
  } else {
    initGenerateButton();
  }
})();
