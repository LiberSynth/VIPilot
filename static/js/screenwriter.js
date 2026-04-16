/* ── Автосохранение черновика сюжета ── */
var resetDraftStoryId;
var setDraftStoryFromRecord;
var getDraftStoryId;
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

  getDraftStoryId = function() { return _draftStoryId; };

  resetDraftStoryId = function() {
    _draftStoryId = null;
    _draftSaving = false;
    _draftPendingRetry = false;
    clearTimeout(_draftTimer);
    setDraftCardState(null);
    if (typeof window.resetGradedAwayFlag === 'function') window.resetGradedAwayFlag();
    else if (typeof window.updateReturnButton === 'function') window.updateReturnButton();
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
    if (typeof window.resetGradedAwayFlag === 'function') window.resetGradedAwayFlag();
    else if (typeof window.updateReturnButton === 'function') window.updateReturnButton();
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
  var _storyGradedAway = false;

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

  function updateStoriesCount(n) {
    var el = document.getElementById('stories-count');
    if (!el) return;
    var mod10 = n % 10, mod100 = n % 100;
    var word;
    if (mod100 >= 11 && mod100 <= 14) {
      word = 'записей';
    } else if (mod10 === 1) {
      word = 'запись';
    } else if (mod10 >= 2 && mod10 <= 4) {
      word = 'записи';
    } else {
      word = 'записей';
    }
    el.textContent = n + ' ' + word;
  }

  function escapeHtml(str) {
    return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  function renderStories(stories) {
    var container = document.getElementById('stories-list');
    if (!container) return;
    if (!stories || stories.length === 0) {
      updateStoriesCount(0);
      container.innerHTML = '<div class="stories-empty">Нет сюжетов</div>';
      updateReturnButton();
      return;
    }
    updateStoriesCount(stories.length);
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
      var exportBtn = '<button class="story-icon story-export-btn" data-id="' + s.id + '" title="Выгрузить">' +
        (window.EXPORT_STORY_SVG || '') + '</button>';
      html += '<div class="story-row" data-id="' + s.id + '" data-used="' + (s.used ? '1' : '0') + '">' +
        '<div class="story-title">' + escapeHtml(s.title || '(без названия)') + modelLabel + ' ' + gradeBadge + '</div>' +
        '<div class="story-row-right">' + icons + exportBtn + '</div>' +
      '</div>';
    }
    container.innerHTML = html;
    container.querySelectorAll('.story-grade-badge').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        var storyId = btn.getAttribute('data-id');
        var storyObj = null;
        for (var j = 0; j < stories.length; j++) {
          if (String(stories[j].id) === String(storyId)) { storyObj = stories[j]; break; }
        }
        if (storyObj && typeof setDraftStoryFromRecord === 'function') {
          setDraftStoryFromRecord(storyObj);
        }
        _updateSelectedRow();
        cycleGrade(btn);
      });
    });
    container.querySelectorAll('.story-export-btn').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        var storyId = btn.getAttribute('data-id');
        if (typeof window.exportStory === 'function') window.exportStory(storyId, btn);
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
        _updateSelectedRow();
      });
    });
    _updateSelectedRow();
    updateReturnButton();
  }

  function updateReturnButton() {
    var btn = document.getElementById('btn-story-return');
    if (!btn) return;
    var forApproval = document.getElementById('filter-for-approval');
    if (!forApproval || !forApproval.checked) { btn.hidden = true; return; }
    if (!_storyGradedAway) { btn.hidden = true; return; }
    var storyId = typeof getDraftStoryId === 'function' ? getDraftStoryId() : null;
    if (!storyId) { btn.hidden = true; return; }
    var container = document.getElementById('stories-list');
    if (!container) { btn.hidden = true; return; }
    var inList = container.querySelector('.story-row[data-id="' + storyId + '"]');
    btn.hidden = !!inList;
  }
  window.updateReturnButton = updateReturnButton;
  window.resetGradedAwayFlag = function() {
    _storyGradedAway = false;
    updateReturnButton();
  };

  function _updateSelectedRow() {
    var currentId = typeof getDraftStoryId === 'function' ? getDraftStoryId() : null;
    var container = document.getElementById('stories-list');
    if (!container) return;
    container.querySelectorAll('.story-row--selected').forEach(function(r) {
      r.classList.remove('story-row--selected');
    });
    if (currentId) {
      var sel = container.querySelector('.story-row[data-id="' + currentId + '"]');
      if (sel) sel.classList.add('story-row--selected');
    }
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

        if (grade !== null) { _storyGradedAway = true; }
        window.loadStoriesList();
      }
    })
    .catch(function() { btn.disabled = false; });
  }

  function getFilterParams() {
    var showUsed = document.getElementById('filter-show-used');
    var onlyGood = document.getElementById('filter-only-good');
    var forApproval = document.getElementById('filter-for-approval');
    var params = new URLSearchParams();
    if (forApproval && forApproval.checked) {
      params.set('for_approval', '1');
    } else {
      params.set('show_used', (showUsed && showUsed.checked) ? '1' : '0');
      params.set('show_bad', (onlyGood && onlyGood.checked) ? '0' : '1');
    }
    var pinId = typeof getDraftStoryId === 'function' ? getDraftStoryId() : null;
    if (pinId) params.set('pin_id', pinId);
    return params.toString();
  }

  window.loadStoriesList = function() {
    var container = document.getElementById('stories-list');
    if (!container) return;
    var hasContent = container.querySelector('.story-row');
    if (!hasContent) {
      container.innerHTML = '<div class="stories-loading">Загрузка...</div>';
    }
    fetch('/producer/stories?' + getFilterParams())
      .then(function(r) { return r.ok ? r.json() : []; })
      .then(renderStories)
      .catch(function() {
        if (container) container.innerHTML = '<div class="stories-empty">Ошибка загрузки</div>';
        var countEl = document.getElementById('stories-count');
        if (countEl) countEl.textContent = '—';
      });
  };

  function initFilterCheckboxes() {
    var showUsed = document.getElementById('filter-show-used');
    var onlyGood = document.getElementById('filter-only-good');
    var forApproval = document.getElementById('filter-for-approval');
    function onFilterChange(key, checkbox) {
      var value = checkbox.checked ? '1' : '0';
      fetch('/producer/env', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key: key, value: value }),
      }).then(function() {
        window.loadStoriesList();
      });
    }
    if (forApproval) {
      forApproval.addEventListener('change', function() {
        if (forApproval.checked) {
          if (onlyGood) onlyGood.checked = false;
          if (showUsed) showUsed.checked = false;
          fetch('/producer/env', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ key: 'screenwriter_only_good', value: '0' }) });
          fetch('/producer/env', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ key: 'screenwriter_show_used', value: '0' }) });
        }
        updateReturnButton();
        onFilterChange('screenwriter_for_approval', forApproval);
      });
    }
    if (showUsed) {
      showUsed.addEventListener('change', function() {
        if (showUsed.checked && forApproval && forApproval.checked) {
          forApproval.checked = false;
          fetch('/producer/env', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ key: 'screenwriter_for_approval', value: '0' }) });
        }
        onFilterChange('screenwriter_show_used', showUsed);
      });
    }
    if (onlyGood) {
      onlyGood.addEventListener('change', function() {
        if (onlyGood.checked && forApproval && forApproval.checked) {
          forApproval.checked = false;
          fetch('/producer/env', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ key: 'screenwriter_for_approval', value: '0' }) });
        }
        onFilterChange('screenwriter_only_good', onlyGood);
      });
    }
  }

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

  function initReturnButton() {
    var btn = document.getElementById('btn-story-return');
    if (!btn) return;
    btn.addEventListener('click', function() {
      var storyId = typeof getDraftStoryId === 'function' ? getDraftStoryId() : null;
      if (!storyId) return;
      btn.disabled = true;
      fetch('/producer/story/' + storyId + '/grade', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ grade: null }),
      })
      .then(function(r) { return r.ok ? r.json() : null; })
      .then(function(d) {
        btn.disabled = false;
        if (d && d.ok) {
          if (typeof window.loadStoriesList === 'function') window.loadStoriesList();
        }
      })
      .catch(function() { btn.disabled = false; });
    });
  }

  var _origLoadStoriesList = window.loadStoriesList;
  window.loadStoriesList = function() {
    if (_origLoadStoriesList) _origLoadStoriesList();
    if (typeof loadGoodPoolCount === 'function') loadGoodPoolCount();
  };

  function initAll() {
    initNewStoryButton();
    initReturnButton();
    initFilterCheckboxes();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initAll);
  } else {
    initAll();
  }
})();

/* ── Кнопка «Сгенерировать» в панели Сценариста ── */
(function() {
  var _DEFAULT_HINT = 'Вы можете сгенерировать сюжет при помощи AI-модели';
  var _pollTimer    = null;
  var _hintResetTimer = null;
  var _batchQueue   = [];
  var _batchTotal   = 0;
  var _batchDone    = 0;

  function setHint(text) {
    var el = document.getElementById('story-generate-hint');
    if (el) el.textContent = text || _DEFAULT_HINT;
  }
  function resetHint() { setHint(_DEFAULT_HINT); }
  function scheduleResetHint() {
    if (_hintResetTimer) clearTimeout(_hintResetTimer);
    _hintResetTimer = setTimeout(function() { _hintResetTimer = null; resetHint(); }, 2000);
  }

  function pollNext() {
    if (_pollTimer) { clearTimeout(_pollTimer); _pollTimer = null; }
    if (_batchQueue.length === 0) {
      var btn = document.getElementById('btn-story-generate');
      if (btn) btn.disabled = false;
      setHint(_batchTotal > 1
        ? 'Готово: сгенерировано ' + _batchDone + ' из ' + _batchTotal
        : 'Сюжет сгенерирован');
      scheduleResetHint();
      return;
    }
    var batchId = _batchQueue.shift();
    var batchIndex = _batchTotal - _batchQueue.length;
    function poll() {
      fetch('/api/batch/' + batchId + '/logs')
        .then(function(r) { return r.json(); })
        .then(function(d) {
          if (d.error) { _pollTimer = setTimeout(poll, 700); return; }
          var prefix = _batchTotal > 1 ? '[' + batchIndex + '/' + _batchTotal + '] ' : '';
          var entries = [];
          if (d.logs && d.logs.length) {
            var lastLog = d.logs[d.logs.length - 1];
            if (lastLog.entries && lastLog.entries.length) entries = lastLog.entries;
          }
          if (entries.length) setHint(prefix + entries[entries.length - 1].message);
          var status = d.batch_status;
          if (status === 'story_probe') {
            _batchDone++;
            var storyId = d.story_id;
            if (storyId) {
              fetch('/api/story/' + encodeURIComponent(storyId))
                .then(function(r) { return r.json(); })
                .then(function(s) {
                  if (typeof setDraftStoryFromRecord === 'function') {
                    setDraftStoryFromRecord({ id: storyId, title: s.title || '', content: s.text || '' });
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
                  pollNext();
                })
                .catch(function() {
                  if (typeof loadStoriesList === 'function') loadStoriesList();
                  pollNext();
                });
            } else {
              if (typeof loadStoriesList === 'function') loadStoriesList();
              pollNext();
            }
          } else if (status === 'error' || status === 'cancelled' || status === 'fatal_error') {
            _batchDone++;
            if (typeof loadStoriesList === 'function') loadStoriesList();
            pollNext();
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
      var countInput = document.getElementById('story-generate-count');
      var count = Math.max(1, Math.min(50, parseInt((countInput && countInput.value) || '1') || 1));
      btn.disabled = true;
      _batchQueue = [];
      _batchTotal = count;
      _batchDone  = 0;
      setHint(count > 1 ? 'Создаю ' + count + ' батчей…' : 'Запускаю генерацию…');
      var remaining = count;
      for (var i = 0; i < count; i++) {
        fetch('/producer/story/generate', { method: 'POST' })
          .then(function(r) { return r.json(); })
          .then(function(d) {
            if (d.batch_id) _batchQueue.push(d.batch_id);
            remaining--;
            if (remaining === 0) {
              _batchTotal = _batchQueue.length;
              if (_batchQueue.length === 0) {
                btn.disabled = false;
                setHint('Не удалось создать батчи');
                setTimeout(resetHint, 3000);
              } else {
                pollNext();
              }
            }
          })
          .catch(function() {
            remaining--;
            if (remaining === 0) {
              _batchTotal = _batchQueue.length;
              if (_batchQueue.length === 0) {
                btn.disabled = false;
                setHint('Ошибка запроса');
                setTimeout(resetHint, 4000);
              } else {
                pollNext();
              }
            }
          });
      }
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initGenerateButton);
  } else {
    initGenerateButton();
  }
})();
