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
    if (!title && !content) return;
    if (_draftSaving && !_draftStoryId) {
      _draftPendingRetry = true;
      return;
    }
    _draftSaving = true;
    fetch('/production/story/draft', {
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
          if (typeof window.onDraftStoryFirstSaved === 'function') window.onDraftStoryFirstSaved();
        }
        if (typeof loadStoryList === 'function') loadStoryList();
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
      var wordCount = s.content ? s.content.trim().split(/\s+/).filter(Boolean).length : 0;
      var wordCountStr = wordCount > 0 ? 'слов:\u00a0' + wordCount : '';
      var innerLabel = s.model_name && wordCountStr
        ? escapeHtml(s.model_name) + ', ' + wordCountStr
        : (s.model_name ? escapeHtml(s.model_name) : wordCountStr);
      var modelLabel = innerLabel ? ' <span class="story-model-name">(' + innerLabel + ')</span>' : '';
      var inlineStyle = gk !== 'null'
        ? 'style="background:' + bg + ';color:' + tc + '" '
        : '';
      var gradeBadge = '<button class="story-grade-badge" data-id="' + s.id + '" data-grade="' + gk + '" ' +
        inlineStyle +
        'title="Оценка: ' + label + '. Нажмите для смены">' +
        label + '</button>';
      var exportBtn = '<button class="story-icon story-export-btn" data-id="' + s.id + '" title="Трассировка: накапливаются в буфере, сброс через 10 с">' +
        (window.EXPORT_STORY_SVG || '') + '</button>';
      var pinTitle = s.pinned ? 'Закреплён' : 'Закрепить';
      var pinBtn = '<button class="story-icon story-pin-btn' + (s.pinned ? ' story-pin-btn--active' : '') + '" data-id="' + s.id + '" data-pinned="' + (s.pinned ? '1' : '0') + '" title="' + pinTitle + '">' +
        '<svg viewBox="0 0 16 16" fill="' + (s.pinned ? 'currentColor' : 'none') + '" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">' +
        '<line x1="8" y1="10" x2="8" y2="15"/>' +
        '<path d="M5 2 L5 7 L2 10 L14 10 L11 7 L11 2 Z"/>' +
        '<line x1="5" y1="2" x2="11" y2="2"/>' +
        '</svg></button>';
      var deleteDisabled = s.pinned || s.has_movie || s.has_active_batch;
      var deleteBlockReason = deleteDisabled
        ? (s.pinned ? 'Сюжет закреплён' : (s.has_movie ? 'К сюжету привязано готовое видео' : 'У сюжета есть активный батч'))
        : '';
      var deleteTitle = deleteDisabled
        ? (s.pinned ? 'Нельзя удалить: сюжет закреплён' : (s.has_movie ? 'Нельзя удалить: к сюжету привязано видео' : 'Нельзя удалить: есть активный батч'))
        : 'Удалить';
      var storyTitleEsc = escapeHtml(s.title || '(без названия)');
      var deleteBtn = '<button class="story-icon story-delete-btn' + (deleteDisabled ? ' btn-blocked' : '') + '" data-id="' + s.id + '" data-title="' + storyTitleEsc + '"' + (deleteDisabled ? ' data-block-reason="' + escapeHtml(deleteBlockReason) + '"' : '') + ' title="' + deleteTitle + '">' +
        '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">' +
        '<polyline points="2,4 14,4"/><path d="M6 4V2h4v2"/><rect x="3" y="4" width="10" height="10" rx="1.5"/><line x1="6" y1="7" x2="6" y2="11"/><line x1="10" y1="7" x2="10" y2="11"/>' +
        '</svg></button>';
      html += '<div class="story-row" data-id="' + s.id + '" data-used="' + (s.used ? '1' : '0') + '">' +
        '<div class="story-title">' + escapeHtml(s.title || '(без названия)') + modelLabel + ' ' + gradeBadge + '</div>' +
        '<div class="story-row-right">' + icons + pinBtn + deleteBtn + exportBtn + '</div>' +
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
    container.querySelectorAll('.story-pin-btn').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        var storyId = btn.getAttribute('data-id');
        var currentPinned = btn.getAttribute('data-pinned') === '1';
        fetch('/production/story/' + storyId + '/pin', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ pinned: !currentPinned })
        }).then(function() {
          if (typeof window.loadStoryList === 'function') window.loadStoryList();
        });
      });
    });
    container.querySelectorAll('.story-export-btn').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        var storyId = btn.getAttribute('data-id');
        if (typeof window.exportStory === 'function') window.exportStory(storyId, btn);
      });
    });
    container.querySelectorAll('.story-delete-btn').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        if (btn.classList.contains('btn-blocked')) {
          var reason = btn.getAttribute('data-block-reason') || 'Удаление невозможно';
          new ConfirmDialog({
            title:       'Удаление невозможно',
            text:        reason,
            cancelLabel: 'Закрыть',
          }).open();
          return;
        }
        var storyId = btn.getAttribute('data-id');
        var storyTitle = btn.getAttribute('data-title') || '(без названия)';
        _openDeleteStoryDialog(storyId, storyTitle, btn);
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
        if (typeof window.loadStoryList === 'function') window.loadStoryList();
      });
    });
    _updateSelectedRow();
  }

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

  function _openDeleteStoryDialog(storyId, storyTitle, triggerBtn) {
    new ConfirmDialog({
      title:        'Удалить сюжет?',
      text:         'Сюжет «' + escapeHtml(storyTitle) + '» и все связанные батчи, лог и записи лога будут удалены безвозвратно.',
      confirmLabel: 'Удалить',
      triggerBtn:   triggerBtn,
      onConfirm: function(btn, dlg) {
        btn.disabled    = true;
        btn.textContent = 'Удаление…';
        fetch('/production/story/' + encodeURIComponent(storyId) + '/delete', { method: 'DELETE' })
          .then(function(r) { return r.ok ? r.json() : r.json().then(function(d) { throw d; }); })
          .then(function() {
            dlg.close();
            var row = document.querySelector('.story-row[data-id="' + storyId + '"]');
            if (row) row.remove();
            if (typeof window.loadStoryList === 'function') window.loadStoryList();
          })
          .catch(function(d) {
            dlg.close();
            var msg = (d && d.error) ? d.error : 'Ошибка удаления';
            if (typeof window.showToast === 'function') window.showToast(msg);
          });
      },
    }).open();
  }

  var _storyGradeReqCounters = {};
  function cycleGrade(btn) {
    var storyId = btn.getAttribute('data-id');
    var currentGradeAttr = btn.getAttribute('data-grade');
    var currentGrade = currentGradeAttr === 'null' ? null : currentGradeAttr;
    var idx = GRADE_CYCLE.indexOf(currentGrade);
    var nextGrade = GRADE_CYCLE[(idx + 1) % GRADE_CYCLE.length];
    var prevAttr = currentGradeAttr;
    _storyGradeReqCounters[storyId] = (_storyGradeReqCounters[storyId] || 0) + 1;
    var myReqId = _storyGradeReqCounters[storyId];
    var nextGk = gradeKey(nextGrade);
    btn.setAttribute('data-grade', nextGk);
    btn.style.background = nextGk !== 'null' ? (GRADE_COLORS[nextGk] || '') : '';
    btn.style.color = nextGk !== 'null' ? (GRADE_TEXT_COLORS[nextGk] || '') : '';
    btn.textContent = GRADE_LABELS[nextGk] || nextGk;
    btn.title = 'Оценка: ' + (GRADE_LABELS[nextGk] || nextGk) + '. Нажмите для смены';
    var _cardId = typeof getDraftStoryId === 'function' ? getDraftStoryId() : null;
    if (_cardId && String(_cardId) === String(storyId) && typeof window.setCardGradeBadge === 'function') {
      window.setCardGradeBadge(nextGrade, false);
    }
    fetch('/production/story/' + storyId + '/grade', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ grade: nextGrade }),
    })
    .then(function(r) { return r.ok ? r.json() : null; })
    .then(function(d) {
      if (myReqId !== _storyGradeReqCounters[storyId]) return;
      if (d && d.ok) {
        var grade = d.grade !== undefined ? d.grade : null;
        var gk = gradeKey(grade);
        btn.setAttribute('data-grade', gk);
        btn.style.background = gk !== 'null' ? (GRADE_COLORS[gk] || '') : '';
        btn.style.color = gk !== 'null' ? (GRADE_TEXT_COLORS[gk] || '') : '';
        btn.textContent = GRADE_LABELS[gk] || gk;
        btn.title = 'Оценка: ' + (GRADE_LABELS[gk] || gk) + '. Нажмите для смены';
        var _cId = typeof getDraftStoryId === 'function' ? getDraftStoryId() : null;
        if (_cId && String(_cId) === String(storyId) && typeof window.setCardGradeBadge === 'function') {
          window.setCardGradeBadge(grade, false);
        }
      } else {
        var prevGk = gradeKey(prevAttr === 'null' ? null : prevAttr);
        btn.setAttribute('data-grade', prevGk);
        btn.style.background = prevGk !== 'null' ? (GRADE_COLORS[prevGk] || '') : '';
        btn.style.color = prevGk !== 'null' ? (GRADE_TEXT_COLORS[prevGk] || '') : '';
        btn.textContent = GRADE_LABELS[prevGk] || prevGk;
        btn.title = 'Оценка: ' + (GRADE_LABELS[prevGk] || prevGk) + '. Нажмите для смены';
        var _cId2 = typeof getDraftStoryId === 'function' ? getDraftStoryId() : null;
        if (_cId2 && String(_cId2) === String(storyId) && typeof window.setCardGradeBadge === 'function') {
          window.setCardGradeBadge(prevAttr === 'null' ? null : prevAttr, false);
        }
      }
    })
    .catch(function() {
      if (myReqId !== _storyGradeReqCounters[storyId]) return;
      var prevGk = gradeKey(prevAttr === 'null' ? null : prevAttr);
      btn.setAttribute('data-grade', prevGk);
      btn.style.background = prevGk !== 'null' ? (GRADE_COLORS[prevGk] || '') : '';
      btn.style.color = prevGk !== 'null' ? (GRADE_TEXT_COLORS[prevGk] || '') : '';
      btn.textContent = GRADE_LABELS[prevGk] || prevGk;
      btn.title = 'Оценка: ' + (GRADE_LABELS[prevGk] || prevGk) + '. Нажмите для смены';
      var _cId3 = typeof getDraftStoryId === 'function' ? getDraftStoryId() : null;
      if (_cId3 && String(_cId3) === String(storyId) && typeof window.setCardGradeBadge === 'function') {
        window.setCardGradeBadge(prevAttr === 'null' ? null : prevAttr, false);
      }
    });
  }

  function getFilterParams() {
    var showUsed = document.getElementById('filter-show-used');
    var onlyGood = document.getElementById('filter-only-good');
    var onlyPinned = document.getElementById('filter-only-pinned');
    var forApproval = document.getElementById('filter-for-approval');
    var params = new URLSearchParams();
    if (forApproval && forApproval.checked) {
      params.set('for_approval', '1');
    } else {
      params.set('show_used', (showUsed && showUsed.checked) ? '1' : '0');
      params.set('show_bad', (onlyGood && onlyGood.checked) ? '0' : '1');
      if (onlyPinned && onlyPinned.checked) params.set('only_pinned', '1');
    }
    var pinId = typeof getDraftStoryId === 'function' ? getDraftStoryId() : null;
    if (pinId) params.set('pin_id', pinId);
    return params.toString();
  }

  window.loadStoryList = function() {
    var container = document.getElementById('stories-list');
    if (!container) return;
    var hasContent = container.querySelector('.story-row');
    if (!hasContent) {
      container.innerHTML = '<div class="stories-loading">Загрузка...</div>';
    }
    fetch('/production/stories?' + getFilterParams())
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
    var onlyPinned = document.getElementById('filter-only-pinned');
    var forApproval = document.getElementById('filter-for-approval');
    function envPost(key, value) {
      fetch('/production/env', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ key: key, value: value }) });
    }
    function onFilterChange(key, checkbox) {
      var value = checkbox.checked ? '1' : '0';
      fetch('/production/env', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key: key, value: value }),
      }).then(function() {
        window.loadStoryList();
      });
    }
    if (forApproval) {
      forApproval.addEventListener('change', function() {
        if (forApproval.checked) {
          if (onlyGood) onlyGood.checked = false;
          if (showUsed) showUsed.checked = false;
          if (onlyPinned) onlyPinned.checked = false;
          envPost('screenwriter_only_good', '0');
          envPost('screenwriter_show_used', '0');
          envPost('screenwriter_only_pinned', '0');
        }
        onFilterChange('screenwriter_for_approval', forApproval);
      });
    }
    if (showUsed) {
      showUsed.addEventListener('change', function() {
        if (showUsed.checked && forApproval && forApproval.checked) {
          forApproval.checked = false;
          envPost('screenwriter_for_approval', '0');
        }
        onFilterChange('screenwriter_show_used', showUsed);
      });
    }
    if (onlyGood) {
      onlyGood.addEventListener('change', function() {
        if (onlyGood.checked && forApproval && forApproval.checked) {
          forApproval.checked = false;
          envPost('screenwriter_for_approval', '0');
        }
        onFilterChange('screenwriter_only_good', onlyGood);
      });
    }
    if (onlyPinned) {
      onlyPinned.addEventListener('change', function() {
        if (onlyPinned.checked && forApproval && forApproval.checked) {
          forApproval.checked = false;
          envPost('screenwriter_for_approval', '0');
        }
        onFilterChange('screenwriter_only_pinned', onlyPinned);
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

  var _origLoadStoryList = window.loadStoryList;
  window.loadStoryList = function() {
    if (_origLoadStoryList) _origLoadStoryList();
    if (typeof loadGoodPoolCount === 'function') loadGoodPoolCount();
  };

  function initAll() {
    initNewStoryButton();
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
                  if (typeof loadStoryList === 'function') {
                    loadStoryList();
                    setTimeout(function() {
                      var container = document.getElementById('stories-list');
                      if (container) {
                        var row = container.querySelector('.story-row[data-id="' + storyId + '"]');
                        if (row) row.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
                      }
                    }, 400);
                  }
                  if (typeof window.loadMovieList === 'function') window.loadMovieList();
                  pollNext();
                })
                .catch(function() {
                  if (typeof loadStoryList === 'function') loadStoryList();
                  if (typeof window.loadMovieList === 'function') window.loadMovieList();
                  pollNext();
                });
            } else {
              if (typeof loadStoryList === 'function') loadStoryList();
              if (typeof window.loadMovieList === 'function') window.loadMovieList();
              pollNext();
            }
          } else if (status === 'error' || status === 'cancelled' || status === 'fatal_error') {
            _batchDone++;
            if (typeof loadStoryList === 'function') loadStoryList();
            if (typeof window.loadMovieList === 'function') window.loadMovieList();
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
      var modelIdEl = document.getElementById('screenwriter-model-id');
      var modelId = modelIdEl ? (modelIdEl.value || '') : '';
      btn.disabled = true;
      _batchQueue = [];
      _batchTotal = count;
      _batchDone  = 0;
      setHint(count > 1 ? 'Создаю ' + count + ' батчей…' : 'Запускаю генерацию…');
      var body = modelId ? JSON.stringify({ model_id: modelId }) : null;
      var remaining = count;
      for (var i = 0; i < count; i++) {
        fetch('/production/story/generate', {
          method: 'POST',
          headers: body ? { 'Content-Type': 'application/json' } : {},
          body: body,
        })
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

/* ── Плашка grade в карточке сюжета ── */
(function() {
  var GRADE_LABELS = { good: 'хорошо', bad: 'плохо', 'null': 'не указано' };
  var GRADE_COLORS = { good: '#3ecf8e', bad: '#ff6060', 'null': 'rgba(255,255,255,.07)' };
  var GRADE_TEXT_COLORS = { good: '#fff', bad: '#fff', 'null': '#aaa' };
  var GRADE_CYCLE = ['good', 'bad', null];

  function gradeKey(g) { return g === null || g === undefined ? 'null' : g; }

  function setCardGradeBadge(grade, hidden) {
    var btn = document.getElementById('card-story-grade');
    if (!btn) return;
    if (hidden) {
      btn.hidden = true;
      btn.disabled = true;
      return;
    }
    var gk = gradeKey(grade);
    btn.hidden = false;
    btn.disabled = false;
    btn.setAttribute('data-grade', gk);
    btn.style.background = gk !== 'null' ? (GRADE_COLORS[gk] || '') : '';
    btn.style.color = gk !== 'null' ? (GRADE_TEXT_COLORS[gk] || '') : '';
    btn.textContent = GRADE_LABELS[gk] || gk;
    btn.title = 'Оценка: ' + (GRADE_LABELS[gk] || gk) + '. Нажмите для смены';
  }

  window.setCardGradeBadge = setCardGradeBadge;

  function _syncStoryListGrade(storyId, grade) {
    var container = document.getElementById('stories-list');
    if (!container) return;
    var listBtn = container.querySelector('.story-grade-badge[data-id="' + storyId + '"]');
    if (!listBtn) return;
    var gk = gradeKey(grade);
    listBtn.setAttribute('data-grade', gk);
    listBtn.style.background = gk !== 'null' ? (GRADE_COLORS[gk] || '') : '';
    listBtn.style.color = gk !== 'null' ? (GRADE_TEXT_COLORS[gk] || '') : '';
    listBtn.textContent = GRADE_LABELS[gk] || gk;
    listBtn.title = 'Оценка: ' + (GRADE_LABELS[gk] || gk) + '. Нажмите для смены';
  }

  function initCardGradeBadge() {
    var btn = document.getElementById('card-story-grade');
    if (!btn) return;
    var _cardReqId = 0;
    btn.addEventListener('click', function() {
      if (btn.disabled) return;
      var storyId = typeof getDraftStoryId === 'function' ? getDraftStoryId() : null;
      if (!storyId) return;
      var currentGradeAttr = btn.getAttribute('data-grade');
      var currentGrade = currentGradeAttr === 'null' ? null : currentGradeAttr;
      var idx = GRADE_CYCLE.indexOf(currentGrade);
      var nextGrade = GRADE_CYCLE[(idx + 1) % GRADE_CYCLE.length];
      var prevAttr = currentGradeAttr;
      var myReqId = ++_cardReqId;
      setCardGradeBadge(nextGrade, false);
      _syncStoryListGrade(storyId, nextGrade);
      fetch('/production/story/' + storyId + '/grade', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ grade: nextGrade }),
      })
      .then(function(r) { return r.ok ? r.json() : null; })
      .then(function(d) {
        if (myReqId !== _cardReqId) return;
        if (d && d.ok) {
          var grade = d.grade !== undefined ? d.grade : null;
          setCardGradeBadge(grade, false);
          _syncStoryListGrade(storyId, grade);
          if (typeof window.loadStoryList === 'function') window.loadStoryList();
        } else {
          var prev = prevAttr === 'null' ? null : prevAttr;
          setCardGradeBadge(prev, false);
          _syncStoryListGrade(storyId, prev);
        }
      })
      .catch(function() {
        if (myReqId !== _cardReqId) return;
        var prev = prevAttr === 'null' ? null : prevAttr;
        setCardGradeBadge(prev, false);
        _syncStoryListGrade(storyId, prev);
      });
    });
  }

  var _origSetDraft = window.setDraftStoryFromRecord;
  window.setDraftStoryFromRecord = function(story) {
    if (_origSetDraft) _origSetDraft(story);
    setCardGradeBadge(story.grade !== undefined ? story.grade : null, false);
  };

  var _origResetDraft = window.resetDraftStoryId;
  window.resetDraftStoryId = function() {
    if (_origResetDraft) _origResetDraft();
    setCardGradeBadge(null, true);
  };

  var _origFirstSaved = window.onDraftStoryFirstSaved;
  window.onDraftStoryFirstSaved = function() {
    if (_origFirstSaved) _origFirstSaved();
    setCardGradeBadge(null, false);
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCardGradeBadge);
  } else {
    initCardGradeBadge();
  }
})();

/* ── Кнопка pin в карточке сюжета ── */
(function() {
  var PIN_SVG_FILLED  = '<svg viewBox="0 0 16 16" fill="currentColor" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><line x1="8" y1="10" x2="8" y2="15"/><path d="M5 2 L5 7 L2 10 L14 10 L11 7 L11 2 Z"/><line x1="5" y1="2" x2="11" y2="2"/></svg>';
  var PIN_SVG_OUTLINE = '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><line x1="8" y1="10" x2="8" y2="15"/><path d="M5 2 L5 7 L2 10 L14 10 L11 7 L11 2 Z"/><line x1="5" y1="2" x2="11" y2="2"/></svg>';

  function setCardPinBtn(pinned, hidden) {
    var btn = document.getElementById('card-story-pin');
    if (!btn) return;
    if (hidden) {
      btn.hidden = true;
      btn.disabled = true;
      return;
    }
    var p = !!pinned;
    btn.hidden = false;
    btn.disabled = false;
    btn.setAttribute('data-pinned', p ? '1' : '0');
    btn.title = p ? 'Закреплён' : 'Закрепить';
    btn.innerHTML = p ? PIN_SVG_FILLED : PIN_SVG_OUTLINE;
    btn.classList.toggle('story-pin-btn--active', p);
  }

  window.setCardPinBtn = setCardPinBtn;

  function initCardPinBtn() {
    var btn = document.getElementById('card-story-pin');
    if (!btn) return;
    btn.addEventListener('click', function() {
      if (btn.disabled) return;
      var storyId = typeof getDraftStoryId === 'function' ? getDraftStoryId() : null;
      if (!storyId) return;
      var newPinned = btn.getAttribute('data-pinned') !== '1';
      btn.disabled = true;
      fetch('/production/story/' + storyId + '/pin', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pinned: newPinned }),
      })
      .then(function(r) { return r.ok ? r.json() : null; })
      .then(function(d) {
        btn.disabled = false;
        if (d && d.ok) {
          setCardPinBtn(newPinned, false);
          if (typeof window.loadStoryList === 'function') window.loadStoryList();
        }
      })
      .catch(function() { btn.disabled = false; });
    });
  }

  var _origSetDraft = window.setDraftStoryFromRecord;
  window.setDraftStoryFromRecord = function(story) {
    if (_origSetDraft) _origSetDraft(story);
    setCardPinBtn(story.pinned || false, false);
  };

  var _origResetDraft = window.resetDraftStoryId;
  window.resetDraftStoryId = function() {
    if (_origResetDraft) _origResetDraft();
    setCardPinBtn(false, true);
  };

  var _origFirstSaved = window.onDraftStoryFirstSaved;
  window.onDraftStoryFirstSaved = function() {
    if (_origFirstSaved) _origFirstSaved();
    setCardPinBtn(false, false);
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCardPinBtn);
  } else {
    initCardPinBtn();
  }
})();

/* ── Кнопка «Очистить» сюжеты ── */
(function() {
  function _openDeleteBadDialog(btn) {
    new ConfirmDialog({
      title:        'Очистить сюжеты?',
      text:         'Будут удалены все незакреплённые сюжеты без оценки или с оценкой «плохо», по которым не создавалось видео и нет незавершённых батчей,<br>а также связанные с ними батчи и записи лога.',
      confirmLabel: 'Удалить',
      triggerBtn:   btn,
      onConfirm: function(confirmBtn, dlg) {
        confirmBtn.disabled    = true;
        confirmBtn.textContent = 'Удаление…';
        btn.classList.add('pending');
        fetch('/production/stories/delete_bad', { method: 'POST' })
          .then(function(r) { return r.ok ? r.json() : null; })
          .then(function(d) {
            btn.classList.remove('pending');
            dlg.close();
            if (d && d.ok) {
              var n = d.deleted ? (d.deleted.stories || 0) : 0;
              var mod10 = n % 10, mod100 = n % 100;
              var word;
              if (mod100 >= 11 && mod100 <= 14) { word = 'сюжетов'; }
              else if (mod10 === 1)              { word = 'сюжет'; }
              else if (mod10 >= 2 && mod10 <= 4) { word = 'сюжета'; }
              else                               { word = 'сюжетов'; }
              if (typeof window.showToast === 'function') window.showToast('Удалено ' + n + ' ' + word);
              if (typeof window.loadStoryList === 'function') window.loadStoryList();
            }
          })
          .catch(function() { btn.classList.remove('pending'); dlg.close(); });
      },
    }).open();
  }

  function initDeleteBadStoriesButton() {
    var btn = document.getElementById('btn-delete-bad-stories');
    if (!btn) return;
    btn.addEventListener('click', function() {
      if (btn.classList.contains('pending')) return;
      _openDeleteBadDialog(btn);
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initDeleteBadStoriesButton);
  } else {
    initDeleteBadStoriesButton();
  }
})();

/* ── Комбобокс выбора текстовой модели в блоке Сценарий ── */
(function() {
  var _models = [];

  function selectModel(id, label) {
    var lbl = document.getElementById('screenwriter-model-label');
    var hid = document.getElementById('screenwriter-model-id');
    if (lbl) lbl.textContent = label;
    if (hid) hid.value = id;
    document.querySelectorAll('#screenwriter-model-list .cust-select-option').forEach(function(o) {
      o.classList.toggle('selected', o.dataset.value === String(id));
    });
  }

  function closeList() {
    var list = document.getElementById('screenwriter-model-list');
    var btn  = document.getElementById('screenwriter-model-btn');
    if (list) list.hidden = true;
    if (btn)  btn.classList.remove('open');
  }

  function buildList() {
    var list = document.getElementById('screenwriter-model-list');
    if (!list) return;
    list.innerHTML = '';
    var defaultOpt = document.createElement('div');
    defaultOpt.className = 'cust-select-option selected';
    defaultOpt.dataset.value = '';
    defaultOpt.textContent = 'Подобрать';
    defaultOpt.addEventListener('click', function() { selectModel('', 'Подобрать'); closeList(); });
    list.appendChild(defaultOpt);
    _models.forEach(function(m) {
      var opt = document.createElement('div');
      opt.className = 'cust-select-option';
      opt.dataset.value = String(m.id);
      opt.textContent = m.name;
      opt.addEventListener('click', function() {
        selectModel(m.id, m.name);
        closeList();
      });
      list.appendChild(opt);
    });
    selectModel('', 'Подобрать');
  }

  function loadScreenwriterModels() {
    fetch('/api/text-models')
      .then(function(r) { return r.ok ? r.json() : []; })
      .then(function(models) {
        _models = (models || []).filter(function(m) { return m.grade === 'good'; });
        buildList();
      })
      .catch(function() {});
  }

  function initModelCombobox() {
    var btn = document.getElementById('screenwriter-model-btn');
    if (!btn) return;
    btn.addEventListener('click', function(e) {
      e.stopPropagation();
      var list = document.getElementById('screenwriter-model-list');
      if (!list) return;
      if (list.hidden) {
        list.hidden = false;
        btn.classList.add('open');
      } else {
        closeList();
      }
    });
    document.addEventListener('click', function(e) {
      var wrap = document.getElementById('screenwriter-model-wrap');
      if (wrap && !wrap.contains(e.target)) closeList();
    });
    loadScreenwriterModels();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initModelCombobox);
  } else {
    initModelCombobox();
  }
})();

/* ── Счётчик слов в карточке сюжета ── */
(function() {
  var _videoDuration = 6;
  var _wordsPerSecond = 8;
  var _currentDraftModel = '';

  function _readConfig() {
    var titleEl = document.querySelector('[data-video-duration]');
    if (titleEl) {
      var d = parseInt(titleEl.getAttribute('data-video-duration'), 10);
      var w = parseInt(titleEl.getAttribute('data-words-per-second'), 10);
      if (d > 0) _videoDuration = d;
      if (w > 0) _wordsPerSecond = w;
    }
  }

  function countWords(text) {
    return text.trim() === '' ? 0 : text.trim().split(/\s+/).length;
  }

  function updateWordCount() {
    var textarea = document.getElementById('draft-story-content');
    var wrap = document.getElementById('draft-story-wc-wrap');
    var counter = document.getElementById('draft-story-wc');
    if (!textarea || !wrap || !counter) return;
    var n = countWords(textarea.value);
    if (n > 0) {
      var wcStr = 'слов:\u00a0' + n;
      var label = _currentDraftModel ? _currentDraftModel + ', ' + wcStr : wcStr;
      counter.textContent = label;
      var threshold = _videoDuration * _wordsPerSecond;
      if (n <= threshold) {
        counter.style.color = '#4caf50';
      } else if (n <= threshold * 1.2) {
        counter.style.color = '#f5a623';
      } else {
        counter.style.color = '#e53935';
      }
      wrap.style.display = '';
    } else {
      wrap.style.display = 'none';
    }
  }

  window.updateDraftWordCount = updateWordCount;

  var _wpsTimer = null;
  function _saveWordsPerSecond(value) {
    clearTimeout(_wpsTimer);
    _wpsTimer = setTimeout(function() {
      fetch('/api/cycle-config/words-per-second', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({value: value})
      });
    }, 500);
  }

  function initWordCount() {
    _readConfig();
    var textarea = document.getElementById('draft-story-content');
    if (textarea) {
      textarea.addEventListener('input', updateWordCount);
      updateWordCount();
    }
    var wpsInput = document.getElementById('words-per-second-input');
    if (wpsInput) {
      wpsInput.addEventListener('input', function() {
        var v = parseInt(wpsInput.value, 10);
        if (v > 0 && v <= 100) {
          _wordsPerSecond = v;
          updateWordCount();
          _saveWordsPerSecond(v);
        }
      });
    }
  }

  var _origSetDraft = window.setDraftStoryFromRecord;
  window.setDraftStoryFromRecord = function(story) {
    _currentDraftModel = (story && story.model_name) ? story.model_name : '';
    if (_origSetDraft) _origSetDraft(story);
    updateWordCount();
  };

  var _origResetDraft = window.resetDraftStoryId;
  window.resetDraftStoryId = function() {
    _currentDraftModel = '';
    if (_origResetDraft) _origResetDraft();
    var wrap = document.getElementById('draft-story-wc-wrap');
    if (wrap) wrap.style.display = 'none';
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initWordCount);
  } else {
    initWordCount();
  }
})();
