/* ── Автосохранение черновика сюжета ── */
var resetDraftStoryId;
var setDraftStoryFromRecord;
var getDraftStoryId;
(function() {
  var _draftStoryId = null;
  var _draftTimer = null;
  var _draftSaving = false;
  var _draftPendingRetry = false;

  getDraftStoryId = function() { return _draftStoryId; };

  resetDraftStoryId = function() {
    _draftStoryId = null;
    _draftSaving = false;
    _draftPendingRetry = false;
    clearTimeout(_draftTimer);
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
        _draftStoryId = d.story_id;
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
  var _currentStories = [];
  var _expandedStoryId = null;
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

  /* ── Управление карточкой-синглтоном ── */
  function _getCard() { return document.getElementById('card-story-draft'); }
  function _getHolder() { return document.getElementById('story-card-holder'); }

  function _saveFocus() {
    var ae = document.activeElement;
    var card = _getCard();
    if (!ae || !card || !card.contains(ae)) return null;
    return {
      id: ae.id,
      start: ae.selectionStart != null ? ae.selectionStart : null,
      end:   ae.selectionEnd   != null ? ae.selectionEnd   : null,
    };
  }

  function _restoreFocus(saved) {
    if (!saved || !saved.id) return;
    var el = document.getElementById(saved.id);
    if (!el) return;
    el.focus();
    if (saved.start !== null) {
      try { el.setSelectionRange(saved.start, saved.end); } catch (e) {}
    }
  }

  function _detachCard() {
    var card = _getCard();
    var holder = _getHolder();
    if (card && holder && card.parentNode !== holder) holder.appendChild(card);
  }

  function _attachCardToExpand(expandEl) {
    var card = _getCard();
    if (card && expandEl) expandEl.appendChild(card);
  }

  function _insertFakeRow(container, focusTitle) {
    var existing = container.querySelector('.story-row[data-id="__new__"]');
    if (existing) existing.remove();
    var chevronSvg = '<svg viewBox="0 0 12 7" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M1 1l5 5 5-5"/></svg>';
    var fakeRow = document.createElement('div');
    fakeRow.className = 'story-row story-row--expanded';
    fakeRow.setAttribute('data-id', '__new__');
    fakeRow.innerHTML =
      '<div class="story-row-header">' +
        '<div class="story-title" style="color:#888;font-style:italic">Новый сюжет</div>' +
        '<div class="story-row-right">' +
          '<span class="story-chevron">' + chevronSvg + '</span>' +
        '</div>' +
      '</div>' +
      '<div class="story-expand"></div>';
    container.insertBefore(fakeRow, container.firstChild);
    var expandEl = fakeRow.querySelector('.story-expand');
    if (expandEl) _attachCardToExpand(expandEl);
    _expandedStoryId = '__new__';
    fakeRow.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    if (focusTitle) {
      var titleEl = document.getElementById('draft-story-title');
      if (titleEl) titleEl.focus();
    }
  }

  /* Экспозиция для других модулей */
  window.setExpandedStoryId = function(id) { _expandedStoryId = id; };

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

  function _setExportStoriesBtnEnabled(enabled) {
    var btn = document.getElementById('btn-export-stories');
    if (!btn) return;
    if (enabled) {
      btn.removeAttribute('disabled');
    } else {
      btn.setAttribute('disabled', 'disabled');
    }
  }

  function _findStory(id) {
    for (var j = 0; j < _currentStories.length; j++) {
      if (String(_currentStories[j].id) === String(id)) return _currentStories[j];
    }
    return null;
  }

  function renderStories(stories) {
    var container = document.getElementById('stories-list');
    if (!container) return;

    var savedFocus = _saveFocus();
    var prevExpandedId = _expandedStoryId;
    _detachCard();

    if (!stories || stories.length === 0) {
      _currentStories = [];
      window._currentStoriesList = [];
      _setExportStoriesBtnEnabled(false);
      updateStoriesCount(0);
      _expandedStoryId = null;
      container.innerHTML = '<div class="stories-empty">Нет сюжетов</div>';
      return;
    }

    _currentStories = stories;
    window._currentStoriesList = stories;
    _setExportStoriesBtnEnabled(true);
    updateStoriesCount(stories.length);

    var chevronSvg = '<svg viewBox="0 0 12 7" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M1 1l5 5 5-5"/></svg>';
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
        '<div class="story-row-header">' +
          '<div class="story-title">' + escapeHtml(s.title || '(без названия)') + modelLabel + ' ' + gradeBadge + '</div>' +
          '<div class="story-row-right">' + icons + pinBtn + deleteBtn + exportBtn +
            '<button class="story-chevron" title="Развернуть">' + chevronSvg + '</button>' +
          '</div>' +
        '</div>' +
        '<div class="story-expand"></div>' +
      '</div>';
    }
    container.innerHTML = html;

    container.querySelectorAll('.story-grade-badge').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
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

    /* ── Аккордеон: клик по заголовку строки ── */
    container.querySelectorAll('.story-row').forEach(function(row) {
      var storyId = row.getAttribute('data-id');
      var header = row.querySelector('.story-row-header');
      if (!header) return;
      header.addEventListener('click', function() {
        if (_expandedStoryId === storyId) {
          row.classList.remove('story-row--expanded');
          _detachCard();
          _expandedStoryId = null;
        } else {
          if (_expandedStoryId) {
            var cur = container.querySelector('.story-row--expanded');
            if (cur) cur.classList.remove('story-row--expanded');
            _detachCard();
          }
          _expandedStoryId = storyId;
          row.classList.add('story-row--expanded');
          var expandEl = row.querySelector('.story-expand');
          if (expandEl) _attachCardToExpand(expandEl);
          var storyObj = _findStory(storyId);
          if (storyObj && typeof setDraftStoryFromRecord === 'function') setDraftStoryFromRecord(storyObj);
          row.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
          if (typeof window.loadStoryList === 'function') window.loadStoryList();
        }
      });
    });

    /* ── Восстановление раскрытой строки после перерисовки ── */
    if (prevExpandedId === '__new__') {
      var draftId = typeof getDraftStoryId === 'function' ? getDraftStoryId() : null;
      if (draftId) {
        var realRow = container.querySelector('.story-row[data-id="' + draftId + '"]');
        if (realRow) {
          _expandedStoryId = draftId;
          realRow.classList.add('story-row--expanded');
          var expandEl2 = realRow.querySelector('.story-expand');
          if (expandEl2) _attachCardToExpand(expandEl2);
        } else {
          _expandedStoryId = null;
        }
      } else {
        _insertFakeRow(container);
      }
    } else if (prevExpandedId) {
      var expandRow = container.querySelector('.story-row[data-id="' + prevExpandedId + '"]');
      if (expandRow) {
        _expandedStoryId = prevExpandedId;
        expandRow.classList.add('story-row--expanded');
        var expandEl3 = expandRow.querySelector('.story-expand');
        if (expandEl3) _attachCardToExpand(expandEl3);
      } else {
        _expandedStoryId = null;
      }
    }

    _restoreFocus(savedFocus);
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
            if (_expandedStoryId === storyId) {
              _detachCard();
              _expandedStoryId = null;
            }
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
      } else {
        var prevGk = gradeKey(prevAttr === 'null' ? null : prevAttr);
        btn.setAttribute('data-grade', prevGk);
        btn.style.background = prevGk !== 'null' ? (GRADE_COLORS[prevGk] || '') : '';
        btn.style.color = prevGk !== 'null' ? (GRADE_TEXT_COLORS[prevGk] || '') : '';
        btn.textContent = GRADE_LABELS[prevGk] || prevGk;
        btn.title = 'Оценка: ' + (GRADE_LABELS[prevGk] || prevGk) + '. Нажмите для смены';
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
    });
  }

  function getFilterParams() {
    var showUsed = document.getElementById('filter-show-used');
    var onlyGood = document.getElementById('filter-only-good');
    var onlyBad = document.getElementById('filter-only-bad');
    var onlyPinned = document.getElementById('filter-only-pinned');
    var forApproval = document.getElementById('filter-for-approval');
    var params = new URLSearchParams();
    if (forApproval && forApproval.checked) {
      params.set('for_approval', '1');
    } else {
      params.set('show_used', (showUsed && showUsed.checked) ? '1' : '0');
      params.set('show_bad', (onlyGood && onlyGood.checked) ? '0' : '1');
      if (onlyBad && onlyBad.checked) params.set('only_bad', '1');
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
    var onlyBad = document.getElementById('filter-only-bad');
    var onlyPinned = document.getElementById('filter-only-pinned');
    var forApproval = document.getElementById('filter-for-approval');
    function envPost(key, value) {
      fetch('/production/env', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ key: key, value: value }) });
    }
    function onFilterChange(key, checkbox) {
      fetch('/production/env', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key: key, value: checkbox.checked ? '1' : '0' }),
      }).then(function() {
        window.loadStoryList();
      });
    }
    if (forApproval) {
      forApproval.addEventListener('change', function() {
        if (forApproval.checked) {
          if (onlyGood) onlyGood.checked = false;
          if (onlyBad) onlyBad.checked = false;
          if (showUsed) showUsed.checked = false;
          if (onlyPinned) onlyPinned.checked = false;
          envPost('screenwriter_only_good', '0');
          envPost('screenwriter_only_bad', '0');
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
    if (onlyBad) {
      onlyBad.addEventListener('change', function() {
        if (onlyBad.checked && forApproval && forApproval.checked) {
          forApproval.checked = false;
          envPost('screenwriter_for_approval', '0');
        }
        onFilterChange('screenwriter_only_bad', onlyBad);
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
      var container = document.getElementById('stories-list');
      if (!container) return;
      if (_expandedStoryId) {
        var cur = container.querySelector('.story-row--expanded');
        if (cur) cur.classList.remove('story-row--expanded');
        _detachCard();
        _expandedStoryId = null;
      }
      if (typeof resetDraftStoryId === 'function') resetDraftStoryId();
      var titleEl = document.getElementById('draft-story-title');
      var contentEl = document.getElementById('draft-story-content');
      if (titleEl) titleEl.value = '';
      if (contentEl) contentEl.value = '';
      if (typeof window.updateDraftWordCount === 'function') window.updateDraftWordCount();
      _insertFakeRow(container, true);
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
                  if (typeof window.setExpandedStoryId === 'function') window.setExpandedStoryId(storyId);
                  if (typeof loadStoryList === 'function') loadStoryList();
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

/* ── Кнопка выгрузки текущего списка сюжетов ── */
(function() {
  function initExportStoriesBtn() {
    var btn = document.getElementById('btn-export-stories');
    if (!btn) return;
    btn.addEventListener('click', function() {
      var stories = window._currentStoriesList;
      if (!stories || stories.length === 0) return;
      var parts = [];
      for (var i = 0; i < stories.length; i++) {
        var s = stories[i];
        var body = (s.title || '') + '\n\n' + (s.content || '');
        parts.push(window.wrapBlock('Сюжет', body, i + 1));
      }
      var text = parts.join('\n\n');
      navigator.clipboard.writeText(text).then(function() {
        btn.classList.add('copied');
        setTimeout(function() { btn.classList.remove('copied'); }, 2000);
      }).catch(function() {});
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initExportStoriesBtn);
  } else {
    initExportStoriesBtn();
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
    defaultOpt.addEventListener('click', function() {
      selectModel('', 'Подобрать');
      closeList();
    });
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

  function _readConfig() {
    var el = document.querySelector('[data-video-duration]');
    if (el) {
      var d = parseInt(el.getAttribute('data-video-duration'), 10);
      var w = parseInt(el.getAttribute('data-words-per-second'), 10);
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
    var numEl = document.getElementById('draft-story-wc-num');
    if (!textarea || !wrap || !numEl) return;
    var n = countWords(textarea.value);
    if (n > 0) {
      numEl.textContent = n;
      var threshold = _videoDuration * _wordsPerSecond;
      if (n <= threshold) {
        numEl.style.color = '#4caf50';
      } else if (n <= threshold * 1.2) {
        numEl.style.color = '#f5a623';
      } else {
        numEl.style.color = '#e53935';
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

  var _gscTimer = null;
  function _saveGoodSamplesCount(value) {
    clearTimeout(_gscTimer);
    _gscTimer = setTimeout(function() {
      fetch('/api/cycle-config/good-samples-count', {
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
    var gscInput = document.getElementById('good-samples-count-input');
    if (gscInput) {
      gscInput.addEventListener('input', function() {
        var v = parseInt(gscInput.value, 10);
        if (v >= 1) {
          _saveGoodSamplesCount(v);
        }
      });
    }
  }

  var _origSetDraft = window.setDraftStoryFromRecord;
  window.setDraftStoryFromRecord = function(story) {
    if (_origSetDraft) _origSetDraft(story);
    updateWordCount();
  };

  var _origResetDraft = window.resetDraftStoryId;
  window.resetDraftStoryId = function() {
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
