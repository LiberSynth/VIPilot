/* ── Единое состояние: ID активного сюжета в карточке ── */
/* null — карточка пуста (ничего не открыто или фиктивная строка нового черновика) */
/* guid — открыт существующий сюжет или черновик уже сохранён в БД              */
var _activeStoryId = null;

/* ── Автосохранение черновика сюжета ── */
var resetDraftStoryId;
var setDraftStoryFromRecord;
(function() {
  var _draftTimer = null;
  var _inflight = null;
  var _pendingRetry = false;

  resetDraftStoryId = function() {
    _activeStoryId = null;
    _inflight = null;
    _pendingRetry = false;
    clearTimeout(_draftTimer);
  };

  setDraftStoryFromRecord = function(story) {
    var titleEl = document.getElementById('draft-story-title');
    var contentEl = document.getElementById('draft-story-content');
    if (titleEl) titleEl.value = story.title || '';
    if (contentEl) contentEl.value = story.content || '';
    _activeStoryId = story.id;
    _inflight = null;
    _pendingRetry = false;
    clearTimeout(_draftTimer);
  };

  function _doPost() {
    var titleEl = document.getElementById('draft-story-title');
    var contentEl = document.getElementById('draft-story-content');
    var title = titleEl ? titleEl.value : '';
    var content = contentEl ? contentEl.value : '';
    return fetch('/production/story/draft', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ story_id: _activeStoryId, title: title, content: content }),
    })
    .then(function(r) { return r.ok ? r.json() : null; })
    .then(function(d) {
      var newId = d && d.story_id ? String(d.story_id) : null;
      if (newId) {
        var wasNew = !_activeStoryId;
        _activeStoryId = newId;
        if (wasNew) {
          if (typeof window.setExpandedStoryId === 'function') window.setExpandedStoryId(_activeStoryId);
          if (typeof loadStoryList === 'function') loadStoryList();
        }
      }
      return newId;
    });
  }

  function _postDraft(force) {
    var titleEl = document.getElementById('draft-story-title');
    var contentEl = document.getElementById('draft-story-content');
    if (!titleEl || !contentEl) return Promise.resolve(null);
    var title = titleEl.value;
    var content = contentEl.value;
    if (!force && !title && !content) return Promise.resolve(null);
    if (_inflight) {
      if (force) {
        return _inflight.then(function() {
          if (_activeStoryId) return _activeStoryId;
          return _postDraft(true);
        });
      }
      _pendingRetry = true;
      return _inflight;
    }
    _inflight = _doPost()
      .then(function(id) {
        _inflight = null;
        if (_pendingRetry) {
          _pendingRetry = false;
          _postDraft(false);
        }
        return id;
      })
      .catch(function(err) {
        _inflight = null;
        if (_pendingRetry) {
          _pendingRetry = false;
          _postDraft(false);
        }
        throw err;
      });
    return _inflight;
  }

  function saveDraft() { _postDraft(false).catch(function() {}); }

  window.ensureDraftStoryId = function() {
    if (_activeStoryId) return Promise.resolve(_activeStoryId);
    return _postDraft(true).then(function(id) {
      if (!id) throw new Error('failed_to_create_draft');
      return id;
    });
  };

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
  function _setExportStoriesBtnEnabled(enabled) {
    var btn = document.getElementById('btn-export-stories');
    if (!btn) return;
    if (enabled) {
      btn.removeAttribute('disabled');
    } else {
      btn.setAttribute('disabled', 'disabled');
    }
  }

  function _renderStoryIcons(s) {
    var icons = '';
    if (s.used) {
      icons += '<span class="story-icon story-icon-used" title="Использован в производстве">'
        + '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">'
        + '<polyline points="2,8 6,12 14,4"/></svg></span>';
    }
    if (s.ai_generated && s.manual_changed) {
      icons += '<span class="story-icon story-icon-ai-manual" title="Происхождение: Сгенерировано AI, отредактировано вручную">'
        + '<svg viewBox="0 0 26 24" fill="none" stroke="none" stroke-linecap="round" stroke-linejoin="round">'
        + '<path d="M14 21v-1.5a3.5 3.5 0 0 0-3.5-3.5H3.5A3.5 3.5 0 0 0 0 19.5V21" stroke="#fbbf24" stroke-width="1.7"/>'
        + '<circle cx="8.5" cy="7.5" r="3.8" stroke="#fbbf24" stroke-width="1.7"/>'
        + '<path d="M21 1 L22.1 4.4 L25.5 5.5 L22.1 6.6 L21 10 L19.9 6.6 L16.5 5.5 L19.9 4.4 Z" fill="#ffffff"/>'
        + '</svg></span>';
    } else if (s.manual_changed) {
      icons += '<span class="story-icon story-icon-manual" title="Происхождение: Написано вручную">'
        + '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">'
        + '<path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg></span>';
    } else {
      icons += '<span class="story-icon story-icon-ai" title="Происхождение: Сгенерировано AI">'
        + '<svg viewBox="0 0 16 16" fill="currentColor" stroke="none">'
        + '<path d="M8 1 L9.3 6.7 L15 8 L9.3 9.3 L8 15 L6.7 9.3 L1 8 L6.7 6.7 Z"/></svg></span>';
    }
    return icons;
  }

  function _renderPinBtn(s) {
    var pinTitle = s.pinned ? 'Закреплён' : 'Закрепить';
    return '<button class="story-icon story-pin-btn' + (s.pinned ? ' story-pin-btn--active' : '')
      + '" data-id="' + s.id + '" data-pinned="' + (s.pinned ? '1' : '0') + '" title="' + pinTitle + '">'
      + '<svg viewBox="0 0 16 16" fill="' + (s.pinned ? 'currentColor' : 'none') + '" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">'
      + '<line x1="8" y1="10" x2="8" y2="15"/>'
      + '<path d="M5 2 L5 7 L2 10 L14 10 L11 7 L11 2 Z"/>'
      + '<line x1="5" y1="2" x2="11" y2="2"/>'
      + '</svg></button>';
  }

  function _renderDeleteBtn(s) {
    if (s.id === '__new__') {
      return '<button class="story-icon story-delete-btn btn-blocked" data-id="__new__"'
        + ' title="Нельзя удалить: сюжет ещё не создан">'
        + '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">'
        + '<polyline points="2,4 14,4"/><path d="M6 4V2h4v2"/><rect x="3" y="4" width="10" height="10" rx="1.5"/><line x1="6" y1="7" x2="6" y2="11"/><line x1="10" y1="7" x2="10" y2="11"/>'
        + '</svg></button>';
    }
    var deleteDisabled = s.pinned || s.has_movie || s.has_active_batch;
    var deleteBlockReason = deleteDisabled
      ? (s.pinned ? 'Сюжет закреплён' : (s.has_movie ? 'К сюжету привязано готовое видео' : 'У сюжета есть активный батч'))
      : '';
    var deleteTitle = deleteDisabled
      ? (s.pinned ? 'Нельзя удалить: сюжет закреплён' : (s.has_movie ? 'Нельзя удалить: к сюжету привязано видео' : 'Нельзя удалить: есть активный батч'))
      : 'Удалить';
    var storyTitleEsc = AccordionList.escapeHtml(s.title || '(без названия)');
    return '<button class="story-icon story-delete-btn' + (deleteDisabled ? ' btn-blocked' : '')
      + '" data-id="' + s.id + '" data-title="' + storyTitleEsc + '"'
      + (deleteDisabled ? ' data-block-reason="' + AccordionList.escapeHtml(deleteBlockReason) + '"' : '')
      + ' title="' + deleteTitle + '">'
      + '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">'
      + '<polyline points="2,4 14,4"/><path d="M6 4V2h4v2"/><rect x="3" y="4" width="10" height="10" rx="1.5"/><line x1="6" y1="7" x2="6" y2="11"/><line x1="10" y1="7" x2="10" y2="11"/>'
      + '</svg></button>';
  }

  function _renderExportBtn(s) {
    var mode = (s.id === '__new__') ? 'manual-new'
             : (s.manual_changed && !s.ai_generated) ? 'manual'
             : 'ai';
    return '<button class="story-icon story-export-btn" data-id="' + s.id
      + '" data-mode="' + mode + '"'
      + ' title="Трассировка: накапливаются в буфере, сброс через 10 с">'
      + (window.EXPORT_STORY_SVG || '') + '</button>';
  }

  var _accordionList = new AccordionList({
    listId:   'stories-list',
    cardId:   'card-story-draft',
    holderId: 'story-card-holder',
    countId:  'stories-count',
    gradeUrl: function(id) { return '/production/story/' + id + '/grade'; },
    renderTitle: function(item) {
      var modelLabel = item.model_name
        ? ' <span class="story-model-name">(' + AccordionList.escapeHtml(item.model_name) + ')</span>'
        : '';
      var titleHtml = AccordionList.escapeHtml(item.title || '(без названия)');
      if (item._dim) titleHtml = '<span class="story-title-dim">' + titleHtml + '</span>';
      return titleHtml + modelLabel;
    },
    renderButtons: function(item) {
      return _renderStoryIcons(item) + _renderPinBtn(item) + _renderDeleteBtn(item) + _renderExportBtn(item);
    },
    onExpand: function(item) {
      _activeStoryId = item ? item.id : null;
      if (item && typeof setDraftStoryFromRecord === 'function') setDraftStoryFromRecord(item);
      if (typeof window.loadStoryList === 'function') window.loadStoryList();
    },
    onCollapse: function() {
      _activeStoryId = null;
    },
    canAddNew: true,
    newRowItem: {
      id: '__new__', grade: null, pinned: false,
      ai_generated: false, manual_changed: true, used: false,
      has_movie: false, has_active_batch: false, title: '', content: '',
    },
    onNewRowReady: function(_expandEl, fakeRow) {
      if (fakeRow) _bindFakeRowButtons(fakeRow);
    },
    emptyHtml: '<div class="stories-empty">Нет сюжетов</div>',
  });

  function _setPinVisual(btn, pinned) {
    btn.setAttribute('data-pinned', pinned ? '1' : '0');
    btn.classList.toggle('story-pin-btn--active', pinned);
    btn.title = pinned ? 'Закреплён' : 'Закрепить';
    var svg = btn.querySelector('svg');
    if (svg) svg.setAttribute('fill', pinned ? 'currentColor' : 'none');
  }

  function _applyGradeBadge(btn, gk) {
    var L = AccordionList.GRADE_LABELS;
    var C = AccordionList.GRADE_COLORS;
    var T = AccordionList.GRADE_TEXT_COLORS;
    btn.setAttribute('data-grade', gk);
    btn.style.background = gk !== 'null' ? (C[gk] || '') : '';
    btn.style.color      = gk !== 'null' ? (T[gk] || '') : '';
    btn.textContent = L[gk] || gk;
    btn.title = 'Оценка: ' + (L[gk] || gk) + '. Нажмите для смены';
  }

  function _bindFakeRowButtons(fakeRow) {
    var pinBtn = fakeRow.querySelector('.story-pin-btn');
    if (pinBtn) {
      pinBtn.addEventListener('click', function(e) {
        e.stopPropagation();
        if (pinBtn.dataset.busy === '1') return;
        var currentPinned = pinBtn.getAttribute('data-pinned') === '1';
        var newPinned = !currentPinned;
        pinBtn.dataset.busy = '1';
        _setPinVisual(pinBtn, newPinned);
        if (typeof window.ensureDraftStoryId !== 'function') {
          pinBtn.dataset.busy = '0';
          return;
        }
        window.ensureDraftStoryId().then(function(id) {
          return fetch('/production/story/' + id + '/pin', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ pinned: newPinned }),
          });
        }).then(function(r) {
          if (!r || !r.ok) throw new Error('pin_failed');
          pinBtn.dataset.busy = '0';
          if (typeof window.loadStoryList === 'function') window.loadStoryList();
        }).catch(function() {
          pinBtn.dataset.busy = '0';
          _setPinVisual(pinBtn, currentPinned);
        });
      });
    }

    var delBtn = fakeRow.querySelector('.story-delete-btn');
    if (delBtn) {
      delBtn.addEventListener('click', function(e) { e.stopPropagation(); });
    }

    var expBtn = fakeRow.querySelector('.story-export-btn');
    if (expBtn) {
      expBtn.addEventListener('click', function(e) {
        e.stopPropagation();
        if (typeof window.exportStory === 'function') window.exportStory(null, expBtn);
      });
    }

    var grBtn = fakeRow.querySelector('.story-grade-badge');
    if (grBtn) {
      grBtn.addEventListener('click', function(e) {
        e.stopPropagation();
        if (grBtn.dataset.busy === '1') return;
        var CYCLE = AccordionList.GRADE_CYCLE;
        var currentAttr = grBtn.getAttribute('data-grade');
        var current = currentAttr === 'null' ? null : currentAttr;
        var idx = CYCLE.indexOf(current);
        var next = CYCLE[(idx + 1) % CYCLE.length];
        var nextKey = AccordionList.gradeKey(next);
        grBtn.dataset.busy = '1';
        _applyGradeBadge(grBtn, nextKey);
        if (typeof window.ensureDraftStoryId !== 'function') {
          grBtn.dataset.busy = '0';
          return;
        }
        window.ensureDraftStoryId().then(function(id) {
          return fetch('/production/story/' + id + '/grade', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ grade: next }),
          });
        }).then(function(r) {
          if (!r || !r.ok) throw new Error('grade_failed');
          grBtn.dataset.busy = '0';
          if (typeof window.loadStoryList === 'function') window.loadStoryList();
        }).catch(function() {
          grBtn.dataset.busy = '0';
          _applyGradeBadge(grBtn, currentAttr);
        });
      });
    }
  }

  window.setExpandedStoryId = function(id) {
    var sid = id != null ? String(id) : null;
    _activeStoryId = sid;
    _accordionList.setActiveId(sid);
  };

  window.openStoryInScreenwriter = function(storyId) {
    if (!storyId) return;
    if (typeof switchPanel === 'function') switchPanel('screenwriter');
    var searchInput = document.getElementById('story-search-input');
    if (searchInput && searchInput.value) {
      searchInput.value = '';
      var clearBtn = document.getElementById('story-search-clear');
      if (clearBtn) clearBtn.hidden = true;
    }
    _activeStoryId = String(storyId);
    var container = document.getElementById('stories-list');
    if (!container) return;
    container.innerHTML = '<div class="stories-loading">Загрузка...</div>';
    fetch('/production/stories?' + getFilterParams())
      .then(function(r) { return r.ok ? r.json() : []; })
      .then(function(stories) {
        renderStories(stories);
        _accordionList.selectRow(String(storyId));
      })
      .catch(function() {
        container.innerHTML = '<div class="stories-empty">Ошибка загрузки</div>';
      });
  };

  function _bindListButtons(container) {
    container.querySelectorAll('.story-pin-btn:not([data-id="__new__"])').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        var storyId = btn.getAttribute('data-id');
        var currentPinned = btn.getAttribute('data-pinned') === '1';
        fetch('/production/story/' + storyId + '/pin', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ pinned: !currentPinned }),
        }).then(function() {
          if (typeof window.loadStoryList === 'function') window.loadStoryList();
        });
      });
    });
    container.querySelectorAll('.story-export-btn:not([data-id="__new__"])').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        var storyId = btn.getAttribute('data-id');
        if (typeof window.exportStory === 'function') window.exportStory(storyId, btn);
      });
    });
    container.querySelectorAll('.story-delete-btn:not([data-id="__new__"])').forEach(function(btn) {
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
  }

  function _openDeleteStoryDialog(storyId, storyTitle, triggerBtn) {
    new ConfirmDialog({
      title:        'Удалить сюжет?',
      text:         'Сюжет «' + AccordionList.escapeHtml(storyTitle) + '» и все связанные батчи, лог и записи лога будут удалены безвозвратно.',
      confirmLabel: 'Удалить',
      triggerBtn:   triggerBtn,
      onConfirm: function(btn, dlg) {
        btn.disabled    = true;
        btn.textContent = 'Удаление…';
        fetch('/production/story/' + encodeURIComponent(storyId) + '/delete', { method: 'DELETE' })
          .then(function(r) { return r.ok ? r.json() : r.json().then(function(d) { throw d; }); })
          .then(function() {
            dlg.close();
            if (_activeStoryId === storyId) {
              _accordionList.collapse();
              _activeStoryId = null;
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

  function renderStories(stories) {
    var container = document.getElementById('stories-list');
    if (!container) return;

    if (!stories || stories.length === 0) {
      window._currentStoriesList = [];
      _setExportStoriesBtnEnabled(false);
      if (!_accordionList.getActiveId()) _activeStoryId = null;
      _accordionList.render([]);
      return;
    }

    window._currentStoriesList = stories;
    _setExportStoriesBtnEnabled(true);
    _accordionList.render(stories);
    _bindListButtons(container);
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
    if (_activeStoryId) params.set('pin_id', _activeStoryId);
    return params.toString();
  }

  window._getStoryFilterParams = getFilterParams;
  window._renderStories = renderStories;

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
      _accordionList.collapse();
      if (typeof resetDraftStoryId === 'function') resetDraftStoryId();
      var titleEl = document.getElementById('draft-story-title');
      var contentEl = document.getElementById('draft-story-content');
      if (titleEl) titleEl.value = '';
      if (contentEl) contentEl.value = '';
      if (typeof window.updateDraftWordCount === 'function') window.updateDraftWordCount();
      _accordionList.insertFakeRow(true);
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
  var _DEFAULT_HINT = 'Вы можете сгенерировать контент при помощи AI-модели.';
  var _FINAL_STATUSES = [
    'published', 'published_partially', 'ready',
    'cancelled', 'error', 'fatal_error',
    'video_error', 'transcode_error', 'publish_error', 'donated',
  ];
  var _controller = null;
  var _isCreating = false;

  function _fallbackMemo(text) {
    var el = document.getElementById('story-generate-console');
    if (el) el.value = text || _DEFAULT_HINT;
  }

  function _refreshLists() {
    if (typeof loadStoryList === 'function') loadStoryList();
    if (typeof window.loadMovieList === 'function') window.loadMovieList();
  }

  function _controllerOrNull() {
    if (_controller) return _controller;
    if (typeof GenerationConsoleController !== 'function') return null;
    _controller = new GenerationConsoleController({
      consoleId: 'story-generate-console',
      defaultHint: _DEFAULT_HINT,
      maxLines: 5,
      pollIntervalMs: 700,
      finalStatuses: _FINAL_STATUSES,
      onBatchFinal: function(_batchId, data) {
        var status = String((data && data.batch_status) || '');
        var storyId = data ? data.story_id : null;
        if (status === 'ready' && storyId) {
          fetch('/api/story/' + encodeURIComponent(storyId))
            .then(function(r) { return r.json(); })
            .then(function(s) {
              if (typeof setDraftStoryFromRecord === 'function') {
                setDraftStoryFromRecord({ id: storyId, title: s.title || '', content: s.text || '' });
              }
              if (typeof window.setExpandedStoryId === 'function') window.setExpandedStoryId(storyId);
              _refreshLists();
            })
            .catch(function() { _refreshLists(); });
          return;
        }
        _refreshLists();
      },
    });
    return _controller;
  }

  function _setCreatingState(btn, value) {
    _isCreating = !!value;
    if (btn) btn.disabled = _isCreating;
  }

  function initGenerateButton() {
    var btn = document.getElementById('btn-story-generate');
    if (!btn) return;
    var status = _controllerOrNull();
    if (status) status.setDefaultHint(_DEFAULT_HINT);
    else _fallbackMemo(_DEFAULT_HINT);

    btn.addEventListener('click', function() {
      if (_isCreating) return;
      var countInput = document.getElementById('story-generate-count');
      var count = Math.max(1, Math.min(50, parseInt((countInput && countInput.value) || '1') || 1));
      var modelIdEl = document.getElementById('screenwriter-model-id');
      var modelId = modelIdEl ? (modelIdEl.value || '') : '';
      _setCreatingState(btn, true);
      if (status) status.beginCreation(count > 1 ? ('Создаю ' + count + ' батчей…') : 'Запускаю генерацию…');
      else _fallbackMemo(count > 1 ? ('Создаю ' + count + ' батчей…') : 'Запускаю генерацию…');
      var body = modelId ? JSON.stringify({ model_id: modelId }) : null;
      var remaining = count;
      var createdBatches = [];
      var hadRequestError = false;

      function finishRequest() {
        remaining--;
        if (remaining > 0) return;

        _setCreatingState(btn, false);
        if (status) status.endCreation();
        else _fallbackMemo(_DEFAULT_HINT);

        if (createdBatches.length === 0) {
          if (status) {
            status.showTemporaryHint(hadRequestError ? 'Ошибка запроса' : 'Не удалось создать батчи', 3500);
          } else {
            _fallbackMemo(hadRequestError ? 'Ошибка запроса' : 'Не удалось создать батчи');
            setTimeout(function() { _fallbackMemo(_DEFAULT_HINT); }, 3500);
          }
          return;
        }

        createdBatches.sort(function(a, b) { return a.requestIndex - b.requestIndex; });
        var batchIds = createdBatches.map(function(item) { return item.id; });

        if (status) {
          status.addLine('Создано батчей: ' + batchIds.length);
          if (hadRequestError) status.addLine('Часть запросов завершилась ошибкой');
          status.trackBatches(batchIds);
        }
      }

      for (var i = 0; i < count; i++) {
        (function(requestIndex) {
          fetch('/production/story/generate', {
            method: 'POST',
            headers: body ? { 'Content-Type': 'application/json' } : {},
            body: body,
          })
            .then(function(r) {
              return r.json()
                .then(function(d) { return { ok: r.ok, data: d || {} }; })
                .catch(function() { return { ok: r.ok, data: {} }; });
            })
            .then(function(d) {
              if (d.ok && d.data && d.data.batch_id) {
                createdBatches.push({
                  id: String(d.data.batch_id),
                  requestIndex: requestIndex,
                });
              } else hadRequestError = true;
              finishRequest();
            })
            .catch(function() {
              hadRequestError = true;
              finishRequest();
            });
        })(i);
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
      window.clipboardWrite(text, function() {
        btn.classList.add('copied');
        setTimeout(function() { btn.classList.remove('copied'); }, 2000);
      });
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initExportStoriesBtn);
  } else {
    initExportStoriesBtn();
  }
})();

/* ── Клиентский фильтр сюжетов ── */
(function() {
  function initStoryClientFilter() {
    var input = document.getElementById('story-search-input');
    if (!input) return;
    var clearBtn = document.getElementById('story-search-clear');

    var _debounce = null;

    function _updateClearVisibility() {
      if (clearBtn) clearBtn.hidden = !input.value;
    }
    _updateClearVisibility();

    if (clearBtn) {
      clearBtn.addEventListener('click', function() {
        input.value = '';
        _updateClearVisibility();
        clearTimeout(_debounce);
        input.focus();
        if (typeof window.loadStoryList === 'function') window.loadStoryList();
      });
    }

    function _doSearch() {
      var q = input.value.trim().toLowerCase();
      if (!q) {
        window.loadStoryList();
        return;
      }
      var filterQs = (window._getStoryFilterParams ? window._getStoryFilterParams() : '');
      var filterParams = new URLSearchParams(filterQs);
      filterParams.delete('pin_id');
      var filterIdsUrl = '/production/stories/filter-ids?' + filterParams.toString();
      Promise.all([
        fetch('/production/stories?show_used=1&show_bad=1').then(function(r) { return r.ok ? r.json() : []; }),
        fetch(filterIdsUrl).then(function(r) { return r.ok ? r.json() : { ids: [] }; }),
      ]).then(function(results) {
        var allStories = results[0];
        var filterIds = new Set(results[1].ids || []);
        var words = q.split(/\s+/).filter(Boolean);
        var matched = allStories.filter(function(s) {
          var t = (s.title || '').toLowerCase();
          return words.every(function(w) { return t.indexOf(w) !== -1; });
        });
        matched.forEach(function(s) { s._dim = !filterIds.has(s.id); });
        if (typeof window._renderStories === 'function') window._renderStories(matched);
      }).catch(function() {
        window.loadStoryList();
      });
    }

    input.addEventListener('input', function() {
      _updateClearVisibility();
      clearTimeout(_debounce);
      _debounce = setTimeout(_doSearch, 400);
    });

    var _origLoad = window.loadStoryList;
    window.loadStoryList = function() {
      if (input.value.trim()) {
        _doSearch();
      } else if (typeof _origLoad === 'function') {
        _origLoad();
      }
    };
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initStoryClientFilter);
  } else {
    initStoryClientFilter();
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
        _models = (models || []).filter(function(m) { return m.active; });
        buildList();
        var cntEl = document.getElementById('screenwriter-model-count');
        if (cntEl) cntEl.textContent = 'Записей: ' + _models.length;
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
