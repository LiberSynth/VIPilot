(function() {
  var _expandedMovieId = null;
  var _forceNoAutoplay = false;
  var _generationStatus = null;

  var _FINAL_STATUSES = [
    'published', 'published_partially', 'ready',
    'cancelled', 'error', 'fatal_error',
    'video_error', 'transcode_error', 'publish_error', 'donated',
  ];
  var _HINT_DEFAULT = 'Вы можете сгенерировать контент при помощи AI-модели.';

  function _statusController() {
    if (_generationStatus) return _generationStatus;
    if (typeof GenerationConsoleController !== 'function') return null;
    _generationStatus = new GenerationConsoleController({
      consoleId: 'director-generate-console',
      defaultHint: _HINT_DEFAULT,
      maxLines: 5,
      pollIntervalMs: 2500,
      finalStatuses: _FINAL_STATUSES,
      onBatchFinal: function(_batchId, data, meta) {
        if (typeof window.loadMovieList === 'function') {
          window.loadMovieList(function() {
            if (meta && meta.selectMovieOnReady && data && data.has_video_data && data.movie_id) {
              var panel = document.getElementById('panel-director');
              var panelHidden = !panel || !panel.classList.contains('active');
              selectMovie(data.movie_id, panelHidden);
            }
          });
        }
      },
    });
    return _generationStatus;
  }

  /* ── фильтры ── */
  function getFilterParams() {
    var forApproval   = document.getElementById('movie-filter-for-approval');
    var onlyGood      = document.getElementById('movie-filter-only-good');
    var showPublished = document.getElementById('movie-filter-show-published');
    var params = new URLSearchParams();
    if (forApproval && forApproval.checked) {
      params.set('for_approval', '1');
    } else {
      params.set('show_published', (showPublished && showPublished.checked) ? '1' : '0');
      params.set('show_bad', (onlyGood && onlyGood.checked) ? '0' : '1');
    }
    if (_expandedMovieId) params.set('pin_id', _expandedMovieId);
    return params.toString();
  }

  window._getMovieFilterParams = getFilterParams;

  /* ── плеер ── */
  function loadMovieInPlayer(movieId, forceNoAutoplay) {
    var wrap = document.getElementById('director-video-wrap');
    if (!wrap) return;
    if (!movieId) {
      wrap.innerHTML = '<video class="movie-video" controls></video>';
      return;
    }
    var src = '/production/movie/' + encodeURIComponent(movieId) + '/video';
    var autoplayChk = document.getElementById('director-autoplay-check');
    var autoplayAttr = (!forceNoAutoplay && autoplayChk && autoplayChk.checked) ? ' autoplay' : '';
    wrap.innerHTML = '<video class="movie-video" controls' + autoplayAttr + ' src="' + src + '"></video>';
  }

  /* ── кнопки роликов ── */
  function _renderPublishedIcon(m) {
    if (!m.published) return '';
    return '<span class="story-icon story-icon-used" title="Опубликовано">'
      + '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">'
      + '<polyline points="2,8 6,12 14,4"/></svg></span>';
  }

  function _renderGoToStoryBtn(m) {
    var disabled = !m.story_id;
    var attrs = 'class="story-icon movie-goto-story-btn"'
      + ' title="К сюжету"'
      + (disabled ? ' disabled' : ' data-story-id="' + AccordionList.escapeHtml(m.story_id) + '"');
    return '<button type="button" ' + attrs + '>'
      + '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">'
      + '<polyline points="10,3 4,8 10,13"/>'
      + '</svg></button>';
  }

  function _renderInfoBtn(m) {
    return '<button class="story-icon story-info-btn" data-copy="movie_id: ' + m.id + '" title="Инфо">'
      + '<svg viewBox="0 0 16 16" fill="none" stroke="#8888b0" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">'
      + '<circle cx="8" cy="8" r="6.5"/>'
      + '<line x1="8" y1="7.5" x2="8" y2="11"/>'
      + '<circle cx="8" cy="5" r="1.1" fill="#8888b0" stroke="none"/>'
      + '</svg></button>';
  }

  function _renderMovieDeleteBtn(m) {
    return '<button class="story-icon movie-delete-btn" data-id="' + m.id
      + '" data-title="' + AccordionList.escapeHtml(m.story_title || '(без названия)') + '" title="Удалить">'
      + '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">'
      + '<polyline points="2,4 14,4"/><path d="M6 4V2h4v2"/><rect x="3" y="4" width="10" height="10" rx="1.5"/><line x1="6" y1="7" x2="6" y2="11"/><line x1="10" y1="7" x2="10" y2="11"/>'
      + '</svg></button>';
  }

  /* ── AccordionList ── */
  var _accordionList = new AccordionList({
    listId:   'movies-list',
    cardId:   'director-player-card',
    holderId: 'director-player-holder',
    countId:  'movies-count',
    gradeUrl: function(id) { return '/production/movie/' + id + '/grade'; },
    renderTitle: function(item) {
      var modelLabel = item.model_name
        ? ' <span class="story-model-name">(' + AccordionList.escapeHtml(item.model_name) + ')</span>'
        : '';
      var titleHtml = AccordionList.escapeHtml(item.story_title || '(без названия)');
      if (item._dim) titleHtml = '<span class="story-title-dim">' + titleHtml + '</span>';
      return titleHtml + modelLabel;
    },
    renderButtons: function(item) {
      return _renderPublishedIcon(item) + _renderGoToStoryBtn(item) + _renderInfoBtn(item) + _renderMovieDeleteBtn(item);
    },
    onExpand: function(item) {
      _expandedMovieId = item ? String(item.id) : null;
      loadMovieInPlayer(_expandedMovieId, _forceNoAutoplay);
      _forceNoAutoplay = false;
      if (item && item.active_batch_id) {
        var status = _statusController();
        if (status) {
          status.trackBatch(item.active_batch_id, { selectMovieOnReady: true });
        }
      }
    },
    onCollapse: function() {
      _expandedMovieId = null;
      var wrap = document.getElementById('director-video-wrap');
      if (wrap) { var vid = wrap.querySelector('video'); if (vid) vid.pause(); }
    },
    canAddNew: false,
    emptyHtml: '<div class="stories-empty">Нет видео</div>',
  });

  function _bindMovieListButtons(container) {
    container.querySelectorAll('.story-info-btn').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        var text = btn.getAttribute('data-copy') || '';
        window.clipboardWrite(text, function() {
          btn.classList.add('copied');
          setTimeout(function() { btn.classList.remove('copied'); }, 2000);
        });
      });
    });
    container.querySelectorAll('.movie-goto-story-btn').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        if (btn.disabled) return;
        var storyId = btn.getAttribute('data-story-id');
        if (!storyId) return;
        if (typeof window.openStoryInScreenwriter === 'function') {
          window.openStoryInScreenwriter(storyId);
        }
      });
    });
    container.querySelectorAll('.movie-delete-btn').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        var movieId = btn.getAttribute('data-id');
        var movieTitle = btn.getAttribute('data-title') || '(без названия)';
        _openDeleteMovieDialog(movieId, movieTitle, btn);
      });
    });
  }

  function renderMovies(movies) {
    var container = document.getElementById('movies-list');
    if (!container) return;
    _accordionList.render(movies || []);
    if (movies && movies.length) _bindMovieListButtons(container);
  }

  window._renderMovies = renderMovies;

  function selectMovie(movieId, forceNoAutoplay) {
    _forceNoAutoplay = !!forceNoAutoplay;
    _accordionList.selectRow(movieId || null);
  }

  function _openDeleteMovieDialog(movieId, movieTitle, triggerBtn) {
    new ConfirmDialog({
      title:        'Удалить видео?',
      text:         'Видео «' + AccordionList.escapeHtml(movieTitle) + '» и все связанные батчи, лог и записи лога будут удалены безвозвратно. Сюжет останется нетронутым.',
      confirmLabel: 'Удалить',
      triggerBtn:   triggerBtn,
      onConfirm: function(btn, dlg) {
        btn.disabled    = true;
        btn.textContent = 'Удаление…';
        fetch('/production/movie/' + encodeURIComponent(movieId) + '/delete', { method: 'DELETE' })
          .then(function(r) { return r.ok ? r.json() : r.json().then(function(d) { throw d; }); })
          .then(function() {
            dlg.close();
            if (typeof window.showToast === 'function') window.showToast('Видео удалено');
            if (String(movieId) === String(_expandedMovieId)) {
              selectMovie(null);
            }
            window.loadMovieList();
          })
          .catch(function(d) {
            dlg.close();
            var msg = (d && d.error) ? d.error : 'Ошибка удаления';
            if (typeof window.showToast === 'function') window.showToast(msg);
          });
      },
    }).open();
  }

  /* ── загрузка списка ── */
  window.loadMovieList = function(callback) {
    var container = document.getElementById('movies-list');
    if (!container) return;
    var hasContent = container.querySelector('.story-row');
    if (!hasContent) container.innerHTML = '<div class="stories-loading">Загрузка...</div>';
    fetch('/production/movies?' + getFilterParams())
      .then(function(r) { return r.json(); })
      .then(function(data) {
        renderMovies(data);
        if (typeof callback === 'function') callback();
      })
      .catch(function() {
        container.innerHTML = '<div class="stories-empty">Ошибка загрузки</div>';
      });
  };

  /* ── фильтры ── */
  function initFilters() {
    var forApproval   = document.getElementById('movie-filter-for-approval');
    var onlyGood      = document.getElementById('movie-filter-only-good');
    var showPublished = document.getElementById('movie-filter-show-published');

    function saveFilter(key, checkbox) {
      fetch('/production/env', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key: key, value: checkbox.checked ? '1' : '0' }),
      });
    }

    function onFilterChange(changed) {
      if (changed === forApproval && forApproval && forApproval.checked) {
        if (onlyGood)      { onlyGood.checked      = false; saveFilter('director_filter_only_good', onlyGood); }
        if (showPublished) { showPublished.checked  = false; saveFilter('director_filter_show_published', showPublished); }
      } else if (changed !== forApproval && forApproval) {
        forApproval.checked = false;
        saveFilter('director_filter_for_approval', forApproval);
      }
      if (changed === forApproval)   saveFilter('director_filter_for_approval', forApproval);
      if (changed === onlyGood)      saveFilter('director_filter_only_good', onlyGood);
      if (changed === showPublished)  saveFilter('director_filter_show_published', showPublished);
      window.loadMovieList();
    }

    if (forApproval)   forApproval.addEventListener('change',   function() { onFilterChange(forApproval); });
    if (onlyGood)      onlyGood.addEventListener('change',      function() { onFilterChange(onlyGood); });
    if (showPublished) showPublished.addEventListener('change', function() { onFilterChange(showPublished); });
  }

  function _openDeleteBadMoviesDialog(btn) {
    new ConfirmDialog({
      title:        'Удалить неудачные видео?',
      text:         'Будут удалены все видео с оценкой «плохо»,<br>а также связанные с ними батчи и записи лога.<br>Сюжеты затронуты не будут.',
      confirmLabel: 'Удалить',
      triggerBtn:   btn,
      onConfirm: function(confirmBtn, dlg) {
        dlg.close();
        btn.classList.add('pending');
        var deleteDlg = new DeleteMoviesDialog({ triggerBtn: btn });
        deleteDlg.open();
        fetch('/production/movies/delete_bad', { method: 'POST' })
          .then(function(r) { return r.ok ? r.json() : null; })
          .then(function(d) {
            btn.classList.remove('pending');
            if (d && d.ok) {
              var n = d.deleted ? (d.deleted.movies || 0) : 0;
              deleteDlg.finish(n);
              if (typeof window.loadMovieList === 'function') window.loadMovieList();
            } else {
              deleteDlg.error();
            }
          })
          .catch(function() {
            btn.classList.remove('pending');
            deleteDlg.error();
          });
      },
    }).open();
  }

  function initDeleteBadMoviesButton() {
    var btn = document.getElementById('btn-delete-bad-movies');
    if (!btn) return;
    btn.addEventListener('click', function() {
      if (btn.classList.contains('pending')) return;
      _openDeleteBadMoviesDialog(btn);
    });
  }

  function initImportMovieButton() {
    var btn   = document.getElementById('btn-import-movie');
    var input = document.getElementById('input-import-movie');
    if (!btn || !input) return;
    btn.addEventListener('click', function() {
      if (btn.disabled) return;
      input.value = '';
      input.click();
    });
    input.addEventListener('change', function() {
      var file = input.files && input.files[0];
      if (!file) return;
      var fd = new FormData();
      fd.append('file', file);
      btn.disabled = true;
      btn.classList.add('pending');
      fetch('/production/movies/upload', { method: 'POST', body: fd })
        .then(function(r) { return r.json().then(function(d) { return { ok: r.ok, data: d }; }); })
        .then(function(res) {
          btn.disabled = false;
          btn.classList.remove('pending');
          if (res.ok && res.data && res.data.ok) {
            if (typeof window.showToast === 'function') window.showToast('Ролик загружен');
            if (typeof window.loadMovieList === 'function') window.loadMovieList();
          } else {
            var msg = (res.data && res.data.error) ? res.data.error : 'Ошибка загрузки';
            if (typeof window.showToast === 'function') window.showToast(msg);
          }
        })
        .catch(function() {
          btn.disabled = false;
          btn.classList.remove('pending');
          if (typeof window.showToast === 'function') window.showToast('Ошибка загрузки файла');
        });
    });
  }

  function initAutoplayToggle() {
    var chk = document.getElementById('director-autoplay-check');
    if (!chk) return;
    chk.addEventListener('change', function() {
      var fd = new FormData();
      fd.append('producer_autoplay_movie', chk.checked ? '1' : '0');
      fetch('/save', { method: 'POST', body: fd });
    });
  }

  function updateVideoWrapHeight() {
    var wrap = document.getElementById('director-video-wrap');
    if (!wrap) return;
    var headerEl = document.querySelector('.header-top');
    var headerH  = headerEl ? headerEl.offsetHeight : 0;
    var availableH = window.innerHeight - headerH;
    wrap.style.height = Math.round(availableH / 1.618) + 'px';
  }

  function initDirector() {
    _statusController();
    initFilters();
    initDeleteBadMoviesButton();
    initImportMovieButton();
    initAutoplayToggle();
    updateVideoWrapHeight();
    window.addEventListener('resize', updateVideoWrapHeight);
  }

  window._directorApi = {
    startBatchPoll: function(batchId, movieId) {
      var status = _statusController();
      if (!status) return;
      status.trackBatch(batchId, { selectMovieOnReady: !!movieId });
    },
    beginGenerationCreation: function(hintText) {
      var status = _statusController();
      if (status) status.beginCreation(hintText);
    },
    endGenerationCreation: function() {
      var status = _statusController();
      if (status) status.endCreation();
    },
    appendGenerationLine: function(text) {
      var status = _statusController();
      if (status) status.addLine(text);
    },
    showGenerationHint: function(text, ttlMs) {
      var status = _statusController();
      if (status) status.showTemporaryHint(text, ttlMs);
    },
    trackGenerationBatches: function(batchIds) {
      var status = _statusController();
      if (status) status.trackBatches(batchIds);
    },
    getSelectedMovieId: function() { return _expandedMovieId; },
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initDirector);
  } else {
    initDirector();
  }
})();

/* ── Клиентский фильтр роликов ── */
(function() {
  function initMovieClientFilter() {
    var input = document.getElementById('movie-search-input');
    if (!input) return;
    var clearBtn = document.getElementById('movie-search-clear');

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
        if (typeof window.loadMovieList === 'function') window.loadMovieList();
      });
    }

    function _doSearch() {
      var q = input.value.trim().toLowerCase();
      if (!q) {
        window.loadMovieList();
        return;
      }
      var filterQs = (window._getMovieFilterParams ? window._getMovieFilterParams() : '');
      var filterParams = new URLSearchParams(filterQs);
      filterParams.delete('pin_id');
      var filterIdsUrl = '/production/movies/filter-ids?' + filterParams.toString();
      Promise.all([
        fetch('/production/movies?show_published=1&show_bad=1').then(function(r) { return r.ok ? r.json() : []; }),
        fetch(filterIdsUrl).then(function(r) { return r.ok ? r.json() : { ids: [] }; }),
      ]).then(function(results) {
        var allMovies = results[0];
        var filterIds = new Set(results[1].ids || []);
        var words = q.split(/\s+/).filter(Boolean);
        var matched = allMovies.filter(function(m) {
          var t = (m.story_title || '').toLowerCase();
          return words.every(function(w) { return t.indexOf(w) !== -1; });
        });
        matched.forEach(function(m) { m._dim = !filterIds.has(m.id); });
        if (typeof window._renderMovies === 'function') window._renderMovies(matched);
      }).catch(function() {
        window.loadMovieList();
      });
    }

    input.addEventListener('input', function() {
      _updateClearVisibility();
      clearTimeout(_debounce);
      _debounce = setTimeout(_doSearch, 400);
    });

    var _origLoad = window.loadMovieList;
    window.loadMovieList = function(callback) {
      if (input.value.trim()) {
        _doSearch();
        if (typeof callback === 'function') callback();
      } else if (typeof _origLoad === 'function') {
        _origLoad(callback);
      }
    };
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initMovieClientFilter);
  } else {
    initMovieClientFilter();
  }
})();
