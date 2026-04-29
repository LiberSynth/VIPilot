(function() {
  var _expandedMovieId = null;
  var _pollTimer       = null;
  var _pollVersion     = 0;
  var _forceNoAutoplay = false;

  var _FINAL_STATUSES = [
    'published', 'published_partially', 'movie_probe', 'story_probe',
    'cancelled', 'error', 'fatal_error',
    'video_error', 'transcode_error', 'publish_error', 'donated',
  ];
  var _HINT_DEFAULT = 'Вы можете сгенерировать ролик при помощи AI-модели.';

  function _setHint(text) {
    var el = document.getElementById('director-generate-hint');
    if (el) el.textContent = text;
  }

  function _stopActiveBatchPoll() {
    if (_pollTimer !== null) {
      clearTimeout(_pollTimer);
      _pollTimer = null;
    }
    _pollVersion++;
  }

  function _startActiveBatchPoll(batchId, movieId) {
    _stopActiveBatchPoll();
    var myVersion = _pollVersion;
    function poll() {
      fetch('/api/batch/' + encodeURIComponent(batchId) + '/logs')
        .then(function(r) { return r.json(); })
        .then(function(data) {
          if (_pollVersion !== myVersion) return;
          if (!data || data.error) {
            _pollTimer = setTimeout(poll, 2500);
            return;
          }
          var logs = data.logs || [];
          var lastLog = logs.length ? logs[logs.length - 1] : null;
          var lastEntry = lastLog && lastLog.entries && lastLog.entries.length
            ? lastLog.entries[lastLog.entries.length - 1]
            : null;
          if (lastEntry && lastEntry.message) {
            _setHint(lastEntry.message);
          }
          var status = data.batch_status || '';
          if (_FINAL_STATUSES.indexOf(status) !== -1) {
            _stopActiveBatchPoll();
            if (data.has_video_data && data.movie_id) {
              if (typeof window.loadMovieList === 'function') {
                window.loadMovieList(function() {
                  var panel = document.getElementById('panel-director');
                  var panelHidden = !panel || !panel.classList.contains('active');
                  selectMovie(data.movie_id, panelHidden);
                });
              }
            } else {
              if (typeof window.loadMovieList === 'function') window.loadMovieList();
              _setHint('Генерация завершилась без видео');
              var resetVersion = _pollVersion;
              setTimeout(function() {
                if (_pollVersion === resetVersion) {
                  _setHint(_HINT_DEFAULT);
                }
              }, 3000);
            }
            return;
          }
          _pollTimer = setTimeout(poll, 2500);
        })
        .catch(function() {
          if (_pollVersion !== myVersion) return;
          _pollTimer = setTimeout(poll, 2500);
        });
    }
    poll();
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

  /* ── плеер ── */
  function loadMovieInPlayer(movieId, forceNoAutoplay) {
    var wrap = document.getElementById('director-video-wrap');
    if (!wrap) return;
    if (!movieId) {
      wrap.innerHTML = '<video class="probe-video" controls></video>';
      return;
    }
    var src = '/production/movie/' + encodeURIComponent(movieId) + '/video';
    var autoplayChk = document.getElementById('director-autoplay-check');
    var autoplayAttr = (!forceNoAutoplay && autoplayChk && autoplayChk.checked) ? ' autoplay' : '';
    wrap.innerHTML = '<video class="probe-video" controls' + autoplayAttr + ' src="' + src + '"></video>';
  }

  /* ── кнопки роликов ── */
  function _renderPublishedIcon(m) {
    if (!m.published) return '';
    return '<span class="story-icon story-icon-used" title="Опубликовано">'
      + '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">'
      + '<polyline points="2,8 6,12 14,4"/></svg></span>';
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
      return AccordionList.escapeHtml(item.story_title || '(без названия)') + modelLabel;
    },
    renderButtons: function(item) {
      return _renderPublishedIcon(item) + _renderInfoBtn(item) + _renderMovieDeleteBtn(item);
    },
    onExpand: function(item) {
      _stopActiveBatchPoll();
      _setHint(_HINT_DEFAULT);
      _expandedMovieId = item ? String(item.id) : null;
      loadMovieInPlayer(_expandedMovieId, _forceNoAutoplay);
      _forceNoAutoplay = false;
      if (item && item.active_batch_id) {
        _startActiveBatchPoll(item.active_batch_id, _expandedMovieId);
      }
    },
    onCollapse: function() {
      _expandedMovieId = null;
      _stopActiveBatchPoll();
      _setHint(_HINT_DEFAULT);
    },
    canAddNew: false,
    emptyHtml: '<div class="stories-empty">Нет видео</div>',
  });

  function _bindMovieListButtons(container) {
    container.querySelectorAll('.story-info-btn').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        var text = btn.getAttribute('data-copy') || '';
        navigator.clipboard.writeText(text).then(function() {
          btn.classList.add('copied');
          setTimeout(function() { btn.classList.remove('copied'); }, 2000);
        }).catch(function() {});
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

    function onFilterChange(changed) {
      if (changed === forApproval && forApproval && forApproval.checked) {
        if (onlyGood)      onlyGood.checked      = false;
        if (showPublished) showPublished.checked  = false;
      } else if (changed !== forApproval && forApproval) {
        forApproval.checked = false;
      }
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

  function _sanitizeFilename(name) {
    return String(name || '').replace(/[\\/:*?"<>|]/g, '_').replace(/\s+/g, ' ').trim();
  }

  function _fmtCreatedAt(isoStr) {
    if (!isoStr) return 'unknown';
    var d    = new Date(isoStr);
    var yyyy = d.getUTCFullYear();
    var mo   = String(d.getUTCMonth() + 1).padStart(2, '0');
    var dd   = String(d.getUTCDate()).padStart(2, '0');
    var hh   = String(d.getUTCHours()).padStart(2, '0');
    var nn   = String(d.getUTCMinutes()).padStart(2, '0');
    var ss   = String(d.getUTCSeconds()).padStart(2, '0');
    var zzz  = String(d.getUTCMilliseconds()).padStart(3, '0');
    return yyyy + '-' + mo + '-' + dd + ' ' + hh + '-' + nn + '-' + ss + '.' + zzz;
  }

  function _buildExportFilename(meta) {
    var dateStr  = _fmtCreatedAt(meta.created_at);
    var story    = (meta.story_title || '').trim();
    var b        = story || String(meta.id);
    var gradeRaw = (meta.grade !== null && meta.grade !== undefined) ? String(meta.grade) : null;
    var grade    = gradeRaw ? (AccordionList.GRADE_LABELS[gradeRaw] || '') : '';
    var parts    = [dateStr, b];
    if (grade) parts.push(grade);
    return _sanitizeFilename(parts.join(' - ')) + '.mp4';
  }

  async function _exportMovies(triggerBtn) {
    var metaList;
    try {
      var r = await fetch('/production/movies/good_meta?' + getFilterParams());
      if (!r.ok) throw new Error('http ' + r.status);
      metaList = await r.json();
    } catch (e) {
      if (typeof window.showToast === 'function') window.showToast('Ошибка получения списка роликов');
      return;
    }

    triggerBtn.disabled = true;
    var dlg = new ExportMoviesDialog({ total: metaList.length });
    dlg.open();

    var done = 0;
    var failedItems = [];
    for (var i = 0; i < metaList.length; i++) {
      if (dlg.isCancelled()) break;
      var meta = metaList[i];
      var filename = _buildExportFilename(meta);
      dlg.setCurrentFile(filename);
      try {
        var resp = await fetch('/production/movies/' + encodeURIComponent(meta.id) + '/download');
        if (!resp.ok) throw new Error('http ' + resp.status);
        var blob = await resp.blob();
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
        var reason = 'ошибка скачивания';
        if (e && e.message && e.message.startsWith('http ')) {
          var code = parseInt(e.message.slice(5), 10);
          if (code === 404 || code === 410) reason = 'данные очищены после публикации';
          else if (Number.isFinite(code)) reason = 'ошибка HTTP ' + code;
        }
        failedItems.push({ filename: filename, reason: reason });
      }
    }

    triggerBtn.disabled = false;
    dlg.finish(done, dlg.isCancelled(), failedItems);
  }

  function initExportGoodMoviesButton() {
    var btn = document.getElementById('btn-export-good-movies');
    if (!btn) return;
    btn.addEventListener('click', function() {
      if (btn.disabled) return;
      _exportMovies(btn);
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
    initFilters();
    initDeleteBadMoviesButton();
    initImportMovieButton();
    initExportGoodMoviesButton();
    initAutoplayToggle();
    updateVideoWrapHeight();
    window.addEventListener('resize', updateVideoWrapHeight);
  }

  window._directorApi = {
    startBatchPoll:     _startActiveBatchPoll,
    getSelectedMovieId: function() { return _expandedMovieId; },
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initDirector);
  } else {
    initDirector();
  }
})();
