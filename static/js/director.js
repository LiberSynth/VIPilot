(function() {
  var GRADE_LABELS      = { 'null': 'не указано', 'good': 'хорошо', 'bad': 'плохо' };
  var GRADE_COLORS      = { 'null': 'rgba(255,255,255,.07)', 'good': 'rgba(80,200,120,.25)', 'bad': 'rgba(200,80,80,.25)' };
  var GRADE_TEXT_COLORS = { 'null': '#aaa', 'good': '#6ee7a0', 'bad': '#f87171' };
  var GRADE_CYCLE       = ['good', 'bad', null];


  var _selectedMovieId = null;
  var _moviesData      = [];
  var _pollTimer       = null;
  var _pollVersion     = 0;

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
            if (typeof window.loadMovieList === 'function') window.loadMovieList();
            if (data.has_video_data) {
              loadMovieInPlayer(movieId);
            } else {
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

  function gradeKey(g) { return (g === null || g === undefined) ? 'null' : String(g); }

  function escHtml(s) {
    return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  /* ── счётчик ── */
  function updateCount(n) {
    var el = document.getElementById('movies-count');
    if (!el) return;
    if (n === null || n === undefined) { el.textContent = ''; return; }
    var words = ['запись', 'записи', 'записей'];
    var mod = n % 100;
    var w = (mod >= 11 && mod <= 19) ? words[2]
          : (n % 10 === 1) ? words[0]
          : (n % 10 >= 2 && n % 10 <= 4) ? words[1]
          : words[2];
    el.textContent = n + ' ' + w;
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
    return params.toString();
  }

  /* ── плашка grade в карточке ВИДЕО ── */
  function setCardMovieGradeBadge(grade, hidden) {
    var btn = document.getElementById('card-movie-grade');
    if (!btn) return;
    if (hidden) { btn.hidden = true; btn.disabled = true; return; }
    var gk = gradeKey(grade);
    btn.hidden   = false;
    btn.disabled = false;
    btn.setAttribute('data-grade', gk);
    btn.style.background = gk !== 'null' ? (GRADE_COLORS[gk] || '') : '';
    btn.style.color      = gk !== 'null' ? (GRADE_TEXT_COLORS[gk] || '') : '';
    btn.textContent = GRADE_LABELS[gk] || gk;
    btn.title = 'Оценка: ' + (GRADE_LABELS[gk] || gk) + '. Нажмите для смены';
  }

  function initCardMovieGradeBadge() {
    var btn = document.getElementById('card-movie-grade');
    if (!btn) return;
    btn.addEventListener('click', function() {
      if (btn.disabled || !_selectedMovieId) return;
      var currentAttr = btn.getAttribute('data-grade');
      var current = currentAttr === 'null' ? null : currentAttr;
      var idx = GRADE_CYCLE.indexOf(current);
      var next = GRADE_CYCLE[(idx + 1) % GRADE_CYCLE.length];
      btn.disabled = true;
      fetch('/production/movie/' + _selectedMovieId + '/grade', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ grade: next }),
      })
      .then(function(r) { return r.ok ? r.json() : null; })
      .then(function(d) {
        btn.disabled = false;
        if (d && d.ok) {
          var g = d.grade !== undefined ? d.grade : null;
          setCardMovieGradeBadge(g, false);
          _syncListRowGrade(_selectedMovieId, g);
          window.loadMovieList();
        }
      })
      .catch(function() { btn.disabled = false; });
    });
  }

  /* синхронизирует плашку в строке списка без перезагрузки */
  function _syncListRowGrade(movieId, grade) {
    var container = document.getElementById('movies-list');
    if (!container) return;
    var btn = container.querySelector('.story-grade-badge[data-id="' + movieId + '"]');
    if (!btn) return;
    var gk = gradeKey(grade);
    btn.setAttribute('data-grade', gk);
    btn.style.background = gk !== 'null' ? (GRADE_COLORS[gk] || '') : '';
    btn.style.color      = gk !== 'null' ? (GRADE_TEXT_COLORS[gk] || '') : '';
    btn.textContent = GRADE_LABELS[gk] || gk;
    btn.title = 'Оценка: ' + (GRADE_LABELS[gk] || gk) + '. Нажмите для смены';
  }

  /* ── выделение строки в списке ── */
  function _updateMovieSelection() {
    var container = document.getElementById('movies-list');
    if (!container) return;
    container.querySelectorAll('.story-row--selected').forEach(function(r) {
      r.classList.remove('story-row--selected');
    });
    if (_selectedMovieId) {
      var sel = container.querySelector('.story-row[data-id="' + _selectedMovieId + '"]');
      if (sel) sel.classList.add('story-row--selected');
    }
  }

  /* ── плеер: высота рамки ── */
  function updateVideoWrapHeight() {
    var wrap = document.getElementById('director-video-wrap');
    if (!wrap) return;
    var header = document.querySelector('header');
    var headerBottom = header ? header.getBoundingClientRect().bottom : 0;
    var cardPad = 15;
    var avail = window.innerHeight - headerBottom - cardPad * 2;
    wrap.style.height = Math.round(avail / 1.618) + 'px';
  }

  /* ── плеер ── */
  function loadMovieInPlayer(movieId) {
    var wrap    = document.getElementById('director-video-wrap');
    var titleEl = document.getElementById('director-movie-title');
    if (!wrap) return;
    if (!movieId) {
      wrap.innerHTML = '<video class="probe-video" controls></video>';
      if (titleEl) { titleEl.textContent = ''; titleEl.style.display = 'none'; }
      setCardMovieGradeBadge(null, true);
      updateVideoWrapHeight();
      return;
    }
    var rec = _moviesData.filter(function(m) { return String(m.id) === String(movieId); })[0];
    if (titleEl) {
      var title = (rec && rec.story_title) ? rec.story_title : '';
      var model = (rec && rec.model_name)  ? rec.model_name  : '';
      var modelLabel = model ? ' <span class="story-model-name">(' + escHtml(model) + ')</span>' : '';
      titleEl.innerHTML = escHtml(title) + modelLabel;
      titleEl.style.display = (title || model) ? '' : 'none';
    }
    var src = '/production/movie/' + encodeURIComponent(movieId) + '/video';
    var autoplayChk = document.getElementById('director-autoplay-check');
    var autoplayAttr = (autoplayChk && autoplayChk.checked) ? ' autoplay' : '';
    wrap.innerHTML = '<video class="probe-video" controls' + autoplayAttr + ' src="' + src + '"></video>';
    setCardMovieGradeBadge(rec ? rec.grade : null, false);
    updateVideoWrapHeight();
  }

  function selectMovie(movieId) {
    _stopActiveBatchPoll();
    _setHint(_HINT_DEFAULT);
    _selectedMovieId = movieId || null;
    _updateMovieSelection();
    loadMovieInPlayer(_selectedMovieId);
    if (_selectedMovieId) {
      var rec = _moviesData.filter(function(m) { return String(m.id) === String(_selectedMovieId); })[0];
      if (rec && rec.active_batch_id) {
        _startActiveBatchPoll(rec.active_batch_id, _selectedMovieId);
      }
    }
  }

  /* ── рендер списка ── */
  function renderMovies(movies) {
    _moviesData = movies || [];
    if (_selectedMovieId) {
      var stillExists = _moviesData.some(function(m) { return String(m.id) === String(_selectedMovieId); });
      if (!stillExists) selectMovie(null);
    }
    var container = document.getElementById('movies-list');
    if (!container) return;
    if (!_moviesData.length) {
      updateCount(0);
      container.innerHTML = '<div class="stories-empty">Нет видео</div>';
      return;
    }
    updateCount(_moviesData.length);
    var html = '';
    for (var i = 0; i < _moviesData.length; i++) {
      var m = _moviesData[i];
      var grade = m.grade !== undefined ? m.grade : null;
      var gk = gradeKey(grade);
      var label = GRADE_LABELS[gk] || gk;
      var inlineStyle = gk !== 'null'
        ? 'style="background:' + (GRADE_COLORS[gk] || '') + ';color:' + (GRADE_TEXT_COLORS[gk] || '') + '" '
        : '';
      var modelLabel = m.model_name ? ' <span class="story-model-name">(' + escHtml(m.model_name) + ')</span>' : '';
      var gradeBadge = '<button class="story-grade-badge" data-id="' + m.id + '" data-grade="' + gk + '" '
        + inlineStyle
        + 'title="Оценка: ' + label + '. Нажмите для смены">'
        + label + '</button>';
      var publishedIcon = '';
      if (m.published) {
        publishedIcon = '<span class="story-icon story-icon-used" title="Опубликовано">'
          + '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">'
          + '<polyline points="2,8 6,12 14,4"/></svg></span>';
      }
      var infoBtn = '<button class="story-icon story-info-btn" data-copy="movie_id: ' + m.id + '" title="Инфо">'
        + '<svg viewBox="0 0 16 16" fill="none" stroke="#8888b0" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">'
        + '<circle cx="8" cy="8" r="6.5"/>'
        + '<line x1="8" y1="7.5" x2="8" y2="11"/>'
        + '<circle cx="8" cy="5" r="1.1" fill="#8888b0" stroke="none"/>'
        + '</svg></button>';
      var deleteBtn = '<button class="story-icon movie-delete-btn" data-id="' + m.id + '" data-title="' + escHtml(m.story_title || '(без названия)') + '" title="Удалить">'
        + '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">'
        + '<polyline points="2,4 14,4"/><path d="M6 4V2h4v2"/><rect x="3" y="4" width="10" height="10" rx="1.5"/><line x1="6" y1="7" x2="6" y2="11"/><line x1="10" y1="7" x2="10" y2="11"/>'
        + '</svg></button>';
      html += '<div class="story-row" data-id="' + m.id + '">'
        + '<div class="story-title">' + escHtml(m.story_title || '(без названия)') + modelLabel + ' ' + gradeBadge + '</div>'
        + '<div class="story-row-right">' + publishedIcon + infoBtn + deleteBtn + '</div>'
        + '</div>';
    }
    container.innerHTML = html;
    container.querySelectorAll('.story-row').forEach(function(row) {
      row.addEventListener('click', function() {
        selectMovie(row.getAttribute('data-id'));
      });
    });
    container.querySelectorAll('.story-grade-badge').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        cycleMovieGrade(btn);
      });
    });
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
    _updateMovieSelection();
    /* обновляем плашку карточки если выбранный фильм есть в новом списке */
    if (_selectedMovieId) {
      var rec = _moviesData.filter(function(m) { return String(m.id) === String(_selectedMovieId); })[0];
      if (rec) setCardMovieGradeBadge(rec.grade !== undefined ? rec.grade : null, false);
    }
  }

  /* ── цикл grade в строке списка ── */
  function cycleMovieGrade(btn) {
    var movieId = btn.getAttribute('data-id');
    var currentAttr = btn.getAttribute('data-grade');
    var current = currentAttr === 'null' ? null : currentAttr;
    var idx = GRADE_CYCLE.indexOf(current);
    var next = GRADE_CYCLE[(idx + 1) % GRADE_CYCLE.length];
    btn.disabled = true;
    fetch('/production/movie/' + movieId + '/grade', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ grade: next }),
    })
    .then(function(r) { return r.ok ? r.json() : null; })
    .then(function(d) {
      btn.disabled = false;
      if (d && d.ok) {
        var g = d.grade !== undefined ? d.grade : null;
        var gk = gradeKey(g);
        btn.setAttribute('data-grade', gk);
        btn.style.background = gk !== 'null' ? (GRADE_COLORS[gk] || '') : '';
        btn.style.color      = gk !== 'null' ? (GRADE_TEXT_COLORS[gk] || '') : '';
        btn.textContent = GRADE_LABELS[gk] || gk;
        btn.title = 'Оценка: ' + (GRADE_LABELS[gk] || gk) + '. Нажмите для смены';
        /* если это выбранный элемент — синхронизируем карточку */
        if (String(movieId) === String(_selectedMovieId)) {
          setCardMovieGradeBadge(g, false);
        }
        window.loadMovieList();
      }
    })
    .catch(function() { btn.disabled = false; });
  }

  function _openDeleteMovieDialog(movieId, movieTitle, triggerBtn) {
    new ConfirmDialog({
      title:        'Удалить видео?',
      text:         'Видео «' + escHtml(movieTitle) + '» и все связанные батчи, лог и записи лога будут удалены безвозвратно. Сюжет останется нетронутым.',
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
            if (String(movieId) === String(_selectedMovieId)) {
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
  window.loadMovieList = function() {
    var container = document.getElementById('movies-list');
    if (!container) return;
    var hasContent = container.querySelector('.story-row');
    if (!hasContent) container.innerHTML = '<div class="stories-loading">Загрузка...</div>';
    fetch('/production/movies?' + getFilterParams())
      .then(function(r) { return r.json(); })
      .then(function(data) { renderMovies(data); })
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

    if (forApproval)   forApproval.addEventListener('change', function() { onFilterChange(forApproval); });
    if (onlyGood)      onlyGood.addEventListener('change',    function() { onFilterChange(onlyGood); });
    if (showPublished) showPublished.addEventListener('change',function() { onFilterChange(showPublished); });
  }

  function _openDeleteBadMoviesDialog(btn) {
    new ConfirmDialog({
      title:        'Удалить неудачные видео?',
      text:         'Будут удалены все видео с оценкой «плохо»,<br>а также связанные с ними батчи и записи лога.<br>Сюжеты затронуты не будут.',
      confirmLabel: 'Удалить',
      triggerBtn:   btn,
      onConfirm: function(confirmBtn, dlg) {
        confirmBtn.disabled    = true;
        confirmBtn.textContent = 'Удаление…';
        btn.classList.add('pending');
        fetch('/production/movies/delete_bad', { method: 'POST' })
          .then(function(r) { return r.ok ? r.json() : null; })
          .then(function(d) {
            btn.classList.remove('pending');
            dlg.close();
            if (d && d.ok) {
              var n = d.deleted ? (d.deleted.movies || 0) : 0;
              if (typeof window.showToast === 'function') window.showToast('Удалено ' + n + ' видео');
              if (typeof window.loadMovieList === 'function') window.loadMovieList();
            }
          })
          .catch(function() { btn.classList.remove('pending'); dlg.close(); });
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

  function _sanitizeFilename(name) {
    return String(name || '').replace(/[\\/:*?"<>|]/g, '_').replace(/\s+/g, ' ').trim();
  }

  function _buildExportFilename(meta) {
    var model = (meta.model_name || '').trim();
    var story = (meta.story_title || '').trim();
    var gradeRaw = (meta.grade !== null && meta.grade !== undefined) ? String(meta.grade) : null;
    var grade = gradeRaw ? (GRADE_LABELS[gradeRaw] || '') : '';
    var parts = [];
    if (model) parts.push(model);
    if (story) parts.push(story);
    if (grade) parts.push(grade);
    var raw = parts.length ? parts.join(' - ') : String(meta.id);
    return _sanitizeFilename(raw) + '.mp4';
  }

  async function _resolveUniqueFilename(dirHandle, baseName) {
    var dot = baseName.lastIndexOf('.');
    var name = dot !== -1 ? baseName.slice(0, dot) : baseName;
    var ext  = dot !== -1 ? baseName.slice(dot)    : '';
    var candidate = baseName;
    var n = 1;
    while (true) {
      try {
        await dirHandle.getFileHandle(candidate);
        n++;
        candidate = name + ' (' + n + ')' + ext;
      } catch (e) {
        return candidate;
      }
    }
  }

  async function _exportGoodMovies(triggerBtn) {
    var dirHandle;
    try {
      dirHandle = await window.showDirectoryPicker({ mode: 'readwrite' });
    } catch (e) {
      return;
    }

    var metaList;
    try {
      var r = await fetch('/production/movies/good_meta');
      if (!r.ok) throw new Error('http ' + r.status);
      metaList = await r.json();
    } catch (e) {
      if (typeof window.showToast === 'function') window.showToast('Ошибка получения списка роликов');
      return;
    }

    if (!Array.isArray(metaList) || metaList.length === 0) {
      if (typeof window.showToast === 'function') window.showToast('Нет роликов с оценкой «хорошо»');
      return;
    }

    triggerBtn.disabled = true;
    var dlg = new ExportMoviesDialog({ total: metaList.length, dirHandle: dirHandle, triggerBtn: triggerBtn });
    dlg.open();

    var done = 0;
    var failed = 0;
    for (var i = 0; i < metaList.length; i++) {
      if (dlg.isCancelled()) break;
      var meta = metaList[i];
      var filename = await _resolveUniqueFilename(dirHandle, _buildExportFilename(meta));
      dlg.setProgress(done, filename);
      try {
        var resp = await fetch('/production/movies/' + encodeURIComponent(meta.id) + '/download');
        if (!resp.ok) throw new Error('http ' + resp.status);
        var blob = await resp.blob();
        var fh = await dirHandle.getFileHandle(filename, { create: true });
        var writable = await fh.createWritable();
        await writable.write(blob);
        await writable.close();
        done++;
        dlg.setProgress(done, filename);
      } catch (e) {
        if (dlg.isCancelled()) break;
        failed++;
      }
    }

    triggerBtn.disabled = false;
    dlg.finish(done, dlg.isCancelled(), failed);
  }

  function initExportGoodMoviesButton() {
    var btn = document.getElementById('btn-export-good-movies');
    if (!btn) return;
    if (!window.showDirectoryPicker) {
      btn.disabled = true;
      btn.title = 'Ваш браузер не поддерживает выбор папки';
      return;
    }
    btn.addEventListener('click', function() {
      if (btn.disabled) return;
      _exportGoodMovies(btn);
    });
  }

  window.directorUpdateVideoWrapHeight = updateVideoWrapHeight;

  function initAutoplayToggle() {
    var chk = document.getElementById('director-autoplay-check');
    if (!chk) return;
    chk.addEventListener('change', function() {
      var fd = new FormData();
      fd.append('producer_autoplay_movie', chk.checked ? '1' : '0');
      fetch('/save', { method: 'POST', body: fd });
    });
  }

  function initDirector() {
    initFilters();
    initCardMovieGradeBadge();
    initDeleteBadMoviesButton();
    initExportGoodMoviesButton();
    initAutoplayToggle();
    loadMovieInPlayer(null);
    window.addEventListener('resize', updateVideoWrapHeight);
  }

  window._directorApi = {
    startBatchPoll: _startActiveBatchPoll,
    getSelectedMovieId: function() { return _selectedMovieId; },
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initDirector);
  } else {
    initDirector();
  }
})();
