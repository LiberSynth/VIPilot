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
      html += '<div class="story-row" data-id="' + m.id + '">'
        + '<div class="story-title">' + escHtml(m.story_title || '(без названия)') + modelLabel + ' ' + gradeBadge + '</div>'
        + '<div class="story-row-right">' + publishedIcon + infoBtn + '</div>'
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

  function _closeDeleteBadMoviesDialog() {
    var el = document.getElementById('deleteBadMoviesOverlay');
    if (el) el.remove();
  }

  function _openDeleteBadMoviesDialog(btn) {
    var existing = document.getElementById('deleteBadMoviesOverlay');
    if (existing) existing.remove();
    var el = document.createElement('div');
    el.className = 'confirm-overlay open';
    el.id = 'deleteBadMoviesOverlay';
    el.innerHTML =
      '<div class="confirm-box">' +
        '<div class="confirm-box-title">Удалить неудачные видео?</div>' +
        '<div class="confirm-box-text">' +
          'Будут удалены все видео с оценкой «плохо»,<br>а также связанные с ними батчи и записи лога.<br>Сюжеты затронуты не будут.' +
        '</div>' +
        '<div class="confirm-box-btns">' +
          '<button class="confirm-cancel" id="deleteBadMoviesCancelBtn">Отмена</button>' +
          '<button class="confirm-confirm" id="deleteBadMoviesConfirmBtn" style="background:#b05820">Удалить</button>' +
        '</div>' +
      '</div>';
    document.body.appendChild(el);
    document.getElementById('deleteBadMoviesCancelBtn').addEventListener('click', _closeDeleteBadMoviesDialog);
    document.getElementById('deleteBadMoviesConfirmBtn').addEventListener('click', function() {
      var confirmBtn = document.getElementById('deleteBadMoviesConfirmBtn');
      if (confirmBtn) { confirmBtn.disabled = true; confirmBtn.textContent = 'Удаление…'; }
      btn.classList.add('pending');
      fetch('/production/movies/delete_bad', { method: 'POST' })
        .then(function(r) { return r.ok ? r.json() : null; })
        .then(function(d) {
          btn.classList.remove('pending');
          _closeDeleteBadMoviesDialog();
          if (d && d.ok) {
            var n = d.deleted ? (d.deleted.movies || 0) : 0;
            var mod10 = n % 10, mod100 = n % 100;
            var word;
            if (mod100 >= 11 && mod100 <= 14) { word = 'видео'; }
            else if (mod10 === 1) { word = 'видео'; }
            else if (mod10 >= 2 && mod10 <= 4) { word = 'видео'; }
            else { word = 'видео'; }
            if (typeof window.showToast === 'function') window.showToast('Удалено ' + n + ' ' + word);
            if (typeof window.loadMovieList === 'function') window.loadMovieList();
          }
        })
        .catch(function() { btn.classList.remove('pending'); _closeDeleteBadMoviesDialog(); });
    });
  }

  function initDeleteBadMoviesButton() {
    var btn = document.getElementById('btn-delete-bad-movies');
    if (!btn) return;
    btn.addEventListener('click', function() {
      if (btn.classList.contains('pending')) return;
      _openDeleteBadMoviesDialog(btn);
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
