(function() {
  var GRADE_LABELS      = { 'null': 'не указано', 'good': 'хорошо', 'bad': 'плохо' };
  var GRADE_COLORS      = { 'null': 'rgba(255,255,255,.07)', 'good': 'rgba(80,200,120,.25)', 'bad': 'rgba(200,80,80,.25)' };
  var GRADE_TEXT_COLORS = { 'null': '#aaa', 'good': '#6ee7a0', 'bad': '#f87171' };
  var GRADE_CYCLE       = ['good', 'bad', null];

  var _INFO_SVG = '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="8" cy="8" r="6.5"/><line x1="8" y1="7.5" x2="8" y2="11.5"/><circle cx="8" cy="5" r="1" fill="currentColor" stroke="none"/></svg>';

  var _selectedMovieId = null;
  var _moviesData      = [];

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

  /* ── плеер ── */
  function loadMovieInPlayer(movieId) {
    var wrap     = document.getElementById('director-video-wrap');
    var empty    = document.getElementById('director-movie-empty');
    var titleEl  = document.getElementById('director-movie-title');
    if (!wrap) return;
    if (!movieId) {
      wrap.innerHTML = '';
      wrap.style.display = 'none';
      if (empty)   empty.style.display   = '';
      if (titleEl) { titleEl.textContent = ''; titleEl.style.display = 'none'; }
      setCardMovieGradeBadge(null, true);
      return;
    }
    var rec = _moviesData.filter(function(m) { return String(m.id) === String(movieId); })[0];
    if (titleEl) {
      var t = (rec && rec.story_title) ? rec.story_title : '';
      titleEl.textContent  = t;
      titleEl.style.display = t ? '' : 'none';
    }
    var src = '/production/movie/' + encodeURIComponent(movieId) + '/video';
    wrap.innerHTML = '<video class="probe-video" controls autoplay src="' + src + '"></video>';
    wrap.style.display = 'block';
    if (empty) empty.style.display = 'none';
    setCardMovieGradeBadge(rec ? rec.grade : null, false);
  }

  function selectMovie(movieId) {
    _selectedMovieId = movieId || null;
    _updateMovieSelection();
    loadMovieInPlayer(_selectedMovieId);
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
      var infoBtn = '<button class="story-info-btn" data-copy="movie_id: ' + m.id + '" title="movie_id: ' + m.id + '">'
        + _INFO_SVG + '</button>';
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

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function() { initFilters(); initCardMovieGradeBadge(); });
  } else {
    initFilters();
    initCardMovieGradeBadge();
  }
})();
