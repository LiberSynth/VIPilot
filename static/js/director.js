(function() {
  var GRADE_LABELS      = { 'null': 'не указано', 'good': 'хорошо', 'bad': 'плохо' };
  var GRADE_COLORS      = { 'null': 'rgba(255,255,255,.07)', 'good': 'rgba(80,200,120,.25)', 'bad': 'rgba(200,80,80,.25)' };
  var GRADE_TEXT_COLORS = { 'null': '#aaa', 'good': '#6ee7a0', 'bad': '#f87171' };
  var GRADE_CYCLE       = ['good', 'bad', null];

  var _selectedMovieId = null;

  function gradeKey(g) { return (g === null || g === undefined) ? 'null' : String(g); }

  function escHtml(s) {
    return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

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

  function loadMovieInPlayer(movieId) {
    var wrap  = document.getElementById('director-video-wrap');
    var empty = document.getElementById('director-movie-empty');
    if (!wrap) return;
    if (!movieId) {
      wrap.innerHTML = '';
      wrap.style.display = 'none';
      if (empty) empty.style.display = '';
      return;
    }
    var src = '/production/movie/' + encodeURIComponent(movieId) + '/video';
    wrap.innerHTML = '<video class="probe-video" controls autoplay src="' + src + '"></video>';
    wrap.style.display = 'block';
    if (empty) empty.style.display = 'none';
  }

  function selectMovie(movieId) {
    _selectedMovieId = movieId || null;
    _updateMovieSelection();
    loadMovieInPlayer(_selectedMovieId);
  }

  function renderMovies(movies) {
    var container = document.getElementById('movies-list');
    if (!container) return;
    if (!movies || movies.length === 0) {
      updateCount(0);
      container.innerHTML = '<div class="stories-empty">Нет видео</div>';
      return;
    }
    updateCount(movies.length);
    var html = '';
    for (var i = 0; i < movies.length; i++) {
      var m = movies[i];
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
      html += '<div class="story-row" data-id="' + m.id + '">'
        + '<div class="story-title">' + escHtml(m.story_title || '(без названия)') + modelLabel + ' ' + gradeBadge + '</div>'
        + '<div class="story-row-right">' + publishedIcon + '</div>'
        + '</div>';
    }
    container.innerHTML = html;
    container.querySelectorAll('.story-row').forEach(function(row) {
      row.addEventListener('click', function() {
        var movieId = row.getAttribute('data-id');
        selectMovie(movieId);
      });
    });
    container.querySelectorAll('.story-grade-badge').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        cycleMovieGrade(btn);
      });
    });
    _updateMovieSelection();
  }

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
        btn.style.color = gk !== 'null' ? (GRADE_TEXT_COLORS[gk] || '') : '';
        btn.textContent = GRADE_LABELS[gk] || gk;
        btn.title = 'Оценка: ' + (GRADE_LABELS[gk] || gk) + '. Нажмите для смены';
        window.loadMovieList();
      }
    })
    .catch(function() { btn.disabled = false; });
  }

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
    if (showPublished) showPublished.addEventListener('change',  function() { onFilterChange(showPublished); });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initFilters);
  } else {
    initFilters();
  }
})();
