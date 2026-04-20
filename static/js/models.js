var _gradeSequence = ['good', 'limited', 'poor', 'fallback', 'rejected'];
var _gradeColors   = { good: '#4a8', limited: '#a84', poor: '#b60', fallback: '#a33', rejected: '#666' };
var _gradeLabels   = { good: 'хорошо', limited: 'ограничен', poor: 'слабо', fallback: 'запасной', rejected: 'отклонён' };


window.cycleGrade = function(el) {
  var id   = el.getAttribute('data-grade-id');
  var cur  = el.getAttribute('data-grade') || 'good';
  var next = _gradeSequence[(_gradeSequence.indexOf(cur) + 1) % _gradeSequence.length];
  el.setAttribute('data-grade', next);
  el.textContent = _gradeLabels[next] || next;
  el.style.background = _gradeColors[next] || '#555';
  fetch('/api/text-models/' + encodeURIComponent(id) + '/grade', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ grade: next })
  });
};

window.cycleVideoGrade = function(el) {
  var id   = el.getAttribute('data-grade-id');
  var cur  = el.getAttribute('data-grade') || 'good';
  var next = _gradeSequence[(_gradeSequence.indexOf(cur) + 1) % _gradeSequence.length];
  el.setAttribute('data-grade', next);
  el.textContent = _gradeLabels[next] || next;
  el.style.background = _gradeColors[next] || '#555';
  fetch('/api/video-models/' + encodeURIComponent(id) + '/grade', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ grade: next })
  });
};

window.createDirectorVideo = function(modelId, modelName, btn) {
  var storyIdEl = document.getElementById('director-story-id');
  var storyId = storyIdEl ? (storyIdEl.value || '') : '';

  var hintEl = document.getElementById('director-video-hint');
  var videoWrap = document.getElementById('director-video-wrap');
  var _DEFAULT_HINT = 'Вы можете сгенерировать ролик, нажав кнопку «Создать» на нужной модели.';
  var _hintResetTimer = null;

  function setHint(text) {
    if (hintEl) hintEl.textContent = text;
  }

  function scheduleResetHint() {
    if (_hintResetTimer) clearTimeout(_hintResetTimer);
    _hintResetTimer = setTimeout(function() { _hintResetTimer = null; setHint(_DEFAULT_HINT); }, 2000);
  }

  btn.classList.add('probing');

  if (videoWrap) { videoWrap.innerHTML = ''; videoWrap.style.display = 'none'; }
  setHint(_DEFAULT_HINT);

  fetch('/api/video-models/' + encodeURIComponent(modelId) + '/probe', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(storyId ? { story_id: storyId } : {})
  })
    .then(function(r) { return r.json(); })
    .then(function(data) {
      if (data.error) {
        btn.classList.remove('probing');
        setHint('Ошибка: ' + data.error);
        return;
      }
      var batchId = data.batch_id;
      var _TERMINAL = ['probe', 'movie_probe', 'story_probe', 'video_error', 'transcode_error', 'publish_error', 'cancelled', 'error'];

      function poll() {
        fetch('/api/batch/' + batchId + '/logs')
          .then(function(r) { return r.json(); })
          .then(function(d) {
            if (d.error) { setTimeout(poll, 700); return; }
            var logs = d.logs || [];
            if (logs.length) {
              var lastLog = logs[logs.length - 1];
              var entries = lastLog.entries || [];
              if (entries.length) {
                setHint(entries[entries.length - 1].message);
              } else if (lastLog.message) {
                setHint(lastLog.message);
              }
            }
            var status = d.batch_status;
            if (_TERMINAL.indexOf(status) !== -1) {
              btn.classList.remove('probing');
              if (d.has_video_data) {
                var src = '/api/batch/' + encodeURIComponent(batchId) + '/video';
                if (videoWrap) {
                  videoWrap.innerHTML = '<video class="probe-video" controls autoplay src="' + src + '"></video>';
                  videoWrap.style.display = 'block';
                }
              }
              scheduleResetHint();
            } else {
              setTimeout(poll, 700);
            }
          })
          .catch(function() { setTimeout(poll, 700); });
      }
      poll();
    })
    .catch(function(e) {
      btn.classList.remove('probing');
      setHint('Ошибка запроса: ' + e);
    });
};

(function() {
  var dragSrcId = null;

  function escHtml(s) {
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  function makeDragHandlers(item, containerId, m, saveOrderFn) {
    item.addEventListener('dragstart', function(e) {
      dragSrcId = m.id;
      item.classList.add('dragging');
      e.dataTransfer.effectAllowed = 'move';
      e.dataTransfer.setData('text/plain', m.id);
    });
    item.addEventListener('dragend', function() {
      item.classList.remove('dragging');
      document.getElementById(containerId).querySelectorAll('.model-item').forEach(function(el) {
        el.classList.remove('drag-over-top', 'drag-over-bottom');
      });
    });
    item.addEventListener('dragover', function(e) {
      e.preventDefault();
      e.dataTransfer.dropEffect = 'move';
      if (dragSrcId === m.id) return;
      const rect = item.getBoundingClientRect();
      const mid  = rect.top + rect.height / 2;
      document.getElementById(containerId).querySelectorAll('.model-item').forEach(function(el) {
        el.classList.remove('drag-over-top', 'drag-over-bottom');
      });
      if (e.clientY < mid) item.classList.add('drag-over-top');
      else                 item.classList.add('drag-over-bottom');
    });
    item.addEventListener('dragleave', function() {
      item.classList.remove('drag-over-top', 'drag-over-bottom');
    });
    item.addEventListener('drop', function(e) {
      e.preventDefault();
      if (dragSrcId === null || dragSrcId === m.id) return;
      const container = document.getElementById(containerId);
      container.querySelectorAll('.model-item').forEach(function(el) {
        el.classList.remove('drag-over-top', 'drag-over-bottom');
      });
      const rect = item.getBoundingClientRect();
      const mid  = rect.top + rect.height / 2;
      const srcEl = container.querySelector('[data-id="' + dragSrcId + '"]');
      if (srcEl) {
        if (e.clientY < mid) container.insertBefore(srcEl, item);
        else                 container.insertBefore(srcEl, item.nextSibling);
      }
      const ids = Array.from(container.querySelectorAll('.model-item')).map(function(el) { return el.dataset.id; });
      dragSrcId = null;
      saveOrderFn(ids);
    });
  }

  function nearestAllowedDuration(value, allowed) {
    if (!allowed || (allowed.length === 1 && allowed[0] === 0)) return value;
    var best = null, bestDiff = null;
    for (var i = 0; i < allowed.length; i++) {
      var diff = Math.abs(allowed[i] - value);
      if (bestDiff === null || diff < bestDiff || (diff === bestDiff && allowed[i] < best)) {
        best = allowed[i];
        bestDiff = diff;
      }
    }
    return best;
  }

  function showFloatingTooltip(anchorEl, text) {
    var existing = document.getElementById('_dur_tooltip');
    if (existing) existing.parentNode.removeChild(existing);
    var tip = document.createElement('div');
    tip.id = '_dur_tooltip';
    tip.textContent = text;
    tip.style.cssText = 'position:fixed;z-index:9999;background:#333;color:#fff;font-size:12px;padding:5px 9px;border-radius:4px;max-width:280px;pointer-events:none;white-space:normal;line-height:1.4;box-shadow:0 2px 8px rgba(0,0,0,.3);';
    document.body.appendChild(tip);
    var rect = anchorEl.getBoundingClientRect();
    var tw = tip.offsetWidth, th = tip.offsetHeight;
    var left = Math.min(rect.left, window.innerWidth - tw - 8);
    var top = rect.top - th - 6;
    if (top < 4) top = rect.bottom + 6;
    tip.style.left = Math.max(4, left) + 'px';
    tip.style.top = top + 'px';
    var hide = function() {
      if (tip.parentNode) tip.parentNode.removeChild(tip);
      document.removeEventListener('touchstart', hide);
      document.removeEventListener('click', hide);
    };
    setTimeout(function() {
      document.addEventListener('touchstart', hide, { once: true, passive: true });
      document.addEventListener('click', hide, { once: true });
    }, 10);
    setTimeout(hide, 3000);
  }

  function makeDurationIndicator(m, videoDuration) {
    var allowed = m.allowed_durations;
    if (!allowed || !allowed.length) return '';
    var isUnlimited = allowed.length === 1 && allowed[0] === 0;
    var supported, nearest, tipText;
    if (isUnlimited) {
      supported = true;
      tipText = 'Указанная длительность (' + videoDuration + ' сек) поддерживается моделью';
    } else {
      nearest = nearestAllowedDuration(videoDuration, allowed);
      supported = nearest === videoDuration;
      tipText = supported
        ? 'Указанная длительность (' + videoDuration + ' сек) поддерживается моделью'
        : 'Указанная длительность (' + videoDuration + ' сек) не поддерживается моделью и будет скорректирована до ' + nearest + ' сек';
    }
    var icon = supported ? '✓' : '⚠';
    var color = supported ? '#4a8' : '#c80';
    var span = document.createElement('span');
    span.setAttribute('data-role', 'duration-indicator');
    span.textContent = icon;
    span.title = tipText;
    span.style.cssText = 'font-size:13px;color:' + color + ';margin-left:auto;margin-right:4px;cursor:default;user-select:none;flex-shrink:0;';
    span.draggable = false;
    var touchTimer = null;
    span.addEventListener('touchstart', function(e) {
      e.stopPropagation();
      touchTimer = setTimeout(function() {
        touchTimer = null;
        showFloatingTooltip(span, tipText);
      }, 500);
    });
    span.addEventListener('touchend', function() {
      if (touchTimer) { clearTimeout(touchTimer); touchTimer = null; }
    });
    span.addEventListener('touchmove', function() {
      if (touchTimer) { clearTimeout(touchTimer); touchTimer = null; }
    });
    span.addEventListener('dragstart', function(e) {
      e.stopPropagation();
      e.preventDefault();
    });
    return span;
  }

  window.renderModelList = function(containerId, list, opts) {
    opts = opts || {};
    var gradeFn     = opts.gradeFn     || 'cycleVideoGrade';
    var saveOrderFn = opts.saveOrderFn || saveVideoOrder;
    var activateFn  = opts.activateFn  || 'activateModel';
    var actionTitle = opts.actionTitle || 'Пробный запрос';
    var actionFn    = opts.actionFn    || 'createProbeVideo';
    var videoDuration = opts.videoDuration || 6;

    const container = document.getElementById(containerId);
    if (!container) return;
    if (!list || list.length === 0) {
      container.innerHTML = '<div class="model-loading">Нет моделей</div>';
      return;
    }
    container.innerHTML = '';
    list.forEach(function(m) {
      const item = document.createElement('div');
      item.className = 'model-item' + (m.active ? ' model-active' : '');
      item.dataset.id = m.id;
      item.draggable = true;
      if (m.allowed_durations) item.dataset.allowedDurations = JSON.stringify(m.allowed_durations);
      if (m.price) item.dataset.price = m.price;
      var caption = m.platform_name ? escHtml(m.platform_name) + ': ' + escHtml(m.name) : escHtml(m.name);
      var grade = m.grade || 'good';
      var gradeHtml = '<span data-grade-id="' + m.id + '" data-grade="' + grade + '" onclick="event.stopPropagation();' + gradeFn + '(this)" title="Нажмите для смены" style="cursor:pointer;font-size:10px;padding:1px 6px;border-radius:3px;background:' + (_gradeColors[grade]||'#555') + ';color:#fff;margin-left:6px;opacity:.85">' + (_gradeLabels[grade]||grade) + '</span>';
      item.innerHTML =
        '<div class="model-radio" onclick="' + activateFn + '(\'' + m.id + '\')">' +
          '<div class="model-radio-dot"></div>' +
        '</div>' +
        '<div class="model-name">' + caption + gradeHtml + '</div>' +
        '<button class="model-probe-btn" title="' + escHtml(actionTitle) + '" onclick="event.stopPropagation();' + actionFn + '(\'' + m.id + '\',\'' + escHtml(m.name) + '\',this)"><svg viewBox="0 0 16 16" fill="none" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><polygon points="4,2 13,8 4,14"/></svg></button>' +
        '<div class="model-drag-handle" title="Перетащить">⠿</div>';
      var probeBtn = item.querySelector('.model-probe-btn');
      if (probeBtn) {
        if (m.price) {
          var priceSpan = document.createElement('span');
          priceSpan.style.cssText = 'font-size:11px;color:#888;margin-left:auto;padding-right:6px;white-space:nowrap';
          priceSpan.textContent = m.price;
          item.insertBefore(priceSpan, probeBtn);
        }
        var durationIndicator = makeDurationIndicator(m, videoDuration);
        if (durationIndicator) {
          if (m.price) durationIndicator.style.marginLeft = '';
          item.insertBefore(durationIndicator, probeBtn);
        }
      }
      makeDragHandlers(item, containerId, m, saveOrderFn);
      container.appendChild(item);
    });
  };

  function refreshDurationIndicators(containerId, videoDuration) {
    var container = document.getElementById(containerId);
    if (!container) return;
    container.querySelectorAll('.model-item').forEach(function(item) {
      var existing = item.querySelector('[data-role="duration-indicator"]');
      if (existing) existing.parentNode.removeChild(existing);
      var rawDurations = item.dataset.allowedDurations;
      if (!rawDurations) return;
      var allowed;
      try { allowed = JSON.parse(rawDurations); } catch(e) { return; }
      var fakeModel = { allowed_durations: allowed };
      var newIndicator = makeDurationIndicator(fakeModel, videoDuration);
      if (newIndicator) {
        var probeBtn = item.querySelector('.model-probe-btn');
        if (probeBtn) {
          if (item.dataset.price) newIndicator.style.marginLeft = '';
          item.insertBefore(newIndicator, probeBtn);
        }
      }
    });
  }

  var _videoDurationListenerAdded = false;

  function attachVideoDurationListener() {
    if (_videoDurationListenerAdded) return;
    var durationEl = document.getElementById('video_duration');
    if (!durationEl) return;
    _videoDurationListenerAdded = true;
    durationEl.addEventListener('input', function() {
      var duration = getVideoDuration();
      refreshDurationIndicators('model-list', duration);
    });
  }

  window.attachVideoDurationListener = attachVideoDurationListener;

  function saveVideoOrder(ids) {
    fetch('/api/models/reorder', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ids: ids})
    }).catch(function(e) { console.error('reorder error', e); loadModels(); });
  }

  function toggleVideoModelActive(containerId, id) {
    var container = document.getElementById(containerId);
    if (container) {
      var item = container.querySelector('[data-id="' + id + '"]');
      if (item) item.classList.toggle('model-active');
    }
  }

  window.activateModel = function(id) {
    toggleVideoModelActive('model-list', id);
    fetch('/api/models/' + id + '/activate', {method: 'POST'})
      .catch(function(e) { console.error('activate error', e); loadModels(); });
  };

  function getVideoDuration() {
    var el = document.getElementById('video_duration');
    var v = el ? parseInt(el.value, 10) : NaN;
    return isNaN(v) ? 6 : v;
  }

  function loadModels() {
    fetch('/api/models')
      .then(function(r) { return r.json(); })
      .then(function(data) {
        window.renderModelList('model-list', data, {
          gradeFn: 'cycleVideoGrade',
          saveOrderFn: saveVideoOrder,
          activateFn: 'activateModel',
          actionTitle: 'Пробный запрос',
          actionFn: 'createProbeVideo',
          videoDuration: getVideoDuration()
        });
        attachVideoDurationListener();
      })
      .catch(function() {
        const c = document.getElementById('model-list');
        if (c) c.innerHTML = '<div class="model-loading">Ошибка загрузки</div>';
      });
  }

  window.loadModels = loadModels;

  (function() {
    const panel = document.getElementById('panel-request');
    if (panel && panel.classList.contains('active')) loadModels();
  })();
})();

(function() {
  var dragSrcId = null;

  function escHtml(s) {
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  function renderTextModels(list) {
    const container = document.getElementById('text-model-list');
    if (!container) return;
    if (!list || list.length === 0) {
      container.innerHTML = '<div class="model-loading">Нет моделей</div>';
      return;
    }
    container.innerHTML = '';
    list.forEach(function(m) {
      const item = document.createElement('div');
      item.className = 'model-item' + (m.active ? ' model-active' : '');
      item.dataset.id = m.id;
      item.draggable = true;
      var caption = m.platform_name ? escHtml(m.platform_name) + ': ' + escHtml(m.name) : escHtml(m.name);
      var grade = m.grade || 'good';
      var gradeHtml = '<span data-grade-id="' + m.id + '" data-grade="' + grade + '" onclick="event.stopPropagation();cycleGrade(this)" title="Нажмите для смены" style="cursor:pointer;font-size:10px;padding:1px 6px;border-radius:3px;background:' + (_gradeColors[grade]||'#555') + ';color:#fff;margin-left:6px;opacity:.85">' + (_gradeLabels[grade]||grade) + '</span>';
      item.innerHTML =
        '<div class="model-radio" onclick="activateTextModel(\'' + m.id + '\')">' +
          '<div class="model-radio-dot"></div>' +
        '</div>' +
        '<div class="model-name">' + caption + gradeHtml + '</div>' +
        '<button class="model-probe-btn" title="Пробный запрос" onclick="event.stopPropagation();probeTextModel(\'' + m.id + '\',\'' + escHtml(m.name) + '\',this)"><svg viewBox="0 0 16 16" fill="none" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><polygon points="4,2 13,8 4,14"/></svg></button>' +
        '<div class="model-drag-handle" title="Перетащить">⠿</div>';

      item.addEventListener('dragstart', function(e) {
        dragSrcId = m.id;
        item.classList.add('dragging');
        e.dataTransfer.effectAllowed = 'move';
        e.dataTransfer.setData('text/plain', m.id);
      });
      item.addEventListener('dragend', function() {
        item.classList.remove('dragging');
        container.querySelectorAll('.model-item').forEach(function(el) {
          el.classList.remove('drag-over-top', 'drag-over-bottom');
        });
      });
      item.addEventListener('dragover', function(e) {
        e.preventDefault();
        e.dataTransfer.dropEffect = 'move';
        if (dragSrcId === m.id) return;
        const rect = item.getBoundingClientRect();
        const mid  = rect.top + rect.height / 2;
        container.querySelectorAll('.model-item').forEach(function(el) {
          el.classList.remove('drag-over-top', 'drag-over-bottom');
        });
        if (e.clientY < mid) item.classList.add('drag-over-top');
        else                 item.classList.add('drag-over-bottom');
      });
      item.addEventListener('dragleave', function() {
        item.classList.remove('drag-over-top', 'drag-over-bottom');
      });
      item.addEventListener('drop', function(e) {
        e.preventDefault();
        if (dragSrcId === null || dragSrcId === m.id) return;
        container.querySelectorAll('.model-item').forEach(function(el) {
          el.classList.remove('drag-over-top', 'drag-over-bottom');
        });
        const rect = item.getBoundingClientRect();
        const mid  = rect.top + rect.height / 2;
        const srcEl = container.querySelector('[data-id="' + dragSrcId + '"]');
        if (srcEl) {
          if (e.clientY < mid) container.insertBefore(srcEl, item);
          else                 container.insertBefore(srcEl, item.nextSibling);
        }
        const ids = Array.from(container.querySelectorAll('.model-item')).map(function(el) { return el.dataset.id; });
        dragSrcId = null;
        saveTextOrder(ids);
      });

      container.appendChild(item);
    });
  }

  function saveTextOrder(ids) {
    fetch('/api/text-models/reorder', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ids: ids})
    }).catch(function(e) { console.error('text reorder error', e); loadTextModels(); });
  }

  window.activateTextModel = function(id) {
    var container = document.getElementById('text-model-list');
    if (container) {
      var item = container.querySelector('[data-id="' + id + '"]');
      if (item) item.classList.toggle('model-active');
    }
    fetch('/api/text-models/' + id + '/activate', {method: 'POST'})
      .catch(function(e) { console.error('text activate error', e); loadTextModels(); });
  };

  function loadTextModels() {
    fetch('/api/text-models')
      .then(function(r) { return r.json(); })
      .then(function(data) { renderTextModels(data); })
      .catch(function() {
        const c = document.getElementById('text-model-list');
        if (c) c.innerHTML = '<div class="model-loading">Ошибка загрузки</div>';
      });
  }

  window.loadTextModels = loadTextModels;

  (function() {
    const panel = document.getElementById('panel-story');
    if (panel && panel.classList.contains('active')) loadTextModels();
  })();
})();
