(function() {
  var _MODEL_GRADE_LABELS = {
    good: 'хорошо', limited: 'ограничен', poor: 'слабо',
    fallback: 'запасной', rejected: 'отклонён', 'null': '—'
  };
  var _MODEL_GRADE_COLORS = {
    good:     'rgba(62,207,142,.18)',
    limited:  'rgba(245,166,35,.18)',
    poor:     'rgba(255,120,0,.18)',
    fallback: 'rgba(180,80,80,.18)',
    rejected: 'rgba(100,100,100,.18)',
    'null':   'rgba(255,255,255,.06)'
  };
  var _MODEL_GRADE_TEXT_COLORS = {
    good:     '#3ecf8e',
    limited:  '#f5a623',
    poor:     '#e07030',
    fallback: '#c06060',
    rejected: '#7a7a7a',
    'null':   '#888'
  };
  var _MODEL_GRADE_CYCLE = ['good', 'limited', 'poor', 'fallback', 'rejected'];

  function escHtml(s) {
    return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  function nearestAllowedDuration(value, allowed) {
    if (!allowed || (allowed.length === 1 && allowed[0] === 0)) return value;
    var best = null, bestDiff = null;
    for (var i = 0; i < allowed.length; i++) {
      var diff = Math.abs(allowed[i] - value);
      if (bestDiff === null || diff < bestDiff || (diff === bestDiff && allowed[i] < best)) {
        best = allowed[i]; bestDiff = diff;
      }
    }
    return best;
  }

  function makeDurationIndicatorHtml(m, videoDuration) {
    var allowed = m.allowed_durations;
    if (!allowed || !allowed.length) return '';
    var isUnlimited = allowed.length === 1 && allowed[0] === 0;
    var supported, nearest, tipText;
    if (isUnlimited) {
      supported = true;
      tipText = 'Длительность (' + videoDuration + ' сек) поддерживается';
    } else {
      nearest = nearestAllowedDuration(videoDuration, allowed);
      supported = nearest === videoDuration;
      tipText = supported
        ? 'Длительность (' + videoDuration + ' сек) поддерживается'
        : 'Длительность (' + videoDuration + ' сек) будет скорректирована до ' + nearest + ' сек';
    }
    var icon = supported ? '✓' : '⚠';
    var color = supported ? '#4a8' : '#c80';
    return '<span data-role="duration-indicator" title="' + escHtml(tipText) + '" '
      + 'style="font-size:13px;color:' + color + ';margin-right:4px;cursor:default;user-select:none;flex-shrink:0;">'
      + icon + '</span>';
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

  function makeExpandContent(item, noteUrl, bodyUrl) {
    var wrap = document.createElement('div');
    wrap.style.cssText = 'display:flex;flex-direction:column;gap:10px;padding-top:4px;';

    var noteLabel = document.createElement('div');
    noteLabel.className = 'hint';
    noteLabel.style.cssText = 'margin-bottom:2px;color:#e8e8f0;font-size:12px;';
    noteLabel.textContent = 'Заметки';
    wrap.appendChild(noteLabel);

    var noteArea = document.createElement('textarea');
    noteArea.style.cssText = 'width:100%;min-height:60px;resize:vertical;';
    noteArea.placeholder = 'Наблюдения и ограничения';
    noteArea.value = item.note || '';
    noteArea.dataset.modelId = item.id;
    noteArea.dataset.modelField = 'note';
    noteArea.dataset.modelUrl = noteUrl;
    wrap.appendChild(noteArea);

    var bodyLabel = document.createElement('div');
    bodyLabel.className = 'hint';
    bodyLabel.style.cssText = 'margin-bottom:2px;color:#e8e8f0;font-size:12px;';
    bodyLabel.textContent = 'Параметры (body)';
    wrap.appendChild(bodyLabel);

    var bodyArea = document.createElement('textarea');
    bodyArea.style.cssText = 'width:100%;min-height:80px;resize:vertical;font-family:monospace;font-size:13px;';
    bodyArea.placeholder = 'JSON-параметры запроса к модели (body)';
    var bodyVal = item.body && typeof item.body === 'object'
      ? JSON.stringify(item.body, null, 2) : (item.body || '');
    bodyArea.value = bodyVal;
    bodyArea.dataset.modelId = item.id;
    bodyArea.dataset.modelField = 'body';
    bodyArea.dataset.modelUrl = bodyUrl;
    wrap.appendChild(bodyArea);

    var bodyErr = document.createElement('div');
    bodyErr.style.cssText = 'display:none;color:#f55;font-size:12px;margin-top:2px;';
    bodyErr.dataset.modelId = item.id;
    bodyErr.dataset.role = 'body-error';
    wrap.appendChild(bodyErr);

    return wrap;
  }

  function bindExpandSave(container, accordion) {
    var noteTimers = {};
    var bodyTimers = {};

    function syncCached(id, field, val) {
      if (!accordion) return;
      var items = accordion.getData();
      for (var i = 0; i < items.length; i++) {
        if (String(items[i].id) === String(id)) {
          items[i][field] = val;
          break;
        }
      }
    }

    container.addEventListener('input', function(e) {
      var ta = e.target;
      if (!ta.dataset || !ta.dataset.modelId) return;
      var id = ta.dataset.modelId;
      var field = ta.dataset.modelField;
      var url = ta.dataset.modelUrl;
      if (!url) return;

      if (field === 'note') {
        var val = ta.value;
        syncCached(id, 'note', val);
        clearTimeout(noteTimers[id]);
        noteTimers[id] = setTimeout(function() {
          fetch(url, {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ note: val })
          });
        }, 800);
      } else if (field === 'body') {
        syncCached(id, 'body', ta.value);
        clearTimeout(bodyTimers[id]);
        var capturedVal = ta.value;
        bodyTimers[id] = setTimeout(function() {
          _saveBody(container, ta, id, capturedVal);
        }, 800);
      }
    });

    container.addEventListener('blur', function(e) {
      var ta = e.target;
      if (!ta.dataset || !ta.dataset.modelId) return;
      if (ta.dataset.modelField !== 'body') return;
      var id = ta.dataset.modelId;
      clearTimeout(bodyTimers[id]);
      _saveBody(container, ta, id, ta.value);
    }, true);
  }

  function _saveBody(container, ta, modelId, val) {
    var errEl = container.querySelector('[data-role="body-error"][data-model-id="' + modelId + '"]');
    var parsed;
    try {
      parsed = JSON.parse(val);
      if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) throw new Error('not object');
    } catch(e) {
      ta.classList.add('input-error');
      if (errEl) { errEl.style.display = 'block'; errEl.textContent = 'Невалидный JSON'; }
      return;
    }
    ta.classList.remove('input-error');
    if (errEl) { errEl.style.display = 'none'; errEl.textContent = ''; }
    var url = ta.dataset.modelUrl;
    if (!url) return;
    fetch(url, {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ body: parsed })
    });
  }

  function makeDragHandler(container, saveOrderFn, rowSelector) {
    var dragSrcId = null;

    container.addEventListener('pointerdown', function(e) {
      var handle = e.target.closest('.model-drag-handle');
      if (!handle) return;
      var row = handle.closest(rowSelector);
      if (!row) return;
      row.setAttribute('draggable', 'true');
      var releaseHandler = function() {
        row.setAttribute('draggable', 'false');
        document.removeEventListener('pointerup', releaseHandler);
      };
      document.addEventListener('pointerup', releaseHandler, { once: true });
    });

    container.addEventListener('dragstart', function(e) {
      var row = e.target.closest(rowSelector);
      if (!row || row.getAttribute('draggable') !== 'true') return;
      dragSrcId = row.getAttribute('data-id');
      row.classList.add('dragging');
      e.dataTransfer.effectAllowed = 'move';
      e.dataTransfer.setData('text/plain', dragSrcId);
    });
    container.addEventListener('dragend', function(e) {
      var row = e.target.closest(rowSelector);
      if (row) {
        row.classList.remove('dragging');
        row.setAttribute('draggable', 'false');
      }
      container.querySelectorAll(rowSelector).forEach(function(r) {
        r.classList.remove('drag-over-top', 'drag-over-bottom');
      });
    });
    container.addEventListener('dragover', function(e) {
      var row = e.target.closest(rowSelector);
      if (!row || !dragSrcId) return;
      e.preventDefault();
      e.dataTransfer.dropEffect = 'move';
      if (dragSrcId === row.getAttribute('data-id')) return;
      container.querySelectorAll(rowSelector).forEach(function(r) {
        r.classList.remove('drag-over-top', 'drag-over-bottom');
      });
      var rect = row.getBoundingClientRect();
      if (e.clientY < rect.top + rect.height / 2) row.classList.add('drag-over-top');
      else row.classList.add('drag-over-bottom');
    });
    container.addEventListener('dragleave', function(e) {
      var row = e.target.closest(rowSelector);
      if (row) row.classList.remove('drag-over-top', 'drag-over-bottom');
    });
    container.addEventListener('drop', function(e) {
      var row = e.target.closest(rowSelector);
      if (!row || !dragSrcId || dragSrcId === row.getAttribute('data-id')) return;
      e.preventDefault();
      container.querySelectorAll(rowSelector).forEach(function(r) {
        r.classList.remove('drag-over-top', 'drag-over-bottom');
      });
      var srcEl = container.querySelector(rowSelector + '[data-id="' + dragSrcId + '"]');
      if (srcEl) {
        var rect = row.getBoundingClientRect();
        if (e.clientY < rect.top + rect.height / 2) container.insertBefore(srcEl, row);
        else container.insertBefore(srcEl, row.nextSibling);
      }
      var ids = Array.from(container.querySelectorAll(rowSelector)).map(function(r) { return r.getAttribute('data-id'); });
      dragSrcId = null;
      saveOrderFn(ids);
    });
  }

  function bindActivateBtn(container, activateFn, activeClass) {
    container.addEventListener('click', function(e) {
      var btn = e.target.closest('[data-role="activate-btn"]');
      if (!btn) return;
      e.stopPropagation();
      var id = btn.getAttribute('data-model-id');
      activateFn(id, btn, container, activeClass);
    });
  }

  function bindProbeBtn(container, probeFn) {
    container.addEventListener('click', function(e) {
      var btn = e.target.closest('[data-role="probe-btn"]');
      if (!btn) return;
      e.stopPropagation();
      var id = btn.getAttribute('data-model-id');
      var name = btn.getAttribute('data-model-name');
      probeFn(id, name, btn);
    });
  }

  /* ════════════════════════════════════════════
     VIDEO MODELS
  ════════════════════════════════════════════ */
  var _videoData = [];
  var _videoDuration = 6;
  var _videoAccordion;
  var _videoDurationListenerAdded = false;

  function getVideoDuration() {
    var el = document.getElementById('video_duration');
    var v = el ? parseInt(el.value, 10) : NaN;
    return isNaN(v) ? 6 : v;
  }

  function saveVideoOrder(ids) {
    fetch('/api/models/reorder', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ids: ids})
    }).catch(function(e) { console.error('reorder error', e); loadModels(); });
  }

  function activateVideoModel(id, btn, container, activeClass) {
    var row = btn ? btn.closest('.story-row') : null;
    fetch('/api/models/' + id + '/activate', {method: 'POST'})
      .then(function(r) { return r.json(); })
      .then(function() { loadModels(); })
      .catch(function(e) { console.error('activate error', e); loadModels(); });
    if (row) row.classList.toggle(activeClass);
    btn && btn.classList.toggle('model-radio-widget--active');
  }

  window.activateModel = function(id) {
    fetch('/api/models/' + id + '/activate', {method: 'POST'})
      .catch(function(e) { console.error('activate error', e); loadModels(); });
  };

  function initVideoAccordion() {
    var listEl = document.getElementById('model-list');
    if (!listEl || _videoAccordion) return;

    _videoAccordion = new AccordionList({
      listId: 'model-list',
      gradeUrl: function(id) { return '/api/video-models/' + encodeURIComponent(id) + '/grade'; },
      gradeLabels:    _MODEL_GRADE_LABELS,
      gradeColors:    _MODEL_GRADE_COLORS,
      gradeTextColors:_MODEL_GRADE_TEXT_COLORS,
      gradeCycle:     _MODEL_GRADE_CYCLE,
      onExpandOnRerender: true,
      rowClassFn: function(item) {
        return item.active ? 'story-row--model-active' : '';
      },
      renderTitle: function(item) {
        return item.platform_name
          ? escHtml(item.platform_name) + ': ' + escHtml(item.name)
          : escHtml(item.name);
      },
      renderButtons: function(item) {
        var dur = makeDurationIndicatorHtml(item, _videoDuration);
        var radioActive = item.active ? ' model-radio-widget--active' : '';
        var html = (dur || '');
        html += '<div class="model-radio-widget' + radioActive + '" data-role="activate-btn" data-model-id="' + item.id + '" title="Активировать/деактивировать">'
          + '<div class="model-radio-dot"></div></div>';
        if (item.price) {
          html += '<span style="font-size:11px;color:#888;padding-right:4px;white-space:nowrap;">' + escHtml(String(item.price)) + '</span>';
        }
        html += '<button class="model-probe-btn" data-role="probe-btn" data-model-id="' + item.id + '" data-model-name="' + escHtml(item.name || '') + '" title="Пробный запрос">'
          + '<svg viewBox="0 0 16 16" fill="none" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><polygon points="4,2 13,8 4,14"/></svg></button>';
        html += '<div class="model-drag-handle" title="Перетащить">⠿</div>';
        return html;
      },
      onExpand: function(item, expandEl) {
        if (!expandEl || !item) return;
        expandEl.innerHTML = '';
        var content = makeExpandContent(
          item,
          '/api/video-models/' + encodeURIComponent(item.id) + '/note',
          '/api/video-models/' + encodeURIComponent(item.id) + '/body'
        );
        expandEl.appendChild(content);
      },
      emptyHtml: '<div class="model-loading">Нет моделей</div>',
    });

    var container = document.getElementById('model-list');
    bindExpandSave(container, _videoAccordion);
    bindActivateBtn(container, activateVideoModel, 'story-row--model-active');
    bindProbeBtn(container, function(id, name, btn) {
      if (typeof window.createProbeVideo === 'function') window.createProbeVideo(id, name, btn);
    });
    makeDragHandler(container, saveVideoOrder, '.story-row');
  }

  function refreshDurationIndicators() {
    _videoDuration = getVideoDuration();
    if (_videoAccordion && _videoData.length) {
      _videoAccordion.render(_videoData);
    }
  }

  function attachVideoDurationListener() {
    if (_videoDurationListenerAdded) return;
    var durationEl = document.getElementById('video_duration');
    if (!durationEl) return;
    _videoDurationListenerAdded = true;
    durationEl.addEventListener('input', function() {
      refreshDurationIndicators();
    });
  }

  window.attachVideoDurationListener = attachVideoDurationListener;

  function loadModels() {
    fetch('/api/models')
      .then(function(r) { return r.json(); })
      .then(function(data) {
        _videoData = data || [];
        _videoDuration = getVideoDuration();
        initVideoAccordion();
        if (_videoAccordion) _videoAccordion.render(_videoData);
        attachVideoDurationListener();
      })
      .catch(function() {
        var c = document.getElementById('model-list');
        if (c) c.innerHTML = '<div class="model-loading">Ошибка загрузки</div>';
      });
  }

  window.loadModels = loadModels;

  (function() {
    var panel = document.getElementById('panel-request');
    if (panel && panel.classList.contains('active')) loadModels();
  })();

  /* ════════════════════════════════════════════
     TEXT MODELS
  ════════════════════════════════════════════ */
  var _textData = [];
  var _textAccordion;

  function saveTextOrder(ids) {
    fetch('/api/text-models/reorder', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ids: ids})
    }).catch(function(e) { console.error('text reorder error', e); loadTextModels(); });
  }

  function activateTextModelFn(id, btn, container, activeClass) {
    fetch('/api/text-models/' + id + '/activate', {method: 'POST'})
      .then(function() { loadTextModels(); })
      .catch(function(e) { console.error('text activate error', e); loadTextModels(); });
    var row = btn ? btn.closest('.story-row') : null;
    if (row) row.classList.toggle(activeClass);
    if (btn) btn.classList.toggle('model-radio-widget--active');
  }

  window.activateTextModel = function(id) {
    fetch('/api/text-models/' + id + '/activate', {method: 'POST'})
      .catch(function(e) { console.error('text activate error', e); loadTextModels(); });
  };

  function initTextAccordion() {
    var listEl = document.getElementById('text-model-list');
    if (!listEl || _textAccordion) return;

    _textAccordion = new AccordionList({
      listId: 'text-model-list',
      gradeUrl: function(id) { return '/api/text-models/' + encodeURIComponent(id) + '/grade'; },
      gradeLabels:    _MODEL_GRADE_LABELS,
      gradeColors:    _MODEL_GRADE_COLORS,
      gradeTextColors:_MODEL_GRADE_TEXT_COLORS,
      gradeCycle:     _MODEL_GRADE_CYCLE,
      onExpandOnRerender: true,
      rowClassFn: function(item) {
        return item.active ? 'story-row--model-active' : '';
      },
      renderTitle: function(item) {
        var caption = item.platform_name
          ? escHtml(item.platform_name) + ': ' + escHtml(item.name)
          : escHtml(item.name);
        return caption;
      },
      renderButtons: function(item) {
        var radioActive = item.active ? ' model-radio-widget--active' : '';
        var html = '<div class="model-radio-widget' + radioActive + '" data-role="activate-btn" data-model-id="' + item.id + '" title="Активировать/деактивировать">'
          + '<div class="model-radio-dot"></div></div>';
        html += '<button class="model-probe-btn" data-role="probe-btn" data-model-id="' + item.id + '" data-model-name="' + escHtml(item.name || '') + '" title="Пробный запрос">'
          + '<svg viewBox="0 0 16 16" fill="none" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><polygon points="4,2 13,8 4,14"/></svg></button>';
        html += '<div class="model-drag-handle" title="Перетащить">⠿</div>';
        return html;
      },
      onExpand: function(item, expandEl) {
        if (!expandEl || !item) return;
        expandEl.innerHTML = '';
        var content = makeExpandContent(
          item,
          '/api/text-models/' + encodeURIComponent(item.id) + '/note',
          '/api/text-models/' + encodeURIComponent(item.id) + '/body'
        );
        expandEl.appendChild(content);
      },
      emptyHtml: '<div class="model-loading">Нет моделей</div>',
    });

    var container = document.getElementById('text-model-list');
    bindExpandSave(container, _textAccordion);
    bindActivateBtn(container, activateTextModelFn, 'story-row--model-active');
    bindProbeBtn(container, function(id, name, btn) {
      if (typeof window.probeTextModel === 'function') window.probeTextModel(id, name, btn);
    });
    makeDragHandler(container, saveTextOrder, '.story-row');
  }

  function loadTextModels() {
    fetch('/api/text-models')
      .then(function(r) { return r.json(); })
      .then(function(data) {
        _textData = data || [];
        initTextAccordion();
        if (_textAccordion) _textAccordion.render(_textData);
      })
      .catch(function() {
        var c = document.getElementById('text-model-list');
        if (c) c.innerHTML = '<div class="model-loading">Ошибка загрузки</div>';
      });
  }

  window.loadTextModels = loadTextModels;

  (function() {
    var panel = document.getElementById('panel-story');
    if (panel && panel.classList.contains('active')) loadTextModels();
  })();
})();
