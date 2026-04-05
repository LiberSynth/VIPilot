var _gradeSequence = ['good', 'limited', 'poor', 'fallback', 'rejected'];
var _gradeColors   = { good: '#4a8', limited: '#a84', poor: '#b60', fallback: '#a33', rejected: '#666' };

window.cycleGrade = function(el) {
  var id   = el.getAttribute('data-grade-id');
  var cur  = el.getAttribute('data-grade') || 'good';
  var next = _gradeSequence[(_gradeSequence.indexOf(cur) + 1) % _gradeSequence.length];
  el.setAttribute('data-grade', next);
  el.textContent = next;
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
  el.textContent = next;
  el.style.background = _gradeColors[next] || '#555';
  fetch('/api/video-models/' + encodeURIComponent(id) + '/grade', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ grade: next })
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

  function renderVideoModels(list) {
    const container = document.getElementById('model-list');
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
      var gradeHtml = '<span data-grade-id="' + m.id + '" data-grade="' + grade + '" onclick="event.stopPropagation();cycleVideoGrade(this)" title="Нажмите для смены" style="cursor:pointer;font-size:10px;padding:1px 6px;border-radius:3px;background:' + (_gradeColors[grade]||'#555') + ';color:#fff;margin-left:6px;opacity:.85">' + grade + '</span>';
      var priceHtml = m.price ? '<span style="font-size:10px;color:#888;margin-left:8px">' + escHtml(m.price) + '</span>' : '';
      item.innerHTML =
        '<div class="model-radio" onclick="activateModel(\'' + m.id + '\')">' +
          '<div class="model-radio-dot"></div>' +
        '</div>' +
        '<div class="model-name">' + caption + gradeHtml + priceHtml + '</div>' +
        '<button class="model-probe-btn" title="Пробный запрос" onclick="event.stopPropagation();probeVideoModel(\'' + m.id + '\',\'' + escHtml(m.name) + '\',this)"><svg viewBox="0 0 16 16" fill="none" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><polygon points="4,2 13,8 4,14"/></svg></button>' +
        '<div class="model-drag-handle" title="Перетащить">⠿</div>';
      makeDragHandlers(item, 'model-list', m, saveVideoOrder);
      container.appendChild(item);
    });
  }

  function saveVideoOrder(ids) {
    fetch('/api/models/reorder', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ids: ids})
    }).catch(function(e) { console.error('reorder error', e); loadModels(); });
  }

  window.activateModel = function(id) {
    var container = document.getElementById('model-list');
    if (container) {
      var item = container.querySelector('[data-id="' + id + '"]');
      if (item) item.classList.toggle('model-active');
    }
    fetch('/api/models/' + id + '/activate', {method: 'POST'})
      .catch(function(e) { console.error('activate error', e); loadModels(); });
  };

  function loadModels() {
    fetch('/api/models')
      .then(function(r) { return r.json(); })
      .then(function(data) { renderVideoModels(data); })
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
      var gradeHtml = '<span data-grade-id="' + m.id + '" data-grade="' + grade + '" onclick="event.stopPropagation();cycleGrade(this)" title="Нажмите для смены" style="cursor:pointer;font-size:10px;padding:1px 6px;border-radius:3px;background:' + (_gradeColors[grade]||'#555') + ';color:#fff;margin-left:6px;opacity:.85">' + grade + '</span>';
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
