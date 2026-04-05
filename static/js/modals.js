(function() {
  window._closeVideoModalInner = function() {
    var overlay = document.getElementById('video-modal-overlay');
    var body    = document.getElementById('video-modal-body');
    if (overlay) overlay.classList.remove('open');
    if (body)    body.innerHTML = '';
    document.body.style.overflow = '';
  };

  window.openVideoModal = function(batchId, modelName) {
    var overlay = document.getElementById('video-modal-overlay');
    var body    = document.getElementById('video-modal-body');
    var title   = document.getElementById('video-modal-title');
    if (!overlay || !body) return;
    if (title) title.textContent = modelName ? 'Видео · ' + modelName : 'Видео';
    body.innerHTML = '<div class="video-modal-loading">Загрузка…</div>';
    overlay.classList.add('open');
    document.body.style.overflow = 'hidden';
    history.pushState({ modal: 'video' }, '');
    var src = '/api/batch/' + encodeURIComponent(batchId) + '/video';
    body.innerHTML = '<video controls autoplay src="' + src + '"></video>';
  };

  window.closeVideoModal = function() {
    var overlay = document.getElementById('video-modal-overlay');
    if (overlay && overlay.classList.contains('open')) history.back();
  };

  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
      var overlay = document.getElementById('video-modal-overlay');
      if (overlay && overlay.classList.contains('open')) history.back();
    }
  });
})();

(function() {
  var _probeBtn = null;

  var _SVG_COPY = '<svg viewBox="0 0 16 16" fill="none" stroke="#8888b0" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="5" width="9" height="9" rx="1.5"/><path d="M3 11V3a1 1 0 0 1 1-1h8"/></svg>';
  var _SVG_INFO = '<svg viewBox="0 0 16 16" fill="none" stroke="#8888b0" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><circle cx="8" cy="8" r="6"/><line x1="8" y1="7" x2="8" y2="11"/><circle cx="8" cy="5" r=".5" fill="#8888b0" stroke="none"/></svg>';

  function _esc(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

  function _flashCopied(btn) {
    btn.classList.add('copied');
    setTimeout(function() { btn.classList.remove('copied'); }, 2000);
  }

  window.copyProbeModalLogs = function(btn) {
    var body = document.getElementById('probe-modal-body');
    if (!body) return;
    var logList = body.querySelector('.monitor-log-list');
    var text = (logList || body).innerText || (logList || body).textContent || '';
    navigator.clipboard.writeText(text.trim()).then(function() { _flashCopied(btn); }).catch(function() {});
  };

  window.copyProbeModalInfo = function(btn) {
    var overlay = document.getElementById('probe-modal-overlay');
    var title   = document.getElementById('probe-modal-title');
    var batchId = overlay ? (overlay.dataset.batchId || '') : '';
    var lines = [];
    if (title) lines.push(title.textContent);
    if (batchId) lines.push('batch: ' + batchId);
    navigator.clipboard.writeText(lines.join('\n')).then(function() { _flashCopied(btn); }).catch(function() {});
  };

  function _openProbeOverlay(modelName) {
    var overlay = document.getElementById('probe-modal-overlay');
    var body    = document.getElementById('probe-modal-body');
    var info    = document.getElementById('probe-modal-info');
    var title   = document.getElementById('probe-modal-title');
    if (!overlay || !body) return false;
    if (title) title.textContent = 'Пробный запрос · ' + modelName;
    body.className     = 'probe-modal-body';
    body.style.padding = '';
    body.innerHTML     = '<span style="color:#888">Создаю батч…</span>';
    if (info) info.textContent = '';
    if (overlay.dataset) overlay.dataset.batchId = '';
    overlay.classList.add('open');
    document.body.style.overflow = 'hidden';
    return true;
  }

  window.probeTextModel = function(modelId, modelName, btn) {
    if (!_openProbeOverlay(modelName)) return;
    var body = document.getElementById('probe-modal-body');

    if (_probeBtn) _probeBtn.classList.remove('probing');
    _probeBtn = btn;
    btn.classList.add('probing');

    fetch('/api/text-models/' + encodeURIComponent(modelId) + '/probe', { method: 'POST' })
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (data.error) {
          if (_probeBtn) { _probeBtn.classList.remove('probing'); _probeBtn = null; }
          body.innerHTML = '<span style="color:#c05050">' + _esc(data.error) + '</span>';
          return;
        }
        var overlay = document.getElementById('probe-modal-overlay');
        if (overlay && overlay.dataset) overlay.dataset.batchId = data.batch_id;
        body.innerHTML = '';
        _doBatchLogPoll(data.batch_id, body, function(d) {
          if (d.story_id) _showStoryResult(d.story_id, body);
        });
      })
      .catch(function(e) {
        if (_probeBtn) { _probeBtn.classList.remove('probing'); _probeBtn = null; }
        body.className   = 'probe-modal-body probe-modal-error';
        body.textContent = 'Ошибка запроса: ' + e;
      });
  };

  function _showStoryResult(storyId, body) {
    fetch('/api/story/' + encodeURIComponent(storyId))
      .then(function(r) { return r.json(); })
      .then(function(d) {
        if (!d.text) return;
        var sep = document.createElement('div');
        sep.className = 'probe-story-sep';
        body.appendChild(sep);

        var wrap = document.createElement('div');
        wrap.className = 'probe-story-result';

        var hdr = document.createElement('div');
        hdr.className = 'probe-story-hdr';
        var lbl = document.createElement('span');
        lbl.className = 'probe-story-label';
        lbl.textContent = 'Сюжет';
        var copyBtn = document.createElement('button');
        copyBtn.className = 'cycle-float-btn';
        copyBtn.title = 'Скопировать';
        copyBtn.innerHTML = _SVG_COPY;
        copyBtn.onclick = function() {
          navigator.clipboard.writeText(d.text).then(function() { _flashCopied(copyBtn); }).catch(function() {});
        };
        hdr.appendChild(lbl);
        hdr.appendChild(copyBtn);

        var pre = document.createElement('div');
        pre.className = 'probe-story-text';
        pre.textContent = d.text;

        wrap.appendChild(hdr);
        wrap.appendChild(pre);
        body.appendChild(wrap);
        body.scrollTop = body.scrollHeight;
      })
      .catch(function() {});
  }

  window.probeVideoModel = function(modelId, modelName, btn) {
    if (!_openProbeOverlay(modelName)) return;
    var body = document.getElementById('probe-modal-body');
    var overlay = document.getElementById('probe-modal-overlay');

    if (_probeBtn) _probeBtn.classList.remove('probing');
    _probeBtn = btn;
    btn.classList.add('probing');

    fetch('/api/video-models/' + encodeURIComponent(modelId) + '/probe', { method: 'POST' })
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (data.error) {
          if (_probeBtn) { _probeBtn.classList.remove('probing'); _probeBtn = null; }
          body.innerHTML = '<span style="color:#c05050">' + _esc(data.error) + '</span>';
          return;
        }
        if (overlay && overlay.dataset) overlay.dataset.batchId = data.batch_id;
        body.innerHTML = '';
        _doBatchLogPoll(data.batch_id, body, null);
      })
      .catch(function(e) {
        if (_probeBtn) { _probeBtn.classList.remove('probing'); _probeBtn = null; }
        body.className   = 'probe-modal-body probe-modal-error';
        body.textContent = 'Ошибка запроса: ' + e;
      });
  };

  function _doBatchLogPoll(batchId, body, onDone) {
    var _TERMINAL = ['probe', 'story_probe', 'video_error', 'transcode_error', 'publish_error', 'отменён'];

    var videoSection = document.createElement('div');
    videoSection.className    = 'probe-modal-video-section';
    videoSection.style.display = 'none';
    body.appendChild(videoSection);

    var container = document.createElement('div');
    container.className  = 'monitor-log-list';
    body.style.padding   = '0';
    body.style.minHeight = '80px';
    body.appendChild(container);

    var seenIds = {};

    function render(d) {
      var openIds = {};
      container.querySelectorAll('.monitor-log-item.open').forEach(function(el) {
        openIds[el.dataset.lid] = true;
      });

      var groups = groupLogsByPipeline(d.logs || []);
      container.innerHTML = groups.map(function(log) {
        return renderLogItem(
          log,
          d.batch_id,
          d.story_id,
          false,
          d.text_model_name,
          d.video_model_name
        );
      }).join('');

      container.querySelectorAll('.monitor-log-item').forEach(function(el) {
        var lid = el.dataset.lid;
        var hasEntries = !!el.querySelector('.monitor-entries');
        if (openIds[lid]) {
          el.classList.add('open');
          seenIds[lid] = true;
        } else if (!seenIds[lid] && hasEntries) {
          el.classList.add('open');
          seenIds[lid] = true;
        }
      });

      body.scrollTop = body.scrollHeight;
    }

    function showVideo() {
      var src = '/api/batch/' + encodeURIComponent(batchId) + '/video';
      videoSection.innerHTML    = '<video controls autoplay src="' + src + '"></video>';
      videoSection.style.display = 'block';
    }

    function poll() {
      fetch('/api/batch/' + batchId + '/logs')
        .then(function(r) { return r.json(); })
        .then(function(d) {
          if (d.error) { setTimeout(poll, 2000); return; }
          render(d);
          var status = d.batch_status;
          if (_TERMINAL.indexOf(status) !== -1) {
            if (_probeBtn) { _probeBtn.classList.remove('probing'); _probeBtn = null; }
            container.querySelectorAll('.monitor-log-item.open').forEach(function(el) {
              el.classList.remove('open');
            });
            body.scrollTop = 0;
            if (onDone) { onDone(d); }
            else if (d.has_video_data) { showVideo(); }
          } else {
            setTimeout(poll, 2000);
          }
        })
        .catch(function() { setTimeout(poll, 3000); });
    }
    poll();
  }

  window.closeProbeModal = function() {
    var overlay = document.getElementById('probe-modal-overlay');
    if (overlay) overlay.classList.remove('open');
    document.body.style.overflow = '';
    if (_probeBtn) { _probeBtn.classList.remove('probing'); _probeBtn = null; }
  };

  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
      var overlay = document.getElementById('probe-modal-overlay');
      if (overlay && overlay.classList.contains('open')) closeProbeModal();
    }
  });
})();

(function() {
  var _storyText = '';

  window.openStoryModal = function(storyId, modelName) {
    var overlay = document.getElementById('story-modal-overlay');
    var body    = document.getElementById('story-modal-body');
    var copyBtn = document.getElementById('story-modal-copy-btn');
    var title   = document.getElementById('story-modal-title');
    if (!overlay || !body) return;
    if (title) title.textContent = modelName ? 'Сюжет · ' + modelName : 'Сюжет';
    _storyText    = '';
    body.innerHTML = '<span class="story-modal-loading">Загрузка…</span>';
    copyBtn.classList.remove('copied');
    overlay.classList.add('open');
    document.body.style.overflow = 'hidden';
    history.pushState({ modal: 'story' }, '');
    fetch('/api/story/' + encodeURIComponent(storyId))
      .then(function(r) { return r.json(); })
      .then(function(d) {
        if (d.text) {
          _storyText     = d.text;
          body.textContent = d.text;
        } else {
          body.innerHTML = '<span class="story-modal-loading">Сюжет не найден</span>';
        }
      })
      .catch(function() {
        body.innerHTML = '<span class="story-modal-loading">Ошибка загрузки</span>';
      });
  };

  window._closeStoryModalInner = function() {
    var overlay = document.getElementById('story-modal-overlay');
    if (overlay) overlay.classList.remove('open');
    document.body.style.overflow = '';
  };

  window.closeStoryModal = function() {
    var overlay = document.getElementById('story-modal-overlay');
    if (overlay && overlay.classList.contains('open')) history.back();
  };

  window.copyStoryText = function() {
    if (!_storyText) return;
    var btn = document.getElementById('story-modal-copy-btn');
    navigator.clipboard.writeText(_storyText).then(function() {
      if (btn) btn.classList.add('copied');
      setTimeout(function() {
        if (btn) btn.classList.remove('copied');
      }, 2000);
    }).catch(function() {});
  };

  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
      var overlay = document.getElementById('story-modal-overlay');
      if (overlay && overlay.classList.contains('open')) history.back();
    }
  });

  window.addEventListener('popstate', function(e) {
    var state = e.state || {};
    if (state.modal === 'video') {
      _closeVideoModalInner && _closeVideoModalInner();
    } else {
      var storyOverlay = document.getElementById('story-modal-overlay');
      var videoOverlay = document.getElementById('video-modal-overlay');
      if (storyOverlay && storyOverlay.classList.contains('open')) {
        _closeStoryModalInner && _closeStoryModalInner();
      } else if (videoOverlay && videoOverlay.classList.contains('open')) {
        _closeVideoModalInner && _closeVideoModalInner();
      }
    }
  });
})();
