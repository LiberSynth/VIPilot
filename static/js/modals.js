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

  window.probeTextModel = function(modelId, modelName, btn) {
    var overlay = document.getElementById('probe-modal-overlay');
    var body    = document.getElementById('probe-modal-body');
    var info    = document.getElementById('probe-modal-info');
    var title   = document.getElementById('probe-modal-title');
    if (!overlay || !body) return;

    if (_probeBtn) _probeBtn.classList.remove('probing');
    _probeBtn = btn;
    btn.classList.add('probing');

    if (title) title.textContent = 'Пробный запрос · ' + modelName;
    body.className    = 'probe-modal-body';
    body.style.padding = '';
    body.innerHTML    = '<span style="color:#888">Выполняю запрос…</span>';
    if (info) info.textContent = '';
    overlay.classList.add('open');
    document.body.style.overflow = 'hidden';

    fetch('/api/text-models/' + encodeURIComponent(modelId) + '/probe', { method: 'POST' })
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (data.error) {
          if (_probeBtn) { _probeBtn.classList.remove('probing'); _probeBtn = null; }
          body.innerHTML = '<span style="color:#c05050">' + String(data.error).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;') + '</span>';
          return;
        }
        _doProbePoll(data.job_id, '/api/text-models/probe/', btn, body);
      })
      .catch(function(e) {
        if (_probeBtn) { _probeBtn.classList.remove('probing'); _probeBtn = null; }
        body.className  = 'probe-modal-body probe-modal-error';
        body.textContent = 'Ошибка запроса: ' + e;
        if (info) info.textContent = '';
      });
  };

  function _doProbePoll(jobId, pollUrl, btn, body, onResult) {
    var cursor = 0;
    body.innerHTML = '';

    var levelColor = { info: '#888', warn: '#c09050', ok: '#6a6', error: '#c05050' };

    function appendEvent(line) {
      var color = levelColor[line.level] || '#888';
      var div   = document.createElement('div');
      div.style.cssText = 'color:' + color + ';margin-bottom:3px;font-size:12px;line-height:1.4';
      div.textContent   = line.text;
      body.appendChild(div);
      body.scrollTop = body.scrollHeight;
    }

    function showResult(text) {
      if (onResult) {
        onResult(text);
      } else {
        var sep = document.createElement('div');
        sep.style.cssText = 'border-top:1px solid rgba(255,255,255,.1);margin:10px 0 8px';
        body.appendChild(sep);
        var pre = document.createElement('div');
        pre.style.cssText = 'white-space:pre-wrap;line-height:1.55;font-size:13px';
        pre.textContent   = text;
        body.appendChild(pre);
        body.scrollTop = body.scrollHeight;
      }
    }

    function poll() {
      fetch(pollUrl + jobId + '?cursor=' + cursor)
        .then(function(r) { return r.json(); })
        .then(function(d) {
          (d.events || []).forEach(appendEvent);
          cursor = d.cursor || cursor;
          if (d.done) {
            if (_probeBtn) { _probeBtn.classList.remove('probing'); _probeBtn = null; }
            if (d.result) showResult(d.result);
          } else {
            setTimeout(poll, 500);
          }
        })
        .catch(function() { setTimeout(poll, 1000); });
    }
    poll();
  }

  window.probeVideoModel = function(modelId, modelName, btn) {
    var overlay = document.getElementById('probe-modal-overlay');
    var body    = document.getElementById('probe-modal-body');
    var info    = document.getElementById('probe-modal-info');
    var title   = document.getElementById('probe-modal-title');
    if (!overlay || !body) return;

    function _esc(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

    if (_probeBtn) _probeBtn.classList.remove('probing');
    _probeBtn = btn;
    btn.classList.add('probing');

    if (title) title.textContent = 'Пробный запрос · ' + modelName;
    body.className    = 'probe-modal-body';
    body.style.padding = '';
    body.innerHTML    = '<span style="color:#888">Создаю батч…</span>';
    if (info) info.textContent = '';
    overlay.classList.add('open');
    document.body.style.overflow = 'hidden';

    fetch('/api/video-models/' + encodeURIComponent(modelId) + '/probe', { method: 'POST' })
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (data.error) {
          if (_probeBtn) { _probeBtn.classList.remove('probing'); _probeBtn = null; }
          body.innerHTML = '<span style="color:#c05050">' + _esc(data.error) + '</span>';
          return;
        }
        body.innerHTML = '';
        _doBatchLogPoll(data.batch_id, body);
      })
      .catch(function(e) {
        if (_probeBtn) { _probeBtn.classList.remove('probing'); _probeBtn = null; }
        body.className  = 'probe-modal-body probe-modal-error';
        body.textContent = 'Ошибка запроса: ' + e;
        if (info) info.textContent = '';
      });
  };

  function _doBatchLogPoll(batchId, body) {
    var _TERMINAL = ['probe', 'video_error', 'transcode_error', 'publish_error', 'отменён'];

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
            if (d.has_video_data) showVideo();
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
