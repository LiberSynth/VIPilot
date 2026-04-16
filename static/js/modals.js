(function() {
  function _buildVideoModalHTML(modelName) {
    var el = document.createElement('div');
    el.id = 'video-modal-overlay';
    el.className = 'video-modal-overlay open';
    el.innerHTML =
      '<div class="video-modal" onclick="event.stopPropagation()">' +
        '<div class="video-modal-head">' +
          '<span class="video-modal-title" id="video-modal-title"></span>' +
          '<button class="story-modal-close" onclick="closeVideoModal()">&times;</button>' +
        '</div>' +
        '<div class="video-modal-body" id="video-modal-body"></div>' +
      '</div>';
    var titleEl = el.querySelector('#video-modal-title');
    if (titleEl) titleEl.textContent = modelName ? 'Видео · ' + modelName : 'Видео';
    return el;
  }

  window._closeVideoModalInner = function() {
    var overlay = document.getElementById('video-modal-overlay');
    if (overlay) overlay.remove();
    document.body.style.overflow = '';
  };

  window.openVideoModal = function(batchId, modelName) {
    var existing = document.getElementById('video-modal-overlay');
    if (existing) existing.remove();

    var overlay = _buildVideoModalHTML(modelName);
    document.body.appendChild(overlay);

    var body = document.getElementById('video-modal-body');
    if (!body) return;

    body.innerHTML = '<div class="video-modal-loading">Загрузка…</div>';
    document.body.style.overflow = 'hidden';
    history.pushState({ modal: 'video' }, '');
    var src = '/api/batch/' + encodeURIComponent(batchId) + '/video';
    body.innerHTML = '<video controls autoplay src="' + src + '"></video>';

    overlay.addEventListener('click', function(e) {
      if (e.target === overlay) closeVideoModal();
    });
  };

  window.closeVideoModal = function() {
    var overlay = document.getElementById('video-modal-overlay');
    if (overlay) history.back();
  };

  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
      var overlay = document.getElementById('video-modal-overlay');
      if (overlay) history.back();
    }
  });
})();

(function() {
  var _probeBtn = null;

  var _SVG_COPY = '<svg viewBox="0 0 16 16" fill="none" stroke="#8888b0" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="5" width="9" height="9" rx="1.5"/><path d="M3 11V3a1 1 0 0 1 1-1h8"/></svg>';
  var _SVG_EXPORT = window.EXPORT_STORY_SVG || '';

  function _esc(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

  function _flashCopied(btn) { window.flashCopied(btn); }

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

  window.exportProbeModal = function(btn) {
    var overlay = document.getElementById('probe-modal-overlay');
    var storyId = overlay ? (overlay.dataset.storyId || '') : '';
    window.exportStory(storyId, btn);
  };

  function _buildProbeModalHTML(modelName, isVideo) {
    var exportBtn = isVideo ? '' : '<button id="probe-export-btn" class="cycle-float-btn" title="Выгрузка" onclick="exportProbeModal(this)" style="display:none">' + _SVG_EXPORT + '</button>';
    var el = document.createElement('div');
    el.id = 'probe-modal-overlay';
    el.className = 'probe-modal-overlay open';
    el.innerHTML =
      '<div class="probe-modal" onclick="event.stopPropagation()">' +
        '<div class="probe-modal-head">' +
          '<span class="probe-modal-title" id="probe-modal-title">Пробный запрос · ' + _esc(modelName) + '</span>' +
          '<div class="probe-modal-hdr-actions">' +
            exportBtn +
            '<button class="cycle-float-btn" title="Скопировать логи" onclick="copyProbeModalLogs(this)">' + _SVG_COPY + '</button>' +
            '<button class="cycle-float-btn" title="Скопировать инфо" onclick="copyProbeModalInfo(this)">' +
              '<svg viewBox="0 0 16 16" fill="none" stroke="#8888b0" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><circle cx="8" cy="8" r="6"/><line x1="8" y1="7" x2="8" y2="11"/><circle cx="8" cy="5" r=".5" fill="#8888b0" stroke="none"/></svg>' +
            '</button>' +
          '</div>' +
          '<button class="probe-modal-close" onclick="closeProbeModal()">&times;</button>' +
        '</div>' +
        '<div class="probe-modal-body" id="probe-modal-body"><span style="color:#888">Создаю батч…</span></div>' +
      '</div>';
    return el;
  }

  function _openProbeOverlay(modelName, isVideo) {
    var existing = document.getElementById('probe-modal-overlay');
    if (existing) existing.remove();

    var overlay = _buildProbeModalHTML(modelName, isVideo);
    document.body.appendChild(overlay);
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
        }, btn);
      })
      .catch(function(e) {
        if (_probeBtn) { _probeBtn.classList.remove('probing'); _probeBtn = null; }
        body.className   = 'probe-modal-body probe-modal-error';
        body.textContent = 'Ошибка запроса: ' + e;
      });
  };

  function _showStoryResult(storyId, body) {
    var overlay = document.getElementById('probe-modal-overlay');
    if (overlay && overlay.dataset) overlay.dataset.storyId = storyId;
    var exportBtn = document.getElementById('probe-export-btn');
    if (exportBtn) exportBtn.style.display = '';
    fetch('/api/story/' + encodeURIComponent(storyId))
      .then(function(r) { return r.json(); })
      .then(function(d) {
        if (!d.text) return;
        var sep = document.createElement('div');
        sep.className = 'probe-story-sep';

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
          var copyText = d.title ? d.title + '\n\n' + d.text : d.text;
          navigator.clipboard.writeText(copyText).then(function() { _flashCopied(copyBtn); }).catch(function() {});
        };
        hdr.appendChild(lbl);
        hdr.appendChild(copyBtn);

        if (d.title) {
          var titleEl = document.createElement('div');
          titleEl.className = 'probe-story-title';
          titleEl.textContent = d.title;
          wrap.appendChild(hdr);
          wrap.appendChild(titleEl);
        } else {
          wrap.appendChild(hdr);
        }

        var pre = document.createElement('div');
        pre.className = 'probe-story-text';
        pre.textContent = d.text;

        wrap.appendChild(pre);
        body.prepend(sep);
        body.prepend(wrap);
        body.scrollTop = 0;
      })
      .catch(function() {});
  }

  window.createProbeVideo = function(modelId, modelName, btn) {
    if (!_openProbeOverlay(modelName, true)) return;
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
        _doBatchLogPoll(data.batch_id, body, null, btn);
      })
      .catch(function(e) {
        if (_probeBtn) { _probeBtn.classList.remove('probing'); _probeBtn = null; }
        body.className   = 'probe-modal-body probe-modal-error';
        body.textContent = 'Ошибка запроса: ' + e;
      });
  };

  function _doBatchLogPoll(batchId, body, onDone, btn) {
    var _TERMINAL = ['probe', 'movie_probe', 'story_probe', 'video_error', 'transcode_error', 'publish_error', 'cancelled', 'error'];

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

      var nearBottom = body.scrollHeight - body.scrollTop - body.clientHeight < 60;
      if (nearBottom) body.scrollTop = body.scrollHeight;
    }

    function showVideo() {
      var src = '/api/batch/' + encodeURIComponent(batchId) + '/video';
      videoSection.innerHTML    = '<video class="probe-video" controls autoplay src="' + src + '"></video>';
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
            if (btn) { btn.classList.remove('probing'); btn = null; }
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

  window._doBatchLogPoll = _doBatchLogPoll;

  window.closeProbeModal = function() {
    var overlay = document.getElementById('probe-modal-overlay');
    if (overlay) overlay.remove();
    document.body.style.overflow = '';
    if (_probeBtn) { _probeBtn.classList.remove('probing'); _probeBtn = null; }
  };

  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
      var overlay = document.getElementById('probe-modal-overlay');
      if (overlay) closeProbeModal();
    }
  });
})();

(function() {
  var _storyText = '';

  function _buildStoryModalHTML(modelName) {
    var el = document.createElement('div');
    el.id = 'story-modal-overlay';
    el.className = 'story-modal-overlay open';
    el.innerHTML =
      '<div class="story-modal" onclick="event.stopPropagation()">' +
        '<div class="story-modal-head">' +
          '<span class="story-modal-title" id="story-modal-title"></span>' +
          '<button class="cycle-float-btn" id="story-modal-copy-btn" title="Скопировать" onclick="copyStoryText()">' +
            '<svg viewBox="0 0 16 16" fill="none" stroke="#8888b0" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="5" width="9" height="9" rx="1.5"/><path d="M3 11V3a1 1 0 0 1 1-1h8"/></svg>' +
          '</button>' +
          '<button class="story-modal-close" onclick="closeStoryModal()">&times;</button>' +
        '</div>' +
        '<div class="story-modal-body" id="story-modal-body"><span class="story-modal-loading">Загрузка…</span></div>' +
      '</div>';
    var titleEl = el.querySelector('#story-modal-title');
    if (titleEl) titleEl.textContent = modelName ? 'Сюжет · ' + modelName : 'Сюжет';
    return el;
  }

  window.openStoryModal = function(storyId, modelName) {
    var existing = document.getElementById('story-modal-overlay');
    if (existing) existing.remove();

    _storyText = '';
    var overlay = _buildStoryModalHTML(modelName);
    document.body.appendChild(overlay);

    document.body.style.overflow = 'hidden';
    history.pushState({ modal: 'story' }, '');

    var body = document.getElementById('story-modal-body');
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
    if (overlay) overlay.remove();
    document.body.style.overflow = '';
    _storyText = '';
  };

  window.closeStoryModal = function() {
    var overlay = document.getElementById('story-modal-overlay');
    if (overlay) history.back();
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
      if (overlay) history.back();
    }
  });

  window.addEventListener('popstate', function(e) {
    var state = e.state || {};
    if (state.modal === 'video') {
      _closeVideoModalInner && _closeVideoModalInner();
    } else {
      var storyOverlay = document.getElementById('story-modal-overlay');
      var videoOverlay = document.getElementById('video-modal-overlay');
      if (storyOverlay) {
        _closeStoryModalInner && _closeStoryModalInner();
      } else if (videoOverlay) {
        _closeVideoModalInner && _closeVideoModalInner();
      }
    }
  });
})();
