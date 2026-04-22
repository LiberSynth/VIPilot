(function() {
  function _esc(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
  function _flashCopied(btn) { window.flashCopied(btn); }
  var _SVG_COPY = '<svg viewBox="0 0 16 16" fill="none" stroke="#8888b0" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="5" width="9" height="9" rx="1.5"/><path d="M3 11V3a1 1 0 0 1 1-1h8"/></svg>';

  // ── VideoModal ─────────────────────────────────────────────────────────────
  class VideoModal extends Dialog {
    overlayClass() { return 'video-modal-overlay'; }
    overlayId()    { return 'video-modal-overlay'; }

    open(batchId, modelName) {
      var existing = document.getElementById(this.overlayId());
      if (existing) existing.remove();

      this._el = document.createElement('div');
      this._el.id        = this.overlayId();
      this._el.className = this.overlayClass() + ' open';
      this._el.innerHTML =
        '<div class="video-modal" onclick="event.stopPropagation()">' +
          '<div class="video-modal-head">' +
            '<span class="video-modal-title" id="video-modal-title"></span>' +
            '<button class="story-modal-close" id="_vm-close">&times;</button>' +
          '</div>' +
          '<div class="video-modal-body" id="video-modal-body"></div>' +
        '</div>';
      var titleEl = this._el.querySelector('#video-modal-title');
      if (titleEl) titleEl.textContent = modelName ? 'Видео · ' + modelName : 'Видео';
      document.body.appendChild(this._el);
      this._setupKeyboard();

      var body = document.getElementById('video-modal-body');
      if (!body) return this;
      document.body.style.overflow = 'hidden';
      history.pushState({ modal: 'video' }, '');
      body.innerHTML = '<video controls autoplay src="/api/batch/' + encodeURIComponent(batchId) + '/video"></video>';

      var self = this;
      this._el.addEventListener('click', function(e) {
        if (e.target === self._el) self.close();
      });
      var closeBtn = document.getElementById('_vm-close');
      if (closeBtn) closeBtn.addEventListener('click', function() { self.close(); });
      return this;
    }

    close() {
      if (document.getElementById(this.overlayId())) history.back();
    }

    closeInner() {
      document.body.style.overflow = '';
      this._removeEl();
    }
  }

  var _videoModal = new VideoModal();

  window.openVideoModal          = function(batchId, modelName) { _videoModal.open(batchId, modelName); };
  window.closeVideoModal         = function() { _videoModal.close(); };
  window._closeVideoModalInner   = function() { _videoModal.closeInner(); };

  // ── ProbeModal ─────────────────────────────────────────────────────────────
  class ProbeModal extends Dialog {
    constructor() {
      super();
      this._probeBtn = null;
    }

    overlayClass() { return 'probe-modal-overlay'; }
    overlayId()    { return 'probe-modal-overlay'; }

    open(modelName, isVideo) {
      var existing = document.getElementById(this.overlayId());
      if (existing) existing.remove();

      var exportBtn = isVideo
        ? ''
        : '<button id="probe-export-btn" class="cycle-float-btn" title="Выгрузка" onclick="exportProbeModal(this)" style="display:none">' + (window.EXPORT_STORY_SVG || '') + '</button>';
      this._el = document.createElement('div');
      this._el.id        = this.overlayId();
      this._el.className = this.overlayClass() + ' open';
      this._el.innerHTML =
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
            '<button class="probe-modal-close" id="_pm-close">&times;</button>' +
          '</div>' +
          '<div class="probe-modal-body" id="probe-modal-body" data-memo-scroll><span style="color:#888">Создаю батч…</span></div>' +
        '</div>';
      document.body.appendChild(this._el);
      this._setupKeyboard();
      document.body.style.overflow = 'hidden';

      var self = this;
      var closeBtn = document.getElementById('_pm-close');
      if (closeBtn) closeBtn.addEventListener('click', function() { self.close(); });
      return this;
    }

    onClose() {
      document.body.style.overflow = '';
      if (this._probeBtn) { this._probeBtn.classList.remove('probing'); this._probeBtn = null; }
    }

    setProbeBtn(btn) {
      if (this._probeBtn) this._probeBtn.classList.remove('probing');
      this._probeBtn = btn;
      if (btn) btn.classList.add('probing');
    }

    clearProbeBtn() {
      if (this._probeBtn) { this._probeBtn.classList.remove('probing'); this._probeBtn = null; }
    }
  }

  var _probeModal = new ProbeModal();

  window.closeProbeModal = function() { _probeModal.close(); };

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
    var lines   = [];
    if (title)   lines.push(title.textContent);
    if (batchId) lines.push('batch: ' + batchId);
    navigator.clipboard.writeText(lines.join('\n')).then(function() { _flashCopied(btn); }).catch(function() {});
  };

  window.exportProbeModal = function(btn) {
    var overlay = document.getElementById('probe-modal-overlay');
    var storyId = overlay ? (overlay.dataset.storyId || '') : '';
    window.exportStory(storyId, btn);
  };

  window.probeTextModel = function(modelId, modelName, btn) {
    _probeModal.open(modelName);
    var body = document.getElementById('probe-modal-body');
    _probeModal.setProbeBtn(btn);

    fetch('/api/text-models/' + encodeURIComponent(modelId) + '/probe', { method: 'POST' })
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (data.error) {
          _probeModal.clearProbeBtn();
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
        _probeModal.clearProbeBtn();
        body.className   = 'probe-modal-body probe-modal-error';
        body.textContent = 'Ошибка запроса: ' + e;
      });
  };

  window.createProbeVideo = function(modelId, modelName, btn) {
    _probeModal.open(modelName, true);
    var body    = document.getElementById('probe-modal-body');
    var overlay = document.getElementById('probe-modal-overlay');
    _probeModal.setProbeBtn(btn);

    fetch('/api/video-models/' + encodeURIComponent(modelId) + '/probe', { method: 'POST' })
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (data.error) {
          _probeModal.clearProbeBtn();
          body.innerHTML = '<span style="color:#c05050">' + _esc(data.error) + '</span>';
          return;
        }
        if (overlay && overlay.dataset) overlay.dataset.batchId = data.batch_id;
        body.innerHTML = '';
        _doBatchLogPoll(data.batch_id, body, null, btn);
      })
      .catch(function(e) {
        _probeModal.clearProbeBtn();
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
        copyBtn.title     = 'Скопировать';
        copyBtn.innerHTML = _SVG_COPY;
        copyBtn.onclick   = function() {
          var copyText = d.title ? d.title + '\n\n' + d.text : d.text;
          navigator.clipboard.writeText(copyText).then(function() { _flashCopied(copyBtn); }).catch(function() {});
        };
        hdr.appendChild(lbl);
        hdr.appendChild(copyBtn);

        if (d.title) {
          var titleEl = document.createElement('div');
          titleEl.className   = 'probe-story-title';
          titleEl.textContent = d.title;
          wrap.appendChild(hdr);
          wrap.appendChild(titleEl);
        } else {
          wrap.appendChild(hdr);
        }

        var pre = document.createElement('div');
        pre.className   = 'probe-story-text';
        pre.textContent = d.text;
        wrap.appendChild(pre);

        body.prepend(sep);
        body.prepend(wrap);
        body.scrollTop = 0;
      })
      .catch(function() {});
  }

  function _doBatchLogPoll(batchId, body, onDone, btn) {
    var _TERMINAL = ['probe', 'movie_probe', 'story_probe', 'video_error', 'transcode_error', 'publish_error', 'cancelled', 'error'];

    var videoSection = document.createElement('div');
    videoSection.className     = 'probe-modal-video-section';
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
        return renderLogItem(log, d.batch_id, d.story_id, false, d.text_model_name, d.video_model_name);
      }).join('');

      container.querySelectorAll('.monitor-log-item').forEach(function(el) {
        var lid        = el.dataset.lid;
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
      videoSection.innerHTML     = '<video class="probe-video" controls autoplay src="' + src + '"></video>';
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

  // ── StoryModal ─────────────────────────────────────────────────────────────
  class StoryModal extends Dialog {
    constructor() {
      super();
      this._storyText = '';
    }

    overlayClass() { return 'story-modal-overlay'; }
    overlayId()    { return 'story-modal-overlay'; }

    open(storyId, modelName) {
      var existing = document.getElementById(this.overlayId());
      if (existing) existing.remove();

      this._storyText = '';
      this._el = document.createElement('div');
      this._el.id        = this.overlayId();
      this._el.className = this.overlayClass() + ' open';
      this._el.innerHTML =
        '<div class="story-modal" onclick="event.stopPropagation()">' +
          '<div class="story-modal-head">' +
            '<span class="story-modal-title" id="story-modal-title"></span>' +
            '<button class="cycle-float-btn" id="story-modal-copy-btn" title="Скопировать" onclick="copyStoryText()">' +
              '<svg viewBox="0 0 16 16" fill="none" stroke="#8888b0" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="5" width="9" height="9" rx="1.5"/><path d="M3 11V3a1 1 0 0 1 1-1h8"/></svg>' +
            '</button>' +
            '<button class="story-modal-close" id="_sm-close">&times;</button>' +
          '</div>' +
          '<div class="story-modal-body" id="story-modal-body" data-memo-scroll><span class="story-modal-loading">Загрузка…</span></div>' +
        '</div>';
      var titleEl = this._el.querySelector('#story-modal-title');
      if (titleEl) titleEl.textContent = modelName ? 'Сюжет · ' + modelName : 'Сюжет';
      document.body.appendChild(this._el);
      this._setupKeyboard();
      document.body.style.overflow = 'hidden';
      history.pushState({ modal: 'story' }, '');

      var self = this;
      var closeBtn = document.getElementById('_sm-close');
      if (closeBtn) closeBtn.addEventListener('click', function() { self.close(); });

      var body = document.getElementById('story-modal-body');
      fetch('/api/story/' + encodeURIComponent(storyId))
        .then(function(r) { return r.json(); })
        .then(function(d) {
          if (d.text) {
            self._storyText  = d.text;
            body.textContent = d.text;
          } else {
            body.innerHTML = '<span class="story-modal-loading">Сюжет не найден</span>';
          }
        })
        .catch(function() {
          body.innerHTML = '<span class="story-modal-loading">Ошибка загрузки</span>';
        });

      return this;
    }

    close() {
      if (document.getElementById(this.overlayId())) history.back();
    }

    closeInner() {
      document.body.style.overflow = '';
      this._storyText = '';
      this._removeEl();
    }

    getStoryText() { return this._storyText; }
  }

  var _storyModal = new StoryModal();

  window.openStoryModal        = function(storyId, modelName) { _storyModal.open(storyId, modelName); };
  window.closeStoryModal       = function() { _storyModal.close(); };
  window._closeStoryModalInner = function() { _storyModal.closeInner(); };

  window.copyStoryText = function() {
    var text = _storyModal.getStoryText();
    if (!text) return;
    var btn = document.getElementById('story-modal-copy-btn');
    navigator.clipboard.writeText(text).then(function() {
      if (btn) btn.classList.add('copied');
      setTimeout(function() { if (btn) btn.classList.remove('copied'); }, 2000);
    }).catch(function() {});
  };

  // ── popstate ───────────────────────────────────────────────────────────────
  window.addEventListener('popstate', function(e) {
    var state = e.state || {};
    if (state.modal === 'video') {
      _videoModal.closeInner();
    } else {
      var storyOverlay = document.getElementById('story-modal-overlay');
      var videoOverlay = document.getElementById('video-modal-overlay');
      if (storyOverlay)      { _storyModal.closeInner(); }
      else if (videoOverlay) { _videoModal.closeInner(); }
    }
  });
})();
