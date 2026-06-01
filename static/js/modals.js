(function() {
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

  // ── StoryModal ─────────────────────────────────────────────────────────────
  class StoryModal extends Dialog {
    constructor() {
      super();
      this._storyTitle = '';
      this._storyText = '';
    }

    overlayClass() { return 'story-modal-overlay'; }
    overlayId()    { return 'story-modal-overlay'; }

    open(storyId, modelName) {
      var existing = document.getElementById(this.overlayId());
      if (existing) existing.remove();

      this._storyTitle = '';
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
            self._storyTitle = d.title || '';
            self._storyText  = d.text;
            var titleEl = document.getElementById('story-modal-title');
            if (titleEl) titleEl.textContent = self._storyTitle || '(без названия)';
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
      this._storyTitle = '';
      this._storyText = '';
      this._removeEl();
    }

    getStoryCopyText() {
      if (!this._storyText) return '';
      return this._storyTitle + '\n\n' + this._storyText;
    }
  }

  var _storyModal = new StoryModal();

  window.openStoryModal        = function(storyId, modelName) { _storyModal.open(storyId, modelName); };
  window.closeStoryModal       = function() { _storyModal.close(); };
  window._closeStoryModalInner = function() { _storyModal.closeInner(); };

  window.copyStoryText = function() {
    var text = _storyModal.getStoryCopyText();
    if (!text) return;
    var btn = document.getElementById('story-modal-copy-btn');
    window.clipboardWrite(text, function() {
      if (btn) btn.classList.add('copied');
      setTimeout(function() { if (btn) btn.classList.remove('copied'); }, 2000);
    });
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
