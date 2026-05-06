(function() {
  var SVG = '<svg viewBox="0 0 16 16" fill="none" stroke="#8888b0" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M8 2v7M5 6l3 3 3-3"/><path d="M3 11v2a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1v-2"/></svg>';

  window.EXPORT_STORY_SVG = SVG;

  var _clipBuffer = null;
  var _lastClickAt = 0;
  var _writeQueue = Promise.resolve();

  window.wrapBlock = function(title, body, number) {
    var label = (number !== undefined && number !== null) ? title + ' ' + number : title;
    return '/* ' + label + ' НАЧАЛО */\n' + body + '\n/* ' + label + ' КОНЕЦ */';
  };

  window.flashCopied = function(btn) {
    btn.classList.add('copied');
    setTimeout(function() { btn.classList.remove('copied'); }, 2000);
  };

  window.clipboardWrite = function(text, cb) {
    var done = typeof cb === 'function' ? cb : function() {};
    var fallback = function() {
      var ta = document.createElement('textarea');
      ta.value = text;
      ta.style.cssText = 'position:fixed;top:0;left:0;width:1px;height:1px;opacity:0;';
      ta.setAttribute('readonly', '');
      document.body.appendChild(ta);
      ta.focus();
      ta.select();
      try { document.execCommand('copy'); } catch(_e) {}
      document.body.removeChild(ta);
      done();
    };
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(done).catch(fallback);
    } else {
      fallback();
    }
  };

  function _processBlock(blockFactory, btn, clickTime) {
    var dt = clickTime - _lastClickAt;
    var isFirst = (_clipBuffer === null || dt > 10000);
    _lastClickAt = clickTime;
    var block = blockFactory(isFirst);
    var toWrite = isFirst ? block : (_clipBuffer + '\n\n' + block);
    _clipBuffer = toWrite;
    window.clipboardWrite(toWrite, function() { window.flashCopied(btn); });
  }

  function _manualBlock(title, content) {
    return '/* Ручной сюжет НАЧАЛО */\n' + (title || '') + '\n\n' + (content || '') + '\n/* Ручной сюжет КОНЕЦ */';
  }

  window.exportStory = function(storyId, btn) {
    var clickTime = Date.now();
    var mode = (btn && btn.dataset) ? btn.dataset.mode : null;

    if (mode === 'manual-new') {
      var titleEl = document.getElementById('draft-story-title');
      var contentEl = document.getElementById('draft-story-content');
      var draftTitle = titleEl ? titleEl.value : '';
      var draftContent = contentEl ? contentEl.value : '';
      _writeQueue = _writeQueue.then(function() {
        return _processBlock(function() { return _manualBlock(draftTitle, draftContent); }, btn, clickTime);
      });
      return;
    }

    if (!storyId) return;

    _writeQueue = _writeQueue.then(function() {
      return fetch('/api/story/' + encodeURIComponent(storyId))
        .then(function(r) { return r.json(); })
        .then(function(d) {
          if (!d.ai_generated) {
            return _processBlock(function() { return _manualBlock(d.title || '', d.text || ''); }, btn, clickTime);
          }
          if (!d.text) return;
          var hasManualEdit = !!d.manual_changed;
          var modelLabel = (d.platform_name || '') + ': ' + (d.model_name || '');
          var body = d.model_body || {};
          var SKIP_KEYS = { messages: true };
          var configLines = Object.keys(body).filter(function(k) { return !SKIP_KEYS[k]; }).map(function(k) { return k + ': ' + body[k]; });
          if (hasManualEdit) {
            configLines = configLines.concat(['Вносились ручные правки.']);
          }
          var answer = d.title ? d.title + '\n\n' + d.text : d.text;
          return _processBlock(function(isFirst) {
            var modelBlock = '/* Текстовая модель: ' + modelLabel + ' */';
            if (configLines.length) {
              modelBlock += '\n' + window.wrapBlock('Конфиг модели', configLines.join('\n'));
            }
            var answerBlock = window.wrapBlock('Ответ текстовой модели', answer);
            var tail = modelBlock + '\n' + answerBlock;
            if (isFirst) {
              var promptBlock = window.wrapBlock('Системный промпт', d.format_prompt || '');
              promptBlock += '\n\n' + window.wrapBlock('Промпт', d.user_prompt || '');
              return promptBlock + '\n\n' + tail;
            }
            return tail;
          }, btn, clickTime);
        })
        .catch(function() {});
    });
  };
})();
