(function() {
  var SVG = '<svg viewBox="0 0 16 16" fill="none" stroke="#8888b0" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M8 2v7M5 6l3 3 3-3"/><path d="M3 11v2a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1v-2"/></svg>';

  window.EXPORT_STORY_SVG = SVG;

  var _clipBuffer = null;
  var _clipTimer = null;
  var ACCUMULATE_MS = 10000;

  window.wrapBlock = function(title, body, number) {
    var label = (number !== undefined && number !== null) ? title + ' ' + number : title;
    return '/* ' + label + ' НАЧАЛО */\n' + body + '\n/* ' + label + ' КОНЕЦ */';
  };

  function _resetBuffer() {
    _clipBuffer = null;
    _clipTimer = null;
  }

  window.flashCopied = function(btn) {
    btn.classList.add('copied');
    setTimeout(function() { btn.classList.remove('copied'); }, 2000);
  };

  function _appendBlock(blockFactory, btn) {
    var isFirst = (_clipBuffer === null);
    var block = blockFactory(isFirst);
    var toWrite = isFirst ? block : (_clipBuffer + '\n\n' + block);
    _clipBuffer = toWrite;
    clearTimeout(_clipTimer);
    _clipTimer = setTimeout(_resetBuffer, ACCUMULATE_MS);
    navigator.clipboard.writeText(toWrite).then(function() { window.flashCopied(btn); }).catch(function() {});
  }

  function _manualBlock(title, content) {
    return '/* Ручной сюжет НАЧАЛО */\n' + (title || '') + '\n\n' + (content || '') + '\n/* Ручной сюжет КОНЕЦ */';
  }

  function _writeManualBlock(title, content, btn) {
    _appendBlock(function() { return _manualBlock(title, content); }, btn);
  }

  window.exportStory = function(storyId, btn) {
    var mode = (btn && btn.dataset) ? btn.dataset.mode : null;
    if (mode === 'manual-new') {
      var titleEl = document.getElementById('draft-story-title');
      var contentEl = document.getElementById('draft-story-content');
      _writeManualBlock(titleEl ? titleEl.value : '', contentEl ? contentEl.value : '', btn);
      return;
    }
    if (!storyId) return;
    fetch('/api/story/' + encodeURIComponent(storyId))
      .then(function(r) { return r.json(); })
      .then(function(d) {
        if (mode === 'manual') {
          _writeManualBlock(d.title || '', d.text || '', btn);
          return;
        }
        if (!d.text) return;
        var modelLabel = (d.platform_name || '') + ': ' + (d.model_name || '');
        var body = d.model_body || {};
        var SKIP_KEYS = { messages: true };
        var configLines = Object.keys(body).filter(function(k) { return !SKIP_KEYS[k]; }).map(function(k) { return k + ': ' + body[k]; });
        var answer = d.title ? d.title + '\n\n' + d.text : d.text;

        _appendBlock(function(isFirst) {
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
        }, btn);
      })
      .catch(function() {});
  };
})();
