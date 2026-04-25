(function() {
  var SVG = '<svg viewBox="0 0 16 16" fill="none" stroke="#8888b0" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M8 2v7M5 6l3 3 3-3"/><path d="M3 11v2a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1v-2"/></svg>';

  window.EXPORT_STORY_SVG = SVG;

  var _clipBuffer = null;
  var _clipTimer = null;
  var ACCUMULATE_MS = 10000;

  window.wrapBlock = function(title, body) {
    return '/* ' + title + ' НАЧАЛО */\n' + body + '\n/* ' + title + ' КОНЕЦ */';
  };

  function _resetBuffer() {
    _clipBuffer = null;
    _clipTimer = null;
  }

  window.flashCopied = function(btn) {
    btn.classList.add('copied');
    setTimeout(function() { btn.classList.remove('copied'); }, 2000);
  };

  window.exportStory = function(storyId, btn) {
    if (!storyId) return;
    fetch('/api/story/' + encodeURIComponent(storyId))
      .then(function(r) { return r.json(); })
      .then(function(d) {
        if (!d.text) return;
        var modelLabel = (d.platform_name || '') + ': ' + (d.model_name || '');
        var body = d.model_body || {};
        var SKIP_KEYS = { messages: true };
        var configLines = Object.keys(body).filter(function(k) { return !SKIP_KEYS[k]; }).map(function(k) { return k + ': ' + body[k]; });
        var answer = d.title ? d.title + '\n\n' + d.text : d.text;

        var modelBlock = '/* Текстовая модель: ' + modelLabel + ' */';
        if (configLines.length) {
          modelBlock += '\n' + window.wrapBlock('Конфиг модели', configLines.join('\n'));
        }
        var answerBlock = window.wrapBlock('Ответ текстовой модели', answer);

        var toWrite;
        if (_clipBuffer === null) {
          var promptBlock = window.wrapBlock('Системный промпт', d.format_prompt || '');
          promptBlock += '\n\n' + window.wrapBlock('Промпт', d.user_prompt || '');
          toWrite = promptBlock + '\n\n' + modelBlock + '\n' + answerBlock;
        } else {
          toWrite = _clipBuffer + '\n\n' + modelBlock + '\n' + answerBlock;
        }

        _clipBuffer = toWrite;
        clearTimeout(_clipTimer);
        _clipTimer = setTimeout(_resetBuffer, ACCUMULATE_MS);
        navigator.clipboard.writeText(toWrite).then(function() { window.flashCopied(btn); }).catch(function() {});
      })
      .catch(function() {});
  };
})();
