(function() {
  var SVG = '<svg viewBox="0 0 16 16" fill="none" stroke="#8888b0" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M8 2v7M5 6l3 3 3-3"/><path d="M3 11v2a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1v-2"/></svg>';

  window.EXPORT_STORY_SVG = SVG;

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
        var parts = [];
        var modelLabel = (d.platform_name || '') + ': ' + (d.model_name || '');
        parts.push('/* Текстовая модель: ' + modelLabel + ' */');
        parts.push('/* Системный промпт НАЧАЛО */\n' + (d.system_prompt || '') + '\n/* Системный промпт КОНЕЦ */');
        parts.push('/* Промпт НАЧАЛО */\n' + (d.user_prompt || '') + '\n/* Промпт КОНЕЦ */');
        var answer = d.title ? d.title + '\n\n' + d.text : d.text;
        parts.push('/* Ответ текстовой модели НАЧАЛО */\n' + answer + '\n/* Ответ текстовой модели КОНЕЦ */');
        navigator.clipboard.writeText(parts.join('\n\n')).then(function() { window.flashCopied(btn); }).catch(function() {});
      })
      .catch(function() {});
  };
})();
