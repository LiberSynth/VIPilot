(function() {
  function init() {
    document.querySelectorAll('textarea[id]').forEach(function(ta) {
      var key = 'memo_height_' + ta.id;
      var saved = localStorage.getItem(key);
      if (saved) ta.style.height = saved;
      if (typeof ResizeObserver !== 'undefined') {
        new ResizeObserver(function() {
          if (ta.style.height) localStorage.setItem(key, ta.style.height);
        }).observe(ta);
      }
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
