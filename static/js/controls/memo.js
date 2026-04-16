(function() {
  var attached = typeof WeakSet !== 'undefined' ? new WeakSet() : null;

  function attachTextarea(ta) {
    if (attached) {
      if (attached.has(ta)) return;
      attached.add(ta);
    }
    var key = 'memo_height_' + ta.id;
    var saved = localStorage.getItem(key);
    if (saved) ta.style.height = saved;
    if (typeof ResizeObserver !== 'undefined') {
      new ResizeObserver(function() {
        if (ta.style.height) localStorage.setItem(key, ta.style.height);
      }).observe(ta);
    }
  }

  function init() {
    document.querySelectorAll('textarea[id]').forEach(attachTextarea);

    if (typeof MutationObserver !== 'undefined') {
      new MutationObserver(function(mutations) {
        mutations.forEach(function(mutation) {
          mutation.addedNodes.forEach(function(node) {
            if (node.nodeType !== 1) return;
            if (node.tagName === 'TEXTAREA' && node.id) {
              attachTextarea(node);
            }
            node.querySelectorAll && node.querySelectorAll('textarea[id]').forEach(attachTextarea);
          });
        });
      }).observe(document.body, { childList: true, subtree: true });
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
