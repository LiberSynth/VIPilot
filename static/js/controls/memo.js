(function() {
  var attached = typeof WeakSet !== 'undefined' ? new WeakSet() : null;
  var scrollAttached = typeof WeakSet !== 'undefined' ? new WeakSet() : null;

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

  function attachScrollable(el) {
    if (!el.id) return;
    if (scrollAttached) {
      if (scrollAttached.has(el)) return;
      scrollAttached.add(el);
    }
    var key = 'memo_scroll_' + el.id;
    var saved = localStorage.getItem(key);
    if (saved) {
      var parts = saved.split(',');
      el.scrollLeft = parseInt(parts[0], 10) || 0;
      el.scrollTop = parseInt(parts[1], 10) || 0;
    }
    el.addEventListener('scroll', function() {
      localStorage.setItem(key, el.scrollLeft + ',' + el.scrollTop);
    });
  }

  function init() {
    document.querySelectorAll('textarea[id]').forEach(attachTextarea);
    document.querySelectorAll('[data-memo-scroll][id]').forEach(attachScrollable);

    if (typeof MutationObserver !== 'undefined') {
      new MutationObserver(function(mutations) {
        mutations.forEach(function(mutation) {
          mutation.addedNodes.forEach(function(node) {
            if (node.nodeType !== 1) return;
            if (node.tagName === 'TEXTAREA' && node.id) {
              attachTextarea(node);
            }
            node.querySelectorAll && node.querySelectorAll('textarea[id]').forEach(attachTextarea);
            if (node.dataset && node.dataset.memoScroll !== undefined && node.id) {
              attachScrollable(node);
            }
            node.querySelectorAll && node.querySelectorAll('[data-memo-scroll][id]').forEach(attachScrollable);
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
