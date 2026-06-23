var ListDragReorder = (function() {
  function handleHtml() {
    return '<div class="model-drag-handle" title="Перетащить">⠿</div>';
  }

  function bind(container, opts) {
    if (!container) return;
    opts = opts || {};
    var rowSelector = opts.rowSelector || '.story-row';
    var handleSelector = opts.handleSelector || '.model-drag-handle';
    var onReorder = opts.onReorder;
    if (typeof onReorder !== 'function') return;

    var dragSrcId = null;

    container.addEventListener('pointerdown', function(e) {
      var handle = e.target.closest(handleSelector);
      if (!handle) return;
      var row = handle.closest(rowSelector);
      if (!row) return;
      row.setAttribute('draggable', 'true');
      var releaseHandler = function() {
        row.setAttribute('draggable', 'false');
        document.removeEventListener('pointerup', releaseHandler);
      };
      document.addEventListener('pointerup', releaseHandler, { once: true });
    });

    container.addEventListener('dragstart', function(e) {
      var row = e.target.closest(rowSelector);
      if (!row || row.getAttribute('draggable') !== 'true') return;
      dragSrcId = row.getAttribute('data-id');
      row.classList.add('dragging');
      e.dataTransfer.effectAllowed = 'move';
      e.dataTransfer.setData('text/plain', dragSrcId);
    });

    container.addEventListener('dragend', function(e) {
      var row = e.target.closest(rowSelector);
      if (row) {
        row.classList.remove('dragging');
        row.setAttribute('draggable', 'false');
      }
      container.querySelectorAll(rowSelector).forEach(function(r) {
        r.classList.remove('drag-over-top', 'drag-over-bottom');
      });
    });

    container.addEventListener('dragover', function(e) {
      var row = e.target.closest(rowSelector);
      if (!row || !dragSrcId) return;
      e.preventDefault();
      e.dataTransfer.dropEffect = 'move';
      if (dragSrcId === row.getAttribute('data-id')) return;
      container.querySelectorAll(rowSelector).forEach(function(r) {
        r.classList.remove('drag-over-top', 'drag-over-bottom');
      });
      var rect = row.getBoundingClientRect();
      if (e.clientY < rect.top + rect.height / 2) row.classList.add('drag-over-top');
      else row.classList.add('drag-over-bottom');
    });

    container.addEventListener('dragleave', function(e) {
      var row = e.target.closest(rowSelector);
      if (row) row.classList.remove('drag-over-top', 'drag-over-bottom');
    });

    container.addEventListener('drop', function(e) {
      var row = e.target.closest(rowSelector);
      if (!row || !dragSrcId || dragSrcId === row.getAttribute('data-id')) return;
      e.preventDefault();
      container.querySelectorAll(rowSelector).forEach(function(r) {
        r.classList.remove('drag-over-top', 'drag-over-bottom');
      });
      var srcEl = container.querySelector(rowSelector + '[data-id="' + dragSrcId + '"]');
      if (srcEl) {
        var rect = row.getBoundingClientRect();
        if (e.clientY < rect.top + rect.height / 2) container.insertBefore(srcEl, row);
        else container.insertBefore(srcEl, row.nextSibling);
      }
      var movedId = dragSrcId;
      var ids = Array.from(container.querySelectorAll(rowSelector)).map(function(r) {
        return r.getAttribute('data-id');
      });
      dragSrcId = null;
      onReorder(ids, movedId);
    });
  }

  return { bind: bind, handleHtml: handleHtml };
})();
