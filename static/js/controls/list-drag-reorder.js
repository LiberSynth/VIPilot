var ListDragReorder = (function() {
  function _escapeAttr(str) {
    return String(str || '')
      .replace(/&/g, '&amp;')
      .replace(/"/g, '&quot;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  function handleHtml(opts) {
    opts = opts || {};
    var disabled = !!opts.disabled;
    var title = opts.title || 'Перетащить';
    var cls = 'model-drag-handle' + (disabled ? ' model-drag-handle--disabled' : '');
    var attrs = 'class="' + cls + '" title="' + _escapeAttr(title) + '"';
    if (disabled) attrs += ' data-drag-disabled="1" aria-disabled="true"';
    return '<div ' + attrs + '>⠿</div>';
  }

  function bind(container, opts) {
    if (!container) return;
    opts = opts || {};
    var rowSelector = opts.rowSelector || '.story-row';
    var handleSelector = opts.handleSelector || '.model-drag-handle';
    var onReorder = opts.onReorder;
    var canDrop = opts.canDrop;
    if (typeof onReorder !== 'function') return;

    var dragSrcId = null;

    function _dropPositionFor(row, clientY) {
      var rect = row.getBoundingClientRect();
      return clientY < rect.top + rect.height / 2 ? 'before' : 'after';
    }

    function _isDropAllowed(row, clientY) {
      if (typeof canDrop !== 'function') return true;
      return canDrop({
        movedId: dragSrcId,
        targetId: row.getAttribute('data-id'),
        position: _dropPositionFor(row, clientY),
        targetRow: row,
        container: container,
        rowSelector: rowSelector,
      }) !== false;
    }

    container.addEventListener('pointerdown', function(e) {
      var handle = e.target.closest(handleSelector);
      if (!handle) return;
      if (handle.getAttribute('data-drag-disabled') === '1') return;
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
      var rowHandle = row.querySelector(handleSelector);
      if (rowHandle && rowHandle.getAttribute('data-drag-disabled') === '1') {
        row.setAttribute('draggable', 'false');
        e.preventDefault();
        return;
      }
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
      if (dragSrcId === row.getAttribute('data-id')) return;
      if (!_isDropAllowed(row, e.clientY)) {
        container.querySelectorAll(rowSelector).forEach(function(r) {
          r.classList.remove('drag-over-top', 'drag-over-bottom');
        });
        if (e.dataTransfer) e.dataTransfer.dropEffect = 'none';
        return;
      }
      e.preventDefault();
      if (e.dataTransfer) e.dataTransfer.dropEffect = 'move';
      container.querySelectorAll(rowSelector).forEach(function(r) {
        r.classList.remove('drag-over-top', 'drag-over-bottom');
      });
      var position = _dropPositionFor(row, e.clientY);
      if (position === 'before') row.classList.add('drag-over-top');
      else row.classList.add('drag-over-bottom');
    });

    container.addEventListener('dragleave', function(e) {
      var row = e.target.closest(rowSelector);
      if (row) row.classList.remove('drag-over-top', 'drag-over-bottom');
    });

    container.addEventListener('drop', function(e) {
      var row = e.target.closest(rowSelector);
      if (!row || !dragSrcId || dragSrcId === row.getAttribute('data-id')) return;
      if (!_isDropAllowed(row, e.clientY)) {
        container.querySelectorAll(rowSelector).forEach(function(r) {
          r.classList.remove('drag-over-top', 'drag-over-bottom');
        });
        return;
      }
      e.preventDefault();
      container.querySelectorAll(rowSelector).forEach(function(r) {
        r.classList.remove('drag-over-top', 'drag-over-bottom');
      });
      var srcEl = container.querySelector(rowSelector + '[data-id="' + dragSrcId + '"]');
      if (srcEl) {
        var position = _dropPositionFor(row, e.clientY);
        if (position === 'before') container.insertBefore(srcEl, row);
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
