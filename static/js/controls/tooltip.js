(function() {
  if (!('ontouchstart' in window)) return;

  let _timer   = null;
  let _tooltip = null;
  let _autoHide = null;

  function getTooltip() {
    if (!_tooltip) {
      _tooltip = document.createElement('div');
      _tooltip.id = 'touch-tooltip';
      _tooltip.classList.add('hidden');
      document.body.appendChild(_tooltip);
    }
    return _tooltip;
  }

  function showTooltip(text, targetEl) {
    const tip = getTooltip();
    tip.textContent = text;
    tip.classList.remove('hidden');

    const rect = targetEl.getBoundingClientRect();
    const tipW = 220;
    let x = rect.left + rect.width / 2 - tipW / 2;
    let y = rect.top - 10;

    x = Math.max(8, Math.min(x, window.innerWidth - tipW - 8));

    tip.style.width  = tipW + 'px';
    tip.style.left   = x + 'px';

    tip.style.top    = '0px';
    tip.style.bottom = '';
    const tipH = tip.offsetHeight || 34;
    if (y - tipH < 8) {
      tip.style.top  = (rect.bottom + 10) + 'px';
    } else {
      tip.style.top  = (y - tipH) + 'px';
    }

    clearTimeout(_autoHide);
    _autoHide = setTimeout(hideTooltip, 2500);
  }

  function hideTooltip() {
    clearTimeout(_autoHide);
    if (_tooltip) _tooltip.classList.add('hidden');
  }

  function findTitle(el) {
    while (el && el !== document.body) {
      if (el.title) return { text: el.title, el };
      el = el.parentElement;
    }
    return null;
  }

  document.addEventListener('touchstart', function(e) {
    clearTimeout(_timer);
    const found = findTitle(e.target);
    if (!found) return;
    _timer = setTimeout(function() {
      showTooltip(found.text, found.el);
    }, 500);
  }, { passive: true });

  document.addEventListener('touchend',    function() { clearTimeout(_timer); }, { passive: true });
  document.addEventListener('touchmove',   function() { clearTimeout(_timer); hideTooltip(); }, { passive: true });
  document.addEventListener('touchcancel', function() { clearTimeout(_timer); hideTooltip(); }, { passive: true });
})();
