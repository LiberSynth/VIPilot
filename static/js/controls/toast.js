function showToast(msg, type) {
  var el = document.createElement('div');
  el.className = 'flash ' + (type || 'success');
  el.style.cssText = 'position:fixed;bottom:24px;left:50%;transform:translateX(-50%);z-index:2000;max-width:420px;width:90%;text-align:center;box-shadow:0 4px 24px rgba(0,0,0,.4);transition:opacity .3s';
  el.textContent = msg;
  document.body.appendChild(el);
  setTimeout(function() {
    el.style.opacity = '0';
    setTimeout(function() { el.remove(); }, 300);
  }, 3500);
}
