class ExportMoviesDialog extends Dialog {
  constructor(opts) {
    super(opts);
    opts = opts || {};
    this._total      = opts.total      || 0;
    this._dirHandle  = opts.dirHandle  || null;
    this._cancelled  = false;
    this._onCancel   = opts.onCancel   || null;
  }

  overlayClass() { return 'confirm-overlay'; }
  overlayId()    { return 'export-movies-dialog-overlay'; }

  render() {
    return '<div class="confirm-box">' +
      '<div class="confirm-box-title">Выгрузка роликов</div>' +
      '<div style="margin:12px 0 6px">' +
        '<div style="background:rgba(255,255,255,.08);border-radius:6px;height:6px;overflow:hidden">' +
          '<div id="_emd-bar" style="height:100%;width:0%;background:#6ee7a0;transition:width .2s"></div>' +
        '</div>' +
      '</div>' +
      '<div id="_emd-counter" style="font-size:13px;color:#aaa;margin-bottom:4px">0 из ' + this._total + '</div>' +
      '<div id="_emd-filename" style="font-size:12px;color:#888;min-height:16px;word-break:break-all"></div>' +
      '<div class="confirm-box-btns" style="margin-top:14px">' +
        '<button class="confirm-cancel" id="_emd-cancel">Отмена</button>' +
      '</div>' +
    '</div>';
  }

  onOpen() {
    var self = this;
    var cancelBtn = document.getElementById('_emd-cancel');
    if (cancelBtn) {
      cancelBtn.addEventListener('click', function() {
        self._cancelled = true;
        if (self._onCancel) self._onCancel();
      });
      cancelBtn.focus();
    }
  }

  _handleKeyDown(e) {
    if (e.key === 'Tab') {
      e.preventDefault();
      var btn = document.getElementById('_emd-cancel') || document.getElementById('_emd-close');
      if (btn) btn.focus();
    }
  }

  isCancelled() {
    return this._cancelled;
  }

  setProgress(done, filename) {
    var bar     = document.getElementById('_emd-bar');
    var counter = document.getElementById('_emd-counter');
    var fnEl    = document.getElementById('_emd-filename');
    var pct = this._total > 0 ? Math.round(done / this._total * 100) : 0;
    if (bar)     bar.style.width = pct + '%';
    if (counter) counter.textContent = done + ' из ' + this._total;
    if (fnEl)    fnEl.textContent = filename || '';
  }

  finish(done, cancelled, failed) {
    var cancelBtn = document.getElementById('_emd-cancel');
    if (cancelBtn) cancelBtn.remove();

    var bar = document.getElementById('_emd-bar');
    if (bar) bar.style.width = (this._total > 0 ? Math.round(done / this._total * 100) : 100) + '%';

    var counter = document.getElementById('_emd-counter');
    if (counter) {
      var msg = cancelled
        ? 'Отменено. Выгружено ' + done + ' из ' + this._total
        : 'Готово. Выгружено ' + done + ' из ' + this._total;
      if (failed) msg += ', ошибок: ' + failed;
      counter.textContent = msg;
    }

    var fnEl = document.getElementById('_emd-filename');
    if (fnEl) fnEl.textContent = '';

    var btns = this._el ? this._el.querySelector('.confirm-box-btns') : null;
    if (btns) {
      var closeBtn = document.createElement('button');
      closeBtn.className = 'confirm-cancel';
      closeBtn.id = '_emd-close';
      closeBtn.textContent = 'Закрыть';
      var self = this;
      closeBtn.addEventListener('click', function() { self.close(); });
      btns.appendChild(closeBtn);
      closeBtn.focus();
    }
  }
}
