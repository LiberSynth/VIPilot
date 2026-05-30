function _emdEscapeHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

class ExportMoviesDialog extends Dialog {
  constructor(opts) {
    super(opts);
    opts = opts || {};
    this._total    = opts.total    || 0;
    this._title    = opts.title    || 'Выгрузка роликов';
    this._cancelled = false;
    this._onCancel  = opts.onCancel || null;
    this._onRetry   = opts.onRetry  || null;
  }

  overlayClass() { return 'confirm-overlay'; }
  overlayId()    { return 'export-movies-dialog-overlay'; }

  render() {
    return '<div class="confirm-box">' +
      '<div class="confirm-box-title">' + _emdEscapeHtml(this._title) + '</div>' +
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
    this._barEl = this._el ? this._el.querySelector('#_emd-bar') : null;
    this._counterEl = this._el ? this._el.querySelector('#_emd-counter') : null;
    this._filenameEl = this._el ? this._el.querySelector('#_emd-filename') : null;
    var cancelBtn = this._el ? this._el.querySelector('#_emd-cancel') : null;
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
      var btn = (this._el && this._el.querySelector('#_emd-cancel'))
        || (this._el && this._el.querySelector('#_emd-close'));
      if (btn) btn.focus();
    }
  }

  isCancelled() {
    return this._cancelled;
  }

  setCurrentFile(filename) {
    if (this._filenameEl) this._filenameEl.textContent = filename || '';
  }

  setProgress(done, filename, activeIndex) {
    var completed = Math.max(0, Math.min(done, this._total));
    var active = (activeIndex != null && activeIndex > 0)
      ? Math.min(activeIndex, this._total)
      : null;
    var barPct = this._total > 0
      ? Math.round((active != null ? active : completed) / this._total * 100)
      : 0;
    if (this._barEl) this._barEl.style.width = barPct + '%';
    if (this._counterEl) {
      this._counterEl.textContent = completed + ' из ' + this._total;
    }
    if (filename != null && this._filenameEl) {
      this._filenameEl.textContent = filename || '';
    }
  }

  finish(done, cancelled, failedItems) {
    failedItems = failedItems || [];
    var failed = failedItems.length;

    var cancelBtn = this._el ? this._el.querySelector('#_emd-cancel') : null;
    if (cancelBtn) cancelBtn.remove();

    if (this._barEl) {
      this._barEl.style.width = (this._total > 0 ? Math.round(done / this._total * 100) : 100) + '%';
    }

    if (this._counterEl) {
      var msg = cancelled
        ? 'Отменено. Выгружено ' + done + ' из ' + this._total
        : 'Готово. Выгружено ' + done + ' из ' + this._total;
      if (failed) msg += ', ошибок: ' + failed;
      this._counterEl.textContent = msg;
    }

    if (this._filenameEl) {
      if (failed > 0) {
        var html = '<div style="margin-top:8px;font-size:12px;color:#f87171;font-weight:600">Не удалось выгрузить:</div>' +
          '<ul style="margin:4px 0 0;padding:0 0 0 16px;max-height:120px;overflow-y:auto;font-size:12px;color:#f87171">';
        for (var i = 0; i < failedItems.length; i++) {
          var item = failedItems[i];
          html += '<li style="margin-bottom:3px"><span style="color:#fca5a5">' +
            _emdEscapeHtml(item.filename) + '</span>' +
            ' — ' + _emdEscapeHtml(item.reason) + '</li>';
        }
        html += '</ul>';
        this._filenameEl.innerHTML = html;
      } else {
        this._filenameEl.textContent = '';
      }
    }

    var btns = this._el ? this._el.querySelector('.confirm-box-btns') : null;
    if (btns) {
      if (failed > 0 && this._onRetry) {
        var retryBtn = document.createElement('button');
        retryBtn.className = 'confirm-cancel';
        retryBtn.id = '_emd-retry';
        retryBtn.textContent = 'Повторить ошибки';
        var self = this;
        var _failedItems = failedItems;
        retryBtn.addEventListener('click', function() {
          self.close();
          self._onRetry(_failedItems);
        });
        btns.appendChild(retryBtn);
      }
      var closeBtn = document.createElement('button');
      closeBtn.className = 'confirm-cancel';
      closeBtn.id = '_emd-close';
      closeBtn.textContent = 'Закрыть';
      var self = this;
      closeBtn.addEventListener('click', function() { self.close(); });
      btns.appendChild(closeBtn);
      if (failed === 0 || !this._onRetry) closeBtn.focus();
    }
  }
}
