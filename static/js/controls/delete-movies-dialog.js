class DeleteMoviesDialog extends Dialog {
  constructor(opts) {
    super(opts);
  }

  overlayClass() { return 'confirm-overlay'; }
  overlayId()    { return 'delete-movies-dialog-overlay'; }

  render() {
    return '<div class="confirm-box">' +
      '<div class="confirm-box-title">Удаление роликов</div>' +
      '<div style="margin:12px 0 6px;position:relative;overflow:hidden;background:rgba(255,255,255,.08);border-radius:6px;height:6px">' +
        '<div id="_dmd-bar" style="position:absolute;height:100%;background:#f87171;border-radius:6px;animation:bar-indeterminate 1.4s linear infinite"></div>' +
      '</div>' +
      '<div id="_dmd-status" style="font-size:13px;color:#aaa;margin-bottom:4px">Удаление…</div>' +
    '</div>';
  }

  onOpen() {}

  _handleKeyDown(e) {
    if (e.key === 'Tab') {
      e.preventDefault();
      var btn = document.getElementById('_dmd-close');
      if (btn) btn.focus();
    }
  }

  _stopBar() {
    var bar = document.getElementById('_dmd-bar');
    if (bar) {
      bar.style.animation = 'none';
      bar.style.left      = '0';
      bar.style.width     = '100%';
    }
  }

  _addCloseButton() {
    if (document.getElementById('_dmd-close')) return;
    var box = this._el ? this._el.querySelector('.confirm-box') : null;
    if (!box) return;
    var btns = document.createElement('div');
    btns.className   = 'confirm-box-btns';
    btns.style.marginTop = '14px';
    var closeBtn = document.createElement('button');
    closeBtn.className   = 'confirm-cancel';
    closeBtn.id          = '_dmd-close';
    closeBtn.textContent = 'Закрыть';
    var self = this;
    closeBtn.addEventListener('click', function() { self.close(); });
    btns.appendChild(closeBtn);
    box.appendChild(btns);
    closeBtn.focus();
  }

  finish(n) {
    this._stopBar();
    var status = document.getElementById('_dmd-status');
    if (status) {
      status.textContent = n > 0
        ? 'Удалено ' + n + ' ' + _pluralMovies(n)
        : 'Нечего удалять';
    }
    this._addCloseButton();
  }

  error() {
    this._stopBar();
    var bar = document.getElementById('_dmd-bar');
    if (bar) bar.style.width = '0';
    var status = document.getElementById('_dmd-status');
    if (status) { status.textContent = 'Ошибка удаления'; status.style.color = '#f87171'; }
    this._addCloseButton();
  }
}

function _pluralMovies(n) {
  var mod10  = n % 10;
  var mod100 = n % 100;
  if (mod10 === 1 && mod100 !== 11) return 'ролик';
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 10 || mod100 >= 20)) return 'ролика';
  return 'роликов';
}
