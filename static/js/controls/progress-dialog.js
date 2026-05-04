var _DB_OP_DIALOG = null;

function _fmtMb(bytes) {
  var mb = (bytes || 0) / (1024 * 1024);
  return mb.toFixed(mb < 10 ? 2 : 1);
}

function _escHtml(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

class DbOpProgressDialog extends Dialog {
  constructor() {
    super({});
    this._timer = null;
    this._closed = false;
    this._lastBackupState = null;
    this._lastRestoreState = null;
    this._downloadStarted = false;
    this._uploading = false;
    this._uploadFileName = '';
  }

  overlayClass() { return 'confirm-overlay'; }
  overlayId()    { return 'db-op-progress-overlay'; }

  _setupKeyboard() { /* блокирующий модал: ESC не закрывает */ }

  render() {
    return '<div class="confirm-box" id="_dop-box">' +
      '<div class="confirm-box-title" id="_dop-title">Подготовка…</div>' +
      '<div class="confirm-box-text"  id="_dop-text"></div>' +
      '<div class="confirm-box-btns"  id="_dop-btns"></div>' +
    '</div>';
  }

  open() {
    super.open();
    if (this._uploading) {
      this._renderUploading();
    } else {
      this._poll();
      this._timer = setInterval(this._poll.bind(this), 1500);
    }
    return this;
  }

  close() {
    if (this._timer) { clearInterval(this._timer); this._timer = null; }
    this._closed = true;
    if (_DB_OP_DIALOG === this) _DB_OP_DIALOG = null;
    super.close();
  }

  setUploadingMode(fileName) {
    this._uploading = true;
    this._uploadFileName = fileName || '';
    if (this._el) this._renderUploading();
  }

  endUploadingMode() {
    this._uploading = false;
    if (this._timer) clearInterval(this._timer);
    this._poll();
    this._timer = setInterval(this._poll.bind(this), 1500);
  }

  _renderUploading() {
    var titleEl = document.getElementById('_dop-title');
    var textEl  = document.getElementById('_dop-text');
    var btnsEl  = document.getElementById('_dop-btns');
    if (titleEl) titleEl.textContent = 'Восстановление базы данных';
    if (textEl)  textEl.innerHTML =
      'Загрузка файла на сервер…<br>' +
      '<span class="hint">Файл: ' + _escHtml(this._uploadFileName) + '</span>';
    if (btnsEl)  btnsEl.innerHTML = '';
  }

  _poll() {
    if (this._closed) return;
    var self = this;
    fetch('/api/db_op/status')
      .then(function(r) { return r.json(); })
      .then(function(d) { if (!self._closed) self._render(d); })
      .catch(function() {});
  }

  _render(d) {
    var b = (d && d.backup)  || { state: 'idle' };
    var r = (d && d.restore) || { state: 'idle' };

    var bs = b.state || 'idle';
    var rs = r.state || 'idle';

    if (bs === 'idle' && rs === 'idle') {
      if (this._lastRestoreState === 'running') {
        showToast('База восстановлена из бэкапа', 'success');
      } else if (this._lastBackupState === 'ready') {
        showToast('Бэкап скачан', 'success');
      } else if (this._lastBackupState === 'running' || this._lastBackupState === 'failed') {
        // отменён или ошибка уже была показана
      }
      this.close();
      return;
    }

    var titleEl = document.getElementById('_dop-title');
    var textEl  = document.getElementById('_dop-text');
    var btnsEl  = document.getElementById('_dop-btns');
    if (!titleEl || !textEl || !btnsEl) return;

    var title = '';
    var text  = '';
    var btns  = '';
    var op    = null; // 'backup' | 'restore' для кнопок cancel/close

    if (rs === 'running') {
      title = 'Восстановление базы данных';
      var phaseLabel = (r.phase === 'restoring') ? 'Восстановление'
                     : (r.phase === 'preparing') ? 'Подготовка'
                     : 'Восстановление';
      text  = phaseLabel + '…';
      if (r.bytes_total) text += '<br><span class="hint">Размер дампа: ' + _fmtMb(r.bytes_total) + ' МБ</span>';
      btns  = '<button class="confirm-cancel" id="_dop-cancel">Отмена</button>';
      op    = 'restore';
    } else if (rs === 'failed') {
      title = 'Восстановление не удалось';
      text  = '<span style="color:#f55">' + _escHtml(r.error || 'неизвестная ошибка') + '</span>';
      btns  = '<button class="confirm-confirm" id="_dop-close" style="background:#b05820">Закрыть</button>';
      op    = 'restore';
    } else if (bs === 'running') {
      title = 'Создание бэкапа';
      var wr = b.bytes_written || 0;
      var tot = b.bytes_total_est || 0;
      text = 'Записано ' + _fmtMb(wr) + ' МБ';
      if (tot) text += '<br><span class="hint">Размер БД: ~' + _fmtMb(tot) + ' МБ (несжато)</span>';
      btns = '<button class="confirm-cancel" id="_dop-cancel">Отмена</button>';
      op   = 'backup';
    } else if (bs === 'ready') {
      title = 'Бэкап готов';
      var sz = b.bytes_total || 0;
      text  = 'Файл создан (' + _fmtMb(sz) + ' МБ). Скачивание начнётся автоматически.';
      btns  = '<button class="confirm-cancel" id="_dop-cancel">Удалить файл</button>';
      op    = 'backup';
      if (!this._downloadStarted) {
        this._downloadStarted = true;
        this._triggerDownload();
      }
    } else if (bs === 'failed') {
      title = 'Не удалось создать бэкап';
      text  = '<span style="color:#f55">' + _escHtml(b.error || 'неизвестная ошибка') + '</span>';
      btns  = '<button class="confirm-confirm" id="_dop-close" style="background:#b05820">Закрыть</button>';
      op    = 'backup';
    }

    titleEl.textContent = title;
    textEl.innerHTML    = text;
    btnsEl.innerHTML    = btns;

    var self = this;
    var cancelBtn = document.getElementById('_dop-cancel');
    if (cancelBtn) cancelBtn.onclick = function() { self._sendCancel(op); };
    var closeBtn  = document.getElementById('_dop-close');
    if (closeBtn)  closeBtn.onclick  = function() { self._sendCancel(op); };

    this._lastBackupState  = bs;
    this._lastRestoreState = rs;
  }

  _sendCancel(op) {
    if (op !== 'backup' && op !== 'restore') return;
    var self = this;
    fetch('/api/db_' + op + '/cancel', { method: 'POST' })
      .then(function() { self._poll(); })
      .catch(function() {});
  }

  _triggerDownload() {
    var a = document.createElement('a');
    a.href = '/api/db_backup/download';
    a.style.display = 'none';
    document.body.appendChild(a);
    a.click();
    setTimeout(function() { a.remove(); }, 200);
  }
}

function openDbOpProgressDialog() {
  if (_DB_OP_DIALOG) return _DB_OP_DIALOG;
  _DB_OP_DIALOG = new DbOpProgressDialog();
  _DB_OP_DIALOG.open();
  return _DB_OP_DIALOG;
}

function refreshDbOpStatus() {
  fetch('/api/db_op/status')
    .then(function(r) { return r.json(); })
    .then(function(d) {
      var b = (d && d.backup  && d.backup.state)  || 'idle';
      var r = (d && d.restore && d.restore.state) || 'idle';
      if (b !== 'idle' || r !== 'idle') {
        openDbOpProgressDialog();
      }
    })
    .catch(function() {});
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', refreshDbOpStatus);
} else {
  refreshDbOpStatus();
}
