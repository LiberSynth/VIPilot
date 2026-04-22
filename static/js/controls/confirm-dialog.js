class ConfirmDialog extends Dialog {
  constructor(opts) {
    super(opts);
    opts = opts || {};
    this._title        = opts.title        || '';
    this._text         = opts.text         || '';
    this._confirmLabel = opts.confirmLabel || null;
    this._cancelLabel  = opts.cancelLabel  || 'Отмена';
    this._confirmStyle = opts.confirmStyle || 'background:#b05820';
    this._onConfirm    = opts.onConfirm    || null;
    this._onCancel     = opts.onCancel     || null;
  }

  overlayClass() { return 'confirm-overlay'; }
  overlayId()    { return 'confirm-dialog-overlay'; }

  render() {
    var confirmHtml = this._confirmLabel
      ? '<button class="confirm-confirm" id="_cd-confirm" style="' + this._confirmStyle + '">' + this._confirmLabel + '</button>'
      : '';
    return '<div class="confirm-box">' +
      '<div class="confirm-box-title">' + this._title + '</div>' +
      '<div class="confirm-box-text">'  + this._text  + '</div>' +
      '<div class="confirm-box-btns">' +
        '<button class="confirm-cancel" id="_cd-cancel">' + this._cancelLabel + '</button>' +
        confirmHtml +
      '</div>' +
    '</div>';
  }

  onOpen() {
    var self = this;
    var cancelBtn  = document.getElementById('_cd-cancel');
    var confirmBtn = document.getElementById('_cd-confirm');
    if (cancelBtn) cancelBtn.addEventListener('click', function() {
      if (self._onCancel) self._onCancel();
      self.close();
    });
    if (confirmBtn) confirmBtn.addEventListener('click', function() {
      if (self._onConfirm) self._onConfirm(confirmBtn, self);
    });
    if (cancelBtn) cancelBtn.focus();
  }
}
