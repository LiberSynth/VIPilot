class Dialog {
  constructor(opts) {
    opts = opts || {};
    this._triggerBtn = opts.triggerBtn || null;
    this._el = null;
    this._boundKeyDown = this._handleKeyDown.bind(this);
  }

  overlayClass() { return 'dialog-overlay'; }
  overlayId()    { return 'dialog-overlay'; }
  render()       { return ''; }
  onOpen()       {}
  onClose()      {}

  open() {
    var existing = document.getElementById(this.overlayId());
    if (existing) existing.remove();
    this._el = document.createElement('div');
    this._el.className = this.overlayClass() + ' open';
    this._el.id = this.overlayId();
    this._el.innerHTML = this.render();
    document.body.appendChild(this._el);
    this._setupKeyboard();
    this.onOpen();
    return this;
  }

  _setupKeyboard() {
    document.addEventListener('keydown', this._boundKeyDown);
  }

  _teardownKeyboard() {
    document.removeEventListener('keydown', this._boundKeyDown);
  }

  _removeEl() {
    if (this._el) { this._el.remove(); this._el = null; }
    this._teardownKeyboard();
  }

  close() {
    this._removeEl();
    if (this._triggerBtn) this._triggerBtn.focus();
    this.onClose();
  }

  _getFocusable() {
    if (!this._el) return [];
    return Array.from(this._el.querySelectorAll(
      'button:not([disabled]),[href],input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex="-1"])'
    ));
  }

  _handleKeyDown(e) {
    if (e.key === 'Escape') {
      e.preventDefault();
      this.close();
      return;
    }
    if (e.key === 'Tab') {
      var focusable = this._getFocusable();
      if (focusable.length === 0) return;
      var first = focusable[0], last = focusable[focusable.length - 1];
      var outside = !this._el.contains(document.activeElement);
      if (e.shiftKey) {
        if (outside || document.activeElement === first) { e.preventDefault(); last.focus(); }
      } else {
        if (outside || document.activeElement === last)  { e.preventDefault(); first.focus(); }
      }
    }
  }
}
