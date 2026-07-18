class GenerationConsoleController {
  constructor(opts) {
    opts = opts || {};
    this._hintEl = opts.hintId ? document.getElementById(opts.hintId) : null;
    this._consoleEl = document.getElementById(opts.consoleId || '');
    this._defaultHint = opts.defaultHint || '';
    this._maxLines = Math.max(1, parseInt(opts.maxLines, 10) || 5);
    this._pollIntervalMs = (typeof BATCH_ENTRIES_POLL_MS !== 'undefined')
      ? BATCH_ENTRIES_POLL_MS
      : 200;
    this._finalStatuses = new Set(opts.finalStatuses || []);
    this._onBatchFinal = typeof opts.onBatchFinal === 'function' ? opts.onBatchFinal : null;
    this._onAllIdle = typeof opts.onAllIdle === 'function' ? opts.onAllIdle : null;

    this._displayLines = [];
    this._completionVisible = false;
    this._tracked = new Map();
    this._creating = 0;
    this._creationHint = '';
    this._pollTimer = null;
    this._pollInFlight = false;
    this._hintTimer = null;
    this._hadActiveBatches = false;
    this._multiRequestMode = false;
    this._statusText = this._defaultHint;

    this._refreshHint();
    this._renderConsole();
  }

  beginCreation(hintText, opts) {
    opts = opts || {};
    this._resetSession();
    if (opts.multiRequest) this._multiRequestMode = true;
    this._creating += 1;
    if (hintText) this._creationHint = String(hintText);
    this._refreshHint();
  }

  endCreation() {
    this._creating = Math.max(0, this._creating - 1);
    if (this._creating === 0) this._creationHint = '';
    this._refreshHint();
    this._ensurePolling();
  }

  showTemporaryHint(text, ttlMs) {
    if (this._hintTimer) clearTimeout(this._hintTimer);
    this._setHint(text || this._defaultHint);
    var self = this;
    this._hintTimer = setTimeout(function() {
      self._hintTimer = null;
      self._refreshHint();
    }, Math.max(300, parseInt(ttlMs, 10) || 2000));
  }

  setDefaultHint(text) {
    this._defaultHint = String(text || '');
    this._refreshHint();
  }

  addLine(_text, _opts) {
    // Консоль заполняется только из log_entries через poll.
  }

  clearLines() {
    this._displayLines = [];
    this._completionVisible = false;
    this._renderConsole();
  }

  trackBatch(batchId, meta) {
    var id = String(batchId || '').trim();
    if (!id) return;
    if (!this._tracked.has(id)) {
      this._tracked.set(id, { meta: meta || {} });
      this._hadActiveBatches = true;
      this._completionVisible = false;
    } else if (meta && typeof meta === 'object') {
      var existing = this._tracked.get(id);
      existing.meta = Object.assign({}, existing.meta || {}, meta);
    }
    this._refreshHint();
    this._ensurePolling();
  }

  trackBatches(batchIds, metaFactory) {
    if (!Array.isArray(batchIds) || batchIds.length === 0) return;
    var multi = batchIds.length > 1;
    if (multi) this._multiRequestMode = true;
    for (var i = 0; i < batchIds.length; i++) {
      var bid = batchIds[i];
      var meta = (typeof metaFactory === 'function')
        ? metaFactory(bid, i)
        : Object.assign({}, metaFactory || {});
      if (multi && !meta.requestIndex) meta.requestIndex = i + 1;
      this.trackBatch(bid, meta);
    }
  }

  activeCount() {
    return this._tracked.size;
  }

  _resetSession() {
    this._displayLines = [];
    this._completionVisible = false;
    this._multiRequestMode = false;
  }

  _setHint(text) {
    this._statusText = String(text || '');
    if (this._hintEl) this._hintEl.textContent = this._statusText;
    this._renderConsole();
  }

  _refreshHint() {
    if (this._hintTimer && (this._creating > 0 || this._tracked.size > 0)) {
      clearTimeout(this._hintTimer);
      this._hintTimer = null;
    }
    if (this._hintTimer) return;
    if (this._creating > 0) {
      this._setHint(this._creationHint || 'Создаю батчи…');
      return;
    }
    var active = this._tracked.size;
    if (active > 0) {
      this._setHint('Выполняется заказов: ' + active);
      return;
    }
    if (this._hadActiveBatches) {
      this._displayLines = [];
      this._completionVisible = true;
    }
    if (!this._completionVisible) {
      this._setHint(this._defaultHint);
    } else {
      this._renderConsole();
    }
    if (this._hadActiveBatches) {
      this._hadActiveBatches = false;
      if (this._onAllIdle) this._onAllIdle();
    }
  }

  _renderConsole() {
    if (!this._consoleEl) return;
    var parts = [];

    if (this._completionVisible) {
      parts.push(this._defaultHint);
    } else if (this._displayLines.length > 0) {
      parts = this._displayLines.slice();
    } else {
      parts.push(this._statusText || this._defaultHint);
    }

    this._consoleEl.value = parts.join('\n');
  }

  _ensurePolling() {
    if (this._pollTimer || this._pollInFlight || this._tracked.size === 0) return;
    var self = this;
    this._pollTimer = setTimeout(function() {
      self._pollTimer = null;
      self._pollTick();
    }, 0);
  }

  _pollTick() {
    if (this._pollInFlight) return;
    if (this._tracked.size === 0) {
      this._refreshHint();
      return;
    }
    this._pollInFlight = true;
    var ids = Array.from(this._tracked.keys());
    var self = this;
    fetch('/api/generation-console/poll', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ batch_ids: ids, limit: this._maxLines }),
    })
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (data && !data.error) self._consumePollData(data);
      })
      .catch(function() {})
      .finally(function() {
        self._pollInFlight = false;
        self._refreshHint();
        if (self._tracked.size > 0) {
          self._pollTimer = setTimeout(function() {
            self._pollTimer = null;
            self._pollTick();
          }, self._pollIntervalMs);
        }
      });
  }

  _consumePollData(data) {
    var entries = Array.isArray(data.entries) ? data.entries : [];
    var batches = data.batches || {};
    var lines = [];
    var i;

    for (i = 0; i < entries.length; i++) {
      lines.push(this._formatEntryLine(entries[i]));
    }
    this._displayLines = lines;
    this._renderConsole();

    var batchIds = Array.from(this._tracked.keys());
    for (i = 0; i < batchIds.length; i++) {
      var bid = batchIds[i];
      var info = batches[bid];
      if (!info) continue;
      var status = String(info.batch_status || '');
      if (!this._finalStatuses.has(status)) continue;
      var state = this._tracked.get(bid);
      this._tracked.delete(bid);
      if (this._onBatchFinal) {
        this._onBatchFinal(bid, {
          batch_status: status,
          story_id: info.story_id,
          movie_id: info.movie_id,
          has_video_data: info.has_video_data,
        }, state ? state.meta : {});
      }
    }
  }

  _formatEntryLine(entry) {
    var ts = this._formatTime(entry.created_at);
    var requestTag = '';
    var batchId = entry.batch_id || '';
    if (batchId && this._tracked.has(batchId)) {
      var state = this._tracked.get(batchId);
      var requestIndex = state && state.meta ? state.meta.requestIndex : 0;
      if (this._multiRequestMode && requestIndex) {
        requestTag = '[Запрос ' + requestIndex + '] ';
      }
    }
    return '[' + ts + '] ' + requestTag + String(entry.message || '');
  }

  _formatTime(iso) {
    if (!iso) return '--:--:--';
    var d = new Date(iso);
    if (isNaN(d.getTime())) return '--:--:--';
    var hh = String(d.getHours()).padStart(2, '0');
    var mm = String(d.getMinutes()).padStart(2, '0');
    var ss = String(d.getSeconds()).padStart(2, '0');
    return hh + ':' + mm + ':' + ss;
  }
}
