class GenerationConsoleController {
  constructor(opts) {
    opts = opts || {};
    this._hintEl = opts.hintId ? document.getElementById(opts.hintId) : null;
    this._consoleEl = document.getElementById(opts.consoleId || '');
    this._defaultHint = opts.defaultHint || '';
    this._maxLines = Math.max(1, parseInt(opts.maxLines, 10) || 5);
    this._pollIntervalMs = Math.max(300, parseInt(opts.pollIntervalMs, 10) || 1000);
    this._finalStatuses = new Set(opts.finalStatuses || []);
    this._onBatchFinal = typeof opts.onBatchFinal === 'function' ? opts.onBatchFinal : null;
    this._onAllIdle = typeof opts.onAllIdle === 'function' ? opts.onAllIdle : null;
    this._completionMessage = opts.completionMessage || 'Генерация завершена.';

    this._pinnedLines = [];
    this._recentLines = [];
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

  beginCreation(hintText) {
    this._resetSession();
    this._creating += 1;
    if (hintText) this._creationHint = String(hintText);
    this._refreshHint();
  }

  endCreation() {
    this._creating = Math.max(0, this._creating - 1);
    if (this._creating === 0) this._creationHint = '';
    this._refreshHint();
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

  addLine(text, opts) {
    opts = opts || {};
    var line = String(text || '').trim();
    if (!line) return;
    if (opts.pinned || this._isErrorLine(line)) {
      this._addPinnedLine(line);
    } else {
      this._addRecentLine(line);
    }
    this._renderConsole();
  }

  clearLines() {
    this._pinnedLines = [];
    this._recentLines = [];
    this._completionVisible = false;
    this._renderConsole();
  }

  trackBatch(batchId, meta) {
    var id = String(batchId || '').trim();
    if (!id) return;
    if (!this._tracked.has(id)) {
      this._tracked.set(id, {
        meta: meta || {},
        seen: new Set(),
        seenQueue: [],
        initialized: false,
      });
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
    this._pinnedLines = [];
    this._recentLines = [];
    this._completionVisible = false;
    this._multiRequestMode = false;
  }

  _addPinnedLine(line) {
    if (this._pinnedLines.indexOf(line) >= 0) return;
    this._pinnedLines.push(line);
  }

  _addRecentLine(line) {
    if (this._recentLines.indexOf(line) >= 0) return;
    this._recentLines.unshift(line);
    if (this._recentLines.length > this._maxLines) {
      this._recentLines.length = this._maxLines;
    }
  }

  _isErrorLevel(level) {
    var value = String(level || '').toLowerCase();
    return value === 'error' || value === 'fatal' || value === 'fatal_error';
  }

  _isErrorLine(text) {
    var value = String(text || '').toLowerCase();
    return value.indexOf('ошибк') >= 0 || value.indexOf('error') >= 0;
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
      this._recentLines = [];
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
    var i;

    for (i = 0; i < this._pinnedLines.length; i++) {
      parts.push(this._pinnedLines[i]);
    }
    for (i = 0; i < this._recentLines.length; i++) {
      parts.push(this._recentLines[i]);
    }

    if (this._completionVisible) {
      parts.push(this._completionMessage);
      parts.push(this._defaultHint);
    } else if (parts.length === 0) {
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
    Promise.all(ids.map(function(batchId) {
      return fetch('/api/batch/' + encodeURIComponent(batchId) + '/logs')
        .then(function(r) { return r.json(); })
        .catch(function() { return null; });
    }))
      .then(function(results) {
        for (var i = 0; i < ids.length; i++) {
          self._consumeBatchState(ids[i], results[i]);
        }
      })
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

  _consumeBatchState(batchId, data) {
    var state = this._tracked.get(batchId);
    if (!state || !data || data.error) return;

    var logs = Array.isArray(data.logs) ? data.logs : [];
    var entries = [];
    for (var i = 0; i < logs.length; i++) {
      var log = logs[i] || {};
      var logEntries = Array.isArray(log.entries) ? log.entries : [];
      for (var j = 0; j < logEntries.length; j++) {
        var e = logEntries[j] || {};
        entries.push({
          created_at: e.created_at || '',
          message: e.message || '',
          level: e.level || '',
          pipeline: log.pipeline || '',
        });
      }
    }
    entries.sort(function(a, b) {
      return String(a.created_at || '').localeCompare(String(b.created_at || ''));
    });

    if (!state.initialized) {
      state.initialized = true;
      for (var k = 0; k < entries.length; k++) {
        this._rememberEntry(state, this._entryKey(entries[k]));
      }
      if (entries.length > 0) {
        this._appendEntryLine(batchId, entries[entries.length - 1]);
      }
    } else {
      for (var n = 0; n < entries.length; n++) {
        var entry = entries[n];
        var key = this._entryKey(entry);
        if (state.seen.has(key)) continue;
        this._rememberEntry(state, key);
        this._appendEntryLine(batchId, entry);
      }
    }

    var status = String(data.batch_status || '');
    if (this._finalStatuses.has(status)) {
      this._tracked.delete(batchId);
      if (this._onBatchFinal) this._onBatchFinal(batchId, data, state.meta || {});
    }
  }

  _appendEntryLine(batchId, entry) {
    var state = this._tracked.get(batchId);
    var requestIndex = state && state.meta ? state.meta.requestIndex : 0;
    var line = this._formatEntryLine(entry, requestIndex);
    if (this._isErrorLevel(entry.level)) {
      this._addPinnedLine(line);
    } else {
      this._addRecentLine(line);
    }
    this._renderConsole();
  }

  _rememberEntry(state, key) {
    state.seen.add(key);
    state.seenQueue.push(key);
    while (state.seenQueue.length > 400) {
      var old = state.seenQueue.shift();
      state.seen.delete(old);
    }
  }

  _entryKey(entry) {
    return [
      entry.created_at || '',
      entry.pipeline || '',
      entry.level || '',
      entry.message || '',
    ].join('|');
  }

  _formatEntryLine(entry, requestIndex) {
    var ts = this._formatTime(entry.created_at);
    var prefix = '';
    if (this._multiRequestMode && requestIndex) {
      prefix = '[Запрос ' + requestIndex + '] ';
    }
    return prefix + '[' + ts + '] ' + String(entry.message || '');
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
