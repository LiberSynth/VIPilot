(function() {
  function formatEntryLine(en) {
    return fmtMsk(en.created_at) + ' — ' + (en.channel || '—') + ' — ' + (en.message || '');
  }

  const STATUS_LABELS = {
    pending:          'ожидание',
    processed:        'обработано',
    processing:       'в работе',
    running:          'выполняется',
    ok:               'готово',
    error:            'ошибка',
    ready:            'готово',
    completed:            'опубликовано',
    partially:              'частично опубликовано',
  };

  const COMPOSITE_PHASE_LABELS = {
    posting:   'публикуется',
    completed: 'опубликовано',
    pending:   'ожидание публикации',
    failed:    'ошибка публикации',
  };

  const PLATFORM_LABELS = {
    dzen:    'Дзен',
    vk:      'ВКонтакте',
    rutube:  'Rutube',
    vkvideo: 'VK Видео',
  };

  const TYPE_RESTARTABLE = ['story', 'movie', 'transcode', 'publish'];
  const TYPE_LINKED_MEDIA = ['transcode', 'publish', 'planning'];
  const PIPELINE_CHAIN_TYPES = ['story', 'movie', 'planning', 'transcode', 'publish'];
  const TYPE_TO_RESET_PIPELINE = {
    story:     'story',
    movie:     'video',
    transcode: 'transcode',
    publish:   'publish',
  };
  const TYPE_ERROR_STATUSES = ['error', 'fatal_error'];
  const FINAL_BATCH_STATUSES = ['completed', 'partially', 'ready', 'error', 'fatal_error'];
  const MONITOR_POLL_MS = 200;
  const BATCH_ENTRIES_POLL_MS = 200;
  const SYSTEM_ENTRIES_POLL_MS = 200;

  const MON_SVG_COPY     = `<svg viewBox="0 0 16 16"><rect x="5" y="5" width="9" height="9" rx="1.5"/><path d="M3 11V3a1 1 0 0 1 1-1h8"/></svg>`;
  const MON_SVG_RESTART  = `<svg viewBox="0 0 16 16" fill="none" stroke-linecap="round" stroke-linejoin="round"><polyline points="15.3,2.7 15.3,6.7 11.3,6.7"/><path d="M13.66 10a6 6 0 1 1-.08-5"/></svg>`;
  const MON_SVG_EYE      = `<svg viewBox="0 0 16 16"><rect x="3" y="2" width="10" height="12" rx="1.5"/><line x1="5.5" y1="5.5" x2="10.5" y2="5.5"/><line x1="5.5" y1="8" x2="10.5" y2="8"/><line x1="5.5" y1="10.5" x2="8.5" y2="10.5"/></svg>`;
  const MON_SVG_PLAY     = `<svg viewBox="0 0 16 16"><polygon points="4,2 13,8 4,14"/></svg>`;
  const MON_SVG_INFO     = `<svg viewBox="0 0 16 16"><circle cx="8" cy="8" r="6.2"/><line x1="8" y1="5.5" x2="8" y2="5.5"/><line x1="8" y1="7.5" x2="8" y2="11"/></svg>`;
  const MON_SVG_DELETE   = `<svg viewBox="0 0 16 16"><polyline points="2,4 14,4"/><path d="M5 4V2h6v2"/><rect x="3" y="4" width="10" height="10" rx="1.5"/><line x1="6" y1="7" x2="6" y2="11"/><line x1="10" y1="7" x2="10" y2="11"/></svg>`;
  const MON_SVG_CHAIN_PREV = `<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polyline points="3,6 8,11 13,6"/></svg>`;
  const MON_SVG_CHAIN_NEXT = `<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polyline points="3,10 8,5 13,10"/></svg>`;

  function translateStatus(s) {
    if (STATUS_LABELS[s]) return STATUS_LABELS[s];
    if (s.indexOf('.') >= 0) {
      var parts = s.split('.');
      var phase = parts[parts.length - 1];
      if (COMPOSITE_PHASE_LABELS[phase]) {
        var platformSlug = parts[0];
        var platformName = PLATFORM_LABELS[platformSlug] || platformSlug;
        return platformName + ' · ' + COMPOSITE_PHASE_LABELS[phase];
      }
    }
    return s;
  }

  function esc(s) {
    return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  function fmtMsk(iso) {
    if (!iso) return '—';
    return new Date(iso).toLocaleString('ru-RU', {
      day: '2-digit', month: '2-digit', year: 'numeric',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
      timeZone: 'Europe/Moscow',
    });
  }

  function fmtMskShort(iso) {
    if (!iso) return '—';
    return new Date(iso).toLocaleString('ru-RU', {
      day: '2-digit', month: '2-digit', year: 'numeric',
      hour: '2-digit', minute: '2-digit',
      timeZone: 'Europe/Moscow',
    });
  }

  function renderEntries(entries) {
    if (!entries || entries.length === 0) return '';
    return '<div class="monitor-entries">' +
      entries.map(function(e) {
        return _buildEntryRow(e);
      }).join('') +
    '</div>';
  }

  function _batchDotClass(bs, batchId) {
    const doneStatuses    = ['completed', 'ready'];
    const partialStatuses = ['partially'];
    const waitStatuses    = ['pending', 'processing', 'processed'];
    const errorStatuses   = ['error', 'fatal_error'];
    const finalStatuses   = doneStatuses.concat(partialStatuses).concat(errorStatuses);
    const isFinal         = finalStatuses.indexOf(bs) >= 0;
    const isActive        = !isFinal && _activeBatchIds.indexOf(batchId) >= 0;
    if (isActive) return 'md-active';
    if (doneStatuses.indexOf(bs) >= 0) return 'md-ok';
    if (partialStatuses.indexOf(bs) >= 0) return 'md-partial';
    if (errorStatuses.indexOf(bs) >= 0) return 'md-error';
    if (waitStatuses.indexOf(bs) >= 0) return 'md-wait';
    return 'md-warn';
  }

  function capitalizeFirst(s) {
    if (!s) return s;
    return s.charAt(0).toUpperCase() + s.slice(1);
  }

  function formatEntryCount(n) {
    n = Number(n) || 0;
    var mod10 = n % 10;
    var mod100 = n % 100;
    var word = 'записей';
    if (mod10 === 1 && mod100 !== 11) word = 'запись';
    else if (mod10 >= 2 && mod10 <= 4 && (mod100 < 10 || mod100 >= 20)) word = 'записи';
    return n + ' ' + word;
  }

  const FIXED_BATCH_TITLES = {
    planning:  'Планирование',
    story:     'Генерация сюжета',
    movie:     'Генерация видео',
    transcode: 'Транскодирование',
    publish:   'Публикация',
  };

  function buildFixedBatchSub(_batch, _btype, bs, entryCnt) {
    return capitalizeFirst(translateStatus(bs)) + ' · ' + entryCnt;
  }

  function fmtMskSlotTime(iso) {
    if (!iso) return '—';
    return new Date(iso).toLocaleString('ru-RU', {
      hour: '2-digit', minute: '2-digit',
      timeZone: 'Europe/Moscow',
    });
  }

  function formatPublishTimeLabel(batch) {
    return batch.scheduled_at ? fmtMskSlotTime(batch.scheduled_at) : 'сейчас';
  }

  function renderBatch(batch) {
    const bs = batch.batch_status || 'pending';
    const btype = batch.type || '';
    const headTime = batch.created_at;
    const isScheduledPlanning = btype === 'planning' && !!batch.scheduled_at;
    const fixedBatchTitle = FIXED_BATCH_TITLES[btype];
    var headTitle;
    if (fixedBatchTitle) {
      headTitle = (btype === 'planning' || btype === 'publish')
        ? fixedBatchTitle + ' (' + formatPublishTimeLabel(batch) + ')'
        : fixedBatchTitle;
    } else {
      headTitle = fmtMsk(headTime);
    }
    const entryCnt = formatEntryCount(batch.entry_count);
    var sub;
    if (fixedBatchTitle) {
      sub = buildFixedBatchSub(batch, btype, bs, entryCnt);
    } else {
      var subParts = [translateStatus(bs)];
      if (batch.title) subParts.push(batch.title);
      sub = capitalizeFirst(subParts.filter(Boolean).join(' · ')) + ' · ' + entryCnt;
    }
    const md = _batchDotClass(bs, batch.batch_id);
    const isActive = md === 'md-active';

    const resetPipeline = TYPE_TO_RESET_PIPELINE[btype];
    const canRestart = batch.batch_id
      && resetPipeline
      && TYPE_RESTARTABLE.indexOf(btype) >= 0
      && TYPE_ERROR_STATUSES.indexOf(bs) >= 0;
    const restartBtn = canRestart
      ? '<button class="cycle-float-btn" title="Перезапустить" data-bid="' + esc(batch.batch_id) + '" data-pip="' + esc(resetPipeline) + '" onclick="monitorPipelineRestart(this)">' + MON_SVG_RESTART + '</button>'
      : '';

    const hasLinkedMedia = TYPE_LINKED_MEDIA.indexOf(btype) >= 0;
    const previewDisabled = (btype === 'story' || btype === 'movie') && bs !== 'ready';
    const batchStoryBtn = batch.story_id && (btype === 'story' || hasLinkedMedia)
      ? '<button class="cycle-float-btn story-view-btn" title="Посмотреть сюжет"' +
        (previewDisabled ? ' disabled' : '') +
        ' onclick="openStoryModal(\'' + esc(batch.story_id) + '\',\'\')">' + MON_SVG_EYE + '</button>'
      : '';

    const batchVideoBtn = batch.movie_id && (btype === 'movie' || hasLinkedMedia)
      ? '<button class="cycle-float-btn" title="Просмотр видео"' +
        (previewDisabled ? ' disabled' : '') +
        ' onclick="openVideoModal(\'' + esc(batch.batch_id) + '\',\'\')">' + MON_SVG_PLAY + '</button>'
      : '';

    const chainNavBtns = PIPELINE_CHAIN_TYPES.indexOf(btype) >= 0
      ? '<button class="cycle-float-btn monitor-chain-nav" data-dir="prev" title="Предыдущий батч" disabled onclick="monitorChainNav(event,\'prev\')">' + MON_SVG_CHAIN_PREV + '</button>' +
        '<button class="cycle-float-btn monitor-chain-nav" data-dir="next" title="Следующий батч" disabled onclick="monitorChainNav(event,\'next\')">' + MON_SVG_CHAIN_NEXT + '</button>'
      : '';

    const hdrActionBtns =
      chainNavBtns +
      restartBtn +
      batchStoryBtn +
      batchVideoBtn +
      '<button class="cycle-float-btn" title="Скопировать логи" onclick="monitorCopy(this)">'          + MON_SVG_COPY + '</button>' +
      '<button class="cycle-float-btn" title="Скопировать инфо" onclick="monitorBatchCopyInfo(this)">' + MON_SVG_INFO + '</button>' +
      (isActive || FINAL_BATCH_STATUSES.indexOf(bs) === -1
        ? '<button class="cycle-float-btn btn-blocked" title="Удалить батч" data-warn="1" onclick="monitorDeleteBatch(\'' + esc(batch.batch_id) + '\',this)">' + MON_SVG_DELETE + '</button>'
        : '<button class="cycle-float-btn" title="Удалить батч" onclick="monitorDeleteBatch(\'' + esc(batch.batch_id) + '\',this)">' + MON_SVG_DELETE + '</button>');

    const hdrTools =
      '<div class="monitor-header-tools" onclick="event.stopPropagation()">' +
        '<div class="monitor-hdr-actions-always">' + hdrActionBtns + '</div>' +
        '<span class="monitor-batch-arrow">▼</span>' +
      '</div>';

    const frameHtml = (btype === 'publish' && batch.batch_id)
      ? '<div class="monitor-pub-frame">' +
          '<canvas class="monitor-pub-canvas" data-bid="' + esc(batch.batch_id) + '" ' +
          'width="1280" height="720"></canvas>' +
        '</div>'
      : '';

    var chainClass = '';
    if (_openChainIds && _openChainIds.length >= 2 && _openChainIds.indexOf(batch.batch_id) >= 0) {
      chainClass = ' chain-linked' + (batch.batch_id === _openBid ? ' chain-anchor' : '');
    }

    return '<div class="monitor-batch bs-' + esc(bs) + chainClass + '" data-bid="' + esc(batch.batch_id) +
      '" data-log-id="'   + esc(batch.log_id || '') +
      '" data-type="'       + esc(btype) +
      '" data-scheduled="'  + esc(isScheduledPlanning ? fmtMsk(batch.scheduled_at) : 'сейчас') +
      '" data-bstatus="'    + esc(bs) +
      '" onclick="monitorToggleBatch(event,this)">' +
      '<div class="monitor-batch-header">' +
        '<span class="monitor-dot ' + md + '"></span>' +
        '<div class="monitor-batch-meta">' +
          '<div class="monitor-batch-title">' + esc(headTitle) + '</div>' +
          '<div class="monitor-batch-sub">'   + esc(sub)         + '</div>' +
        '</div>' +
        hdrTools +
      '</div>' +
      '<div class="monitor-batch-body">' + frameHtml + '</div>' +
    '</div>';
  }

  function renderSystemBlock(sys) {
    const sysActions =
      '<div class="monitor-hdr-actions-always" onclick="event.stopPropagation()">' +
        '<button class="cycle-float-btn" title="Скопировать логи" onclick="monitorSystemCopy(this)">' + MON_SVG_COPY + '</button>' +
        '<button class="cycle-float-btn" title="Скопировать инфо" onclick="monitorSystemCopyInfo(this)">' + MON_SVG_INFO + '</button>' +
      '</div>';

    return '<div class="monitor-sysgroup monitor-system-block" data-lid="' + esc(sys.id) +
      '" onclick="monitorToggleSystemBlock(event,this)">' +
      '<div class="monitor-sysgroup-header">' +
        '<span class="monitor-sysgroup-dot"></span>' +
        '<div class="monitor-sysgroup-meta">' +
          '<div class="monitor-sysgroup-title">Приложение</div>' +
          '<div class="monitor-sysgroup-sub">' + esc(formatEntryCount(sys.entry_count)) + '</div>' +
        '</div>' +
        sysActions +
        '<span class="monitor-sysgroup-arrow">▼</span>' +
      '</div>' +
      '<div class="monitor-sysgroup-body"></div>' +
    '</div>';
  }

  function buildTimeline(batches, system) {
    var items = [];
    (batches || []).forEach(function(b) {
      items.push({ type: 'batch', time: b.created_at, data: b });
    });
    (system || []).forEach(function(s) {
      items.push({ type: 'system', time: s.created_at, data: s });
    });
    items.sort(function(a, b) { return new Date(b.time) - new Date(a.time); });
    return items;
  }

  var _openBid              = null;
  var _openSysLid           = null;
  var _openChainIds         = null;
  var _activeBatchIds       = [];
  var _batchEntriesCache    = {};
  var _batchChainCache      = {};
  var _batchEntriesFetching = {};
  var _sysLogEntriesCache   = {};
  var _sysLogFetching       = {};
  var _pubViewers = {};
  var _lastRenderedHtml     = {};
  var _lastMonitorData      = null;

  function _buildPipelineChainIds(focusBid, batches) {
    if (!focusBid || !batches || !batches.length) return [];
    var byId = {};
    var children = {};
    batches.forEach(function(b) {
      byId[b.batch_id] = b;
      if (b.batch_id_source) {
        if (!children[b.batch_id_source]) children[b.batch_id_source] = [];
        children[b.batch_id_source].push(b);
      }
    });
    var focus = byId[focusBid];
    if (!focus || PIPELINE_CHAIN_TYPES.indexOf(focus.type) < 0) return [];

    var ancestors = [];
    var seen = {};
    var cur = focus;
    while (cur) {
      var src = cur.batch_id_source;
      if (!src || seen[src]) break;
      seen[src] = true;
      var parent = byId[src];
      if (!parent || PIPELINE_CHAIN_TYPES.indexOf(parent.type) < 0) break;
      ancestors.unshift(parent);
      cur = parent;
    }

    var chain = ancestors.concat([focus]);
    var tail = focus;
    while (true) {
      var tidx = PIPELINE_CHAIN_TYPES.indexOf(tail.type);
      if (tidx < 0 || tidx >= PIPELINE_CHAIN_TYPES.length - 1) break;
      var nextType = PIPELINE_CHAIN_TYPES[tidx + 1];
      var kids = (children[tail.batch_id] || []).filter(function(k) { return k.type === nextType; });
      if (!kids.length) break;
      kids.sort(function(a, b) {
        return new Date(b.created_at || 0) - new Date(a.created_at || 0);
      });
      tail = kids[0];
      chain.push(tail);
    }

    return chain.length >= 2 ? chain.map(function(b) { return b.batch_id; }) : [];
  }

  function _findMonitorBatch(focusBid) {
    if (!_lastMonitorData || !_lastMonitorData.batches) return null;
    var batches = _lastMonitorData.batches;
    for (var i = 0; i < batches.length; i++) {
      if (batches[i].batch_id === focusBid) return batches[i];
    }
    return null;
  }

  function _resolveOpenChainIds(focusBid) {
    if (_batchChainCache.hasOwnProperty(focusBid) && _batchChainCache[focusBid].length >= 2) {
      return _batchChainCache[focusBid];
    }
    var batch = _findMonitorBatch(focusBid);
    if (batch && batch.pipeline_chain_ids && batch.pipeline_chain_ids.length >= 2) {
      return batch.pipeline_chain_ids;
    }
    if (_lastMonitorData && _lastMonitorData.batches) {
      return _buildPipelineChainIds(focusBid, _lastMonitorData.batches);
    }
    return null;
  }

  function _refreshMonitorView() {
    if (_lastMonitorData) {
      renderTimeline(_lastMonitorData);
      return;
    }
    applyChainHighlight();
  }

  function applyChainHighlight() {
    document.querySelectorAll('.monitor-batch.chain-linked').forEach(function(el) {
      el.classList.remove('chain-linked', 'chain-anchor');
    });
    if (!_openBid) {
      _openChainIds = null;
      updateChainNavButtons();
      return;
    }
    if (!_openChainIds || _openChainIds.length < 2) {
      _openChainIds = _resolveOpenChainIds(_openBid);
    }
    if (!_openChainIds || _openChainIds.length < 2) {
      updateChainNavButtons();
      return;
    }
    _openChainIds.forEach(function(id) {
      var el = document.querySelector('.monitor-batch[data-bid="' + id + '"]');
      if (!el) return;
      el.classList.add('chain-linked');
      if (id === _openBid) el.classList.add('chain-anchor');
    });
    updateChainNavButtons();
  }

  function updateChainNavButtons() {
    if (!_openBid) return;
    var openEl = document.querySelector('.monitor-batch.open[data-bid="' + _openBid + '"]');
    if (!openEl) return;
    var idx = _openChainIds ? _openChainIds.indexOf(_openBid) : -1;
    var hasChain = idx >= 0 && _openChainIds && _openChainIds.length >= 2;
    openEl.querySelectorAll('.monitor-chain-nav').forEach(function(btn) {
      if (!hasChain) {
        btn.disabled = true;
        return;
      }
      btn.disabled = btn.dataset.dir === 'prev' ? idx <= 0 : idx >= _openChainIds.length - 1;
    });
  }

  function _groupKey(item) {
    if (item.type === 'batch') return 'batch:' + item.data.batch_id;
    return 'sys:' + item.data.id;
  }

  function getOpenState() {
    var openBids = {};
    document.querySelectorAll('.monitor-batch.open').forEach(function(el) {
      if (el.dataset.bid) openBids[el.dataset.bid] = true;
    });
    var openSys = {};
    document.querySelectorAll('.monitor-system-block.open').forEach(function(el) {
      if (el.dataset.lid) openSys[el.dataset.lid] = true;
    });
    return { openBids: openBids, openSys: openSys };
  }

  function restoreOpenState(state) {
    if (_openBid) {
      if (_batchChainCache.hasOwnProperty(_openBid)) {
        _openChainIds = _batchChainCache[_openBid];
      }
      var batchEl = document.querySelector('.monitor-batch[data-bid="' + _openBid + '"]');
      if (batchEl) {
        batchEl.classList.add('open');
        if (_batchEntriesCache[_openBid]) {
          _applyBatchEntries(batchEl, _batchEntriesCache[_openBid]);
        }
      } else {
        _openBid = null;
      }
    }
    document.querySelectorAll('.monitor-system-block').forEach(function(el) {
      var lid = el.dataset.lid;
      if (lid && state.openSys[lid]) {
        el.classList.add('open');
        if (_sysLogEntriesCache[lid]) _applySystemEntries(el, _sysLogEntriesCache[lid]);
      }
    });
  }

  function _buildEntryRow(en) {
    var lvl = en.level || 'info';
    return '<div class="monitor-entry-row">' +
      '<span class="monitor-entry-ts">' + fmtMsk(en.created_at) + '</span>' +
      '<span class="monitor-entry-ch">' + esc(en.channel || '—') + '</span>' +
      '<span class="monitor-entry-msg ' + esc(lvl) + '">' + esc(en.message || '') + '</span>' +
    '</div>';
  }

  function _applyBatchEntries(batchEl, entries) {
    var body = batchEl.querySelector('.monitor-batch-body');
    if (!body) return;
    var existing = body.querySelector('.monitor-entries');
    if (existing) existing.remove();
    if (!entries || !entries.length) return;
    body.insertAdjacentHTML('beforeend', renderEntries(entries));
  }

  function _applySystemEntries(sysEl, entries) {
    var body = sysEl.querySelector('.monitor-sysgroup-body');
    if (!body) return;
    body.innerHTML = renderEntries(entries);
  }

  function _injectBatchEntries(batchEl, entries) {
    var body = batchEl.querySelector('.monitor-batch-body');
    if (!body) return;
    var existingDiv = body.querySelector('.monitor-entries');
    if (!existingDiv) {
      body.insertAdjacentHTML('beforeend', renderEntries(entries));
      return;
    }
    var existingCount = existingDiv.querySelectorAll('.monitor-entry-row').length;
    var totalNew = entries.length - existingCount;
    if (totalNew > 0) {
      var newEntries = entries.slice(0, totalNew);
      existingDiv.insertAdjacentHTML('afterbegin', newEntries.map(_buildEntryRow).join(''));
    }
  }

  function _fetchAndInjectEntries(bid) {
    var batchEl = document.querySelector('.monitor-batch[data-bid="' + bid + '"]');
    if (!batchEl) return;
    if (_batchEntriesFetching[bid]) return;
    _batchEntriesFetching[bid] = true;
    fetch('/api/monitor/batch/' + encodeURIComponent(bid) + '/entries')
      .then(function(r) { return r.json(); })
      .then(function(data) {
        delete _batchEntriesFetching[bid];
        if (_openBid !== bid) return;
        var entries = data.entries || [];
        _openChainIds = Array.isArray(data.pipeline_chain_ids) ? data.pipeline_chain_ids : [];
        if (_openChainIds.length < 2 && _lastMonitorData && _lastMonitorData.batches) {
          _openChainIds = _buildPipelineChainIds(bid, _lastMonitorData.batches);
        }
        _batchEntriesCache[bid] = entries;
        _batchChainCache[bid] = _openChainIds;
        var batchEl2 = document.querySelector('.monitor-batch[data-bid="' + bid + '"]');
        if (batchEl2) _injectBatchEntries(batchEl2, entries);
        if (_openBid === bid) _refreshMonitorView();
      })
      .catch(function() { delete _batchEntriesFetching[bid]; });
  }

  function _fetchSystemEntries(lid) {
    if (_sysLogFetching[lid]) return;
    _sysLogFetching[lid] = true;
    fetch('/api/monitor/log/' + encodeURIComponent(lid) + '/entries')
      .then(function(r) { return r.json(); })
      .then(function(data) {
        delete _sysLogFetching[lid];
        var entries = data.entries || [];
        _sysLogEntriesCache[lid] = entries;
        if (_openSysLid !== lid) return;
        var el = document.querySelector('.monitor-system-block[data-lid="' + lid + '"]');
        if (!el) return;
        _applySystemEntries(el, entries);
      })
      .catch(function() { delete _sysLogFetching[lid]; });
  }

  var PUB_VIEWPORT_W = 1280;
  var PUB_VIEWPORT_H = 720;

  function _findPubCanvas(bid) {
    return document.querySelector(
      '.monitor-batch.open[data-type="publish"] .monitor-pub-canvas[data-bid="' + bid + '"]'
    );
  }

  function _disconnectPubViewer(bid) {
    var v = _pubViewers[bid];
    if (!v) return;
    if (v.sse) { v.sse.close(); v.sse = null; }
    delete _pubViewers[bid];
  }

  function _disconnectAllPubViewers() {
    Object.keys(_pubViewers).forEach(_disconnectPubViewer);
  }

  function _handlePubViewerStopped(bid) {
    var v = _pubViewers[bid];
    if (!v) return;
    if (v.sse) { v.sse.close(); v.sse = null; }
    v.stopped = true;
    if (v.ctx) {
      v.ctx.clearRect(0, 0, PUB_VIEWPORT_W, PUB_VIEWPORT_H);
    }
  }

  function _connectPubViewer(bid) {
    var canvas = _findPubCanvas(bid);
    if (!canvas) return;

    var v = _pubViewers[bid];
    if (_activeBatchIds.indexOf(bid) < 0) {
      if (v && v.sse) { v.sse.close(); v.sse = null; }
      return;
    }

    if (v && v.canvas === canvas && v.sse) return;
    if (v && v.sse) { v.sse.close(); v.sse = null; }

    if (!v) v = {};
    v.stopped = false;

    if (v.canvas !== canvas) {
      canvas.width = PUB_VIEWPORT_W;
      canvas.height = PUB_VIEWPORT_H;
      v.ctx = canvas.getContext('2d');
      v.canvas = canvas;
    }

    var sse = new EventSource('/api/batch/' + encodeURIComponent(bid) + '/publish-stream');
    v.sse = sse;
    _pubViewers[bid] = v;

    sse.onmessage = function(e) {
      var data = e.data;
      if (data === 'STOPPED') {
        _handlePubViewerStopped(bid);
        return;
      }
      if (!data || data.charAt(0) === ':') return;
      var viewer = _pubViewers[bid];
      if (!viewer || !viewer.ctx) return;
      var img = new Image();
      img.onload = function() {
        viewer.ctx.drawImage(img, 0, 0);
      };
      img.src = 'data:image/jpeg;base64,' + data;
    };
    sse.onerror = function() {};
  }

  function syncPubFrameStreams() {
    var alive = {};
    document.querySelectorAll(
      '.monitor-batch.open[data-type="publish"] .monitor-pub-canvas[data-bid]'
    ).forEach(function(canvas) {
      var bid = canvas.dataset.bid;
      if (!bid) return;
      alive[bid] = true;
      _connectPubViewer(bid);
    });
    Object.keys(_pubViewers).forEach(function(bid) {
      if (!alive[bid]) _disconnectPubViewer(bid);
    });
  }

  function renderTimeline(data) {
    var el = document.getElementById('monitor-timeline');
    if (!el) return;
    _lastMonitorData = data;
    _activeBatchIds = Array.isArray(data.active_batch_ids) ? data.active_batch_ids : [];
    var prev = getOpenState();
    var items = buildTimeline(data.batches, data.system);
    if (items.length === 0) {
      el.innerHTML = '<div style="font-size:12px;color:#444;padding:4px 0">Нет данных</div>';
      return;
    }

    var newHtmlMap = {};
    var newItemMap = {};
    var newKeys = [];
    items.forEach(function(item) {
      var key = _groupKey(item);
      newKeys.push(key);
      newItemMap[key] = item;
      newHtmlMap[key] = item.type === 'batch'
        ? renderBatch(item.data)
        : renderSystemBlock(item.data);
    });

    var oldEls = {};
    Array.prototype.slice.call(el.children).forEach(function(child) {
      var bid = child.dataset.bid;
      var lid = child.dataset.lid;
      var key = bid ? ('batch:' + bid) : lid ? ('sys:' + lid) : null;
      if (key) oldEls[key] = child;
    });

    var keysSet = {};
    newKeys.forEach(function(k) { keysSet[k] = true; });
    Object.keys(oldEls).forEach(function(key) {
      if (!keysSet[key]) {
        el.removeChild(oldEls[key]);
        delete _lastRenderedHtml[key];
      }
    });

    var resolvedNodes = newKeys.map(function(key) {
      var existing = oldEls[key];
      var newHtml = newHtmlMap[key];
      var item = newItemMap[key];
      if (existing) {
        if (_lastRenderedHtml[key] !== newHtml) {
          var isOpen = existing.classList.contains('open');
          var tmp = document.createElement('div');
          tmp.innerHTML = newHtml;
          var newNode = tmp.firstChild;
          if (isOpen) {
            newNode.classList.add('open');
            if (item.type === 'batch') {
              var oldEntries = existing.querySelector('.monitor-entries');
              if (oldEntries) {
                var body = newNode.querySelector('.monitor-batch-body');
                if (body) body.appendChild(oldEntries);
              }
              var oldFrame = existing.querySelector('.monitor-pub-frame');
              if (oldFrame) {
                var newBody = newNode.querySelector('.monitor-batch-body');
                var newFrame = newBody && newBody.querySelector('.monitor-pub-frame');
                if (newBody && newFrame) {
                  newBody.replaceChild(oldFrame, newFrame);
                }
              }
            } else {
              var oldBody = existing.querySelector('.monitor-sysgroup-body');
              var newBody = newNode.querySelector('.monitor-sysgroup-body');
              if (oldBody && newBody && oldBody.innerHTML.trim()) {
                newBody.innerHTML = oldBody.innerHTML;
              }
            }
          }
          el.replaceChild(newNode, existing);
          _lastRenderedHtml[key] = newHtml;
          return newNode;
        }
        return existing;
      }
      var tmp2 = document.createElement('div');
      tmp2.innerHTML = newHtml;
      _lastRenderedHtml[key] = newHtml;
      return tmp2.firstChild;
    });

    var needsReorder = resolvedNodes.some(function(node, i) {
      return el.children[i] !== node;
    });
    if (needsReorder) {
      var frag = document.createDocumentFragment();
      resolvedNodes.forEach(function(node) { frag.appendChild(node); });
      el.appendChild(frag);
    }

    restoreOpenState(prev);
    applyChainHighlight();
    syncPubFrameStreams();
  }

  function refreshMonitor() {
    var panel = document.getElementById('panel-log');
    if (!panel || !panel.classList.contains('active')) return;
    fetch('/api/monitor')
      .then(function(r) { return r.json(); })
      .then(renderTimeline)
      .catch(function() {});
  }

  window.monitorRefresh = refreshMonitor;

  function _formatMonitorEntryLines(entries) {
    return (entries || []).map(formatEntryLine);
  }

  function _monitorCopyText(text, btn) {
    window.clipboardWrite(text, function() {
      btn.classList.add('copied');
      setTimeout(function() { btn.classList.remove('copied'); }, 200);
    });
  }

  function _batchInfoLines(batchEl) {
    if (!batchEl) return [];
    return [
      'batch_id: ' + (batchEl.dataset.bid    || ''),
      'log_id: '   + (batchEl.dataset.logId  || ''),
      'type: '     + (batchEl.dataset.type   || ''),
      'status: '   + (batchEl.dataset.bstatus || ''),
    ];
  }

  function _systemInfoLines(sysEl) {
    if (!sysEl) return [];
    return [
      'log_id: ' + (sysEl.dataset.lid || ''),
    ];
  }

  window.monitorToggleBatch = function(e, el) {
    if (e.target.closest('.monitor-batch-body')) return;
    var bid = el.dataset.bid;
    var isOpen = el.classList.contains('open');
    document.querySelectorAll('.monitor-batch.open').forEach(function(b) {
      b.classList.remove('open');
    });
    if (isOpen) {
      _openBid = null;
      _openChainIds = null;
      _refreshMonitorView();
    } else {
      document.querySelectorAll('.monitor-system-block.open').forEach(function(b) {
        b.classList.remove('open');
      });
      _openSysLid = null;
      el.classList.add('open');
      _openBid = bid || null;
      _openChainIds = _resolveOpenChainIds(_openBid);
      _refreshMonitorView();
      if (_openBid) {
        var openEl = document.querySelector('.monitor-batch[data-bid="' + _openBid + '"]');
        if (_batchEntriesCache[_openBid]) {
          if (openEl) _applyBatchEntries(openEl, _batchEntriesCache[_openBid]);
          if (!_batchChainCache.hasOwnProperty(_openBid)) {
            _fetchAndInjectEntries(_openBid);
          }
        } else {
          _fetchAndInjectEntries(_openBid);
        }
      }
    }
    syncPubFrameStreams();
  };

  window.monitorToggleSystemBlock = function(e, el) {
    if (e.target.closest('.monitor-sysgroup-body')) return;
    var lid = el.dataset.lid;
    var isOpen = el.classList.contains('open');
    document.querySelectorAll('.monitor-system-block.open').forEach(function(b) {
      b.classList.remove('open');
    });
    if (isOpen) {
      _openSysLid = null;
    } else {
      document.querySelectorAll('.monitor-batch.open').forEach(function(b) {
        b.classList.remove('open');
      });
      _openBid = null;
      _openChainIds = null;
      _refreshMonitorView();
      el.classList.add('open');
      _openSysLid = lid || null;
      if (_openSysLid) {
        if (_sysLogEntriesCache[_openSysLid]) {
          _applySystemEntries(el, _sysLogEntriesCache[_openSysLid]);
        }
        _fetchSystemEntries(_openSysLid);
      }
    }
    syncPubFrameStreams();
  };

  window.monitorSystemCopyInfo = function(btn) {
    _monitorCopyText(_systemInfoLines(btn.closest('.monitor-system-block')).join('\n'), btn);
  };

  window.monitorSystemCopy = function(btn) {
    var block = btn.closest('.monitor-system-block');
    if (!block) return;
    var infoText = _systemInfoLines(block).join('\n');
    var logId = block.dataset.lid || '';

    function finish(entryLines) {
      var text = infoText;
      if (entryLines.length) text += '\n\n' + entryLines.join('\n');
      _monitorCopyText(text, btn);
    }

    if (!logId) {
      finish([]);
      return;
    }

    fetch('/api/monitor/log/' + encodeURIComponent(logId) + '/entries')
      .then(function(r) { return r.json(); })
      .then(function(data) { finish(_formatMonitorEntryLines(data.entries || [])); })
      .catch(function() { finish([]); });
  };

  window.monitorCopy = function(btn) {
    var batchEl = btn.closest('.monitor-batch');
    if (!batchEl) return;
    var infoText = _batchInfoLines(batchEl).join('\n');
    var batchId = batchEl.dataset.bid || '';

    function finish(entryLines) {
      var text = infoText;
      if (entryLines.length) text += '\n\n' + entryLines.join('\n');
      _monitorCopyText(text, btn);
    }

    if (!batchId) {
      finish([]);
      return;
    }

    fetch('/api/monitor/batch/' + encodeURIComponent(batchId) + '/entries')
      .then(function(r) { return r.json(); })
      .then(function(data) { finish(_formatMonitorEntryLines(data.entries || [])); })
      .catch(function() { finish([]); });
  };

  window.monitorBatchCopyInfo = function(btn) {
    _monitorCopyText(_batchInfoLines(btn.closest('.monitor-batch')).join('\n'), btn);
  };

  window.monitorChainNav = function(e, dir) {
    e.stopPropagation();
    if (!_openChainIds || _openChainIds.length < 2 || !_openBid) return;
    var idx = _openChainIds.indexOf(_openBid);
    if (idx < 0) return;
    var nextIdx = dir === 'prev' ? idx - 1 : idx + 1;
    if (nextIdx < 0 || nextIdx >= _openChainIds.length) return;
    var targetId = _openChainIds[nextIdx];
    var targetEl = document.querySelector('.monitor-batch[data-bid="' + targetId + '"]');
    if (!targetEl) return;
    document.querySelectorAll('.monitor-batch.open').forEach(function(b) {
      b.classList.remove('open');
    });
    targetEl.classList.add('open');
    _openBid = targetId;
    _openChainIds = _batchChainCache[_openBid] || _openChainIds;
    if (_batchEntriesCache[_openBid]) {
      _applyBatchEntries(targetEl, _batchEntriesCache[_openBid]);
    } else {
      _fetchAndInjectEntries(_openBid);
    }
    _refreshMonitorView();
    var scrolled = document.querySelector('.monitor-batch[data-bid="' + targetId + '"]');
    if (scrolled) scrolled.scrollIntoView({ behavior: 'smooth', block: 'center' });
    syncPubFrameStreams();
  };

  window.monitorPipelineRestart = function(btn) {
    var batchId  = btn.dataset.bid;
    var pipeline = btn.dataset.pip;
    if (!batchId || !pipeline) return;
    btn.disabled = true;
    btn.classList.add('pending');
    fetch('/api/batch/' + encodeURIComponent(batchId) + '/reset/' + encodeURIComponent(pipeline), { method: 'POST' })
      .then(function(r) { return r.json(); })
      .then(function(data) {
        btn.classList.remove('pending');
        if (data.ok) {
          btn.classList.add('copied');
          setTimeout(function() {
            btn.classList.remove('copied');
            refreshMonitor();
          }, 200);
        } else {
          btn.disabled = false;
          alert(data.error || 'Ошибка');
        }
      })
      .catch(function() {
        btn.classList.remove('pending');
        btn.disabled = false;
      });
  };

  window.monitorDeleteBatch = function(batchId, btn) {
    var isBlocked = !!(btn && btn.dataset && btn.dataset.warn);
    if (isBlocked) {
      new ConfirmDialog({
        title:       'Удаление невозможно',
        text:
          '<div class="confirm-box-warn">Батч находится в активном статусе. Удаление заблокировано — дождитесь завершения обработки.</div>' +
          'Сюжеты (stories) не затрагиваются.',
        cancelLabel: 'Закрыть',
      }).open();
    } else {
      new ConfirmDialog({
        title:        'Удалить батч?',
        text:
          'Батч и его лог будут удалены без возможности восстановления.<br><br>' +
          'Видео (movies), сюжеты (stories) и файлы в video/ не затрагиваются.',
        confirmLabel: 'Удалить',
        onConfirm: function(confirmBtn, dlg) {
          confirmBtn.disabled    = true;
          confirmBtn.textContent = 'Удаляем…';
          fetch('/api/monitor/batch/' + encodeURIComponent(batchId) + '/delete', { method: 'POST' })
            .then(function(r) { return r.json(); })
            .then(function(data) {
              dlg.close();
              if (data.ok) {
                delete _batchEntriesCache[batchId];
                delete _batchChainCache[batchId];
                var batchEl = document.querySelector('.monitor-batch[data-bid="' + batchId + '"]');
                if (batchEl) batchEl.remove();
                if (_openBid === batchId) {
                  _openBid = null;
                  _openChainIds = null;
                  applyChainHighlight();
                }
                showToast('Батч удалён', 'success');
              } else {
                showToast('Ошибка: ' + (data.error || 'неизвестная ошибка'), 'error');
              }
            })
            .catch(function() { dlg.close(); showToast('Ошибка соединения', 'error'); });
        },
      }).open();
    }
  };

  function refreshOpenBatchEntries() {
    if (_openBid) _fetchAndInjectEntries(_openBid);
  }

  function refreshOpenSystemEntries() {
    if (_openSysLid) _fetchSystemEntries(_openSysLid);
  }

  var _timerMonitor           = null;
  var _timerOpenBatchEntries  = null;
  var _timerOpenSystemEntries = null;

  function _monitorPanelActive() {
    var panel = document.getElementById('panel-log');
    return panel && panel.classList.contains('active');
  }

  if (!document.hidden && _monitorPanelActive()) {
    _resumeMonitorPolling();
  }

  function _pauseMonitorPolling() {
    clearInterval(_timerMonitor);
    clearInterval(_timerOpenBatchEntries);
    clearInterval(_timerOpenSystemEntries);
    _timerMonitor = _timerOpenBatchEntries = _timerOpenSystemEntries = null;
    _disconnectAllPubViewers();
  }

  function _resumeMonitorPolling() {
    if (_timerMonitor) return;
    refreshMonitor();
    _timerMonitor           = setInterval(refreshMonitor, MONITOR_POLL_MS);
    _timerOpenBatchEntries  = setInterval(refreshOpenBatchEntries, BATCH_ENTRIES_POLL_MS);
    _timerOpenSystemEntries = setInterval(refreshOpenSystemEntries, SYSTEM_ENTRIES_POLL_MS);
    syncPubFrameStreams();
  }

  document.addEventListener('visibilitychange', function() {
    if (document.hidden) {
      _pauseMonitorPolling();
    } else if (_monitorPanelActive()) {
      _resumeMonitorPolling();
    }
  });

  window.monitorPausePolling = _pauseMonitorPolling;
  window.monitorResumePolling = _resumeMonitorPolling;
})();
