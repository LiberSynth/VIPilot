(function() {
  const PIPELINE_LABELS = {
    root:        'Система',
    planning:    'Планирование',
    story:       'Сюжет',
    video:       'Видео',
    transcode:   'Транскодирование',
    transcoding: 'Транскодирование',
    publish:     'Публикация',
    publishing:  'Публикация',
    cleanup:     'Очистка',
  };

  const STATUS_LABELS = {
    pending:          'ожидание',
    story_generating: 'генерация сюжета',
    running:          'выполняется',
    ok:               'готово',
    error:            'ошибка',
    info:             'инфо',
    story_ready:      'сюжет готов',
    video_pending:    'видео: ожидание fal.ai',
    video_ready:      'видео готово',
    video_error:      'видео: ошибка',
    transcode_ready:  'транскод готов',
    transcode_error:  'транскод: ошибка',
    published:        'опубликовано',
    publish_error:    'публикация: ошибка',
    probe:            'пробный',
    'отменён':        'отменён',
  };

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
        const lvl = e.level || 'info';
        return '<div class="monitor-entry-row">' +
          '<span class="monitor-entry-ts">'  + fmtMsk(e.created_at)  + '</span>' +
          '<span class="monitor-entry-msg ' + esc(lvl) + '">' + esc(e.message) + '</span>' +
        '</div>';
      }).join('') +
    '</div>';
  }

  const PIPELINE_RESTARTABLE    = ['story', 'video', 'transcode', 'publish'];
  const PIPELINE_ERROR_STATUSES = ['error', 'video_error', 'transcode_error', 'publish_error'];

  const MON_SVG_EXPAND   = `<svg viewBox="0 0 16 16"><polyline points="2,6 2,2 6,2"/><polyline points="10,2 14,2 14,6"/><polyline points="14,10 14,14 10,14"/><polyline points="6,14 2,14 2,10"/></svg>`;
  const MON_SVG_COLLAPSE = `<svg viewBox="0 0 16 16"><polyline points="6,2 6,6 2,6"/><polyline points="10,2 10,6 14,6"/><polyline points="14,10 10,10 10,14"/><polyline points="2,10 6,10 6,14"/></svg>`;
  const MON_SVG_COPY     = `<svg viewBox="0 0 16 16"><rect x="5" y="5" width="9" height="9" rx="1.5"/><path d="M3 11V3a1 1 0 0 1 1-1h8"/></svg>`;
  const MON_SVG_RESTART  = `<svg viewBox="0 0 16 16" fill="none" stroke-linecap="round" stroke-linejoin="round"><polyline points="15.3,2.7 15.3,6.7 11.3,6.7"/><path d="M13.66 10a6 6 0 1 1-.08-5"/></svg>`;
  const MON_SVG_EYE      = `<svg viewBox="0 0 16 16"><rect x="3" y="2" width="10" height="12" rx="1.5"/><line x1="5.5" y1="5.5" x2="10.5" y2="5.5"/><line x1="5.5" y1="8" x2="10.5" y2="8"/><line x1="5.5" y1="10.5" x2="8.5" y2="10.5"/></svg>`;
  const MON_SVG_PLAY     = `<svg viewBox="0 0 16 16"><polygon points="4,2 13,8 4,14"/></svg>`;
  const MON_SVG_INFO     = `<svg viewBox="0 0 16 16"><circle cx="8" cy="8" r="6.2"/><line x1="8" y1="5.5" x2="8" y2="5.5"/><line x1="8" y1="7.5" x2="8" y2="11"/></svg>`;

  function renderLogItem(log, batchId, storyId, hasVideoData, textModelName, videoModelName) {
    const st  = log.status || 'info';
    const pip = PIPELINE_LABELS[log.pipeline] || log.pipeline;
    const modelName = (log.pipeline === 'video' && videoModelName) ? videoModelName
      : (log.pipeline === 'story' && textModelName) ? textModelName
      : null;
    const modelTag   = modelName ? '<span class="monitor-log-model">' + esc(modelName) + '</span>' : '';
    const hasEntries = log.entries && log.entries.length > 0;
    const chevron    = hasEntries ? '<span class="monitor-log-chevron">▼</span>' : '';
    const comment    = log.message ? '<div class="monitor-log-comment">' + esc(log.message) + '</div>' : '';

    const canRestart = batchId
      && PIPELINE_RESTARTABLE.indexOf(log.pipeline) >= 0
      && PIPELINE_ERROR_STATUSES.indexOf(st) >= 0;
    const restartBtn = canRestart
      ? '<button class="cycle-float-btn" title="Перезапустить" data-bid="' + esc(batchId) + '" data-pip="' + esc(log.pipeline) + '" onclick="monitorPipelineRestart(this)">' + MON_SVG_RESTART + '</button>'
      : '';
    const pipActions = '<div class="monitor-pip-actions" onclick="event.stopPropagation()">' +
      restartBtn +
      '<button class="cycle-float-btn" title="Скопировать логи" onclick="monitorPipelineCopy(this)">'     + MON_SVG_COPY + '</button>' +
      '<button class="cycle-float-btn" title="Скопировать инфо" onclick="monitorPipelineCopyInfo(this)">' + MON_SVG_INFO + '</button>' +
    '</div>';

    return '<div class="monitor-log-item" data-lid="' + esc(log.id) + '" data-status="' + esc(st) + '" onclick="monitorToggleLog(event,this)">' +
      '<div class="monitor-log-header">' +
        '<div class="monitor-log-header-top">' +
          '<span class="monitor-log-dot ls-' + esc(st) + '"></span>' +
          '<span class="monitor-log-ts">'       + fmtMsk(log.created_at) + '</span>' +
          '<span class="monitor-log-pipeline">' + esc(pip) + modelTag    + '</span>' +
          pipActions + chevron +
        '</div>' +
        comment +
      '</div>' +
      renderEntries(log.entries) +
    '</div>';
  }

  function groupLogsByPipeline(logs) {
    const order = [];
    const map   = {};
    logs.forEach(function(log) {
      if (!map[log.pipeline]) {
        map[log.pipeline] = {
          id:         log.id,
          pipeline:   log.pipeline,
          created_at: log.created_at,
          status:     log.status,
          message:    log.message,
          entries:    []
        };
        order.push(log.pipeline);
      }
      const g = map[log.pipeline];
      g.status = log.status;
      if (log.message) g.message = log.message;
      if (log.entries) log.entries.forEach(function(e) { g.entries.push(e); });
    });
    order.forEach(function(pip) {
      map[pip].entries.sort(function(a, b) {
        return (a.created_at || '').localeCompare(b.created_at || '');
      });
    });
    return order.map(function(pip) { return map[pip]; });
  }

  function renderBatch(batch) {
    const bs   = batch.batch_status || 'pending';
    const logs = groupLogsByPipeline(batch.logs || []);

    const headTime = batch.created_at;
    const schedStr = batch.adhoc
      ? (batch.target_name ? 'Публикация: сейчас' : 'Публикация: пробный')
      : 'Публикация: ' + fmtMskShort(batch.scheduled_at);
    const sub = [schedStr, batch.target_name, STATUS_LABELS[bs] || bs]
      .filter(Boolean).join(' · ');

    if (logs.length === 0) {
      return '<div class="monitor-batch bs-' + esc(bs) + '" data-bid="' + esc(batch.batch_id) + '">' +
        '<div class="monitor-batch-header" style="cursor:default">' +
          '<span class="monitor-dot md-warn"></span>' +
          '<div class="monitor-batch-meta">' +
            '<div class="monitor-batch-title">' + fmtMskShort(headTime) + '</div>' +
            '<div class="monitor-batch-sub">'   + esc(sub) + '</div>' +
          '</div>' +
        '</div>' +
      '</div>';
    }

    const isActive      = logs.some(function(l) { return l.status === 'running'; });
    const doneStatuses  = ['published', 'probe'];
    const waitStatuses  = ['story_ready', 'video_pending', 'video_ready', 'transcode_ready'];
    const errorStatuses = ['error', 'video_error', 'transcode_error', 'publish_error'];
    const md = isActive ? 'md-active'
             : bs === 'отменён'                   ? 'md-skip'
             : doneStatuses.indexOf(bs)  >= 0     ? 'md-ok'
             : errorStatuses.indexOf(bs) >= 0     ? 'md-error'
             : waitStatuses.indexOf(bs)  >= 0     ? 'md-wait'
             : 'md-warn';

    const logHtml = '<div class="monitor-log-list">' + logs.map(function(log) {
      return renderLogItem(log, batch.batch_id, batch.story_id, batch.has_video_data, batch.text_model_name, batch.video_model_name);
    }).join('') + '</div>';

    const batchStoryBtn = batch.story_id
      ? '<button class="cycle-float-btn story-view-btn" title="Посмотреть сюжет" onclick="openStoryModal(\'' + esc(batch.story_id) + '\',\'' + esc(batch.text_model_name || '') + '\')">' + MON_SVG_EYE + '</button>'
      : '';
    const batchVideoBtn = batch.has_video_data
      ? '<button class="cycle-float-btn" title="Просмотр видео" onclick="openVideoModal(\'' + esc(batch.batch_id) + '\',\'' + esc(batch.video_model_name || '') + '\')">' + MON_SVG_PLAY + '</button>'
      : '';

    const hdrActions =
      '<div class="monitor-hdr-actions" onclick="event.stopPropagation()">' +
        '<button class="cycle-float-btn" title="Развернуть все"   onclick="monitorExpandAll(this)">'   + MON_SVG_EXPAND   + '</button>' +
        '<button class="cycle-float-btn" title="Свернуть все"     onclick="monitorCollapseAll(this)">' + MON_SVG_COLLAPSE + '</button>' +
      '</div>' +
      '<div class="monitor-hdr-actions-always" onclick="event.stopPropagation()">' +
        batchStoryBtn +
        batchVideoBtn +
        '<button class="cycle-float-btn" title="Скопировать логи" onclick="monitorCopy(this)">'          + MON_SVG_COPY + '</button>' +
        '<button class="cycle-float-btn" title="Скопировать инфо" onclick="monitorBatchCopyInfo(this)">' + MON_SVG_INFO + '</button>' +
      '</div>';

    return '<div class="monitor-batch bs-' + esc(bs) + '" data-bid="' + esc(batch.batch_id) +
      '" data-scheduled="'  + esc(batch.adhoc ? 'сейчас' : fmtMsk(batch.scheduled_at)) +
      '" data-bstatus="'    + esc(bs) +
      '" data-target="'     + esc(batch.target_name     || '') +
      '" data-text-model="' + esc(batch.text_model_name || '') +
      '" data-video-model="'+ esc(batch.video_model_name|| '') +
      '" onclick="monitorToggleBatch(event,this)">' +
      '<div class="monitor-batch-header">' +
        '<span class="monitor-dot ' + md + '"></span>' +
        '<div class="monitor-batch-meta">' +
          '<div class="monitor-batch-title">' + fmtMsk(headTime) + '</div>' +
          '<div class="monitor-batch-sub">'   + esc(sub)         + '</div>' +
        '</div>' +
        hdrActions +
        '<span class="monitor-batch-arrow">▼</span>' +
      '</div>' +
      '<div class="monitor-batch-body">' + logHtml + '</div>' +
    '</div>';
  }

  function renderSysGroup(items) {
    const key      = items[0] ? items[0].created_at : '';
    const headTime = items.length ? items[items.length - 1].created_at : null;
    const n        = items.length;
    const sub      = n + ' ' + (n === 1 ? 'событие' : n < 5 ? 'события' : 'событий');

    const rows = items.map(function(e) {
      const st = e.status || 'info';
      return '<div class="monitor-sysgroup-item">' +
        '<span class="monitor-sysgroup-ts">'  + fmtMsk(e.created_at)                          + '</span>' +
        '<span class="monitor-sysgroup-pip">' + esc(PIPELINE_LABELS[e.pipeline] || e.pipeline) + '</span>' +
        '<span class="monitor-sysgroup-msg '  + esc(st) + '">' + esc(e.message || '')          + '</span>' +
      '</div>';
    }).join('');

    const sysActions =
      '<div class="monitor-hdr-actions" onclick="event.stopPropagation()">' +
        '<button class="cycle-float-btn" title="Скопировать" onclick="monitorSysCopy(this)">' + MON_SVG_COPY + '</button>' +
      '</div>';

    return '<div class="monitor-sysgroup" data-sg-key="' + esc(key) + '" onclick="monitorToggleSys(event,this)">' +
      '<div class="monitor-sysgroup-header">' +
        '<span class="monitor-sysgroup-dot"></span>' +
        '<div class="monitor-sysgroup-meta">' +
          '<div class="monitor-sysgroup-title">' + fmtMsk(headTime) + '</div>' +
          '<div class="monitor-sysgroup-sub">'   + esc(sub)         + '</div>' +
        '</div>' +
        sysActions +
        '<span class="monitor-sysgroup-arrow">▼</span>' +
      '</div>' +
      '<div class="monitor-sysgroup-body"><div class="monitor-sysgroup-items">' + rows + '</div></div>' +
    '</div>';
  }

  function buildTimeline(batches, system) {
    var items = [];
    (batches || []).forEach(function(b) { items.push({ type: 'batch', time: b.created_at, data: b }); });
    (system  || []).forEach(function(s) { items.push({ type: 'sys',   time: s.created_at, data: s }); });
    items.sort(function(a, b) { return new Date(b.time) - new Date(a.time); });

    var groups = [], sysAccum = null;
    items.forEach(function(item) {
      if (item.type === 'sys') {
        if (!sysAccum) { sysAccum = []; groups.push({ type: 'sysgroup', items: sysAccum }); }
        sysAccum.push(item.data);
      } else {
        sysAccum = null;
        groups.push({ type: 'batch', data: item.data });
      }
    });
    return groups;
  }

  var _collapsedBids = {};
  var _collapsedLids = {};
  var _seenLids      = {};
  var _seenBids      = {};

  function getOpenState() {
    var bids = {}, sgkeys = {}, lids = {};
    document.querySelectorAll('.monitor-batch.open').forEach(function(el) {
      if (el.dataset.bid) bids[el.dataset.bid] = true;
    });
    document.querySelectorAll('.monitor-sysgroup.open').forEach(function(el) {
      if (el.dataset.sgKey) sgkeys[el.dataset.sgKey] = true;
    });
    document.querySelectorAll('.monitor-log-item.open').forEach(function(el) {
      if (el.dataset.lid) lids[el.dataset.lid] = true;
    });
    document.querySelectorAll('.monitor-log-item').forEach(function(el) {
      if (el.dataset.lid) _seenLids[el.dataset.lid] = true;
    });
    document.querySelectorAll('.monitor-batch').forEach(function(el) {
      if (el.dataset.bid) _seenBids[el.dataset.bid] = true;
    });
    return { bids: bids, sgkeys: sgkeys, lids: lids };
  }

  function restoreOpenState(state) {
    document.querySelectorAll('.monitor-batch').forEach(function(el) {
      if (state.bids[el.dataset.bid] && !_collapsedBids[el.dataset.bid]) el.classList.add('open');
    });
    document.querySelectorAll('.monitor-sysgroup').forEach(function(el) {
      if (state.sgkeys[el.dataset.sgKey]) el.classList.add('open');
    });
    document.querySelectorAll('.monitor-log-item').forEach(function(el) {
      if (state.lids && state.lids[el.dataset.lid] && !_collapsedLids[el.dataset.lid]) el.classList.add('open');
    });
  }

  function autoExpandRunningLogs() {
    document.querySelectorAll('.monitor-batch.open').forEach(function(batch) {
      batch.querySelectorAll('.monitor-log-item[data-status="running"]').forEach(function(li) {
        var lid = li.dataset.lid;
        if (li.querySelector('.monitor-entries') && !_seenLids[lid] && !_collapsedLids[lid]) {
          li.classList.add('open');
        }
      });
    });
  }

  var AUTO_OPEN_BATCH_STATUSES = ['running', 'error', 'video_error', 'transcode_error', 'publish_error'];

  function autoOpenActiveBatches() {
    document.querySelectorAll('.monitor-batch').forEach(function(el) {
      var bid = el.dataset.bid;
      if (!bid || _seenBids[bid] || _collapsedBids[bid]) return;
      var bs         = el.dataset.bstatus || '';
      var hasRunning = !!el.querySelector('.monitor-log-item[data-status="running"]');
      var hasError   = !!el.querySelector('.monitor-log-item[data-status="error"]');
      var isAutoStatus = AUTO_OPEN_BATCH_STATUSES.indexOf(bs) >= 0;
      if (hasRunning || hasError || isAutoStatus) {
        el.classList.add('open');
        el.querySelectorAll('.monitor-log-item').forEach(function(li) {
          var lid = li.dataset.lid;
          if (li.querySelector('.monitor-entries') && !_seenLids[lid] && !_collapsedLids[lid]) {
            li.classList.add('open');
          }
        });
      }
    });
  }

  function renderTimeline(data) {
    var el = document.getElementById('monitor-timeline');
    if (!el) return;
    var prev   = getOpenState();
    var groups = buildTimeline(data.batches, data.system);
    if (groups.length === 0) {
      el.innerHTML = '<div style="font-size:12px;color:#444;padding:4px 0">Нет данных</div>';
      return;
    }
    el.innerHTML = groups.map(function(g) {
      return g.type === 'batch' ? renderBatch(g.data) : renderSysGroup(g.items);
    }).join('');
    restoreOpenState(prev);
    autoOpenActiveBatches();
    autoExpandRunningLogs();
  }

  function refreshMonitor() {
    fetch('/api/monitor')
      .then(function(r) { return r.json(); })
      .then(renderTimeline)
      .catch(function() {});
  }

  function _monitorCopyText(text, btn) {
    const doFlash = function() {
      btn.classList.add('copied');
      setTimeout(function() { btn.classList.remove('copied'); }, 2000);
    };
    navigator.clipboard.writeText(text).then(doFlash).catch(function() {
      var ta = document.createElement('textarea');
      ta.value = text; ta.style.cssText = 'position:fixed;opacity:0';
      document.body.appendChild(ta); ta.select();
      document.execCommand('copy'); document.body.removeChild(ta);
      doFlash();
    });
  }

  function _batchInfoLines(batchEl) {
    if (!batchEl) return [];
    return [
      'batch_id: '    + (batchEl.dataset.bid       || ''),
      'scheduled: '   + (batchEl.dataset.scheduled  || ''),
      'status: '      + (batchEl.dataset.bstatus    || ''),
      'target: '      + (batchEl.dataset.target     || ''),
      'text_model: '  + (batchEl.dataset.textModel  || ''),
      'video_model: ' + (batchEl.dataset.videoModel || ''),
    ];
  }

  window.monitorToggleBatch = function(e, el) {
    if (e.target.closest('.monitor-batch-body')) return;
    el.classList.toggle('open');
    var bid = el.dataset.bid;
    if (bid) {
      if (el.classList.contains('open')) delete _collapsedBids[bid];
      else _collapsedBids[bid] = true;
    }
  };

  window.monitorToggleSys = function(e, el) {
    if (e.target.closest('.monitor-sysgroup-body')) return;
    el.classList.toggle('open');
  };

  window.monitorExpandAll = function(btn) {
    const batch = btn.closest('.monitor-batch');
    if (!batch) return;
    batch.querySelectorAll('.monitor-log-item').forEach(function(item) {
      if (item.querySelector('.monitor-entries')) {
        item.classList.add('open');
        if (item.dataset.lid) delete _collapsedLids[item.dataset.lid];
      }
    });
  };

  window.monitorCollapseAll = function(btn) {
    const batch = btn.closest('.monitor-batch');
    if (!batch) return;
    batch.querySelectorAll('.monitor-log-item').forEach(function(item) {
      item.classList.remove('open');
      if (item.dataset.lid) _collapsedLids[item.dataset.lid] = true;
    });
  };

  window.monitorSysCopy = function(btn) {
    const sg = btn.closest('.monitor-sysgroup');
    if (!sg) return;
    var lines = [];
    sg.querySelectorAll('.monitor-sysgroup-item').forEach(function(item) {
      const ts  = item.querySelector('.monitor-sysgroup-ts')  ? item.querySelector('.monitor-sysgroup-ts').textContent.trim()  : '';
      const pip = item.querySelector('.monitor-sysgroup-pip') ? item.querySelector('.monitor-sysgroup-pip').textContent.trim() : '';
      const msg = item.querySelector('.monitor-sysgroup-msg') ? item.querySelector('.monitor-sysgroup-msg').textContent.trim() : '';
      lines.push('[' + ts + '] ' + pip + ' ' + msg);
    });
    _monitorCopyText(lines.join('\n'), btn);
  };

  window.monitorCopy = function(btn) {
    const batchEl = btn.closest('.monitor-batch');
    const body    = batchEl ? batchEl.querySelector('.monitor-batch-body') : null;
    if (!body) return;
    var lines = _batchInfoLines(batchEl);
    body.querySelectorAll('.monitor-log-item').forEach(function(item) {
      lines.push('');
      const ts  = item.querySelector('.monitor-log-ts')       ? item.querySelector('.monitor-log-ts').textContent.trim()       : '';
      const pip = item.querySelector('.monitor-log-pipeline') ? item.querySelector('.monitor-log-pipeline').textContent.trim() : '';
      const msg = item.querySelector('.monitor-log-comment')  ? item.querySelector('.monitor-log-comment').textContent.trim()  : '';
      const st  = item.dataset.status || '';
      lines.push('pipeline: ' + pip);
      lines.push('status: '   + st);
      lines.push('time: '     + ts);
      if (msg) lines.push('message: ' + msg);
      var entries = [];
      item.querySelectorAll('.monitor-entry-row').forEach(function(row) {
        const ets  = row.querySelector('.monitor-entry-ts')  ? row.querySelector('.monitor-entry-ts').textContent.trim()  : '';
        const emsg = row.querySelector('.monitor-entry-msg') ? row.querySelector('.monitor-entry-msg').textContent.trim() : '';
        entries.push('  [' + ets + '] ' + emsg);
      });
      if (entries.length) { lines.push(''); entries.forEach(function(e) { lines.push(e); }); }
    });
    _monitorCopyText(lines.join('\n'), btn);
  };

  window.monitorPipelineCopy = function(btn) {
    const item = btn.closest('.monitor-log-item');
    if (!item) return;
    var lines = [];
    const ts  = item.querySelector('.monitor-log-ts')       ? item.querySelector('.monitor-log-ts').textContent.trim()       : '';
    const pip = item.querySelector('.monitor-log-pipeline') ? item.querySelector('.monitor-log-pipeline').textContent.trim() : '';
    const msg = item.querySelector('.monitor-log-comment')  ? item.querySelector('.monitor-log-comment').textContent.trim()  : '';
    const st  = item.dataset.status || '';
    lines.push('pipeline: ' + pip);
    lines.push('status: '   + st);
    lines.push('time: '     + ts);
    if (msg) lines.push('message: ' + msg);
    var entries = [];
    item.querySelectorAll('.monitor-entry-row').forEach(function(row) {
      const ets  = row.querySelector('.monitor-entry-ts')  ? row.querySelector('.monitor-entry-ts').textContent.trim()  : '';
      const emsg = row.querySelector('.monitor-entry-msg') ? row.querySelector('.monitor-entry-msg').textContent.trim() : '';
      entries.push('  [' + ets + '] ' + emsg);
    });
    if (entries.length) { lines.push(''); entries.forEach(function(e) { lines.push(e); }); }
    _monitorCopyText(lines.join('\n'), btn);
  };

  window.monitorBatchCopyInfo = function(btn) {
    const batchEl = btn.closest('.monitor-batch');
    _monitorCopyText(_batchInfoLines(batchEl).join('\n'), btn);
  };

  window.monitorPipelineCopyInfo = function(btn) {
    const item    = btn.closest('.monitor-log-item');
    if (!item) return;
    const batchEl = item.closest('.monitor-batch');
    var lines = _batchInfoLines(batchEl);
    lines.push('');
    const ts  = item.querySelector('.monitor-log-ts')       ? item.querySelector('.monitor-log-ts').textContent.trim()       : '';
    const pip = item.querySelector('.monitor-log-pipeline') ? item.querySelector('.monitor-log-pipeline').textContent.trim() : '';
    const msg = item.querySelector('.monitor-log-comment')  ? item.querySelector('.monitor-log-comment').textContent.trim()  : '';
    const st  = item.dataset.status || '';
    lines.push('pipeline: ' + pip);
    lines.push('status: '   + st);
    lines.push('time: '     + ts);
    if (msg) lines.push('message: ' + msg);
    _monitorCopyText(lines.join('\n'), btn);
  };

  window.monitorPipelineRestart = function(btn) {
    const batchId  = btn.dataset.bid;
    const pipeline = btn.dataset.pip;
    if (!batchId || !pipeline) return;
    btn.disabled = true;
    fetch('/api/batch/' + encodeURIComponent(batchId) + '/reset/' + encodeURIComponent(pipeline), { method: 'POST' })
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (data.ok) {
          btn.classList.add('copied');
          setTimeout(function() {
            btn.classList.remove('copied');
            btn.disabled = false;
            refreshMonitor();
          }, 1500);
        } else {
          btn.disabled = false;
          alert(data.error || 'Ошибка');
        }
      })
      .catch(function() { btn.disabled = false; });
  };

  window.monitorToggleLog = function(e, el) {
    e.stopPropagation();
    if (e.target.closest('.monitor-entries')) return;
    if (el.querySelector('.monitor-entries')) {
      el.classList.toggle('open');
      var lid = el.dataset.lid;
      if (lid) {
        if (el.classList.contains('open')) delete _collapsedLids[lid];
        else _collapsedLids[lid] = true;
      }
    }
  };

  window.groupLogsByPipeline = groupLogsByPipeline;
  window.renderLogItem       = renderLogItem;

  refreshMonitor();
  setInterval(refreshMonitor, 5000);
})();
