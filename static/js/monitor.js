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
    video_pending:    'видео: ожидание',
    video_ready:      'видео готово',
    video_error:      'видео: ошибка',
    transcode_ready:  'транскод готов',
    transcode_error:  'транскод: ошибка',
    published:            'опубликовано',
    published_partially:  'частично опубликовано',
    publish_error:        'публикация: ошибка',
    movie_probe:      'пробный',
    story_probe:      'пробный (сюжет)',
    cancelled:        'отменён',
    donated:          'из пула',
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
  const FINAL_BATCH_STATUSES    = ['published', 'published_partially', 'movie_probe', 'story_probe', 'cancelled', 'error', 'fatal_error', 'video_error', 'transcode_error', 'publish_error', 'donated'];

  const MON_SVG_COPY     = `<svg viewBox="0 0 16 16"><rect x="5" y="5" width="9" height="9" rx="1.5"/><path d="M3 11V3a1 1 0 0 1 1-1h8"/></svg>`;
  const MON_SVG_RESTART  = `<svg viewBox="0 0 16 16" fill="none" stroke-linecap="round" stroke-linejoin="round"><polyline points="15.3,2.7 15.3,6.7 11.3,6.7"/><path d="M13.66 10a6 6 0 1 1-.08-5"/></svg>`;
  const MON_SVG_EYE      = `<svg viewBox="0 0 16 16"><rect x="3" y="2" width="10" height="12" rx="1.5"/><line x1="5.5" y1="5.5" x2="10.5" y2="5.5"/><line x1="5.5" y1="8" x2="10.5" y2="8"/><line x1="5.5" y1="10.5" x2="8.5" y2="10.5"/></svg>`;
  const MON_SVG_PLAY     = `<svg viewBox="0 0 16 16"><polygon points="4,2 13,8 4,14"/></svg>`;
  const MON_SVG_INFO     = `<svg viewBox="0 0 16 16"><circle cx="8" cy="8" r="6.2"/><line x1="8" y1="5.5" x2="8" y2="5.5"/><line x1="8" y1="7.5" x2="8" y2="11"/></svg>`;
  const MON_SVG_DELETE   = `<svg viewBox="0 0 16 16"><polyline points="2,4 14,4"/><path d="M5 4V2h6v2"/><rect x="3" y="4" width="10" height="10" rx="1.5"/><line x1="6" y1="7" x2="6" y2="11"/><line x1="10" y1="7" x2="10" y2="11"/></svg>`;
  const MON_SVG_EXPORT   = `<svg viewBox="0 0 16 16"><path d="M8 2v7M5 6l3 3 3-3"/><path d="M3 11v2a1 1 0 0 0 1 1h8a1 1 0 0 0 1-1v-2"/></svg>`;

  function renderLogItem(log, batchId, storyId, hasVideoData, textModelName, videoModelName, batchStatus) {
    const isPublish = (log.pipeline === 'publish');
    const rawSt = log.status || 'info';
    const st = (isPublish && rawSt === 'ok' && batchStatus === 'published_partially') ? 'partial' : rawSt;
    const pip = PIPELINE_LABELS[log.pipeline] || log.pipeline;
    const modelName = (log.pipeline === 'video' && videoModelName) ? videoModelName
      : (log.pipeline === 'story' && textModelName) ? textModelName
      : (log.pipeline === 'transcode' || log.pipeline === 'transcoding') ? 'H.264'
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
    const storyExportBtn = (log.pipeline === 'story' && storyId)
      ? '<button class="cycle-float-btn" title="Выгрузка" onclick="exportStory(\'' + esc(storyId) + '\',this)">' + MON_SVG_EXPORT + '</button>'
      : '';
    const pipActions = '<div class="monitor-pip-actions" onclick="event.stopPropagation()">' +
      restartBtn +
      storyExportBtn +
      '<button class="cycle-float-btn" title="Скопировать логи" onclick="monitorPipelineCopy(this)">'     + MON_SVG_COPY + '</button>' +
      '<button class="cycle-float-btn" title="Скопировать инфо" onclick="monitorPipelineCopyInfo(this)">' + MON_SVG_INFO + '</button>' +
    '</div>';

    const frameHtml = (isPublish && batchId)
      ? '<div class="monitor-pub-frame">' +
          '<img data-bid="' + esc(batchId) + '" style="width:100%;height:auto;display:block">' +
        '</div>'
      : '';

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
      frameHtml +
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
    const schedStr = batch.type === 'slot'
      ? 'Публикация: ' + fmtMskShort(batch.scheduled_at)
      : batch.type === 'movie_probe'
        ? 'Пробное видео'
        : batch.type === 'story_probe'
          ? 'Пробный сюжет'
          : 'Публикация: сейчас';
    const statusLabel = (bs === 'movie_probe' || bs === 'story_probe') ? 'выполнен' : (STATUS_LABELS[bs] || bs);
    const sub = [schedStr, statusLabel]
      .filter(Boolean).join(' · ');

    if (logs.length === 0) {
      const noLogDot = _activeBatchIds.indexOf(batch.batch_id) >= 0 ? 'md-active' : 'md-warn';
      return '<div class="monitor-batch bs-' + esc(bs) + '" data-bid="' + esc(batch.batch_id) + '">' +
        '<div class="monitor-batch-header" style="cursor:default">' +
          '<span class="monitor-dot ' + noLogDot + '"></span>' +
          '<div class="monitor-batch-meta">' +
            '<div class="monitor-batch-title">' + fmtMskShort(headTime) + '</div>' +
            '<div class="monitor-batch-sub">'   + esc(sub) + '</div>' +
          '</div>' +
        '</div>' +
      '</div>';
    }

    const doneStatuses    = ['published', 'movie_probe', 'story_probe'];
    const partialStatuses = ['published_partially'];
    const waitStatuses    = ['story_ready', 'video_pending', 'video_ready', 'transcode_ready'];
    const errorStatuses   = ['error', 'video_error', 'transcode_error', 'publish_error', 'fatal_error'];
    const finalStatuses   = doneStatuses.concat(partialStatuses).concat(errorStatuses).concat(['cancelled']);
    const isFinal         = finalStatuses.indexOf(bs) >= 0;
    const isActive        = !isFinal && (
                              logs.some(function(l) { return l.status === 'running'; })
                           || _activeBatchIds.indexOf(batch.batch_id) >= 0
                           );
    const md = isActive ? 'md-active'
             : bs === 'cancelled'                    ? 'md-skip'
             : doneStatuses.indexOf(bs)    >= 0      ? 'md-ok'
             : partialStatuses.indexOf(bs) >= 0      ? 'md-partial'
             : errorStatuses.indexOf(bs)   >= 0      ? 'md-error'
             : waitStatuses.indexOf(bs)    >= 0      ? 'md-wait'
             : bs === 'donated'                      ? 'md-white'
             : 'md-warn';

    const logHtml = '<div class="monitor-log-list">' + logs.map(function(log) {
      return renderLogItem(log, batch.batch_id, batch.story_id, batch.has_video_data, batch.text_model_name, batch.video_model_name, bs);
    }).join('') + '</div>';

    const batchStoryBtn = batch.story_id
      ? '<button class="cycle-float-btn story-view-btn" title="Посмотреть сюжет" onclick="openStoryModal(\'' + esc(batch.story_id) + '\',\'' + esc(batch.text_model_name || '') + '\')">' + MON_SVG_EYE + '</button>'
      : '';
    const batchVideoBtn = batch.has_video_data
      ? '<button class="cycle-float-btn" title="Просмотр видео" onclick="openVideoModal(\'' + esc(batch.batch_id) + '\',\'' + esc(batch.video_model_name || '') + '\')">' + MON_SVG_PLAY + '</button>'
      : '';

    const hdrActions =
      '<div class="monitor-hdr-actions-always" onclick="event.stopPropagation()">' +
        batchStoryBtn +
        batchVideoBtn +
        '<button class="cycle-float-btn" title="Скопировать логи" onclick="monitorCopy(this)">'          + MON_SVG_COPY + '</button>' +
        '<button class="cycle-float-btn" title="Скопировать инфо" onclick="monitorBatchCopyInfo(this)">' + MON_SVG_INFO + '</button>' +
        (isActive || FINAL_BATCH_STATUSES.indexOf(bs) === -1
          ? '<button class="cycle-float-btn btn-blocked" title="Удалить батч" data-warn="1" onclick="monitorDeleteBatch(\'' + esc(batch.batch_id) + '\',this)">' + MON_SVG_DELETE + '</button>'
          : '<button class="cycle-float-btn" title="Удалить батч" onclick="monitorDeleteBatch(\'' + esc(batch.batch_id) + '\',this)">' + MON_SVG_DELETE + '</button>') +
      '</div>';

    return '<div class="monitor-batch bs-' + esc(bs) + '" data-bid="' + esc(batch.batch_id) +
      '" data-scheduled="'  + esc(batch.type !== 'slot' ? 'сейчас' : fmtMsk(batch.scheduled_at)) +
      '" data-bstatus="'    + esc(bs) +
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

  function renderSysGroup(items, posKey) {
    const key      = posKey || '';
    const headTime = items.length ? items[items.length - 1].created_at : null;
    const n        = items.length;
    const sub      = n + ' ' + (n === 1 ? 'событие' : n < 5 ? 'события' : 'событий');

    const rows = items.map(function(e) {
      if (e._orphan) {
        const lvl = e.level || 'info';
        return '<div class="monitor-sysgroup-item">' +
          '<span class="monitor-sysgroup-ts">'  + fmtMsk(e.created_at)         + '</span>' +
          '<span class="monitor-sysgroup-pip orphan-pip">—</span>'              +
          '<span class="monitor-sysgroup-msg '  + esc(lvl) + '">' + esc(e.message || '') + '</span>' +
        '</div>';
      }
      const st = e.status || 'info';
      return '<div class="monitor-sysgroup-item">' +
        '<span class="monitor-sysgroup-ts">'  + fmtMsk(e.created_at)                          + '</span>' +
        '<span class="monitor-sysgroup-pip">' + esc(PIPELINE_LABELS[e.pipeline] || e.pipeline) + '</span>' +
        '<span class="monitor-sysgroup-msg '  + esc(st) + '">' + esc(e.message || '')          + '</span>' +
      '</div>';
    }).join('');

    const sysActions =
      '<div class="monitor-hdr-actions-always" onclick="event.stopPropagation()">' +
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

  function buildTimeline(batches, system, orphan) {
    var items = [];
    (batches || []).forEach(function(b) { items.push({ type: 'batch', time: b.created_at, data: b }); });
    (system  || []).forEach(function(s) { items.push({ type: 'sys',   time: s.created_at, data: s }); });
    (orphan  || []).forEach(function(e) { items.push({ type: 'sys',   time: e.created_at, data: Object.assign({}, e, { _orphan: true }) }); });
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

    for (var i = 0; i < groups.length; i++) {
      if (groups[i].type !== 'sysgroup') continue;
      var prev = null, next = null;
      for (var j = i - 1; j >= 0; j--) {
        if (groups[j].type === 'batch') { prev = groups[j].data.batch_id; break; }
      }
      for (var k = i + 1; k < groups.length; k++) {
        if (groups[k].type === 'batch') { next = groups[k].data.batch_id; break; }
      }
      groups[i]._posKey = (prev || 'top') + ':' + (next || 'bottom');
    }

    return groups;
  }

  var _openBid          = null;
  var _collapsedSgKeys  = {};
  var _collapsedLids    = {};
  var _activeBatchIds   = [];
  var _seenLids      = {};
  var _seenBids      = {};
  var _firstRender   = true;
  var _pubFrameCache    = {};  // batchId → blob URL
  var _pubFrameFetching = {};  // batchId → true (in-flight guard)
  var _lastRenderedHtml = {};  // key → last rendered HTML string

  function getOpenState() {
    var sgkeys = {}, lids = {};
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
    return { sgkeys: sgkeys, lids: lids };
  }

  function restoreOpenState(state) {
    if (_openBid) {
      var batchEl = document.querySelector('.monitor-batch[data-bid="' + _openBid + '"]');
      if (batchEl) {
        batchEl.classList.add('open');
        _fetchAndInjectEntries(_openBid);
      } else {
        _openBid = null;
      }
    }
    document.querySelectorAll('.monitor-sysgroup').forEach(function(el) {
      if (state.sgkeys[el.dataset.sgKey] && !_collapsedSgKeys[el.dataset.sgKey]) el.classList.add('open');
    });
    document.querySelectorAll('.monitor-log-item').forEach(function(el) {
      if (state.lids && state.lids[el.dataset.lid] && !_collapsedLids[el.dataset.lid]) el.classList.add('open');
    });
  }

  function autoExpandNewActivity() {
    if (_openBid !== null) return;
  }

  function _evictPubFrameCache() {
    var inDom = {};
    document.querySelectorAll('.monitor-pub-frame img[data-bid]').forEach(function(img) {
      if (img.dataset.bid) inDom[img.dataset.bid] = true;
    });
    Object.keys(_pubFrameCache).forEach(function(bid) {
      if (!inDom[bid]) {
        URL.revokeObjectURL(_pubFrameCache[bid]);
        delete _pubFrameCache[bid];
        delete _pubFrameFetching[bid];
      }
    });
  }

  function refreshPublishFrames() {
    _evictPubFrameCache();

    document.querySelectorAll('.monitor-pub-frame img[data-bid]').forEach(function(img) {
      var bid = img.dataset.bid;
      if (!bid) return;

      // Мгновенно восстанавливаем кадр из кэша (без мигания после перерисовки DOM)
      if (_pubFrameCache[bid] && !img.src) {
        img.src = _pubFrameCache[bid];
        img.parentNode.style.display = 'block';
      }

      // Новый кадр получаем только для активной публикации
      var logItem = img.closest('.monitor-log-item');
      if (!logItem || logItem.dataset.status !== 'running') return;

      // Защита от дублирующих запросов при медленной сети
      if (_pubFrameFetching[bid]) return;
      _pubFrameFetching[bid] = true;

      // Preload-swap: старый кадр виден до тех пор, пока новый не загружен полностью
      fetch('/api/batch/' + bid + '/publish-frame?t=' + Date.now())
        .then(function(r) {
          if (!r.ok || r.status === 204) return null;
          return r.blob();
        })
        .then(function(blob) {
          _pubFrameFetching[bid] = false;
          if (!blob) return;
          if (_pubFrameCache[bid]) URL.revokeObjectURL(_pubFrameCache[bid]);
          var url = URL.createObjectURL(blob);
          _pubFrameCache[bid] = url;
          document.querySelectorAll('.monitor-pub-frame img[data-bid="' + bid + '"]')
            .forEach(function(el) {
              el.src = url;
              el.parentNode.style.display = 'block';
            });
        })
        .catch(function() { _pubFrameFetching[bid] = false; });
    });
  }

  window.addEventListener('beforeunload', function() {
    Object.keys(_pubFrameCache).forEach(function(bid) {
      URL.revokeObjectURL(_pubFrameCache[bid]);
    });
  });

  function _groupKey(g) {
    if (g.type === 'batch') return 'batch:' + g.data.batch_id;
    return 'sys:' + (g._posKey || '');
  }

  function renderTimeline(data) {
    var el = document.getElementById('monitor-timeline');
    if (!el) return;
    _activeBatchIds = Array.isArray(data.active_batch_ids) ? data.active_batch_ids : [];
    var prev   = getOpenState();
    var groups = buildTimeline(data.batches, data.system, data.orphan_entries);
    if (groups.length === 0) {
      el.innerHTML = '<div style="font-size:12px;color:#444;padding:4px 0">Нет данных</div>';
      _firstRender = false;
      return;
    }

    var newHtmlMap = {};
    var newKeys    = [];
    groups.forEach(function(g) {
      var key = _groupKey(g);
      newKeys.push(key);
      newHtmlMap[key] = g.type === 'batch' ? renderBatch(g.data) : renderSysGroup(g.items, g._posKey);
    });

    var oldEls = {};
    var unkeyed = [];
    Array.prototype.slice.call(el.children).forEach(function(child) {
      var bid   = child.dataset.bid;
      var sgKey = child.dataset.sgKey;
      var key   = bid ? ('batch:' + bid) : sgKey ? ('sys:' + sgKey) : null;
      if (key) oldEls[key] = child;
      else unkeyed.push(child);
    });

    unkeyed.forEach(function(child) { el.removeChild(child); });

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
      var newHtml  = newHtmlMap[key];
      if (existing) {
        if (_lastRenderedHtml[key] !== newHtml) {
          var isOpen = existing.classList.contains('open');
          var tmp = document.createElement('div');
          tmp.innerHTML = newHtml;
          var newNode = tmp.firstChild;
          el.replaceChild(newNode, existing);
          if (isOpen) newNode.classList.add('open');
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

    var frag = document.createDocumentFragment();
    resolvedNodes.forEach(function(node) { frag.appendChild(node); });
    el.appendChild(frag);

    restoreOpenState(prev);
    if (!_firstRender) autoExpandNewActivity();
    _firstRender = false;
    refreshPublishFrames();
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
    var lines = [
      'batch_id: '  + (batchEl.dataset.bid      || ''),
      'scheduled: ' + (batchEl.dataset.scheduled || ''),
      'status: '    + (batchEl.dataset.bstatus   || ''),
    ];
    var tm = batchEl.dataset.textModel  || '';
    var vm = batchEl.dataset.videoModel || '';
    if (tm) lines.push('text_model: '  + tm);
    if (vm) lines.push('video_model: ' + vm);
    return lines;
  }

  function _injectEntriesIntoDOM(batchEl, logsData) {
    logsData.forEach(function(logInfo) {
      var li = batchEl.querySelector('.monitor-log-item[data-lid="' + logInfo.id + '"]');
      if (!li) return;
      if (!logInfo.entries || !logInfo.entries.length) return;
      if (li.querySelector('.monitor-entries')) return;
      var html = '';
      logInfo.entries.forEach(function(en) {
        var lvl = en.level || 'info';
        html += '<div class="monitor-entry-row">' +
                  '<span class="monitor-entry-ts">'                        + fmtMsk(en.created_at)   + '</span>' +
                  '<span class="monitor-entry-msg ' + esc(lvl) + '">' + esc(en.message || '') + '</span>' +
                '</div>';
      });
      var chevron = '<span class="monitor-log-chevron">▼</span>';
      var entriesDiv = '<div class="monitor-entries">' + html + '</div>';
      var headerTop = li.querySelector('.monitor-log-header-top');
      if (headerTop) headerTop.insertAdjacentHTML('beforeend', chevron);
      li.insertAdjacentHTML('beforeend', entriesDiv);
      if (!_collapsedLids[logInfo.id]) li.classList.add('open');
    });
  }

  function _fetchAndInjectEntries(bid) {
    var batchEl = document.querySelector('.monitor-batch[data-bid="' + bid + '"]');
    if (!batchEl) return;
    fetch('/api/monitor/batch/' + encodeURIComponent(bid) + '/entries')
      .then(function(r) { return r.json(); })
      .then(function(data) {
        if (_openBid !== bid) return;
        var batchEl2 = document.querySelector('.monitor-batch[data-bid="' + bid + '"]');
        if (batchEl2) _injectEntriesIntoDOM(batchEl2, data.logs || []);
      });
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
    } else {
      el.classList.add('open');
      _openBid = bid || null;
      if (_openBid) _fetchAndInjectEntries(_openBid);
    }
  };

  window.monitorToggleSys = function(e, el) {
    if (e.target.closest('.monitor-sysgroup-body')) return;
    el.classList.toggle('open');
    var key = el.dataset.sgKey;
    if (key) {
      if (el.classList.contains('open')) delete _collapsedSgKeys[key];
      else _collapsedSgKeys[key] = true;
    }
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
          }, 1200);
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
          'Батч и все связанные данные (логи, видео) будут удалены без возможности восстановления.<br><br>' +
          'Сюжеты (stories) не затрагиваются.',
        confirmLabel: 'Удалить',
        onConfirm: function(confirmBtn, dlg) {
          confirmBtn.disabled    = true;
          confirmBtn.textContent = 'Удаляем…';
          fetch('/api/monitor/batch/' + encodeURIComponent(batchId) + '/delete', { method: 'POST' })
            .then(function(r) { return r.json(); })
            .then(function(data) {
              dlg.close();
              if (data.ok) {
                var batchEl = document.querySelector('.monitor-batch[data-bid="' + batchId + '"]');
                if (batchEl) batchEl.remove();
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

  refreshMonitor();
  setInterval(refreshMonitor, 5000);
  setInterval(refreshPublishFrames, 3000);
})();
