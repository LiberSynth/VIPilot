class AccordionList {
  constructor(opts) {
    this._listId         = opts.listId;
    this._cardId         = opts.cardId         || null;
    this._holderId       = opts.holderId       || null;
    this._countId        = opts.countId        || null;
    this._gradeUrl       = opts.gradeUrl       || null;
    this._renderTitle    = opts.renderTitle;
    this._renderButtons  = opts.renderButtons;
    this._onExpand       = opts.onExpand       || null;
    this._onCollapse     = opts.onCollapse     || null;
    this._canAddNew      = opts.canAddNew      || false;
    this._onNewRowReady  = opts.onNewRowReady  || null;
    this._emptyHtml      = opts.emptyHtml      || '<div class="stories-empty">Нет записей</div>';
    this._rowClassFn        = opts.rowClassFn        || null;
    this._onExpandOnRerender= opts.onExpandOnRerender || false;

    this._gradeLabels    = opts.gradeLabels    || null;
    this._gradeColors    = opts.gradeColors    || null;
    this._gradeTextColors= opts.gradeTextColors|| null;
    this._gradeCycle     = opts.gradeCycle     || null;

    this._activeId         = null;
    this._gradeReqCounters = {};
    this._data             = [];
  }

  static escapeHtml(str) {
    return String(str || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  static gradeKey(g) {
    return (g === null || g === undefined) ? 'null' : String(g);
  }

  getActiveId()   { return this._activeId; }
  setActiveId(id) { this._activeId = id; }
  getData()       { return this._data; }

  _gl()  { return this._gradeLabels    || AccordionList.GRADE_LABELS; }
  _gc()  { return this._gradeColors    || AccordionList.GRADE_COLORS; }
  _gtc() { return this._gradeTextColors|| AccordionList.GRADE_TEXT_COLORS; }
  _gcy() { return this._gradeCycle     || AccordionList.GRADE_CYCLE; }

  updateCount(n) {
    var el = document.getElementById(this._countId);
    if (!el) return;
    if (n === null || n === undefined) { el.textContent = ''; return; }
    el.textContent = 'Записей: ' + n;
  }

  _getCard()   { return this._cardId   ? document.getElementById(this._cardId)   : null; }
  _getHolder() { return this._holderId ? document.getElementById(this._holderId) : null; }
  _getList()   { return document.getElementById(this._listId); }

  _saveFocus() {
    var ae   = document.activeElement;
    if (!ae) return null;
    var card = this._getCard();
    if (card && card.contains(ae)) {
      return {
        id:    ae.id,
        start: ae.selectionStart != null ? ae.selectionStart : null,
        end:   ae.selectionEnd   != null ? ae.selectionEnd   : null,
      };
    }
    var container = this._getList();
    if (container && container.contains(ae) && ae.dataset && ae.dataset.modelId) {
      return {
        modelId:    ae.dataset.modelId,
        modelField: ae.dataset.modelField,
        start: ae.selectionStart != null ? ae.selectionStart : null,
        end:   ae.selectionEnd   != null ? ae.selectionEnd   : null,
      };
    }
    return null;
  }

  _restoreFocus(saved) {
    if (!saved) return;
    if (saved.id) {
      var el = document.getElementById(saved.id);
      if (!el) return;
      el.focus();
      if (saved.start !== null) {
        try { el.setSelectionRange(saved.start, saved.end); } catch (e) {}
      }
      return;
    }
    if (saved.modelId) {
      var container = this._getList();
      if (!container) return;
      var el2 = container.querySelector('[data-model-id="' + saved.modelId + '"][data-model-field="' + saved.modelField + '"]');
      if (!el2) return;
      el2.focus();
      if (saved.start !== null) {
        try { el2.setSelectionRange(saved.start, saved.end); } catch (e) {}
      }
    }
  }

  _detachCard() {
    var card   = this._getCard();
    var holder = this._getHolder();
    if (card && holder && card.parentNode !== holder) holder.appendChild(card);
  }

  _attachCardToExpand(expandEl) {
    var card = this._getCard();
    if (card && expandEl) expandEl.appendChild(card);
  }

  _findItem(id) {
    for (var i = 0; i < this._data.length; i++) {
      if (String(this._data[i].id) === String(id)) return this._data[i];
    }
    return null;
  }

  _renderGradeBadge(item) {
    var LABELS  = this._gl();
    var COLORS  = this._gc();
    var TCOLORS = this._gtc();
    var grade   = item.grade !== undefined ? item.grade : null;
    var gk      = AccordionList.gradeKey(grade);
    var label   = LABELS[gk] || gk;
    var inlineStyle = gk !== 'null'
      ? 'style="background:' + (COLORS[gk] || '') + ';color:' + (TCOLORS[gk] || '') + '" '
      : '';
    return '<button class="story-grade-badge" data-id="' + item.id + '" data-grade="' + gk + '" '
      + inlineStyle
      + 'title="Оценка: ' + label + '. Нажмите для смены">'
      + label + '</button>';
  }

  _renderRow(item) {
    var gradeBadge  = this._renderGradeBadge(item);
    var titleHtml   = this._renderTitle(item) + ' ' + gradeBadge;
    var buttonsHtml = this._renderButtons(item);
    var extraClass  = this._rowClassFn ? (' ' + this._rowClassFn(item)) : '';
    return '<div class="story-row' + extraClass + '" data-id="' + item.id + '">'
      + '<div class="story-row-header">'
        + '<div class="story-title">' + titleHtml + '</div>'
        + '<div class="story-row-right">' + buttonsHtml
          + '<button class="story-chevron" title="Развернуть">' + AccordionList.CHEVRON_SVG + '</button>'
        + '</div>'
      + '</div>'
      + '<div class="story-expand"></div>'
      + '</div>';
  }

  _applyGradeToBadge(btn, gk) {
    var LABELS  = this._gl();
    var COLORS  = this._gc();
    var TCOLORS = this._gtc();
    btn.setAttribute('data-grade', gk);
    btn.style.background = gk !== 'null' ? (COLORS[gk]  || '') : '';
    btn.style.color      = gk !== 'null' ? (TCOLORS[gk] || '') : '';
    btn.textContent = LABELS[gk] || gk;
    btn.title = 'Оценка: ' + (LABELS[gk] || gk) + '. Нажмите для смены';
  }

  _bindEvents(container) {
    var self = this;
    container.querySelectorAll('.story-row-header').forEach(function(header) {
      var row = header.closest('.story-row');
      if (!row || row.getAttribute('data-id') === '__new__') return;
      header.addEventListener('click', function(e) {
        if (e.target.closest('.story-grade-badge')) return;
        if (e.target.closest('[data-role]')) return;
        if (e.target.closest('button:not(.story-chevron)')) return;
        self._toggleRow(row.getAttribute('data-id'));
      });
    });
    container.querySelectorAll('.story-grade-badge').forEach(function(btn) {
      btn.addEventListener('click', function(e) {
        e.stopPropagation();
        self.cycleGrade(btn);
      });
    });
  }

  _toggleRow(rowId) {
    var container = this._getList();
    if (!container) return;

    if (this._activeId && String(this._activeId) === String(rowId)) {
      var expandedRow = container.querySelector('.story-row--expanded');
      if (expandedRow) expandedRow.classList.remove('story-row--expanded');
      this._detachCard();
      this._activeId = null;
      if (this._onCollapse) this._onCollapse();
    } else {
      if (this._activeId) {
        var oldRow = container.querySelector('.story-row--expanded');
        if (oldRow) oldRow.classList.remove('story-row--expanded');
        this._detachCard();
      }
      this._activeId = rowId;
      var newRow = container.querySelector('.story-row[data-id="' + rowId + '"]');
      if (newRow) {
        newRow.classList.add('story-row--expanded');
        var expandEl = newRow.querySelector('.story-expand');
        if (expandEl) this._attachCardToExpand(expandEl);
        newRow.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        var item = this._findItem(rowId);
        if (this._onExpand) this._onExpand(item, expandEl);
      }
    }
  }

  selectRow(rowId) {
    var container = this._getList();
    if (!container) return;

    if (this._activeId) {
      var oldRow = container.querySelector('.story-row--expanded');
      if (oldRow) oldRow.classList.remove('story-row--expanded');
      this._detachCard();
    }

    this._activeId = rowId || null;

    if (this._activeId) {
      var newRow = container.querySelector('.story-row[data-id="' + this._activeId + '"]');
      if (newRow) {
        newRow.classList.add('story-row--expanded');
        var expandEl = newRow.querySelector('.story-expand');
        if (expandEl) this._attachCardToExpand(expandEl);
        newRow.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
        var item = this._findItem(this._activeId);
        if (this._onExpand) this._onExpand(item, expandEl);
      }
    } else {
      if (this._onCollapse) this._onCollapse();
    }
  }

  collapse() {
    var container = this._getList();
    if (!container) return;
    var expandedRow = container.querySelector('.story-row--expanded');
    if (expandedRow) expandedRow.classList.remove('story-row--expanded');
    this._detachCard();
    this._activeId = null;
  }

  render(items) {
    var container = this._getList();
    if (!container) return;

    var savedFocus   = this._saveFocus();
    var prevActiveId = this._activeId;
    var hadFakeRow   = this._canAddNew && !!container.querySelector('.story-row[data-id="__new__"]');
    this._detachCard();

    this._data = items || [];

    if (!this._data.length) {
      this.updateCount(0);
      this._activeId = null;
      container.innerHTML = this._emptyHtml;
      return;
    }

    this.updateCount(this._data.length);

    var html = '';
    for (var i = 0; i < this._data.length; i++) {
      html += this._renderRow(this._data[i]);
    }
    container.innerHTML = html;

    this._bindEvents(container);

    if (prevActiveId) {
      var expandRow = container.querySelector('.story-row[data-id="' + prevActiveId + '"]');
      if (expandRow) {
        this._activeId = prevActiveId;
        expandRow.classList.add('story-row--expanded');
        var expandEl2 = expandRow.querySelector('.story-expand');
        if (expandEl2) {
          this._attachCardToExpand(expandEl2);
          if (this._onExpandOnRerender) {
            var item2 = this._findItem(prevActiveId);
            if (this._onExpand) this._onExpand(item2, expandEl2);
          }
        }
      } else {
        this._activeId = null;
      }
    } else if (hadFakeRow) {
      this.insertFakeRow(false);
    }

    this._restoreFocus(savedFocus);
  }

  insertFakeRow(focusTitle) {
    if (!this._canAddNew) return;
    var container = this._getList();
    if (!container) return;
    var existing = container.querySelector('.story-row[data-id="__new__"]');
    if (existing) existing.remove();
    var fakeRow = document.createElement('div');
    fakeRow.className = 'story-row story-row--expanded';
    fakeRow.setAttribute('data-id', '__new__');
    fakeRow.innerHTML =
      '<div class="story-row-header">'
        + '<div class="story-title" style="color:#888;font-style:italic">Новый сюжет</div>'
        + '<div class="story-row-right">'
          + '<span class="story-chevron">' + AccordionList.CHEVRON_SVG + '</span>'
        + '</div>'
      + '</div>'
      + '<div class="story-expand"></div>';
    container.insertBefore(fakeRow, container.firstChild);
    var expandEl = fakeRow.querySelector('.story-expand');
    if (expandEl) this._attachCardToExpand(expandEl);
    fakeRow.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    if (this._onNewRowReady) this._onNewRowReady(expandEl);
    if (focusTitle) {
      var titleEl = document.getElementById('draft-story-title');
      if (titleEl) titleEl.focus();
    }
  }

  cycleGrade(btn) {
    var self         = this;
    var CYCLE        = this._gcy();
    var itemId       = btn.getAttribute('data-id');
    var currentAttr  = btn.getAttribute('data-grade');
    var current      = currentAttr === 'null' ? null : currentAttr;
    var idx          = CYCLE.indexOf(current);
    var next         = CYCLE[(idx + 1) % CYCLE.length];
    var prevAttr     = currentAttr;

    this._gradeReqCounters[itemId] = (this._gradeReqCounters[itemId] || 0) + 1;
    var myReqId = this._gradeReqCounters[itemId];

    this._applyGradeToBadge(btn, AccordionList.gradeKey(next));

    fetch(this._gradeUrl(itemId), {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ grade: next }),
    })
    .then(function(r) { return r.ok ? r.json() : null; })
    .then(function(d) {
      if (myReqId !== self._gradeReqCounters[itemId]) return;
      if (d && d.ok) {
        var confirmed = d.grade !== undefined ? d.grade : next;
        self._applyGradeToBadge(btn, AccordionList.gradeKey(confirmed));
        var item = self._findItem(itemId);
        if (item) item.grade = confirmed;
      } else {
        self._applyGradeToBadge(btn, AccordionList.gradeKey(prevAttr === 'null' ? null : prevAttr));
      }
    })
    .catch(function() {
      if (myReqId !== self._gradeReqCounters[itemId]) return;
      self._applyGradeToBadge(btn, AccordionList.gradeKey(prevAttr === 'null' ? null : prevAttr));
    });
  }
}

AccordionList.GRADE_LABELS      = { good: 'хорошо', bad: 'плохо', 'null': 'не указано' };
AccordionList.GRADE_COLORS      = { good: 'rgba(62,207,142,.18)', bad: 'rgba(255,80,80,.18)', 'null': 'rgba(255,255,255,.1)' };
AccordionList.GRADE_TEXT_COLORS = { good: '#3ecf8e', bad: '#ff6060', 'null': '#888' };
AccordionList.GRADE_CYCLE       = ['good', 'bad', null];
AccordionList.CHEVRON_SVG       = '<svg viewBox="0 0 12 7" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M1 1l5 5 5-5"/></svg>';
