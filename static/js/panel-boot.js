(function() {
  var TITLES = {
    screenwriter: 'Сценарист',
    director:     'Режиссер',
    workflow:     'Рабочий поток',
    story:        'Генерация сюжета',
    request:      'Генерация видео',
    publish:      'Публикация',
    log:          'Монитор',
    service:      'Служебные',
    info:         'Информация',
  };

  function resolveName(name) {
    return name === 'pipeline' ? 'workflow' : name;
  }

  function storageKey() {
    return 'vip_active_panel_' + window.location.pathname;
  }

  function readInitialPanelName() {
    var tabParam = new URLSearchParams(window.location.search).get('tab');
    if (tabParam) {
      tabParam = resolveName(tabParam);
      if (document.getElementById('panel-' + tabParam)) return tabParam;
    }
    try {
      var saved = localStorage.getItem(storageKey());
      if (!saved) return null;
      saved = resolveName(saved);
      return document.getElementById('panel-' + saved) ? saved : null;
    } catch (e) {
      return null;
    }
  }

  function applyVisual(name) {
    if (!name) return;
    document.querySelectorAll('.tab-panel').forEach(function(p) { p.classList.remove('active'); });
    document.querySelectorAll('.sidebar-item').forEach(function(b) { b.classList.remove('active'); });
    var panel = document.getElementById('panel-' + name);
    if (panel) panel.classList.add('active');
    var btn = document.querySelector('.sidebar-item[data-panel="' + name + '"]');
    if (btn) btn.classList.add('active');
    var titleEl = document.getElementById('page-title');
    if (titleEl) titleEl.textContent = TITLES[name] || name;
  }

  applyVisual(readInitialPanelName());
  document.documentElement.classList.remove('vip-panels-pending');
})();
