(function () {
  'use strict';

  var currentDB = '0';
  var keyCursor = 0;
  var hasMoreKeys = false;
  var selectedKey = '';
  var refreshTimer = null;
  var searchTimer = null;
  var toastTimer = null;
  var lastKeyCount = 0;
  var sparkHistory = { replica_lag: [], log_bytes: [], key_count: [] };
  var SPARK_MAX = 24;

  var els = {
    dbSelect: document.getElementById('db-select'),
    dbState: document.getElementById('db-state'),
    summaryKeys: document.getElementById('summary-keys'),
    summaryTxs: document.getElementById('summary-txs'),
    lastUpdated: document.getElementById('last-updated'),
    keyCount: document.getElementById('key-count'),
    keyCountInline: document.getElementById('key-count-inline'),
    overviewKeys: document.getElementById('overview-keys'),
    overviewTxs: document.getElementById('overview-txs'),
    overviewState: document.getElementById('overview-state'),
    overviewUpdated: document.getElementById('overview-updated'),
    globalStateDot: document.getElementById('global-state-dot'),
    dbStats: document.getElementById('db-stats'),
    serverMetrics: document.getElementById('server-metrics'),
    dbMetrics: document.getElementById('db-metrics'),
    dbMetricsProm: document.getElementById('db-metrics-prom'),
    metricsDot: document.getElementById('metrics-dot'),
    prefixFilter: document.getElementById('prefix-filter'),
    headerSearch: document.getElementById('header-search'),
    keyList: document.getElementById('key-list'),
    btnMoreKeys: document.getElementById('btn-more-keys'),
    editKey: document.getElementById('edit-key'),
    editValue: document.getElementById('edit-value'),
    valueSize: document.getElementById('value-size'),
    editorStatus: document.getElementById('editor-status'),
    toast: document.getElementById('toast'),
    confirmDialog: document.getElementById('confirm-dialog'),
    confirmMessage: document.getElementById('confirm-message'),
    confirmCancel: document.getElementById('confirm-cancel'),
    confirmOk: document.getElementById('confirm-ok'),
    btnRefresh: document.getElementById('btn-refresh'),
    settingsDbSelect: document.getElementById('settings-db-select'),
    btnSettingsRefresh: document.getElementById('btn-settings-refresh'),
    dataArea: document.querySelector('.data-area')
  };

  function api(path, opts) {
    return fetch(path, opts).then(function (res) {
      if (!res.ok) {
        return res.json().catch(function () {
          return { error: res.statusText };
        }).then(function (body) {
          throw new Error(body.error || res.statusText);
        });
      }
      if (res.status === 204) return null;
      return res.json();
    });
  }

  function fmt(n) {
    if (typeof n !== 'number') return String(n);
    if (n >= 1e9) return (n / 1e9).toFixed(1) + ' GB';
    if (n >= 1e6) return (n / 1e6).toFixed(1) + ' MB';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + ' KB';
    return String(n);
  }

  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function pushSpark(key, value) {
    if (!sparkHistory[key]) sparkHistory[key] = [];
    sparkHistory[key].push(value);
    if (sparkHistory[key].length > SPARK_MAX) sparkHistory[key].shift();
  }

  function drawSparkline(canvas, values) {
    if (!canvas || !values || values.length < 2) return;
    var ctx = canvas.getContext('2d');
    var w = canvas.width = canvas.offsetWidth || 120;
    var h = canvas.height = 28;
    ctx.clearRect(0, 0, w, h);
    var max = 0;
    var min = Infinity;
    for (var i = 0; i < values.length; i++) {
      if (values[i] > max) max = values[i];
      if (values[i] < min) min = values[i];
    }
    if (max === min) max = min + 1;
    ctx.strokeStyle = '#ffc107';
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    for (var j = 0; j < values.length; j++) {
      var x = (j / (values.length - 1)) * (w - 4) + 2;
      var y = h - 4 - ((values[j] - min) / (max - min)) * (h - 8);
      if (j === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.stroke();
  }

  function setStatus(msg, isErr) {
    els.editorStatus.textContent = msg || '';
    els.editorStatus.className = 'status' + (isErr ? ' err' : msg ? ' ok' : '');
  }

  function showToast(msg, type) {
    els.toast.textContent = msg;
    els.toast.className = 'toast show' + (type ? ' ' + type : '');
    els.toast.hidden = false;
    if (toastTimer) clearTimeout(toastTimer);
    toastTimer = setTimeout(function () {
      els.toast.className = 'toast';
      els.toast.hidden = true;
    }, 2800);
  }

  function setLoading(btn, loading) {
    if (!btn) return;
    btn.disabled = loading;
    btn.classList.toggle('loading', loading);
  }

  function updateValueSize() {
    var bytes = new TextEncoder().encode(els.editValue.value).length;
    els.valueSize.textContent = bytes + (bytes === 1 ? ' byte' : ' bytes');
  }

  function updateLastUpdated() {
    var now = new Date();
    var timeStr = now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    els.lastUpdated.textContent = timeStr;
    if (els.overviewUpdated) els.overviewUpdated.textContent = 'Last Updated: ' + timeStr;
  }

  function updateKeyCount(count) {
    lastKeyCount = count;
    var label = count === 1 ? '1 key' : count + ' keys';
    els.keyCount.textContent = label;
    if (els.keyCountInline) els.keyCountInline.textContent = label;
    els.summaryKeys.textContent = fmt(count);
    if (els.overviewKeys) els.overviewKeys.textContent = fmt(count);
    pushSpark('key_count', count);
  }

  function updateMoreButton() {
    els.btnMoreKeys.disabled = !hasMoreKeys;
    els.btnMoreKeys.textContent = hasMoreKeys ? 'Load more' : 'No more keys';
  }

  function emptyKeyList(title, hint) {
    els.keyList.innerHTML =
      '<p class="empty-state">' +
      '<span class="empty-title">' + escapeHtml(title) + '</span>' +
      (hint ? '<span class="empty-hint">' + escapeHtml(hint) + '</span>' : '') +
      '</p>';
  }

  function metricIconClass(label) {
    if (/active|tx/i.test(label)) return 'orange';
    return 'teal';
  }

  function renderMetrics(container, samples, labelFn, highlights, sparkKeys) {
    highlights = highlights || [];
    sparkKeys = sparkKeys || {};
    if (!samples || samples.length === 0) {
      container.innerHTML = '<p class="empty-state"><span class="empty-title">No metrics available</span></p>';
      return;
    }
    var html = '<div class="metrics-grid">';
    for (var i = 0; i < samples.length; i++) {
      var s = samples[i];
      var label = labelFn ? labelFn(s) : s.name.replace('turnstone_', '').replace(/_/g, ' ');
      var cls = highlights.indexOf(label) !== -1 ? ' highlight' : '';
      if (/active tx/i.test(label)) cls += ' warn-tile';
      var valueCls = (s.name.indexOf('conflict') !== -1 && s.value > 0) ||
        (s.name.indexOf('lag') !== -1 && s.value > 0) ? ' warn' : '';
      var iconCls = metricIconClass(label);
      var sparkId = 'spark-' + container.id + '-' + i;
      var sparkHtml = '';
      var sparkKey = sparkKeys[label] || sparkKeys[s.name];
      if (sparkKey && sparkHistory[sparkKey] && sparkHistory[sparkKey].length > 1) {
        sparkHtml = '<div class="metric-sparkline"><canvas id="' + sparkId + '" height="28"></canvas></div>';
      }
      html += '<div class="metric' + cls + '"><div class="label">' +
        '<span class="metric-icon ' + iconCls + '"></span>' + escapeHtml(label) +
        '</div><div class="value' + valueCls + '">' + escapeHtml(fmt(s.value)) + '</div>' +
        sparkHtml + '</div>';
    }
    html += '</div>';
    container.innerHTML = html;

    for (var j = 0; j < samples.length; j++) {
      var s2 = samples[j];
      var lbl = labelFn ? labelFn(s2) : s2.name.replace('turnstone_', '').replace(/_/g, ' ');
      var sk = sparkKeys[lbl] || sparkKeys[s2.name];
      if (sk && sparkHistory[sk] && sparkHistory[sk].length > 1) {
        var canvas = document.getElementById('spark-' + container.id + '-' + j);
        drawSparkline(canvas, sparkHistory[sk]);
      }
    }
  }

  function renderStateBadge(state) {
    var stateClass = state.split(' ')[0];
    return '<span class="state-badge state-' + escapeHtml(stateClass) + '">' + escapeHtml(state) + '</span>';
  }

  function updateStateUI(state) {
    var badge = renderStateBadge(state);
    els.dbState.innerHTML = badge;
    if (els.overviewState) els.overviewState.innerHTML = badge;
    if (els.globalStateDot) {
      els.globalStateDot.className = 'status-dot state';
      if (stateClass(state) === 'PRIMARY') els.globalStateDot.style.background = 'var(--ok)';
      else if (stateClass(state) === 'REPLICA') els.globalStateDot.style.background = 'var(--orange)';
      else els.globalStateDot.style.background = 'var(--text-muted)';
    }
  }

  function stateClass(state) {
    return state.split(' ')[0];
  }

  function loadDatabases() {
    return api('/api/databases').then(function (data) {
      var dbs = data.databases || [];
      els.dbSelect.innerHTML = '';
      if (els.settingsDbSelect) els.settingsDbSelect.innerHTML = '';
      for (var i = 0; i < dbs.length; i++) {
        var opt = document.createElement('option');
        opt.value = dbs[i];
        opt.textContent = 'DB ' + dbs[i];
        els.dbSelect.appendChild(opt);
        if (els.settingsDbSelect) {
          var opt2 = document.createElement('option');
          opt2.value = dbs[i];
          opt2.textContent = 'DB ' + dbs[i];
          els.settingsDbSelect.appendChild(opt2);
        }
      }
      if (dbs.length > 0) {
        currentDB = dbs[0];
        els.dbSelect.value = currentDB;
        if (els.settingsDbSelect) els.settingsDbSelect.value = currentDB;
      }
    });
  }

  function loadStats() {
    return api('/api/databases/' + currentDB + '/stats').then(function (stats) {
      updateStateUI(stats.state);
      els.summaryTxs.textContent = String(stats.active_txs);
      if (els.overviewTxs) els.overviewTxs.textContent = String(stats.active_txs);
      updateKeyCount(stats.key_count);
      pushSpark('replica_lag', stats.replica_lag || 0);
      pushSpark('log_bytes', stats.log_bytes || 0);

      var items = [
        ['Keys', stats.key_count, true, 'key_count'],
        ['Active Txs', stats.active_txs, true, null],
        ['Connections', stats.active_connections, false, null],
        ['Conflicts', stats.conflicts, false, null],
        ['Log Size', stats.log_bytes, false, 'log_bytes'],
        ['Log Allocated', stats.log_allocated_bytes, false, null],
        ['Replica Lag', stats.replica_lag, false, 'replica_lag'],
        ['Uptime', stats.uptime, false, null],
        ['Min Replicas', stats.min_replicas, false, null]
      ];

      var html = '<div class="metrics-grid">';
      for (var i = 0; i < items.length; i++) {
        var val = typeof items[i][1] === 'number' ? fmt(items[i][1]) : items[i][1];
        var hl = items[i][2] ? ' highlight' : '';
        if (items[i][0] === 'Active Txs') hl += ' warn-tile';
        var iconCls = items[i][0] === 'Active Txs' ? 'orange' : 'teal';
        var sparkHtml = '';
        if (items[i][3] && sparkHistory[items[i][3]] && sparkHistory[items[i][3]].length > 1) {
          sparkHtml = '<div class="metric-sparkline"><canvas class="db-spark" data-spark="' +
            items[i][3] + '" height="28"></canvas></div>';
        }
        html += '<div class="metric' + hl + '"><div class="label">' +
          '<span class="metric-icon ' + iconCls + '"></span>' + escapeHtml(items[i][0]) +
          '</div><div class="value">' + escapeHtml(val) + '</div>' + sparkHtml + '</div>';
      }
      html += '</div>';
      els.dbStats.innerHTML = html;

      var sparks = els.dbStats.querySelectorAll('.db-spark');
      for (var s = 0; s < sparks.length; s++) {
        var key = sparks[s].getAttribute('data-spark');
        drawSparkline(sparks[s], sparkHistory[key]);
      }

      updateLastUpdated();
    });
  }

  function loadMetrics() {
    return api('/api/metrics').then(function (data) {
      renderMetrics(els.serverMetrics, data.server, function (s) {
        return s.name.replace('turnstone_server_', '').replace(/_/g, ' ');
      });
      var dbSamples = (data.db || []).filter(function (s) {
        return s.labels && s.labels.db === currentDB;
      });
      var sparkMap = { 'key count': 'key_count', 'log bytes': 'log_bytes', 'replica lag': 'replica_lag' };
      renderMetrics(els.dbMetrics, dbSamples, function (s) {
        return s.name.replace('turnstone_db_', '').replace(/_/g, ' ');
      }, [], sparkMap);
      if (els.dbMetricsProm) {
        renderMetrics(els.dbMetricsProm, dbSamples, function (s) {
          return s.name.replace('turnstone_db_', '').replace(/_/g, ' ');
        }, [], sparkMap);
      }
      els.metricsDot.className = 'dot ok';
    }).catch(function (e) {
      els.serverMetrics.innerHTML = '<p class="empty-state err"><span class="empty-title">' +
        escapeHtml(e.message) + '</span></p>';
      els.metricsDot.className = 'dot err';
    });
  }

  function loadKeys(append) {
    var prefix = els.prefixFilter.value;
    if (!append) keyCursor = 0;
    els.keyList.classList.add('loading');

    return api('/api/databases/' + currentDB + '/keys?prefix=' +
      encodeURIComponent(prefix) + '&cursor=' + keyCursor + '&limit=100')
      .then(function (data) {
        var keys = data.keys || [];
        keyCursor = data.next_cursor;
        hasMoreKeys = data.has_more;
        updateMoreButton();

        if (keys.length === 0 && !append) {
          if (prefix) {
            emptyKeyList('No matching keys', 'Try a different prefix or create a new key.');
          } else {
            emptyKeyList('No keys found', 'Create one in the editor.');
          }
          return;
        }

        if (!append) {
          els.keyList.innerHTML = '';
        } else if (els.keyList.querySelector('.empty-state')) {
          els.keyList.innerHTML = '';
        }

        for (var i = 0; i < keys.length; i++) {
          (function (k) {
            var el = document.createElement('div');
            el.className = 'key-item' + (k === selectedKey ? ' selected' : '');
            el.textContent = k;
            el.setAttribute('role', 'option');
            el.setAttribute('tabindex', '0');
            el.setAttribute('data-testid', 'key-item');
            el.addEventListener('click', function () { selectKey(k); });
            el.addEventListener('keydown', function (ev) {
              if (ev.key === 'Enter' || ev.key === ' ') {
                ev.preventDefault();
                selectKey(k);
              }
            });
            els.keyList.appendChild(el);
          })(keys[i]);
        }
      })
      .finally(function () {
        els.keyList.classList.remove('loading');
      });
  }

  function selectKey(k) {
    selectedKey = k;
    els.editKey.value = k;
    var items = els.keyList.querySelectorAll('.key-item');
    for (var i = 0; i < items.length; i++) {
      items[i].classList.toggle('selected', items[i].textContent === k);
    }
    getKey();
  }

  function getKey() {
    var key = els.editKey.value.trim();
    if (!key) { setStatus('Enter a key name', true); return; }
    setLoading(document.getElementById('btn-get'), true);
    api('/api/databases/' + currentDB + '/keys/' + encodeURIComponent(key))
      .then(function (data) {
        els.editValue.value = data.value;
        updateValueSize();
        setStatus('Loaded');
        showToast('Key loaded', 'ok');
      })
      .catch(function (e) { setStatus(e.message, true); })
      .finally(function () { setLoading(document.getElementById('btn-get'), false); });
  }

  function setKey() {
    var key = els.editKey.value.trim();
    var value = els.editValue.value;
    if (!key) { setStatus('Enter a key name', true); return; }
    setLoading(document.getElementById('btn-set'), true);
    api('/api/databases/' + currentDB + '/keys/' + encodeURIComponent(key), {
      method: 'PUT',
      headers: { 'Content-Type': 'text/plain' },
      body: value
    }).then(function () {
      selectedKey = key;
      setStatus('Saved');
      showToast('Key saved', 'ok');
      loadKeys();
      loadStats();
    }).catch(function (e) { setStatus(e.message, true); })
      .finally(function () { setLoading(document.getElementById('btn-set'), false); });
  }

  function copyValue() {
    var value = els.editValue.value;
    if (!value) {
      setStatus('Nothing to copy', true);
      return;
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(value).then(function () {
        showToast('Copied to clipboard', 'ok');
      }).catch(function () {
        setStatus('Copy failed', true);
      });
    } else {
      els.editValue.select();
      try {
        document.execCommand('copy');
        showToast('Copied to clipboard', 'ok');
      } catch (_) {
        setStatus('Copy failed', true);
      }
    }
  }

  function clearEditor() {
    els.editKey.value = '';
    els.editValue.value = '';
    selectedKey = '';
    updateValueSize();
    setStatus('');
    var items = els.keyList.querySelectorAll('.key-item');
    for (var i = 0; i < items.length; i++) {
      items[i].classList.remove('selected');
    }
    els.editKey.focus();
  }

  function confirmDelete(message) {
    return new Promise(function (resolve) {
      els.confirmMessage.textContent = message;
      function cleanup(result) {
        els.confirmDialog.close();
        els.confirmOk.removeEventListener('click', onOk);
        els.confirmCancel.removeEventListener('click', onCancel);
        document.removeEventListener('keydown', onKey);
        resolve(result);
      }
      function onOk() { cleanup(true); }
      function onCancel() { cleanup(false); }
      function onKey(ev) {
        if (ev.key === 'Escape') cleanup(false);
      }
      els.confirmOk.addEventListener('click', onOk);
      els.confirmCancel.addEventListener('click', onCancel);
      document.addEventListener('keydown', onKey);
      els.confirmDialog.showModal();
      els.confirmCancel.focus();
    });
  }

  function delKey() {
    var key = els.editKey.value.trim();
    if (!key) { setStatus('Enter a key name', true); return; }
    confirmDelete('This will permanently delete "' + key + '".').then(function (ok) {
      if (!ok) return;
      setLoading(document.getElementById('btn-delete'), true);
      api('/api/databases/' + currentDB + '/keys/' + encodeURIComponent(key), {
        method: 'DELETE'
      }).then(function () {
        els.editValue.value = '';
        selectedKey = '';
        updateValueSize();
        setStatus('Deleted');
        showToast('Key deleted', 'ok');
        loadStats().then(function () { loadKeys(); });
      }).catch(function (e) { setStatus(e.message, true); })
        .finally(function () { setLoading(document.getElementById('btn-delete'), false); });
    });
  }

  function refreshAll() {
    setLoading(els.btnRefresh, true);
    Promise.all([loadStats(), loadMetrics(), loadKeys()])
      .finally(function () { setLoading(els.btnRefresh, false); });
  }

  function onDbChange() {
    currentDB = els.dbSelect.value;
    if (els.settingsDbSelect) els.settingsDbSelect.value = currentDB;
    keyCursor = 0;
    selectedKey = '';
    clearEditor();
    sparkHistory = { replica_lag: [], log_bytes: [], key_count: [] };
    refreshAll();
  }

  function debouncedSearch() {
    if (searchTimer) clearTimeout(searchTimer);
    searchTimer = setTimeout(function () { loadKeys(); }, 300);
  }

  function syncSearchFields(fromHeader) {
    if (fromHeader) {
      els.prefixFilter.value = els.headerSearch.value;
    } else {
      els.headerSearch.value = els.prefixFilter.value;
    }
  }

  function scheduleRefresh() {
    if (refreshTimer) clearInterval(refreshTimer);
    refreshTimer = setInterval(function () {
      loadStats();
      loadMetrics();
    }, 5000);
  }

  function switchView(view) {
    var tabs = document.querySelectorAll('.view-tab');
    var panels = document.querySelectorAll('[data-view-panel]');
    for (var i = 0; i < tabs.length; i++) {
      tabs[i].classList.toggle('active', tabs[i].getAttribute('data-view') === view);
    }
    for (var j = 0; j < panels.length; j++) {
      var panelView = panels[j].getAttribute('data-view-panel');
      var active = panelView === view;
      if (view === 'data' && panelView === 'editor') active = false;
      if (view === 'editor' && panelView === 'data') {
        active = true;
        if (els.dataArea) {
          els.dataArea.classList.add('editor-only');
        }
      } else if (els.dataArea) {
        els.dataArea.classList.remove('editor-only');
      }
      panels[j].classList.toggle('active', active);
    }
  }

  document.getElementById('btn-refresh').addEventListener('click', refreshAll);
  document.getElementById('btn-search-keys').addEventListener('click', function () { loadKeys(); });
  els.btnMoreKeys.addEventListener('click', function () {
    if (hasMoreKeys) loadKeys(true);
  });
  document.getElementById('btn-get').addEventListener('click', getKey);
  document.getElementById('btn-set').addEventListener('click', setKey);
  document.getElementById('btn-copy').addEventListener('click', copyValue);
  document.getElementById('btn-clear').addEventListener('click', clearEditor);
  document.getElementById('btn-delete').addEventListener('click', delKey);
  els.dbSelect.addEventListener('change', onDbChange);
  els.prefixFilter.addEventListener('input', function () {
    syncSearchFields(false);
    debouncedSearch();
  });
  els.prefixFilter.addEventListener('keydown', function (ev) {
    if (ev.key === 'Enter') loadKeys();
  });
  if (els.headerSearch) {
    els.headerSearch.addEventListener('input', function () {
      syncSearchFields(true);
      debouncedSearch();
    });
    els.headerSearch.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter') loadKeys();
    });
  }
  els.editValue.addEventListener('input', updateValueSize);

  if (els.settingsDbSelect) {
    els.settingsDbSelect.addEventListener('change', function () {
      els.dbSelect.value = els.settingsDbSelect.value;
      onDbChange();
    });
  }
  if (els.btnSettingsRefresh) {
    els.btnSettingsRefresh.addEventListener('click', refreshAll);
  }

  document.querySelectorAll('.view-tab').forEach(function (tab) {
    tab.addEventListener('click', function () {
      switchView(tab.getAttribute('data-view'));
    });
  });

  document.addEventListener('keydown', function (ev) {
    var mod = ev.ctrlKey || ev.metaKey;
    if (mod && ev.key === 's') {
      ev.preventDefault();
      setKey();
    }
    if (mod && ev.key === 'Enter') {
      ev.preventDefault();
      getKey();
    }
    if (ev.key === 'r' && !mod && document.activeElement.tagName !== 'INPUT' &&
        document.activeElement.tagName !== 'TEXTAREA') {
      ev.preventDefault();
      refreshAll();
    }
  });

  updateMoreButton();
  updateValueSize();
  loadDatabases().then(refreshAll).then(scheduleRefresh);
})();
