(function () {
  'use strict';

  var currentDB = '0';
  var keyCursor = 0;
  var hasMoreKeys = false;
  var selectedKey = '';
  var refreshTimer = null;

  var els = {
    dbSelect: document.getElementById('db-select'),
    dbState: document.getElementById('db-state'),
    dbStats: document.getElementById('db-stats'),
    serverMetrics: document.getElementById('server-metrics'),
    dbMetrics: document.getElementById('db-metrics'),
    metricsDot: document.getElementById('metrics-dot'),
    prefixFilter: document.getElementById('prefix-filter'),
    keyList: document.getElementById('key-list'),
    editKey: document.getElementById('edit-key'),
    editValue: document.getElementById('edit-value'),
    editorStatus: document.getElementById('editor-status'),
    confirmDialog: document.getElementById('confirm-dialog'),
    confirmMessage: document.getElementById('confirm-message'),
    confirmCancel: document.getElementById('confirm-cancel'),
    confirmOk: document.getElementById('confirm-ok')
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
    if (n >= 1e9) return (n / 1e9).toFixed(1) + 'G';
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
    return String(n);
  }

  function setStatus(msg, isErr) {
    els.editorStatus.textContent = msg || '';
    els.editorStatus.className = 'status' + (isErr ? ' err' : msg ? ' ok' : '');
  }

  function renderMetrics(container, samples, labelFn) {
    if (!samples || samples.length === 0) {
      container.innerHTML = '<p class="placeholder">No metrics available</p>';
      return;
    }
    var html = '<div class="metrics-grid">';
    for (var i = 0; i < samples.length; i++) {
      var s = samples[i];
      var label = labelFn ? labelFn(s) : s.name.replace('turnstone_', '').replace(/_/g, ' ');
      var cls = '';
      if (s.name.indexOf('conflict') !== -1 && s.value > 0) cls = 'warn';
      if (s.name.indexOf('lag') !== -1 && s.value > 0) cls = 'warn';
      html += '<div class="metric"><div class="label">' + escapeHtml(label) +
        '</div><div class="value ' + cls + '">' + escapeHtml(fmt(s.value)) + '</div></div>';
    }
    html += '</div>';
    container.innerHTML = html;
  }

  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function loadDatabases() {
    return api('/api/databases').then(function (data) {
      var dbs = data.databases || [];
      els.dbSelect.innerHTML = '';
      for (var i = 0; i < dbs.length; i++) {
        var opt = document.createElement('option');
        opt.value = dbs[i];
        opt.textContent = dbs[i];
        els.dbSelect.appendChild(opt);
      }
      if (dbs.length > 0) {
        currentDB = dbs[0];
        els.dbSelect.value = currentDB;
      }
    });
  }

  function loadStats() {
    return api('/api/databases/' + currentDB + '/stats').then(function (stats) {
      var stateClass = stats.state.split(' ')[0];
      els.dbState.innerHTML = '<span class="state-badge state-' + escapeHtml(stateClass) + '">' +
        escapeHtml(stats.state) + '</span>';

      var items = [
        ['Keys', stats.key_count],
        ['Active Txs', stats.active_txs],
        ['Connections', stats.active_connections],
        ['Conflicts', stats.conflicts],
        ['Log Bytes', stats.log_bytes],
        ['Log Allocated', stats.log_allocated_bytes],
        ['Replica Lag', stats.replica_lag],
        ['Uptime', stats.uptime],
        ['Min Replicas', stats.min_replicas]
      ];

      var html = '<div class="metrics-grid">';
      for (var i = 0; i < items.length; i++) {
        var val = typeof items[i][1] === 'number' ? fmt(items[i][1]) : items[i][1];
        html += '<div class="metric"><div class="label">' + escapeHtml(items[i][0]) +
          '</div><div class="value">' + escapeHtml(val) + '</div></div>';
      }
      html += '</div>';
      els.dbStats.innerHTML = html;
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
      renderMetrics(els.dbMetrics, dbSamples, function (s) {
        return s.name.replace('turnstone_db_', '').replace(/_/g, ' ');
      });
      els.metricsDot.className = 'dot ok';
    }).catch(function (e) {
      els.serverMetrics.innerHTML = '<p class="placeholder err">' + escapeHtml(e.message) + '</p>';
      els.metricsDot.className = 'dot err';
    });
  }

  function loadKeys(append) {
    var prefix = els.prefixFilter.value;
    if (!append) keyCursor = 0;

    return api('/api/databases/' + currentDB + '/keys?prefix=' +
      encodeURIComponent(prefix) + '&cursor=' + keyCursor + '&limit=100')
      .then(function (data) {
        var keys = data.keys || [];
        keyCursor = data.next_cursor;
        hasMoreKeys = data.has_more;

        if (keys.length === 0 && !append) {
          els.keyList.innerHTML = '<p class="placeholder">No keys found</p>';
          return;
        }

        if (!append) {
          els.keyList.innerHTML = '';
        } else if (els.keyList.querySelector('.placeholder')) {
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
    if (!key) { setStatus('Enter a key', true); return; }
    api('/api/databases/' + currentDB + '/keys/' + encodeURIComponent(key))
      .then(function (data) {
        els.editValue.value = data.value;
        setStatus('Loaded');
      })
      .catch(function (e) { setStatus(e.message, true); });
  }

  function setKey() {
    var key = els.editKey.value.trim();
    var value = els.editValue.value;
    if (!key) { setStatus('Enter a key', true); return; }
    api('/api/databases/' + currentDB + '/keys/' + encodeURIComponent(key), {
      method: 'PUT',
      headers: { 'Content-Type': 'text/plain' },
      body: value
    }).then(function () {
      setStatus('Set OK');
      loadKeys();
      loadStats();
    }).catch(function (e) { setStatus(e.message, true); });
  }

  function confirmDelete(message) {
    return new Promise(function (resolve) {
      els.confirmMessage.textContent = message;
      function cleanup(result) {
        els.confirmDialog.close();
        els.confirmOk.removeEventListener('click', onOk);
        els.confirmCancel.removeEventListener('click', onCancel);
        resolve(result);
      }
      function onOk() { cleanup(true); }
      function onCancel() { cleanup(false); }
      els.confirmOk.addEventListener('click', onOk);
      els.confirmCancel.addEventListener('click', onCancel);
      els.confirmDialog.showModal();
    });
  }

  function delKey() {
    var key = els.editKey.value.trim();
    if (!key) { setStatus('Enter a key', true); return; }
    confirmDelete('Delete key "' + key + '"?').then(function (ok) {
      if (!ok) return;
      api('/api/databases/' + currentDB + '/keys/' + encodeURIComponent(key), {
        method: 'DELETE'
      }).then(function () {
        els.editValue.value = '';
        selectedKey = '';
        setStatus('Deleted');
        loadKeys();
        loadStats();
      }).catch(function (e) { setStatus(e.message, true); });
    });
  }

  function refreshAll() {
    loadStats();
    loadMetrics();
    loadKeys();
  }

  function onDbChange() {
    currentDB = els.dbSelect.value;
    keyCursor = 0;
    selectedKey = '';
    refreshAll();
  }

  function scheduleRefresh() {
    if (refreshTimer) clearInterval(refreshTimer);
    refreshTimer = setInterval(function () {
      loadStats();
      loadMetrics();
    }, 5000);
  }

  document.getElementById('btn-refresh').addEventListener('click', refreshAll);
  document.getElementById('btn-search-keys').addEventListener('click', function () { loadKeys(); });
  document.getElementById('btn-more-keys').addEventListener('click', function () {
    if (hasMoreKeys) loadKeys(true);
  });
  document.getElementById('btn-get').addEventListener('click', getKey);
  document.getElementById('btn-set').addEventListener('click', setKey);
  document.getElementById('btn-delete').addEventListener('click', delKey);
  els.dbSelect.addEventListener('change', onDbChange);
  els.prefixFilter.addEventListener('keydown', function (ev) {
    if (ev.key === 'Enter') loadKeys();
  });

  loadDatabases().then(refreshAll).then(scheduleRefresh);
})();
