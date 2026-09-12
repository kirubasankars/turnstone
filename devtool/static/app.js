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

  var els = {
    dbSelect: document.getElementById('db-select'),
    dbState: document.getElementById('db-state'),
    summaryKeys: document.getElementById('summary-keys'),
    summaryTxs: document.getElementById('summary-txs'),
    lastUpdated: document.getElementById('last-updated'),
    keyCount: document.getElementById('key-count'),
    dbStats: document.getElementById('db-stats'),
    serverMetrics: document.getElementById('server-metrics'),
    dbMetrics: document.getElementById('db-metrics'),
    metricsDot: document.getElementById('metrics-dot'),
    prefixFilter: document.getElementById('prefix-filter'),
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
    btnRefresh: document.getElementById('btn-refresh')
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
    els.lastUpdated.textContent = now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }

  function updateKeyCount(count) {
    lastKeyCount = count;
    var label = count === 1 ? '1 key' : count + ' keys';
    els.keyCount.textContent = label;
    els.summaryKeys.textContent = fmt(count);
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

  function renderMetrics(container, samples, labelFn, highlights) {
    highlights = highlights || [];
    if (!samples || samples.length === 0) {
      container.innerHTML = '<p class="empty-state"><span class="empty-title">No metrics available</span></p>';
      return;
    }
    var html = '<div class="metrics-grid">';
    for (var i = 0; i < samples.length; i++) {
      var s = samples[i];
      var label = labelFn ? labelFn(s) : s.name.replace('turnstone_', '').replace(/_/g, ' ');
      var cls = highlights.indexOf(label) !== -1 ? ' highlight' : '';
      if (s.name.indexOf('conflict') !== -1 && s.value > 0) cls += ' warn-value';
      if (s.name.indexOf('lag') !== -1 && s.value > 0) cls += ' warn-value';
      var valueCls = (s.name.indexOf('conflict') !== -1 && s.value > 0) ||
        (s.name.indexOf('lag') !== -1 && s.value > 0) ? ' warn' : '';
      html += '<div class="metric' + cls + '"><div class="label">' + escapeHtml(label) +
        '</div><div class="value' + valueCls + '">' + escapeHtml(fmt(s.value)) + '</div></div>';
    }
    html += '</div>';
    container.innerHTML = html;
  }

  function loadDatabases() {
    return api('/api/databases').then(function (data) {
      var dbs = data.databases || [];
      els.dbSelect.innerHTML = '';
      for (var i = 0; i < dbs.length; i++) {
        var opt = document.createElement('option');
        opt.value = dbs[i];
        opt.textContent = 'DB ' + dbs[i];
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
      els.summaryTxs.textContent = String(stats.active_txs);
      updateKeyCount(stats.key_count);

      var items = [
        ['Keys', stats.key_count, true],
        ['Active Txs', stats.active_txs, true],
        ['Connections', stats.active_connections, false],
        ['Conflicts', stats.conflicts, false],
        ['Log Size', stats.log_bytes, false],
        ['Log Allocated', stats.log_allocated_bytes, false],
        ['Replica Lag', stats.replica_lag, false],
        ['Uptime', stats.uptime, false],
        ['Min Replicas', stats.min_replicas, false]
      ];

      var html = '<div class="metrics-grid">';
      for (var i = 0; i < items.length; i++) {
        var val = typeof items[i][1] === 'number' ? fmt(items[i][1]) : items[i][1];
        var hl = items[i][2] ? ' highlight' : '';
        html += '<div class="metric' + hl + '"><div class="label">' + escapeHtml(items[i][0]) +
          '</div><div class="value">' + escapeHtml(val) + '</div></div>';
      }
      html += '</div>';
      els.dbStats.innerHTML = html;
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
      renderMetrics(els.dbMetrics, dbSamples, function (s) {
        return s.name.replace('turnstone_db_', '').replace(/_/g, ' ');
      });
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
    keyCursor = 0;
    selectedKey = '';
    clearEditor();
    refreshAll();
  }

  function debouncedSearch() {
    if (searchTimer) clearTimeout(searchTimer);
    searchTimer = setTimeout(function () { loadKeys(); }, 300);
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
    var panels = document.querySelectorAll('.view-panel');
    for (var i = 0; i < tabs.length; i++) {
      tabs[i].classList.toggle('active', tabs[i].getAttribute('data-view') === view);
    }
    for (var j = 0; j < panels.length; j++) {
      panels[j].classList.toggle('active', panels[j].getAttribute('data-view-panel') === view);
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
  els.prefixFilter.addEventListener('input', debouncedSearch);
  els.prefixFilter.addEventListener('keydown', function (ev) {
    if (ev.key === 'Enter') loadKeys();
  });
  els.editValue.addEventListener('input', updateValueSize);

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
