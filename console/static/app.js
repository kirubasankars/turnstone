(function () {
  'use strict';

  var currentDB = '0';
  var refreshTimer = null;
  var sparkHistory = { replica_lag: [], log_bytes: [], key_count: [] };
  var SPARK_MAX = 24;
  var seriesHistory = {};
  var CHART_MAX = 72;
  var CHART_H = 140;

  var els = {
    dbSelect: document.getElementById('db-select'),
    keyCount: document.getElementById('key-count'),
    overviewKeys: document.getElementById('overview-keys'),
    overviewTxs: document.getElementById('overview-txs'),
    overviewState: document.getElementById('overview-state'),
    overviewUpdated: document.getElementById('overview-updated'),
    dbStats: document.getElementById('db-stats'),
    gcStats: document.getElementById('gc-stats'),
    serverMetrics: document.getElementById('server-metrics'),
    dbMetrics: document.getElementById('db-metrics'),
    dbMetricsProm: document.getElementById('db-metrics-prom'),
    metricsDot: document.getElementById('metrics-dot'),
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

  function compact(n, suffix) {
    var s = n.toFixed(1);
    if (s.charAt(s.length - 1) === '0' && s.charAt(s.length - 2) === '.') {
      s = s.slice(0, -2);
    }
    return s + suffix;
  }

  function fmtBytes(n) {
    if (typeof n !== 'number' || !isFinite(n)) return String(n);
    var abs = Math.abs(n);
    if (abs >= 1e9) return compact(n / 1e9, ' GB');
    if (abs >= 1e6) return compact(n / 1e6, ' MB');
    if (abs >= 1e3) return compact(n / 1e3, ' KB');
    return String(n);
  }

  function fmtCount(n) {
    if (typeof n !== 'number' || !isFinite(n)) return String(n);
    var abs = Math.abs(n);
    if (abs >= 1e9) return compact(n / 1e9, 'B');
    if (abs >= 1e6) return compact(n / 1e6, 'M');
    if (abs >= 1e4) return compact(n / 1e3, 'K');
    return String(Math.round(n));
  }

  function metricText(label, name) {
    return (String(name || '') + ' ' + String(label || '')).toLowerCase().replace(/_/g, ' ');
  }

  function isTimestampMetric(label, name) {
    return /timestamp/.test(metricText(label, name));
  }

  function isByteMetric(label, name) {
    var s = metricText(label, name);
    if (/\bkeys?\b|\bkey count\b|\bshards?\b|\bsegments\b|\bcompacted\b|\breplicas\b|\btimestamp\b/.test(s)) return false;
    return /bytes|\blog size\b|\blog allocated\b|\boffset\b|\breplica lag\b|\blag\b|\barena\b|\blive\b|\bgarbage\b|\breclaimed\b/.test(s);
  }

  function fmt(n, label, name) {
    if (typeof n !== 'number') return String(n);
    if (isTimestampMetric(label, name)) {
      return String(Math.round(n));
    }
    return isByteMetric(label, name) ? fmtBytes(n) : fmtCount(n);
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
    ctx.strokeStyle = '#fbbf24';
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

  function seriesKey(sample) {
    var labels = sample.labels || {};
    var keys = Object.keys(labels).sort();
    var parts = [sample.name];
    for (var i = 0; i < keys.length; i++) {
      parts.push(keys[i] + '=' + labels[keys[i]]);
    }
    return parts.join('|');
  }

  function recordSeries(sample) {
    var key = seriesKey(sample);
    if (!seriesHistory[key]) seriesHistory[key] = [];
    seriesHistory[key].push(sample.value);
    if (seriesHistory[key].length > CHART_MAX) seriesHistory[key].shift();
  }

  function chartColor(label) {
    if (/active|tx|conflict|compact/i.test(label)) return '#f59e0b';
    if (/lag/i.test(label)) return '#fbbf24';
    return '#2dd4bf';
  }

  function drawLineChart(canvas, values, color, label, name) {
    if (!canvas || !values || values.length === 0) return;
    var pts = values.length === 1 ? [values[0], values[0]] : values;
    var cssW = canvas.clientWidth || canvas.parentElement && canvas.parentElement.clientWidth || 0;
    if (cssW < 40) return;
    var dpr = window.devicePixelRatio || 1;
    var padR = 8;
    var padT = 10;
    var padB = 8;
    canvas.width = Math.max(1, Math.floor(cssW * dpr));
    canvas.height = Math.floor(CHART_H * dpr);
    canvas.style.width = cssW + 'px';
    canvas.style.height = CHART_H + 'px';
    var ctx = canvas.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, cssW, CHART_H);

    var max = -Infinity;
    var min = Infinity;
    var i;
    for (i = 0; i < pts.length; i++) {
      if (pts[i] > max) max = pts[i];
      if (pts[i] < min) min = pts[i];
    }
    if (!isFinite(max)) return;

    var asBytes = isByteMetric(label, name);
    if (max === min) {
      if (max === 0) {
        max = 1;
      } else if (asBytes) {
        min = min * 0.95;
        max = max * 1.05;
      } else {
        min = Math.max(0, min - 1);
        max = max + 1;
      }
    } else if (!asBytes) {
      min = Math.floor(min);
      max = Math.ceil(max);
      if (min === max) max = min + 1;
    }

    ctx.font = '10px ui-monospace, SFMono-Regular, Menlo, monospace';
    var maxLabel = fmt(max, label, name);
    var minLabel = fmt(min, label, name);
    var padL = Math.ceil(Math.max(ctx.measureText(maxLabel).width, ctx.measureText(minLabel).width)) + 10;
    if (padL < 28) padL = 28;
    if (padL > cssW * 0.42) padL = Math.floor(cssW * 0.42);

    var plotW = cssW - padL - padR;
    var plotH = CHART_H - padT - padB;
    if (plotW < 8) return;
    ctx.strokeStyle = 'rgba(148, 163, 184, 0.16)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    for (var g = 0; g < 3; g++) {
      var gy = padT + (plotH * g) / 2;
      ctx.moveTo(padL, gy);
      ctx.lineTo(cssW - padR, gy);
    }
    ctx.stroke();

    ctx.fillStyle = '#8b98a8';
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    ctx.fillText(maxLabel, padL - 6, padT);
    ctx.fillText(minLabel, padL - 6, padT + plotH);
    ctx.textAlign = 'left';

    ctx.beginPath();
    for (var j = 0; j < pts.length; j++) {
      var x = padL + (j / (pts.length - 1)) * plotW;
      var y = padT + plotH - ((pts[j] - min) / (max - min)) * plotH;
      if (j === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.strokeStyle = color || '#2dd4bf';
    ctx.lineWidth = 1.5;
    ctx.lineJoin = 'miter';
    ctx.lineCap = 'square';
    ctx.stroke();
  }

  function metricGroupSlug(title) {
    return String(title || '').toLowerCase().replace(/[^a-z0-9]+/g, '-');
  }

  function renderChartCards(samples, labelFn) {
    var html = '<div class="chart-grid">';
    for (var i = 0; i < samples.length; i++) {
      var s = samples[i];
      var label = labelFn(s);
      var color = chartColor(label);
      html += '<div class="chart-card">' +
        '<div class="chart-head">' +
        '<span class="chart-label">' + escapeHtml(label) + '</span>' +
        '<span class="chart-value">' + escapeHtml(fmt(s.value, label, s.name)) + '</span>' +
        '</div>' +
        '<canvas class="metric-chart" data-series="' + escapeHtml(seriesKey(s)) +
        '" data-color="' + color +
        '" data-label="' + escapeHtml(label) +
        '" data-name="' + escapeHtml(s.name) +
        '" height="' + CHART_H + '"></canvas>' +
        '</div>';
    }
    html += '</div>';
    return html;
  }

  function renderMetricCharts(container, groups) {
    var hasAny = false;
    var html = '<div class="chart-groups">';
    for (var g = 0; g < groups.length; g++) {
      var group = groups[g];
      var nested = group.groups;
      var body = '';
      var n;
      if (nested && nested.length) {
        var inner = '';
        for (n = 0; n < nested.length; n++) {
          var nestedSamples = nested[n].samples || [];
          if (!nestedSamples.length) continue;
          inner += '<div class="chart-subgroup" data-testid="metrics-group-' +
            escapeHtml(metricGroupSlug(nested[n].title)) + '">' +
            '<h4 class="chart-subgroup-label">' + escapeHtml(nested[n].title) + '</h4>' +
            renderChartCards(nestedSamples, nested[n].labelFn) +
            '</div>';
        }
        if (!inner) continue;
        body = '<div class="chart-subgroups">' + inner + '</div>';
      } else {
        var samples = group.samples || [];
        if (!samples.length) continue;
        body = renderChartCards(samples, group.labelFn);
      }
      hasAny = true;
      html += '<div class="dash-group" data-testid="metrics-group-' +
        escapeHtml(metricGroupSlug(group.title)) + '"><h3 class="dash-group-label">' +
        escapeHtml(group.title) + '</h3>' + body + '</div>';
    }
    html += '</div>';
    if (!hasAny) {
      container.innerHTML = '<p class="empty-state"><span class="empty-title">No metrics available</span></p>';
      return;
    }
    container.innerHTML = html;
    redrawMetricCharts();
  }

  function redrawMetricCharts() {
    if (!els.dbMetricsProm) return;
    var canvases = els.dbMetricsProm.querySelectorAll('canvas.metric-chart');
    for (var i = 0; i < canvases.length; i++) {
      var c = canvases[i];
      var key = c.getAttribute('data-series');
      var color = c.getAttribute('data-color');
      drawLineChart(c, seriesHistory[key] || [], color, c.getAttribute('data-label'), c.getAttribute('data-name'));
    }
  }

  function setLoading(btn, loading) {
    if (!btn) return;
    btn.disabled = loading;
    btn.classList.toggle('loading', loading);
  }

  function updateLastUpdated() {
    var now = new Date();
    var timeStr = now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    if (els.overviewUpdated) els.overviewUpdated.textContent = timeStr;
  }

  function updateKeyCount(count) {
    var n = typeof count === 'number' ? count : 0;
    var label = n === 1 ? '1 key' : fmtCount(n) + ' keys';
    els.keyCount.textContent = label;
    if (els.overviewKeys) els.overviewKeys.textContent = fmtCount(n);
    pushSpark('key_count', n);
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
        '</div><div class="value' + valueCls + '">' + escapeHtml(fmt(s.value, label, s.name)) + '</div>' +
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
    if (els.overviewState) els.overviewState.innerHTML = renderStateBadge(state);
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

  function renderMetricTiles(items) {
    var html = '<div class="metrics-grid">';
    for (var i = 0; i < items.length; i++) {
      var val = typeof items[i][1] === 'number' ? fmt(items[i][1], items[i][0]) : items[i][1];
      var hl = items[i][2] ? ' highlight' : '';
      html += '<div class="metric' + hl + '"><div class="label">' +
        '<span class="metric-icon teal"></span>' + escapeHtml(items[i][0]) +
        '</div><div class="value">' + escapeHtml(val) + '</div></div>';
    }
    html += '</div>';
    return html;
  }

  function fmtAgo(unix) {
    var sec = Math.max(0, Math.floor(Date.now() / 1000 - unix));
    if (sec < 60) return sec + 's ago';
    if (sec < 3600) return Math.floor(sec / 60) + 'm ago';
    if (sec < 86400) return Math.floor(sec / 3600) + 'h ago';
    return Math.floor(sec / 86400) + 'd ago';
  }

  function renderGcStats(stats) {
    if (!els.gcStats) return;
    var arena = stats.index_arena_bytes || 0;
    var indexLive = stats.index_live_bytes || 0;
    var indexGarbage = arena > indexLive ? arena - indexLive : 0;
    var lastCompact = stats.hash_compact_unix ? fmtAgo(stats.hash_compact_unix) : 'never';
    els.gcStats.innerHTML = renderMetricTiles([
      ['WAL segments', stats.wal_segments || 0, false],
      ['Hash segments', stats.hash_shards || 0, false],
      ['Hash compacted', stats.hash_shards_compacted || 0, true],
      ['Hash reclaimed', stats.hash_compact_bytes_reclaimed || 0, false],
      ['Last compact', lastCompact, false],
      ['Index arena', arena, false],
      ['Index allocated', stats.index_allocated_bytes || 0, false],
      ['Index live', indexLive, true],
      ['WAL live', stats.wal_live_bytes || 0, true],
      ['WAL garbage', stats.wal_garbage_bytes || 0, false],
      ['Index garbage', indexGarbage, false]
    ]);
  }

  function loadStats() {
    return api('/api/databases/' + currentDB + '/stats').then(function (stats) {
      updateStateUI(stats.state);
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
        var val = typeof items[i][1] === 'number' ? fmt(items[i][1], items[i][0]) : items[i][1];
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
      renderGcStats(stats);

      var sparks = els.dbStats.querySelectorAll('.db-spark');
      for (var s = 0; s < sparks.length; s++) {
        var key = sparks[s].getAttribute('data-spark');
        drawSparkline(sparks[s], sparkHistory[key]);
      }

      updateLastUpdated();
    });
  }

  function dbMetricGroupTitle(name) {
    var n = String(name || '').replace(/^turnstone_db_/, '');
    if (/^hash_/.test(n) || /^index_/.test(n)) return 'Hash index';
    if (/^log_/.test(n) || /^wal_/.test(n) || n === 'offset' || n === 'replica_lag') return 'WAL';
    return 'Overview';
  }

  var dbChartRank = {
    turnstone_db_connections: 0,
    turnstone_db_active_txs: 1,
    turnstone_db_conflicts_total: 2,
    turnstone_db_key_count: 3,
    turnstone_db_replicas: 4,
    turnstone_db_log_bytes: 0,
    turnstone_db_log_allocated_bytes: 1,
    turnstone_db_offset: 2,
    turnstone_db_wal_segments: 3,
    turnstone_db_replica_lag: 4,
    turnstone_db_hash_shards: 0,
    turnstone_db_index_live_bytes: 1,
    turnstone_db_index_arena_bytes: 2,
    turnstone_db_index_allocated_bytes: 3,
    turnstone_db_hash_shards_compacted_total: 4,
    turnstone_db_index_compact_bytes_reclaimed_total: 5,
    turnstone_db_hash_compact_last_timestamp_seconds: 6
  };

  function groupDbMetricCharts(samples, labelFn) {
    var order = ['Overview', 'WAL', 'Hash index'];
    var buckets = {};
    var i;
    for (i = 0; i < order.length; i++) buckets[order[i]] = [];
    for (i = 0; i < samples.length; i++) {
      var title = dbMetricGroupTitle(samples[i].name);
      if (!buckets[title]) buckets[title] = [];
      buckets[title].push(samples[i]);
    }
    var groups = [];
    for (i = 0; i < order.length; i++) {
      buckets[order[i]].sort(function (a, b) {
        var ra = dbChartRank[a.name];
        var rb = dbChartRank[b.name];
        if (ra == null) ra = 50;
        if (rb == null) rb = 50;
        if (ra !== rb) return ra - rb;
        return String(a.name).localeCompare(b.name);
      });
      groups.push({ title: order[i], samples: buckets[order[i]], labelFn: labelFn });
    }
    return groups;
  }

  function loadMetrics() {
    return api('/api/metrics').then(function (data) {
      var serverSamples = data.server || [];
      var dbSamples = (data.db || []).filter(function (s) {
        return s.labels && s.labels.db === currentDB;
      });
      var i;
      for (i = 0; i < serverSamples.length; i++) recordSeries(serverSamples[i]);
      for (i = 0; i < dbSamples.length; i++) recordSeries(dbSamples[i]);

      renderMetrics(els.serverMetrics, serverSamples, function (s) {
        return s.name.replace('turnstone_server_', '').replace(/_/g, ' ');
      });
      var sparkMap = { 'key count': 'key_count', 'log bytes': 'log_bytes', 'replica lag': 'replica_lag' };
      var dbLabel = function (s) {
        return s.name.replace('turnstone_db_', '').replace(/_/g, ' ');
      };
      renderMetrics(els.dbMetrics, dbSamples, dbLabel, [], sparkMap);
      if (els.dbMetricsProm) {
        renderMetricCharts(els.dbMetricsProm, [
          {
            title: 'Server',
            samples: serverSamples,
            labelFn: function (s) {
              return s.name.replace('turnstone_server_', '').replace(/_/g, ' ');
            }
          },
          {
            title: 'Database',
            groups: groupDbMetricCharts(dbSamples, dbLabel)
          }
        ]);
      }
      els.metricsDot.className = 'dot ok';
    }).catch(function (e) {
      var err = '<p class="empty-state err"><span class="empty-title">' +
        escapeHtml(e.message) + '</span></p>';
      els.serverMetrics.innerHTML = err;
      if (els.dbMetricsProm) els.dbMetricsProm.innerHTML = err;
      els.metricsDot.className = 'dot err';
    });
  }

  function refreshAll() {
    setLoading(els.btnRefresh, true);
    Promise.all([loadStats(), loadMetrics()])
      .finally(function () { setLoading(els.btnRefresh, false); });
  }

  function onDbChange() {
    currentDB = els.dbSelect.value;
    sparkHistory = { replica_lag: [], log_bytes: [], key_count: [] };
    seriesHistory = {};
    refreshAll();
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
      panels[j].classList.toggle('active', panels[j].getAttribute('data-view-panel') === view);
    }
    if (view === 'prometheus') {
      requestAnimationFrame(redrawMetricCharts);
    }
  }

  document.getElementById('btn-refresh').addEventListener('click', refreshAll);
  els.dbSelect.addEventListener('change', onDbChange);

  document.querySelectorAll('.view-tab').forEach(function (tab) {
    tab.addEventListener('click', function () {
      switchView(tab.getAttribute('data-view'));
    });
  });

  document.addEventListener('keydown', function (ev) {
    var mod = ev.ctrlKey || ev.metaKey;
    if (ev.key === 'r' && !mod && document.activeElement.tagName !== 'INPUT' &&
        document.activeElement.tagName !== 'TEXTAREA' &&
        document.activeElement.tagName !== 'SELECT') {
      ev.preventDefault();
      refreshAll();
    }
  });

  window.addEventListener('resize', function () {
    requestAnimationFrame(redrawMetricCharts);
  });
  loadDatabases().then(refreshAll).then(scheduleRefresh);
})();
