// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package devtool

var indexHTML = []byte(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Turnstone Devtool</title>
<style>
  :root {
    --bg: #0f1419;
    --panel: #1a2332;
    --border: #2d3a4f;
    --text: #e6edf3;
    --muted: #8b9cb3;
    --accent: #3d8bfd;
    --accent-dim: #2563b8;
    --ok: #3fb950;
    --warn: #d29922;
    --err: #f85149;
    --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    --sans: system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: var(--sans); background: var(--bg); color: var(--text); min-height: 100vh; }
  header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 12px 20px; border-bottom: 1px solid var(--border); background: var(--panel);
  }
  header h1 { font-size: 1.1rem; font-weight: 600; letter-spacing: -0.02em; }
  header h1 span { color: var(--accent); }
  .toolbar { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
  select, input, button, textarea {
    font: inherit; border: 1px solid var(--border); border-radius: 6px;
    background: var(--bg); color: var(--text); padding: 6px 10px;
  }
  button {
    background: var(--accent); border-color: var(--accent-dim); color: #fff;
    cursor: pointer; font-weight: 500;
  }
  button:hover { background: var(--accent-dim); }
  button.secondary { background: var(--panel); color: var(--text); }
  button.danger { background: #8b2635; border-color: #6e1f2a; }
  main { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; padding: 16px; max-width: 1400px; margin: 0 auto; }
  @media (max-width: 900px) { main { grid-template-columns: 1fr; } }
  .card {
    background: var(--panel); border: 1px solid var(--border); border-radius: 10px;
    overflow: hidden;
  }
  .card.full { grid-column: 1 / -1; }
  .card-header {
    padding: 10px 14px; border-bottom: 1px solid var(--border);
    font-size: 0.85rem; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em;
    display: flex; justify-content: space-between; align-items: center;
  }
  .card-body { padding: 14px; }
  .metrics-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr)); gap: 10px; }
  .metric {
    background: var(--bg); border: 1px solid var(--border); border-radius: 8px; padding: 10px;
  }
  .metric .label { font-size: 0.72rem; color: var(--muted); margin-bottom: 4px; word-break: break-all; }
  .metric .value { font-size: 1.25rem; font-weight: 600; font-family: var(--mono); }
  .metric .value.ok { color: var(--ok); }
  .metric .value.warn { color: var(--warn); }
  .state-badge {
    display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.75rem;
    font-weight: 600; text-transform: uppercase;
  }
  .state-PRIMARY { background: #1a3d2e; color: var(--ok); }
  .state-REPLICA { background: #3d2e1a; color: var(--warn); }
  .state-UNDEFINED { background: #2d2d2d; color: var(--muted); }
  .key-list { max-height: 280px; overflow-y: auto; border: 1px solid var(--border); border-radius: 6px; }
  .key-item {
    padding: 8px 12px; border-bottom: 1px solid var(--border); cursor: pointer;
    font-family: var(--mono); font-size: 0.85rem;
  }
  .key-item:hover { background: var(--bg); }
  .key-item.selected { background: #1e3a5f; border-left: 3px solid var(--accent); }
  .key-item:last-child { border-bottom: none; }
  .editor { display: flex; flex-direction: column; gap: 10px; }
  .editor label { font-size: 0.8rem; color: var(--muted); }
  textarea { min-height: 120px; font-family: var(--mono); font-size: 0.85rem; resize: vertical; width: 100%; }
  .actions { display: flex; gap: 8px; flex-wrap: wrap; }
  .status { font-size: 0.8rem; color: var(--muted); margin-top: 8px; min-height: 1.2em; }
  .status.err { color: var(--err); }
  .status.ok { color: var(--ok); }
  .empty { color: var(--muted); font-size: 0.85rem; padding: 20px; text-align: center; }
  .refresh-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--ok); display: inline-block; margin-right: 6px; }
</style>
</head>
<body>
<header>
  <h1><span>Turnstone</span> Devtool</h1>
  <div class="toolbar">
    <label>DB <select id="db-select"></select></label>
    <button class="secondary" onclick="refreshAll()">Refresh</button>
  </div>
</header>
<main>
  <section class="card">
    <div class="card-header">
      <span>Database Stats</span>
      <span id="db-state"></span>
    </div>
    <div class="card-body" id="db-stats"><div class="empty">Select a database</div></div>
  </section>
  <section class="card">
    <div class="card-header">
      <span>Server Metrics</span>
      <span><span class="refresh-dot" id="metrics-dot"></span>Prometheus</span>
    </div>
    <div class="card-body" id="server-metrics"><div class="empty">Loading...</div></div>
  </section>
  <section class="card">
    <div class="card-header"><span>Key Browser</span></div>
    <div class="card-body">
      <div class="toolbar" style="margin-bottom:10px">
        <input id="prefix-filter" placeholder="Prefix filter" style="flex:1;min-width:120px">
        <button class="secondary" onclick="loadKeys()">Search</button>
        <button class="secondary" onclick="loadMoreKeys()">More</button>
      </div>
      <div class="key-list" id="key-list"><div class="empty">No keys loaded</div></div>
    </div>
  </section>
  <section class="card">
    <div class="card-header"><span>Key Editor</span></div>
    <div class="card-body editor">
      <div>
        <label>Key</label>
        <input id="edit-key" style="width:100%;margin-top:4px" placeholder="key name">
      </div>
      <div>
        <label>Value</label>
        <textarea id="edit-value" placeholder="value"></textarea>
      </div>
      <div class="actions">
        <button onclick="getKey()">Get</button>
        <button onclick="setKey()">Set</button>
        <button class="danger" onclick="delKey()">Delete</button>
      </div>
      <div class="status" id="editor-status"></div>
    </div>
  </section>
  <section class="card full">
    <div class="card-header"><span>Per-DB Prometheus Metrics</span></div>
    <div class="card-body" id="db-metrics"><div class="empty">Loading...</div></div>
  </section>
</main>
<script>
let currentDB = '0';
let keyCursor = 0;
let hasMoreKeys = false;
let selectedKey = '';

async function api(path, opts) {
  const res = await fetch(path, opts);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }));
    throw new Error(err.error || res.statusText);
  }
  if (res.status === 204) return null;
  return res.json();
}

function fmt(n) {
  if (n >= 1e9) return (n/1e9).toFixed(1) + 'G';
  if (n >= 1e6) return (n/1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n/1e3).toFixed(1) + 'K';
  return String(n);
}

function renderMetrics(container, samples, labelFn) {
  if (!samples || samples.length === 0) {
    container.innerHTML = '<div class="empty">No metrics available</div>';
    return;
  }
  container.innerHTML = '<div class="metrics-grid">' + samples.map(s => {
    const label = labelFn ? labelFn(s) : s.name.replace('turnstone_', '').replace(/_/g, ' ');
    const cls = s.name.includes('conflict') && s.value > 0 ? 'warn' : (s.name.includes('lag') && s.value > 0 ? 'warn' : '');
    return '<div class="metric"><div class="label">' + label + '</div><div class="value ' + cls + '">' + fmt(s.value) + '</div></div>';
  }).join('') + '</div>';
}

async function loadDatabases() {
  const data = await api('/api/databases');
  const sel = document.getElementById('db-select');
  sel.innerHTML = data.databases.map(d => '<option value="' + d + '">' + d + '</option>').join('');
  if (data.databases.length > 0) {
    currentDB = data.databases[0];
    sel.value = currentDB;
  }
  sel.onchange = () => { currentDB = sel.value; keyCursor = 0; refreshAll(); };
}

async function loadStats() {
  const stats = await api('/api/databases/' + currentDB + '/stats');
  const stateEl = document.getElementById('db-state');
  const stateClass = stats.state.split(' ')[0];
  stateEl.innerHTML = '<span class="state-badge state-' + stateClass + '">' + stats.state + '</span>';
  const items = [
    ['Keys', stats.key_count],
    ['Active Txs', stats.active_txs],
    ['Connections', stats.active_connections],
    ['Conflicts', stats.conflicts],
    ['Log Bytes', stats.log_bytes],
    ['Log Allocated', stats.log_allocated_bytes],
    ['Replica Lag', stats.replica_lag],
    ['Uptime', stats.uptime],
    ['Min Replicas', stats.min_replicas],
  ];
  document.getElementById('db-stats').innerHTML = '<div class="metrics-grid">' +
    items.map(([l,v]) => '<div class="metric"><div class="label">' + l + '</div><div class="value">' + (typeof v === 'number' ? fmt(v) : v) + '</div></div>').join('') +
    '</div>';
}

async function loadMetrics() {
  try {
    const data = await api('/api/metrics');
    renderMetrics(document.getElementById('server-metrics'), data.server, s => s.name.replace('turnstone_server_', '').replace(/_/g, ' '));
    renderMetrics(document.getElementById('db-metrics'), data.db.filter(s => s.labels && s.labels.db === currentDB), s => {
      return s.name.replace('turnstone_db_', '').replace(/_/g, ' ');
    });
    document.getElementById('metrics-dot').style.background = 'var(--ok)';
  } catch (e) {
    document.getElementById('server-metrics').innerHTML = '<div class="empty err">' + e.message + '</div>';
    document.getElementById('metrics-dot').style.background = 'var(--err)';
  }
}

async function loadKeys(append) {
  const prefix = document.getElementById('prefix-filter').value;
  if (!append) keyCursor = 0;
  const data = await api('/api/databases/' + currentDB + '/keys?prefix=' + encodeURIComponent(prefix) + '&cursor=' + keyCursor + '&limit=100');
  keyCursor = data.next_cursor;
  hasMoreKeys = data.has_more;
  const list = document.getElementById('key-list');
  if (!append || !list.querySelector('.key-item')) {
    list.innerHTML = '';
  }
  if (data.keys.length === 0 && keyCursor === 0) {
    list.innerHTML = '<div class="empty">No keys found</div>';
    return;
  }
  if (list.querySelector('.empty')) list.innerHTML = '';
  data.keys.forEach(k => {
    const el = document.createElement('div');
    el.className = 'key-item' + (k === selectedKey ? ' selected' : '');
    el.textContent = k;
    el.onclick = () => selectKey(k);
    list.appendChild(el);
  });
}

function loadMoreKeys() {
  if (hasMoreKeys) loadKeys(true);
}

function selectKey(k) {
  selectedKey = k;
  document.getElementById('edit-key').value = k;
  document.querySelectorAll('.key-item').forEach(el => {
    el.classList.toggle('selected', el.textContent === k);
  });
  getKey();
}

function setStatus(msg, isErr) {
  const el = document.getElementById('editor-status');
  el.textContent = msg;
  el.className = 'status' + (isErr ? ' err' : msg ? ' ok' : '');
}

async function getKey() {
  const key = document.getElementById('edit-key').value;
  if (!key) { setStatus('Enter a key', true); return; }
  try {
    const data = await api('/api/databases/' + currentDB + '/keys/' + encodeURIComponent(key));
    document.getElementById('edit-value').value = data.value;
    setStatus('Loaded');
  } catch (e) { setStatus(e.message, true); }
}

async function setKey() {
  const key = document.getElementById('edit-key').value;
  const value = document.getElementById('edit-value').value;
  if (!key) { setStatus('Enter a key', true); return; }
  try {
    await api('/api/databases/' + currentDB + '/keys/' + encodeURIComponent(key), {
      method: 'PUT',
      headers: { 'Content-Type': 'text/plain' },
      body: value,
    });
    setStatus('Set OK');
    loadKeys();
    loadStats();
  } catch (e) { setStatus(e.message, true); }
}

async function delKey() {
  const key = document.getElementById('edit-key').value;
  if (!key) { setStatus('Enter a key', true); return; }
  if (!confirm('Delete key "' + key + '"?')) return;
  try {
    await api('/api/databases/' + currentDB + '/keys/' + encodeURIComponent(key), { method: 'DELETE' });
    document.getElementById('edit-value').value = '';
    setStatus('Deleted');
    loadKeys();
    loadStats();
  } catch (e) { setStatus(e.message, true); }
}

function refreshAll() {
  loadStats();
  loadMetrics();
  loadKeys();
}

loadDatabases().then(refreshAll);
setInterval(() => { loadStats(); loadMetrics(); }, 5000);
</script>
</body>
</html>`)
