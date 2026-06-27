// Lightweight control dashboard. No framework — just fetch + EventSource.
// The base is empty so it works whether you open the dashboard on this server
// or proxy it; all paths are root-relative.

const $ = (id) => document.getElementById(id);

async function api(path, opts) {
  const res = await fetch(path, opts);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

// ---- status polling -------------------------------------------------------

let topicsLoaded = false;

async function refreshStatus() {
  try {
    const s = await api('/api/sources');
    $('conn').textContent = 'connected';
    $('conn').classList.remove('err');

    // Time series
    setBadge('ts-badge', s.timeSeries.running);
    $('ts-series').textContent = s.timeSeries.seriesCount.toLocaleString();
    $('ts-produced').textContent = s.timeSeries.produced.toLocaleString();
    $('ts-res').textContent = s.timeSeries.resolution;

    // Stream
    setBadge('stream-badge', s.stream.running);
    $('stream-topics').textContent = s.stream.topics;
    $('stream-partitions').textContent = s.stream.partitions;
    $('stream-produced').textContent = s.stream.produced.toLocaleString();
    if (!$('stream-pps').dataset.touched) $('stream-pps').value = s.stream.pps;

    if (!topicsLoaded) loadTopics();
  } catch (e) {
    $('conn').textContent = 'disconnected — is the server running?';
    $('conn').classList.add('err');
  }
}

function setBadge(id, on) {
  const el = $(id);
  el.textContent = on ? 'running' : 'off';
  el.className = 'badge ' + (on ? 'on' : 'off');
}

async function loadTopics() {
  try {
    const meta = await api('/api/stream/topics');
    const sel = $('live-topic');
    sel.innerHTML = '';
    for (const t of meta.topics) {
      const opt = document.createElement('option');
      opt.value = t.topic;
      opt.textContent = t.topic;
      sel.appendChild(opt);
    }
    topicsLoaded = true;
  } catch { /* stream metadata not ready yet */ }
}

// ---- start / stop buttons -------------------------------------------------

document.querySelectorAll('button[data-source]').forEach((btn) => {
  btn.addEventListener('click', async () => {
    const source = btn.dataset.source;
    const action = btn.dataset.action;
    const body = {};
    if (source === 'stream' && action === 'start') {
      body.pps = Number($('stream-pps').value) || undefined;
    }
    try {
      await api(`/api/sources/${source}/${action}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      refreshStatus();
    } catch (e) {
      alert(`Failed to ${action} ${source}: ${e.message}`);
    }
  });
});

$('stream-pps').addEventListener('input', (e) => { e.target.dataset.touched = '1'; });

$('reset-all').addEventListener('click', async () => {
  try {
    await api('/api/sources/reset', { method: 'POST' });
    refreshStatus();
  } catch (e) {
    alert(`Failed to reset: ${e.message}`);
  }
});

// ---- live SSE tail --------------------------------------------------------

let es = null;
let recvCount = 0;
let rateTimer = null;

$('live-toggle').addEventListener('click', () => {
  if (es) { stopTail(); } else { startTail(); }
});

function startTail() {
  const topic = $('live-topic').value;
  if (!topic) { alert('No topics yet — start the stream first.'); return; }
  const log = $('live-log');
  log.textContent = '';
  recvCount = 0;

  // offset=latest: only show new records from now on.
  es = new EventSource(`/stream/sse?topic=${encodeURIComponent(topic)}&offset=latest`);
  $('live-toggle').textContent = 'Stop tail';

  es.addEventListener('records', (ev) => {
    const msg = JSON.parse(ev.data);
    recvCount += msg.count;
    for (const r of msg.records.slice(-12)) {
      appendLine(`p${r.partition}@${r.offset}  ${r.key}  ${r.metric}=${r.value}${r.unit}`);
    }
  });
  es.onerror = () => { $('live-rate').textContent = 'reconnecting…'; };

  let last = 0;
  rateTimer = setInterval(() => {
    $('live-rate').textContent = `${recvCount - last} rec/s · ${recvCount.toLocaleString()} total`;
    last = recvCount;
  }, 1000);
}

function stopTail() {
  if (es) { es.close(); es = null; }
  if (rateTimer) { clearInterval(rateTimer); rateTimer = null; }
  $('live-toggle').textContent = 'Tail via SSE';
  $('live-rate').textContent = 'stopped';
}

function appendLine(line) {
  const log = $('live-log');
  log.textContent += line + '\n';
  // Keep the log bounded so a long tail doesn't grow without limit.
  const lines = log.textContent.split('\n');
  if (lines.length > 300) log.textContent = lines.slice(-300).join('\n');
  log.scrollTop = log.scrollHeight;
}

// ---- dump links -----------------------------------------------------------

function dumpURL(download) {
  const count = Number($('dump-count').value) || 1;
  const format = $('dump-format').value;
  return `/api/dump?count=${count}&format=${format}${download ? '&download=1' : ''}`;
}
function refreshDumpLinks() {
  $('dump-download').href = dumpURL(true);
  $('dump-open').href = dumpURL(false);
}
$('dump-count').addEventListener('input', refreshDumpLinks);
$('dump-format').addEventListener('change', refreshDumpLinks);

// ---- boot -----------------------------------------------------------------

refreshDumpLinks();
refreshStatus();
setInterval(refreshStatus, 2000);
