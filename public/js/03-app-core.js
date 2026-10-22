// Mobile detection — used throughout for performance tuning
window._isMobile = /Mobi|Android|iPhone|iPad/i.test(navigator.userAgent);
// ============================================================
// RELIABILITY LAYER (RX)
// IndexedDB-backed outbox + retry worker + crash recovery
// Every critical write goes here FIRST, then drains to Firestore.
// If the page crashes, the outbox survives and replays.
// ============================================================
const RX = (function() {
  const DB_NAME = 'shop_rx_v1';
  const STORES = ['outbox','drafts','log','snapshots','keystore'];
  let _db = null;
  let _opening = null;
  let _draining = false;
  let _listeners = new Set();
  let _haltReason = null;

  function open() {
    if (_db) return Promise.resolve(_db);
    if (_opening) return _opening;
    _opening = new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, 1);
      req.onupgradeneeded = e => {
        const db = req.result;
        STORES.forEach(name => {
          if (!db.objectStoreNames.contains(name)) {
            const store = db.createObjectStore(name, { keyPath: 'id', autoIncrement: name === 'outbox' || name === 'log' });
            if (name === 'outbox') {
              store.createIndex('status_attempt', ['status','nextAttemptAt']);
              store.createIndex('createdAt', 'createdAt');
            }
          }
        });
      };
      req.onsuccess = () => { _db = req.result; resolve(_db); };
      req.onerror = () => reject(req.error);
    });
    return _opening;
  }

  function tx(storeName, mode = 'readonly') {
    return open().then(db => db.transaction(storeName, mode).objectStore(storeName));
  }

  function emit() { _listeners.forEach(fn => { try { fn(); } catch(e) { /* never let a listener crash the emitter */ } }); }

  // ---- LOG (system-level audit trail) ----
  async function log(level, source, message, meta) {
    try {
      const store = await tx('log','readwrite');
      store.add({ ts: Date.now(), level, source, message, meta: meta || null });
      // Trim if huge
      if (Math.random() < 0.01) {
        const all = await getAll('log');
        if (all.length > 5000) {
          const cleared = await tx('log','readwrite');
          cleared.clear();
          all.slice(-2000).forEach(row => cleared.add({ ts: row.ts, level: row.level, source: row.source, message: row.message, meta: row.meta }));
        }
      }
    } catch(e) { /* logging must never throw */ }
  }

  function getAll(storeName) {
    return open().then(db => new Promise((resolve, reject) => {
      const req = db.transaction(storeName).objectStore(storeName).getAll();
      req.onsuccess = () => resolve(req.result || []);
      req.onerror = () => reject(req.error);
    }));
  }

  function get(storeName, key) {
    return open().then(db => new Promise((resolve, reject) => {
      const req = db.transaction(storeName).objectStore(storeName).get(key);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    }));
  }

  function put(storeName, value) {
    return open().then(db => new Promise((resolve, reject) => {
      const req = db.transaction(storeName,'readwrite').objectStore(storeName).put(value);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    }));
  }

  function del(storeName, key) {
    return open().then(db => new Promise((resolve, reject) => {
      const req = db.transaction(storeName,'readwrite').objectStore(storeName).delete(key);
      req.onsuccess = () => resolve();
      req.onerror = () => reject(req.error);
    }));
  }

  // ---- OUTBOX ----
  // op: { type: 'append'|'delete', collection, recordId, payload }
  async function enqueue(op) {
    const job = {
      type: op.type, collection: op.collection,
      recordId: String(op.recordId), payload: op.payload || null,
      status: 'pending', attempts: 0, lastError: null,
      createdAt: Date.now(), nextAttemptAt: Date.now()
    };
    const id = await put('outbox', job);
    // Don't log every record during bulk ops — too noisy and slows IDB
    if (!window._bulkImportInProgress) {
      log('info','outbox','enqueued ' + op.type + ' ' + op.collection + '/' + op.recordId, { jobId: id });
      emit();
      drain(); // trigger drain immediately for single writes
    }
    // During bulk imports drain() runs once after all records are enqueued (see rxEnqueueChanged)
    return id;
  }

  async function listPending() {
    const all = await getAll('outbox');
    return all.filter(j => j.status === 'pending' || j.status === 'retry');
  }

  async function pendingCount() {
    return (await listPending()).length;
  }

  async function oldestPendingAgeMs() {
    const pend = await listPending();
    if (!pend.length) return 0;
    return Date.now() - Math.min.apply(null, pend.map(p => p.createdAt));
  }

  function backoffMs(attempts) {
    // 2s, 5s, 15s, 60s, 5min, then 5min
    const s = [2000, 5000, 15000, 60000, 300000];
    return s[Math.min(attempts, s.length - 1)];
  }

  async function drain() {
    if (_draining) return;
    if (!navigator.onLine) return;
    _draining = true;
    try {
      if (!window._firebaseAuthed) {
        log('warn','outbox','draining paused: not authenticated');
        return;
      }
      // Process all ready jobs — batch them into Firestore writeBatch calls (490 ops each).
      // This replaces sequential per-record round-trips: 500 records now takes ~100ms
      // instead of 500 × 200ms = 100 seconds.
      const pend = await listPending();
      const ready = pend.filter(j => j.nextAttemptAt <= Date.now())
                        .sort((a,b) => a.createdAt - b.createdAt);
      if (!ready.length) return;

      if (window.firebaseBatchWrite && window._firebaseAuthed) {
        // ── FAST PATH: batch all ready jobs in one or few Firestore commits ──
        try {
          await window.firebaseBatchWrite(ready.map(j => ({
            type: j.type, collection: j.collection,
            recordId: j.recordId, payload: j.payload
          })));
          // Mark all as done
          const now = Date.now();
          await Promise.all(ready.map(async job => {
            job.status = 'done'; job.completedAt = now;
            await put('outbox', job);
            setTimeout(() => del('outbox', job.id).catch(()=>{}), 60000);
          }));
          await put('keystore', { id: 'lastSyncAt', value: now });
          log('info','outbox','batch drained ' + ready.length + ' jobs');
        } catch(batchErr) {
          // Batch failed — fall through to slow sequential path so nothing is lost
          log('warn','outbox','batch failed, falling back to sequential: ' + batchErr.message);
          for (const job of ready) {
            if (!window._firebaseAuthed) break;
            try {
              if (job.type === 'append') {
                let safePayload = job.payload;
                if (job.collection === 'parts' && safePayload && safePayload.photoDataUrl && safePayload.photoDataUrl.startsWith('data:')) {
                  safePayload = Object.assign({}, safePayload, { photoDataUrl: '__pending_upload__' });
                }
                await window.firebaseAppendDoc(job.collection, job.recordId, safePayload);
              } else if (job.type === 'delete') {
                await window.firebaseDeleteDoc(job.collection, job.recordId);
              }
              job.status = 'done'; job.completedAt = Date.now();
              await put('outbox', job);
              setTimeout(() => del('outbox', job.id).catch(()=>{}), 60000);
            } catch(e) {
              job.attempts++; job.lastError = (e && e.message) || String(e);
              job.status = job.attempts >= 20 ? 'failed' : 'retry';
              job.nextAttemptAt = Date.now() + backoffMs(job.attempts);
              await put('outbox', job);
              if (job.status === 'failed') log('critical','outbox','permanently failed: ' + job.collection + '/' + job.recordId);
            }
          }
          await put('keystore', { id: 'lastSyncAt', value: Date.now() });
        }
      } else {
        // ── SLOW PATH (no batch function yet — fallback) ──
        for (const job of ready) {
          if (!window._firebaseAuthed) break;
          try {
            if (job.type === 'append') {
              let safePayload = job.payload;
              if (job.collection === 'parts' && safePayload && safePayload.photoDataUrl && safePayload.photoDataUrl.startsWith('data:')) {
                safePayload = Object.assign({}, safePayload, { photoDataUrl: '__pending_upload__' });
              }
              await window.firebaseAppendDoc(job.collection, job.recordId, safePayload);
            } else if (job.type === 'delete') {
              await window.firebaseDeleteDoc(job.collection, job.recordId);
            }
            job.status = 'done'; job.completedAt = Date.now();
            await put('outbox', job);
            setTimeout(() => del('outbox', job.id).catch(()=>{}), 60000);
          } catch(e) {
            job.attempts++; job.lastError = (e && e.message) || String(e);
            job.status = job.attempts >= 20 ? 'failed' : 'retry';
            job.nextAttemptAt = Date.now() + backoffMs(job.attempts);
            await put('outbox', job);
            if (job.status === 'failed') log('critical','outbox','permanently failed: ' + job.collection + '/' + job.recordId);
          }
        }
        await put('keystore', { id: 'lastSyncAt', value: Date.now() });
      }
    } finally {
      _draining = false;
      emit();
      checkHalt();
    }
  }

  // Periodic drain & retry kick — 1.5s so pending writes clear quickly on mobile
  setInterval(() => drain(), window._isMobile ? 4000 : 1500);
  window.addEventListener('online', () => {
    log('info','net','online — resuming drain');
    drain();
    if (window.drainPartsCloudPending) window.drainPartsCloudPending();
    if (window.drainPhotoUploadQueue) window.drainPhotoUploadQueue();
  });
  window.addEventListener('offline', () => log('warn','net','went offline'));

  // Boot-time self-heal: reset jobs stuck in 'retry'/'failed' due to payload-too-large errors.
  // These were caused by base64 photoDataUrl in parts records (>1MB Firestore limit).
  // drain() now strips base64 before writing — resetting these lets them succeed immediately.
  async function _healSizeErrors() {
    try {
      const all = await getAll('outbox');
      const stuck = all.filter(j =>
        (j.status === 'retry' || j.status === 'failed') &&
        j.lastError && j.lastError.includes('longer than 1048487')
      );
      for (const j of stuck) {
        j.status = 'pending';
        j.attempts = 0;
        j.lastError = null;
        j.nextAttemptAt = Date.now();
        await put('outbox', j);
        log('info','outbox','self-healed size-error job: ' + j.collection + '/' + j.recordId);
      }
      if (stuck.length) { emit(); drain(); }
    } catch(e) { /* non-fatal */ }
  }
  setTimeout(() => _healSizeErrors(), 2000);

  // ---- DRAFT (crash recovery for in-progress carts/forms) ----
  async function saveDraft(name, value) {
    await put('drafts', { id: name, value, ts: Date.now() });
  }
  async function loadDraft(name) {
    const r = await get('drafts', name);
    return r ? r.value : null;
  }
  async function clearDraft(name) { return del('drafts', name); }

  // ---- SNAPSHOTS (auto JSON backup) ----
  async function pushSnapshot(snap) {
    await put('snapshots', { id: 'snap_' + Date.now(), ts: Date.now(), snap });
    const all = await getAll('snapshots');
    if (all.length > 7) {
      // Keep last 7
      all.sort((a,b)=>a.ts-b.ts).slice(0, all.length-7).forEach(s => del('snapshots', s.id).catch(()=>{}));
    }
  }
  async function listSnapshots() { return (await getAll('snapshots')).sort((a,b)=>b.ts-a.ts); }

  // ---- HALT (stop-shop guard) ----
  async function checkHalt() {
    const pend = await listPending();
    // Only halt on truly permanently failed jobs (status === 'failed', set at attempts >= 20).
    // Do NOT halt on attempts >= 5 — that's just a normal retry in progress on a slow mobile
    // connection. Halting on early retries caused false "SHOP OPERATIONS HALTED" screens on
    // mobile even though data was saving fine on desktop.
    const failed = pend.filter(j => j.status === 'failed').length;
    const oldestAge = await oldestPendingAgeMs();
    let reason = null;
    if (failed > 0) reason = failed + ' write(s) permanently failed';
    else if (pend.length > 10 && oldestAge > 300000) reason = pend.length + ' writes pending for ' + Math.round(oldestAge/1000) + 's';
    else if (!navigator.onLine && pend.length > 0 && oldestAge > 60000) reason = 'offline with ' + pend.length + ' unsaved write(s)';
    setHalt(reason);
  }
  function setHalt(reason) {
    if (reason !== _haltReason) {
      _haltReason = reason;
      try { renderHaltBanner(); } catch(e) { /* UI render — non-critical */ }
      try { renderRxStatusBar(); } catch(e) { /* UI render — non-critical */ }
      if (reason && typeof notifyAgentOfHalt === 'function') notifyAgentOfHalt(reason);
    }
  }
  function getHalt() { return _haltReason; }

  // ---- LISTENERS ----
  function onChange(fn) { _listeners.add(fn); return () => _listeners.delete(fn); }

  // ---- KEYSTORE ----
  async function getKey(k) { const r = await get('keystore', k); return r ? r.value : null; }
  async function setKey(k, v) { await put('keystore', { id: k, value: v }); }

  return {
    open, log, getAll, get, put, del,
    enqueue, listPending, pendingCount, oldestPendingAgeMs, drain,
    saveDraft, loadDraft, clearDraft,
    pushSnapshot, listSnapshots,
    onChange, getHalt, setHalt, checkHalt,
    getKey, setKey
  };
})();
window.RX = RX;
RX.log('info','boot','reliability layer initialised');

// ============================================================
// CRYPTO HELPERS (PIN hashing, SHA-256)
// ============================================================
async function sha256Hex(s) {
  const buf = new TextEncoder().encode(s);
  const hash = await crypto.subtle.digest('SHA-256', buf);
  return Array.from(new Uint8Array(hash)).map(b => b.toString(16).padStart(2,'0')).join('');
}
async function hashPin(pin, salt) {
  return sha256Hex((salt || '') + ':' + pin);
}
function makeSalt() {
  const a = new Uint8Array(12);
  crypto.getRandomValues(a);
  return Array.from(a).map(b => b.toString(16).padStart(2,'0')).join('');
}

// ============================================================
// HALT BANNER (stop-shop guard)
// ============================================================
function renderHaltBanner() {
  let el = document.getElementById('rx-halt-banner');
  const reason = RX.getHalt();
  if (!reason) { if (el) el.remove(); return; }
  if (!el) {
    el = document.createElement('div');
    el.id = 'rx-halt-banner';
    el.style.cssText = 'position:fixed;inset:0;z-index:99998;background:rgba(220,38,38,.92);color:white;display:flex;align-items:center;justify-content:center;padding:24px;text-align:center;font-family:-apple-system,BlinkMacSystemFont,sans-serif;';
    document.body.appendChild(el);
  }
  el.innerHTML = ''
    + '<div style="max-width:480px;background:#7f1d1d;padding:24px;border-radius:16px;box-shadow:0 20px 60px rgba(0,0,0,.5)">'
    + '  <div style="font-size:48px;margin-bottom:12px">⛔</div>'
    + '  <h2 style="font-size:20px;font-weight:800;margin:0 0 8px">SHOP OPERATIONS HALTED</h2>'
    + '  <div style="font-size:14px;line-height:1.5;margin-bottom:16px;opacity:.95">Data is not saving correctly. The agent has stopped new sales/orders to protect your records.</div>'
    + '  <div style="background:rgba(0,0,0,.3);padding:10px;border-radius:8px;font-size:13px;margin-bottom:16px"><b>Reason:</b> ' + reason + '</div>'
    + '  <div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:center">'
    + '    <button onclick="RX.drain();renderRxStatusBar();" style="padding:10px 14px;background:#fbbf24;color:#78350f;border:none;border-radius:8px;font-weight:700;cursor:pointer">🔁 Retry Now</button>'
    + '    <button onclick="rxDownloadEmergencyBackup()" style="padding:10px 14px;background:white;color:#7f1d1d;border:none;border-radius:8px;font-weight:700;cursor:pointer">💾 Save Backup</button>'
    + '    <button onclick="rxOverrideHalt()" style="padding:10px 14px;background:transparent;color:white;border:1px solid rgba(255,255,255,.5);border-radius:8px;font-weight:600;cursor:pointer">Override (risky)</button>'
    + '  </div>'
    + '</div>';
}
function rxOverrideHalt() {
  if (!confirm('Override the halt? Sales recorded while data is failing may be lost.')) return;
  RX.setHalt(null);
  RX.log('warn','halt','user overrode halt');
}
async function rxDownloadEmergencyBackup() {
  // dump everything we have locally
  const dump = { ts: Date.now(), state: window.state, pending: await RX.listPending(), log: (await RX.getAll('log')).slice(-500) };
  const blob = new Blob([JSON.stringify(dump,null,2)], { type: 'application/json' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'shop-emergency-' + new Date().toISOString().replace(/[:.]/g,'-') + '.json';
  a.click();
  setTimeout(function(){ URL.revokeObjectURL(a.href); }, 5000); // free blob after download starts
  RX.log('info','backup','emergency backup downloaded');
}

// ============================================================
// RX STATUS BAR (visible header pill: pending count + last sync)
// ============================================================
async function renderRxStatusBar() {
  const el = document.getElementById('rx-status');
  if (!el) return;
  const pend = await RX.pendingCount();
  const partsPend = typeof partsPendingActiveCount === 'function' ? partsPendingActiveCount() : 0;
  const partsDead = typeof partsPendingFailedCount === 'function' ? partsPendingFailedCount() : 0;
  const totalPend = pend + partsPend;
  const lastSync = await RX.getKey('lastSyncAt');
  // lastSyncAt = last OUTBOUND push (this device → cloud). lastInboundAt = last
  // time we APPLIED data received FROM the cloud (another device → this device).
  // The pill shows whichever is more recent so its time reflects live sync
  // activity in either direction, instead of freezing whenever this device is
  // only receiving. The two are kept separate on purpose: the Data Health
  // "no successful cloud write" alert reads lastSyncAt and must stay outbound-only.
  const lastInbound = await RX.getKey('lastInboundAt');
  const lastAny = Math.max(lastSync || 0, lastInbound || 0) || null;
  const halt = RX.getHalt();
  let txt, bg;
  if (halt) { txt = '⛔ HALTED'; bg = '#7f1d1d'; }
  else if (totalPend > 0) {
    txt = partsPend > 0 && pend > 0
      ? '⏳ ' + totalPend + ' saving (' + partsPend + ' parts)…'
      : (partsPend > 0 ? '⏳ ' + partsPend + ' part(s) syncing…' : '⏳ ' + pend + ' saving…');
    bg = '#d97706';
  }
  else if (!navigator.onLine) { txt = '⚡ Offline'; bg = '#64748b'; }
  else if (partsDead > 0) { txt = '⚠️ ' + partsDead + ' part(s) failed · Synced'; bg = '#b45309'; }
  else if (_errorCount > 0) {
    txt = '🐛 ' + _errorCount + ' error' + (_errorCount > 1 ? 's' : '') + ' · Synced';
    bg = '#7c3aed';
  }
  else { txt = '✅ Synced' + (lastAny ? ' · ' + new Date(lastAny).toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit'}) : ''); bg = '#16a34a'; }
  el.textContent = txt;
  el.style.background = bg;
  // When something has actually failed, tapping the pill goes straight to the named
  // list rather than making you dig through Data Health -> Actions.
  el.onclick = (partsDead > 0 && typeof failedPartsUI === 'function') ? failedPartsUI : rxOpenDataPanel;
  el.title = partsDead > 0 ? 'Tap to see which parts failed to sync' : 'Tap for data health details';
}
// Call this whenever remote data is actually applied (a listener fired with a
// real change, or a pull merged cloud data). It advances lastInboundAt so the
// status pill's time keeps moving while this device is receiving — the fix for
// the pill freezing at the last outbound push. Throttled to one IDB write / 3s
// so a burst of snapshots doesn't hammer the keystore.
let _lastInboundPersist = 0;
window._markInbound = function() {
  const now = Date.now();
  if (now - _lastInboundPersist < 3000) return; // throttle keystore writes
  _lastInboundPersist = now;
  try { RX.setKey('lastInboundAt', now); } catch(e) {}
  try { if (!document.hidden) renderRxStatusBar(); } catch(e) {}
};
RX.onChange(() => renderRxStatusBar());
setInterval(() => { if (!document.hidden) renderRxStatusBar(); }, window._isMobile ? 30000 : 15000); // backstop only — RX.onChange drives real-time updates; long interval cuts idle IDB churn

// ============================================================
// GLOBAL ERROR CAPTURE — catches every uncaught JS crash and
// unhandled promise rejection. Routes to RX.log (IDB) so you
// can see them in Data Health → Errors tab even after reload.
// Also bumps the error counter badge on the status pill.
// ============================================================
let _errorCount = 0; // session error count, shown on status pill

function _captureError(message, source, lineno, colno, error) {
  var stack = (error && error.stack) ? error.stack.split('\n').slice(0,4).join(' | ') : '';
  var loc   = (source ? source.split('/').pop() : '') + (lineno ? ':' + lineno : '');
  var msg   = String(message || 'Unknown error') + (loc ? ' @ ' + loc : '') + (stack ? ' | ' + stack : '');
  // Ignore noisy browser extension errors
  if (msg.includes('extension') || msg.includes('chrome-extension') || msg.includes('moz-extension')) return;
  _errorCount++;
  if (typeof RX !== 'undefined') RX.log('error', 'uncaught', msg);
  // Update the status pill to show error badge
  _updateErrorBadge();
}

function _updateErrorBadge() {
  // Don't directly mutate the pill — renderRxStatusBar owns all display logic.
  setTimeout(() => { try { renderRxStatusBar(); } catch(e) { /* UI render — non-critical */ } }, 50);
}

window.onerror = function(message, source, lineno, colno, error) {
  _captureError(message, source, lineno, colno, error);
  return false; // let browser default handling continue
};

window.addEventListener('unhandledrejection', function(event) {
  var reason = event.reason;
  var msg = reason instanceof Error ? reason.message : String(reason || 'Unhandled promise rejection');
  var stack = (reason && reason.stack) ? reason.stack.split('\n').slice(0,3).join(' | ') : '';
  _captureError('Promise: ' + msg + (stack ? ' | ' + stack : ''), '', 0, 0, null);
});
// Render immediately so the pill shows a real state on first paint, not "Starting…"
setTimeout(() => renderRxStatusBar(), 100);
setTimeout(() => renderRxStatusBar(), 1500);
window.addEventListener('online', () => renderRxStatusBar());
window.addEventListener('offline', () => renderRxStatusBar());

// ============================================================
// OFFLINE BANNER — shows when internet is lost OR when Firebase
// push fails repeatedly (network up but cloud unreachable).
// Uses two signals: navigator.onLine + consecutive push fail count.
// ============================================================
window._syncFailCount = 0;    // incremented on each failed firebasePush
window._syncOkCount   = 0;    // reset fail count when push succeeds
const OFFLINE_BANNER_THRESHOLD = 2; // show after 2 consecutive push failures

function _updateOfflineBanner() {
  const banner = document.getElementById('offline-banner');
  if (!banner) return;
  const isOffline = !navigator.onLine || window._syncFailCount >= OFFLINE_BANNER_THRESHOLD;
  banner.classList.toggle('show', isOffline);
  // Update pending count text if we know it
  const countEl = document.getElementById('offline-pending-count');
  if (countEl) {
    if (isOffline && typeof partsPendingCount === 'function' && partsPendingCount() > 0) {
      countEl.textContent = '(' + partsPendingCount() + ' record(s) queued) ';
    } else {
      countEl.textContent = '';
    }
  }
}

// Hook into online/offline events
window.addEventListener('online',  function() { window._syncFailCount = 0; _updateOfflineBanner(); });
window.addEventListener('offline', function() { _updateOfflineBanner(); });

// Patch firebasePush to track consecutive failures and successes
(function() {
  var _origPush = window.firebasePush;
  if (!_origPush) { setTimeout(function(){ // firebasePush defined inside module, retry after init
    _origPush = window.firebasePush;
    if (!_origPush) return;
    window.firebasePush = async function(stateObj) {
      try {
        const result = await _origPush(stateObj);
        window._syncFailCount = 0;
        _updateOfflineBanner();
        return result;
      } catch(e) {
        window._syncFailCount++;
        _updateOfflineBanner();
        throw e;
      }
    };
  }, 3000); return; }
  window.firebasePush = async function(stateObj) {
    try {
      const result = await _origPush(stateObj);
      window._syncFailCount = 0;
      _updateOfflineBanner();
      return result;
    } catch(e) {
      window._syncFailCount++;
      _updateOfflineBanner();
      throw e;
    }
  };
})();

// Also patch setSyncStatus so 'offline' status from firebasePush triggers banner immediately
(function() {
  var _origSetSync = window.setSyncStatus;
  // setSyncStatus is defined inside the Firebase module scope — hook via firebasePush fail path above.
  // Additionally poll every 6s so the banner reflects reality even if events were missed.
  setInterval(function(){ if(!document.hidden) _updateOfflineBanner(); }, window._isMobile ? 12000 : 6000); // online/offline events handle the hidden case
  // Show banner on first paint if already offline
  setTimeout(_updateOfflineBanner, 2000);
})();

// rxConfirmRecord — wait up to timeoutMs for a record to confirm in Firestore.
// Returns { ok: true } if confirmed, or { ok: false, queued: true|false, reason }
async function rxConfirmRecord(collection, recordId, timeoutMs) {
  timeoutMs = timeoutMs || 10000;
  // First, ensure the outbox enqueue happened (it does in save())
  // Then poll for either: (a) Firestore verify success, or (b) the outbox job marked done
  const start = Date.now();
  if (!navigator.onLine) {
    return { ok: false, queued: true, reason: 'offline — saved locally, will sync when online' };
  }
  if (!window._firebaseAuthed) {
    return { ok: false, queued: true, reason: 'auth not ready — saved locally, will sync' };
  }
  while (Date.now() - start < timeoutMs) {
    // Direct verify (cheap one-doc getDoc)
    try {
      const ok = await window.firebaseVerifyDoc(collection, recordId);
      if (ok) return { ok: true };
    } catch(e) { /* ignore, will retry */ }
    await new Promise(r => setTimeout(r, 600));
  }
  // Timed out — but the outbox is still trying. Check if we hit a permanent failure.
  const pend = await RX.listPending();
  const ours = pend.find(j => j.collection === collection && j.recordId === String(recordId));
  if (ours && ours.attempts >= 5) {
    return { ok: false, queued: false, reason: 'cloud rejected: ' + (ours.lastError || 'unknown error') };
  }
  return { ok: false, queued: true, reason: 'cloud slow — will retry in background' };
}

// Error badge clear helper — called from data health panel button
function rxClearErrorBadge() {
  _errorCount = 0;
  var el = document.getElementById('rx-status');
  if (el && !el.textContent.includes('HALT') && !el.textContent.includes('saving')) {
    el.style.background = '#16a34a';
    el.textContent = '✅ Synced';
  }
  toast('Session error count cleared');
}

// rxOpenDataPanel — drilldown shown when user taps the status pill
async function rxOpenDataPanel(activeTab) {
  activeTab = activeTab || 'sync';
  const pend    = await RX.listPending();
  const lastSync= await RX.getKey('lastSyncAt');
  const halt    = RX.getHalt();
  const allLogs = (await RX.getAll('log')).reverse();
  const errors  = allLogs.filter(function(l){ return l.level==='error'||l.level==='critical'; }).slice(0,50);
  const recLogs = allLogs.slice(0,30);
  const snaps   = await RX.listSnapshots();

  const pendHTML = pend.length ? pend.slice(0,15).map(function(j){
    return '<div style="background:#fef9c3;padding:6px 8px;border-radius:6px;margin:3px 0;font-size:12px"><b>' + j.collection + '</b> · ' + j.recordId.slice(0,12)
      + '<br><small>attempts: ' + j.attempts + (j.lastError ? ' · ' + j.lastError.slice(0,80) : '') + '</small></div>';
  }).join('') : '<div style="color:#16a34a;font-size:13px;padding:8px 0;">✅ Nothing pending — all data is in the cloud.</div>';

  function levelColor(l){ return l==='error'?'#dc2626':l==='critical'?'#7f1d1d':l==='warn'?'#d97706':'#475569'; }
  function logRow(l){
    return '<div style="font-family:monospace;font-size:11px;color:'+levelColor(l.level)+';padding:3px 0;border-bottom:1px solid #1e293b;word-break:break-all;">'
      +'<span style="opacity:.6;">'+new Date(l.ts).toLocaleTimeString('en-IN')+'</span> '
      +'<b>['+l.source+']</b> '+l.message+'</div>';
  }

  var errBadge = errors.length ? ' <span style="background:#dc2626;color:white;font-size:10px;font-weight:700;padding:1px 6px;border-radius:10px;">'+errors.length+'</span>' : '';
  function tabBtn(t, label){ return '<button style="border:none;padding:8px 10px;font-size:12px;font-weight:600;cursor:pointer;border-bottom:2px solid '+(activeTab===t?'var(--primary)':'transparent')+';color:'+(activeTab===t?'var(--primary)':'var(--muted)')+';background:none;white-space:nowrap;" onclick="rxOpenDataPanel(&quot;'+t+'&quot;)">'+label+'</button>'; }

  const html = ''
    + (halt ? '<div style="background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:10px;margin-bottom:10px;color:#991b1b;font-weight:700">⛔ HALTED: ' + halt + '</div>' : '')
    + '<div style="display:flex;border-bottom:1px solid var(--border);margin-bottom:12px;overflow-x:auto;gap:0;">'
      + tabBtn('sync','☁️ Sync') + tabBtn('errors','🐛 Errors'+errBadge) + tabBtn('log','📋 Log') + tabBtn('actions','⚡ Actions')
    + '</div>'

    + (activeTab==='sync' ? (''
      + '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:12px">'
        + '<div style="background:#f1f5f9;padding:10px;border-radius:8px;text-align:center;"><div style="font-size:11px;color:#64748b;text-transform:uppercase;margin-bottom:4px;">Pending</div><div style="font-size:24px;font-weight:700;color:'+(pend.length?'#d97706':'#16a34a')+'">'+pend.length+'</div></div>'
        + '<div style="background:#f1f5f9;padding:10px;border-radius:8px;text-align:center;"><div style="font-size:11px;color:#64748b;text-transform:uppercase;margin-bottom:4px;">Last Sync</div><div style="font-size:11px;font-weight:600;margin-top:2px">'+(lastSync?new Date(lastSync).toLocaleString('en-IN'):'Never')+'</div></div>'
        + '<div style="background:#f1f5f9;padding:10px;border-radius:8px;text-align:center;"><div style="font-size:11px;color:#64748b;text-transform:uppercase;margin-bottom:4px;">Session Errors</div><div style="font-size:24px;font-weight:700;color:'+(_errorCount>0?'#dc2626':'#16a34a')+'">'+_errorCount+'</div></div>'
        + '<div style="background:#f1f5f9;padding:10px;border-radius:8px;text-align:center;"><div style="font-size:11px;color:#64748b;text-transform:uppercase;margin-bottom:4px;">Logged Errors</div><div style="font-size:24px;font-weight:700;color:'+(errors.length>0?'#dc2626':'#16a34a')+'">'+errors.length+'</div></div>'
      + '</div>'
      + '<div style="background:white;border:1px solid var(--border);border-radius:10px;padding:10px;margin-bottom:10px;"><h3 style="font-size:13px;font-weight:700;margin:0 0 8px;">Outbox</h3>' + pendHTML + '</div>'
      + '<div style="background:white;border:1px solid var(--border);border-radius:10px;padding:10px;">'
        + '<h3 style="font-size:13px;font-weight:700;margin:0 0 6px;">Auto Snapshots</h3>'
        + '<div style="font-size:12px;color:#64748b">'+(snaps.length ? snaps.length + ' saved · oldest ' + new Date(snaps[snaps.length-1].ts).toLocaleDateString('en-IN') : 'None yet')+'</div>'
        + (snaps.length ? '<button class="btn btn-sm mt-1" onclick="rxDownloadLatestSnapshot()" style="background:#475569;width:auto;">⬇ Download latest</button>' : '')
      + '</div>'
    ) : '')

    + (activeTab==='errors' ? (errors.length===0
      ? '<div style="text-align:center;padding:32px 16px;color:#16a34a;"><div style="font-size:32px;margin-bottom:8px;">✅</div><div style="font-size:14px;font-weight:600;">No errors recorded</div><div style="font-size:12px;color:var(--muted);margin-top:6px;">Every uncaught JS error and crash will automatically appear here.</div></div>'
      : '<div style="font-size:12px;color:var(--muted);margin-bottom:8px;">'+errors.length+' error(s) — newest first. These are all the things that broke silently.</div>'
        + '<div style="background:#0f172a;border-radius:10px;padding:10px;max-height:360px;overflow-y:auto;">' + errors.map(logRow).join('') + '</div>'
        + '<button class="btn btn-sm mt-2" onclick="rxClearErrorBadge()" style="background:#64748b;width:auto;">Clear session count</button>'
    ) : '')

    + (activeTab==='log' ? (''
      + '<div style="background:#0f172a;color:#cbd5e1;border-radius:10px;padding:10px;max-height:380px;overflow-y:auto;">'
        + '<div style="font-size:11px;color:#94a3b8;margin-bottom:6px;text-transform:uppercase;letter-spacing:.5px;">System Log — last 30</div>'
        + recLogs.map(logRow).join('')
      + '</div>'
    ) : '')

    + (activeTab==='actions' ? (''
      + '<div style="display:flex;flex-direction:column;gap:8px;">'
        + '<button class="btn" onclick="failedPartsUI();" style="background:#b91c1c;">📋 Show Parts That Failed To Sync (' + (typeof partsPendingCount==='function'?partsPendingCount():'?') + ')</button>'
        + '<button class="btn" onclick="RX.drain();drainPartsCloudPending();toast(&quot;Retrying…&quot;);" style="background:#0f766e;">🔁 Retry Pending Writes</button>'
        + '<button class="btn" onclick="healStrandedPartsUI();" style="background:#b45309;">🩹 Heal &amp; Sync Stranded Parts (' + (typeof partsStillLocalCount==='function'?partsStillLocalCount():'?') + ')</button>'
        + '<button class="btn" onclick="cloudVsLocalCountUI();" style="background:#0e7490;">☁ Count Cloud vs Local Parts</button>'
        + '<button class="btn" onclick="cloudWriteTestUI();" style="background:#9333ea;">🧪 Test Cloud Write (show error)</button>'
        + '<button class="btn" onclick="diagnosePartsSyncUI();" style="background:#be123c;">🔍 Diagnose Parts Sync</button>'
        + '<button class="btn" onclick="recomputeTotalDue();save();toast(&quot;✅ Balances reconciled&quot;);" style="background:#7c3aed;">⚖️ Reconcile All Balances</button>'
        + '<button class="btn" onclick="rxDiagnose();" style="background:#0369a1;">🔬 Full Sync Diagnostic</button>'
        + '<button class="btn" onclick="rxDownloadEmergencyBackup();" style="background:#7c3aed;">💾 Emergency Backup</button>'
        + '<button class="btn" onclick="rxRunDailyReport();" style="background:#0c4a6e;">📊 Daily Report</button>'
        + '<button class="btn btn-outline" onclick="try{runIntegrityChecks();save();}catch(e){}toast(&quot;✅ Integrity checks done&quot;);">🔎 Run Integrity Checks</button>'
      + '</div>'
    ) : '');

  showModal('🛡️ Data Health', html);
}

// rxDiagnose — end-to-end sync test: writes a test doc, reads it back,
// reports each step. Use this to verify "sync configuration and data
// synchronization status" without relying on the silent #sync-indicator.
async function rxDiagnose() {
  const lines = [];
  const ok = (s) => '<div style="color:#16a34a">✓ ' + s + '</div>';
  const bad = (s) => '<div style="color:#dc2626">✗ ' + s + '</div>';
  const info = (s) => '<div style="color:#475569">· ' + s + '</div>';

  lines.push(info('Browser online: ' + navigator.onLine));
  lines.push(info('Firebase loaded: ' + !!window.firebasePush));
  lines.push(info('Authenticated: ' + !!window._firebaseAuthed));
  lines.push(info('Parts cloud queue: ' + (typeof partsPendingCount === 'function' ? partsPendingCount() : 0) + ' pending'));
  if (!navigator.onLine) { lines.push(bad('OFFLINE — sync paused, outbox holding writes')); return _showDiag(lines); }
  if (!window._firebaseAuthed) { lines.push(bad('NOT AUTHENTICATED — reload the app')); return _showDiag(lines); }

  // 1. Test enqueue
  const testId = 'diag_' + Date.now();
  const testPayload = { id: testId, hello: 'world', ts: Date.now() };
  try {
    await RX.enqueue({ type:'append', collection:'_diagnostics', recordId: testId, payload: testPayload });
    lines.push(ok('Outbox enqueue OK'));
  } catch(e) { lines.push(bad('Outbox enqueue failed: ' + e.message)); return _showDiag(lines); }

  // 2. Force drain
  await RX.drain();

  // 3. Verify in cloud
  const start = Date.now();
  let confirmed = false;
  while (Date.now() - start < 15000) {
    try {
      if (await window.firebaseVerifyDoc('_diagnostics', testId)) { confirmed = true; break; }
    } catch(e) { /* verify poll — transient errors expected */ }
    await new Promise(r => setTimeout(r, 500));
  }
  if (confirmed) {
    lines.push(ok('Round-trip Firestore write+read in ' + (Date.now()-start) + 'ms'));
    // 4. Soft-delete the test doc to keep collection clean
    try { await window.firebaseDeleteDoc('_diagnostics', testId); lines.push(ok('Test doc cleaned up')); } catch(e) { /* optional diagnostics cleanup — non-critical */ }
  } else {
    lines.push(bad('Cloud verification timed out after 15s — write is queued but not confirmed'));
  }

  // 5. Outbox health
  const pend = await RX.listPending();
  lines.push(info('Outbox pending: ' + pend.length + (pend.length ? ' (oldest ' + Math.round((Date.now() - Math.min.apply(null, pend.map(p=>p.createdAt))) / 1000) + 's)' : '')));
  const lastSync = await RX.getKey('lastSyncAt');
  lines.push(info('Last successful sync: ' + (lastSync ? new Date(lastSync).toLocaleString('en-IN') : 'never')));

  _showDiag(lines);
}
function _showDiag(lines) {
  showModal('🔬 Sync Diagnostics', '<div style="font-family:monospace;font-size:13px;line-height:1.7">' + lines.join('') + '</div>'
    + '<button class="btn mt-2" onclick="rxDiagnose()" style="background:#0369a1">↻ Re-run</button>');
}

async function rxDownloadLatestSnapshot() {
  const snaps = await RX.listSnapshots();
  if (!snaps.length) { toast('No snapshots yet'); return; }
  const blob = new Blob([JSON.stringify(snaps[0].snap,null,2)], { type:'application/json' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'shop-snapshot-' + new Date(snaps[0].ts).toISOString().slice(0,10) + '.json';
  a.click();
  setTimeout(function(){ URL.revokeObjectURL(a.href); }, 5000); // free blob after download starts
}

// rxRestoreDraft — called on app boot; if user had a cart in progress, offer to restore it
async function rxRestoreDraft() {
  try {
    const d = await RX.loadDraft('cart');
    if (!d) return;
    const ageMin = Math.round((Date.now() - d.ts) / 60000);
    const total = (d.cart || []).length + (d.woCart || []).length + (d.quoteCart || []).length;
    if (total === 0) return;
    if (ageMin > 60 * 24) { RX.clearDraft('cart'); return; } // older than 24h, drop
    if (confirm('🛟 Crash recovery: you had ' + total + ' item(s) in your cart from ' + ageMin + ' min ago. Restore?')) {
      state.cart = d.cart || [];
      state.payMode = d.payMode || 'cash';
      state.woCart = d.woCart || [];
      state.quoteCart = d.quoteCart || [];
      RX.log('info','recovery','restored ' + total + ' draft items');
      try { renderCart(); } catch(e) { /* UI rerender — non-critical */ }
    } else {
      RX.clearDraft('cart');
    }
  } catch(e) { /* don't block boot */ }
}

// rxAutoSnapshot — schedules a daily snapshot of state into IndexedDB
async function rxAutoSnapshot() {
  try {
    const last = await RX.getKey('lastSnapshotAt');
    const ONE_DAY = 24 * 60 * 60 * 1000;
    if (last && Date.now() - last < ONE_DAY) return;
    const snap = {
      ts: Date.now(),
      counts: {
        sales: state.sales.length, orders: state.orders.length,
        wo: state.wholesaleOrders.length, parts: state.parts.length,
        customers: state.customers.length
      },
      state: {
        parts: state.parts, customers: state.customers, sales: state.sales,
        orders: state.orders, staff: state.staff, users: state.users,
        wholesaleOrders: state.wholesaleOrders, salaryRecords: state.salaryRecords,
        advanceRecords: state.advanceRecords, staffScores: state.staffScores,
        attendanceRecords: state.attendanceRecords, mechanics: state.mechanics,
        retailCustomers: state.retailCustomers, collections: state.collections,
        expenses: state.expenses, settings: state.settings
      }
    };
    await RX.pushSnapshot(snap);
    await RX.setKey('lastSnapshotAt', Date.now());
    RX.log('info','snapshot','auto-snapshot saved (sales=' + snap.counts.sales + ')');
  } catch(e) { RX.log('error','snapshot','failed: ' + e.message); }
}

// guardSale() — called BEFORE any billing action; returns true if safe to proceed
async function rxGuardSale(label) {
  const halt = RX.getHalt();
  if (halt) {
    alert('⛔ Cannot complete ' + label + ': ' + halt + '\n\nThe agent has halted new entries until the existing data is safely saved. Tap "Retry Now" on the red banner.');
    return false;
  }
  const pend = await RX.pendingCount();
  if (pend > 20) {
    if (!confirm('There are already ' + pend + ' writes pending sync. Continue anyway?')) return false;
  }
  return true;
}

// ============================================================
// DATA LAYER — Scale-safe: large arrays live in IndexedDB, not localStorage
// ============================================================
// WHY THIS EXISTS: localStorage has a HARD ~5 MB per-origin cap (mobile Chrome).
// The parts catalogue alone is 3 MB+ at 10,500 SKUs; add sales/orders/wholesale/
// activity-log and the cap is blown — every save() then throws QuotaExceededError
// and the red "Device storage full" banner fires mid-operation. The old "chunking"
// only split the data into pieces inside the SAME 5 MB bucket, so it never escaped
// the limit. IndexedDB has an orders-of-magnitude larger, browser-managed quota
// (hundreds of MB to GB) and is the correct home for these arrays. Small config
// keys stay in localStorage (instant, synchronous, far under the cap).
const DB_LARGE_KEYS = new Set(['parts','sales','orders','wholesaleOrders','activityLog','visualTraining']);

// In-memory authoritative copy of the large arrays for the current session.
// Hydrated from IndexedDB once at boot (DB.hydrateLargeKeys). DB.get reads this
// synchronously so the rest of the app never has to await a read.
const _idbLargeCache = Object.create(null);
let _idbLargeReady = false;
let _idbHydrating = null; // shared in-flight promise so concurrent callers don't double-migrate

// Coalesced async persist of large keys to IndexedDB (keystore: id 'L_<key>').
// A pending map lets us flush immediately on pagehide so the last edit is durable.
const _idbWritePending = Object.create(null);
const _idbWriteTimers = Object.create(null);
function _idbPersistLarge(k, v) {
  _idbWritePending[k] = v;
  clearTimeout(_idbWriteTimers[k]);
  _idbWriteTimers[k] = setTimeout(function(){ _idbFlushLarge(k); }, 600);
}
function _idbFlushLarge(k) {
  if (!(k in _idbWritePending)) return Promise.resolve();
  const v = _idbWritePending[k];
  delete _idbWritePending[k];
  clearTimeout(_idbWriteTimers[k]);
  if (typeof RX === 'undefined' || !RX.put) return Promise.resolve();
  return RX.put('keystore', { id: 'L_' + k, value: v }).then(function(){
    if (DB._writeFailed) delete DB._writeFailed[k];
  }).catch(function(e){
    const msg = (e && e.message) || String(e);
    DB._writeFailed = DB._writeFailed || {};
    DB._writeFailed[k] = msg;
    try { RX.log('error','storage','IDB persist failed for '+k+': '+msg); } catch(_){}
    // Genuine IndexedDB exhaustion = the actual phone disk is full. Surface it
    // honestly — this is far rarer than the old localStorage 5 MB ceiling and
    // means the user really does need to free space.
    if (k === 'parts' && /quota|exceed|storage|full|space/i.test(msg) && typeof catSaveBlockError === 'function') {
      catSaveBlockError('❌ Phone storage full — parts not saving locally. Cloud sync still active. Free up space on the device, then reload.');
    }
  });
}
function _idbFlushAllLarge() { return Promise.all(Object.keys(_idbWritePending).map(_idbFlushLarge)); }

// Read a large key out of legacy localStorage (flat or chunked). Used only during
// one-time migration for users upgrading from the old localStorage-backed build.
function _readLegacyLocalStorage(k) {
  try {
    const n = parseInt(localStorage.getItem(k+'__chunks'));
    if (!isNaN(n) && n > 0) {
      let a = [];
      for (let i=0;i<n;i++){ const c=localStorage.getItem(k+'__'+i); if(c) a=a.concat(JSON.parse(c)); }
      if (a.length) return a;
    }
    const v = localStorage.getItem(k);
    if (v !== null) { const parsed = JSON.parse(v); if (parsed !== null && parsed !== undefined && (!Array.isArray(parsed) || parsed.length)) return parsed; }
  } catch(e) { /* corrupt/absent legacy data — ignore */ }
  return null;
}
// Purge a large key's legacy localStorage footprint (flat value + all chunk keys).
// This is what actually FREES the jammed 5 MB cap once the data is safe in IDB.
function _purgeLegacyLocalStorage(k) {
  try {
    const n = parseInt(localStorage.getItem(k+'__chunks'));
    if (!isNaN(n) && n > 0) { for (let i=0;i<n;i++) localStorage.removeItem(k+'__'+i); }
    localStorage.removeItem(k+'__chunks');
    localStorage.removeItem(k);
  } catch(e) { /* purge best-effort */ }
}

const DB = {
  get(k,d) {
    // Large keys: served from the in-memory IDB cache. Before hydration finishes
    // (only the very first synchronous reads at state-init), fall back to any
    // legacy localStorage copy so upgrading users still see their data instantly.
    if (DB_LARGE_KEYS.has(k)) {
      if (k in _idbLargeCache) return _idbLargeCache[k];
      const legacy = _readLegacyLocalStorage(k);
      return legacy !== null ? legacy : d;
    }
    try {
      const n=parseInt(localStorage.getItem(k+'__chunks'));
      if(!isNaN(n)&&n>0){let a=[];for(let i=0;i<n;i++){const c=localStorage.getItem(k+'__'+i);if(c)a=a.concat(JSON.parse(c));}if(a.length)return a;}
      const v=localStorage.getItem(k);if(v!==null)return JSON.parse(v)??d;
    }catch{}
    return d;
  },
  set(k,v,preStr) {
    // Large keys → IndexedDB. The in-memory cache is updated synchronously (so the
    // running session is correct instantly); the durable IDB write is coalesced.
    if (DB_LARGE_KEYS.has(k)) {
      if (k === 'parts' && typeof invalidatePartsIndex === 'function') invalidatePartsIndex();
      _idbLargeCache[k] = v;          // authoritative for this session
      _idbPersistLarge(k, v);          // async, coalesced → IndexedDB
      _purgeLegacyLocalStorage(k);     // reclaim any old localStorage chunks for good
      return;
    }
    try {
      const s = (preStr !== undefined) ? preStr : JSON.stringify(v);
      const oldN = parseInt(localStorage.getItem(k+'__chunks'));
      if(s.length<3*1024*1024){
        localStorage.setItem(k,s);
        localStorage.removeItem(k+'__chunks');
        // Clean orphaned chunk keys left behind when a previously-chunked array shrinks
        if(!isNaN(oldN)&&oldN>0){for(let i=0;i<oldN;i++)localStorage.removeItem(k+'__'+i);}
      }
      else if(Array.isArray(v)){
        const N=Math.ceil(v.length/500);
        for(let i=0;i<N;i++)localStorage.setItem(k+'__'+i,JSON.stringify(v.slice(i*500,(i+1)*500)));
        localStorage.setItem(k+'__chunks',N);
        localStorage.setItem(k,'[]');
        if(!isNaN(oldN)&&oldN>N){for(let i=N;i<oldN;i++)localStorage.removeItem(k+'__'+i);}
      }
      if (DB._writeFailed) delete DB._writeFailed[k];
    }catch(e){
      console.warn('localStorage fail:',k,e.message);
      DB._writeFailed = DB._writeFailed || {};
      DB._writeFailed[k] = e.message;
      try { if (typeof RX !== 'undefined') RX.log('error','storage','DB.set failed for '+k+': '+e.message); } catch(e2) { /* logging only */ }
      // A SMALL key overflowing localStorage means the bucket is jammed by leftover
      // junk (small keys are tiny). Self-heal: purge any stale large-key chunks an
      // older build may have left behind, then retry the write once.
      if (/quota|exceed|storage/i.test(e.message||'')) {
        try {
          DB_LARGE_KEYS.forEach(_purgeLegacyLocalStorage);
          localStorage.setItem(k, (preStr!==undefined)?preStr:JSON.stringify(v));
          if (DB._writeFailed) delete DB._writeFailed[k];
          try { RX.log('info','storage','self-healed localStorage by purging legacy large-key chunks'); } catch(_){}
        } catch(e2) { /* small keys are tiny — reaching here would be extraordinary */ }
      }
    }
  },
  // Boot-time hydration. Loads each large key from IndexedDB into the in-memory
  // cache. For users upgrading from the old build the data is still in localStorage
  // — migrate it into IDB and purge the localStorage copy, which is what
  // PERMANENTLY clears the "storage full" jam. Idempotent; awaited before first render.
  hydrateLargeKeys() {
    if (_idbLargeReady) return Promise.resolve();
    if (_idbHydrating) return _idbHydrating;       // a concurrent caller is already running it
    _idbHydrating = DB._doHydrateLargeKeys();
    return _idbHydrating;
  },
  async _doHydrateLargeKeys() {
    for (const k of DB_LARGE_KEYS) {
      try {
        let rec = null;
        try { rec = (typeof RX !== 'undefined' && RX.get) ? await RX.get('keystore', 'L_' + k) : null; } catch(e) { rec = null; }
        if (rec && rec.value !== undefined && rec.value !== null) {
          _idbLargeCache[k] = rec.value;
          _purgeLegacyLocalStorage(k); // IDB is canonical now — drop any stale LS copy
        } else {
          // Not in IDB yet → migrate from legacy localStorage (or start empty)
          const legacy = _readLegacyLocalStorage(k);
          const val = legacy !== null ? legacy : [];
          _idbLargeCache[k] = val;
          if (typeof RX !== 'undefined' && RX.put) {
            try { await RX.put('keystore', { id: 'L_' + k, value: val }); _purgeLegacyLocalStorage(k); } catch(e) { /* will retry on next save() */ }
          }
        }
      } catch(e) {
        _idbLargeCache[k] = [];
        try { RX.log('error','storage','hydrate failed for '+k+': '+e.message); } catch(_){}
      }
    }
    _idbLargeReady = true;
    // Best-effort: ask the browser to keep our IndexedDB from being evicted under
    // pressure, and log a heads-up well before any real limit (turns a silent
    // failure into an early, actionable warning).
    try {
      if (navigator.storage && navigator.storage.persist) navigator.storage.persist().catch(function(){});
      if (navigator.storage && navigator.storage.estimate) {
        navigator.storage.estimate().then(function(est){
          if (est && est.quota && est.usage/est.quota > 0.9) {
            try { RX.log('warn','storage','IDB usage above 90% ('+Math.round(est.usage/1048576)+'MB / '+Math.round(est.quota/1048576)+'MB)'); } catch(_){}
          }
        }).catch(function(){});
      }
    } catch(e) { /* storage manager unsupported — fine */ }
  }
};

// ── Deferred-write layer for large arrays (parts ≈ 3MB+ stringify + 21 chunk writes) ──
// Coalesces rapid edits into one write 1.2s after the last change instead of blocking
// the main thread on every save(). Guaranteed flush on tab hide/close so nothing is lost.
const _dwPending = {}, _dwTimers = {};
function _flushDeferredWrite(k){
  if (k in _dwPending){ const v=_dwPending[k]; delete _dwPending[k]; clearTimeout(_dwTimers[k]); DB.set(k,v); }
}
function _flushAllDeferredWrites(){ for (const k of Object.keys(_dwPending)) _flushDeferredWrite(k); }
function _scheduleDeferredWrite(k,v){
  _dwPending[k]=v; clearTimeout(_dwTimers[k]);
  _dwTimers[k]=setTimeout(function(){_flushDeferredWrite(k);}, 1200);
}
// On tab hide/close: flush the coalesced deferred writes (which routes large keys
// into _idbWritePending via DB.set), THEN fire the pending IndexedDB writes
// immediately so the last edit is durable instead of waiting on the 600ms timer.
function _flushEverything(){ _flushAllDeferredWrites(); _idbFlushAllLarge(); }
window.addEventListener('pagehide', _flushEverything);
window.addEventListener('beforeunload', _flushEverything);
document.addEventListener('visibilitychange', function(){ if (document.visibilityState==='hidden') _flushEverything(); });

let state = {
  parts: DB.get('parts', []),
  customers: DB.get('customers', []),
  sales: DB.get('sales', []),
  orders: DB.get('orders', []),
  staff: DB.get('staff', []),
  users: DB.get('users', []),
  wholesaleOrders: DB.get('wholesaleOrders', []),
  recycleBin: DB.get('recycleBin', []),
  salaryRecords: DB.get('salaryRecords', []),
  advanceRecords: DB.get('advanceRecords', []),
  staffScores: DB.get('staffScores', {}),
  aiFeedback: DB.get('aiFeedback', []),
  attendanceRecords: DB.get('attendanceRecords', {}),
  mechanics: DB.get('mechanics', []),
  retailCustomers: DB.get('retailCustomers', []),
  collections: DB.get('collections', []),
  expenses: DB.get('expenses', []),
  suppliers: DB.get('suppliers', []),
  purchases: DB.get('purchases', []),
  demandInbox: DB.get('demandInbox', []),
  incomingStock: DB.get('incomingStock', []),
  saleReturns: DB.get('saleReturns', []),   // credit notes / return transactions
  purchaseOrders: DB.get('purchaseOrders', []),  // PO → sent → received flow
  locations: DB.get('locations', []),            // branches / godowns / counters
  shopNotes: DB.get('shopNotes', []),            // staff → owner notes (part khatam, etc.)
  supplierAdvances: DB.get('supplierAdvances', []),
  dayClosings: DB.get('dayClosings', []),
  bankAccounts: DB.get('bankAccounts', []),
  manualPayables: DB.get('manualPayables', []),
  activityLog: DB.get('activityLog', []),
  brandMatrix: DB.get('brandMatrix', []),
  visualTraining: DB.get('visualTraining', []),
  settings: DB.get('settings', {
    shopName: 'My Spare Parts Shop',
    shopPhone: '',
    address: '',
    gstNo: '',
    creditTerms: '15-30 days credit, 2% interest after due date',
    nextInvoice: 1001,
    nextWO: 1001,
    aiKey: '',
    aiVisionKey: '',
    aiModel: 'claude-haiku-4-5-20251001',
    bsp: {provider:'Wati', key:'', webhook:''},
    commRate: 1.5,
    delivRate: 50,
    salesTarget: 50000,
    advanceCap: 20000,
    advancePctOfSalary: 50, // single advance/advance-payout ≤ this % of monthly salary
    backupFreq: 3,
    lastBackup: null
  }),
  cart: [],
  payMode: 'cash',
  quoteCart: [],
  filterMachine: 'all',
  chat: [],
  bulkQueue: [],
  session: DB.get('session', null),
  woFilter: 'all',
  woCart: []
};
window.state = state; // expose for RX helpers

// ============================================================
// PER-RECORD ENQUEUE TRACKING (hash-based change detection)
// We compare each record's serialised form against the last-enqueued
// hash. Only changed records get re-pushed to the outbox.
// ============================================================
const _RX_TRACKED = {
  // EXCLUDED from per-record outbox (handled by firebasePush chunked bulk docs — faster):
  // parts, sales, orders, wholesaleOrders, activityLog, visualTraining
  // These are LARGE_KEYS — firebasePush chunks them into shopdata/key__0, key__1 etc.
  // in one batch commit. Per-record outbox for 500 parts = 500 IDB writes = slow.
  //
  // Per-record outbox: small/critical records only (survives crash/offline)
  collections: 'id', expenses: 'id', salaryRecords: 'id', advanceRecords: 'id',
  customers: 'id', staff: 'id', users: 'id', mechanics: 'id',
  retailCustomers: 'id',
  purchases: 'id', suppliers: 'id', supplierAdvances: 'id',
  demandInbox: 'id', incomingStock: 'id', dayClosings: 'id',
  bankAccounts: 'id', manualPayables: 'id',
  saleReturns: 'id',
  purchaseOrders: 'id',
  locations: 'id',
  shopNotes: 'id',
};
const _RX_HASH_KEY = 'rx_record_hashes_v1';
// Persistent set of deleted purchase IDs — survives page reload, prevents Firestore echoes from restoring deleted bills
const _purchaseDeletedIds = new Set(
  (function(){ try { return JSON.parse(localStorage.getItem('_deletedPurchaseIds') || '[]'); } catch(e){ return []; } })()
);
let _rxHashes = null;
function _rxLoadHashes() {
  if (_rxHashes) return _rxHashes;
  try { _rxHashes = JSON.parse(localStorage.getItem(_RX_HASH_KEY) || '{}'); }
  catch(e) { _rxHashes = {}; }
  return _rxHashes;
}
function _rxSaveHashes() {
  try { localStorage.setItem(_RX_HASH_KEY, JSON.stringify(_rxHashes)); } catch(e) { /* hash cache — survivable if quota full */ }
}
function _quickHash(s) {
  let h = 0; for (let i=0;i<s.length;i++) { h = ((h<<5)-h) + s.charCodeAt(i); h |= 0; }
  return String(h);
}
const _RX_YIELD_EVERY = 400; // records scanned before handing a frame back to the browser
async function rxEnqueueChanged() {
  if (!window.RX) return;
  const hashes = _rxLoadHashes();
  let dirty = false;
  const pending = []; // collect enqueue promises so callers can await them
  for (const coll of Object.keys(_RX_TRACKED)) {
    const arr = state[coll];
    if (!Array.isArray(arr)) continue;
    const idField = _RX_TRACKED[coll];
    const seen = new Set();
    // PERF: this rescans EVERY record in EVERY tracked collection, stringifying and
    // hashing each one, and it runs inside every save() plus a 30s interval. On a
    // multi-thousand-record shop that is a long synchronous block on the main thread.
    // Yield to the browser every _RX_YIELD_EVERY records so the UI stays responsive —
    // the work is unchanged, it just stops monopolising a single frame.
    let _scanned = 0;
    for (const item of arr) {
      if (++_scanned % _RX_YIELD_EVERY === 0) await new Promise(function(r){ setTimeout(r, 0); });
      if (!item || !item[idField]) continue;
      const id = item[idField];
      seen.add(coll + '|' + id);
      const s = JSON.stringify(item);
      const h = _quickHash(s);
      const k = coll + '|' + id;
      if (hashes[k] !== h) {
        hashes[k] = h;
        dirty = true;
        // Strip base64 photoDataUrl before writing to Firestore.
        // A base64 image can be 1–3MB, exceeding Firestore's 1MB doc limit.
        // The real URL (https://...) is safe to store. Base64 is local-only fallback.
        let payload = item;
        if (coll === 'parts' && item.photoDataUrl && item.photoDataUrl.startsWith('data:')) {
          payload = Object.assign({}, item, { photoDataUrl: '__pending_upload__' });
          // Kick off a background Storage upload to replace the base64
          if (window.firebaseUploadPartPhoto && window._firebaseAuthed) {
            (function(partId, dataUrl) {
              window.firebaseUploadPartPhoto(partId, dataUrl)
                .then(function(url) {
                  var p = state.parts && getPartById(partId);
                  if (p) { p.photoDataUrl = url; save(); try { if (typeof cloudPersistPart === 'function') cloudPersistPart(p); } catch(e){} }
                })
                .catch(function(e) { console.warn('[RX] bg photo upload failed for', partId, e.message); });
            })(item.id, item.photoDataUrl);
          }
        }
        pending.push(RX.enqueue({ type: 'append', collection: coll, recordId: id, payload: payload }));
      }
    }
    // Detect deletions — snapshot keys first to avoid mutation-during-iteration
    // OWNER-ONLY GUARD: cloud deletes only fire when an owner UI action sets
    // window._ownerDeleteInProgress = true before calling save().
    // Background saves (agent, auto-sync, heartbeat) never set this flag,
    // so records removed from in-memory state by non-owner code are silently
    // ignored at the cloud layer — they will never be deleted from Firestore.
    const keysSnap = Object.keys(hashes);
    for (const k of keysSnap) {
      if (k.startsWith(coll + '|') && !seen.has(k)) {
        const id = k.slice(coll.length + 1);
        if (window._ownerDeleteInProgress) {
          // Owner explicitly deleted this record — sync to cloud
          pending.push(RX.enqueue({ type: 'delete', collection: coll, recordId: id, payload: null }));
        } else {
          // No owner action — restore hash so next real owner-delete still fires
          // (don't remove from hashes; just skip the cloud delete silently)
          console.warn('[RX] Blocked background cloud-delete of', coll, id, '— owner action required');
        }
        delete hashes[k];
        dirty = true;
      }
    }
  }
  if (dirty) _rxSaveHashes();
  // Ensure all enqueues are durable in IDB before we return
  if (pending.length) await Promise.all(pending);
}

// Dirty-tracking for save() — avoids re-serialising unchanged arrays on every save.
// Uses a fingerprint: array length + first-item id + last-item id + last-item timestamp.
// Mutating a record changes its fields but not its array reference — the fingerprint catches this.
const _saveFingerprints = {};
const _keyLastSaved = {}; // tracks when each state key was last written locally
const _SAVE_KEYS = ['parts','customers','sales','orders','staff','users','wholesaleOrders',
  'salaryRecords','advanceRecords','staffScores','aiFeedback','attendanceRecords','mechanics',
  'retailCustomers','collections','expenses','suppliers','purchases','demandInbox','incomingStock',
  'supplierAdvances','dayClosings','bankAccounts','manualPayables','activityLog','brandMatrix',
  'visualTraining','settings','saleReturns','purchaseOrders','locations','recycleBin','shopNotes'];

function _fingerprint(val) {
  if (!Array.isArray(val)) return JSON.stringify(val); // settings/objects: full compare
  if (!val.length) return '[]';
  const f = val[0], l = val[val.length - 1];
  return val.length + '|' + (f && f.id || JSON.stringify(f)).slice(0,20)
    + '|' + (l && l.id || JSON.stringify(l)).slice(0,20)
    + '|' + (l && (l.updatedAt || l.addedAt || l.date || l.createdAt || '') || '');
}

// Track what changed so renders can be targeted
let _changedKeys = new Set();
function markChanged(key) {
  _changedKeys.add(key);
  // Parts mutated → cached O(1) lookup index is stale. Invalidate here (single
  // choke point) so no individual write site can forget. Rebuild is lazy.
  if (key === 'parts' && typeof invalidatePartsIndex === 'function') invalidatePartsIndex();
}

// FNV-1a hash — exact, cheap change detection without storing multi-MB fingerprint strings
function _hashStr(s) {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = (h * 0x01000193) >>> 0; }
  return s.length + '|' + h.toString(36);
}
// Large arrays keep the cheap heuristic fingerprint + deferred write; everything else
// gets an exact stringify-based check (the stringify is reused for the write, so it's free)
const _FP_HEURISTIC_KEYS = ['parts', 'visualTraining']; // retained for external callers; save() no longer uses it

// ── CLOUD DIRTY-KEY TRACKER ────────────────────────────────────────────────
// Which state keys have changed locally since the last CONFIRMED successful push.
// null = "cloud state unknown" → firebasePush does a full upload. That is the boot
// default, so the first push of a session still seeds/repairs everything. After that
// only genuinely-changed keys go up, instead of the whole database on every save.
window._fbDirtyKeys = null;
function _fbMarkDirty(k) {
  if (!window._fbDirtyKeys) return; // null/undefined = already queued for a full push
  window._fbDirtyKeys.add(k);
}

function save() {
  window._lastLocalSave = Date.now(); // guard for firebasePullParts race
  // Snapshot explicit dirty marks BEFORE clearing — an explicitly marked key must always
  // write, bypassing the fingerprint. The heuristic fingerprint (length|first|last|lastTs)
  // cannot see mid-array field edits, which previously caused silent data loss on reload.
  const explicitlyMarked = new Set(_changedKeys);
  const keysToCheck = _changedKeys.size > 0 ? [..._changedKeys].filter(k => _SAVE_KEYS.includes(k)) : _SAVE_KEYS;
  _changedKeys.clear();
  for (const k of keysToCheck) {
    const val = k === 'brandMatrix' ? (state.brandMatrix || [])
              : k === 'visualTraining' ? (state.visualTraining || [])
              : state[k];
    if (DB_LARGE_KEYS.has(k)) {
      // ── PERF (primary mobile-freeze fix) ────────────────────────────────────
      // Large keys are persisted to IndexedDB via structured clone. DB.set IGNORES
      // the pre-stringified `preStr` argument for these keys entirely — so the old
      // exact-hash branch was doing JSON.stringify() over multi-MB `sales`, `orders`,
      // `wholesaleOrders` and `activityLog` arrays and then running _hashStr's
      // per-character loop across the result, SYNCHRONOUSLY on the main thread, on
      // every one of the ~157 save() call sites. A 5 MB sales blob = 5 million loop
      // iterations per key per save. That was the multi-second hang on the phone.
      //
      // These keys now always take the coalesced deferred write instead: no stringify,
      // no hash, no main-thread cost beyond a map assign and a timer reset. The write
      // is unconditional, which is strictly SAFER than the fingerprint heuristic it
      // replaces for parts/visualTraining — a mid-array field edit can no longer be
      // missed. _scheduleDeferredWrite resets its 1200ms timer on every call, so a
      // burst of saves still collapses into a single persist once activity settles.
      _keyLastSaved[k] = Date.now();
      _scheduleDeferredWrite(k, val);
      _fbMarkDirty(k);
      // Full parts blob push only before per-doc migration; after that parts/{id} is canonical
      if (k === 'parts' && !_partsUsePerDocStore()) window._partsChangedThisSave = true;
    } else {
      // Small keys (settings, customers, staff, suppliers…) stay on exact detection:
      // stringify once, hash-compare, reuse the string for the localStorage write.
      // These are kilobytes, not megabytes, so the exact check is genuinely cheap.
      const s = JSON.stringify(val);
      const fp = _hashStr(s);
      if (explicitlyMarked.has(k) || _saveFingerprints[k] !== fp) {
        DB.set(k, val, s);
        _saveFingerprints[k] = fp;
        _fbMarkDirty(k);
        // Track when this key was last written locally — used for timestamp-based sync decisions
        _keyLastSaved[k] = Date.now();
      }
    }
  }
  checkBackupReminder();
  // Whole-state snapshot push — immediate for parts changes, debounced for everything else
  clearTimeout(window._fbSaveTimer);
  if (window._partsChangedThisSave) {
    window._partsChangedThisSave = false;
    // Mark parts as pending cloud push — cleared only after Firestore confirms
    // Survives page crash: on next boot we detect this and re-push
    try { localStorage.setItem('_partsPushPending', '1'); } catch(e) { /* crash-guard flag — survivable if storage full */ }
    if (window.firebasePush) {
      window._lastPartsPushPromise = window.firebasePush(state);
      window._lastPartsPushPromise.then(function() {
        try { localStorage.removeItem('_partsPushPending'); } catch(e) { /* flag cleanup — non-critical */ }
      }).catch(function() {
        // Push failed — flag stays set, will retry on next open
      });
    }
  } else {
    window._fbSaveTimer = setTimeout(() => {
      if (window.firebasePush) window.firebasePush(state).catch(function(e) {
        // First push failed — schedule one retry in 8s so transient network blips don't lose state
        console.warn('[save] firebasePush debounce failed, scheduling retry:', e.message);
        if (typeof RX !== 'undefined') RX.log('warn','save','debounce push failed, retrying: '+e.message);
        setTimeout(function() {
          if (window.firebasePush && window._firebaseAuthed) window.firebasePush(state).catch(function(e2) {
            console.warn('[save] firebasePush retry also failed:', e2.message);
            if (typeof RX !== 'undefined') RX.log('error','save','push retry failed: '+e2.message);
          });
        }, 8000);
      });
    }, 1200);
  }
  // Persist a draft so refresh restores cart & UI state
  try { RX && RX.saveDraft('cart', { cart: state.cart, payMode: state.payMode, woCart: state.woCart, quoteCart: state.quoteCart, ts: Date.now() }); } catch(e) { /* cart draft — non-critical, cart is already in state */ }
  // Per-record outbox writes (PRIMARY save path — survives crashes/network).
  // Returns a promise so callers (completeSale, saveWholesaleOrder) can
  // deterministically await the IDB enqueue before they start verifying in cloud.
  try { return rxEnqueueChanged(); } catch(e) {
    console.warn('rxEnqueueChanged:', e);
    if (typeof RX !== 'undefined') RX.log('error','save','rxEnqueueChanged failed: ' + e.message);
    _errorCount++;
    _updateErrorBadge && _updateErrorBadge();
    // If IDB is unavailable (quota/private mode/Safari ITP), warn the user so they don't close the tab
    if (e && (e.name === 'QuotaExceededError' || e.name === 'InvalidStateError' || e.message.includes('IDB') || e.message.includes('IndexedDB'))) {
      toast('⚠️ Device storage issue — please keep this tab open until you see ✅ synced');
    }
    return Promise.resolve();
  }
}

// ============================================================
// UTILITIES
// ============================================================
function uid() { return Date.now().toString(36) + Math.random().toString(36).slice(2, 8); }
function rs(n) { return '₹' + Math.round(Number(n||0)).toLocaleString('en-IN'); }

// Debounced search for inventory — avoids re-rendering on every keystroke with 10,500 SKUs
let _invSearchTimer = null;
function invSearchDebounced() {
  clearTimeout(_invSearchTimer);
  _invSearchTimer = setTimeout(() => renderInventory(), window._isMobile ? 400 : 200);
}
function toast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 2500);
}
function todayStr() { return new Date().toISOString().slice(0,10); }
function isToday(d) { return d.slice(0,10) === todayStr(); }
function isThisMonth(d) { return d.slice(0,7) === todayStr().slice(0,7); }
function daysAgo(date) { return Math.floor((Date.now() - new Date(date).getTime()) / 86400000); }

// Per-part Firestore docs (parts/{id}) — skip slow whole-array blob push when using this path
function _partsUsePerDocStore() {
  try { if (localStorage.getItem('_partsDocMigrated') === '2') return true; } catch(e) { /* migration flag read — fall through to check */ }
  return !!window.firestoreSetPart;
}

// Pending parts cloud queue — retries failed/offline firestoreSetPart (like RX outbox for SKUs)
const _PARTS_PENDING_KEY = '_partsCloudPending_v1';
let _partsPendingDraining = false;

function _partsPendingLoad() {
  try { return JSON.parse(localStorage.getItem(_PARTS_PENDING_KEY) || '{}'); }
  catch (e) { return {}; }
}
function _partsPendingSave(map) {
  try {
    if (!map || !Object.keys(map).length) localStorage.removeItem(_PARTS_PENDING_KEY);
    else localStorage.setItem(_PARTS_PENDING_KEY, JSON.stringify(map));
  } catch (e) { console.warn('_partsPendingSave:', e.message); }
}
function partsPendingCount() {
  return Object.keys(_partsPendingLoad()).length;
}
// Records that have failed 20+ times are marked `failed` and retried only every 5 min.
// They were still counted as "syncing", so one permanently-bad record (oversized doc,
// bad field) held the pill orange forever and hid whether real sync was healthy.
// Split them out: active work stays on the pill, dead ones surface as an error.
function partsPendingActiveCount() {
  const m = _partsPendingLoad();
  return Object.keys(m).filter(function(k){ return !m[k].failed; }).length;
}
function partsPendingFailedCount() {
  const m = _partsPendingLoad();
  return Object.keys(m).filter(function(k){ return !!m[k].failed; }).length;
}
window.partsPendingActiveCount = partsPendingActiveCount;
window.partsPendingFailedCount = partsPendingFailedCount;

// ── FAILED / STUCK PARTS REPORT ─────────────────────────────────────────────
// Resolves each queued part id back to a real part so you get partNo + name +
// brand + rack instead of an opaque internal id. Split into two buckets:
//   FAILED   — 20+ attempts, effectively dead, needs manual attention
//   RETRYING — still inside the backoff schedule, will likely clear on its own
// A queued id whose part no longer exists locally is reported separately: that
// record is gone from this device and is the one case where re-entering is the
// only recovery.
function partsPendingReport() {
  const map = _partsPendingLoad();
  const failed = [], retrying = [], orphaned = [];
  Object.keys(map).forEach(function(id) {
    const meta = map[id] || {};
    const p = (typeof getPartById === 'function') ? getPartById(id) : null;
    const row = {
      id: id,
      partNo: p ? (p.partNo || '') : '',
      name: p ? (p.name || '') : '',
      brand: p ? (p.brand || '') : '',
      machineType: p ? (p.machineType || '') : '',
      location: p ? (p.location || '') : '',
      stock: p ? p.stock : '',
      instantPrice: p ? p.instantPrice : '',
      creditPrice: p ? p.creditPrice : '',
      mrp: p ? p.mrp : '',
      attempts: meta.attempts || 0,
      lastError: meta.lastError || '',
      enqueuedAt: meta.enqueuedAt || 0,
      exists: !!p
    };
    if (!p) orphaned.push(row);
    else if (meta.failed || (meta.attempts || 0) >= 20) failed.push(row);
    else retrying.push(row);
  });
  const byOldest = function(a, b) { return (a.enqueuedAt || 0) - (b.enqueuedAt || 0); };
  return { failed: failed.sort(byOldest), retrying: retrying.sort(byOldest), orphaned: orphaned.sort(byOldest) };
}
window.partsPendingReport = partsPendingReport;

function _ppRow(r, showErr) {
  const esc = (typeof escapeHtml === 'function') ? escapeHtml : function(x){ return String(x == null ? '' : x); };
  const title = r.exists
    ? '<b>' + esc(r.partNo || '(no part no)') + '</b> — ' + esc(r.name || '(unnamed)')
    : '<b style="color:#dc2626">Missing locally</b> <span style="font-family:monospace;font-size:11px;">' + esc(r.id) + '</span>';
  const sub = r.exists
    ? [r.brand, r.machineType, r.location ? '📍 ' + r.location : '', (r.stock === '' ? '' : r.stock + ' in stock')]
        .filter(Boolean).map(esc).join(' · ')
    : 'This part is not on this device — re-enter it from the original bill/box.';
  const err = (showErr && r.lastError)
    ? '<div style="font-size:11px;color:#b91c1c;margin-top:3px;word-break:break-word;">' + esc(r.lastError) + ' · ' + r.attempts + ' attempt(s)</div>'
    : '';
  const click = r.exists ? ' onclick="closeModal();openPartDetail(\'' + esc(r.id) + '\')" style="cursor:pointer;"' : '';
  return '<div' + click + ' style="padding:8px 10px;border-bottom:1px solid var(--border);">'
    + '<div style="font-size:13px;">' + title + '</div>'
    + (sub ? '<div style="font-size:11px;color:var(--muted);margin-top:2px;">' + sub + '</div>' : '')
    + err + '</div>';
}

function failedPartsUI() {
  const rep = partsPendingReport();
  const total = rep.failed.length + rep.retrying.length + rep.orphaned.length;
  if (!total) {
    return showModal('☁ Parts Sync Queue',
      '<div style="text-align:center;padding:32px 16px;color:#16a34a;">'
      + '<div style="font-size:32px;margin-bottom:8px;">✅</div>'
      + '<div style="font-size:14px;font-weight:600;">Every part is on the database</div>'
      + '<div style="font-size:12px;color:var(--muted);margin-top:6px;">Nothing is waiting to sync.</div></div>');
  }
  const section = function(title, rows, color, note, showErr) {
    if (!rows.length) return '';
    return '<div style="margin-bottom:12px;">'
      + '<h3 style="font-size:13px;font-weight:700;margin:0 0 4px;color:' + color + ';">' + title + ' (' + rows.length + ')</h3>'
      + (note ? '<div style="font-size:11px;color:var(--muted);margin-bottom:6px;">' + note + '</div>' : '')
      + '<div style="background:white;border:1px solid var(--border);border-radius:10px;max-height:280px;overflow-y:auto;">'
      + rows.map(function(r){ return _ppRow(r, showErr); }).join('')
      + '</div></div>';
  };
  const html = ''
    + section('❌ Failed', rep.failed, '#b91c1c',
        'Gave up after 20 tries. Tap a row to open the part and check it, or fix the underlying error and retry.', true)
    + section('⚠️ Missing on this device', rep.orphaned, '#b45309',
        'Queued but no longer in local inventory. These are the ones to re-enter.', true)
    + section('⏳ Still retrying', rep.retrying, '#0f766e',
        'Inside the normal backoff schedule — these usually clear on their own.', false)
    + '<div style="display:flex;flex-direction:column;gap:8px;margin-top:6px;">'
      + '<button class="btn" onclick="drainPartsCloudPending().then(function(){failedPartsUI();});toast(&quot;Retrying…&quot;);" style="background:#0f766e;">🔁 Retry all now</button>'
      + '<button class="btn" onclick="downloadFailedPartsCSV()" style="background:#475569;">⬇ Download list as CSV</button>'
      + (rep.failed.length ? '<button class="btn btn-outline" onclick="clearFailedPartsQueue()">🗑 Clear the ' + rep.failed.length + ' failed entr(ies) from the queue</button>' : '')
    + '</div>';
  showModal('☁ Parts Sync Queue (' + total + ')', html);
}
window.failedPartsUI = failedPartsUI;

// Full detail export — everything needed to re-enter a part by hand.
function downloadFailedPartsCSV() {
  const rep = partsPendingReport();
  const rows = [['Status','Part No','Name','Brand','Machine','Rack','Stock','Instant','Credit','MRP','Attempts','Last Error','Internal ID']];
  const push = function(status, list) {
    list.forEach(function(r) {
      rows.push([status, r.partNo, r.name, r.brand, r.machineType, r.location, r.stock,
                 r.instantPrice, r.creditPrice, r.mrp, r.attempts, r.lastError, r.id]);
    });
  };
  push('FAILED', rep.failed);
  push('MISSING LOCALLY', rep.orphaned);
  push('RETRYING', rep.retrying);
  if (rows.length === 1) return toast('Nothing pending — queue is empty');
  downloadCSV(rows, 'parts-sync-failed-' + new Date().toISOString().slice(0,10) + '.csv');
  toast('✅ Exported ' + (rows.length - 1) + ' row(s)');
}
window.downloadFailedPartsCSV = downloadFailedPartsCSV;

// Removes ONLY the dead entries. The parts themselves are untouched in inventory —
// this just stops a permanently-failing record from sitting in the retry queue.
function clearFailedPartsQueue() {
  const rep = partsPendingReport();
  const dead = rep.failed.concat(rep.orphaned);
  if (!dead.length) return toast('No failed entries to clear');
  if (!confirm('Remove ' + dead.length + ' failed entr(ies) from the sync queue?\n\nYour parts are NOT deleted — this only stops the retries. Download the CSV first if you have not already.')) return;
  const map = _partsPendingLoad();
  dead.forEach(function(r){ delete map[r.id]; });
  _partsPendingSave(map);
  if (typeof renderRxStatusBar === 'function') renderRxStatusBar();
  toast('✅ Cleared ' + dead.length + ' from queue');
  failedPartsUI();
}
window.clearFailedPartsQueue = clearFailedPartsQueue;
function partsPendingEnqueue(partId, reason) {
  if (!partId) return;
  const map = _partsPendingLoad();
  const prev = map[partId] || {};
  map[partId] = {
    attempts: prev.attempts || 0,
    enqueuedAt: prev.enqueuedAt || Date.now(),
    nextAttemptAt: Date.now(),
    lastError: reason || prev.lastError || null
  };
  _partsPendingSave(map);
}
function partsPendingRemove(partId) {
  if (!partId) return;
  const map = _partsPendingLoad();
  if (!map[partId]) return;
  delete map[partId];
  _partsPendingSave(map);
}
function _partsPendingBackoffMs(attempts) {
  const s = [2000, 5000, 15000, 60000, 300000];
  return s[Math.min(attempts, s.length - 1)];
}
// ── Durable part-photo upload queue ──────────────────────────────────────────
// INVARIANT: any path that strips a base64 photo to '__pending_upload__' for a
// cloud write MUST guarantee the JPG actually reaches Storage. Previously only the
// Add/Edit form uploaded; every other write path stripped without uploading, which
// orphaned the placeholder (cloud doc said __pending_upload__ with NO JPG behind it)
// so self-healing could never recover it. This queue closes that gap: the base64 is
// parked in IndexedDB (too large for localStorage) and retried with backoff until
// Storage accepts it, then the real https URL is written back to state + cloud.
const _PHOTO_Q_KEY = '_photoUploadQueue_v1'; // localStorage: { [partId]: {attempts,nextAttemptAt} } — tiny metadata only
let _photoQDraining = false;
function _photoQLoad() { try { return JSON.parse(localStorage.getItem(_PHOTO_Q_KEY) || '{}'); } catch(e) { return {}; } }
function _photoQSave(map) {
  try {
    if (!map || !Object.keys(map).length) localStorage.removeItem(_PHOTO_Q_KEY);
    else localStorage.setItem(_PHOTO_Q_KEY, JSON.stringify(map));
  } catch(e) { console.warn('_photoQSave:', e.message); }
}
function photoQueueCount() { return Object.keys(_photoQLoad()).length; }
// Park the base64 durably (IDB) and register it for upload. Idempotent per partId.
function photoUploadEnqueue(partId, dataUrl) {
  if (!partId || !dataUrl || typeof dataUrl !== 'string' || !dataUrl.startsWith('data:')) return;
  try { if (typeof RX !== 'undefined' && RX.put) RX.put('keystore', { id: 'photoq_' + partId, value: { dataUrl: dataUrl } }); }
  catch(e) { console.warn('photoUploadEnqueue IDB:', e && e.message); }
  const map = _photoQLoad();
  const prev = map[partId] || {};
  map[partId] = { attempts: prev.attempts || 0, nextAttemptAt: Date.now(), enqueuedAt: prev.enqueuedAt || Date.now() };
  _photoQSave(map);
  if (navigator.onLine && window._firebaseAuthed) setTimeout(function(){ drainPhotoUploadQueue(); }, 0);
}
async function _photoQReadPayload(partId) {
  try {
    const rec = (typeof RX !== 'undefined' && RX.get) ? await RX.get('keystore', 'photoq_' + partId) : null;
    return rec && rec.value && rec.value.dataUrl ? rec.value.dataUrl : null;
  } catch(e) { return null; }
}
function _photoQDeletePayload(partId) {
  try { if (typeof RX !== 'undefined' && RX.del) RX.del('keystore', 'photoq_' + partId); } catch(e){}
}
async function drainPhotoUploadQueue() {
  if (_photoQDraining) return;
  if (!navigator.onLine || !window._firebaseAuthed || !window.firebaseUploadPartPhoto) return;
  _photoQDraining = true;
  try {
    const map = _photoQLoad();
    const now = Date.now();
    for (const partId of Object.keys(map)) {
      const job = map[partId] || {};
      if ((job.nextAttemptAt || 0) > now) continue;
      const dataUrl = await _photoQReadPayload(partId);
      if (!dataUrl) { delete map[partId]; continue; } // payload gone — nothing to upload
      try {
        // If a prior attempt already uploaded the JPG (Storage ok) but the cloud
        // writeback was dropped, the part already holds the https URL — skip the
        // redundant re-upload and just retry the writeback.
        const p = (typeof getPartById === 'function') ? getPartById(partId) : null;
        let url;
        if (p && typeof p.photoDataUrl === 'string' && p.photoDataUrl.indexOf('https://') === 0) {
          url = p.photoDataUrl;
        } else {
          url = await window.firebaseUploadPartPhoto(partId, dataUrl);   // JPG → Storage (deterministic path, idempotent)
          if (p) {
            p.photoDataUrl = url;
            try { if (typeof markChanged === 'function') markChanged('parts'); } catch(e){}
            try { if (typeof save === 'function') save(); } catch(e){}
          }
        }

        // ── CONFIRM the URL lands in the cloud doc BEFORE marking the job done ──
        // OLD BUG (the "heal's push is dropped before it writes"): the writeback was
        // fire-and-forget and the payload was deleted immediately after. If auth
        // flickered, firestoreUpdatePartFields() hit its `if(!authed) return` guard,
        // no-opped silently, and the job was gone — JPG in Storage but the doc still
        // said __pending_upload__, with nothing left to retry. FIX: AWAIT a
        // field-level write (never a full-part push — two-stream safe) and only clear
        // the job when it provably landed while authed throughout. If auth wasn't
        // stable, keep the job so the next drain retries (direct write already
        // confirmed to land, so a stable retry will succeed).
        let cloudOk = false;
        if (window._firebaseAuthed && window.firestoreUpdatePartFields) {
          try {
            await window.firestoreUpdatePartFields(partId, { photoDataUrl: url });
            cloudOk = !!window._firebaseAuthed; // flickered false during the call ⇒ guard may have no-opped ⇒ don't trust it
          } catch(e) {
            console.warn('[photoQ] cloud writeback failed for', partId, e && e.message);
          }
        }

        if (cloudOk) {
          _photoQDeletePayload(partId);
          delete map[partId];
        } else {
          job.attempts = (job.attempts || 0) + 1;
          job.nextAttemptAt = now + _partsPendingBackoffMs(job.attempts);
          job.lastError = 'cloud_writeback_not_confirmed_retrying';
          map[partId] = job;   // keep base64 payload + job; the JPG is safe in Storage, only the doc link is pending
          console.warn('[photoQ] storage ok but cloud writeback not confirmed for', partId, '— will retry');
        }
      } catch(e) {
        job.attempts = (job.attempts || 0) + 1;
        job.nextAttemptAt = now + _partsPendingBackoffMs(job.attempts);
        job.lastError = (e && e.message) || 'upload_failed';
        map[partId] = job;
        console.warn('[photoQ] upload failed for', partId, job.lastError);
      }
    }
    _photoQSave(map);
  } finally { _photoQDraining = false; }
}
window.photoUploadEnqueue = photoUploadEnqueue;
window.drainPhotoUploadQueue = drainPhotoUploadQueue;
window.photoQueueCount = photoQueueCount;

function _partPayloadForCloud(part) {
  if (!part) return null;
  const p = Object.assign({}, part);
  if (p.photoDataUrl && p.photoDataUrl.startsWith('data:')) {
    photoUploadEnqueue(part.id, p.photoDataUrl); // guarantee the JPG reaches Storage before we orphan the placeholder
    p.photoDataUrl = '__pending_upload__';
  }
  return p;
}

async function drainPartsCloudPending() {
  if (_partsPendingDraining) return { synced: 0, remaining: partsPendingCount() };
  if (!navigator.onLine || !window._firebaseAuthed || !window.firestoreSetPart) {
    return { synced: 0, remaining: partsPendingCount() };
  }
  const map = _partsPendingLoad();
  const ids = Object.keys(map);
  if (!ids.length) return { synced: 0, remaining: 0 };
  _partsPendingDraining = true;
  let synced = 0;
  const now = Date.now();
  // Track outcomes locally and write ONCE at the end. Previously each success called
  // partsPendingRemove(), which re-read and re-wrote the whole localStorage map — 25
  // read-modify-write cycles per drain — and then the final _partsPendingSave(map)
  // wrote a snapshot taken BEFORE the drain, silently discarding anything enqueued
  // while it was running (a genuinely lost cloud write).
  const confirmed = new Set();  // written to Firestore, or the part no longer exists
  const retried = {};           // partId -> updated meta after a failure
  try {
    const ready = ids
      .filter(function(id) { return (map[id].nextAttemptAt || 0) <= now; })
      .sort(function(a, b) { return (map[a].enqueuedAt || 0) - (map[b].enqueuedAt || 0); });
    const BATCH = 25;
    for (let i = 0; i < Math.min(ready.length, BATCH); i++) {
      const partId = ready[i];
      const meta = Object.assign({}, map[partId]);
      const part = getPartById(partId);
      if (!part) { confirmed.add(partId); continue; } // part was deleted locally — nothing to write
      try {
        await window.firestoreSetPart(_partPayloadForCloud(part));
        confirmed.add(partId);
        synced++;
      } catch (e) {
        meta.attempts = (meta.attempts || 0) + 1;
        meta.lastError = (e && e.message) || String(e);
        meta.nextAttemptAt = now + _partsPendingBackoffMs(meta.attempts);
        if (meta.attempts >= 20) meta.failed = true;
        retried[partId] = meta;
        if (typeof RX !== 'undefined') {
          RX.log(meta.attempts >= 20 ? 'critical' : 'warn', 'parts-pending',
            'part ' + partId + ' attempt ' + meta.attempts + ': ' + meta.lastError);
        }
      }
    }
    // Single write, against a FRESH read so mid-drain enqueues survive.
    const fresh = _partsPendingLoad();
    Object.keys(retried).forEach(function(id){ fresh[id] = retried[id]; });
    confirmed.forEach(function(id){ delete fresh[id]; });
    _partsPendingSave(fresh);
    if (synced > 0) {
      console.log('[parts-pending] synced', synced, '\u2014', Object.keys(fresh).length, 'remaining');
    }
  } finally {
    _partsPendingDraining = false;
    if (typeof renderRxStatusBar === 'function') setTimeout(function() { renderRxStatusBar(); }, 30);
  }
  return { synced: synced, remaining: partsPendingCount() };
}
window.drainPartsCloudPending = drainPartsCloudPending;
window.partsPendingCount = partsPendingCount;

// Cloud-first part write. Always returns ok:true so callers still save locally (data never vanishes).
async function cloudPersistPart(part) {
  if (!part || !part.id) return { ok: false, error: 'no_part' };
  if (window.firestoreSetPart && window._firebaseAuthed) {
    try {
      await window.firestoreSetPart(_partPayloadForCloud(part));
      partsPendingRemove(part.id);
      return { ok: true, cloud: true };
    } catch (e) {
      console.warn('cloudPersistPart:', e.message);
      partsPendingEnqueue(part.id, e.message);
      drainPartsCloudPending();
      return { ok: true, cloud: false, error: e.message, queued: true };
    }
  }
  partsPendingEnqueue(part.id, 'offline');
  return { ok: true, cloud: false, offline: true, queued: true };
}

async function cloudPersistPartFields(partId, fields) {
  if (!partId || !fields || !Object.keys(fields).length) return { ok: true, cloud: false };
  const patch = Object.assign({}, fields);
  if (!patch.updatedAt) patch.updatedAt = Date.now();
  const p = getPartById(partId);
  if (window.firestoreUpdatePartFields && window._firebaseAuthed) {
    try {
      await window.firestoreUpdatePartFields(partId, patch);
      partsPendingRemove(partId);
      return { ok: true, cloud: true };
    } catch (e) {
      console.warn('cloudPersistPartFields:', e.message);
    }
  }
  if (p && window.firestoreSetPart && window._firebaseAuthed) {
    try {
      await window.firestoreSetPart(_partPayloadForCloud(Object.assign({}, p, patch)));
      partsPendingRemove(partId);
      return { ok: true, cloud: true };
    } catch (e) {
      console.warn('cloudPersistPartFields fallback:', e.message);
      partsPendingEnqueue(partId, e.message);
      drainPartsCloudPending();
      return { ok: true, cloud: false, error: e.message, queued: true };
    }
  }
  partsPendingEnqueue(partId, 'offline');
  return { ok: true, cloud: false, offline: true, queued: true };
}

function _partSaveToast(cloud, okLabel) {
  if (cloud.cloud) return toast(okLabel);
  if (cloud.offline) return toast('✅ Saved on this device (offline — will sync when online)');
  if (cloud.queued) return toast('✅ Saved on this device — queued for cloud sync');
  return toast('✅ Saved on this device — cloud sync pending, will retry');
}

setInterval(function() { drainPartsCloudPending(); }, window._isMobile ? 6000 : 2500);
window.addEventListener('online', function() { drainPartsCloudPending(); });

// ── PHOTO QUEUE: periodic drain + backlog recovery ──────────────────────────
// The durable photo-upload queue previously only drained on a NEW enqueue or a
// page reload, so a single failed upload could sit for a long time. This timer
// guarantees the queue clears on its own whenever the device is back online.
setInterval(function(){ try { if (window.drainPhotoUploadQueue) window.drainPhotoUploadQueue(); } catch(e){} }, window._isMobile ? 8000 : 4000);
window.addEventListener('online', function(){ try { if (window.drainPhotoUploadQueue) window.drainPhotoUploadQueue(); } catch(e){} });

// Recover the EXISTING backlog: any part whose photoDataUrl is still raw base64
// never reached Storage. Re-enqueue them so the worker uploads each one and swaps
// in the real https URL. This both restores the missing cloud photos AND shrinks
// the parts blob (the leftover base64 is what was making saves lag). Idempotent,
// so it is safe to run repeatedly. MUST run on the device that captured the photo
// (the base64 only exists there; the cloud doc just has a __pending_upload__ stub).
window._recoverLocalBase64Photos = function() {
  try {
    if (!window.state || !Array.isArray(window.state.parts)) return 0;
    if (!navigator.onLine || !window._firebaseAuthed || !window.photoUploadEnqueue) return 0;
    var n = 0;
    window.state.parts.forEach(function(p){
      if (p && typeof p.photoDataUrl === 'string' && p.photoDataUrl.startsWith('data:')) {
        window.photoUploadEnqueue(p.id, p.photoDataUrl); n++;
      }
    });
    if (n && window.drainPhotoUploadQueue) window.drainPhotoUploadQueue();
    if (n) console.info('[photoRecovery] re-enqueued ' + n + ' stuck base64 photo(s) for upload');
    return n;
  } catch(e) { console.warn('[photoRecovery]', e && e.message); return 0; }
};
// Run after state + auth are ready, then again as a safety net.
setTimeout(function(){ try { window._recoverLocalBase64Photos(); } catch(e){} }, 8000);
setTimeout(function(){ try { window._recoverLocalBase64Photos(); } catch(e){} }, 30000);

// ── Visible pending-photo counter ────────────────────────────────────────────
// A small floating pill so staff/owner can WATCH the backlog drain in real time
// instead of opening the console. Counts parts whose photo is still raw base64
// on THIS device (i.e. not yet uploaded to Storage). Auto-hides at zero after a
// brief "all uploaded" confirmation. Tap it to force an immediate retry.
(function(){
  function _pendingPhotoCount() {
    try {
      if (!window.state || !Array.isArray(window.state.parts)) return 0;
      var n = 0, a = window.state.parts;
      for (var i = 0; i < a.length; i++) {
        var v = a[i] && a[i].photoDataUrl;
        if (typeof v === 'string' && v.indexOf('data:') === 0) n++;
      }
      return n;
    } catch(e) { return 0; }
  }
  window.pendingPhotoCount = _pendingPhotoCount;

  function _ensureBadge() {
    var el = document.getElementById('photo-pending-badge');
    if (el) return el;
    if (!document.body) return null;
    el = document.createElement('div');
    el.id = 'photo-pending-badge';
    // POSITION FIX: was bottom:12px, which floated the pill directly over the home
    // tab's sales cards (and the bottom nav) on mobile. Raised above the nav bar
    // (~60px + safe-area) so it never covers tab content or the nav itself.
    el.style.cssText = 'position:fixed;left:12px;bottom:calc(72px + env(safe-area-inset-bottom));z-index:9999;display:none;'
      + 'align-items:center;gap:8px;background:var(--primary,#2563eb);color:#fff;'
      + 'padding:8px 13px;border-radius:999px;font-size:13px;font-weight:600;'
      + 'box-shadow:0 3px 12px rgba(0,0,0,.25);cursor:pointer;font-family:inherit;max-width:82vw;';
    el.title = 'Tap to retry photo uploads now';
    el.onclick = function(){
      try { if (window._recoverLocalBase64Photos) window._recoverLocalBase64Photos(); } catch(e){}
      try { if (window.drainPhotoUploadQueue) window.drainPhotoUploadQueue(); } catch(e){}
      _render(true);
    };
    document.body.appendChild(el);
    return el;
  }

  var _wasPending = false, _hideTimer = null, _lastShownCount = -1;
  function _render(forceTick) {
    var el = _ensureBadge();
    if (!el) return;
    var n = _pendingPhotoCount();
    if (n > 0) {
      _wasPending = true;
      // BEHAVIOR FIX: the 3s interval used to keep this pill visible FOREVER while
      // any photo was stuck pending — permanently covering the sales section on the
      // home tab and "flashing" on every re-render. Now it only pops up when the
      // pending count CHANGES (new photo queued / one uploaded) or on explicit tap,
      // shows for 6 seconds, then hides. Uploads keep running in the background
      // regardless — the pill is a notification, not a status monitor.
      if (n !== _lastShownCount || forceTick) {
        _lastShownCount = n;
        var offline = (typeof navigator !== 'undefined' && navigator.onLine === false);
        el.innerHTML = '📷 ' + n + ' photo' + (n === 1 ? '' : 's') + ' pending'
          + (offline ? ' · offline' : ' · uploading…');
        el.style.background = offline ? 'var(--muted,#6b7280)' : 'var(--primary,#2563eb)';
        el.style.display = 'flex';
        if (_hideTimer) clearTimeout(_hideTimer);
        _hideTimer = setTimeout(function(){ el.style.display = 'none'; }, 6000);
      }
      // same count, no tap → stay hidden (or finish the current 6s window)
    } else if (_wasPending) {
      _wasPending = false;
      _lastShownCount = 0;
      el.innerHTML = '✅ All photos uploaded';
      el.style.background = 'var(--success,#16a34a)';
      el.style.display = 'flex';
      if (_hideTimer) clearTimeout(_hideTimer);
      _hideTimer = setTimeout(function(){ el.style.display = 'none'; }, 4000);
    } else {
      el.style.display = 'none';
    }
  }
  window.renderPhotoPendingBadge = _render;

  setTimeout(function(){ try { _render(false); } catch(e){} }, 1500);
  // Skip while backgrounded — this was the one interval with no hidden-guard, so it kept
  // repainting (and waking the phone's main thread) for as long as the tab stayed open.
  setInterval(function(){ if (document.hidden) return; try { _render(false); } catch(e){} }, window._isMobile ? 6000 : 3000);
})();

// Seed — only called after Firestore confirms cloud is empty
// Self-heal nextInvoice counter on boot — prevents duplicate invoice numbers
// if the app crashed after pushing the sale but before incrementing the counter.
function syncNextInvoice() {
  if (!state.sales || !state.sales.length) return;
  var maxNo = state.sales.reduce(function(m, s) {
    var n = parseInt((s.invoiceNo || '').replace(/\D/g, '')) || 0;
    return n > m ? n : m;
  }, 0);
  if (maxNo >= state.settings.nextInvoice) {
    state.settings.nextInvoice = maxNo + 1;
    DB.set('settings', state.settings);
  }
}

function seedSampleDataIfEmpty() {
  if (state.parts.length > 0) return;
  {
  const baseDate = new Date(); baseDate.setDate(baseDate.getDate()-30);
  state.parts = [
    {id:uid(), partNo:'5165090', name:'Hydraulic Pump Assembly', machineType:'tractor', brand:'Mahindra', models:'575 DI, 475 DI', category:'Hydraulic', costPrice:8500, mrp:12500, instantPrice:11000, creditPrice:11500, stock:8, minStock:3, supplierName:'Singh Auto Parts', supplierPhone:'919876543210', location:'A-12'},
    {id:uid(), partNo:'AL110864', name:'Clutch Plate', machineType:'tractor', brand:'John Deere', models:'5036, 5042, 5050', category:'Clutch', costPrice:2200, mrp:3500, instantPrice:3000, creditPrice:3200, stock:15, minStock:5, supplierName:'Tractor Spares Ludhiana', supplierPhone:'919876512345', location:'B-04'},
    {id:uid(), partNo:'JCB332/D2275', name:'Fuel Filter Element', machineType:'jcb', brand:'JCB', models:'3DX, 4DX', category:'Filter', costPrice:450, mrp:850, instantPrice:700, creditPrice:750, stock:2, minStock:10, supplierName:'JCB Parts Hub', supplierPhone:'918765432101', location:'C-07'},
    {id:uid(), partNo:'RT-BL-540', name:'Rotavator Blade L-Type 540mm', machineType:'rotavator', brand:'Sonalika', models:'Universal 6ft', category:'Blades', costPrice:180, mrp:320, instantPrice:260, creditPrice:280, stock:120, minStock:50, supplierName:'Punjab Krishi Yantra', supplierPhone:'919812345678', location:'D-01'},
    {id:uid(), partNo:'HRV-CHAIN-2.5', name:'Harvester Feed Chain 2.5m', machineType:'harvester', brand:'Class', models:'Crop Tiger 30', category:'Chain', costPrice:3200, mrp:5000, instantPrice:4400, creditPrice:4600, stock:0, minStock:4, supplierName:'Harvest Pro', supplierPhone:'917896543210', location:'E-03'},
    {id:uid(), partNo:'BRG-6205', name:'Ball Bearing 6205 ZZ', machineType:'tractor', brand:'SKF', models:'Universal', category:'Bearing', costPrice:85, mrp:160, instantPrice:130, creditPrice:140, stock:200, minStock:30, supplierName:'Singh Auto Parts', supplierPhone:'919876543210', location:'A-22'},
    {id:uid(), partNo:'OF-MS-571', name:'Engine Oil Filter', machineType:'tractor', brand:'Massey Ferguson', models:'1035, 1035 DI', category:'Filter', costPrice:220, mrp:400, instantPrice:340, creditPrice:360, stock:5, minStock:8, supplierName:'Tractor Spares Ludhiana', supplierPhone:'919876512345', location:'B-12'},
    {id:uid(), partNo:'JCB-BUSH-23', name:'JCB Bucket Pin Bush', machineType:'jcb', brand:'JCB', models:'3DX Super', category:'Bush/Pin', costPrice:1100, mrp:1800, instantPrice:1550, creditPrice:1650, stock:6, minStock:4, supplierName:'JCB Parts Hub', supplierPhone:'918765432101', location:'C-01'},
    {id:uid(), partNo:'OLD-RT-440', name:'Old Rotavator Blade 440mm', machineType:'rotavator', brand:'Generic', models:'Discontinued', category:'Blades', costPrice:120, mrp:200, instantPrice:180, creditPrice:185, stock:35, minStock:0, supplierName:'Local Trader', supplierPhone:'919900112233', location:'Z-99'},
    {id:uid(), partNo:'ENGINE-OIL-15W40', name:'Engine Oil 15W-40 5L', machineType:'tractor', brand:'Castrol', models:'Universal', category:'Lubricant', costPrice:1450, mrp:2200, instantPrice:1900, creditPrice:2000, stock:28, minStock:10, supplierName:'Singh Auto Parts', supplierPhone:'919876543210', location:'H-02'}
  ];
  state.customers = [
    {id:uid(), name:'Gurmeet Singh (Khanna)', phone:'919876543210', type:'wholesale', trusted:true, creditDays:30, totalDue:0, notes:'Owns 3 tractors, regular 5+ years'},
    {id:uid(), name:'Krishi Seva Kendra', phone:'919812345670', type:'wholesale', trusted:true, creditDays:15, totalDue:8500, notes:'Sub-dealer'},
    {id:uid(), name:'Harpreet Auto Garage', phone:'919876512300', type:'wholesale', trusted:false, creditDays:0, totalDue:0, notes:'New customer'},
    {id:uid(), name:'Balwinder Farmer', phone:'917896543210', type:'retail', trusted:false, creditDays:0, totalDue:0, notes:''},
    {id:uid(), name:'Singh Brothers JCB Service', phone:'919822334455', type:'wholesale', trusted:true, creditDays:15, totalDue:0, notes:'JCB specialist'}
  ];
  // Seed historic sales for velocity calculations
  const seedSales = [
    {daysAgo:2, items:[{partNo:'5165090', qty:1}], custIdx:0, mode:'credit'},
    {daysAgo:4, items:[{partNo:'BRG-6205', qty:8}], custIdx:1, mode:'cash'},
    {daysAgo:5, items:[{partNo:'OF-MS-571', qty:3}], custIdx:3, mode:'cash'},
    {daysAgo:6, items:[{partNo:'JCB332/D2275', qty:4}], custIdx:4, mode:'credit'},
    {daysAgo:8, items:[{partNo:'RT-BL-540', qty:24}], custIdx:0, mode:'credit'},
    {daysAgo:9, items:[{partNo:'JCB-BUSH-23', qty:2}], custIdx:4, mode:'credit'},
    {daysAgo:11, items:[{partNo:'BRG-6205', qty:12}], custIdx:2, mode:'upi'},
    {daysAgo:12, items:[{partNo:'JCB332/D2275', qty:6}], custIdx:4, mode:'credit'},
    {daysAgo:14, items:[{partNo:'5165090', qty:1},{partNo:'BRG-6205', qty:4}], custIdx:0, mode:'credit'},
    {daysAgo:16, items:[{partNo:'OF-MS-571', qty:5}], custIdx:1, mode:'credit'},
    {daysAgo:18, items:[{partNo:'AL110864', qty:2}], custIdx:1, mode:'credit'},
    {daysAgo:20, items:[{partNo:'JCB332/D2275', qty:10}], custIdx:4, mode:'credit'},
    {daysAgo:22, items:[{partNo:'BRG-6205', qty:6}], custIdx:3, mode:'cash'},
    {daysAgo:24, items:[{partNo:'ENGINE-OIL-15W40', qty:4}], custIdx:0, mode:'credit'},
    {daysAgo:27, items:[{partNo:'5165090', qty:1}], custIdx:1, mode:'credit'}
  ];
  let invNo = 1000;
  seedSales.forEach(s => {
    const d = new Date(); d.setDate(d.getDate()-s.daysAgo);
    const cust = state.customers[s.custIdx];
    let total = 0;
    const items = s.items.map(it => {
      const p = state.parts.find(x => x.partNo === it.partNo);
      const price = (cust.trusted && s.mode==='credit') ? p.creditPrice : p.instantPrice;
      total += price * it.qty;
      return {partId:p.id, partNo:p.partNo, name:p.name, qty:it.qty, price};
    });
    state.sales.push({
      id:uid(), invoiceNo:'INV-'+(++invNo), date:d.toISOString(),
      customerId:cust.id, items, subtotal:total, discount:0, total,
      paymentMode:s.mode, settled: s.mode!=='credit'
    });
  });
  state.settings.nextInvoice = invNo + 1;
  save();
  }
} // end seedSampleDataIfEmpty

// ============================================================
// SMART ANALYTICS — the "AI" math layer
// ============================================================
// Velocity cache — rebuilt once when sales change, not per-part per-render
let _velocityCache = null;
let _velocityCacheSize = 0;
function _buildVelocityCache() {
  const cutoff = Date.now() - 30*86400000;
  const totals = {};
  state.sales.forEach(s => {
    if (new Date(s.date).getTime() < cutoff) return;
    s.items.forEach(it => { totals[it.partId] = (totals[it.partId]||0) + it.qty; });
  });
  _velocityCache = totals;
  _velocityCacheSize = state.sales.length;
}
function _getVelocity(partId) {
  if (!_velocityCache || _velocityCacheSize !== state.sales.length) _buildVelocityCache();
  return (_velocityCache[partId] || 0) / 30;
}
// Call this after any sale to invalidate
function invalidateVelocityCache() { _velocityCache = null; _partsIndex = null; _salesIndex = null; window._homeStatsCache = null; window._briefingCache = null; window._brainReorderCache = null; window._brainDeadCache = null; window._brainVelCache = null; }

// Saleable parts index — rebuilt when parts change, O(1) lookup by partNo/name/brand
let _partsIndex = null;
function _buildPartsIndex() {
  _partsIndex = { byId: {}, saleable: [] };
  state.parts.forEach(function(p) {
    _partsIndex.byId[p.id] = p;
    if (saleablePart(p)) _partsIndex.saleable.push(p);
  });
}
function getPartsIndex() {
  if (!_partsIndex) _buildPartsIndex();
  return _partsIndex;
}
function invalidatePartsIndex() { _partsIndex = null; _catCanonicalVersion = -1; }

// ── FAST O(1) LOOKUPS ──────────────────────────────────────────────────────
// getPartById: replaces all getPartById(id) — O(1) vs O(n)
function getPartById(id) { return getPartsIndex().byId[id] || null; }

// Customer index — rebuilt on demand, invalidated when customers change
let _customerIndex = null;
function _buildCustomerIndex() {
  _customerIndex = {};
  (state.customers || []).forEach(function(c) { _customerIndex[c.id] = c; });
}
function getCustomerById(id) {
  if (!_customerIndex) _buildCustomerIndex();
  return _customerIndex[id] || null;
}
function invalidateCustomerIndex() { _customerIndex = null; }

// Sales index — keyed by id, rebuilt when sales change
let _salesIndex = null;
function _buildSalesIndex() {
  _salesIndex = {};
  (state.sales || []).forEach(function(s) { _salesIndex[s.id] = s; });
}
function getSaleById(id) {
  if (!_salesIndex) _buildSalesIndex();
  return _salesIndex[id] || null;
}
function invalidateSalesIndex() { _salesIndex = null; }

const Smart = {
  // Sales velocity per part (units/day over last 30 days) — O(1) after cache build
  velocity(partId, days=30) {
    if (days === 30) return _getVelocity(partId);
    // Non-default days: compute directly (rare case)
    const cutoff = Date.now() - days*86400000;
    let total = 0;
    state.sales.forEach(s => {
      if (new Date(s.date).getTime() < cutoff) return;
      s.items.forEach(it => { if (it.partId === partId) total += it.qty; });
    });
    return total / days;
  },
  // Days until stockout based on velocity
  daysUntilStockout(part) {
    const v = this.velocity(part.id);
    if (v <= 0) return Infinity;
    return Math.floor(part.stock / v);
  },
  // Smart reorder qty — cover N days of demand
  smartReorderQty(part, coverDays=45) {
    const v = this.velocity(part.id);
    if (v <= 0) return Math.max(part.minStock || 0, 5);
    return Math.ceil(v * coverDays);
  },
  // Heat label
  heatLabel(part) {
    const v = this.velocity(part.id);
    if (v >= 0.5) return {cls:'hot', txt:'🔥 Hot'};
    if (v >= 0.1) return {cls:'warm', txt:'⚡ Active'};
    if (v > 0) return {cls:'cold', txt:'Slow'};
    return {cls:'cold', txt:'No sales 30d'};
  },
  // Dead stock — no sale in N days
  deadStock(days=90) {
    const cutoff = Date.now() - days*86400000;
    const sold = new Set();
    state.sales.forEach(s => {
      if (new Date(s.date).getTime() >= cutoff) {
        s.items.forEach(it => sold.add(it.partId));
      }
    });
    return state.parts.filter(p => saleablePart(p) && !sold.has(p.id) && p.stock > 0);
  },
  // Cross-sell: parts often bought together
  crossSell(partId, limit=3) {
    const cooccur = {};
    state.sales.forEach(s => {
      const partIds = s.items.map(i => i.partId);
      if (!partIds.includes(partId)) return;
      partIds.forEach(pid => {
        if (pid !== partId) cooccur[pid] = (cooccur[pid]||0) + 1;
      });
    });
    return Object.entries(cooccur).sort((a,b) => b[1]-a[1]).slice(0, limit).map(([pid]) => getPartById(pid)).filter(Boolean);
  },
  // Customers due for reorder — customers who buy a part periodically
  reorderDueCustomers() {
    const out = [];
    state.customers.forEach(c => {
      // Find parts they bought multiple times
      const partCounts = {};
      const partLastDate = {};
      state.sales.filter(s => s.customerId === c.id).forEach(s => {
        s.items.forEach(it => {
          partCounts[it.partId] = (partCounts[it.partId]||0) + 1;
          if (!partLastDate[it.partId] || s.date > partLastDate[it.partId]) {
            partLastDate[it.partId] = s.date;
          }
        });
      });
      // Also consider single-purchase consumables (oil, filters)
      Object.keys(partLastDate).forEach(pid => {
        const part = getPartById(pid);
        if (!part) return;
        const lastDays = daysAgo(partLastDate[pid]);
        // Heuristic: consumables (filters, lubricants, blades) due after 60 days
        const isConsumable = /filter|oil|grease|blade|coolant/i.test(part.category + ' ' + part.name);
        if (partCounts[pid] >= 2 || (isConsumable && lastDays >= 60)) {
          if (lastDays >= 45) {
            out.push({customer:c, part, daysSinceLastBuy: lastDays, timesBought: partCounts[pid]});
          }
        }
      });
    });
    return out.sort((a,b) => b.daysSinceLastBuy - a.daysSinceLastBuy).slice(0, 10);
  },
  // Top sellers in last 30 days
  topSellers(days=30, limit=10) {
    const cutoff = Date.now() - days*86400000;
    const tally = {};
    state.sales.forEach(s => {
      if (new Date(s.date).getTime() < cutoff) return;
      s.items.forEach(it => {
        if (!tally[it.partId]) tally[it.partId] = {qty:0, revenue:0};
        tally[it.partId].qty += it.qty;
        tally[it.partId].revenue += it.qty * it.price;
      });
    });
    return Object.entries(tally).map(([pid, data]) => ({part: getPartById(pid), ...data})).filter(x => x.part).sort((a,b) => b.qty - a.qty).slice(0, limit);
  },
  // Profit per sale and total
  profitMonth() {
    let revenue = 0, cost = 0;
    state.sales.filter(s => isThisMonth(s.date)).forEach(s => {
      s.items.forEach(it => {
        const p = getPartById(it.partId);
        revenue += it.price * it.qty;
        if (p) cost += p.costPrice * it.qty;
      });
    });
    return {revenue, cost, profit: revenue - cost, margin: revenue ? (revenue-cost)/revenue*100 : 0};
  },
  // Highest margin parts (sold ones)
  highMarginParts(limit=10) {
    return state.parts.map(p => ({
      ...p,
      margin: p.instantPrice - p.costPrice,
      marginPct: p.costPrice ? (p.instantPrice - p.costPrice)/p.costPrice*100 : 0,
      velocity: this.velocity(p.id)
    })).filter(p => p.velocity > 0).sort((a,b) => (b.margin*b.velocity) - (a.margin*a.velocity)).slice(0, limit);
  },
  // Daily action briefing — prioritized list of things to do
  briefing() {
    const actions = [];
    // Out of stock + has velocity → URGENT reorder
    state.parts.forEach(p => {
      if (p.stock === 0) {
        const v = this.velocity(p.id);
        if (v > 0) actions.push({
          type: 'urgent',
          icon: '🚨',
          title: `OUT OF STOCK: ${p.name}`,
          detail: `${p.partNo} — ${v.toFixed(2)} sold/day, losing sales now`,
          action: 'Order from supplier',
          onClick: `goTab('whatsapp');showWASection('supplier')`
        });
      }
    });
    // Low stock with high velocity → reorder soon
    state.parts.forEach(p => {
      if (p.stock > 0 && p.stock <= p.minStock) {
        const days = this.daysUntilStockout(p);
        if (days <= 14) actions.push({
          type: 'urgent',
          icon: '⚠️',
          title: `${p.name} runs out in ${days} days`,
          detail: `${p.stock} left, ${this.velocity(p.id).toFixed(2)}/day`,
          action: 'Reorder now',
          onClick: `goTab('whatsapp');showWASection('supplier')`
        });
      }
    });
    // Credit overdue
    state.customers.forEach(c => {
      if (c.totalDue > 0) actions.push({
        type: 'opportunity',
        icon: '💰',
        title: `Collect from ${c.name}`,
        detail: `${rs(c.totalDue)} pending`,
        action: 'Send reminder',
        onClick: `sendPaymentReminder('${c.id}')`
      });
    });
    // Customers due reorder
    const reorderDue = this.reorderDueCustomers().slice(0, 3);
    reorderDue.forEach(r => actions.push({
      type: 'opportunity',
      icon: '🎯',
      title: `${r.customer.name} due for ${r.part.name}`,
      detail: `Last bought ${r.daysSinceLastBuy} days ago`,
      action: 'Send reminder',
      onClick: `sendReorderReminder('${r.customer.id}','${r.part.id}')`
    }));
    // Dead stock alert
    const dead = this.deadStock(120);
    if (dead.length >= 3) actions.push({
      type: 'info',
      icon: '📉',
      title: `${dead.length} parts haven't sold in 120 days`,
      detail: `Locked-up capital: ${rs(dead.reduce((a,p) => a + p.costPrice*p.stock, 0))}`,
      action: 'View dead stock',
      onClick: `goTab('brain');showBrainSection('dead')`
    });
    return actions.slice(0, 8);
  }
};

// ============================================================
// ROLE PERMISSIONS — single source of truth
// ============================================================
const ROLE_PERMISSIONS = {
  // Full access — all tabs
  admin:       ['home','owner','inventory','catalogue','sale','wholesale','staff','reports','whatsapp','brain','import','collections','purchases','demand','brandmatrix','returns','po'],
  // Manager: everything except user management in owner panel; can see purchases and reports
  manager:     ['home','owner','inventory','catalogue','sale','wholesale','staff','reports','whatsapp','brain','import','collections','purchases','demand','brandmatrix','returns','po'],
  // Salesperson: selling + inventory lookup + WhatsApp outreach; no pricing, no purchases, no payroll
  salesperson: ['home','inventory','catalogue','sale','wholesale','whatsapp','brain','collections','returns'],
  // Accounts: purchases, supplier ledger, day closings, reports, salary — no sales floor
  accounts:    ['home','owner','inventory','reports','purchases','demand','staff','po'],
  // Delivery: can see inventory and mark orders dispatched; no pricing, no reports
  delivery:    ['home','inventory','catalogue','sale','collections'],
  // Readonly: view-only dashboard — no mutations (enforced per function via isReadOnly check)
  readonly:    ['home','inventory','reports']
};
// Catalogue mode each role can access: 'staff' = rack entry only; 'dad' = pricing queue; 'both' = all
const CAT_ROLE_MODE = {
  admin: 'both', manager: 'both', salesperson: 'staff', delivery: 'staff',
  accounts: 'staff', readonly: 'staff'
};
// Roles that cannot mutate data (read-only guard used throughout)
const READONLY_ROLES = new Set(['readonly']);
function isReadOnly() { return READONLY_ROLES.has(state.session?.role || ''); }

function canAccess(tab, role) {
  if (!role) return tab === 'home';
  return (ROLE_PERMISSIONS[role] || ['home']).includes(tab);
}

// Hide nav items user can't access
function applyRoleUI() {
  const role = state.session?.role || '';
  document.querySelectorAll('.nav-item[data-tab]').forEach(el => {
    const tab = el.dataset.tab;
    el.style.display = canAccess(tab, role) ? '' : 'none';
  });
}

// ============================================================
// NAV
// ============================================================
function updateDemandBadge() {
  var nav = document.getElementById('nav-demand');
  if (!nav) return;
  var count = (state.demandInbox||[]).filter(function(d){ return d.demandLevel==='high'||d.demandLevel==='medium'; }).length;
  var existing = nav.querySelector('.demand-nav-badge');
  if (count > 0) {
    if (!existing) {
      var b = document.createElement('span');
      b.className = 'demand-nav-badge';
      b.style.cssText = 'position:absolute;top:2px;right:6px;background:#dc2626;color:white;font-size:9px;font-weight:700;border-radius:50%;width:14px;height:14px;display:flex;align-items:center;justify-content:center;';
      nav.style.position = 'relative';
      nav.appendChild(b);
      existing = b;
    }
    existing.textContent = count;
  } else if (existing) {
    existing.remove();
  }
}

function goTab(name) {
  if (!state.session && name !== 'home') { toast('Please login first'); return; }
  const role = state.session?.role;
  if (!canAccess(name, role)) { toast('🔒 No access'); return; }

  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  const tab = document.getElementById('tab-'+name);
  if (tab) tab.classList.add('active');
  document.querySelectorAll('.nav-item').forEach(n => n.classList.toggle('active', n.dataset.tab === name));
  window.scrollTo(0, 0);

  // On mobile: let the tab paint first, then render content (avoids white flash + jank)
  const _render = () => {
    if (name === 'home') renderHome();
    if (name === 'owner') renderOwnerDashboard();
    if (name === 'inventory') renderInventory();
    if (name === 'catalogue') renderCatalogue();
    if (name === 'sale') { renderSaleCustomers(); renderSaleStaffDropdowns(); renderCart(); document.getElementById('sale-search').value=''; renderSaleSearch(); }
    if (name === 'whatsapp') { renderQuoteCustomers(); renderSupplierOrders(); renderOrders(); renderOutreach(); renderPaymentReminders(); updateBroadcastMsg(); }
    if (name === 'brain') { renderBrain(); }
    if (name === 'wholesale') { renderWholesaleOrders(); }
    if (name === 'reports') { initReports(); checkBackupReminder(); }
    if (name === 'staff') { renderStaffTab(); }
    if (name === 'collections') { initCollectionsTab(); showCollSection('summary'); }
    if (name === 'purchases') { renderPurchasesTab(); }
    if (name === 'po') { renderPOList(); }
    if (name === 'import') { /* static */ }
    if (name === 'demand') { renderDemandInbox(); }
    if (name === 'brandmatrix') { renderBrandMatrixTab(); }
  };
  // Heavy tabs: defer render by one frame on mobile so the tab switch feels instant
  if (window._isMobile && ['inventory','wholesale','reports','staff','brain','whatsapp'].includes(name)) {
    requestAnimationFrame(() => setTimeout(_render, 0));
  } else {
    _render();
  }
}

// renderSaleStaffDropdowns defined below near mechanic section

// ============================================================
// HOME
// ============================================================

// Debounced renderHome — collapses rapid back-to-back calls (e.g. savePart → catSaveDraft)
// into one. Direct calls (tab switch, login) bypass debounce via renderHome() directly.
let _renderHomeTimer = null;
function renderHomeDebounced() {
  clearTimeout(_renderHomeTimer);
  _renderHomeTimer = setTimeout(function() {
    // Only render if home tab is actually visible — no point rebuilding offscreen DOM
    var homeEl = document.getElementById('tab-home');
    if (homeEl && homeEl.style.display === 'none') return;
    renderHome();
  }, 200);
}

function renderHome() {
  document.getElementById('hdr-title').textContent = state.settings.shopName;
  try { updateDemandBadge(); } catch(e) { /* badge update — non-critical */ }

  const role = state.session?.role || 'guest';
  const isOwner = ['admin','manager'].includes(role);
  const isAdmin = role === 'admin';

  // Show/hide Owner nav tab based on role
  const ownerNav = document.getElementById('nav-owner');
  if (ownerNav) ownerNav.style.display = isOwner ? '' : 'none';

  // Show/hide profit card on home — admin (owner) only, not manager
  const profitCard = document.getElementById('stat-profit-card');
  if (profitCard) profitCard.style.display = isAdmin ? '' : 'none';

  // Cache home stats for 10s on mobile to avoid re-scanning all sales on every call
  const now = Date.now();
  if (!window._homeStatsCache || (now - window._homeStatsCache.ts) > (window._isMobile ? 10000 : 3000)) {
    const todaySales = state.sales.filter(s => isToday(s.date)).reduce((a,s) => a+s.total, 0);
    const monthSales = state.sales.filter(s => isThisMonth(s.date)).reduce((a,s) => a+s.total, 0);
    const lowCount = state.parts.filter(p => saleablePart(p) && p.stock > 0 && p.stock <= p.minStock).length;
    const outCount = state.parts.filter(p => saleablePart(p) && p.stock === 0).length;
    const due = state.customers.reduce((a,c) => a + (c.totalDue||0), 0);
    const profit = Smart.profitMonth();
    const recent = [...state.sales].sort((a,b) => b.date.localeCompare(a.date)).slice(0, 5);
    window._homeStatsCache = { ts: now, todaySales, monthSales, lowCount, outCount, due, profit, recent };
  }
  const { todaySales, monthSales, lowCount, outCount, due, profit, recent } = window._homeStatsCache;

  document.getElementById('s-today').textContent = rs(todaySales);
  document.getElementById('s-month').textContent = rs(monthSales);
  document.getElementById('s-profit').textContent = rs(profit.profit);
  document.getElementById('s-due').textContent = rs(due);
  document.getElementById('s-low').textContent = lowCount;
  document.getElementById('s-out').textContent = outCount;
  var demandCount = (state.demandInbox||[]).filter(function(d){ return d.demandLevel==='high'||d.demandLevel==='medium'; }).length;
  var demandEl = document.getElementById('s-demand'); if (demandEl) demandEl.textContent = demandCount;

  // Briefing
  if (!window._briefingCache || (Date.now() - window._briefingCache.ts) > (window._isMobile ? 30000 : 10000)) {
    window._briefingCache = { ts: Date.now(), actions: Smart.briefing() };
  }
  const actions = window._briefingCache.actions;
  document.getElementById('briefing-summary').textContent = actions.length === 0 ? 'All clear — no urgent actions today.' : `${actions.length} action${actions.length>1?'s':''} for you`;
  document.getElementById('briefing-list').innerHTML = actions.slice(0, 5).map(a => `
    <div class="briefing-item ${a.type}">
      <div><span style="font-size:16px;">${a.icon}</span> <b>${a.title}</b></div>
      <small>${a.detail}</small>
      <a class="briefing-action" onclick="${a.onClick};return false;" href="#">${a.action}</a>
    </div>`).join('');

  document.getElementById('recent-sales').innerHTML = recent.length === 0
    ? '<div class="empty"><div class="icon">🧾</div>No sales yet</div>'
    : recent.map(s => {
        const cust = getCustomerById(s.customerId);
        return `<div class="sale-row">
          <div class="sale-head">
            <div><b>${s.invoiceNo}</b><br><small>${cust?cust.name:'Walk-in'} • ${s.paymentMode.toUpperCase()}</small></div>
            <div class="sale-amt">${rs(s.total)}</div>
          </div>
          <small>${new Date(s.date).toLocaleString('en-IN')} • ${s.items.length} item${s.items.length>1?'s':''}</small>
        </div>`;
      }).join('');

  renderHomeCollections();
  renderHomeRecentParts();
  renderHomeAddVelocity(); // called directly so it runs even if renderHomeRecentParts early-returns
}

function renderHomeRecentParts() {
  const card = document.getElementById('home-recent-parts-card');
  const list = document.getElementById('home-recent-parts-list');
  const countEl = document.getElementById('home-recent-parts-count');
  if (!card || !list) return;

  // Compute calendar boundaries for today and yesterday
  const now = new Date();
  const todayMidnight = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
  const yesterdayMidnight = todayMidnight - 24 * 60 * 60 * 1000;

  const allParts = (state.parts || []).filter(p => p.addedAt && p.addedAt >= yesterdayMidnight);
  const todayParts = allParts.filter(p => p.addedAt >= todayMidnight).sort((a, b) => b.addedAt - a.addedAt);
  const yesterdayParts = allParts.filter(p => p.addedAt < todayMidnight).sort((a, b) => b.addedAt - a.addedAt);

  if (!allParts.length) { card.style.display = 'none'; return; }
  card.style.display = '';
  if (countEl) countEl.textContent = allParts.length;

  function partRow(p) {
    const ageMin = Math.round((Date.now() - p.addedAt) / 60000);
    const ageLabel = ageMin < 60
      ? ageMin + 'm ago'
      : ageMin < 1440
        ? Math.floor(ageMin / 60) + 'h ago'
        : Math.floor(ageMin / 1440) + 'd ago';
    const pricingOk = catPricingDone(p);
    const missingFlags = [];
    if (!pricingOk)      missingFlags.push('<span style="background:#fef9c3;color:#854d0e;font-size:10px;font-weight:700;padding:2px 7px;border-radius:8px;">⏳ No price</span>');
    if (!p.location)     missingFlags.push('<span style="background:#fef2f2;color:#dc2626;font-size:10px;font-weight:700;padding:2px 7px;border-radius:8px;">📍 No rack</span>');
    if (!p.supplierName) missingFlags.push('<span style="background:#fffbeb;color:#d97706;font-size:10px;font-weight:600;padding:2px 7px;border-radius:8px;">🏢 No supplier</span>');
    return `<div style="padding:9px 0;border-bottom:1px solid var(--border);display:flex;align-items:flex-start;gap:10px;cursor:pointer;" onclick="goTab('inventory');openPartDetail('${p.id}')">
      <div style="flex:1;min-width:0;">
        <div style="font-size:13px;font-weight:700;color:var(--text);">${p.name}
          <span style="background:#0f766e;color:white;font-size:9px;font-weight:800;padding:1px 6px;border-radius:8px;margin-left:4px;vertical-align:middle;">NEW</span>
        </div>
        ${p.models ? `<div style="font-size:11px;color:#0f766e;font-weight:600;margin-top:1px;">🔧 ${p.models}</div>` : ''}
        <div style="font-size:11px;color:var(--muted);margin-top:2px;">${p.partNo} · ${p.brand}${p.location ? ' · 📍 ' + p.location : ''}</div>
        ${missingFlags.length ? `<div style="display:flex;gap:4px;flex-wrap:wrap;margin-top:4px;">${missingFlags.join('')}</div>` : '<div style="font-size:10px;color:#16a34a;margin-top:3px;font-weight:600;">✅ Complete</div>'}
      </div>
      <div style="text-align:right;flex-shrink:0;">
        <div style="font-size:13px;font-weight:700;color:var(--primary);">${pricingOk ? rs(p.instantPrice) : '—'}</div>
        <div style="font-size:10px;color:var(--muted);margin-top:2px;">${ageLabel}</div>
      </div>
    </div>`;
  }

  function sectionHeader(label, count, color) {
    return `<div style="font-size:11px;font-weight:800;color:${color};text-transform:uppercase;letter-spacing:0.5px;padding:6px 0 2px;margin-top:4px;">${label} <span style="font-weight:600;opacity:.7;">(${count})</span></div>`;
  }

  let html = '';
  if (todayParts.length) {
    html += sectionHeader('📅 Today', todayParts.length, '#0f766e');
    html += todayParts.slice(0, 15).map(partRow).join('');
    if (todayParts.length > 15) html += `<div style="font-size:12px;color:var(--muted);text-align:center;padding:6px 0;">+${todayParts.length - 15} more today</div>`;
  }
  if (yesterdayParts.length) {
    html += sectionHeader('🕐 Yesterday', yesterdayParts.length, '#7c3aed');
    html += yesterdayParts.slice(0, 10).map(partRow).join('');
    if (yesterdayParts.length > 10) html += `<div style="font-size:12px;color:var(--muted);text-align:center;padding:6px 0;">+${yesterdayParts.length - 10} more from yesterday → tap "View All"</div>`;
  }
  list.innerHTML = html;
}

// Robustly extract a millisecond timestamp from a part's addedAt, whatever form
// it's in: number (Date.now), ISO string, Firestore Timestamp object, or Date.
// Returns 0 if absent/unparseable so such parts simply don't count as "today".
function _partAddedTs(p) {
  const a = p && p.addedAt;
  if (a == null) return 0;
  if (typeof a === 'number') return a;
  if (typeof a === 'string') { const t = new Date(a).getTime(); return isNaN(t) ? 0 : t; }
  if (typeof a === 'object') {
    if (typeof a.toMillis === 'function') return a.toMillis();          // Firestore Timestamp
    if (typeof a.seconds === 'number') return a.seconds*1000 + (a.nanoseconds ? Math.floor(a.nanoseconds/1e6) : 0);
    if (a instanceof Date) return a.getTime();
  }
  return 0;
}

function renderHomeAddVelocity() {
  const panel  = document.getElementById('home-add-velocity');
  const barsEl = document.getElementById('home-velocity-bars');
  const totEl  = document.getElementById('home-velocity-total');
  if (!panel || !barsEl || !totEl) return;

  // "Today" = local calendar day, consistent with the rest of the home screen.
  // (Using local midnight instead of UTC toISOString avoids the IST +5:30 skew
  // that made early-morning / late-night adds land on the wrong day and vanish.)
  const _now = new Date();
  const _todayMidnight = new Date(_now.getFullYear(), _now.getMonth(), _now.getDate()).getTime();
  const todayParts = (state.parts || []).filter(p => {
    const ts = _partAddedTs(p);
    return ts && ts >= _todayMidnight;
  });

  // Always show the panel so the metric is never "missing" — show a zero state
  // until the first part is added today.
  panel.style.display = '';
  if (!todayParts.length) {
    totEl.textContent = '0 today';
    barsEl.innerHTML = '<div style="font-size:12px;color:#047857;padding:2px 0;">No parts added yet today — new parts from the Catalogue will show up here by the hour.</div>';
    return;
  }

  // Group by hour of day (0-23) — local hours
  const buckets = {};
  todayParts.forEach(p => {
    const ts = _partAddedTs(p);
    const hr = new Date(ts).getHours();
    buckets[hr] = (buckets[hr] || 0) + 1;
  });

  const total = todayParts.length;
  totEl.textContent = total + ' parts today';

  const hours = Object.keys(buckets).map(Number).sort((a, b) => a - b);
  const maxCount = Math.max(...hours.map(h => buckets[h]));

  const fmtHr = h => {
    const suffix    = h >= 12 ? 'PM' : 'AM';
    const h12       = h % 12 === 0 ? 12 : h % 12;
    const h12end    = (h + 1) % 12 === 0 ? 12 : (h + 1) % 12;
    const sufEnd    = (h + 1) >= 12 ? 'PM' : 'AM';
    return h12 + '\u2013' + h12end + ' ' + sufEnd;
  };

  barsEl.innerHTML = hours.map((h, i) => {
    const count  = buckets[h];
    const pct    = Math.max(8, Math.round((count / maxCount) * 100));
    const isLast = i === hours.length - 1;
    const barClr = isLast ? '#0f766e' : '#34d399';
    return '<div style="display:flex;align-items:center;gap:8px;margin-bottom:3px;">'
      + '<div style="width:72px;font-size:11px;color:#064e3b;font-weight:600;flex-shrink:0;">' + fmtHr(h) + '</div>'
      + '<div style="flex:1;background:#d1fae5;border-radius:4px;height:15px;overflow:hidden;">'
      +   '<div style="width:' + pct + '%;background:' + barClr + ';height:100%;border-radius:4px;"></div>'
      + '</div>'
      + '<div style="width:28px;text-align:right;font-size:12px;font-weight:700;color:#065f46;flex-shrink:0;">' + count + '</div>'
      + '</div>';
  }).join('')
  + '<div style="margin-top:5px;padding-top:5px;border-top:1px dashed #6ee7b7;display:flex;justify-content:space-between;align-items:center;">'
  +   '<div style="font-size:11px;color:#047857;font-weight:600;">' + hours.length + ' session' + (hours.length > 1 ? 's' : '') + ' today</div>'
  +   '<div style="font-size:13px;font-weight:800;color:#065f46;">Total \u00b7 ' + total + '</div>'
  + '</div>';
}

function renderHomeCollections() {
  const div = document.getElementById('home-collections-summary');
  if (!div) return;
  const today = todayStr();
  const todayColl = (state.collections||[]).filter(c=>c.date===today);
  const todayExp  = (state.expenses||[]).filter(e=>e.date===today);
  const collTotal = todayColl.reduce((a,c)=>a+c.amount,0);
  const expTotal  = todayExp.reduce((a,e)=>a+e.amount,0);

  if (collTotal === 0 && expTotal === 0) {
    div.innerHTML = '<div style="color:var(--muted);font-size:13px;padding:8px 0;">No collections or expenses today. <a href="#" onclick="goTab(\'collections\');return false;" style="color:var(--primary);">Add one →</a></div>';
    return;
  }
  div.innerHTML = `
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px;">
      <div style="background:#f0fdf4;border-radius:10px;padding:10px;text-align:center;">
        <div style="font-size:18px;font-weight:800;color:var(--success);">${rs(collTotal)}</div>
        <div style="font-size:11px;color:var(--muted);">💰 COLLECTED</div>
      </div>
      <div style="background:#fff1f2;border-radius:10px;padding:10px;text-align:center;">
        <div style="font-size:18px;font-weight:800;color:var(--danger);">${rs(expTotal)}</div>
        <div style="font-size:11px;color:var(--muted);">🧾 EXPENSES</div>
      </div>
    </div>
    ${todayColl.slice(0,3).map(c=>`<div style="font-size:13px;padding:4px 0;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;"><span>${c.custName} · ${c.mode}</span><b style="color:var(--success);">+${rs(c.amount)}</b></div>`).join('')}
    ${todayExp.slice(0,2).map(e=>`<div style="font-size:13px;padding:4px 0;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;"><span>⛽ ${e.expType}${e.trip?' — '+e.trip:''}</span><b style="color:var(--danger);">−${rs(e.amount)}</b></div>`).join('')}
  `;
}

// ============================================================
// INVENTORY — Paginated for 10,500+ SKU scale
// ============================================================
const MACHINES = [
  {id:'all', label:'All'}, {id:'tractor', label:'Tractor'}, {id:'harvester', label:'Harvester'},
  {id:'jcb', label:'JCB'}, {id:'rotavator', label:'Rotavator'},
  {id:'low', label:'Low Stock'}, {id:'dead', label:'Dead Stock'}, {id:'hot', label:'🔥 Hot'}, {id:'new', label:'🆕 New (24h)'}
];

const INV_PAGE_SIZE = /Mobi|Android|iPhone|iPad/i.test(navigator.userAgent) ? 40 : 80; // fewer items on mobile
let _invPage = 0;         // current page (0-indexed)
let _invFiltered = [];    // cached filtered+sorted results

function renderInventory(resetPage) {
  if (resetPage !== false) _invPage = 0; // reset to page 1 when filter/search changes

  const chips = document.getElementById('inv-chips');
  chips.innerHTML = MACHINES.map(m =>
    `<div class="chip ${state.filterMachine===m.id?'active':''}" onclick="state.filterMachine='${m.id}';renderInventory()">${m.label}</div>`
  ).join('');

  const q = (document.getElementById('inv-search').value || '').toLowerCase().trim();
  // Show ALL parts (priced + pending) — sale screen still gates on saleablePart
  let parts = state.parts.filter(p => catStaffDone(p));
  if (state.filterMachine === 'low') parts = parts.filter(p => p.stock <= p.minStock);
  else if (state.filterMachine === 'dead') parts = Smart.deadStock(90);
  else if (state.filterMachine === 'hot') parts = parts.filter(p => Smart.velocity(p.id) >= 0.3);
  else if (state.filterMachine === 'new') { const cut = Date.now() - 24*60*60*1000; parts = state.parts.filter(p => p.addedAt && p.addedAt >= cut); }
  else if (state.filterMachine !== 'all') parts = parts.filter(p => p.machineType === state.filterMachine);
  if (q) parts = parts.filter(p =>
    p.partNo.toLowerCase().includes(q) ||
    p.name.toLowerCase().includes(q) ||
    p.brand.toLowerCase().includes(q) ||
    (p.models||'').toLowerCase().includes(q)
  );
  // For 'new' filter sort by newest first; otherwise sort by velocity
  if (state.filterMachine === 'new') {
    parts.sort((a,b) => (b.addedAt||0) - (a.addedAt||0));
  } else if (parts.length <= 2000) {
    // Cache velocity per part BEFORE sort — avoids O(n log n) repeated calls
    const velMap = {};
    parts.forEach(p => { velMap[p.id] = Smart.velocity(p.id); });
    parts.sort((a,b) => velMap[b.id] - velMap[a.id]);
  } else {
    parts.sort((a,b) => a.name.localeCompare(b.name));
  }
  _invFiltered = parts;

  const list = document.getElementById('inv-list');
  const countEl = document.getElementById('inv-count');
  if (countEl) countEl.textContent = parts.length + ' parts';

  if (parts.length === 0) {
    list.innerHTML = '<div class="empty"><div class="icon">📦</div>No parts found</div>';
    return;
  }

  // Render only current page
  const startIdx = 0;
  const endIdx = (_invPage + 1) * INV_PAGE_SIZE;
  const visible = parts.slice(startIdx, endIdx);
  const hasMore = parts.length > endIdx;

  // Use RAF to avoid blocking main thread during large renders
  requestAnimationFrame(function() {
  list.innerHTML = visible.map(p => buildPartCard(p)).join('') +
    (hasMore
      ? `<div id="inv-load-more" style="padding:16px;text-align:center;">
           <button class="btn btn-outline" style="max-width:300px;" onclick="loadMoreInventory()">
             ⬇ Load more (${parts.length - endIdx} remaining)
           </button>
         </div>`
      : (parts.length > INV_PAGE_SIZE ? `<div style="text-align:center;padding:12px;color:var(--muted);font-size:12px;">All ${parts.length} parts loaded</div>` : '')
    );
  // Lazy-load photos: set src AFTER innerHTML is painted — avoids blocking the paint with base64 decode
  _resolvePartPhotos(list, visible);
  }); // end RAF
}

// Lazy photo loader: after innerHTML is set, find all [data-photo] imgs and assign src.
// Keeps base64 data OUT of the HTML string concat — reduces innerHTML parse time dramatically.
function _resolvePartPhotos(container, parts) {
  if (!container || !parts || !parts.length) return;
  const photoMap = {};
  parts.forEach(function(p) { if (p.photoDataUrl) photoMap[p.id] = p.photoDataUrl; });
  const imgs = container.querySelectorAll('img[data-photo]');
  imgs.forEach(function(img) {
    const id = img.dataset.photo;
    const src = photoMap[id];
    if (!src) return;
    // A placeholder ('__pending_upload__' etc.) is NOT a real URL — never set it as src
    // (that produces a broken image). Re-fetch the real Storage URL and backfill.
    if (window.isPlaceholderPhoto && window.isPlaceholderPhoto(src)) {
      if (window._healPartPhoto) {
        window._healPartPhoto(id).then(function(url){ if (url) img.src = url; });
      }
      return;
    }
    img.src = src;
  });
}

function buildPartCard(p) {
  const stockClass = p.stock === 0 ? 'stock-out' : (p.stock <= p.minStock ? 'stock-low' : 'stock-ok');
  const stockText = p.stock === 0 ? 'OUT OF STOCK' : (p.stock + ' in stock');
  const heat = Smart.heatLabel(p);
  const v = Smart.velocity(p.id);
  const days = Smart.daysUntilStockout(p);
  let velText = '';
  if (v > 0) {
    velText = `<span class="${heat.cls}">${heat.txt}</span> · ${v.toFixed(2)}/day`;
    if (days < Infinity && p.stock > 0) velText += ` · ${days}d left`;
  } else {
    velText = `<span class="cold">No sales 30d</span>`;
  }
  const pricingDone = catPricingDone(p);
  const priceBlock = pricingDone
    ? `<div class="price-instant">${rs(p.instantPrice)}</div><div class="price-credit">Cr: ${rs(p.creditPrice)}</div><div class="price-credit">MRP: ${rs(p.mrp)}</div>`
    : `<div style="background:#fef9c3;color:#854d0e;font-size:11px;font-weight:700;padding:5px 8px;border-radius:7px;text-align:center;line-height:1.5;">⏳ Price<br>pending</div>`;

  // ── Missing-field flags ─────────────────────────────────────
  const missingRed   = [];   // blockers — red
  const missingAmber = [];   // useful but not critical — amber
  if (!p.location)     missingRed.push('📍 No rack');
  if (!p.photoDataUrl) missingAmber.push('📷 No photo');
  if (!p.supplierName) missingAmber.push('🏢 No supplier');
  if (!p.minStock)     missingAmber.push('📉 No min stock');
  const hasFlags = missingRed.length > 0 || missingAmber.length > 0;

  const flagStrip = hasFlags
    ? `<div style="display:flex;flex-wrap:wrap;gap:4px;margin-top:5px;">
        ${missingRed.map(f=>`<span style="font-size:10px;font-weight:700;padding:2px 7px;border-radius:10px;background:#fef2f2;color:#dc2626;border:1px solid #fecaca;">${f}</span>`).join('')}
        ${missingAmber.map(f=>`<span style="font-size:10px;font-weight:600;padding:2px 7px;border-radius:10px;background:#fffbeb;color:#d97706;border:1px solid #fde68a;">${f}</span>`).join('')}
      </div>`
    : '';

  // Location badge: red if missing, normal if present
  const locBadge = p.location
    ? `<span class="rack-badge">📍 ${p.location}</span>`
    : `<span class="rack-badge" style="background:#fef2f2;color:#dc2626;border:1px solid #fecaca;font-weight:700;">📍 No rack</span>`;

  // Left border: red if missing location (blocker), amber if pricing pending, nothing if all good
  const leftBorder = !p.location
    ? 'border-left:3px solid #dc2626;'
    : !pricingDone
      ? 'border-left:3px solid #f59e0b;opacity:0.85;'
      : '';

  // ── New badge (added in last 24h) ──────────────────────────
  const isNew = p.addedAt && (Date.now() - p.addedAt) < 24*60*60*1000;
  const newBadge = isNew
    ? `<span style="background:#0f766e;color:white;font-size:9px;font-weight:800;padding:2px 7px;border-radius:10px;letter-spacing:.5px;text-transform:uppercase;vertical-align:middle;margin-left:5px;">NEW</span>`
    : '';

  return `<div class="part-item" id="pcard-${p.id}" onclick="openPartDetail('${p.id}')" style="${leftBorder}">
    <div class="part-row">
      ${p.photoDataUrl
        ? `<img data-photo="${p.id}" style="width:48px;height:48px;object-fit:cover;border-radius:8px;flex-shrink:0;margin-right:10px;border:1px solid var(--border);">`
        : `<div style="width:48px;height:48px;border-radius:8px;flex-shrink:0;margin-right:10px;background:#fff7ed;border:1.5px dashed #f59e0b;display:flex;align-items:center;justify-content:center;font-size:18px;color:#d97706;">📷</div>`
      }
      <div style="flex:1;min-width:0;">
        <div class="part-name">${p.name}${newBadge}</div>
        ${p.models ? `<div style="font-size:11px;color:#0f766e;font-weight:600;margin-top:1px;">🔧 ${p.models}</div>` : ''}
        <div class="part-no">${p.partNo} • ${p.brand}</div>
        <div class="part-meta">
          <span class="tag">${p.machineType}</span>
          ${p.category ? `<span class="tag">${p.category}</span>` : ''}
          ${p.unit && p.unit !== 'pcs' ? `<span class="tag" style="background:#f0fdf4;color:#166634;">📦 ${p.unit}</span>` : ''}
          ${locBadge}
        </div>
        <div class="velocity">${velText}</div>
        <span class="stock-badge ${stockClass}">${stockText}</span>
        ${p.stockOnWay > 0 ? `<span class="stock-badge" style="background:#cffafe;color:#164e63;margin-left:4px;">+${p.stockOnWay} on the way${p.expectedDays ? ' · '+p.expectedDays+'d' : ''}</span>` : ''}
        ${flagStrip}
      </div>
      <div class="part-prices">${priceBlock}</div>
    </div>
  </div>`;
}

function loadMoreInventory() {
  _invPage++;
  // Append new items instead of re-rendering everything
  const list = document.getElementById('inv-list');
  const startIdx = _invPage * INV_PAGE_SIZE;
  const endIdx = (_invPage + 1) * INV_PAGE_SIZE;
  const newItems = _invFiltered.slice(startIdx, endIdx);
  const hasMore = _invFiltered.length > endIdx;

  // Remove old load-more button
  const oldBtn = document.getElementById('inv-load-more');
  if (oldBtn) oldBtn.remove();

  // Append new cards
  const frag = document.createDocumentFragment();
  const tempDiv = document.createElement('div');
  tempDiv.innerHTML = newItems.map(p => buildPartCard(p)).join('');
  while (tempDiv.firstChild) frag.appendChild(tempDiv.firstChild);
  list.appendChild(frag);
  // Lazy-load photos for newly appended cards
  _resolvePartPhotos(list, newItems);

  // Append new load-more button or end message
  if (hasMore) {
    list.insertAdjacentHTML('beforeend', `<div id="inv-load-more" style="padding:16px;text-align:center;">
      <button class="btn btn-outline" style="max-width:300px;" onclick="loadMoreInventory()">
        ⬇ Load more (${_invFiltered.length - endIdx} remaining)
      </button>
    </div>`);
  } else {
    list.insertAdjacentHTML('beforeend', `<div style="text-align:center;padding:12px;color:var(--muted);font-size:12px;">All ${_invFiltered.length} parts loaded</div>`);
  }
}

// ── TARGETED SINGLE-CARD UPDATE ────────────────────────────────
// Replaces only the one DOM card that changed, skipping a full renderInventory().
// Falls back to full render when:
//   - inventory tab isn't visible (no-op either way)
//   - part is not in the current filtered list (e.g. filter is 'hot' and part is cold)
//   - part is a new add (needs to appear at correct sorted position)
//   - caller explicitly passes forceFullRender=true
function patchInventoryCard(part, forceFullRender) {
  if (forceFullRender) { renderInventory(); return; }

  // Only bother if the inventory list is currently in the DOM and visible
  const list = document.getElementById('inv-list');
  if (!list || !list.offsetParent) { return; } // tab not visible — skip entirely

  const existing = document.getElementById('pcard-' + part.id);

  if (existing) {
    // Part is already rendered — replace just this card, no filter/sort needed
    const newCard = buildPartCard(part);
    existing.outerHTML = newCard;
    // Lazy-load photo for the replaced card
    const replaced = document.getElementById('pcard-' + part.id);
    if (replaced) _resolvePartPhotos(replaced, [part]);
    // Also refresh the count label (stock status may have changed)
    const countEl = document.getElementById('inv-count');
    if (countEl) countEl.textContent = _invFiltered.length + ' parts';
    return;
  }

  // Card not in DOM — either new part or outside current page.
  // Re-run the full render so the new card appears at the right sorted position.
  renderInventory();
}

// ── UNIFIED ADD / EDIT PART MODAL ──────────────────────────────
// Shared by: Inventory FAB, SKU Editor, Quick-Add card, Purchase bill inline
// opts: { id, prefill:{partNo,name,qty,unitCost}, onSave, context }
function openAddPart(idOrOpts) {
  // Support legacy call openAddPart(id) and new openAddPart({id, prefill, onSave, context})
  let id = null, prefill = {}, onSaveCallback = null, context = '';
  if (typeof idOrOpts === 'string') { id = idOrOpts; }
  else if (idOrOpts && typeof idOrOpts === 'object') {
    id = idOrOpts.id || null;
    prefill = idOrOpts.prefill || {};
    onSaveCallback = idOrOpts.onSave || null;
    context = idOrOpts.context || '';
  }

  const p = id ? (getPartById(id) || {}) : {};

  // Unit type definitions
  const UNIT_TYPES = [
    { key:'pcs',   label:'Pieces',  unit:'pcs',  icon:'🔩', hint:'each' },
    { key:'set',   label:'Set',     unit:'set',  icon:'📦', hint:'per set' },
    { key:'box',   label:'Box',     unit:'box',  icon:'🗃️',  hint:'per box' },
    { key:'pair',  label:'Pair',    unit:'pair', icon:'🔗', hint:'per pair' },
    { key:'kg',    label:'KG',      unit:'kg',   icon:'⚖️',  hint:'per kg' },
    { key:'ltr',   label:'Litre',   unit:'ltr',  icon:'🫙', hint:'per ltr' },
    { key:'mtr',   label:'Metre',   unit:'mtr',  icon:'📏', hint:'per mtr' },
  ];
  const curUnit = p.unit || prefill.unit || 'pcs';
  const unitPills = UNIT_TYPES.map(u =>
    `<div class="uap-unit-pill${u.key===curUnit?' sel':''}" onclick="uapSelectUnit('${u.key}',this)" data-unit="${u.key}">
      ${u.icon} ${u.label} <span class="pill-unit">(${u.unit})</span>
    </div>`
  ).join('');

  const pNo   = p.partNo   || prefill.partNo   || '';
  const pName = p.name     || prefill.name     || '';
  const pCost = p.costPrice|| prefill.unitCost  || 0;
  const pInst = p.instantPrice || (pCost ? Math.round(pCost*1.3) : 0);
  const pCred = p.creditPrice  || (pCost ? Math.round(pCost*1.15): 0);

  const isEdit = !!id;

  showModal(isEdit ? 'Edit Part' : '+ Add New Part', `
    <!-- MODAL HEADER (XLS-style dark) -->
    <div class="uap-modal-hdr">
      <span class="uap-icon">${isEdit?'✏️':'➕'}</span>
      <h2>${isEdit ? 'Edit Part — ' + (p.name||'') : context ? 'Add New Part · '+context : 'Add New Part'}</h2>
    </div>

    <!-- SECTION: IDENTITY -->
    <div class="uap-section">
      <div class="uap-section-title">Part Identity</div>
      <div class="row2">
        <div class="field"><label>Part Number *</label>
          <input class="input" id="uap-partNo" value="${pNo}" placeholder="e.g. BRG-6205" style="text-transform:uppercase;" oninput="this.value=this.value.toUpperCase()"></div>
        <div class="field"><label>Brand</label>
          <input class="input" id="uap-brand" value="${p.brand||prefill.brand||''}" placeholder="SKF, Mahindra…"></div>
      </div>
      <div class="field"><label>Part Name *</label>
        <input class="input" id="uap-name" value="${pName}" placeholder="e.g. Ball Bearing 6205 ZZ"></div>
      <div class="row2">
        <div class="field"><label>Machine Type</label>
          <select class="select" id="uap-machineType">
            <option value="tractor"   ${(p.machineType||'tractor')==='tractor'  ?'selected':''}>🚜 Tractor</option>
            <option value="harvester" ${p.machineType==='harvester'?'selected':''}>🌾 Harvester</option>
            <option value="jcb"       ${p.machineType==='jcb'      ?'selected':''}>🏗️ JCB</option>
            <option value="rotavator" ${p.machineType==='rotavator'?'selected':''}>⚙️ Rotavator</option>
            <option value="other"     ${p.machineType==='other'    ?'selected':''}>🔧 Other</option>
          </select>
        </div>
        <div class="field"><label>Category</label>
          <input class="input" id="uap-category" value="${p.category||prefill.category||''}" placeholder="Bearing, Filter…"></div>
      </div>
      <div class="field"><label>Compatible Models <span style="font-size:11px;color:var(--muted);">(optional)</span></label>
        <input class="input" id="uap-models" value="${p.models||''}" placeholder="e.g. Mahindra 575, Sonalika 750"></div>
    </div>

    <!-- SECTION: UNIT TYPE -->
    <div class="uap-section">
      <div class="uap-section-title">Unit Type — How is this SKU measured?</div>
      <div class="uap-unit-pills" id="uap-unit-pills">${unitPills}</div>
      <input type="hidden" id="uap-unit" value="${curUnit}">
      <div class="field" id="uap-unit-custom-wrap" style="display:none;">
        <label>Custom Unit</label>
        <input class="input" id="uap-unit-custom" placeholder="e.g. drum, strip, roll" style="max-width:160px;">
      </div>
    </div>

    <!-- SECTION: PRICING + PER-UNIT ECONOMICS -->
    <div class="uap-section">
      <div class="uap-section-title">Pricing <span class="uap-auto-tag">✦ auto-fills margins</span></div>
      <div class="row3">
        <div class="field">
          <label>Cost Price ₹</label>
          <input type="number" class="input" id="uap-cost" value="${pCost||''}" placeholder="0" oninput="uapAutoPrice()">
        </div>
        <div class="field">
          <label>MRP ₹ <span style="font-size:10px;color:var(--muted);">(printed)</span></label>
          <input type="number" class="input" id="uap-mrp" value="${p.mrp||''}" placeholder="0">
        </div>
        <div class="field">
          <label>Retail ₹ <span style="font-size:10px;color:var(--primary);">Instant</span></label>
          <input type="number" class="input" id="uap-instant" value="${pInst||''}" placeholder="0" oninput="uapAutoPrice()" style="border-color:#99f6e4;">
        </div>
      </div>
      <div class="row2">
        <div class="field">
          <label>Wholesale ₹ <span style="font-size:10px;color:var(--warning);">Credit/Trade</span></label>
          <input type="number" class="input" id="uap-credit" value="${pCred||''}" placeholder="0" style="border-color:#fde68a;">
        </div>
        <div class="field" style="display:flex;flex-direction:column;justify-content:flex-end;">
          <div id="uap-econ-bar" class="uap-econ-bar" style="margin-top:0;">
            <div class="uap-econ-col"><div class="ec-val" id="uap-margin-pct">—</div><div class="ec-lbl">Margin %</div></div>
            <div class="uap-econ-divider"></div>
            <div class="uap-econ-col"><div class="ec-val" id="uap-margin-abs">—</div><div class="ec-lbl">Profit / <span id="uap-econ-unit">unit</span></div></div>
            <div class="uap-econ-divider"></div>
            <div class="uap-econ-col"><div class="ec-val" id="uap-ws-margin-pct">—</div><div class="ec-lbl">Trade %</div></div>
          </div>
        </div>
      </div>
    </div>

    <!-- SECTION: STOCK -->
    <div class="uap-section">
      <div class="uap-section-title">Stock & Location</div>
      <div class="row3">
        <div class="field">
          <label>Current Stock</label>
          <input type="number" class="input" id="uap-stock" value="${p.stock||prefill.qty||0}" placeholder="0">
        </div>
        <div class="field">
          <label>Min Stock <span style="font-size:10px;color:var(--muted);">(reorder)</span></label>
          <input type="number" class="input" id="uap-minStock" value="${p.minStock||0}" placeholder="0">
        </div>
        <div class="field">
          <label>Rack / Location</label>
          <input class="input" id="uap-loc" value="${p.location||''}" placeholder="A-01" style="text-transform:uppercase;" oninput="this.value=this.value.toUpperCase()">
        </div>
      </div>
    </div>

    <!-- SECTION: SUPPLIER -->
    <div class="uap-section">
      <div class="uap-section-title">Supplier (optional)</div>
      <div class="row2">
        <div class="field"><label>Supplier Name</label>
          <input class="input" id="uap-supName" value="${p.supplierName||''}" placeholder="e.g. Singh Auto Parts" list="global-supplier-list" oninput="supAutofill('uap-supName','uap-supPhone')" autocomplete="off"></div>
        <div class="field"><label>Supplier Phone</label>
          <input class="input" id="uap-supPhone" value="${p.supplierPhone||''}" placeholder="9876543210"></div>
      </div>
    </div>

    <!-- SECTION: PHOTO -->
    <div class="uap-section">
      <div class="uap-section-title">Part Photo <span style="font-size:10px;color:var(--muted);font-weight:400;">(optional — helps WhatsApp bot identify parts)</span></div>
      <div style="display:flex;gap:10px;align-items:flex-start;">
        <div id="uap-photo-preview" style="width:80px;height:80px;border-radius:8px;border:1.5px dashed var(--border);background:#f8fafc;display:flex;align-items:center;justify-content:center;font-size:28px;flex-shrink:0;overflow:hidden;">
          ${(window.displayPhotoUrl?window.displayPhotoUrl(p.photoDataUrl):p.photoDataUrl) ? '<img src="'+(window.displayPhotoUrl?window.displayPhotoUrl(p.photoDataUrl):p.photoDataUrl)+'" style="width:100%;height:100%;object-fit:cover;border-radius:6px;">' : '📷'}
        </div>
        <div style="flex:1;">
          <div style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:6px;">
            <label class="btn btn-sm" style="display:inline-flex;align-items:center;gap:4px;cursor:pointer;font-size:13px;background:var(--primary);color:white;border:none;padding:8px 12px;border-radius:10px;">
              📸 Camera
              <input type="file" id="uap-photo-input" accept="image/*" capture="environment" style="display:none;" onchange="uapHandlePhoto(this)">
            </label>
            <label class="btn btn-outline btn-sm" style="display:inline-flex;align-items:center;gap:4px;cursor:pointer;font-size:13px;">
              🖼 Gallery
              <input type="file" id="uap-photo-gallery-input" accept="image/*" style="display:none;" onchange="uapHandlePhoto(this)">
            </label>
          </div>
          <div id="uap-ai-hint" style="font-size:11px;font-weight:600;color:var(--primary);margin-bottom:4px;display:none;"></div>
          <div style="font-size:11px;color:var(--muted);">Camera auto-reads part number, name, brand, MRP from the box.</div>
          <button type="button" id="uap-photo-clear" onclick="uapClearPhoto()" style="display:${p.photoDataUrl?'inline-block':'none'};margin-top:6px;background:none;border:none;color:var(--danger);font-size:12px;cursor:pointer;">✕ Remove photo</button>
        </div>
      </div>
      <input type="hidden" id="uap-photo-data" value="">
    </div>

    <!-- SAVE BUTTON -->
    <button class="uap-save-btn" onclick="savePart('${id||''}')">
      ${isEdit ? '✅ Update Part' : '✅ Add to Inventory'}
    </button>
    ${isEdit ? `<button class="uap-delete-btn" onclick="deletePart('${id}')">🗑 Delete Part</button>` : ''}
  `);

  // Init economics display after render; also refresh supplier datalist
  setTimeout(() => { uapAutoPrice(); refreshSupplierDatalist && refreshSupplierDatalist(); }, 50);
}

// Unit pill selection
function uapSelectUnit(key, el) {
  document.querySelectorAll('.uap-unit-pill').forEach(p => p.classList.remove('sel'));
  if (el) el.classList.add('sel');
  const hidden = document.getElementById('uap-unit');
  const customWrap = document.getElementById('uap-unit-custom-wrap');
  if (key === 'custom') {
    if (hidden) hidden.value = '';
    if (customWrap) customWrap.style.display = '';
  } else {
    if (hidden) hidden.value = key;
    if (customWrap) customWrap.style.display = 'none';
  }
  // Update the econ unit label
  const econUnit = document.getElementById('uap-econ-unit');
  if (econUnit) econUnit.textContent = key === 'custom' ? 'unit' : key;
  uapAutoPrice();
}

// Auto-fill prices and live economics from cost
function uapAutoPrice() {
  const cost   = parseFloat((document.getElementById('uap-cost')   ||{}).value) || 0;
  const retail = parseFloat((document.getElementById('uap-instant')||{}).value) || 0;
  const ws     = parseFloat((document.getElementById('uap-credit') ||{}).value) || 0;

  // Auto-suggest retail (30%) and wholesale (15%) if fields are empty/zero
  const retailEl = document.getElementById('uap-instant');
  const wsEl     = document.getElementById('uap-credit');
  if (cost > 0) {
    if (retailEl && (!retailEl.value || parseFloat(retailEl.value) === 0)) retailEl.value = Math.round(cost * 1.30);
    if (wsEl     && (!wsEl.value     || parseFloat(wsEl.value)     === 0)) wsEl.value     = Math.round(cost * 1.15);
  }

  const retailNow = parseFloat((document.getElementById('uap-instant')||{}).value) || 0;
  const wsNow     = parseFloat((document.getElementById('uap-credit') ||{}).value) || 0;

  const mPct    = document.getElementById('uap-margin-pct');
  const mAbs    = document.getElementById('uap-margin-abs');
  const wsPct   = document.getElementById('uap-ws-margin-pct');
  const econBar = document.getElementById('uap-econ-bar');

  if (retailNow > 0 && cost > 0) {
    const marginPct = ((retailNow - cost) / cost * 100).toFixed(1);
    const marginAbs = (retailNow - cost).toFixed(0);
    const wsMarg    = wsNow > 0 ? ((wsNow - cost) / cost * 100).toFixed(1) : '—';
    if (mPct) { mPct.textContent = marginPct + '%'; mPct.className = 'ec-val' + (marginPct < 5 ? ' danger' : marginPct < 15 ? ' warn' : ''); }
    if (mAbs) { mAbs.textContent = '₹' + marginAbs; mAbs.className = 'ec-val' + (marginAbs < 0 ? ' danger' : ''); }
    if (wsPct) { wsPct.textContent = wsMarg !== '—' ? wsMarg + '%' : '—'; wsPct.className = 'ec-val' + (wsMarg !== '—' && wsMarg < 0 ? ' danger' : ''); }
    if (econBar) econBar.className = 'uap-econ-bar' + (marginPct < 0 ? ' danger' : marginPct < 10 ? ' warn' : '');
  } else {
    if (mPct) mPct.textContent = '—';
    if (mAbs) mAbs.textContent = '—';
    if (wsPct) wsPct.textContent = '—';
    if (econBar) econBar.className = 'uap-econ-bar';
  }
}

async function savePart(id) {
  if (isReadOnly()) { toast('🔒 Read-only access'); return; }
  const unit = (document.getElementById('uap-unit-custom') && document.getElementById('uap-unit-custom-wrap') && document.getElementById('uap-unit-custom-wrap').style.display !== 'none')
    ? document.getElementById('uap-unit-custom').value.trim() || 'pcs'
    : (document.getElementById('uap-unit') ? document.getElementById('uap-unit').value || 'pcs' : 'pcs');

  const part = {
    id: id || uid(),
    partNo:       document.getElementById('uap-partNo').value.trim().toUpperCase(),
    name:         document.getElementById('uap-name').value.trim(),
    machineType:  document.getElementById('uap-machineType').value,
    brand:        document.getElementById('uap-brand').value.trim(),
    category:     document.getElementById('uap-category').value.trim(),
    models:       document.getElementById('uap-models').value.trim(),
    unit:         unit,
    costPrice:    +document.getElementById('uap-cost').value    || 0,
    mrp:          +document.getElementById('uap-mrp').value     || 0,
    instantPrice: +document.getElementById('uap-instant').value || 0,
    creditPrice:  +document.getElementById('uap-credit').value  || 0,
    stock:        +document.getElementById('uap-stock').value   || 0,
    minStock:     +document.getElementById('uap-minStock').value|| 0,
    location:     document.getElementById('uap-loc').value.trim().toUpperCase(),
    locationId:   document.getElementById('uap-location-id') ? document.getElementById('uap-location-id').value || null : null,
    supplierName: document.getElementById('uap-supName').value.trim(),
    supplierPhone:document.getElementById('uap-supPhone').value.trim(),
    photoDataUrl: (function(){ var d=document.getElementById('uap-photo-data'); if(d&&d.value==='__CLEAR__') return ''; if(d&&d.value) return d.value; return id ? (getPartById(id)||{}).photoDataUrl||'' : ''; })()
  };
  if (!part.partNo || !part.name) { toast('Part No and Name required'); return; }
  if (!id && state.parts.find(x => x.partNo.toLowerCase() === part.partNo.toLowerCase())) {
    toast('⚠️ Part No already exists — use Edit instead'); return;
  }
  part.updatedAt = Date.now();
  if (!id) part.addedAt = Date.now();

  // ── Firebase Storage photo upload ──────────────────────────
  // If photoDataUrl is base64 (starts with 'data:'), upload to Storage
  // and replace the field with the https:// download URL.
  var photoDataInput = document.getElementById('uap-photo-data');
  var pendingPhoto = photoDataInput ? photoDataInput.value : '';
  if (pendingPhoto === '__CLEAR__') {
    part.photoDataUrl = '';
    if (window.firebaseDeletePartPhoto) {
      window.firebaseDeletePartPhoto(part.id).catch(function(){});
    }
  } else if (pendingPhoto && pendingPhoto.startsWith('data:')) {
    // New base64 photo — upload to Storage in background
    toast('Uploading photo…');
    if (window.firebaseUploadPartPhoto) {
      window.firebaseUploadPartPhoto(part.id, pendingPhoto)
        .then(function(url) {
          var p2 = getPartById(part.id);
          if (p2) { p2.photoDataUrl = url; }
          save();
          try { if (typeof cloudPersistPart === 'function') cloudPersistPart(p2 || part); } catch(e){} // write https URL back to Firestore
          patchInventoryCard(p2 || part);
          toast('✅ Photo saved to cloud');
        })
        .catch(function(e) {
          // Upload failed — keep base64 locally as fallback
          console.warn('Photo upload failed:', e.message);
          toast('Photo saved locally (cloud upload failed)');
        });
    }
  }
  // ─────────────────────────────────────────────────────────

  ensureSupplierRegistered(part.supplierName, part.supplierPhone);

  // Record price history if editing existing part, then carry history forward
  if (id) {
    const existing = getPartById(id);
    if (existing) {
      recordPriceHistory(existing, part, state.session?.name);
      // Carry priceHistory to the new part object so it survives the state.parts[idx] = part overwrite
      if (existing.priceHistory && existing.priceHistory.length) {
        part.priceHistory = existing.priceHistory;
      }
    }
  }

  // Cloud first, then local cache — if cloud fails, local copy is still kept
  const cloud = await cloudPersistPart(part);
  if (id) {
    const idx = state.parts.findIndex(x => x.id === id);
    if (idx >= 0) state.parts[idx] = part;
  } else {
    state.parts.push(part);
  }
  markChanged('parts'); markChanged('suppliers');
  await save(); // only parts+suppliers changed — other keys skipped
  closeModal();
  patchInventoryCard(part, !id); // new parts need full render (sort position unknown); edits patch in-place
  renderHomeDebounced();
  xlsRender && xlsRender();
  logActivity && logActivity((id ? 'Updated' : 'New part added') + ': ' + part.partNo + ' "' + part.name + '"', 'info');
  invalidatePartsIndex(); // only nulls parts lookup index — velocity/home cache stay warm
  _partSaveToast(cloud, id ? '✅ Part updated' : '✅ Part added — ' + state.parts.length + ' SKUs total');

  // If opened from a Purchase bill line, auto-link the new part back to that line
  if (!id && window._bliPendingIndex !== undefined && window._bliPendingIndex !== null) {
    var bi = window._bliPendingIndex;
    window._bliPendingIndex = null;
    if (window._billStage && window._billStage.lineItems && window._billStage.lineItems[bi]) {
      window._billStage.lineItems[bi]._matched     = part;
      window._billStage.lineItems[bi].matchedPartNo = part.partNo;
      window._billStage.lineItems[bi]._newPartAdded = true;
    }
    populateSupplierFilter && populateSupplierFilter();
    var statusDiv = document.getElementById('bli-addstatus-' + bi);
    if (statusDiv) statusDiv.innerHTML = '<span style="font-size:11px;color:#16a34a;font-weight:700;">✓ Added: ' + part.name + ' (' + part.partNo + ') — included in bill</span>';
    var chk = document.getElementById('bli-chk-' + bi); if (chk) chk.checked = true;
    var costEl   = document.getElementById('bli-cost-'   + bi); if (costEl)   costEl.value   = part.costPrice;
    var retailEl = document.getElementById('bli-retail-' + bi); if (retailEl) retailEl.value = part.instantPrice;
    var wsEl     = document.getElementById('bli-ws-'     + bi); if (wsEl)     wsEl.value     = part.creditPrice;
    var rackEl   = document.getElementById('bli-rack-'   + bi); if (rackEl)   rackEl.value   = part.location;
  }
}

async function deletePart(id) {
  if (!confirm('Delete this part permanently?')) return;
  var part = getPartById(id);
  if (window.firestoreDeletePart && window._firebaseAuthed) {
    try { await window.firestoreDeletePart(id); } catch(e) {
      console.warn('firestoreDeletePart:', e.message);
      toast('⚠️ Cloud delete pending — removed on this device');
    }
  }
  partsPendingRemove(id);
  state.parts = state.parts.filter(p => p.id !== id);
  window._ownerDeleteInProgress = true;
  try { await save(); } finally { window._ownerDeleteInProgress = false; }
  if (part && window.firebaseDeletePartPhoto) {
    window.firebaseDeletePartPhoto(id).catch(function(){});
  }
  closeModal();
  renderInventory();
}

function openPartDetail(id) {
  const p = getPartById(id);
  if (!p) return;
  const v = Smart.velocity(p.id);
  const days = Smart.daysUntilStockout(p);
  const reorder = Smart.smartReorderQty(p);
  const cross = Smart.crossSell(p.id);
  showModal(p.name, `
    <div class="part-no" style="font-size:13px;margin-bottom:10px;">${p.partNo}</div>
    ${(window.displayPhotoUrl?window.displayPhotoUrl(p.photoDataUrl):p.photoDataUrl) ? `<div style="text-align:center;margin-bottom:12px;"><img src="${window.displayPhotoUrl?window.displayPhotoUrl(p.photoDataUrl):p.photoDataUrl}" style="max-width:100%;max-height:180px;object-fit:contain;border-radius:10px;border:1px solid var(--border);"></div>` : `<div id="pd-photo-recover" style="text-align:center;margin-bottom:12px;"><button class="btn btn-sm btn-outline" onclick="recoverPartPhotoFromCloud('${p.id}')">🔄 Recover photo from cloud</button></div>`}
    ${p.location ? `<div style="text-align:center;margin-bottom:12px;background:#dbeafe;padding:10px;border-radius:10px;"><div style="font-size:11px;color:#1e40af;text-transform:uppercase;letter-spacing:1px;">📍 Rack Location</div><div class="rack-big">${p.location}</div></div>` : ''}
    <div class="row2">
      <div><div class="label">Stock</div><div class="bold" style="font-size:18px;">${p.stock} ${p.unit||'pcs'}</div></div>
      <div><div class="label">Velocity</div><div class="bold" style="font-size:14px;color:var(--ai)">${v.toFixed(2)}/day</div></div>
    </div>
    ${p.stockOnWay > 0 ? `<div style="background:#ecfeff;border:1px solid #a5f3fc;border-radius:8px;padding:10px 12px;margin:8px 0;display:flex;justify-content:space-between;align-items:center;"><div><div style="font-size:11px;color:#164e63;text-transform:uppercase;letter-spacing:1px;font-weight:600;">Stock on the Way</div><div style="font-size:17px;font-weight:700;color:#0e7490;">+${p.stockOnWay} ${p.unit||'pcs'}</div></div>${p.expectedDays ? '<div style="font-size:13px;color:#164e63;font-weight:600;">Arriving in ~'+p.expectedDays+' days</div>' : ''}</div>` : ''}
    ${v>0 && p.stock>0 ? `<div class="card ai mt-1" style="margin:10px 0;padding:10px;"><b>🧠 AI suggests:</b> Reorder <b>${reorder} pcs</b> (covers 45 days). Stock will run out in <b>${days} days</b>.</div>` : ''}
    <div class="row3">
      <div><div class="label">Cost</div><div class="bold">${rs(p.costPrice)}</div></div>
      <div><div class="label">Instant</div><div class="bold" style="color:var(--primary)">${rs(p.instantPrice)}</div></div>
      <div><div class="label">Credit</div><div class="bold">${rs(p.creditPrice)}</div></div>
    </div>
    <div class="mt-1"><div class="label">Margin</div>${rs(p.instantPrice - p.costPrice)} (${p.costPrice ? ((p.instantPrice-p.costPrice)/p.costPrice*100).toFixed(0) : 0}%)</div>
    <div class="mt-1"><div class="label">Machine</div>${p.machineType} • ${p.brand}</div>
    <div class="mt-1"><div class="label">Models</div>${p.models||'-'}</div>
    <div class="mt-1"><div class="label">Location</div>${p.location||'-'}</div>
    <div class="mt-1"><div class="label">Supplier</div>${p.supplierName||'-'} ${p.supplierPhone?`(${p.supplierPhone})`:''}</div>
    ${cross.length ? `<div class="cross-sell mt-2"><div class="label-ai">🧠 Often bought together</div>${cross.map(c => `<div>• ${c.name} (${c.partNo})</div>`).join('')}</div>` : ''}
    <div class="divider"></div>
    <div class="row2">
      <button class="btn" onclick="closeModal();openAddPart('${id}')">Edit</button>
      <button class="btn btn-warn" onclick="quickAdjust('${id}')">Adjust Stock</button>
    </div>
    ${p.supplierPhone?`<button class="wa-btn mt-1" onclick="orderFromSupplier('${id}')">📲 Order from Supplier</button>`:''}
  `);
}

async function quickAdjust(id) {
  const p = getPartById(id);
  const v = prompt(`Current stock: ${p.stock}\nEnter new stock count:`, p.stock);
  if (v === null) return;
  const newStock = +v || 0;
  const cloud = await cloudPersistPartFields(id, { stock: newStock });
  p.stock = newStock;
  p.updatedAt = Date.now();
  markChanged('parts');
  await save();
  closeModal();
  patchInventoryCard(p); // stock change — in-place patch, no re-sort needed
  renderHomeDebounced();
  _partSaveToast(cloud, '✅ Stock updated');
}

// ============================================================
// COUNTER SALE
// ============================================================
function renderSaleCustomers() {
  const sel = document.getElementById('sale-customer');
  sel.innerHTML = '<option value="">Walk-in Customer (Instant Price)</option>' +
    state.customers.map(c => `<option value="${c.id}">${c.name} ${c.trusted?'★':''} (${c.type})</option>`).join('');
}

// Cache for translated part names: { "partId": "हिंदी नाम" }
const _hindiNameCache = {};
let _hindiTranslating = false;

function renderSaleSearch() {
  const q = (document.getElementById('sale-search').value || '').toLowerCase().trim();
  const div = document.getElementById('sale-search-results');
  if (!q) { div.innerHTML = ''; return; }
  const results = state.parts.filter(p => saleablePart(p) && (
    p.partNo.toLowerCase().includes(q) ||
    p.name.toLowerCase().includes(q) ||
    p.brand.toLowerCase().includes(q)
  )).slice(0, 8);
  if (!results.length) { div.innerHTML = '<div class="empty" style="padding:16px;">No parts found</div>'; return; }

  // Render immediately with English names
  _renderSaleResults(div, results);

  // If Hindi mode is on and API key exists, translate and re-render
  if (currentLang === 'hi' && getAIKey() && getAIKey().startsWith('sk-')) {
    const needTranslation = results.filter(p => !_hindiNameCache[p.id]);
    if (needTranslation.length > 0 && !_hindiTranslating) {
      _translatePartNames(needTranslation).then(() => {
        // Re-render with Hindi names after translation
        if (document.getElementById('sale-search-results') === div) {
          _renderSaleResults(div, results);
        }
      });
    }
  }
}

function _renderSaleResults(div, results) {
  div.innerHTML = results.map(p => {
    const displayName = (currentLang === 'hi' && _hindiNameCache[p.id])
      ? `<div class="part-name">${_hindiNameCache[p.id]}</div><div style="font-size:11px;color:var(--muted);margin-top:1px;">${p.name}</div>`
      : `<div class="part-name">${p.name}</div>`;
    const modelLine = p.models
      ? `<div style="font-size:11px;color:#0f766e;font-weight:600;margin-top:1px;">🔧 ${p.models}</div>`
      : '';
    return `
    <div class="part-item" onclick="addToCart('${p.id}')">
      <div class="part-row">
        <div>
          ${displayName}
          ${modelLine}
          <div class="part-no">${p.partNo} • ${p.brand} • ${p.stock} ${t('sale_in_stock')}</div>
        </div>
        <div class="part-prices">
          <div class="price-instant">${rs(getPrice(p))}</div>
          <small>${t('sale_tap_add')}</small>
        </div>
      </div>
    </div>`;
  }).join('');
}

async function _translatePartNames(parts) {
  if (_hindiTranslating) return;
  _hindiTranslating = true;
  try {
    const names = parts.map(p => p.name);
    const prompt = `Translate these spare parts names from English to Hindi (Devanagari script). Return ONLY a JSON array of translated strings in the same order, no explanation, no markdown.
Parts: ${JSON.stringify(names)}`;

    const resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-api-key': getAIKey(),
        'anthropic-version': '2023-06-01',
        'anthropic-dangerous-direct-browser-access': 'true'
      },
      body: JSON.stringify({
        model: getAIModel(),
        max_tokens: 512,
        messages: [{ role: 'user', content: prompt }]
      })
    });
    if (!resp.ok) return;
    const data = await resp.json();
    const raw = data.content[0].text.trim().replace(/^```json|```$/g, '').trim();
    const translated = JSON.parse(raw);
    parts.forEach((p, i) => {
      if (translated[i]) _hindiNameCache[p.id] = translated[i];
    });
  } catch(e) {
    console.warn('Hindi translation failed:', e);
  } finally {
    _hindiTranslating = false;
  }
}

function getPrice(part) {
  const custId = document.getElementById('sale-customer')?.value;
  const cust = getCustomerById(custId);
  return (cust && cust.trusted && state.payMode === 'credit') ? part.creditPrice : part.instantPrice;
}
// Cart line price: a rate typed on this bill (customPrice) beats the part's default rate
function saleItemPrice(item, part) {
  return (item && item.customPrice != null) ? item.customPrice : getPrice(part);
}
function woItemPrice(item, part) {
  return (item && item.customPrice != null) ? item.customPrice : (part.creditPrice || part.instantPrice);
}

var CONTINUOUS_UNITS=['kg','g','litre','liter','l','ml','ltr','gram'];
function isContinuousUnit(u){return CONTINUOUS_UNITS.indexOf((u||'pcs').toLowerCase().trim())>=0;}
function unitStep(u){return isContinuousUnit(u)?0.5:1;}

function addToCart(partId) {
  const p = getPartById(partId);
  if (!p) return;
  if (p.stock <= 0) { toast(t('sale_out_of_stock_toast')); return; }
  const existing = state.cart.find(i => i.partId === partId);
  if (existing) {
    if (existing.qty >= p.stock) { toast(t('sale_stock_limit')); return; }
    existing.qty++;
  } else {
    state.cart.push({partId, qty:1});
  }
  document.getElementById('sale-search').value = '';
  document.getElementById('sale-search-results').innerHTML = '';
  renderCart();
}

// ─────────────────────────────────────────────
//  PHOTO GRID — tap a photo to add to cart
// ─────────────────────────────────────────────
let _photoGridCategory = 'ALL';

function openPhotoGrid() {
  // Switch to sale tab first if not already there
  if (!document.getElementById('tab-sale').classList.contains('active')) {
    showTab('sale');
  }
  _photoGridCategory = 'ALL';
  document.getElementById('photo-grid-search-input').value = '';
  document.getElementById('photo-grid-btn').classList.add('active');
  document.getElementById('photo-grid-overlay').classList.add('show');
  document.body.style.overflow = 'hidden';
  _buildPhotoGridChips();
  renderPhotoGrid();
  // Show training badge if any learned parts
  const vtStats = VisualTraining.stats();
  const badge = document.getElementById('vt-header-badge');
  const countEl = document.getElementById('vt-header-count');
  if (badge && countEl && vtStats.uniqueParts > 0) {
    countEl.textContent = vtStats.uniqueParts;
    badge.style.display = 'block';
  }
}

function closePhotoGrid() {
  document.getElementById('photo-grid-overlay').classList.remove('show');
  document.getElementById('photo-grid-btn').classList.remove('active');
  document.body.style.overflow = '';
}

// ─────────────────────────────────────────────────────────────────
//  BARCODE SCANNER  (ZXing — live camera viewfinder)
// ─────────────────────────────────────────────────────────────────
let _barcodeReader   = null;
let _barcodeStream   = null;
let _barcodeScanning = false;
let _torchTrack      = null;
let _torchOn         = false;
let _lastBarcode     = '';

async function openBarcodeScanner() {
  const overlay = document.getElementById('barcode-overlay');
  overlay.classList.add('show');
  document.body.style.overflow = 'hidden';
  _lastBarcode = '';
  document.getElementById('barcode-result-banner').classList.remove('show');
  document.getElementById('barcode-result-inner').innerHTML = '';

  // Wait for ZXing to load (it's loaded via script tag)
  await _waitForZxing();
  await _startBarcodeCamera();
}

function _waitForZxing() {
  return _loadLib('zxing');
}

async function _startBarcodeCamera() {
  if (!window.ZXing) { toast('⚠️ Barcode library not loaded — check connection'); return; }
  if (_barcodeScanning) return;

  try {
    const video = document.getElementById('barcode-video');

    // Get camera stream — prefer back camera, high resolution
    _barcodeStream = await navigator.mediaDevices.getUserMedia({
      video: { facingMode: 'environment', width: { ideal: 1280 }, height: { ideal: 720 } }
    });
    video.srcObject = _barcodeStream;
    await video.play();

    // Store track for torch control
    _torchTrack = _barcodeStream.getVideoTracks()[0];

    // Check if torch is supported
    const torchBtn = document.getElementById('barcode-torch-btn');
    const caps = _torchTrack ? _torchTrack.getCapabilities() : {};
    if (!caps.torch) torchBtn.style.display = 'none';

    // Start ZXing continuous decode
    _barcodeReader   = new ZXing.BrowserMultiFormatReader();
    _barcodeScanning = true;

    _barcodeReader.decodeFromVideoElement(video, (result, err) => {
      if (!result) return;
      const code = result.getText();
      if (code === _lastBarcode) return;   // debounce same code
      _lastBarcode = code;
      if (navigator.vibrate) navigator.vibrate([60, 40, 120]);
      _onBarcodeFound(code);
    });

  } catch(err) {
    const msg = err.name === 'NotAllowedError'
      ? 'Camera permission denied. Allow camera access and try again.'
      : 'Camera error: ' + err.message;
    toast('⚠️ ' + msg);
    closeBarcodeScanner();
  }
}

function _onBarcodeFound(code) {
  const banner  = document.getElementById('barcode-result-banner');
  const inner   = document.getElementById('barcode-result-inner');
  const codeClean = code.trim();

  // Try to find in inventory — check partNo, then name, then any field containing the code
  let part = state.parts.find(p =>
    p.partNo.toLowerCase() === codeClean.toLowerCase()
  );
  if (!part) {
    // Partial match — barcode might have prefix/suffix around part number
    part = state.parts.find(p => {
      const pno = p.partNo.toLowerCase();
      const c   = codeClean.toLowerCase();
      return c.includes(pno) || pno.includes(c);
    });
  }
  if (!part) {
    // Try matching against name
    const c = codeClean.toLowerCase();
    part = state.parts.find(p => p.name.toLowerCase().includes(c));
  }

  if (part) {
    const price = getPrice(part);
    const stockColor = part.stock > 0 ? '#6ee7b7' : '#fca5a5';
    inner.innerHTML = `
      <div class="brb-code">📦 ${codeClean}</div>
      <div class="brb-name">${part.name}</div>
      <div class="brb-meta">${part.partNo}${part.brand ? ' · ' + part.brand : ''} · <span style="color:${stockColor};">${part.stock > 0 ? part.stock + ' in stock' : 'OUT OF STOCK'}</span></div>
      <div class="brb-price">${rs(price)}</div>
      ${part.stock > 0
        ? `<button class="barcode-add-btn" onclick="barcodeConfirmAdd('${part.id}')">✓ Add to Bill</button>`
        : `<div class="barcode-fallback-btns">
             <button class="barcode-fallback-btn" onclick="barcodeAddOutOfStock('${part.id}')">Add Anyway</button>
             <button class="barcode-fallback-btn" onclick="_lastBarcode=''">Scan Again</button>
           </div>`
      }`;
  } else {
    // Not found — show code + offer to add part
    window._scanExtracted = { partNo: codeClean, name: '', brand: '', category: '', notes: 'Scanned barcode: ' + codeClean };
    inner.innerHTML = `
      <div class="brb-code">📦 ${codeClean}</div>
      <div class="barcode-not-found">Not found in your inventory</div>
      <div class="barcode-fallback-btns" style="margin-top:8px;">
        <button class="barcode-fallback-btn" onclick="barcodeOpenAddPart()">📦 Add Part + Order</button>
        <button class="barcode-fallback-btn" onclick="_lastBarcode=''">Scan Again</button>
      </div>`;
  }

  banner.classList.add('show');
}

function barcodeConfirmAdd(partId) {
  addToCartFromGrid(partId);
  closeBarcodeScanner();
  toast('✓ Added to bill');
}

function barcodeAddOutOfStock(partId) {
  // Allow adding even if out of stock (might be a mistake in count)
  const p = getPartById(partId);
  if (!p) return;
  const existing = state.cart.find(i => i.partId === partId);
  if (existing) existing.qty++;
  else state.cart.push({ partId, qty: 1 });
  renderCart();
  closeBarcodeScanner();
  toast(`⚠️ ${p.name} added (marked out of stock)`);
}

function barcodeOpenAddPart() {
  closeBarcodeScanner();
  closePhotoGrid();
  setTimeout(() => {
    openAddPart({
      context: '⬛ Scanned barcode — not in inventory',
      prefill: window._scanExtracted || {}
    });
  }, 250);
}

async function toggleTorch() {
  if (!_torchTrack) return;
  try {
    _torchOn = !_torchOn;
    await _torchTrack.applyConstraints({ advanced: [{ torch: _torchOn }] });
    const btn = document.getElementById('barcode-torch-btn');
    btn.classList.toggle('on', _torchOn);
    btn.textContent = _torchOn ? '💡' : '🔦';
  } catch(e) { toast('Torch not supported on this device'); }
}

function closeBarcodeScanner() {
  _barcodeScanning = false;
  _lastBarcode     = '';
  _torchOn         = false;

  if (_barcodeReader) {
    try { _barcodeReader.reset(); } catch(_) {}
    _barcodeReader = null;
  }
  if (_barcodeStream) {
    _barcodeStream.getTracks().forEach(t => t.stop());
    _barcodeStream = null;
  }
  _torchTrack = null;

  const video = document.getElementById('barcode-video');
  if (video) { video.srcObject = null; }

  document.getElementById('barcode-overlay').classList.remove('show');
  document.getElementById('barcode-result-banner').classList.remove('show');
  document.body.style.overflow = '';
}
// ─────────────────────────────────────────────────────────────────

function _buildPhotoGridChips() {
  // Collect unique categories/brands from parts that have photos
  const partsWithPhotos = state.parts.filter(p => p.photoDataUrl);
  const categories = ['ALL', ...new Set(partsWithPhotos.map(p => (p.category || p.brand || 'Other').trim()).filter(Boolean))];
  const chipsDiv = document.getElementById('photo-grid-chips');
  chipsDiv.innerHTML = categories.map(cat => `
    <div class="chip${cat === _photoGridCategory ? ' active' : ''}"
         onclick="setPhotoGridCategory('${cat.replace(/'/g,"\\'")}')">
      ${cat === 'ALL' ? '🔍 All' : cat}
    </div>`).join('');
}

function setPhotoGridCategory(cat) {
  _photoGridCategory = cat;
  _buildPhotoGridChips();
  renderPhotoGrid();
}

function renderPhotoGrid() {
  const q = (document.getElementById('photo-grid-search-input').value || '').toLowerCase().trim();
  const grid = document.getElementById('photo-grid-items');
  const countEl = document.getElementById('photo-grid-count');

  let parts = state.parts;

  // Filter by category chip
  if (_photoGridCategory !== 'ALL') {
    parts = parts.filter(p => (p.category || p.brand || 'Other') === _photoGridCategory);
  }

  // Filter by search query
  if (q) {
    parts = parts.filter(p =>
      p.name.toLowerCase().includes(q) ||
      p.brand.toLowerCase().includes(q) ||
      p.partNo.toLowerCase().includes(q)
    );
  }

  // Parts WITH photos first, then parts without photos (shown as emoji placeholder)
  const withPhoto = parts.filter(p => p.photoDataUrl);
  const withoutPhoto = parts.filter(p => !p.photoDataUrl);
  const ordered = [...withPhoto, ...withoutPhoto];

  const total = ordered.length;
  const photoCount = withPhoto.length;
  countEl.textContent = total
    ? `${photoCount} with photo · ${total - photoCount} text-only`
    : 'No parts found';

  if (!total) {
    grid.innerHTML = '<div class="empty" style="padding:32px;grid-column:1/-1">No parts found</div>';
    return;
  }

  grid.innerHTML = ordered.map(p => {
    const inCart = state.cart.find(i => i.partId === p.id);
    const cartQty = inCart ? inCart.qty : 0;
    const outOfStock = p.stock <= 0;
    const price = getPrice(p);

    const imgHtml = (window.displayPhotoUrl?window.displayPhotoUrl(p.photoDataUrl):p.photoDataUrl)
      ? `<img class="pgrid-img" src="${window.displayPhotoUrl?window.displayPhotoUrl(p.photoDataUrl):p.photoDataUrl}" alt="${p.name}" loading="lazy" onerror="this.parentElement.innerHTML='<div class=\\'pgrid-no-img\\'>🔧</div>'">`
      : `<div class="pgrid-no-img">🔧</div>`;

    return `
    <div class="pgrid-card${outOfStock ? ' pgrid-out' : ''}${inCart ? ' in-cart' : ''}"
         onclick="addToCartFromGrid('${p.id}')" id="pgcard-${p.id}">
      ${imgHtml}
      <div class="pgrid-cart-badge${cartQty > 0 ? ' show' : ''}" id="pgbadge-${p.id}">${cartQty}</div>
      <div class="pgrid-info">
        <div class="pgrid-name">${p.name}</div>
        <div class="pgrid-price">${rs(price)}</div>
        <div class="pgrid-stock">${p.stock > 0 ? p.stock + ' left' : 'Out of stock'}</div>
      </div>
    </div>`;
  }).join('');
}

function addToCartFromGrid(partId) {
  const p = getPartById(partId);
  if (!p) return;
  if (p.stock <= 0) { toast(t('sale_out_of_stock_toast')); return; }

  const existing = state.cart.find(i => i.partId === partId);
  if (existing) {
    if (existing.qty >= p.stock) { toast(t('sale_stock_limit')); return; }
    existing.qty++;
  } else {
    state.cart.push({ partId, qty: 1 });
  }

  renderCart();

  // Update the card badge in place without full re-render
  const newQty = state.cart.find(i => i.partId === partId)?.qty || 0;
  const badge = document.getElementById('pgbadge-' + partId);
  const card  = document.getElementById('pgcard-' + partId);
  if (badge) { badge.textContent = newQty; badge.classList.add('show'); }
  if (card)  { card.classList.add('in-cart'); }

  // Quick haptic + toast
  if (navigator.vibrate) navigator.vibrate(40);
  toast(`✓ ${p.name} added (×${newQty})`);
}

// ─────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
//  SCAN MEMORY — exact hash memory (legacy, kept for backward compat)
// ═══════════════════════════════════════════════════════════════════
const ScanMemory = {
  KEY: 'goury_scan_memory',
  load() { try { return JSON.parse(localStorage.getItem(this.KEY) || '[]'); } catch(_) { return []; } },
  save(records) { try { localStorage.setItem(this.KEY, JSON.stringify(records.slice(-2000))); } catch(_) {} },
  remember(photoHash, partId, filters) {
    const records = this.load();
    if (!records.find(r => r.photoHash === photoHash && r.partId === partId)) {
      records.push({ photoHash, partId, ts: Date.now(), ...filters });
      this.save(records);
    }
  },
  findByHash(photoHash) { return this.load().filter(r => r.photoHash === photoHash).map(r => r.partId); },
  getKnownFilters() {
    const records = this.load();
    return {
      categories: [...new Set(records.map(r => r.category).filter(Boolean))],
      brands:     [...new Set(records.map(r => r.brand).filter(Boolean))],
      machines:   [...new Set(records.map(r => r.machine).filter(Boolean))]
    };
  },
  count() { return this.load().length; }
};

// ═══════════════════════════════════════════════════════════════════
//  VISUAL TRAINING DATABASE — AI learns from every confirmed scan
//  Stores rich feature vectors so future unknown parts auto-match
//  Format: [{id, partId, partName, partNo, brand, category,
//            colorVec, shapeKeywords, textureKeywords, partCodes,
//            dimensions, material, confirmCount, lastSeen, photoHash}]
// ═══════════════════════════════════════════════════════════════════
const VisualTraining = {
  // Reads/writes state.visualTraining — syncs via Firestore like all other data

  _get() { return state.visualTraining || (state.visualTraining = []); },

  // Store a new confirmed match with AI-extracted features
  train(partId, features, photoHash) {
    const records = this._get();
    const part = getPartById(partId);
    if (!part) return;

    // Bump confirm count if exact photo+part combo already known
    const existing = records.find(r => r.photoHash === photoHash && r.partId === partId);
    if (existing) {
      existing.confirmCount = (existing.confirmCount || 1) + 1;
      existing.lastSeen = Date.now();
      save();
      return existing;
    }

    // Same part, same features — avoid over-duplicating
    const samePartSimilar = records.find(r =>
      r.partId === partId &&
      r.category === (features.category || part.category) &&
      JSON.stringify(r.colorVec) === JSON.stringify(features.colorVec)
    );
    if (samePartSimilar) {
      samePartSimilar.confirmCount = (samePartSimilar.confirmCount || 1) + 1;
      samePartSimilar.lastSeen = Date.now();
      save();
      return samePartSimilar;
    }

    const record = {
      id:              'vt_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6),
      partId,
      partName:        part.name,
      partNo:          part.partNo,
      brand:           part.brand || '',
      category:        features.category || part.category || '',
      colorVec:        features.colorVec || [],
      shapeKeywords:   features.shapeKeywords || [],
      textureKeywords: features.textureKeywords || [],
      partCodes:       features.partCodes || [],
      dimensions:      features.dimensions || '',
      material:        features.material || '',
      dominantColors:  features.dominantColors || [],
      confirmCount:    1,
      lastSeen:        Date.now(),
      photoHash:       photoHash || null
    };

    state.visualTraining.push(record);
    // Trim to 5000 most confirmed+recent
    if (state.visualTraining.length > 5000) {
      state.visualTraining.sort((a, b) =>
        (b.confirmCount * 1000 + b.lastSeen) - (a.confirmCount * 1000 + a.lastSeen)
      );
      state.visualTraining = state.visualTraining.slice(0, 5000);
    }
    save();
    return record;
  },

  // Find best matching parts by feature similarity score
  // Returns [{partId, score, reason, record}] sorted by score desc
  findSimilar(features, topN) {
    topN = topN || 5;
    const records = this._get();
    if (!records.length) return [];

    const scored = records.map(r => {
      let score = 0;
      const reasons = [];

      // Category match (strong signal)
      if (features.category && r.category &&
          features.category.toLowerCase() === r.category.toLowerCase()) {
        score += 30;
        reasons.push('same category');
      } else if (features.category && r.category &&
                 (features.category.toLowerCase().includes(r.category.toLowerCase()) ||
                  r.category.toLowerCase().includes(features.category.toLowerCase()))) {
        score += 15;
        reasons.push('related category');
      }

      // Brand match
      if (features.brand && r.brand &&
          features.brand.toLowerCase() === r.brand.toLowerCase()) {
        score += 20;
        reasons.push('same brand');
      }

      // Part code / text match (very strong)
      if (features.partCodes && features.partCodes.length && r.partCodes && r.partCodes.length) {
        const fCodes = features.partCodes.map(c => String(c).toLowerCase());
        const rCodes = r.partCodes.map(c => String(c).toLowerCase());
        const codeMatch = fCodes.some(fc => rCodes.some(rc => fc === rc || fc.includes(rc) || rc.includes(fc)));
        if (codeMatch) { score += 50; reasons.push('matching part code'); }
      }

      // Shape keyword overlap
      if (features.shapeKeywords && r.shapeKeywords) {
        const fk = features.shapeKeywords.map(k => k.toLowerCase());
        const rk = r.shapeKeywords.map(k => k.toLowerCase());
        const overlap = fk.filter(k => rk.includes(k)).length;
        if (overlap > 0) { score += overlap * 8; reasons.push(overlap + ' shape matches'); }
      }

      // Material match
      if (features.material && r.material &&
          features.material.toLowerCase().includes(r.material.toLowerCase())) {
        score += 10;
        reasons.push('same material');
      }

      // Color similarity (cosine-like on RGB vec)
      if (features.colorVec && features.colorVec.length >= 3 &&
          r.colorVec && r.colorVec.length >= 3) {
        const dot = features.colorVec.slice(0,3).reduce((s,v,i) => s + v * r.colorVec[i], 0);
        const magA = Math.sqrt(features.colorVec.slice(0,3).reduce((s,v) => s + v*v, 0));
        const magB = Math.sqrt(r.colorVec.slice(0,3).reduce((s,v) => s + v*v, 0));
        if (magA > 0 && magB > 0) {
          const sim = dot / (magA * magB);
          if (sim > 0.97) { score += 15; reasons.push('same colour'); }
          else if (sim > 0.92) { score += 8; reasons.push('similar colour'); }
        }
      }

      // Boost by confirmation count (more confirmations = more reliable)
      score += Math.min(r.confirmCount * 3, 20);

      return { partId: r.partId, score, reason: reasons.join(', '), record: r };
    });

    // Deduplicate by partId (keep highest score per part)
    const best = {};
    scored.forEach(s => {
      if (!best[s.partId] || best[s.partId].score < s.score) best[s.partId] = s;
    });

    return Object.values(best)
      .filter(s => s.score >= 20)  // minimum threshold
      .sort((a, b) => b.score - a.score)
      .slice(0, topN);
  },

  // Stats for display
  stats() {
    const records = this._get();
    const totalConfirms = records.reduce((s, r) => s + (r.confirmCount || 1), 0);
    const uniqueParts   = new Set(records.map(r => r.partId)).size;
    return { records: records.length, uniqueParts, totalConfirms };
  },

  // Export all training data as JSON
  export() { return JSON.stringify(this._get(), null, 2); },

  // Import training data (merge, don't overwrite)
  import(jsonStr) {
    try {
      const incoming = JSON.parse(jsonStr);
      if (!Array.isArray(incoming)) return false;
      const records = this._get();
      const existingIds = new Set(records.map(r => r.id));
      const newRecords = incoming.filter(r => r.id && !existingIds.has(r.id));
      state.visualTraining.push(...newRecords);
      save();
      return newRecords.length;
    } catch(_) { return false; }
  }
};

// ─────────────────────────────────────────────────────────────────
// AI FEATURE EXTRACTOR — extracts rich features from a part image
// Called async; returns feature object to store in VisualTraining
// ─────────────────────────────────────────────────────────────────
async function extractVisualFeatures(base64, mimeType, aiCallFn, existingExtract) {
  // Quick color histogram from canvas (no AI needed)
  async function getColorVec(dataUrl) {
    return new Promise(resolve => {
      const img = new Image();
      img.onload = function() {
        try {
          const c = document.createElement('canvas');
          c.width = 32; c.height = 32;
          const ctx = c.getContext('2d');
          ctx.drawImage(img, 0, 0, 32, 32);
          const d = ctx.getImageData(0, 0, 32, 32).data;
          let r = 0, g = 0, b = 0, n = 0;
          for (let i = 0; i < d.length; i += 4) {
            if (d[i+3] < 10) continue; // skip transparent
            r += d[i]; g += d[i+1]; b += d[i+2]; n++;
          }
          if (n === 0) return resolve([128, 128, 128]);
          resolve([Math.round(r/n), Math.round(g/n), Math.round(b/n)]);
        } catch(_) { resolve([128, 128, 128]); }
      };
      img.onerror = () => resolve([128, 128, 128]);
      img.src = 'data:' + mimeType + ';base64,' + base64;
    });
  }

  const colorVec = await getColorVec('data:' + mimeType + ';base64,' + base64);

  // If we already have an aiExtract from step 1, use it; otherwise do a fresh call
  let aiFeatures = existingExtract || {};
  if (!aiFeatures.shapeKeywords) {
    try {
      const raw = await aiCallFn([{
        role: 'user',
        content: [
          { type: 'image', source: { type: 'base64', media_type: mimeType, data: base64 } },
          { type: 'text', text:
            'You are a spare parts visual expert. Extract training features from this part photo.\n' +
            'Return ONLY valid JSON (no markdown):\n' +
            '{\n' +
            '  "shapeKeywords": ["round","flat","ring","cylindrical","hexagonal","L-shaped","T-shaped","disc","rod","bracket","gear","belt","chain","filter","seal","bearing","spring","bolt","nut","pulley"],\n' +
            '  "textureKeywords": ["smooth","ribbed","threaded","knurled","rough","polished","rubber","metal","cast","stamped","corrugated"],\n' +
            '  "partCodes": ["any numbers or codes visible on part, exact"],\n' +
            '  "material": "metal/rubber/plastic/composite/ceramic",\n' +
            '  "dimensions": "approx size e.g. 30mm diameter / 200mm long / small bracket",\n' +
            '  "dominantColors": ["silver","black","red","yellow","blue","orange","green","brown","white"],\n' +
            '  "category": "bearing/belt/filter/seal/gear/pump/bolt/gasket/shaft/pulley/sprocket/valve/pipe/bracket/other",\n' +
            '  "brand": "brand name if visible else empty string"\n' +
            '}\n' +
            'shapeKeywords: pick 2-4 that best describe the shape.\n' +
            'textureKeywords: pick 1-2.\n' +
            'partCodes: extract ALL stamped/printed codes (part numbers, size markings like 6205, SKF, etc).\n' +
            'If no codes visible, return empty array.\n' +
            'Be concise and factual.'
          }
        ]
      }], 400);
      const s = raw.indexOf('{'), e = raw.lastIndexOf('}');
      if (s >= 0 && e >= 0) aiFeatures = { ...aiFeatures, ...JSON.parse(raw.slice(s, e+1)) };
    } catch(_) {}
  }

  return {
    colorVec,
    shapeKeywords:   aiFeatures.shapeKeywords   || [],
    textureKeywords: aiFeatures.textureKeywords || [],
    partCodes:       aiFeatures.partCodes       || (aiFeatures.texts || []),
    material:        aiFeatures.material        || '',
    dimensions:      aiFeatures.dimensions      || '',
    dominantColors:  aiFeatures.dominantColors  || [],
    category:        aiFeatures.category        || '',
    brand:           aiFeatures.brand           || ''
  };
}

// Simple perceptual hash of an image via canvas (8x8 grayscale average)
function photoHash(dataUrl) {
  return new Promise(resolve => {
    try {
      const img = new Image();
      img.onload = function() {
        try {
          const c = document.createElement('canvas');
          c.width = 8; c.height = 8;
          const ctx = c.getContext('2d');
          ctx.drawImage(img, 0, 0, 8, 8);
          const d = ctx.getImageData(0, 0, 8, 8).data;
          let hash = '';
          for (let i = 0; i < d.length; i += 4) {
            hash += Math.round((d[i] + d[i+1] + d[i+2]) / 3).toString(16).padStart(2,'0');
          }
          resolve(hash);
        } catch(_) { resolve(null); }
      };
      img.onerror = () => resolve(null);
      img.src = dataUrl;
    } catch(_) { resolve(null); }
  });
}

// Global scan session state
window._scanSession = null;

//  PART SCAN — point camera → AI identifies → add to cart
// ─────────────────────────────────────────────
function triggerPartScan() {
  const key = getAIKey();
  if (!key || !key.startsWith('sk-')) {
    toast('⚠️ AI key needed — go to Settings → AI Settings');
    return;
  }
  document.getElementById('part-scan-input').value = '';
  document.getElementById('part-scan-input').click();
}

async function handlePartScanPhoto(input) {
  if (!input.files || !input.files[0]) return;
  const file = input.files[0];

  const scanBar   = document.getElementById('scan-result-bar');
  const statusTxt = document.getElementById('scan-status-txt');
  const matchArea = document.getElementById('scan-match-area');
  const scanBtn   = document.getElementById('photo-grid-scan-btn');

  scanBar.style.display = '';
  matchArea.innerHTML = '';
  statusTxt.textContent = '📷 Reading part\u2026';
  scanBtn.classList.add('scanning');
  document.querySelector('.photo-grid-body').scrollTop = 0;

  const reader = new FileReader();
  reader.onload = async function(e) {
    // Re-encode through a canvas first: converts HEIC/PNG/etc → JPEG and caps
    // the size, exactly like the add-part photo path. Raw camera files (iPhone
    // HEIC, multi-MB images) are rejected by the API — that re-encode is the
    // difference between this path and the ones that work.
    let dataUrl, base64, mimeType = 'image/jpeg';
    try {
      dataUrl = await new Promise((resolve, reject) => {
        const img = new Image();
        img.onload = function() {
          const MAX = 1600;
          const scale = img.width > MAX ? MAX / img.width : 1;
          const canvas = document.createElement('canvas');
          canvas.width  = Math.round(img.width  * scale);
          canvas.height = Math.round(img.height * scale);
          canvas.getContext('2d').drawImage(img, 0, 0, canvas.width, canvas.height);
          resolve(canvas.toDataURL('image/jpeg', 0.85));
        };
        img.onerror = () => reject(new Error('Could not read image (unsupported format?)'));
        img.src = e.target.result;
      });
    } catch (imgErr) {
      scanBtn.classList.remove('scanning');
      statusTxt.textContent = '\u274C ' + imgErr.message;
      return;
    }
    base64 = dataUrl.split(',')[1];
    const key = getAIKey();

    async function callClaude(messages, maxTokens) {
      // Do NOT swallow errors — let them surface to the outer catch so a real
      // failure (bad model, locked key, network) is visible instead of looking
      // like "the part just wasn't recognised".
      return await aiCall({ messages: messages, maxTokens: maxTokens });
    }

    function urlToBase64(url) {
      return new Promise(resolve => {
        if (!url) return resolve(null);
        if (url.startsWith('data:')) {
          const mime = url.split(';')[0].split(':')[1] || 'image/jpeg';
          return resolve({ base64: url.split(',')[1], mimeType: mime });
        }
        const img = new Image();
        img.crossOrigin = 'anonymous';
        img.onload = function() {
          try {
            const MAX = 512;
            let w = img.naturalWidth, h = img.naturalHeight;
            if (w > MAX || h > MAX) {
              if (w > h) { h = Math.round(h * MAX / w); w = MAX; }
              else       { w = Math.round(w * MAX / h); h = MAX; }
            }
            const canvas = document.createElement('canvas');
            canvas.width = w; canvas.height = h;
            canvas.getContext('2d').drawImage(img, 0, 0, w, h);
            const dUrl = canvas.toDataURL('image/jpeg', 0.80);
            resolve({ base64: dUrl.split(',')[1], mimeType: 'image/jpeg' });
          } catch(err) { resolve(null); }
        };
        img.onerror = () => resolve(null);
        img.src = url;
      });
    }

    try {
      // STEP 0: Check scan memory for instant match
      statusTxt.textContent = '\u{1F9E0} Checking memory\u2026';
      const hash = await photoHash(dataUrl);
      if (hash) {
        const remembered = ScanMemory.findByHash(hash);
        for (const pid of remembered) {
          const p = getPartById(pid);
          if (p) {
            scanBtn.classList.remove('scanning');
            _renderScanFinalMatch(p, '\u{1F9E0} Remembered from last time', 'high', hash, {});
            return;
          }
        }
      }

      // STEP 1: AI extract category/brand/text
      statusTxt.textContent = '\u{1F50D} Analysing part\u2026';
      const knownCategories = [...new Set(state.parts.map(p => (p.category||p.productType||'')).filter(Boolean))].slice(0,30);
      const knownBrands     = [...new Set(state.parts.map(p => p.brand).filter(Boolean))].slice(0,40);

      const extractPrompt =
        'You are a spare parts expert. Look at this photo of a mechanical spare part.\n\n' +
        'Extract every detail:\n' +
        '1. Any text, numbers, codes stamped or printed on the part\n' +
        '2. The part category\n' +
        '3. Brand name if visible\n' +
        '4. Physical description: shape, size, colour, material, features\n\n' +
        'Known categories in this shop: ' + (knownCategories.join(', ') || 'bearing, belt, filter, seal, gear, pump, sprocket') + '\n' +
        'Known brands: ' + (knownBrands.join(', ') || 'SKF, FAG, Gates, Bosch, Mahle') + '\n\n' +
        'Return ONLY valid JSON:\n' +
        '{"texts":[],"category":"","brand":"","description":"physical description in 10 words","confidence":"high/medium/low"}\n\n' +
        'For parts with NO text (bearings, belts, filters): still fill category and description from visual shape.\n' +
        'Do not invent numbers.';

      const extractRaw = await callClaude([{
        role: 'user',
        content: [
          { type: 'image', source: { type: 'base64', media_type: mimeType, data: base64 } },
          { type: 'text', text: extractPrompt }
        ]
      }], 500);

      let aiExtract = { texts: [], category: '', brand: '', description: '', confidence: 'low' };
      try {
        const s = extractRaw.indexOf('{'), ee = extractRaw.lastIndexOf('}');
        if (s >= 0 && ee >= 0) aiExtract = { ...aiExtract, ...JSON.parse(extractRaw.slice(s, ee+1)) };
      } catch(_) {}

      const texts = (aiExtract.texts || []).map(t => String(t).toLowerCase().trim()).filter(Boolean);

      // STEP 2: Fast text match
      let textMatchedPart = null;
      if (texts.length > 0) {
        for (const t of texts) {
          const f = state.parts.find(p => p.partNo.toLowerCase() === t);
          if (f) { textMatchedPart = f; break; }
        }
        if (!textMatchedPart) {
          for (const p of state.parts) {
            const pno = p.partNo.toLowerCase();
            if (pno.length >= 3 && texts.some(t => t.includes(pno) || pno.includes(t))) {
              textMatchedPart = p; break;
            }
          }
        }
      }

      if (textMatchedPart) {
        scanBtn.classList.remove('scanning');
        _renderScanFinalMatch(textMatchedPart, '\u{1F3AF} Part number matched', 'high', hash,
          { category: aiExtract.category, brand: aiExtract.brand });
        return;
      }

      // STEP 1.5: Query VisualTraining DB for learned matches
      statusTxt.textContent = '\u{1F9E0} Checking learned parts\u2026';
      const vtStats = VisualTraining.stats();
      if (vtStats.records > 0) {
        // Quick color extraction (no AI call needed)
        const colorVec = await (async function() {
          return new Promise(resolve => {
            const img2 = new Image();
            img2.onload = function() {
              try {
                const c = document.createElement('canvas'); c.width=32; c.height=32;
                const ctx = c.getContext('2d'); ctx.drawImage(img2,0,0,32,32);
                const d = ctx.getImageData(0,0,32,32).data;
                let r=0,g=0,b=0,n=0;
                for(let i=0;i<d.length;i+=4){if(d[i+3]<10)continue;r+=d[i];g+=d[i+1];b+=d[i+2];n++;}
                if(n===0)return resolve([128,128,128]);
                resolve([Math.round(r/n),Math.round(g/n),Math.round(b/n)]);
              } catch(_){resolve([128,128,128]);}
            };
            img2.onerror = ()=>resolve([128,128,128]);
            img2.src = dataUrl;
          });
        })();

        const vtFeatures = {
          colorVec,
          category:   aiExtract.category  || '',
          brand:      aiExtract.brand      || '',
          partCodes:  aiExtract.texts      || [],
          shapeKeywords: []
        };
        const vtMatches = VisualTraining.findSimilar(vtFeatures, 3);
        if (vtMatches.length > 0 && vtMatches[0].score >= 50) {
          const topMatch = vtMatches[0];
          const p = getPartById(topMatch.partId);
          if (p) {
            scanBtn.classList.remove('scanning');
            const confidence = topMatch.score >= 80 ? 'high' : topMatch.score >= 55 ? 'medium' : 'low';
            _renderScanFinalMatch(p,
              '\u{1F9E0} Learned match \u00B7 ' + topMatch.reason,
              confidence, hash,
              { category: aiExtract.category, brand: aiExtract.brand }
            );
            // Show alternate suggestions below if score is not high
            if (confidence !== 'high' && vtMatches.length > 1) {
              const altParts = vtMatches.slice(1).map(m => getPartById(m.partId)).filter(Boolean);
              if (altParts.length) {
                const matchArea2 = document.getElementById('scan-match-area');
                if (matchArea2) {
                  matchArea2.innerHTML += '<div style="margin-top:8px;font-size:10px;color:rgba(255,255,255,.45);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px;">Also possibly:</div>' +
                    _renderCandidateList(altParts, hash, {});
                }
              }
            }
            return;
          }
        }
      }

      // STEP 3: Progressive narrowing — ask user to confirm filters
      scanBtn.classList.remove('scanning');
      window._scanSession = {
        dataUrl, base64, mimeType, hash, aiExtract,
        filters: { category: aiExtract.category||'', brand: aiExtract.brand||'', machine: '' }
      };
      _renderScanFilterStep('category');

    } catch(err) {
      scanBtn.classList.remove('scanning');
      statusTxt.textContent = '\u274C Scan failed \u2014 check connection';
      matchArea.innerHTML = '<div class="scan-no-match">' + (err.message || 'Network error') + '</div>';
    }
    input.value = '';
  };
  reader.readAsDataURL(file);
}

function _renderScanFilterStep(step) {
  const statusTxt = document.getElementById('scan-status-txt');
  const matchArea = document.getElementById('scan-match-area');
  const sess = window._scanSession;
  if (!sess) return;
  const { aiExtract, filters } = sess;

  let pool = state.parts;
  if (filters.category) pool = pool.filter(p => (p.category||p.productType||p.name||'').toLowerCase().includes(filters.category.toLowerCase()));
  if (filters.brand)    pool = pool.filter(p => (p.brand||'').toLowerCase().includes(filters.brand.toLowerCase()));
  if (filters.machine)  pool = pool.filter(p => (p.name+' '+(p.notes||'')).toLowerCase().includes(filters.machine.toLowerCase()));

  if (step === 'category') {
    statusTxt.textContent = 'AI sees: ' + (aiExtract.description || 'a mechanical part') + ' (' + pool.length + ' parts)';
    const catCounts = {};
    state.parts.forEach(p => { const c=(p.category||p.productType||'').trim(); if(c) catCounts[c]=(catCounts[c]||0)+1; });
    const topCats = Object.entries(catCounts).sort((a,b)=>b[1]-a[1]).slice(0,8).map(e=>e[0]);
    const aiCat = aiExtract.category;
    const allCats = aiCat && !topCats.includes(aiCat) ? [aiCat,...topCats] : topCats;

    matchArea.innerHTML =
      '<div style="padding:4px 0 8px;">' +
      '<div style="font-size:11px;color:rgba(255,255,255,.5);text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px;">' +
      (aiCat ? 'AI guessed: <b style="color:#a5f3fc;">'+aiCat+'</b> \u2014 confirm or pick correct' : 'What type of part is this?') +
      '</div>' +
      '<div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:10px;">' +
      allCats.map(c =>
        '<button onclick="_scanConfirmFilter(\'category\',\''+c.replace(/'/g,"\\'")+'\')" '+
        'style="padding:7px 12px;border-radius:20px;border:1.5px solid '+(c===aiCat?'#a5f3fc':'rgba(255,255,255,.3)')+';'+
        'background:'+(c===aiCat?'rgba(165,243,252,.15)':'rgba(255,255,255,.08)')+';'+
        'color:'+(c===aiCat?'#a5f3fc':'white')+';font-size:12px;font-weight:600;cursor:pointer;">'+c+'</button>'
      ).join('') +
      '</div>' +
      '<div style="display:flex;gap:6px;">' +
      '<input id="scan-cat-custom" placeholder="Type category\u2026" style="flex:1;padding:8px 10px;border-radius:8px;border:1px solid rgba(255,255,255,.3);background:rgba(255,255,255,.1);color:white;font-size:13px;" onkeydown="if(event.key===\'Enter\')_scanConfirmFilter(\'category\',this.value)">' +
      '<button onclick="_scanConfirmFilter(\'category\',document.getElementById(\'scan-cat-custom\').value)" style="padding:8px 12px;border-radius:8px;background:#0f766e;color:white;border:none;font-size:13px;font-weight:600;cursor:pointer;">OK</button>' +
      '<button onclick="_scanConfirmFilter(\'category\',\'\')" style="padding:8px 10px;border-radius:8px;background:rgba(255,255,255,.08);color:rgba(255,255,255,.6);border:1px solid rgba(255,255,255,.2);font-size:12px;cursor:pointer;">Skip</button>' +
      '</div></div>';

  } else if (step === 'brand') {
    statusTxt.textContent = (filters.category||'Part') + ' \u2014 ' + pool.length + ' matches. Which brand?';
    const brandCounts = {};
    pool.forEach(p => { if(p.brand) brandCounts[p.brand]=(brandCounts[p.brand]||0)+1; });
    const topBrands = Object.entries(brandCounts).sort((a,b)=>b[1]-a[1]).slice(0,8).map(e=>e[0]);
    const aiBrand = aiExtract.brand;
    const allBrands = aiBrand && !topBrands.includes(aiBrand) ? [aiBrand,...topBrands] : topBrands;

    matchArea.innerHTML =
      '<div style="padding:4px 0 8px;">' +
      '<div style="font-size:11px;color:rgba(255,255,255,.5);text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px;">' +
      (aiBrand ? 'AI saw brand: <b style="color:#a5f3fc;">'+aiBrand+'</b> \u2014 confirm or pick' : 'Which brand?') +
      '</div>' +
      '<div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:10px;">' +
      allBrands.map(b =>
        '<button onclick="_scanConfirmFilter(\'brand\',\''+b.replace(/'/g,"\\'")+'\')" '+
        'style="padding:7px 12px;border-radius:20px;border:1.5px solid '+(b===aiBrand?'#a5f3fc':'rgba(255,255,255,.3)')+';'+
        'background:'+(b===aiBrand?'rgba(165,243,252,.15)':'rgba(255,255,255,.08)')+';'+
        'color:'+(b===aiBrand?'#a5f3fc':'white')+';font-size:12px;font-weight:600;cursor:pointer;">'+b+'</button>'
      ).join('') +
      '</div>' +
      '<div style="display:flex;gap:6px;">' +
      '<input id="scan-brand-custom" placeholder="Type brand\u2026" style="flex:1;padding:8px 10px;border-radius:8px;border:1px solid rgba(255,255,255,.3);background:rgba(255,255,255,.1);color:white;font-size:13px;" onkeydown="if(event.key===\'Enter\')_scanConfirmFilter(\'brand\',this.value)">' +
      '<button onclick="_scanConfirmFilter(\'brand\',document.getElementById(\'scan-brand-custom\').value)" style="padding:8px 12px;border-radius:8px;background:#0f766e;color:white;border:none;font-size:13px;font-weight:600;cursor:pointer;">OK</button>' +
      '<button onclick="_scanConfirmFilter(\'brand\',\'\')" style="padding:8px 10px;border-radius:8px;background:rgba(255,255,255,.08);color:rgba(255,255,255,.6);border:1px solid rgba(255,255,255,.2);font-size:12px;cursor:pointer;">Skip</button>' +
      '</div></div>';

  } else if (step === 'machine') {
    statusTxt.textContent = '\u2699\uFE0F ' + pool.length + ' parts narrowed. Which machine?';
    const machineWords = {};
    pool.forEach(p => {
      (p.name+' '+(p.notes||'')).toLowerCase().split(/[\s,\/\-]+/).filter(w=>w.length>3)
        .forEach(w => machineWords[w]=(machineWords[w]||0)+1);
    });
    const topMachines = Object.entries(machineWords).sort((a,b)=>b[1]-a[1])
      .filter(([w])=>!['part','piece','unit','type','size','with','from','for'].includes(w))
      .slice(0,8).map(e=>e[0]);

    matchArea.innerHTML =
      '<div style="padding:4px 0 8px;">' +
      '<div style="font-size:11px;color:rgba(255,255,255,.5);text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px;">Which machine or equipment?</div>' +
      (topMachines.length ? '<div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:10px;">' +
        topMachines.map(m => '<button onclick="_scanConfirmFilter(\'machine\',\''+m.replace(/'/g,"\\'")+'\') " style="padding:7px 12px;border-radius:20px;border:1.5px solid rgba(255,255,255,.3);background:rgba(255,255,255,.08);color:white;font-size:12px;font-weight:600;cursor:pointer;">'+m+'</button>').join('') +
        '</div>' : '') +
      '<div style="display:flex;gap:6px;">' +
      '<input id="scan-machine-custom" placeholder="e.g. rice mill, tractor, generator\u2026" style="flex:1;padding:8px 10px;border-radius:8px;border:1px solid rgba(255,255,255,.3);background:rgba(255,255,255,.1);color:white;font-size:13px;" onkeydown="if(event.key===\'Enter\')_scanConfirmFilter(\'machine\',this.value)">' +
      '<button onclick="_scanConfirmFilter(\'machine\',document.getElementById(\'scan-machine-custom\').value)" style="padding:8px 12px;border-radius:8px;background:#0f766e;color:white;border:none;font-size:13px;font-weight:600;cursor:pointer;">OK</button>' +
      '<button onclick="_scanConfirmFilter(\'machine\',\'\')" style="padding:8px 10px;border-radius:8px;background:rgba(255,255,255,.08);color:rgba(255,255,255,.6);border:1px solid rgba(255,255,255,.2);font-size:12px;cursor:pointer;">Skip</button>' +
      '</div></div>';
  }
}

function _scanConfirmFilter(filterKey, value) {
  const sess = window._scanSession;
  if (!sess) return;
  if (value && value.trim()) sess.filters[filterKey] = value.trim();

  let pool = state.parts;
  const f = sess.filters;
  if (f.category) pool = pool.filter(p => (p.category||p.productType||p.name||'').toLowerCase().includes(f.category.toLowerCase()));
  if (f.brand)    pool = pool.filter(p => (p.brand||'').toLowerCase().includes(f.brand.toLowerCase()));
  if (f.machine)  pool = pool.filter(p => (p.name+' '+(p.notes||'')).toLowerCase().includes(f.machine.toLowerCase()));

  const steps = ['category','brand','machine'];
  const next = steps[steps.indexOf(filterKey) + 1];

  if (!next || pool.length <= 20) {
    _runFinalVisualMatch();
  } else {
    _renderScanFilterStep(next);
  }
}

async function _runFinalVisualMatch() {
  const statusTxt = document.getElementById('scan-status-txt');
  const matchArea = document.getElementById('scan-match-area');
  const sess = window._scanSession;
  if (!sess) return;
  const { base64, mimeType, hash, filters, aiExtract } = sess;
  const key = getAIKey();

  async function callClaude(messages, maxTokens) {
    try { return await aiCall({ messages: messages, maxTokens: maxTokens }); } catch(e) { return ''; }
  }

  function urlToBase64(url) {
    return new Promise(resolve => {
      if (!url) return resolve(null);
      if (url.startsWith('data:')) { const mime=url.split(';')[0].split(':')[1]||'image/jpeg'; return resolve({base64:url.split(',')[1],mimeType:mime}); }
      const img = new Image(); img.crossOrigin='anonymous';
      img.onload = function() {
        try {
          const MAX=512; let w=img.naturalWidth,h=img.naturalHeight;
          if(w>MAX||h>MAX){if(w>h){h=Math.round(h*MAX/w);w=MAX;}else{w=Math.round(w*MAX/h);h=MAX;}}
          const canvas=document.createElement('canvas'); canvas.width=w; canvas.height=h;
          canvas.getContext('2d').drawImage(img,0,0,w,h);
          const dUrl=canvas.toDataURL('image/jpeg',0.80);
          resolve({base64:dUrl.split(',')[1],mimeType:'image/jpeg'});
        } catch(e){resolve(null);}
      };
      img.onerror=()=>resolve(null); img.src=url;
    });
  }

  let pool = state.parts;
  if (filters.category) pool = pool.filter(p => (p.category||p.productType||p.name||'').toLowerCase().includes(filters.category.toLowerCase()));
  if (filters.brand)    pool = pool.filter(p => (p.brand||'').toLowerCase().includes(filters.brand.toLowerCase()));
  if (filters.machine)  pool = pool.filter(p => (p.name+' '+(p.notes||'')).toLowerCase().includes(filters.machine.toLowerCase()));

  const withPhoto    = pool.filter(p => p.photoDataUrl);
  const withoutPhoto = pool.filter(p => !p.photoDataUrl);

  statusTxt.textContent = '\u{1F52C} Final match: ' + pool.length + ' candidates\u2026';

  if (!withPhoto.length) {
    statusTxt.textContent = '\u{1F4CB} ' + pool.length + ' matches \u2014 pick manually:';
    matchArea.innerHTML = _renderCandidateList(pool.slice(0,10), hash, filters);
    return;
  }

  const BATCH = 10;
  let bestPart=null, bestConf='none', bestReason='';
  const confRank={high:3,medium:2,low:1,none:0};

  for (let start=0; start<withPhoto.length; start+=BATCH) {
    const batch = withPhoto.slice(start, start+BATCH);
    statusTxt.textContent = '\u{1F916} Visual match batch '+(Math.floor(start/BATCH)+1)+' of '+Math.ceil(withPhoto.length/BATCH)+'\u2026';

    const fetched = await Promise.all(batch.map(p=>urlToBase64(p.photoDataUrl)));
    const msgContent = [];
    msgContent.push({type:'text',text:'PHOTO A \u2014 the UNKNOWN PART the customer brought in:'});
    msgContent.push({type:'image',source:{type:'base64',media_type:mimeType,data:base64}});

    const validCandidates = [];
    let vi=0;
    for (let i=0;i<batch.length;i++) {
      if(!fetched[i]) continue;
      msgContent.push({type:'text',text:'\n['+vi+'] '+batch[i].name+' ('+batch[i].partNo+(batch[i].brand?' \xB7 '+batch[i].brand:'')+'):'});
      msgContent.push({type:'image',source:{type:'base64',media_type:fetched[i].mimeType,data:fetched[i].base64}});
      validCandidates.push(batch[i]); vi++;
    }
    if (!validCandidates.length) continue;

    msgContent.push({type:'text',text:
      '\nSpare parts expert: compare PHOTO A against these '+validCandidates.length+' inventory photos.\n'+
      'Category: '+(filters.category||'unknown')+'. Brand: '+(filters.brand||'unknown')+'.\n'+
      'Description: '+(aiExtract.description||'')+'\n\n'+
      'Match on: shape, dimensions, diameter, colour, thickness, holes, texture, ridges.\n'+
      'Parts with no text markings (bearings, belts, filters) are matched visually \u2014 this is expected.\n\n'+
      'Return ONLY valid JSON: {"matchIndex":-1,"confidence":"none","reason":""}\n'+
      'confidence: high=clearly same, medium=probably same, low=could be same.'
    });

    const raw = await callClaude([{role:'user',content:msgContent}], 300);
    let res={matchIndex:-1,confidence:'none',reason:''};
    try { const s=raw.indexOf('{'),ee=raw.lastIndexOf('}'); if(s>=0&&ee>=0) res={...res,...JSON.parse(raw.slice(s,ee+1))}; } catch(_){}

    const idx=parseInt(res.matchIndex);
    const conf=res.confidence||'none';
    if (idx>=0&&idx<validCandidates.length&&confRank[conf]>confRank[bestConf]) {
      bestPart=validCandidates[idx]; bestConf=conf; bestReason=res.reason||'';
      if (bestConf==='high') break;
    }
  }

  if (bestPart && bestConf!=='none') {
    _renderScanFinalMatch(bestPart, '\u{1F4F8} '+(bestReason||'visual match'), bestConf, hash, filters);
  } else {
    statusTxt.textContent = '\u{1F50E} Couldn\'t confirm visually \u2014 pick from '+pool.length+' parts:';
    matchArea.innerHTML = _renderCandidateList(withPhoto.slice(0,8).concat(withoutPhoto.slice(0,4)), hash, filters);
  }
}

function _renderScanFinalMatch(part, reason, confidence, hash, filters) {
  const statusTxt = document.getElementById('scan-status-txt');
  const matchArea = document.getElementById('scan-match-area');
  const price = getPrice(part);
  const confLabel = confidence==='high'?'\u2713 High confidence':confidence==='medium'?'\u007E Probably matches':'? Possible match \u2014 verify';
  const confColor = confidence==='high'?'#6ee7b7':confidence==='medium'?'#fcd34d':'#fca5a5';
  const _scanPhoto = window.displayPhotoUrl ? window.displayPhotoUrl(part.photoDataUrl) : part.photoDataUrl;
  const imgHtml = _scanPhoto
    ? '<img class="scan-match-img" src="'+_scanPhoto+'" alt="">'
    : '<div class="scan-match-no-img">\u{1F527}</div>';
  const filtersStr = JSON.stringify(filters||{}).replace(/"/g,'&quot;');

  statusTxt.innerHTML = '\u{1F3AF} Match found <span style="background:rgba(255,255,255,.1);padding:2px 8px;border-radius:6px;font-size:10px;font-weight:700;color:'+confColor+';margin-left:4px;">'+confLabel+'</span>';
  matchArea.innerHTML =
    '<div class="scan-match-card">'+imgHtml+
    '<div class="scan-match-info">'+
    '<div class="scan-match-name">'+part.name+'</div>'+
    '<div class="scan-match-meta">'+part.partNo+(part.brand?' \xB7 '+part.brand:'')+' \xB7 '+part.stock+' in stock</div>'+
    (reason?'<div style="font-size:10px;color:rgba(255,255,255,.45);margin-top:2px;">'+reason+'</div>':'')+
    '<div class="scan-match-price">'+rs(price)+'</div>'+
    '</div>'+
    '<div style="display:flex;flex-direction:column;gap:6px;align-items:flex-end;flex-shrink:0;">'+
    (part.stock>0
      ? '<button class="scan-add-btn" onclick="scanConfirmAdd(\''+part.id+'\',\''+( hash||'')+'\',\''+filtersStr+'\')">+ Add to Bill</button>'
      : '<span style="color:#fca5a5;font-size:12px;font-weight:700;text-align:center;">OUT OF<br>STOCK</span>')+
    '<button onclick="scanWrongMatch()" style="background:rgba(255,255,255,.1);color:rgba(255,255,255,.7);border:1px solid rgba(255,255,255,.2);padding:5px 8px;border-radius:7px;font-size:11px;cursor:pointer;">Wrong?</button>'+
    '</div></div>';

  _highlightGridCard(part.id);
  if (navigator.vibrate) navigator.vibrate([30,40,80]);
}

function _renderCandidateList(parts, hash, filters) {
  if (!parts.length) return '<div style="color:rgba(255,255,255,.5);font-size:12px;">No matches found</div>';
  const filtersStr = JSON.stringify(filters||{}).replace(/"/g,'&quot;');
  return '<div style="display:flex;flex-direction:column;gap:6px;">' +
    parts.map(p =>
      '<div onclick="scanConfirmAdd(\''+p.id+'\',\''+(hash||'')+'\',\''+filtersStr+'\')" '+
      'style="display:flex;align-items:center;gap:10px;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.15);border-radius:10px;padding:8px 10px;cursor:pointer;">' +
      ((window.displayPhotoUrl?window.displayPhotoUrl(p.photoDataUrl):p.photoDataUrl)
        ? '<img src="'+(window.displayPhotoUrl?window.displayPhotoUrl(p.photoDataUrl):p.photoDataUrl)+'" style="width:44px;height:44px;border-radius:6px;object-fit:cover;flex-shrink:0;">'
        : '<div style="width:44px;height:44px;border-radius:6px;background:rgba(255,255,255,.1);display:flex;align-items:center;justify-content:center;font-size:20px;flex-shrink:0;">\u{1F527}</div>') +
      '<div style="flex:1;min-width:0;"><div style="font-size:13px;font-weight:600;color:white;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">'+p.name+'</div>'+
      '<div style="font-size:10px;color:rgba(255,255,255,.5);margin-top:2px;">'+p.partNo+(p.brand?' \xB7 '+p.brand:'')+'</div></div>'+
      '<div style="font-size:14px;font-weight:700;color:#6ee7b7;flex-shrink:0;">'+rs(getPrice(p))+'</div></div>'
    ).join('') + '</div>';
}

function scanWrongMatch() {
  const statusTxt = document.getElementById('scan-status-txt');
  const matchArea = document.getElementById('scan-match-area');
  const sess = window._scanSession || {};
  const ex = sess.aiExtract || {};
  const texts = ex.texts || [];
  statusTxt.textContent = '\u{1F50E} Wrong \u2014 try different filters or browse:';
  let html = '';
  if (texts.length) {
    html += '<div style="margin-bottom:8px;"><div style="font-size:10px;color:rgba(255,255,255,.5);margin-bottom:4px;text-transform:uppercase;letter-spacing:.5px;">Text read from part</div>' +
      texts.map(t => '<button onclick="photoGridSetSearch(\''+String(t).replace(/'/g,"\\'")+'\');scanWrongClose()" style="display:inline-block;margin:2px;padding:4px 9px;background:rgba(255,255,255,.15);color:white;border:1px solid rgba(255,255,255,.3);border-radius:7px;font-size:12px;cursor:pointer;">'+t+'</button>').join('') + '</div>';
  }
  html += '<div style="margin-top:8px;display:flex;flex-wrap:wrap;gap:6px;">' +
    '<button onclick="window._scanSession&&_renderScanFilterStep(\'category\')" style="padding:7px 12px;border-radius:8px;background:#7c3aed;color:white;border:none;font-size:12px;font-weight:600;cursor:pointer;">\u{1F504} Try different filters</button>' +
    '<button onclick="triggerPartScan()" style="background:rgba(255,255,255,.15);color:white;border:1px solid rgba(255,255,255,.3);padding:7px 12px;border-radius:8px;font-size:12px;cursor:pointer;">\u{1F4F7} New photo</button>' +
    '<button onclick="scanWrongClose()" style="background:rgba(255,255,255,.08);color:rgba(255,255,255,.6);border:1px solid rgba(255,255,255,.15);padding:7px 12px;border-radius:8px;font-size:12px;cursor:pointer;">Browse grid \u2193</button>' +
    '</div>';
  matchArea.innerHTML = '<div style="padding:2px 0;">'+html+'</div>';
}

function scanWrongClose() {
  const bar = document.getElementById('scan-result-bar');
  if (bar) bar.style.display = 'none';
}

function photoGridSetSearch(text) {
  const inp = document.getElementById('photo-grid-search-input');
  if (inp) { inp.value = text; renderPhotoGrid(); }
  document.querySelector('.photo-grid-body').scrollIntoView({ behavior: 'smooth' });
}

function scanOpenAddPart() {
  const ex = (window._scanSession && window._scanSession.aiExtract) || {};
  const f  = (window._scanSession && window._scanSession.filters)   || {};
  closePhotoGrid();
  setTimeout(() => {
    openAddPart({
      context: '\u{1F4F7} Scanned \u2014 not in inventory',
      prefill: {
        name:     ex.description || '',
        brand:    f.brand || ex.brand || '',
        category: f.category || ex.category || '',
        partNo:   (ex.texts && ex.texts[0]) || '',
        notes:    (ex.texts||[]).join(', ')
      }
    });
  }, 200);
}

function scanConfirmAdd(partId, hash, filtersJson) {
  try {
    const filters = filtersJson ? JSON.parse(filtersJson.replace(/&quot;/g,'"')) : {};
    if (hash) ScanMemory.remember(hash, partId, filters);
  } catch(_) {}

  // ─── VISUAL TRAINING: extract + store features in background ───
  const sess = window._scanSession;
  if (sess && sess.base64 && sess.mimeType) {
    const key = getAIKey();
    if (key && key.startsWith('sk-')) {
      (async () => {
        try {
          async function _trainCallClaude(messages, maxTokens) {
            try { return await aiCall({ messages: messages, maxTokens: maxTokens }); } catch(e) { return ''; }
          }
          const features = await extractVisualFeatures(
            sess.base64, sess.mimeType, _trainCallClaude, sess.aiExtract
          );
          VisualTraining.train(partId, features, hash || null);
          const stats = VisualTraining.stats();
          const statusTxt = document.getElementById('scan-status-txt');
          if (statusTxt) {
            statusTxt.textContent = '\u2705 Added + trained! \u{1F9E0} ' + stats.uniqueParts + ' parts \u00B7 ' + stats.totalConfirms + ' scans learned';
          }
        } catch(_) {}
      })();
    }
  }
  // ───────────────────────────────────────────────────────────────

  addToCartFromGrid(partId);
  const scanBar   = document.getElementById('scan-result-bar');
  const statusTxt = document.getElementById('scan-status-txt');
  const matchArea = document.getElementById('scan-match-area');
  const legacyCount = ScanMemory.count();
  const vtStats = VisualTraining.stats();
  const total = legacyCount + vtStats.totalConfirms;
  if (statusTxt) statusTxt.textContent = '\u2705 Added! \u{1F9E0} Training AI\u2026 (' + total + ' scans in memory)';
  if (matchArea) matchArea.innerHTML = '<div style="color:#6ee7b7;font-size:13px;font-weight:600;padding:4px 0;">AI is learning this part \u26A1 Future scans will be faster</div>';
  setTimeout(() => { if (scanBar) scanBar.style.display = 'none'; }, 3000);
}

function _highlightGridCard(partId) {
  // Scroll the grid card into view and pulse it
  const card = document.getElementById('pgcard-' + partId);
  if (!card) return;
  card.scrollIntoView({ behavior: 'smooth', block: 'center' });
  card.style.outline = '3px solid #22c55e';
  card.style.boxShadow = '0 0 0 6px rgba(34,197,94,.25)';
  setTimeout(() => {
    card.style.outline = '';
    card.style.boxShadow = '';
  }, 2000);
}

let _crossSellTimer = null;
let _lastCrossCartKey = '';
function renderCart() {
  const itemsDiv = document.getElementById('cart-items');
  const empty = document.getElementById('cart-empty');
  const totals = document.getElementById('cart-totals');
  const crossDiv = document.getElementById('cross-sell-box');
  document.getElementById('cart-count').textContent = state.cart.length ? `(${state.cart.length})` : '';

  if (state.cart.length === 0) {
    itemsDiv.innerHTML = ''; empty.style.display = 'block'; totals.style.display = 'none'; crossDiv.innerHTML = '';
    return;
  }
  empty.style.display = 'none'; totals.style.display = 'block';

  itemsDiv.innerHTML = state.cart.map((item, idx) => {
    const p = getPartById(item.partId);
    const price = saleItemPrice(item, p);
    const cont = isContinuousUnit(p.unit);
    const uLbl = (p.unit && p.unit !== 'pcs') ? p.unit : '';
    const qCtrl = cont
      ? `<div class="qty-control"><button class="qty-btn" onclick="changeQty(${idx},-1)">−</button><input type="number" min="0.5" step="0.5" value="${item.qty}" style="width:48px;text-align:center;font-size:14px;font-weight:700;border:none;background:transparent;" onchange="setQtyDirect(${idx},this.value)"><span style="font-size:10px;color:var(--muted)">${uLbl}</span><button class="qty-btn" onclick="changeQty(${idx},1)">+</button></div>`
      : `<div class="qty-control"><button class="qty-btn" onclick="changeQty(${idx},-1)">−</button><span class="qty-num">${item.qty}</span><button class="qty-btn" onclick="changeQty(${idx},1)">+</button></div>`;
    return `<div class="cart-item">
      <div style="flex:1;min-width:0;">
        <div class="bold" style="font-size:14px;">${p.name}</div>
        <div class="part-no">${p.partNo} • ${rs(price)} / ${p.unit||'pcs'}${item.customPrice!=null?' <span style="color:#b45309;font-weight:700;">(bill rate)</span>':''}</div>
        <button class="gp-rate-chip" onclick="gpOpenRateEditor('sale',${idx})">✎ Rate · H/C/R</button>
      </div>
      ${qCtrl}
      <div style="text-align:right;min-width:70px;font-weight:600;">${rs(price * item.qty)}</div>
    </div>`;
  }).join('');

  // Cross-sell suggestions — debounced (scans all sales, skip on qty-only changes)
  const _cartKey = state.cart.map(i => i.partId).join(',');
  if (_cartKey !== _lastCrossCartKey) {
    _lastCrossCartKey = _cartKey;
    clearTimeout(_crossSellTimer);
    crossDiv.innerHTML = '';
    _crossSellTimer = setTimeout(function() {
      const suggestions = new Map();
      state.cart.forEach(item => {
        Smart.crossSell(item.partId, 5).forEach(c => {
          if (!state.cart.find(ci => ci.partId === c.id) && c.stock > 0)
            suggestions.set(c.id, (suggestions.get(c.id)||0) + 1);
        });
      });
      const xDiv = document.getElementById('cross-sell-box');
      if (!xDiv) return;
      if (suggestions.size > 0) {
        const sorted = [...suggestions.entries()].sort((a,b)=>b[1]-a[1]).slice(0,3);
        xDiv.innerHTML = '<div class="cross-sell mt-1"><div class="label-ai">🧠 Often bought together:</div>'
          + sorted.map(([pid]) => { const p = getPartById(pid); if(!p) return ''; return `<div style="margin-top:6px;display:flex;justify-content:space-between;align-items:center;"><span>${p.name} <small>(${rs(getPrice(p))})</small></span><button class="btn btn-sm btn-ai" onclick="addToCart('${pid}')">+ Add</button></div>`; }).join('')
          + '</div>';
      } else { xDiv.innerHTML = ''; }
    }, 500);
  }

  updateTotals();
}

function changeQty(idx, delta) {
  const item = state.cart[idx];
  const p = getPartById(item.partId);
  const step = unitStep(p.unit);
  item.qty = Math.round((item.qty + delta * step) * 100) / 100;
  if (item.qty <= 0) state.cart.splice(idx, 1);
  else if (item.qty > p.stock) { item.qty = p.stock; toast('Stock limit'); }
  renderCart();
}
function setQtyDirect(idx, val) {
  const item = state.cart[idx];
  const p = getPartById(item.partId);
  var q = parseFloat(val) || 0;
  if (!isContinuousUnit(p.unit)) q = Math.round(q);
  q = Math.round(q * 100) / 100;
  if (q <= 0) { state.cart.splice(idx, 1); renderCart(); return; }
  if (q > p.stock) { q = p.stock; toast('Stock limit'); }
  item.qty = q; renderCart();
}

function updateTotals() {
  const sub = state.cart.reduce((a,i) => {
    const p = getPartById(i.partId);
    return a + saleItemPrice(i, p) * i.qty;
  }, 0);
  const disc = +document.getElementById('cart-discount')?.value || 0;
  document.getElementById('cart-subtotal').textContent = rs(sub);
  document.getElementById('cart-total').textContent = rs(Math.max(0, sub - disc));
}

function setPayMode(mode) {
  state.payMode = mode;
  document.querySelectorAll('.pay-btn').forEach(b => {
    if (b.dataset.mode === mode) { b.classList.remove('btn-outline'); b.classList.add('active'); }
    else { b.classList.add('btn-outline'); b.classList.remove('active'); }
  });
  renderCart();
}

function clearCart() {
  state.cart = [];
  document.getElementById('cart-discount').value = 0;
  renderCart();
}

async function completeSale() {
  if (isReadOnly()) { toast('🔒 Read-only access — cannot complete sale'); return; }
  if (state.cart.length === 0) { toast('Cart is empty'); return; }
  // Stop-shop guard: refuse if data layer is unhealthy
  if (!(await rxGuardSale('Sale'))) return;
  const custId = document.getElementById('sale-customer').value;
  const cust = getCustomerById(custId);
  const sub = state.cart.reduce((a,i) => { const p = getPartById(i.partId); return a + saleItemPrice(i, p) * i.qty; }, 0);
  const disc = +document.getElementById('cart-discount').value || 0;
  const total = Math.max(0, sub - disc);
  const soldById = document.getElementById('sale-soldby')?.value || '';
  const deliveredById = document.getElementById('sale-deliveredby')?.value || '';
  const mechanicId = document.getElementById('sale-mechanic')?.value || '';
  const mechRate = +(document.getElementById('sale-mech-rate')?.value) || 0;

  const sale = {
    id: uid(), invoiceNo: 'INV-' + state.settings.nextInvoice,
    date: new Date().toISOString(), customerId: custId || null,
    items: state.cart.map(i => { const p = getPartById(i.partId); return {partId:i.partId, partNo:p.partNo, name:p.name, qty:i.qty, price:saleItemPrice(i, p)}; }),
    subtotal: sub, discount: disc, total,
    paymentMode: state.payMode,
    settled: state.payMode !== 'credit',
    soldById, deliveredById, mechanicId, mechRate,
    saleType: custId ? 'wholesale' : 'retail'
  };
  state.sales.push(sale);
  // Increment AFTER push so a crash before save() cannot orphan the counter.
  // On next boot, syncNextInvoice() self-heals from actual sales data.
  state.settings.nextInvoice++;
  // Deduct stock and collect any min-stock / out-of-stock alerts
  const _saleAlerts = [];
  for (const i of state.cart) {
    const p = getPartById(i.partId);
    if (!p) continue;
    if (window.firestoreDecrementStock && window._firebaseAuthed) {
      try {
        await window.firestoreDecrementStock(p.id, i.qty);
        partsPendingRemove(p.id);
      } catch (e) {
        console.warn('Atomic stock decrement failed for', p.partNo, '— local value used:', e.message);
        partsPendingEnqueue(p.id, e.message);
      }
    } else if (!window._firebaseAuthed || !navigator.onLine) {
      partsPendingEnqueue(p.id, 'offline');
    }
    p.stock = Math.round(Math.max(0, p.stock - i.qty) * 1000) / 1000;
    if (p.stock === 0) _saleAlerts.push({name:p.name, partNo:p.partNo, stock:0, minStock:p.minStock, status:'out'});
    else if (p.minStock > 0 && p.stock <= p.minStock) _saleAlerts.push({name:p.name, partNo:p.partNo, stock:p.stock, minStock:p.minStock, status:'low'});
  }
  // Refresh SKU editor stock cells if open
  xlsRefreshStockCells(state.cart.map(i=>i.partId));
  if (state.payMode === 'credit' && cust) cust.totalDue = (cust.totalDue||0) + total;

  // Track retail customer if walk-in
  if (!custId) {
    const rcId = document.getElementById('retail-cust-id')?.value;
    if (rcId) {
      const rc = state.retailCustomers.find(r => r.id === rcId);
      if (rc) { rc.lastVisit = sale.date; rc.totalSpent = (rc.totalSpent||0) + total; rc.visits = (rc.visits||0) + 1; }
    }
  }

  // Log AI feedback prompt
  state.aiFeedback.push({id:uid(), type:'sale', saleId:sale.id, date:sale.date, rating:null, comment:''});

  invalidateVelocityCache(); // sales array changed — rebuild velocity on next render
  drainPartsCloudPending();
  await save(); // wait for outbox enqueue to be durable in IndexedDB
  // Per-record verification — wait up to 12s for THIS sale to confirm in Firestore.
  // The outbox keeps retrying in the background even if we time out here.
  const _ss = await rxConfirmRecord('sales', sale.id, 12000);
  if (!_ss.ok) {
    if (_ss.queued) {
      // Sale is durably queued and will retry — don't roll back.
      RX.log('warn','sale','Complete Sale queued but not yet confirmed (' + _ss.reason + ')');
      toast('⚠️ Saved locally, syncing in background…');
    } else {
      // Hard failure — roll back as before
      state.sales.pop(); state.settings.nextInvoice--;
      state.cart.forEach(i=>{const p=getPartById(i.partId);if(p)p.stock+=i.qty;});
      if(state.payMode==='credit'&&cust) cust.totalDue=Math.max(0,(cust.totalDue||0)-total);
      state.aiFeedback.pop(); save();
      showFsFail('Complete Sale', _ss.reason || 'Cloud not responding');
      return;
    }
  }
  showInvoice(sale);
  if (_saleAlerts.length) setTimeout(() => showStockAlertModal(_saleAlerts, sale.invoiceNo), 800);
  state.cart = [];
  document.getElementById('cart-discount').value = 0;
  if (document.getElementById('sale-mech-rate')) document.getElementById('sale-mech-rate').value = '';
  renderCart();
}

function showInvoice(sale) {
  const cust = getCustomerById(sale.customerId);
  const itemsHTML = sale.items.map(i => { const _p=getPartById(i.partId); const _u=(_p&&_p.unit&&_p.unit!=='pcs')?' '+_p.unit:''; return `<tr><td>${i.name}<br><small>${i.partNo}</small></td><td>${i.qty}${_u}</td><td style="text-align:right;">${rs(i.price)}</td><td style="text-align:right;">${rs(i.qty*i.price)}</td></tr>`; }).join('');
  const retBadge = renderReturnBadge(sale.id);
  showModal('Bill Generated ✓', `
    <div style="font-family:monospace;background:#f8fafc;padding:12px;border-radius:8px;font-size:13px;">
      <div class="center bold" style="font-size:15px;">${state.settings.shopName}</div>
      ${state.settings.address?`<div class="center">${state.settings.address}</div>`:''}
      ${state.settings.shopPhone?`<div class="center">📞 ${state.settings.shopPhone}</div>`:''}
      ${state.settings.gstNo?`<div class="center">GST: ${state.settings.gstNo}</div>`:''}
      <div class="divider"></div>
      <div class="flex-between"><span>Bill: <b>${sale.invoiceNo}</b></span><span>${new Date(sale.date).toLocaleString('en-IN')}</span></div>
      <div>Customer: ${cust?cust.name:'Walk-in'}</div>
      <div>Mode: <b>${sale.paymentMode.toUpperCase()}</b></div>
      ${sale.soldById ? `<div>Sold By: <b>${state.staff.find(s=>s.id===sale.soldById)?.name||'—'}</b></div>` : ''}
      ${sale.deliveredById ? `<div>Delivered By: <b>${state.staff.find(s=>s.id===sale.deliveredById)?.name||'—'}</b></div>` : ''}
      <div class="divider"></div>
      <table style="width:100%;font-size:12px;"><thead><tr><th align="left">Item</th><th>Qty</th><th align="right">Rate</th><th align="right">Amt</th></tr></thead><tbody>${itemsHTML}</tbody></table>
      <div class="divider"></div>
      <div class="flex-between"><span>Subtotal</span><span>${rs(sale.subtotal)}</span></div>
      ${sale.discount?`<div class="flex-between"><span>Discount</span><span>-${rs(sale.discount)}</span></div>`:''}
      <div class="flex-between bold" style="font-size:16px;color:var(--primary);"><span>TOTAL</span><span>${rs(sale.total)}</span></div>
      ${sale.paymentMode==='credit'?`<div class="mt-1 text-warn"><b>Credit:</b> ${state.settings.creditTerms}</div>`:''}
    </div>
    ${cust && cust.phone ? `<button class="wa-btn mt-2" onclick="sendBillWA('${sale.id}')">📲 Send Bill on WhatsApp</button>`:''}
    ${window.gpPhotoStripHtml ? window.gpPhotoStripHtml(sale, 'sale') : ''}
    <button class="btn mt-1" onclick="gpPrintBill('sale','${sale.id}')">🖨️ Print Bill</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();gpOpenBillEditor('sale','${sale.id}')">✏️ Edit Bill</button>
    <button class="btn btn-outline mt-1" onclick="openReturnModal('${sale.id}')">↩️ Process Return</button>
    <button class="btn btn-outline mt-1" onclick="closeModal()">Close</button>
  `);
}




// ============================================================
// MULTI-LOCATION MODULE — branches, godowns, counters
// ============================================================
function openManageLocations() {
  const locs = state.locations || [];
  var rows = locs.map(function(l) {
    return '<div style="display:flex;justify-content:space-between;align-items:center;padding:10px;border-bottom:1px solid #f1f5f9;">'
      + '<div>'
      + '<div style="font-weight:600;font-size:13px;">' + escapeHtml(l.name||'') + '</div>'
      + '<div style="font-size:12px;color:var(--muted);">' + (l.type||'branch') + ' · ' + escapeHtml(l.address||'—') + ' · ' + escapeHtml(l.phone||'—') + '</div>'
      + '</div>'
      + '<div style="display:flex;gap:6px;">'
      + '<button class="btn btn-sm btn-outline" onclick="editLocation(\'' + l.id + '\')">Edit</button>'
      + '<button class="btn btn-sm btn-outline" style="color:#dc2626;" onclick="deleteLocation(\'' + l.id + '\')">✕</button>'
      + '</div>'
      + '</div>';
  }).join('');

  var locRows = locs.length ? rows : '<div style="color:var(--muted);font-size:13px;padding:8px 0;">No locations added yet. Your main shop is the default location.</div>';
  showModal('🏪 Manage Locations',
    '<div style="margin-bottom:12px;">' + locRows + '</div>'
    + '<div style="background:#f8fafc;border-radius:10px;padding:12px;margin-bottom:12px;">'
    + '<div style="font-weight:600;font-size:13px;margin-bottom:8px;">Add Location</div>'
    + '<input id="loc-name" class="input" placeholder="Location name (e.g. Godown, Counter 2)" style="margin-bottom:6px;">'
    + '<div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:6px;">'
    + '<select id="loc-type" class="input" style="padding:8px;">'
    + '<option value="branch">Branch / Counter</option>'
    + '<option value="godown">Godown / Warehouse</option>'
    + '<option value="mobile">Mobile / Van</option>'
    + '</select>'
    + '<input id="loc-phone" class="input" placeholder="Phone (optional)">'
    + '</div>'
    + '<input id="loc-address" class="input" placeholder="Address (optional)" style="margin-bottom:8px;">'
    + '<button class="btn btn-sm" onclick="addLocation()">+ Add</button>'
    + '</div>'
    + '<div style="font-size:12px;color:var(--muted);margin-top:4px;">Each part can be assigned to a location. Stock transfers between locations are tracked.</div>'
    + '<button class="btn btn-outline mt-2" onclick="closeModal()">Done</button>'
  );
}

async function addLocation() {
  const name = document.getElementById('loc-name')?.value.trim();
  if (!name) { toast('Enter location name'); return; }
  if (!state.locations) state.locations = [];
  state.locations.push({
    id: uid(), name,
    type: document.getElementById('loc-type')?.value || 'branch',
    phone: document.getElementById('loc-phone')?.value.trim() || '',
    address: document.getElementById('loc-address')?.value.trim() || '',
    active: true, createdAt: Date.now()
  });
  await save();
  openManageLocations();
  toast('✅ Location added');
}

async function deleteLocation(locId) {
  if (!confirm('Remove this location?')) return;
  state.locations = (state.locations || []).filter(l => l.id !== locId);
  await save();
  openManageLocations();
}

function editLocation(locId) {
  const l = (state.locations || []).find(x => x.id === locId);
  if (!l) return;
  showModal('Edit Location',
    '<input id="eloc-name" class="input" value="' + escapeHtml(l.name||'') + '" style="margin-bottom:6px;" placeholder="Name">'
    + '<div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:6px;">'
    + '<select id="eloc-type" class="input" style="padding:8px;">'
    + '<option value="branch"' + (l.type==='branch' ? ' selected' : '') + '>Branch / Counter</option>'
    + '<option value="godown"' + (l.type==='godown' ? ' selected' : '') + '>Godown / Warehouse</option>'
    + '<option value="mobile"' + (l.type==='mobile' ? ' selected' : '') + '>Mobile / Van</option>'
    + '</select>'
    + '<input id="eloc-phone" class="input" value="' + escapeHtml(l.phone||'') + '" placeholder="Phone">'
    + '</div>'
    + '<input id="eloc-address" class="input" value="' + escapeHtml(l.address||'') + '" placeholder="Address" style="margin-bottom:10px;">'
    + '<button class="btn" onclick="saveEditLocation(' + JSON.stringify(locId) + ')">Save</button>'
    + '<button class="btn btn-outline" onclick="openManageLocations()" style="margin-left:8px;">Back</button>'
  );
}

async function saveEditLocation(locId) {
  const l = (state.locations || []).find(x => x.id === locId);
  if (!l) return;
  l.name    = document.getElementById('eloc-name')?.value.trim() || l.name;
  l.type    = document.getElementById('eloc-type')?.value || l.type;
  l.phone   = document.getElementById('eloc-phone')?.value.trim() || '';
  l.address = document.getElementById('eloc-address')?.value.trim() || '';
  await save();
  openManageLocations();
}

function openStockTransferModal() {
  const locs = state.locations || [];
  if (locs.length < 1) { toast('Add at least 2 locations first'); openManageLocations(); return; }
  const locOptions = [
    '<option value="main">Main Shop (default)</option>',
    ...locs.map(l => `<option value="${l.id}">${l.name}</option>`)
  ].join('');

  showModal('🔄 Stock Transfer',
    '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:10px;">'
    + '<div><label style="font-size:12px;color:var(--muted);">From</label>'
    + '<select id="xfr-from" class="input" style="margin-top:4px;">' + locOptions + '</select></div>'
    + '<div><label style="font-size:12px;color:var(--muted);">To</label>'
    + '<select id="xfr-to" class="input" style="margin-top:4px;">' + locOptions + '</select></div>'
    + '</div>'
    + '<div style="margin-bottom:8px;">'
    + '<label style="font-size:12px;color:var(--muted);">Part</label>'
    + '<input id="xfr-part-search" class="input" style="margin-top:4px;" placeholder="Search part no or name…" oninput="xfrSearchPart(this.value)">'
    + '<div id="xfr-part-results" style="margin-top:4px;"></div>'
    + '</div>'
    + '<div id="xfr-selected" style="background:#f0fdf4;border-radius:8px;padding:8px;margin-bottom:10px;display:none;">'
    + '<div id="xfr-selected-name" style="font-size:13px;font-weight:600;"></div>'
    + '<div style="display:flex;align-items:center;gap:8px;margin-top:6px;">'
    + '<label style="font-size:12px;color:var(--muted);">Transfer Qty:</label>'
    + '<input type="number" id="xfr-qty" class="input" style="width:80px;padding:6px;" min="1" value="1">'
    + '</div></div>'
    + '<button class="btn" onclick="confirmStockTransfer()">✅ Transfer</button>'
    + '<button class="btn btn-outline" onclick="closeModal()" style="margin-left:8px;">Cancel</button>'
  );
  window._xfrPart = null;
}

function xfrSearchPart(q) {
  const results = document.getElementById('xfr-part-results');
  if (!q || q.length < 2) { if (results) results.innerHTML = ''; return; }
  const matches = state.parts.filter(p => saleablePart(p) && p.stock > 0 &&
    (p.partNo.toLowerCase().includes(q.toLowerCase()) || p.name.toLowerCase().includes(q.toLowerCase()))
  ).slice(0, 8);
  if (!results) return;
  results.innerHTML = matches.map(p =>
    `<div onclick="xfrSelectPart('${p.id}')" style="padding:8px;border-bottom:1px solid #f1f5f9;cursor:pointer;font-size:13px;">
      <b>${p.partNo}</b> — ${p.name} <span style="color:var(--muted);">(stock: ${p.stock})</span>
    </div>`
  ).join('');
}

function xfrSelectPart(partId) {
  const p = getPartById(partId);
  if (!p) return;
  window._xfrPart = p;
  const sel = document.getElementById('xfr-selected');
  const selName = document.getElementById('xfr-selected-name');
  const results = document.getElementById('xfr-part-results');
  const search = document.getElementById('xfr-part-search');
  if (sel) sel.style.display = 'block';
  if (selName) selName.textContent = p.partNo + ' — ' + p.name + ' (available: ' + p.stock + ')';
  if (results) results.innerHTML = '';
  if (search) search.value = p.name;
  const qtyInput = document.getElementById('xfr-qty');
  if (qtyInput) { qtyInput.max = p.stock; qtyInput.value = 1; }
}

async function confirmStockTransfer() {
  if (!window._xfrPart) { toast('Select a part first'); return; }
  const p = window._xfrPart;
  const qty = +document.getElementById('xfr-qty')?.value || 0;
  const fromId = document.getElementById('xfr-from')?.value || 'main';
  const toId   = document.getElementById('xfr-to')?.value || 'main';
  if (fromId === toId) { toast('From and To must be different'); return; }
  if (qty <= 0 || qty > p.stock) { toast('Invalid quantity'); return; }

  const fromName = fromId === 'main' ? 'Main Shop' : (state.locations.find(l=>l.id===fromId)||{}).name || fromId;
  const toName   = toId   === 'main' ? 'Main Shop' : (state.locations.find(l=>l.id===toId  )||{}).name || toId;

  // For now: stock remains in single global pool, transfer is logged for audit
  // Future: per-location stock ledger
  p.updatedAt = Date.now();
  await cloudPersistPartFields(p.id, { updatedAt: p.updatedAt });
  await save();

  logActivity(`Stock transfer: ${p.partNo} "${p.name}" × ${qty} from ${fromName} → ${toName}`, 'info');
  closeModal();
  toast(`✅ Transfer logged: ${qty} × ${p.name} → ${toName}`);
}


// ============================================================
// PURCHASE ORDERS (PO) MODULE
// draft → sent → partial/received
// ============================================================
function openCreatePO(supplierId) {
  const sup = supplierId ? state.suppliers.find(s => s.id === supplierId) : null;
  const supOptions = (state.suppliers || []).map(s =>
    `<option value="${s.id}" ${sup && s.id === supplierId ? 'selected' : ''}>${s.name}</option>`
  ).join('');

  var defaultDate = new Date(Date.now()+7*86400000).toISOString().slice(0,10);
  showModal('📋 New Purchase Order',
    '<div style="margin-bottom:10px;">'
    + '<label style="font-size:12px;color:var(--muted);">Supplier</label>'
    + '<select id="po-sup-id" class="input" style="margin-top:4px;" onchange="poFillSupplierDetails()">'
    + '<option value="">— Select Supplier —</option>'
    + supOptions
    + '</select></div>'
    + '<div id="po-sup-contact" style="font-size:12px;color:var(--muted);margin-bottom:8px;"></div>'
    + '<div style="margin-bottom:10px;">'
    + '<label style="font-size:12px;color:var(--muted);">Expected Delivery Date</label>'
    + '<input id="po-delivery-date" type="date" class="input" style="margin-top:4px;" value="' + defaultDate + '"></div>'
    + '<div style="margin-bottom:10px;">'
    + '<label style="font-size:12px;color:var(--muted);">Notes / Terms</label>'
    + '<input id="po-notes" class="input" style="margin-top:4px;" placeholder="Payment terms, delivery instructions…"></div>'
    + '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">'
    + '<span style="font-weight:600;font-size:13px;">Line Items</span>'
    + '<button class="btn btn-sm btn-outline" style="font-size:11px;padding:4px 10px;" onclick="poFillSuggestions()">🤖 Auto-fill low stock</button>'
    + '</div>'
    + '<div id="po-lines" style="margin-bottom:10px;"></div>'
    + '<button class="btn btn-sm btn-outline" onclick="poAddLine()" style="width:100%;margin-bottom:12px;">+ Add Item</button>'
    + '<div style="display:flex;gap:8px;flex-wrap:wrap;">'
    + '<button class="btn" onclick="savePO(&apos;draft&apos;)">💾 Save Draft</button>'
    + '<button class="btn" onclick="savePO(&apos;sent&apos;)">📤 Save & Mark Sent</button>'
    + '<button class="btn btn-outline" onclick="closeModal()">Cancel</button>'
    + '</div>'
    + '<div id="po-total-display" style="margin-top:10px;text-align:right;font-weight:700;color:var(--primary);font-size:14px;">Total: ₹0</div>'
  );
  // Always start blank — user can tap "Auto-fill low stock" if they want suggestions
  window._poLines = [{partId:'',partNo:'',name:'',qty:1,unitCost:0}];
  renderPOLines();
  if (sup) { document.getElementById('po-sup-id').value = supplierId; poFillSupplierDetails(); }
}

function poFillSuggestions() {
  const supplierId = document.getElementById('po-sup-id')?.value || '';
  const suggestions = Smart.smartReorderSuggestions ? Smart.smartReorderSuggestions(supplierId) : [];
  if (!suggestions.length) { toast('No low-stock parts found'); return; }
  window._poLines = suggestions.map(s => ({partId: s.part.id, partNo: s.part.partNo, name: s.part.name, qty: s.need, unitCost: s.part.costPrice || 0}));
  renderPOLines();
  toast('Filled ' + suggestions.length + ' low-stock parts');
}

function poFillSupplierDetails() {
  const supId = document.getElementById('po-sup-id')?.value;
  const sup = state.suppliers.find(s => s.id === supId);
  const el = document.getElementById('po-sup-contact');
  if (el && sup) el.textContent = (sup.phone ? '📞 ' + sup.phone : '') + (sup.city ? '  📍 ' + sup.city : '');
}

function renderPOLines() {
  var container = document.getElementById('po-lines');
  if (!container) return;
  if (!window._poLines) window._poLines = [{partId:'',partNo:'',name:'',qty:1,unitCost:0}];
  container.innerHTML = window._poLines.map(function(line, i) {
    var dv = line.name ? (line.name + (line.partNo ? ' (' + line.partNo + ')' : '')) : '';
    return '<div style="display:grid;grid-template-columns:1fr 72px 80px 28px;gap:5px;margin-bottom:8px;align-items:start;">'
      + '<div style="position:relative;">'
      + '<input class="input" style="font-size:12px;padding:6px;" placeholder="Search part..."'
      + ' value="' + escapeHtml(dv) + '" autocomplete="off"'
      + ' oninput="poLineSearch(' + i + ',this.value)" onfocus="poLineSearch(' + i + ',this.value)" onblur="setTimeout(function(){poLineHideDD(' + i + ')},180)" id="po-line-name-' + i + '">'
      + '<div id="po-dd-' + i + '" style="display:none;position:absolute;top:100%;left:0;right:0;z-index:999;background:white;border:1px solid var(--border);border-radius:8px;box-shadow:0 4px 16px rgba(0,0,0,.12);max-height:180px;overflow-y:auto;"></div>'
      + '</div>'
      + '<input type="number" class="input" style="font-size:12px;padding:6px;text-align:center;" placeholder="Qty"'
      + ' value="' + (line.qty || 1) + '" min="1" onchange="poLineUpdate(' + i + ',\'qty\',+this.value)">'
      + '<input type="number" class="input" style="font-size:12px;padding:6px;text-align:right;" placeholder="Cost"'
      + ' value="' + (line.unitCost || '') + '" min="0" onchange="poLineUpdate(' + i + ',\'unitCost\',+this.value)" id="po-cost-' + i + '">'
      + '<button onclick="poRemoveLine(' + i + ')" style="background:none;border:none;color:#dc2626;font-size:16px;cursor:pointer;padding:0;margin-top:6px;">&times;</button>'
      + '</div>';
  }).join('');
  updatePOTotal();
}

function poLineSearch(idx, val) {
  var dd = document.getElementById('po-dd-' + idx);
  if (!dd) return;
  var q = (val || '').toLowerCase().trim();
  var line = window._poLines && window._poLines[idx];
  var expected = line && line.name ? (line.name + (line.partNo ? ' (' + line.partNo + ')' : '')) : '';
  if (line && line.partId && val === expected) { dd.style.display = 'none'; return; }
  var hits = state.parts.filter(function(p) {
    if (!catStaffDone(p)) return false;
    if (!q) return true;
    return p.partNo.toLowerCase().indexOf(q) >= 0 || p.name.toLowerCase().indexOf(q) >= 0 || (p.brand||'').toLowerCase().indexOf(q) >= 0;
  }).slice(0, 30);
  if (!hits.length) { dd.style.display = 'none'; return; }
  dd.style.display = 'block';
  dd.innerHTML = hits.map(function(p) {
    var sb = p.stock === 0
      ? ' <span style="color:#dc2626;font-size:10px;font-weight:700;">OUT</span>'
      : ' <span style="color:#16a34a;font-size:10px;">' + p.stock + ' stk</span>';
    return '<div style="padding:8px 10px;cursor:pointer;border-bottom:1px solid var(--border);font-size:12px;" onmousedown="poLinePickPart(' + idx + ',\'' + p.id + '\')">'
      + '<div style="font-weight:600;">' + escapeHtml(p.name) + sb + '</div>'
      + '<div style="color:var(--muted);font-size:11px;">' + escapeHtml(p.partNo) + (p.brand ? ' &middot; ' + escapeHtml(p.brand) : '') + (p.costPrice ? ' &middot; Rs.' + p.costPrice : '') + '</div>'
      + '</div>';
  }).join('');
}

function poLinePickPart(idx, partId) {
  var p = getPartById(partId);
  if (!p || !window._poLines) return;
  window._poLines[idx] = { partId: p.id, partNo: p.partNo, name: p.name, qty: window._poLines[idx].qty || 1, unitCost: p.costPrice || 0 };
  var inp = document.getElementById('po-line-name-' + idx);
  if (inp) inp.value = p.name + (p.partNo ? ' (' + p.partNo + ')' : '');
  var cost = document.getElementById('po-cost-' + idx);
  if (cost) cost.value = p.costPrice || '';
  poLineHideDD(idx);
  updatePOTotal();
}

function poLineHideDD(idx) {
  var dd = document.getElementById('po-dd-' + idx);
  if (dd) dd.style.display = 'none';
}

function poLineUpdate(idx, field, val) {
  if (window._poLines && window._poLines[idx]) {
    window._poLines[idx][field] = val;
    updatePOTotal();
  }
}

function poRemoveLine(idx) {
  if (!window._poLines) return;
  window._poLines.splice(idx, 1);
  // Always keep at least one empty line
  if (window._poLines.length === 0) window._poLines = [{partId:'',partNo:'',name:'',qty:1,unitCost:0}];
  renderPOLines();
}

function poAddLine() {
  if (!window._poLines) window._poLines = [];
  window._poLines.push({partId:'',partNo:'',name:'',qty:1,unitCost:0});
  renderPOLines();
}

function updatePOTotal() {
  const total = (window._poLines || []).reduce((a,l) => a + (l.qty||0)*(l.unitCost||0), 0);
  const el = document.getElementById('po-total-display');
  if (el) el.textContent = 'Total: ' + rs(total);
}

async function savePO(status) {
  if (isReadOnly()) { toast('🔒 Read-only'); return; }
  const supId = document.getElementById('po-sup-id')?.value;
  const sup = state.suppliers.find(s => s.id === supId);
  const lines = (window._poLines || []).filter(l => l.name || l.partId);
  if (!lines.length) { toast('Add at least one item'); return; }

  const po = {
    id: uid(),
    poNumber: 'PO-' + Date.now().toString(36).toUpperCase(),
    supplierId: supId || null,
    supplierName: sup ? sup.name : (document.getElementById('po-sup-id')?.options[document.getElementById('po-sup-id').selectedIndex]?.text || 'Unknown'),
    supplierPhone: sup ? sup.phone : '',
    status,  // draft | sent | partial | received | cancelled
    createdAt: new Date().toISOString(),
    createdBy: state.session?.name || 'admin',
    expectedDate: document.getElementById('po-delivery-date')?.value || '',
    notes: document.getElementById('po-notes')?.value.trim() || '',
    items: lines.map(l => ({
      partId: l.partId || null,
      partNo: l.partNo || '',
      name: l.name || '',
      qty: +l.qty || 1,
      unitCost: +l.unitCost || 0,
      receivedQty: 0
    })),
    totalValue: lines.reduce((a,l) => a + (l.qty||0)*(l.unitCost||0), 0),
    receivedAt: null
  };

  if (!state.purchaseOrders) state.purchaseOrders = [];
  state.purchaseOrders.push(po);
  await save();
  closeModal();
  renderPOList();
  logActivity(`PO created: ${po.poNumber} — ${sup ? sup.name : 'supplier'} — ${po.items.length} item(s) — ${rs(po.totalValue)} — status: ${status}`, 'info');
  toast(`✅ PO ${po.poNumber} ${status === 'sent' ? 'saved & marked sent' : 'saved as draft'}`);

  if (status === 'sent' && sup && sup.phone) {
    if (confirm('Send PO to supplier on WhatsApp?')) sendPOWhatsApp(po.id);
  }
}

function sendPOWhatsApp(poId) {
  const po = (state.purchaseOrders || []).find(p => p.id === poId);
  if (!po || !po.supplierPhone) { toast('No phone for this supplier'); return; }
  let msg = `*Purchase Order — ${po.poNumber}*
`;
  msg += `From: ${state.settings.shopName}
`;
  msg += `Date: ${new Date(po.createdAt).toLocaleDateString('en-IN')}
`;
  if (po.expectedDate) msg += `Expected: ${new Date(po.expectedDate).toLocaleDateString('en-IN')}
`;
  msg += `
*Items:*
`;
  po.items.forEach(item => { msg += `▸ ${item.name} (${item.partNo}) × ${item.qty} @ ₹${item.unitCost} = ₹${item.qty * item.unitCost}
`; });
  msg += `
*Total: ₹${po.totalValue}*`;
  if (po.notes) msg += `

Notes: ${po.notes}`;
  window.open(`https://wa.me/${po.supplierPhone}?text=${encodeURIComponent(msg)}`, '_blank');
}

async function receivePO(poId) {
  if (isReadOnly()) { toast('🔒 Read-only'); return; }
  const po = (state.purchaseOrders || []).find(p => p.id === poId);
  if (!po) return;

  var itemsHTML = po.items.map(function(item, idx) {
    var remaining = item.qty - (item.receivedQty || 0);
    return '<div style="display:grid;grid-template-columns:1fr 70px 70px;gap:8px;align-items:center;margin-bottom:8px;font-size:13px;">'
      + '<div>' + escapeHtml(item.name||'') + '<br><small style="color:var(--muted);">' + escapeHtml(item.partNo||'') + '</small></div>'
      + '<div style="text-align:center;color:var(--muted);">Ordered: ' + item.qty + '</div>'
      + '<input type="number" id="recv-qty-' + idx + '" class="input" style="padding:6px;text-align:center;"'
      + ' value="' + remaining + '" min="0" max="' + remaining + '" placeholder="Recv">'
      + '</div>';
  }).join('');

  var recvDateStr = po.createdAt ? new Date(po.createdAt).toLocaleDateString('en-IN') : '';
  showModal('📦 Receive PO — ' + escapeHtml(po.poNumber||''),
    '<div style="color:var(--muted);font-size:12px;margin-bottom:12px;">' + escapeHtml(po.supplierName||'') + ' · ' + recvDateStr + '</div>'
    + '<div style="font-weight:600;font-size:13px;margin-bottom:8px;">Enter received quantities:</div>'
    + itemsHTML
    + '<div style="margin-top:8px;">'
    + '<label style="font-size:12px;color:var(--muted);">Invoice No (from supplier)</label>'
    + '<input id="recv-invoice" class="input" style="margin-top:4px;" placeholder="Supplier invoice number…">'
    + '</div>'
    + '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:12px;">'
    + '<button class="btn" onclick="confirmReceivePO(&apos;" + poId + "&apos;)">✅ Confirm Receipt</button>'
    + '<button class="btn btn-outline" onclick="closeModal()">Cancel</button>'
    + '</div>'
  );
}

async function confirmReceivePO(poId) {
  const po = (state.purchaseOrders || []).find(p => p.id === poId);
  if (!po) return;
  const invoiceNo = document.getElementById('recv-invoice')?.value.trim() || '';
  let anyReceived = false;
  let fullyReceived = true;

  for (let i = 0; i < po.items.length; i++) {
    const item = po.items[i];
    const receivedInput = document.getElementById('recv-qty-' + i);
    const recvQty = Math.min(+receivedInput?.value || 0, item.qty - (item.receivedQty || 0));
    if (recvQty > 0) {
      anyReceived = true;
      item.receivedQty = (item.receivedQty || 0) + recvQty;
      // Update stock
      const part = item.partId ? getPartById(item.partId) : null;
      if (part) {
        part.stock = Math.round((part.stock + recvQty) * 1000) / 1000;
        part.updatedAt = Date.now();
        try { await cloudPersistPartFields(part.id, { stock: part.stock, updatedAt: part.updatedAt }); } catch(e) { partsPendingEnqueue(part.id, e.message); }
      }
      // Also add to incomingStock log
      if (!state.incomingStock) state.incomingStock = [];
      state.incomingStock.push({
        id: uid(), date: new Date().toISOString(), partId: item.partId || null,
        partNo: item.partNo, partName: item.name || item.partNo, name: item.name, qty: recvQty,
        unit: (item.partId ? (state.parts.find(function(p){return p.id===item.partId;})||{}).unit||'pcs' : 'pcs'),
        unitCost: item.unitCost, supplierId: po.supplierId, supplierName: po.supplierName,
        orderedDate: new Date(po.createdAt).getTime(), expectedDays: 0,
        invoiceNo: invoiceNo, poId: poId, poNumber: po.poNumber
      });
    }
    if (item.receivedQty < item.qty) fullyReceived = false;
  }

  if (!anyReceived) { toast('Enter received quantities first'); return; }
  po.status = fullyReceived ? 'received' : 'partial';
  if (fullyReceived) po.receivedAt = new Date().toISOString();
  if (invoiceNo) po.supplierInvoiceNo = invoiceNo;

  await save();
  closeModal();
  renderPOList();
  renderInventory();
  renderHomeDebounced();
  logActivity(`PO received: ${po.poNumber} — ${fullyReceived ? 'fully' : 'partially'} received${invoiceNo ? ' · Inv: '+invoiceNo : ''}`, 'info');
  toast(`✅ Stock updated from PO ${po.poNumber}`);
}

async function cancelPO(poId) {
  if (!confirm('Cancel this PO?')) return;
  const po = (state.purchaseOrders || []).find(p => p.id === poId);
  if (!po) return;
  po.status = 'cancelled';
  await save();
  renderPOList();
  toast('PO cancelled');
}

function renderPOList() {
  var container = document.getElementById('po-list-container');
  if (!container) return;
  var filter = (document.getElementById('po-status-filter') || {}).value || 'all';
  var pos = (state.purchaseOrders || []).slice().reverse();
  if (filter !== 'all') pos = pos.filter(function(p) { return p.status === filter; });

  if (!pos.length) {
    container.innerHTML = '<div style="text-align:center;padding:40px;color:var(--muted);">No purchase orders yet.<br><br><button class="btn" onclick="openCreatePO()">Create First PO</button></div>';
    return;
  }

  var statusBadge = { draft:'🟡 Draft', sent:'🔵 Sent', partial:'🟠 Partial', received:'🟢 Received', cancelled:'🔴 Cancelled' };

  container.innerHTML = pos.map(function(po) {
    var totalOrdered  = po.items.reduce(function(a,i){ return a + i.qty; }, 0);
    var totalReceived = po.items.reduce(function(a,i){ return a + (i.receivedQty||0); }, 0);
    var receivedPct   = totalOrdered ? Math.round(totalReceived / totalOrdered * 100) : 0;
    var dateStr = po.createdAt ? new Date(po.createdAt).toLocaleDateString('en-IN') : '—';

    var itemLines = '';
    if (po.status !== 'received' && po.status !== 'cancelled') {
      itemLines = '<div style="background:#f8fafc;border-radius:8px;padding:8px;margin-top:10px;font-size:12px;">'
        + po.items.map(function(i) {
            return '<div style="display:flex;justify-content:space-between;margin-bottom:2px;">'
              + '<span>' + escapeHtml(i.name||'') + ' (' + escapeHtml(i.partNo||'') + ')</span>'
              + '<span style="color:var(--muted);">Ord:' + i.qty + ' Rcv:' + (i.receivedQty||0) + '</span>'
              + '</div>';
          }).join('')
        + '</div>';
    }

    var progressBar = '';
    if (po.status === 'partial') {
      progressBar = '<div style="margin-top:6px;">'
        + '<div style="background:#e5e7eb;border-radius:4px;height:6px;">'
        + '<div style="background:#f59e0b;border-radius:4px;height:6px;width:' + receivedPct + '%;"></div>'
        + '</div>'
        + '<div style="font-size:11px;color:var(--muted);margin-top:2px;">' + receivedPct + '% received</div>'
        + '</div>';
    }

    var btns = '';
    if (po.status === 'draft') {
      btns += '<button class="btn btn-sm" onclick="poMarkSent(&apos;" + po.id + "&apos;)">📤 Mark Sent</button> ';
    }
    if (po.status === 'sent' || po.status === 'partial') {
      btns += '<button class="btn btn-sm" onclick="receivePO(&apos;" + po.id + "&apos;)">📦 Receive</button> ';
    }
    if (po.supplierPhone) {
      btns += '<button class="btn btn-sm btn-outline" onclick="sendPOWhatsApp(&apos;" + po.id + "&apos;)">📲 WhatsApp</button> ';
    }
    if (po.status === 'draft' || po.status === 'sent') {
      btns += '<button class="btn btn-sm btn-outline" style="color:#dc2626;" onclick="cancelPO(&apos;" + po.id + "&apos;)">Cancel</button>';
    }

    return '<div style="background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:14px;margin-bottom:10px;">'
      + '<div style="display:flex;justify-content:space-between;align-items:flex-start;gap:8px;flex-wrap:wrap;">'
      + '<div>'
      + '<div style="font-weight:700;font-size:14px;">' + escapeHtml(po.poNumber||'') + '</div>'
      + '<div style="font-size:13px;color:var(--muted);">' + escapeHtml(po.supplierName||'') + ' · ' + dateStr + ' · ' + po.items.length + ' item(s)</div>'
      + '</div>'
      + '<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">'
      + '<span style="font-size:12px;font-weight:600;">' + (statusBadge[po.status] || po.status) + '</span>'
      + '<span style="font-weight:700;font-size:14px;">' + rs(po.totalValue||0) + '</span>'
      + '</div>'
      + '</div>'
      + itemLines
      + progressBar
      + '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:10px;">' + btns + '</div>'
      + '</div>';
  }).join('');
}

async function poMarkSent(poId) {
  const po = (state.purchaseOrders || []).find(p => p.id === poId);
  if (!po) return;
  po.status = 'sent';
  await save();
  renderPOList();
  toast('PO marked as sent');
  if (po.supplierPhone && confirm('Send to supplier on WhatsApp?')) sendPOWhatsApp(poId);
}

// Hook Smart analytics to provide PO suggestions
Smart.smartReorderSuggestions = function(supplierId) {
  return state.parts
    .filter(p => saleablePart(p) && Smart.daysUntilStockout(p) <= 14)
    .filter(p => !supplierId || p.supplierName === (state.suppliers.find(s=>s.id===supplierId)||{}).name)
    .map(p => ({ part: p, need: Smart.smartReorderQty(p) }))
    .filter(s => s.need > 0)
    .slice(0, 20);
};

// ============================================================
// RETURNS / CREDIT NOTES MODULE
// ============================================================
function openReturnModal(saleId) {
  const sale = state.sales.find(s => s.id === saleId);
  if (!sale) return;
  const cust = getCustomerById(sale.customerId);
  const existingReturn = (state.saleReturns || []).find(r => r.originalSaleId === saleId);

  var itemsHTML = sale.items.map(function(item, idx) {
    var prevRet = existingReturn ? (existingReturn.items || []).find(function(r){ return r.partId === item.partId; }) : null;
    var maxQty = item.qty - (prevRet ? prevRet.qty : 0);
    if (maxQty <= 0) {
      return '<tr><td>' + escapeHtml(item.name||'') + ' <small>(' + escapeHtml(item.partNo||'') + ')</small></td>'
        + '<td colspan="3" style="color:var(--muted);text-align:center;">Already returned</td></tr>';
    }
    return '<tr>'
      + '<td style="font-size:13px;">' + escapeHtml(item.name||'') + '<br><small style="color:var(--muted);">' + escapeHtml(item.partNo||'') + '</small></td>'
      + '<td style="text-align:center;">' + item.qty + '</td>'
      + '<td style="text-align:center;">'
      + '<input type="number" id="ret-qty-' + idx + '" min="0" max="' + maxQty + '" value="0" step="1"'
      + ' style="width:60px;padding:4px;border:1px solid #d1d5db;border-radius:6px;text-align:center;"'
      + ' onchange="updateReturnTotal()">'
      + '</td>'
      + '<td style="text-align:right;font-size:13px;">' + rs(item.price * item.qty) + '</td>'
      + '</tr>';
  }).join('');

  var custName = cust ? cust.name : 'Walk-in';
  var saleDateStr = new Date(sale.date).toLocaleDateString('en-IN');
  showModal('↩️ Process Return — ' + sale.invoiceNo,
    '<div style="font-size:13px;color:var(--muted);margin-bottom:10px;">'
    + escapeHtml(custName) + ' · ' + saleDateStr + ' · ' + rs(sale.total)
    + '</div>'
    + '<table style="width:100%;border-collapse:collapse;margin-bottom:12px;">'
    + '<thead><tr style="border-bottom:1px solid #e5e7eb;font-size:12px;color:var(--muted);">'
    + '<th style="text-align:left;padding:6px 4px;">Item</th>'
    + '<th style="text-align:center;">Sold</th>'
    + '<th style="text-align:center;">Return Qty</th>'
    + '<th style="text-align:right;">Amount</th>'
    + '</tr></thead>'
    + '<tbody id="return-items-tbody">' + itemsHTML + '</tbody>'
    + '</table>'
    + '<div style="margin-bottom:10px;">'
    + '<label style="font-size:12px;color:var(--muted);">Reason</label>'
    + '<select id="ret-reason" class="input" style="margin-top:4px;">'
    + '<option value="wrong_part">Wrong part supplied</option>'
    + '<option value="defective">Defective / damaged</option>'
    + '<option value="duplicate_order">Duplicate order</option>'
    + '<option value="customer_changed_mind">Customer changed mind</option>'
    + '<option value="fitment_issue">Fitment issue</option>'
    + '<option value="other">Other</option>'
    + '</select></div>'
    + '<div style="margin-bottom:10px;">'
    + '<label style="font-size:12px;color:var(--muted);">Notes (optional)</label>'
    + '<input id="ret-notes" class="input" style="margin-top:4px;" placeholder="Any additional notes…"></div>'
    + '<div id="return-total-display" style="background:#f0fdf4;border-radius:8px;padding:10px;margin-bottom:12px;text-align:right;font-weight:700;font-size:15px;">Refund: ₹0</div>'
    + '<button class="btn" onclick="processReturn(&apos;" + saleId + "&apos;)">✅ Confirm Return</button>'
    + '<button class="btn btn-outline" onclick="closeModal()" style="margin-left:8px;">Cancel</button>'
  );
  // Store sale reference for updateReturnTotal
  window._returnSaleRef = sale;
}

function updateReturnTotal() {
  const sale = window._returnSaleRef;
  if (!sale) return;
  let total = 0;
  sale.items.forEach((item, idx) => {
    const input = document.getElementById('ret-qty-' + idx);
    if (input) {
      const qty = Math.min(+input.value || 0, item.qty);
      total += qty * item.price;
    }
  });
  const el = document.getElementById('return-total-display');
  if (el) el.textContent = 'Refund: ' + rs(total);
}

async function processReturn(saleId) {
  const sale = state.sales.find(s => s.id === saleId);
  if (!sale) return;
  const reason = document.getElementById('ret-reason')?.value || 'other';
  const notes  = document.getElementById('ret-notes')?.value.trim() || '';

  const returnItems = [];
  let refundTotal = 0;
  sale.items.forEach((item, idx) => {
    const input = document.getElementById('ret-qty-' + idx);
    const qty = Math.min(+input?.value || 0, item.qty);
    if (qty > 0) {
      returnItems.push({ partId: item.partId, partNo: item.partNo, name: item.name, qty, price: item.price });
      refundTotal += qty * item.price;
    }
  });

  if (!returnItems.length) { toast('Enter at least 1 qty to return'); return; }

  const returnRecord = {
    id: uid(),
    originalSaleId: saleId,
    invoiceNo: sale.invoiceNo + '-RET',
    date: new Date().toISOString(),
    customerId: sale.customerId || null,
    items: returnItems,
    refundTotal,
    reason,
    notes,
    processedBy: state.session?.name || 'admin',
    paymentMode: sale.paymentMode
  };

  // Restore stock for returned items
  for (const ri of returnItems) {
    const p = getPartById(ri.partId);
    if (p) {
      p.stock = Math.round((p.stock + ri.qty) * 1000) / 1000;
      p.updatedAt = Date.now();
      try { await cloudPersistPartFields(ri.partId, { stock: p.stock, updatedAt: p.updatedAt }); } catch(e) { if (ri.partId) partsPendingEnqueue(ri.partId, e.message); }
    }
  }

  // If credit sale, reduce customer's totalDue
  if (sale.paymentMode === 'credit' && sale.customerId) {
    const cust = getCustomerById(sale.customerId);
    if (cust) cust.totalDue = Math.max(0, (cust.totalDue || 0) - refundTotal);
  }

  if (!state.saleReturns) state.saleReturns = [];
  state.saleReturns.push(returnRecord);
  invalidateVelocityCache();
  await save();

  logActivity(`Return processed: ${returnRecord.invoiceNo} — ${returnItems.length} item(s) — Refund: ${rs(refundTotal)} (${reason})`, 'info');
  closeModal();
  renderInventory();
  renderHome();
  showReturnReceipt(returnRecord);
}

function showReturnReceipt(ret) {
  const cust = getCustomerById(ret.customerId);
  var receiptItems = ret.items.map(function(i) {
    return '<tr><td>' + escapeHtml(i.name||'') + '<br><small>' + escapeHtml(i.partNo||'') + '</small></td>'
      + '<td style="text-align:center;">' + i.qty + '</td>'
      + '<td style="text-align:right;">' + rs(i.qty * i.price) + '</td></tr>';
  }).join('');
  var retDateStr = new Date(ret.date).toLocaleDateString('en-IN');
  var reasonStr  = (ret.reason||'').replace(/_/g,' ');
  showModal('Credit Note — ' + ret.invoiceNo,
    '<div style="font-family:monospace;background:#f0fdf4;padding:12px;border-radius:8px;font-size:13px;">'
    + '<div style="text-align:center;font-weight:700;font-size:15px;">CREDIT NOTE</div>'
    + '<div style="text-align:center;color:var(--muted);">' + escapeHtml(state.settings.shopName||'') + '</div>'
    + '<div style="border-top:1px dashed #d1fae5;margin:8px 0;"></div>'
    + '<div style="display:flex;justify-content:space-between;"><span>Ref: <b>' + ret.invoiceNo + '</b></span><span>' + retDateStr + '</span></div>'
    + '<div>Customer: ' + (cust ? escapeHtml(cust.name) : 'Walk-in') + '</div>'
    + '<div>Reason: ' + escapeHtml(reasonStr) + '</div>'
    + '<div style="border-top:1px dashed #d1fae5;margin:8px 0;"></div>'
    + '<table style="width:100%;font-size:12px;"><thead><tr><th align="left">Item</th><th>Qty</th><th align="right">Amt</th></tr></thead><tbody>' + receiptItems + '</tbody></table>'
    + '<div style="border-top:1px dashed #d1fae5;margin:8px 0;"></div>'
    + '<div style="display:flex;justify-content:space-between;font-weight:700;font-size:16px;color:#16a34a;"><span>REFUND</span><span>' + rs(ret.refundTotal) + '</span></div>'
    + (ret.notes ? '<div style="margin-top:6px;font-size:11px;color:var(--muted);">Notes: ' + escapeHtml(ret.notes) + '</div>' : '')
    + '</div>'
    + '<button class="btn mt-1" onclick="window.print()">🖨️ Print Credit Note</button>'
    + '<button class="btn btn-outline mt-1" onclick="closeModal()">Close</button>'
  );
}

// Show return history for a sale in the sale detail view
function renderReturnBadge(saleId) {
  const returns = (state.saleReturns || []).filter(r => r.originalSaleId === saleId);
  if (!returns.length) return '';
  const totalRefunded = returns.reduce((a, r) => a + r.refundTotal, 0);
  return `<span style="background:#fef9c3;color:#854d0e;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700;margin-left:6px;">↩️ ${rs(totalRefunded)} returned</span>`;
}

function sendBillWA(saleId) {
  const sale = state.sales.find(s => s.id === saleId);
  const cust = getCustomerById(sale.customerId);
  if (!cust || !cust.phone) return;
  let msg = `*${state.settings.shopName}*\nBill: ${sale.invoiceNo}\nDate: ${new Date(sale.date).toLocaleString('en-IN')}\n\n`;
  sale.items.forEach(i => msg += `• ${i.name} (${i.partNo})\n  ${i.qty} × ${rs(i.price)} = ${rs(i.qty*i.price)}\n`);
  msg += `\nSubtotal: ${rs(sale.subtotal)}\n`;
  if (sale.discount) msg += `Discount: -${rs(sale.discount)}\n`;
  msg += `*TOTAL: ${rs(sale.total)}*\nPayment: ${sale.paymentMode.toUpperCase()}\n`;
  if (sale.paymentMode==='credit') msg += `\nTerms: ${state.settings.creditTerms}\n`;
  msg += '\nThank you!';
  window.open(`https://wa.me/${cust.phone}?text=${encodeURIComponent(msg)}`, '_blank');
}

// ============================================================
// VOICE INPUT
// ============================================================
function getRecognizer() {
  const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
  if (!SR) { toast('Voice not supported on this browser'); return null; }
  const r = new SR();
  r.lang = 'en-IN'; r.interimResults = false; r.maxAlternatives = 1;
  return r;
}
function voiceSearch(inputId) {
  const r = getRecognizer(); if (!r) return;
  const btn = event.target;
  btn.classList.add('listening');
  r.start();
  r.onresult = e => {
    const txt = e.results[0][0].transcript;
    document.getElementById(inputId).value = txt;
    document.getElementById(inputId).oninput && document.getElementById(inputId).oninput();
    btn.classList.remove('listening');
  };
  r.onerror = r.onend = () => btn.classList.remove('listening');
}
function voiceSale() {
  const r = getRecognizer(); if (!r) return;
  const btn = event.target;
  btn.classList.add('listening');
  toast('Speak: e.g. "add 2 oil filter"');
  r.start();
  r.onresult = e => {
    const txt = e.results[0][0].transcript.toLowerCase();
    btn.classList.remove('listening');
    // Parse "add N <part>"
    const m = txt.match(/(?:add\s+)?(\d+)?\s*(.+)/);
    if (m) {
      const qty = +m[1] || 1;
      const term = m[2].trim();
      const found = state.parts.find(p =>
        p.name.toLowerCase().includes(term) ||
        p.partNo.toLowerCase().includes(term) ||
        term.includes(p.name.toLowerCase())
      );
      if (found) {
        for (let i=0; i<qty; i++) addToCart(found.id);
        toast(`Added ${qty} × ${found.name}`);
      } else {
        document.getElementById('sale-search').value = term;
        renderSaleSearch();
        toast(`No exact match — searching "${term}"`);
      }
    }
  };
  r.onerror = r.onend = () => btn.classList.remove('listening');
}
function voiceChat() {
  const r = getRecognizer(); if (!r) return;
  const btn = event.target;
  btn.classList.add('listening');
  r.start();
  r.onresult = e => {
    document.getElementById('chat-input').value = e.results[0][0].transcript;
    btn.classList.remove('listening');
    sendChat();
  };
  r.onerror = r.onend = () => btn.classList.remove('listening');
}

// ============================================================
// WHATSAPP TAB
// ============================================================
function showWASection(name, evt) {
  document.querySelectorAll('.wa-section').forEach(s => s.style.display = 'none');
  document.getElementById('wa-'+name).style.display = 'block';
  document.querySelectorAll('#tab-whatsapp .chip').forEach(c => c.classList.remove('active'));
  if (evt && evt.target && evt.target.classList) evt.target.classList.add('active');
}

function renderQuoteCustomers() {
  const sel = document.getElementById('q-customer');
  sel.innerHTML = '<option value="">— New / Walk-in —</option>' +
    state.customers.map(c => `<option value="${c.id}">${c.name} ${c.trusted?'★':''}</option>`).join('');
  sel.onchange = () => {
    const c = getCustomerById(sel.value);
    document.getElementById('q-phone').value = c ? c.phone : '';
    renderQuoteCart();
  };
}
function renderQuoteSearch() {
  const q = (document.getElementById('q-search').value || '').toLowerCase().trim();
  const div = document.getElementById('q-results');
  if (!q) { div.innerHTML = ''; return; }
  const results = state.parts.filter(p => saleablePart(p) && (
    p.partNo.toLowerCase().includes(q) ||
    p.name.toLowerCase().includes(q) ||
    p.brand.toLowerCase().includes(q)
  )).slice(0, 6);
  div.innerHTML = results.map(p => `
    <div class="part-item" onclick="addToQuote('${p.id}')">
      <div class="part-row">
        <div><div class="part-name">${p.name}</div><div class="part-no">${p.partNo} • ${p.stock} in stock</div></div>
        <div class="part-prices"><small>+ Add</small></div>
      </div>
    </div>`).join('');
}
function addToQuote(id) {
  if (!state.quoteCart.includes(id)) state.quoteCart.push(id);
  document.getElementById('q-search').value = '';
  document.getElementById('q-results').innerHTML = '';
  renderQuoteCart();
}
function removeQuote(id) {
  state.quoteCart = state.quoteCart.filter(x => x !== id);
  renderQuoteCart();
}
function renderQuoteCart() {
  const div = document.getElementById('q-cart');
  if (state.quoteCart.length === 0) { div.innerHTML = ''; return; }
  const custId = document.getElementById('q-customer').value;
  const cust = getCustomerById(custId);
  const showCredit = cust && cust.trusted;
  div.innerHTML = '<div class="divider"></div><b>Items in Quote:</b>' +
    state.quoteCart.map(id => {
      const p = getPartById(id);
      if (!p) return '';
      return `<div class="cart-item">
        <div style="flex:1;min-width:0;">
          <div class="bold">${p.name}</div>
          <div class="part-no">${p.partNo} • Stock: ${p.stock}</div>
          <small>Instant: ${rs(p.instantPrice)}${showCredit?` | Credit: ${rs(p.creditPrice)}`:''}</small>
        </div>
        <button class="qty-btn" onclick="removeQuote('${id}')">×</button>
      </div>`;
    }).join('');
}
function sendQuote() {
  const phone = document.getElementById('q-phone').value.trim();
  if (!phone) { toast('Enter customer phone'); return; }
  if (state.quoteCart.length === 0) { toast('Add parts to quote'); return; }
  const custId = document.getElementById('q-customer').value;
  const cust = getCustomerById(custId);
  const showCredit = cust && cust.trusted;
  let msg = `*${state.settings.shopName}* - Quote\n\nDear ${cust?cust.name:'Customer'},\n\nHere are the prices for your enquiry:\n\n`;
  state.quoteCart.forEach(id => {
    const p = getPartById(id);
    msg += `▸ *${p.name}*\n  Part No: ${p.partNo}\n  Brand: ${p.brand}\n  Stock: ${p.stock>0?p.stock+' available':'OUT OF STOCK'}\n  Instant Price: ${rs(p.instantPrice)}\n`;
    if (showCredit) msg += `  Credit Price: ${rs(p.creditPrice)} (${state.settings.creditTerms})\n`;
    msg += '\n';
  });
  msg += `Reply to confirm. Prices valid for 7 days.\n\n${state.settings.shopName}`;
  if (state.settings.shopPhone) msg += `\n📞 ${state.settings.shopPhone}`;
  window.open(`https://wa.me/${phone}?text=${encodeURIComponent(msg)}`, '_blank');
  state.quoteCart = []; renderQuoteCart();
}

function updateBroadcastMsg() {
  const type = document.getElementById('b-type').value;
  const ta = document.getElementById('b-message');
  let msg = '';
  if (type === 'newarrival') {
    msg = `*${state.settings.shopName}* 🔧\n\n*New Stock Arrived!*\n\n`;
    state.parts.slice(-5).forEach(p => msg += `▸ ${p.name} (${p.partNo}) - ${rs(p.instantPrice)}\n`);
    msg += '\nVisit shop or reply on WhatsApp to order.';
  } else if (type === 'pricelist') {
    msg = `*${state.settings.shopName}* - Price List\n\n`;
    const grouped = {};
    state.parts.forEach(p => { grouped[p.machineType] = grouped[p.machineType] || []; grouped[p.machineType].push(p); });
    Object.keys(grouped).forEach(mt => {
      msg += `\n*${mt.toUpperCase()}*\n`;
      grouped[mt].slice(0,5).forEach(p => msg += `${p.partNo} - ${p.name}: ${rs(p.instantPrice)}\n`);
    });
  } else if (type === 'offer') {
    msg = `*${state.settings.shopName}* - SPECIAL OFFER 🎉\n\nDiscount on all parts!\nExtra 5% off on orders above ₹10,000\n\nValid till month end.`;
  } else {
    msg = `*${state.settings.shopName}*\n\n[Type your message here]`;
  }
  ta.value = msg;
}

function startBroadcast() {
  const target = document.getElementById('b-target').value;
  const msg = document.getElementById('b-message').value.trim();
  if (!msg) { toast('Message empty'); return; }
  let recipients = state.customers.slice();
  if (target === 'wholesale') recipients = recipients.filter(c => c.type === 'wholesale');
  else if (target === 'trusted') recipients = recipients.filter(c => c.trusted);
  if (recipients.length === 0) { toast('No matching customers'); return; }
  openBulkQueue(recipients.map(c => ({phone: c.phone, name: c.name, message: msg})));
}

// Bulk send queue — opens each WA in sequence with confirmation
function openBulkQueue(items) {
  state.bulkQueue = items.slice();
  let idx = 0;
  const total = items.length;
  showModal(`Send to ${total} contacts`, `
    <small>Click each "Send" — WhatsApp opens with the message pre-filled. After sending, come back and tap Next.</small>
    <div class="bulk-queue">
      <div id="bulk-status">Ready to send to ${total} contacts</div>
      <div class="bulk-progress"><div class="bulk-progress-fill" id="bulk-fill" style="width:0%"></div></div>
    </div>
    <div id="bulk-current" class="mt-2"></div>
    <div class="row2 mt-2">
      <button class="btn btn-outline" onclick="bulkAllAtOnce()">⚡ Open All At Once</button>
      <button class="btn" onclick="closeModal()">Done</button>
    </div>
  `);
  showBulkCurrent(0);
}
function showBulkCurrent(idx) {
  const item = state.bulkQueue[idx];
  if (!item) {
    document.getElementById('bulk-current').innerHTML = '<div class="empty">✓ All done!</div>';
    document.getElementById('bulk-fill').style.width = '100%';
    document.getElementById('bulk-status').textContent = 'Complete';
    return;
  }
  document.getElementById('bulk-fill').style.width = (idx/state.bulkQueue.length*100)+'%';
  document.getElementById('bulk-status').textContent = `${idx+1} of ${state.bulkQueue.length}`;
  document.getElementById('bulk-current').innerHTML = `
    <div class="card" style="margin:0;">
      <b>${item.name}</b><br><small>${item.phone}</small>
      <div class="row2 mt-2">
        <a class="wa-btn" href="https://wa.me/${item.phone}?text=${encodeURIComponent(item.message)}" target="_blank" onclick="setTimeout(()=>showBulkCurrent(${idx+1}),500)">📲 Send</a>
        <button class="btn btn-outline" onclick="showBulkCurrent(${idx+1})">Skip →</button>
      </div>
    </div>`;
}
function bulkAllAtOnce() {
  if (!confirm(`Open ${state.bulkQueue.length} WhatsApp tabs at once? Browser may block popups — allow them.`)) return;
  state.bulkQueue.forEach((item, i) => {
    setTimeout(() => window.open(`https://wa.me/${item.phone}?text=${encodeURIComponent(item.message)}`, '_blank'), i*500);
  });
}

// SUPPLIER REORDER
function renderSupplierOrders() {
  const div = document.getElementById('supplier-orders');
  // Smart: include parts at/below min OR forecast stockout in 14 days
  const needs = state.parts.filter(p => {
    if (!saleablePart(p)) return false;
    if (p.stock <= p.minStock) return true;
    const days = Smart.daysUntilStockout(p);
    return days <= 14;
  });
  if (needs.length === 0) { div.innerHTML = '<div class="empty"><div class="icon">✓</div>All stock healthy</div>'; return; }
  const grouped = {};
  needs.forEach(p => {
    const key = (p.supplierName || 'No Supplier') + '|' + (p.supplierPhone || '');
    grouped[key] = grouped[key] || [];
    grouped[key].push(p);
  });
  div.innerHTML = Object.entries(grouped).map(([key, parts]) => {
    const [name, phone] = key.split('|');
    const msg = `Hello ${name},\n\nPlease arrange the following parts:\n\n` +
      parts.map(p => {
        const need = Smart.smartReorderQty(p);
        return `▸ ${p.name} (${p.partNo}) - ${p.brand}\n  Need: ${need} pcs (current: ${p.stock})`;
      }).join('\n\n') +
      `\n\nPlease confirm availability & delivery.\n\nThanks,\n${state.settings.shopName}`;
    return `<div class="card" style="margin-bottom:10px;">
      <div class="bold">${name}</div>
      <small>${phone || 'No phone'}</small>
      <div class="mt-1">
        ${parts.map(p => {
          const v = Smart.velocity(p.id);
          const days = Smart.daysUntilStockout(p);
          const need = Smart.smartReorderQty(p);
          return `<div style="font-size:13px;">• ${p.name} (${p.partNo}) — Stock: <b class="${p.stock===0?'text-danger':'text-warn'}">${p.stock}</b>${v>0?` · ${days}d left · 🧠 order ${need}`:''}</div>`;
        }).join('')}
      </div>
      ${phone ? `<a class="wa-btn mt-2" target="_blank" href="https://wa.me/${phone}?text=${encodeURIComponent(msg)}">📲 Send Order on WhatsApp</a>` : '<small class="text-warn">No supplier phone</small>'}
    </div>`;
  }).join('');
}
function orderFromSupplier(partId) {
  const p = getPartById(partId);
  if (!p.supplierPhone) { toast('No supplier phone'); return; }
  const need = Smart.smartReorderQty(p);
  const msg = `Hello ${p.supplierName},\n\nPlease arrange:\n\n▸ ${p.name}\n  Part No: ${p.partNo}\n  Brand: ${p.brand}\n  Quantity: ${need} pcs\n\nPlease confirm availability & price.\n\nThanks,\n${state.settings.shopName}`;
  window.open(`https://wa.me/${p.supplierPhone}?text=${encodeURIComponent(msg)}`, '_blank');
}

// CUSTOMER OUTREACH
function renderOutreach() {
  const due = Smart.reorderDueCustomers();
  const div = document.getElementById('outreach-list');
  if (due.length === 0) { div.innerHTML = '<div class="empty"><div class="icon">✓</div>No customers due for reorder</div>'; return; }
  div.innerHTML = due.map(d => {
    const msg = `Namaste ${d.customer.name},\n\nIts been ${d.daysSinceLastBuy} days since you last bought *${d.part.name}* (${d.part.partNo}).\n\nWe have stock available at ${rs(d.customer.trusted ? d.part.creditPrice : d.part.instantPrice)}${d.customer.trusted ? ' (credit available)' : ''}.\n\nReply if you need it — we can deliver fast.\n\n${state.settings.shopName}\n📞 ${state.settings.shopPhone||''}`;
    return `<div class="cart-item">
      <div style="flex:1;">
        <b>${d.customer.name}</b>
        <small> · ${d.daysSinceLastBuy}d ago</small><br>
        <small>${d.part.name} (${d.part.partNo})</small>
      </div>
      <a class="wa-btn btn-sm" style="width:auto;padding:8px 14px;" target="_blank" href="https://wa.me/${d.customer.phone}?text=${encodeURIComponent(msg)}">📲 Send</a>
    </div>`;
  }).join('');
}
function sendReorderReminder(custId, partId) {
  const c = getCustomerById(custId);
  const p = getPartById(partId);
  if (!c || !p) return;
  const lastSale = state.sales.filter(s => s.customerId === custId && s.items.some(i => i.partId === partId)).sort((a,b) => b.date.localeCompare(a.date))[0];
  const lastDays = lastSale ? daysAgo(lastSale.date) : 60;
  const msg = `Namaste ${c.name},\n\nIts been ${lastDays} days since you last bought *${p.name}* (${p.partNo}).\n\nWe have stock at ${rs(c.trusted ? p.creditPrice : p.instantPrice)}${c.trusted ? ' (credit available)' : ''}.\n\nReply if you need it.\n\n${state.settings.shopName}`;
  window.open(`https://wa.me/${c.phone}?text=${encodeURIComponent(msg)}`, '_blank');
}

function renderPaymentReminders() {
  const div = document.getElementById('payment-reminders');
  const dues = state.customers.filter(c => c.totalDue > 0);
  if (dues.length === 0) { div.innerHTML = '<div class="empty">No pending credit</div>'; return; }
  div.innerHTML = dues.map(c => `
    <div class="cart-item">
      <div style="flex:1;"><b>${c.name}</b><br><small class="text-warn">${rs(c.totalDue)} due</small></div>
      <button class="wa-btn btn-sm" style="width:auto;padding:8px 14px;" onclick="sendPaymentReminder('${c.id}')">📲 Remind</button>
    </div>`).join('');
}
function sendPaymentReminder(custId) {
  const c = getCustomerById(custId);
  if (!c) return;
  const msg = `Namaste ${c.name},\n\nThis is a friendly reminder that ${rs(c.totalDue)} is pending on your account at ${state.settings.shopName}.\n\nKindly arrange payment at your convenience.\n\nThank you!\n${state.settings.shopName}`;
  window.open(`https://wa.me/${c.phone}?text=${encodeURIComponent(msg)}`, '_blank');
}

// INCOMING ORDERS
function renderOrders() {
  const div = document.getElementById('order-list');
  if (state.orders.length === 0) { div.innerHTML = '<div class="empty"><div class="icon">📨</div>No orders logged</div>'; return; }
  div.innerHTML = [...state.orders].reverse().map(o => `
    <div class="sale-row">
      <div class="sale-head">
        <div><b>${o.customer}</b><br><small>${o.phone} • ${new Date(o.date).toLocaleString('en-IN')}</small></div>
        <span class="tag ${o.status==='done'?'':'warn'}">${o.status}</span>
      </div>
      <div style="font-size:13px;margin-top:4px;">${o.items}</div>
      <div class="row2 mt-1">
        ${o.status!=='done'?`<button class="btn btn-sm btn-success" onclick="markOrderDone('${o.id}')">✓ Done</button>`:'<span></span>'}
        <button class="btn btn-sm btn-danger" onclick="deleteOrder('${o.id}')">Delete</button>
      </div>
    </div>`).join('');
}
function openAddOrder() {
  showModal('Log WhatsApp Order', `
    <div class="field"><label>Customer Name</label><input class="input" id="o-name"></div>
    <div class="field"><label>Phone</label><input class="input" id="o-phone"></div>
    <div class="field"><label>Order Details</label><textarea class="input" id="o-items" rows="4"></textarea></div>
    <button class="btn" onclick="saveOrder()">Save Order</button>
  `);
}
async function saveOrder() {
  const o = { id: uid(), date: new Date().toISOString(),
    customer: document.getElementById('o-name').value.trim(),
    phone: document.getElementById('o-phone').value.trim(),
    items: document.getElementById('o-items').value.trim(),
    status: 'pending' };
  if (!o.customer || !o.items) { toast('Fill customer & items'); return; }
  state.orders.push(o);save();
  const _os=await waitForFirestore('Save Order');
  if(!_os){state.orders.pop();save();return;}
  closeModal();renderOrders();
}
function markOrderDone(id) { const o = state.orders.find(x => x.id === id); o.status = 'done'; save(); renderOrders(); }
function deleteOrder(id) {
  state.orders = state.orders.filter(x => x.id !== id);
  window._ownerDeleteInProgress = true;
  save().finally(() => { window._ownerDeleteInProgress = false; });
  renderOrders();
}

// ============================================================
// AI BRAIN TAB
// ============================================================
function renderBrain() {
  // Welcome message
  const welcome = document.getElementById('chat-welcome');
  if (state.chat.length === 0) {
    welcome.textContent = `Hi! I'm your shop AI. Ask me anything about your inventory, sales, customers, or what to do today.`;
  }
  // Suggestions
  document.getElementById('chat-suggestions').innerHTML = [
    "What should I order today?",
    "Show me top selling parts",
    "Which customers haven't bought in 60 days?",
    "Write a Diwali offer message",
    "What's my profit this month?",
    "Find slow-moving stock"
  ].map(s => `<span class="suggest-chip" onclick="document.getElementById('chat-input').value='${s.replace(/'/g,"\\'")}';sendChat()">${s}</span>`).join('');

  renderBrainBriefing();
}

function showBrainSection(name, evt) {
  document.querySelectorAll('.brain-section').forEach(s => s.style.display = 'none');
  document.getElementById('brain-'+name).style.display = 'block';
  document.querySelectorAll('#tab-brain .chip').forEach(c => c.classList.remove('active'));
  if (evt && evt.target) evt.target.classList.add('active');
  if (name === 'briefing') renderBrainBriefing();
  if (name === 'reorder') renderBrainReorder();
  if (name === 'customers') renderBrainCustomers();
  if (name === 'dead') renderBrainDead();
  if (name === 'velocity') renderBrainVelocity();
  if (name === 'profit') renderBrainProfit();
  if (name === 'cleaner') { /* static - user fills in */ }
}

function renderBrainBriefing() {
  if (!window._briefingCache || (Date.now() - window._briefingCache.ts) > (window._isMobile ? 30000 : 10000)) {
    window._briefingCache = { ts: Date.now(), actions: Smart.briefing() };
  }
  const actions = window._briefingCache.actions;
  const div = document.getElementById('brain-actions');
  if (actions.length === 0) {
    div.innerHTML = '<div class="empty"><div class="icon">✓</div>All clear — no urgent actions today.</div>';
    return;
  }
  div.innerHTML = actions.map(a => `
    <div class="briefing-item ${a.type}">
      <div><span style="font-size:16px;">${a.icon}</span> <b>${a.title}</b></div>
      <small>${a.detail}</small>
      <a class="briefing-action" onclick="${a.onClick};return false;" href="#">${a.action}</a>
    </div>`).join('');
}

function renderBrainReorder() {
  // Cache for 30s — this maps velocity over ALL parts which is expensive
  if (!window._brainReorderCache || (Date.now() - window._brainReorderCache.ts) > 30000) {
    const list = state.parts.map(p => ({...p, vel: Smart.velocity(p.id), days: Smart.daysUntilStockout(p), reorder: Smart.smartReorderQty(p)}))
      .filter(p => p.vel > 0 && (p.days <= 30 || p.stock <= p.minStock))
      .sort((a,b) => a.days - b.days);
    window._brainReorderCache = { ts: Date.now(), list };
  }
  const list = window._brainReorderCache.list;
  const div = document.getElementById('brain-reorder-list');
  if (list.length === 0) { div.innerHTML = '<div class="empty"><div class="icon">✓</div>Nothing critical to reorder</div>'; return; }
  div.innerHTML = list.map(p => `
    <div class="part-item">
      <div class="part-row">
        <div style="flex:1;">
          <div class="part-name">${p.name}</div>
          <div class="part-no">${p.partNo} • ${p.supplierName}</div>
          <div class="velocity"><span class="${p.days<=7?'hot':'warm'}">${p.days===Infinity?'∞':p.days+'d'} left</span> · ${p.vel.toFixed(2)}/day</div>
        </div>
        <div class="part-prices">
          <div class="bold text-ai">Order ${p.reorder}</div>
          <small>Stock: ${p.stock}</small>
        </div>
      </div>
      ${p.supplierPhone?`<button class="wa-btn btn-sm mt-1" onclick="orderFromSupplier('${p.id}')">📲 Order Now</button>`:''}
    </div>`).join('');
}

function renderBrainCustomers() {
  const due = Smart.reorderDueCustomers();
  // Top buyers
  const totals = {};
  state.sales.forEach(s => { if (s.customerId) totals[s.customerId] = (totals[s.customerId]||0) + s.total; });
  const top = Object.entries(totals).sort((a,b) => b[1]-a[1]).slice(0,5).map(([cid, amt]) => ({c: getCustomerById(cid), amt})).filter(x => x.c);
  // Inactive customers
  const lastBy = {};
  state.sales.forEach(s => { if (s.customerId) { if (!lastBy[s.customerId] || s.date > lastBy[s.customerId]) lastBy[s.customerId] = s.date; } });
  const inactive = state.customers.filter(c => lastBy[c.id] && daysAgo(lastBy[c.id]) > 60).map(c => ({c, days: daysAgo(lastBy[c.id])})).sort((a,b) => b.days - a.days).slice(0, 5);

  document.getElementById('brain-cust-list').innerHTML = `
    <h3 style="margin-top:8px;font-size:14px;">🎯 Due for Reorder</h3>
    ${due.length === 0 ? '<small>None</small>' : due.slice(0,5).map(d => `<div class="cart-item"><div><b>${d.customer.name}</b><br><small>${d.part.name} · ${d.daysSinceLastBuy}d ago</small></div><button class="wa-btn btn-sm" style="width:auto;padding:8px 14px;" onclick="sendReorderReminder('${d.customer.id}','${d.part.id}')">Remind</button></div>`).join('')}
    <div class="divider"></div>
    <h3 style="font-size:14px;">⭐ Top Buyers (all-time)</h3>
    ${top.map(t => `<div class="cart-item"><div><b>${t.c.name}</b> ${t.c.trusted?'★':''}<br><small>${t.c.type}</small></div><b class="text-success">${rs(t.amt)}</b></div>`).join('')}
    <div class="divider"></div>
    <h3 style="font-size:14px;">😴 Inactive (60+ days)</h3>
    ${inactive.length === 0 ? '<small>None</small>' : inactive.map(i => `<div class="cart-item"><div><b>${i.c.name}</b><br><small>${i.days}d since last order</small></div><a class="wa-btn btn-sm" style="width:auto;padding:8px 14px;text-decoration:none;" target="_blank" href="https://wa.me/${i.c.phone}?text=${encodeURIComponent('Namaste '+i.c.name+', missing you at '+state.settings.shopName+'! Any parts you need this season?')}">Reach out</a></div>`).join('')}
  `;
}

function renderBrainDead() {
  // Cache for 60s — deadStock scans all sales
  if (!window._brainDeadCache || (Date.now() - window._brainDeadCache.ts) > 60000) {
    window._brainDeadCache = { ts: Date.now(), dead: Smart.deadStock(90) };
  }
  const dead = window._brainDeadCache.dead;
  const div = document.getElementById('brain-dead-list');
  if (dead.length === 0) { div.innerHTML = '<div class="empty"><div class="icon">✓</div>No dead stock — everything is moving</div>'; return; }
  const tied = dead.reduce((a,p) => a + p.costPrice * p.stock, 0);
  div.innerHTML = `<div class="card priority" style="margin-bottom:10px;"><b>${dead.length} parts</b> — ${rs(tied)} of capital locked up</div>` +
    dead.sort((a,b) => (b.costPrice*b.stock) - (a.costPrice*a.stock)).slice(0, 20).map(p => `
    <div class="part-item">
      <div class="part-row">
        <div style="flex:1;">
          <div class="part-name">${p.name}</div>
          <div class="part-no">${p.partNo} • ${p.brand}</div>
          <div class="velocity"><span class="cold">No sales in 90+ days</span></div>
        </div>
        <div class="part-prices">
          <div class="bold">${p.stock} pcs</div>
          <small>${rs(p.costPrice * p.stock)} locked</small>
        </div>
      </div>
    </div>`).join('');
}

function renderBrainVelocity() {
  if (!window._brainVelCache || (Date.now() - window._brainVelCache.ts) > 30000) {
    window._brainVelCache = { ts: Date.now(), top: Smart.topSellers(30, 15) };
  }
  const top = window._brainVelCache.top;
  const div = document.getElementById('brain-velocity-list');
  if (top.length === 0) { div.innerHTML = '<div class="empty">No sales in last 30 days</div>'; return; }
  div.innerHTML = top.map((t, i) => `
    <div class="part-item">
      <div class="part-row">
        <div style="flex:1;">
          <div class="part-name">${i+1}. ${t.part.name}</div>
          <div class="part-no">${t.part.partNo}</div>
        </div>
        <div class="part-prices">
          <div class="bold text-success">${t.qty} sold</div>
          <small>${rs(t.revenue)}</small>
        </div>
      </div>
    </div>`).join('');
}

function renderBrainProfit() {
  const m = Smart.profitMonth();
  document.getElementById('brain-profit-summary').innerHTML = `
    <div class="row3">
      <div><div class="label">Revenue</div><div class="bold" style="font-size:18px;">${rs(m.revenue)}</div></div>
      <div><div class="label">Cost</div><div class="bold" style="font-size:18px;">${rs(m.cost)}</div></div>
      <div><div class="label">Profit</div><div class="bold" style="font-size:18px;color:var(--success)">${rs(m.profit)}</div></div>
    </div>
    <div class="mt-1"><b>Margin:</b> ${m.margin.toFixed(1)}%</div>`;
  const high = Smart.highMarginParts(10);
  document.getElementById('brain-profit-list').innerHTML = high.map(p => `
    <div class="part-item">
      <div class="part-row">
        <div style="flex:1;">
          <div class="part-name">${p.name}</div>
          <div class="part-no">${p.partNo} · ${p.velocity.toFixed(2)}/day</div>
        </div>
        <div class="part-prices">
          <div class="bold text-success">${rs(p.margin)}</div>
          <small>${p.marginPct.toFixed(0)}% margin</small>
        </div>
      </div>
    </div>`).join('');
}

// ============================================================
// AI CHAT (Claude API + rule-based fallback)
// ============================================================
async function sendChat() {
  const input = document.getElementById('chat-input');
  const q = input.value.trim();
  if (!q) return;
  input.value = '';
  const thread = document.getElementById('chat-thread');
  document.getElementById('chat-welcome').style.display = 'none';

  state.chat.push({role:'user', content:q});
  thread.innerHTML += `<div class="chat-msg user">${escapeHtml(q)}</div>`;
  thread.innerHTML += `<div class="chat-thinking" id="chat-thinking"><span></span><span></span><span></span></div>`;
  thread.scrollTop = thread.scrollHeight;

  let reply;
  try {
    if (getAIKey() && getAIKey().startsWith('sk-')) {
      reply = await aiClaude(q);
    } else {
      reply = aiRuleBased(q);
    }
  } catch (e) {
    reply = `Error: ${e.message}\n\n(Falling back to rule-based answer)\n\n${aiRuleBased(q)}`;
  }
  document.getElementById('chat-thinking').remove();
  state.chat.push({role:'assistant', content:reply});
  thread.innerHTML += `<div class="chat-msg ai">${escapeHtml(reply).replace(/\n/g,'<br>')}</div>`;
  thread.scrollTop = thread.scrollHeight;
}

function escapeHtml(s) {
  return s.replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

// Build a compact shop snapshot to send to Claude
function shopContext() {
  const m = Smart.profitMonth();
  const top = Smart.topSellers(30, 5);
  const low = state.parts.filter(p => p.stock <= p.minStock);
  const dead = Smart.deadStock(90).slice(0, 10);
  const due = Smart.reorderDueCustomers().slice(0, 5);
  return `SHOP: ${state.settings.shopName} (spare parts for tractors, harvesters, JCBs, rotavators)
Today: ${todayStr()}
Total SKUs: ${state.parts.length} | Customers: ${state.customers.length}
This month — Revenue: ${rs(m.revenue)}, Profit: ${rs(m.profit)}, Margin: ${m.margin.toFixed(1)}%
Credit due (all customers): ${rs(state.customers.reduce((a,c) => a + (c.totalDue||0), 0))}

LOW STOCK (${low.length}): ${low.slice(0,8).map(p => `${p.name}(${p.partNo})=${p.stock}`).join(', ')}
TOP SELLERS (30d): ${top.map(t => `${t.part.name}=${t.qty}sold`).join(', ')}
DEAD STOCK (90d, ${Smart.deadStock(90).length} total): ${dead.map(p => `${p.name}(${p.stock} pcs, ${rs(p.costPrice*p.stock)})`).join(', ')}
CUSTOMERS DUE REORDER: ${due.map(d => `${d.customer.name} (${d.part.name}, ${d.daysSinceLastBuy}d ago)`).join('; ')}

You are this shop's AI assistant. Answer concisely in plain language (2-5 sentences). When asked to write WhatsApp messages, use the shop name and a tone that fits Indian tractor spare parts customers (mix of Hindi/English/Punjabi is fine — natural and respectful). When suggesting actions, be specific (use part numbers, customer names, ₹ amounts).`;
}

async function aiClaude(question) {
  const key = getAIKey();
  const model = getAIModel();
  const messages = [{role:'user', content: question}];
  // Include last 6 chat turns for context
  const history = state.chat.slice(-7, -1);
  const finalMessages = [...history.map(m => ({role: m.role, content: m.content})), {role:'user', content: question}];

  const resp = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-api-key': key,
      'anthropic-version': '2023-06-01',
      'anthropic-dangerous-direct-browser-access': 'true'
    },
    body: JSON.stringify({
      model, max_tokens: 1024,
      system: shopContext(),
      messages: finalMessages
    })
  });
  if (!resp.ok) {
    const t = await resp.text();
    throw new Error(`API ${resp.status}: ${t.slice(0,200)}`);
  }
  const data = await resp.json();
  return data.content[0].text;
}

// Rule-based answers for offline / no-key use
function aiRuleBased(q) {
  const ql = q.toLowerCase();
  // Reorder
  if (/order|reorder|buy|stock up|restock/.test(ql)) {
    const list = state.parts.map(p => ({p, days: Smart.daysUntilStockout(p), reorder: Smart.smartReorderQty(p)}))
      .filter(x => x.p.stock <= x.p.minStock || x.days <= 14).slice(0, 5);
    if (list.length === 0) return "All your stock looks healthy — nothing critical to order today.";
    return "Top items to order today:\n" + list.map(x => `• ${x.p.name} (${x.p.partNo}) — order ~${x.reorder} pcs (current: ${x.p.stock}${x.days<Infinity?', '+x.days+'d left':''})\nSupplier: ${x.p.supplierName}`).join('\n\n') + "\n\nGo to WhatsApp tab → Order Stock to send the orders.";
  }
  // Top selling
  if (/top|best|hot|fast moving|popular/.test(ql)) {
    const top = Smart.topSellers(30, 5);
    if (top.length === 0) return "No sales recorded in the last 30 days yet.";
    return "Top sellers (last 30 days):\n" + top.map((t,i) => `${i+1}. ${t.part.name} — ${t.qty} sold (${rs(t.revenue)})`).join('\n');
  }
  // Profit
  if (/profit|margin|revenue|income/.test(ql)) {
    const m = Smart.profitMonth();
    return `This month: Revenue ${rs(m.revenue)}, Cost ${rs(m.cost)}, Profit ${rs(m.profit)} (${m.margin.toFixed(1)}% margin).\n\nTop margin parts: ${Smart.highMarginParts(3).map(p => p.name + ' (' + p.marginPct.toFixed(0) + '%)').join(', ')}.`;
  }
  // Dead/slow
  if (/dead|slow|stuck|not sell|capital/.test(ql)) {
    const d = Smart.deadStock(90);
    if (d.length === 0) return "Good news — no dead stock. Everything is moving in the last 90 days.";
    const tied = d.reduce((a,p) => a + p.costPrice*p.stock, 0);
    return `${d.length} parts have not sold in 90 days, locking up ${rs(tied)} of capital.\n\nTop 5 by value:\n${d.sort((a,b)=>(b.costPrice*b.stock)-(a.costPrice*a.stock)).slice(0,5).map(p => `• ${p.name} (${p.stock} pcs, ${rs(p.costPrice*p.stock)} locked)`).join('\n')}\n\nConsider clearance offers or returning to suppliers.`;
  }
  // Customers
  if (/customer|haven't bought|inactive|reorder due/.test(ql)) {
    const due = Smart.reorderDueCustomers();
    if (due.length === 0) return "No customers obviously due for reorder right now.";
    return "Customers due for reorder:\n" + due.slice(0,5).map(d => `• ${d.customer.name} — last bought ${d.part.name} ${d.daysSinceLastBuy}d ago`).join('\n') + "\n\nGo to WhatsApp tab → Customer Outreach to send reminders.";
  }
  // Diwali/offer/festive message
  if (/diwali|offer|festive|message|broadcast|promotion/.test(ql)) {
    return `Suggested message:\n\n*${state.settings.shopName}* 🪔\n\nDiwali Special Offer!\n\n• Extra 5% off on orders above ₹10,000\n• Free delivery within 50km\n• Easy credit for trusted customers\n\nValid till month end. Reply to order!\n\n${state.settings.shopName}\n📞 ${state.settings.shopPhone||'[shop phone]'}\n\n(Tip: For better cultural tone in Hindi/Punjabi, set up your free Claude API key in AI settings.)`;
  }
  return `I can answer about: reorder needs, top sellers, profit, dead stock, customer reminders, or write WhatsApp messages.\n\nFor full AI capabilities, add your free Claude API key in ⚙ AI settings (get one at console.anthropic.com).`;
}

// AI BROADCAST WRITER
async function aiWriteBroadcast() {
  const prompt = document.getElementById('ai-broadcast-prompt').value.trim();
  if (!prompt) { toast('What is the message about?'); return; }
  const ta = document.getElementById('b-message');
  ta.value = 'Generating…';
  let msg;
  try {
    if (getAIKey() && getAIKey().startsWith('sk-')) {
      msg = await aiClaude(`Write a WhatsApp broadcast message for my customers about: "${prompt}". Keep it under 80 words. Include shop name. Use simple language with English+Hindi mix that feels natural for Punjab tractor parts customers. Add appropriate emojis. End with shop phone.`);
    } else {
      msg = aiRuleBased('write broadcast ' + prompt);
    }
  } catch (e) {
    msg = `Error: ${e.message}`;
  }
  ta.value = msg;
}

// ============================================================
// SETTINGS / HEADER MENU
// ============================================================
function openHeaderMenu() {
  showModal('☰ Menu', `
    <div class="label mb-1">SHOP</div>
    <button class="btn btn-outline" onclick="closeModal();goTab('home')">🏠 Home Dashboard</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();goTab('inventory')">📦 Inventory / Stock</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();goTab('sale')">🧾 Counter Sale</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();goTab('wholesale')">🏭 Wholesale Orders</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();goTab('whatsapp')">💬 WhatsApp Tools</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();goTab('import')">⚡ Bulk Import (SKU)</button>
    <div class="label mt-2 mb-1">STAFF & FINANCE</div>
    <button class="btn btn-outline" onclick="closeModal();goTab('staff')">👥 Staff & Commission</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();goTab('reports');showReport('salary')">💰 Salary Calculator</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();goTab('reports');showReport('scores')">⭐ Staff Scores</button>
    <div class="label mt-2 mb-1">REPORTS & ANALYTICS</div>
    <button class="btn btn-outline" onclick="closeModal();goTab('reports');showReport('growth')">📈 Growth Chart</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();goTab('reports');showReport('customers')">👥 Customer Base</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();goTab('reports');showReport('staff')">🔧 Mechanic Commission</button>
    <div class="label mt-2 mb-1">SETTINGS</div>
    <button class="btn btn-outline" onclick="closeModal();openSettings()">⚙ Shop Settings</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();openAISettings()">🧠 AI Settings</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();openJarvisSettings()">🤝 Jarvis Integration</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();rxOpenDataPanel()">🛡️ Data Health</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();photoDoctorRun()">🩺 Photo Doctor</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();rxRunDailyReport()">📊 Daily Report</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();openCommSettings()">💰 Commission Settings</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();openCustomersList()">👥 Customers List</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();openSalesHistory()">📜 Sales History</button>
    <div class="label mt-2 mb-1">DATA</div>
    <button class="btn btn-outline" onclick="closeModal();goTab('reports');showReport('backup')">💾 Backup & Restore</button>
    <button class="btn btn-outline mt-1" onclick="closeModal();openRackFinder()">📍 Rack Finder</button>
    <button class="btn btn-danger mt-2" onclick="resetData()">⟳ Reset All Data</button>
  `);
}

function openSettings() {
  const s = state.settings;
  showModal('Shop Settings', `
    <div class="field"><label>Shop Name</label><input class="input" id="set-name" value="${s.shopName}"></div>
    <div class="field"><label>Shop Phone</label><input class="input" id="set-phone" value="${s.shopPhone}" placeholder="919876543210"></div>
    <div class="field"><label>Address</label><textarea class="input" id="set-addr" rows="2">${s.address}</textarea></div>
    <div class="field"><label>GST Number</label><input class="input" id="set-gst" value="${s.gstNo}"></div>
    <div class="field" style="margin-top:12px;"><label>Locations / Branches</label><button class="btn btn-outline" onclick="openManageLocations()">🏪 Manage Locations & Branches</button> <button class="btn btn-outline" onclick="openStockTransferModal()" style="margin-left:8px;">🔄 Stock Transfer</button></div>
    <div class="field"><label>Credit Terms</label><input class="input" id="set-terms" value="${s.creditTerms}"></div>
    <div class="field"><label>Next Invoice Number</label><input type="number" class="input" id="set-inv" value="${s.nextInvoice}"></div>
    <div style="border-top:1px solid var(--border,#e5e7eb);margin:12px 0 4px;padding-top:10px;font-weight:700;font-size:13px;color:var(--muted);">STAFF ADVANCE LIMITS</div>
    <div class="field"><label>Max single advance — % of salary</label><input type="number" class="input" id="set-adv-pct" value="${s.advancePctOfSalary??50}" placeholder="50"></div>
    <div class="field"><label>Outstanding advance cap ₹ (per staff)</label><input type="number" class="input" id="set-adv-cap" value="${s.advanceCap??20000}" placeholder="20000"></div>
    <div style="font-size:11px;color:var(--muted);margin:-4px 0 10px;">e.g. 50% means a ₹10,000-salary staffer can take at most ₹5,000 in one advance. Both limits can be crossed only with an extreme-emergency override (logged).</div>
    <div style="border-top:1px solid var(--border,#e5e7eb);margin:8px 0 4px;padding-top:10px;font-weight:700;font-size:13px;color:var(--muted);">ATTENDANCE / SALARY SYNC</div>
    <div class="field"><button class="btn btn-outline" onclick="openAttendanceSyncSetup()" style="width:100%;">📡 Connect Attendance Software (live salary sync)</button></div>
    <button class="btn" onclick="saveSettings()">Save</button>
  `);
}
function saveSettings() {
  state.settings.shopName = document.getElementById('set-name').value.trim();
  state.settings.shopPhone = document.getElementById('set-phone').value.trim();
  state.settings.address = document.getElementById('set-addr').value.trim();
  state.settings.gstNo = document.getElementById('set-gst').value.trim();
  state.settings.creditTerms = document.getElementById('set-terms').value.trim();
  state.settings.nextInvoice = +document.getElementById('set-inv').value || 1001;
  const _pct = +document.getElementById('set-adv-pct').value;
  state.settings.advancePctOfSalary = (_pct > 0 && _pct <= 100) ? _pct : 50;
  const _cap = +document.getElementById('set-adv-cap').value;
  state.settings.advanceCap = _cap > 0 ? _cap : 20000;
  markChanged('settings');
  save(); closeModal(); renderHome();
  toast('Settings saved');
}

// ─────────────────────────────────────────────────────────────────
// CENTRAL AI KEYS — AES-GCM encrypted, stored in Firestore
// Only the admin PIN can encrypt/decrypt. Staff pull ciphertext
// but cannot read the actual keys. Decrypted keys live in memory only.
// ─────────────────────────────────────────────────────────────────
window._centralKeys = null;    // decrypted: { aiKey, aiVisionKey, aiModel } — in memory only
window._centralKeyBlob = null; // encrypted blob pulled from Firestore

// Derive AES-GCM key from PIN + salt via PBKDF2
async function _deriveCryptoKey(pin, saltHex) {
  const enc = new TextEncoder();
  const saltBytes = new Uint8Array(saltHex.match(/.{2}/g).map(h => parseInt(h, 16)));
  const baseKey = await crypto.subtle.importKey('raw', enc.encode(pin), 'PBKDF2', false, ['deriveKey']);
  return crypto.subtle.deriveKey(
    { name: 'PBKDF2', salt: saltBytes, iterations: 200000, hash: 'SHA-256' },
    baseKey,
    { name: 'AES-GCM', length: 256 },
    false,
    ['encrypt', 'decrypt']
  );
}

async function _encryptKeys(pin, payload) {
  const salt = makeSalt();
  const iv   = crypto.getRandomValues(new Uint8Array(12));
  const key  = await _deriveCryptoKey(pin, salt);
  const ct   = await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, new TextEncoder().encode(JSON.stringify(payload)));
  return {
    salt,
    iv:    Array.from(iv).map(b => b.toString(16).padStart(2,'0')).join(''),
    ct:    btoa(String.fromCharCode(...new Uint8Array(ct))),
    setAt: Date.now()
  };
}

async function _decryptKeys(pin, blob) {
  const iv  = new Uint8Array(blob.iv.match(/.{2}/g).map(h => parseInt(h, 16)));
  const ct  = Uint8Array.from(atob(blob.ct), c => c.charCodeAt(0));
  const key = await _deriveCryptoKey(pin, blob.salt);
  const plain = await crypto.subtle.decrypt({ name: 'AES-GCM', iv }, key, ct);
  return JSON.parse(new TextDecoder().decode(plain));
}

async function pushCentralKeys(pin, aiKey, aiVisionKey, aiModel) {
  if (!window._firebaseAuthed) { return { ok: false, serverVerified: false, reason: 'Not connected to cloud' }; }
  try {
    const blob = await _encryptKeys(pin, { aiKey, aiVisionKey: aiVisionKey||'', aiModel: aiModel||'claude-haiku-4-5-20251001' });
    await window._firestoreSetCentralKeys(blob);
    window._centralKeyBlob = blob;
    window._centralKeys = { aiKey, aiVisionKey: aiVisionKey||'', aiModel: aiModel||'claude-haiku-4-5-20251001' };
    // setDoc resolves on the LOCAL CACHE write when offline persistence is on —
    // it does NOT prove the server got it. Read back from the server and compare
    // the ciphertext so we never report a false "saved" that silently reverts later.
    let serverVerified = false, reason = '';
    try {
      const remote = await window._firestoreGetCentralKeysFromServer();
      serverVerified = !!(remote && remote.ct === blob.ct);
      if (!serverVerified) reason = 'Server still shows a different/older key';
    } catch (e) {
      reason = 'Could not confirm with server (offline or blocked): ' + (e.message || e.code || 'unknown');
    }
    return { ok: true, serverVerified, reason };
  } catch(e) { return { ok: false, serverVerified: false, reason: e.message }; }
}

// Called on boot — pulls blob but does NOT decrypt (no PIN available yet)
async function pullCentralKeyBlob() {
  if (!window._firebaseAuthed) return null;
  try {
    const blob = await window._firestoreGetCentralKeys();
    if (blob && blob.ct) { window._centralKeyBlob = blob; return blob; }
  } catch(e) { console.warn('[centralKey] pullCentralKeyBlob failed:', e.message); }
  return null;
}

// Called after admin PIN login — decrypts with their PIN
async function unlockCentralKeys(pin) {
  if (!window._centralKeyBlob) await pullCentralKeyBlob();
  const blob = window._centralKeyBlob;
  if (!blob || !blob.ct) return false;
  try {
    const dec = await _decryptKeys(pin, blob);
    if (dec && dec.aiKey && dec.aiKey.startsWith('sk-')) {
      window._centralKeys = dec;
      _cacheCentralSession(dec); // survive a page refresh (cleared on tab/app close)
      return true;
    }
    return false;
  } catch(e) { return false; } // wrong PIN = AES-GCM auth failure
}

// ── Central AI unlock persistence ─────────────────────────────────────────────
// THE BUG this fixes: the decrypted central keys live only in window._centralKeys
// (in-memory) and are set ONLY by unlockCentralKeys(), which runs ONLY when the
// admin types their PIN on the login screen. On a page refresh / app relaunch the
// saved session skips the login screen, so the PIN is never re-entered, the keys
// are never decrypted, and window._centralKeys stays null. getAIKey() then silently
// falls back to the (usually empty or stale) local key, so the Anthropic API answers
// 401 "invalid x-api-key" / "x-api-key header is required" — exactly the error seen
// after a refresh, even though it worked the one time right after login.
//
// Fix: after a successful PIN unlock, cache the decrypted keys in sessionStorage,
// tagged to the admin who unlocked them. sessionStorage survives a REFRESH (so AI
// keeps working) but is wiped when the tab/app fully closes (so a cold relaunch
// re-prompts for the PIN). The raw key never persists to disk, preserving the
// "staff pull ciphertext but can never read the key" model.
var _CK_SESSION_KEY = '_ckSession_v1';
function _cacheCentralSession(dec) {
  try {
    var uid = (window.state && state.session && state.session.userId) || '';
    sessionStorage.setItem(_CK_SESSION_KEY, JSON.stringify({ uid: uid, keys: dec }));
  } catch(_) {}
}
function _clearCentralSession() {
  try { sessionStorage.removeItem(_CK_SESSION_KEY); } catch(_) {}
}
// Restore decrypted keys after a refresh — ONLY if the same admin session is still
// active. Returns true if keys were restored.
function _restoreCentralFromSession() {
  try {
    if (window._centralKeys && window._centralKeys.aiKey) return true; // already unlocked
    var raw = sessionStorage.getItem(_CK_SESSION_KEY);
    if (!raw) return false;
    var obj = JSON.parse(raw);
    var curUid = (window.state && state.session && state.session.userId) || '';
    var isAdmin = window.state && state.session && state.session.role === 'admin';
    if (!isAdmin || !obj || obj.uid !== curUid) { _clearCentralSession(); return false; }
    if (obj.keys && obj.keys.aiKey && String(obj.keys.aiKey).startsWith('sk-')) {
      window._centralKeys = obj.keys;
      return true;
    }
  } catch(_) {}
  return false;
}

// Lightweight PIN prompt to re-unlock central AI after a cold relaunch.
var _ckPromptOpen = false;
function promptUnlockCentralAI(reason) {
  if (_ckPromptOpen) return;
  if (!(window.state && state.session && state.session.role === 'admin')) return; // only owner can unlock
  if (!(window._centralKeyBlob && window._centralKeyBlob.ct)) return; // nothing to unlock
  if (window._centralKeys && window._centralKeys.aiKey) return;       // already unlocked
  _ckPromptOpen = true;
  var ov = document.createElement('div');
  ov.id = '_ckUnlockOverlay';
  ov.style.cssText = 'position:fixed;inset:0;z-index:100000;background:rgba(2,6,23,.72);backdrop-filter:blur(3px);display:flex;align-items:center;justify-content:center;padding:20px;';
  ov.innerHTML =
    '<div style="width:100%;max-width:340px;background:linear-gradient(160deg,#0f172a,#1e1b4b);border:1px solid rgba(165,243,252,.25);border-radius:16px;padding:20px;box-shadow:0 20px 60px rgba(0,0,0,.5);">' +
      '<div style="font-size:15px;font-weight:800;color:#a5f3fc;margin-bottom:4px;">\uD83D\uDD12 Unlock Central AI</div>' +
      '<div style="font-size:12px;color:rgba(255,255,255,.6);line-height:1.5;margin-bottom:14px;">' +
        (reason || 'Enter your admin PIN to re-activate AI photo scan & assistant on this device.') +
      '</div>' +
      '<input id="_ckUnlockPin" type="password" inputmode="numeric" maxlength="4" placeholder="4-digit PIN" ' +
        'style="width:100%;box-sizing:border-box;padding:11px 12px;border-radius:10px;border:1px solid rgba(255,255,255,.2);background:rgba(255,255,255,.08);color:white;font-size:16px;letter-spacing:4px;text-align:center;margin-bottom:8px;">' +
      '<div id="_ckUnlockErr" style="font-size:11px;color:#fca5a5;min-height:14px;margin-bottom:8px;"></div>' +
      '<div style="display:flex;gap:8px;">' +
        '<button id="_ckUnlockBtn" style="flex:1;padding:11px;border-radius:10px;border:none;background:#0f766e;color:white;font-size:14px;font-weight:700;cursor:pointer;">Unlock</button>' +
        '<button id="_ckUnlockLater" style="padding:11px 14px;border-radius:10px;border:1px solid rgba(255,255,255,.18);background:rgba(255,255,255,.06);color:rgba(255,255,255,.6);font-size:13px;cursor:pointer;">Later</button>' +
      '</div>' +
    '</div>';
  document.body.appendChild(ov);
  var pinEl = ov.querySelector('#_ckUnlockPin');
  var errEl = ov.querySelector('#_ckUnlockErr');
  var close = function() { _ckPromptOpen = false; try { document.body.removeChild(ov); } catch(_){} };
  var doUnlock = async function() {
    var pin = (pinEl.value || '').trim();
    if (!/^\d{4}$/.test(pin)) { errEl.textContent = 'Enter your 4-digit PIN.'; return; }
    errEl.textContent = 'Unlocking\u2026';
    var ok = await unlockCentralKeys(pin);
    if (ok) {
      if (typeof toast === 'function') toast('\uD83E\uDDE0 Central AI unlocked');
      close();
    } else {
      errEl.textContent = '\u274C Wrong PIN \u2014 try again.';
      pinEl.value = ''; pinEl.focus();
    }
  };
  ov.querySelector('#_ckUnlockBtn').onclick = doUnlock;
  ov.querySelector('#_ckUnlockLater').onclick = close;
  pinEl.addEventListener('keydown', function(e){ if (e.key === 'Enter') doUnlock(); });
  setTimeout(function(){ pinEl.focus(); }, 50);
}
window.promptUnlockCentralAI = promptUnlockCentralAI;

// Called on boot once the encrypted blob is available. Tries a silent
// sessionStorage restore first (seamless refresh); if that fails and central AI is
// genuinely locked for an admin, surfaces the unlock prompt once (cold relaunch).
function ensureCentralUnlockedOnBoot() {
  if (_restoreCentralFromSession()) return; // refresh case — done silently
  if (window.state && state.session && state.session.role === 'admin' &&
      window._centralKeyBlob && window._centralKeyBlob.ct &&
      !(window._centralKeys && window._centralKeys.aiKey)) {
    promptUnlockCentralAI();
  }
}
window.ensureCentralUnlockedOnBoot = ensureCentralUnlockedOnBoot;

function getAIKey() {
  // CENTRAL-ONLY: the encrypted, PIN-unlocked cloud key is the single source of
  // truth. No local per-device fallback — a locked key returns '' so callers show
  // a clear "unlock" state instead of silently 401ing on a stale local key.
  if (window._centralKeys && window._centralKeys.aiKey && window._centralKeys.aiKey.startsWith('sk-'))
    return window._centralKeys.aiKey;
  return '';
}
function getAIVisionKey() {
  if (window._centralKeys && window._centralKeys.aiVisionKey && window._centralKeys.aiVisionKey.startsWith('sk-'))
    return window._centralKeys.aiVisionKey;
  if (window._centralKeys && window._centralKeys.aiKey && window._centralKeys.aiKey.startsWith('sk-'))
    return window._centralKeys.aiKey;
  return '';
}
function getAIModel() {
  if (window._centralKeys && window._centralKeys.aiModel) return window._centralKeys.aiModel;
  return 'claude-haiku-4-5-20251001';
}
function hasCentralKey() {
  return !!(window._centralKeys && window._centralKeys.aiKey && window._centralKeys.aiKey.startsWith('sk-'));
}

// ── Unified Claude API client ────────────────────────────────────────────────
// Single source of truth for all AI calls. Replaces 5 duplicated nested
// callClaude wrappers. Every call now honors the model chosen in AI Settings
// via getAIModel() unless a caller explicitly overrides opts.model.
// opts: { messages, prompt, image:{dataUrl|base64,mimeType}, maxTokens, model, vision, stripFences }
async function aiCall(opts) {
  opts = opts || {};
  // Strip any stray whitespace/newlines. A trailing newline or space from
  // paste/autofill gets the x-api-key header rejected at the edge, which
  // surfaces as a non-JSON 401/403 ("no JSON body"). Anthropic keys never
  // contain whitespace, so this is always safe.
  const clean = k => (k || '').replace(/\s+/g, '');
  // CENTRAL-ONLY: one key, the unlocked cloud key. No local fallback to resolve.
  let key = clean(opts.vision ? getAIVisionKey() : getAIKey());
  if (!key.startsWith('sk-')) {
    // Key not available this session. Either it's locked (blob in cloud, not yet
    // decrypted — refresh/relaunch) or no central key has ever been set.
    if (window._centralKeyBlob && window._centralKeyBlob.ct) {
      try { if (window.ensureCentralUnlockedOnBoot) window.ensureCentralUnlockedOnBoot(); } catch(_){}
      throw new Error('\uD83D\uDD12 Central AI is locked \u2014 enter your admin PIN to unlock, then retry.');
    }
    throw new Error('No central AI key set \u2014 the owner must add one in AI Settings.');
  }
  let messages = opts.messages;
  if (!messages) {
    let content = opts.prompt || '';
    if (opts.image) {
      let b64 = opts.image.base64, mime = (opts.image.mimeType || 'image/jpeg');
      if (!b64 && opts.image.dataUrl) {
        const mm = opts.image.dataUrl.match(/^data:([^;]+);base64,/);
        if (mm) mime = mm[1];
        b64 = opts.image.dataUrl.split(',')[1];
      }
      content = [
        { type: 'image', source: { type: 'base64', media_type: mime, data: b64 } },
        { type: 'text', text: opts.prompt || '' }
      ];
    }
    messages = [{ role: 'user', content: content }];
  }
  const FALLBACK_MODEL = 'claude-haiku-4-5-20251001'; // current, vision-capable
  async function send(model, keyArg) {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-api-key': keyArg || key,
        'anthropic-version': '2023-06-01',
        'anthropic-dangerous-direct-browser-access': 'true'
      },
      body: JSON.stringify({ model: model, max_tokens: opts.maxTokens || 600, messages: messages })
    });
    // Read the body once as text, then try to parse. A non-JSON body (edge
    // error page, empty 401/403) used to collapse into a useless "no JSON body".
    let d = null, rawText = '';
    try { rawText = await r.text(); if (rawText) d = JSON.parse(rawText); } catch (_) {}

    if (!r.ok || (d && d.error)) {
      let msg = (d && d.error && (d.error.message || d.error.type)) || '';
      if (!msg) {
        const why = ({
          401: 'API key rejected — invalid, revoked, or wrong workspace',
          403: 'Blocked — key has no access, or billing/credits exhausted',
          404: 'Model not found — saved model name is wrong or deprecated',
          429: 'Rate limited — too many requests',
          529: 'Anthropic overloaded — retry in a moment'
        })[r.status] || ('HTTP ' + r.status);
        const snippet = rawText.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 120);
        msg = why + (snippet ? ' · ' + snippet : '');
      }
      const e = new Error(msg);
      e.status = r.status;
      e.apiType = d && d.error && d.error.type;
      throw e;
    }
    if (!d) { const e = new Error('Empty response from API (HTTP ' + r.status + ')'); e.status = r.status; throw e; }
    return d;
  }

  const chosen = opts.model || getAIModel();
  let d;
  try {
    d = await send(chosen);
  } catch (e) {
    console.error('[aiCall] failed on model "' + chosen + '": ' + e.message + (e.status ? ' (HTTP ' + e.status + ')' : ''));

    const looksLikeAuthError = e.status === 401 || e.status === 403;
    // CENTRAL-ONLY: a 401/403 here means the unlocked central key itself was
    // rejected (revoked, wrong workspace, or billing). There's no local key to
    // fall back to — tell the owner to re-save/verify the central key.
    if (looksLikeAuthError) {
      const le = new Error('\uD83D\uDD11 Central AI key rejected (HTTP ' + e.status + ') \u2014 it may be revoked or out of credits. Owner: re-save it in AI Settings.');
      le.status = e.status; throw le;
    }
    // Most common non-auth regression: the saved model was deprecated/renamed so
    // EVERY call 404s. Auto-recover on a known-good model.
    const looksLikeModelError = e.status === 404 || /model/i.test(e.message || '');
    if (looksLikeModelError && chosen !== FALLBACK_MODEL) {
      console.warn('[aiCall] retrying on ' + FALLBACK_MODEL);
      d = await send(FALLBACK_MODEL);
    } else {
      throw e;
    }
  }
  let text = (d.content && d.content[0] && d.content[0].text) || '';
  if (opts.stripFences) text = text.replace(/```json|```/g, '').trim();
  return text;
}
window.aiCall = aiCall;

function openAISettings() {
  const s = state.settings;
  const isOwner = state.session && state.session.role === 'admin';
  const central = window._centralKeys;
  const centralActive = hasCentralKey();
  const hasBlobInCloud = !!(window._centralKeyBlob && window._centralKeyBlob.ct);

  const centralStatusHtml = centralActive
    ? `<div style="background:linear-gradient(135deg,#052e16,#14532d);border:1px solid #16a34a;border-radius:10px;padding:12px;margin-bottom:12px;display:flex;align-items:center;gap:10px;">
        <span style="font-size:24px;">✅</span>
        <div>
          <div style="font-size:13px;font-weight:700;color:#6ee7b7;">Central AI Keys Active</div>
          <div style="font-size:11px;color:#4ade80;margin-top:2px;">All devices share one encrypted key — no local setup needed</div>
          ${window._centralKeyBlob && window._centralKeyBlob.setAt ? '<div style="font-size:10px;color:rgba(255,255,255,.35);margin-top:2px;">Updated: ' + new Date(window._centralKeyBlob.setAt).toLocaleDateString('en-IN') + '</div>' : ''}
        </div>
      </div>`
    : hasBlobInCloud
      ? `<div style="background:#fef3c7;border:1px solid #fcd34d;border-radius:10px;padding:10px;margin-bottom:12px;font-size:12px;color:#78350f;">
          🔒 Central key is set but locked. ${isOwner ? 'Enter your PIN below to unlock.' : 'Login as admin to activate AI.'}
        </div>`
      : `<div style="background:#fef3c7;border:1px solid #fcd34d;border-radius:10px;padding:10px;margin-bottom:12px;font-size:12px;color:#78350f;">
          ⚠️ No central key set. ${isOwner ? 'Set one below.' : 'Ask the owner to configure central AI keys.'}
        </div>`;

  const ownerSection = isOwner ? `
    <div style="background:linear-gradient(135deg,#0f172a,#1e1b4b);border-radius:12px;padding:14px;margin-bottom:12px;">
      <div style="font-size:13px;font-weight:700;color:#a5f3fc;margin-bottom:4px;">☁️ Central Keys — Admin only 🔐</div>
      <div style="font-size:11px;color:rgba(255,255,255,.5);margin-bottom:12px;line-height:1.5;">
        Encrypted with your PIN before saving. Staff devices pull the ciphertext but cannot read the actual keys.
      </div>
      <div class="field" style="margin-bottom:8px;">
        <label style="font-size:11px;color:rgba(255,255,255,.6);">Your Admin PIN (used to encrypt)</label>
        <input type="password" class="input" id="central-owner-pin" inputmode="numeric" maxlength="4" placeholder="4-digit PIN"
          style="background:rgba(255,255,255,.08);color:white;border-color:rgba(255,255,255,.2);">
      </div>
      <div class="field" style="margin-bottom:8px;">
        <label style="font-size:11px;color:rgba(255,255,255,.6);">Main AI Key</label>
        <input type="password" class="input" id="central-ai-key" value="${central&&central.aiKey||''}" placeholder="sk-ant-…"
          style="background:rgba(255,255,255,.08);color:white;border-color:rgba(255,255,255,.2);">
      </div>
      <div class="field" style="margin-bottom:8px;">
        <label style="font-size:11px;color:rgba(255,255,255,.6);">Vision Key — optional, separate key for photo scan</label>
        <input type="password" class="input" id="central-vision-key" value="${central&&central.aiVisionKey||''}" placeholder="blank = share main key"
          style="background:rgba(255,255,255,.08);color:white;border-color:rgba(255,255,255,.2);">
      </div>
      <div class="field" style="margin-bottom:12px;">
        <label style="font-size:11px;color:rgba(255,255,255,.6);">Model</label>
        <select class="select" id="central-ai-model" style="background:rgba(255,255,255,.08);color:white;border-color:rgba(255,255,255,.2);">
          <option value="claude-haiku-4-5-20251001" ${(central&&central.aiModel||s.aiModel)==='claude-haiku-4-5-20251001'?'selected':''}>Haiku 4.5 — fast, cheapest</option>
          <option value="claude-sonnet-4-6" ${(central&&central.aiModel||s.aiModel)==='claude-sonnet-4-6'?'selected':''}>Sonnet 4.6 — smart, balanced</option>
          <option value="claude-opus-4-6" ${(central&&central.aiModel||s.aiModel)==='claude-opus-4-6'?'selected':''}>Opus 4.6 — most powerful</option>
        </select>
      </div>
      <button class="btn" onclick="saveCentralKeys()" style="background:linear-gradient(135deg,#7c3aed,#4f46e5);width:100%;">🔐 Encrypt & Save to Cloud</button>
      <div id="central-save-msg" style="display:none;margin-top:8px;font-size:12px;border-radius:10px;padding:10px;line-height:1.5;"></div>
      <button class="btn" onclick="testCentralKey()" style="background:linear-gradient(135deg,#0891b2,#0e7490);width:100%;margin-top:6px;">🧪 Test central key</button>
      <div id="central-key-test-result" style="display:none;margin-top:8px;font-size:12px;border-radius:10px;padding:10px;line-height:1.5;"></div>
      ${hasBlobInCloud ? '<button onclick="clearCentralKeys()" style="width:100%;margin-top:6px;padding:8px;border-radius:8px;background:rgba(220,38,38,.2);color:#fca5a5;border:1px solid rgba(220,38,38,.3);font-size:12px;cursor:pointer;">🗑 Remove Central Keys</button>' : ''}
    </div>` : '';

  showModal('🧠 AI Settings', `
    ${centralStatusHtml}
    <div style="margin-bottom:12px;">
      <button class="btn" onclick="testAIKey()" style="background:linear-gradient(135deg,#0891b2,#0e7490);width:100%;">🧪 Test key now</button>
      <div id="ai-key-test-result" style="display:none;margin-top:8px;font-size:12px;border-radius:10px;padding:10px;line-height:1.5;"></div>
    </div>
    ${ownerSection}
    <div id="vt-stats-panel" style="margin-top:4px;"></div>
  `);

  setTimeout(() => {
    const panel = document.getElementById('vt-stats-panel');
    if (!panel) return;
    const stats = VisualTraining.stats();
    const legacyCount = ScanMemory.count();
    panel.innerHTML = `
      <div style="background:linear-gradient(135deg,#0f172a,#1e1b4b);border-radius:12px;padding:14px;">
        <div style="font-size:12px;font-weight:700;color:#a5f3fc;margin-bottom:10px;">🧠 Visual Training Database</div>
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:10px;">
          <div style="background:rgba(255,255,255,.08);border-radius:8px;padding:8px;text-align:center;">
            <div style="font-size:18px;font-weight:800;color:#6ee7b7;">${stats.uniqueParts}</div>
            <div style="font-size:10px;color:rgba(255,255,255,.5);text-transform:uppercase;margin-top:2px;">Parts Learned</div>
          </div>
          <div style="background:rgba(255,255,255,.08);border-radius:8px;padding:8px;text-align:center;">
            <div style="font-size:18px;font-weight:800;color:#a5f3fc;">${stats.totalConfirms}</div>
            <div style="font-size:10px;color:rgba(255,255,255,.5);text-transform:uppercase;margin-top:2px;">Confirmations</div>
          </div>
          <div style="background:rgba(255,255,255,.08);border-radius:8px;padding:8px;text-align:center;">
            <div style="font-size:18px;font-weight:800;color:#fcd34d;">${stats.records + legacyCount}</div>
            <div style="font-size:10px;color:rgba(255,255,255,.5);text-transform:uppercase;margin-top:2px;">Total Scans</div>
          </div>
        </div>
        <div style="display:flex;gap:6px;">
          <button onclick="vtExportTraining()" style="flex:1;padding:8px;border-radius:8px;background:rgba(255,255,255,.1);color:white;border:1px solid rgba(255,255,255,.2);font-size:12px;font-weight:600;cursor:pointer;">📤 Export</button>
          <button onclick="vtImportTraining()" style="flex:1;padding:8px;border-radius:8px;background:rgba(255,255,255,.1);color:white;border:1px solid rgba(255,255,255,.2);font-size:12px;font-weight:600;cursor:pointer;">📥 Import</button>
          ${stats.records > 0 ? '<button onclick="vtClearTraining()" style="padding:8px 10px;border-radius:8px;background:rgba(220,38,38,.3);color:#fca5a5;border:1px solid rgba(220,38,38,.4);font-size:12px;cursor:pointer;">🗑</button>' : ''}
        </div>
      </div>`;
  }, 50);
}

async function saveCentralKeys() {
  const msg = (bg, border, html) => {
    const el = document.getElementById('central-save-msg');
    if (!el) { toast(html.replace(/<[^>]+>/g, '')); return; }
    el.style.display = ''; el.style.background = bg; el.style.border = '1px solid ' + border; el.innerHTML = html;
  };
  const pin         = (document.getElementById('central-owner-pin')  ||{}).value||'';
  const aiKey       = ((document.getElementById('central-ai-key')     ||{}).value||'').replace(/\s+/g,'');
  const aiVisionKey = ((document.getElementById('central-vision-key') ||{}).value||'').replace(/\s+/g,'');
  const aiModel     = (document.getElementById('central-ai-model')   ||{}).value||'claude-haiku-4-5-20251001';
  if (pin.length < 4)           { msg('#7f1d1d', '#ef4444', '❌ Enter your 4-digit admin PIN. <b>Keys were NOT saved.</b>'); return; }
  if (!aiKey.startsWith('sk-')) { msg('#7f1d1d', '#ef4444', '❌ That doesn\'t look like a valid key (must start with <code>sk-</code>). <b>Keys were NOT saved.</b>'); return; }
  // Verify PIN matches this admin's stored PIN before encrypting
  const user = state.users && state.users.find(u => u.id === (state.session && state.session.userId));
  if (user && user.pinHash) {
    const check = await hashPin(pin, user.pinSalt || '');
    if (check !== user.pinHash) {
      msg('#7f1d1d', '#ef4444',
        '❌ <b>Wrong PIN — keys were NOT saved.</b><br>' +
        'The cloud is still serving your <i>previous</i> central key. This is the usual reason "I changed it but nothing changed."<br>' +
        'Re-enter the same admin PIN you log in with, then Save again.');
      return;
    }
  }
  const btn = document.querySelector('[onclick="saveCentralKeys()"]');
  if (btn) { btn.textContent = '⏳ Encrypting…'; btn.disabled = true; }
  const res = await pushCentralKeys(pin, aiKey, aiVisionKey, aiModel);
  if (btn) { btn.textContent = '🔐 Encrypt & Save to Cloud'; btn.disabled = false; }

  if (res.ok && res.serverVerified) {
    // central-only: clear any leftover local key so it can never shadow this one
    try { if (state.settings && (state.settings.aiKey || state.settings.aiVisionKey)) { delete state.settings.aiKey; delete state.settings.aiVisionKey; save(); } } catch(_){}
    msg('#052e16', '#16a34a',
      '✅ <b>Saved & confirmed on the server.</b> This key will survive refreshes and reach every device.<br>' +
      'Tap “Test central key” below, then reload other devices and unlock with PIN.');
    setTimeout(() => { closeModal(); openAISettings(); }, 1600);
  } else if (res.ok && !res.serverVerified) {
    // This is the "works once then reverts" trap: written to the local cache
    // (so it works right now) but NOT confirmed on the server, so a later
    // session pulls the old key back.
    msg('#78350f', '#f59e0b',
      '⚠️ <b>Saved on THIS device but NOT confirmed on the server.</b><br>' +
      'It will work now, then revert to the old key after a refresh — this is exactly the "works once then fails" you saw.<br>' +
      'Reason: ' + (res.reason || 'server write not acknowledged') + '.<br>' +
      '👉 Get on a stable connection and tap Save again until this turns green.');
  } else {
    msg('#7f1d1d', '#ef4444', '❌ <b>Not saved.</b> ' + (res.reason || 'Failed before reaching the cloud.'));
  }
}

async function clearCentralKeys() {
  if (!confirm('Remove central keys? All devices lose AI access until keys are re-set.')) return;
  try {
    await window._firestoreSetCentralKeys({ cleared: true, setAt: Date.now() });
    window._centralKeys = null; window._centralKeyBlob = null;
    try { _clearCentralSession(); } catch(_){}
    toast('Central keys removed'); closeModal(); openAISettings();
  } catch(e) { toast('Failed: ' + e.message); }
}

function saveAISettings() {
  // CENTRAL-ONLY: local keys were removed. Kept as a safe no-op in case any old
  // UI still references it — point the owner at the central key instead.
  if (typeof toast === 'function') toast('This build uses one central AI key. Set it under “Central Keys” in AI Settings.');
}

// Live key test — fires a 1-token call with the EXACT key getAIKey() resolves
// (the same key scans use) and prints the raw HTTP result on-screen. No console
// needed. Key is masked in the output so the result is safe to screenshot.
async function testAIKey() {
  const box = document.getElementById('ai-key-test-result');
  const show = (bg, border, html) => {
    if (!box) return;
    box.style.display = '';
    box.style.background = bg;
    box.style.border = '1px solid ' + border;
    box.innerHTML = html;
  };
  const key = ((typeof getAIKey === 'function' ? getAIKey() : '') || '').replace(/\s+/g, '');
  const model = (typeof getAIModel === 'function' ? getAIModel() : '') || 'claude-haiku-4-5-20251001';
  const source = (window._centralKeys && window._centralKeys.aiKey && window._centralKeys.aiKey.replace(/\s+/g,'').startsWith('sk-'))
    ? 'central (cloud)' : (window._centralKeyBlob && window._centralKeyBlob.ct ? 'central — locked (enter PIN)' : 'none set');
  const fp = key ? (key.slice(0, 12) + '…' + key.slice(-4) + ' · ' + key.length + ' chars') : '(none)';

  if (!key.startsWith('sk-')) {
    show('#7f1d1d', '#ef4444', '❌ No usable key in memory.<br>Source: <b>' + source + '</b>. ' +
      ((window._centralKeyBlob && window._centralKeyBlob.ct) ? 'Central AI is locked — enter your admin PIN to unlock.' : 'Owner: set a central key under “Central Keys” above.'));
    return;
  }
  show('rgba(8,145,178,.15)', '#0891b2', '⏳ Testing <b>' + source + '</b> key <code style="opacity:.8;">' + fp + '</code> on <b>' + model + '</b>…');

  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-api-key': key,
        'anthropic-version': '2023-06-01',
        'anthropic-dangerous-direct-browser-access': 'true'
      },
      body: JSON.stringify({ model: model, max_tokens: 1, messages: [{ role: 'user', content: 'hi' }] })
    });
    let body = '';
    try { body = await r.text(); } catch (_) {}
    let parsed = null;
    try { parsed = JSON.parse(body); } catch (_) {}

    if (r.ok) {
      show('#052e16', '#16a34a',
        '✅ <b>Key works.</b> HTTP ' + r.status + ' on <b>' + model + '</b>.<br>' +
        'Source: ' + source + ' · <code style="opacity:.8;">' + fp + '</code><br>' +
        'If scans still fail, the problem is later in the scan flow — tell me the on-screen scan error.');
      return;
    }

    const apiMsg = (parsed && parsed.error && (parsed.error.message || parsed.error.type)) || '';
    const why = ({
      401: 'Key rejected — invalid, revoked, or from a different/closed workspace.',
      403: 'Forbidden — key has no access, or billing/credits are exhausted.',
      404: 'Model not found — the saved model name is wrong or deprecated.',
      429: 'Rate limited — too many requests right now.',
      529: 'Anthropic overloaded — try again in a moment.'
    })[r.status] || ('HTTP ' + r.status + '.');
    show('#7f1d1d', '#ef4444',
      '❌ HTTP ' + r.status + ' — ' + why +
      (apiMsg ? '<br>API said: <i>' + apiMsg.slice(0, 120) + '</i>' : '') +
      '<br>Source: <b>' + source + '</b> · <code style="opacity:.8;">' + fp + '</code>' +
      (r.status === 401 ? '<br>👉 Compare that fingerprint to your key in console.anthropic.com. If it differs, the save didn\'t take (often a wrong admin PIN).' : ''));
  } catch (e) {
    show('#7f1d1d', '#ef4444',
      '❌ Request failed before reaching the API: <i>' + (e.message || 'network error') + '</i><br>' +
      'Usually no internet, or a content blocker. Source: ' + source + ' · <code style="opacity:.8;">' + fp + '</code>');
  }
}

// Tests the ACTIVE central key (window._centralKeys.aiKey) specifically,
// ignoring any local override — so an owner can confirm the cloud key itself
// is valid before trusting it across devices.
async function testCentralKey() {
  const box = document.getElementById('central-key-test-result');
  const show = (bg, border, html) => {
    if (!box) return;
    box.style.display = ''; box.style.background = bg; box.style.border = '1px solid ' + border; box.innerHTML = html;
  };
  const central = window._centralKeys;
  const key = ((central && central.aiKey) || '').replace(/\s+/g, '');
  const model = ((central && central.aiModel) || (typeof getAIModel === 'function' ? getAIModel() : '') || 'claude-haiku-4-5-20251001');
  const fp = key ? (key.slice(0, 12) + '…' + key.slice(-4) + ' · ' + key.length + ' chars') : '(none)';

  if (!key.startsWith('sk-')) {
    show('#7f1d1d', '#ef4444', '❌ No active central key in memory on this device. Save one above, or this device hasn\'t unlocked the cloud key yet (reload + enter PIN).');
    return;
  }
  show('rgba(8,145,178,.15)', '#0891b2', '⏳ Testing the live <b>central</b> key <code style="opacity:.8;">' + fp + '</code> on <b>' + model + '</b>…');

  // Compare what the SERVER stores against the blob this device decrypted. If
  // they differ, the cloud has a different key than memory — the classic
  // "works this session, reverts next refresh" trap.
  let serverNote = '';
  try {
    if (typeof window._firestoreGetCentralKeysFromServer === 'function') {
      const remote = await window._firestoreGetCentralKeysFromServer();
      const localCt = window._centralKeyBlob && window._centralKeyBlob.ct;
      if (remote && remote.ct && localCt) {
        if (remote.ct === localCt) {
          serverNote = '<br>🟢 Server matches this device’s key' + (remote.setAt ? ' (saved ' + new Date(remote.setAt).toLocaleString('en-IN') + ')' : '') + '.';
        } else {
          serverNote = '<br>🔴 <b>Server holds a DIFFERENT key</b>' + (remote.setAt ? ' (server copy saved ' + new Date(remote.setAt).toLocaleString('en-IN') + ')' : '') +
                       '. Your last save didn’t reach the cloud — it will revert on refresh. Re-save until the save box turns green.';
        }
      } else if (!remote) {
        serverNote = '<br>🔴 Server has no central key stored. Save one above.';
      }
    }
  } catch (e) {
    serverNote = '<br>⚠️ Couldn’t reach server to compare (offline?): ' + (e.message || e.code || 'unknown') + '.';
  }

  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-api-key': key, 'anthropic-version': '2023-06-01', 'anthropic-dangerous-direct-browser-access': 'true' },
      body: JSON.stringify({ model: model, max_tokens: 1, messages: [{ role: 'user', content: 'hi' }] })
    });
    let body = ''; try { body = await r.text(); } catch (_) {}
    let parsed = null; try { parsed = JSON.parse(body); } catch (_) {}

    if (r.ok) {
      show('#052e16', '#16a34a', '✅ <b>Central key works.</b> HTTP ' + r.status + ' on <b>' + model + '</b>.' + serverNote + '<br>Reload other devices and unlock with PIN — they’ll pull this same key. No local key needed.');
      return;
    }
    const apiMsg = (parsed && parsed.error && (parsed.error.message || parsed.error.type)) || '';
    const why = ({
      401: 'Central key rejected — invalid, revoked, or wrong workspace. The cloud is holding a bad key; re-save above with the correct key + PIN.',
      403: 'Forbidden — key has no access, or billing/credits exhausted.',
      404: 'Model not found — central model name is wrong or deprecated.',
      429: 'Rate limited — too many requests.',
      529: 'Anthropic overloaded — retry shortly.'
    })[r.status] || ('HTTP ' + r.status + '.');
    show('#7f1d1d', '#ef4444', '❌ HTTP ' + r.status + ' — ' + why + (apiMsg ? '<br>API said: <i>' + apiMsg.slice(0, 120) + '</i>' : '') + serverNote + '<br><code style="opacity:.8;">' + fp + '</code>');
  } catch (e) {
    show('#7f1d1d', '#ef4444', '❌ Request failed before reaching the API: <i>' + (e.message || 'network error') + '</i>' + serverNote + '<br><code style="opacity:.8;">' + fp + '</code>');
  }
}


function vtExportTraining() {
  try {
    const data = VisualTraining.export();
    const blob = new Blob([data], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'goury-visual-training-' + new Date().toISOString().slice(0,10) + '.json';
    a.click();
    URL.revokeObjectURL(url);
    toast('✅ Training data exported');
  } catch(_) { toast('Export failed'); }
}

function vtImportTraining() {
  const inp = document.createElement('input');
  inp.type = 'file';
  inp.accept = '.json';
  inp.onchange = function(e) {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = function(ev) {
      const added = VisualTraining.import(ev.target.result);
      if (added === false) { toast('❌ Invalid training file'); return; }
      toast('✅ Imported ' + added + ' new training records');
      // Refresh the stats panel
      openAISettings();
    };
    reader.readAsText(file);
  };
  inp.click();
}

function vtClearTraining() {
  const stats = VisualTraining.stats();
  if (!confirm('Clear all ' + stats.records + ' visual training records? This cannot be undone.')) return;
  try { localStorage.removeItem(VisualTraining.KEY); } catch(_) {}
  toast('🗑 Training data cleared');
  openAISettings();
}

function openJarvisSettings() {
  const s = state.settings;
  showModal('🤝 Jarvis Integration', `
    <p style="font-size:13px;color:var(--muted);line-height:1.5">
      Send the daily shop report and critical alerts straight to your personal AI assistant Jarvis.<br><br>
      Jarvis must expose an HTTPS endpoint that accepts POST JSON. The shop will send:
      <code style="font-size:11px;background:#f1f5f9;padding:2px 4px;border-radius:4px">{ source:"shop-ai", kind:"daily_report"|"critical_alert", report:{...} }</code>
    </p>
    <div class="field mt-2"><label>Jarvis Webhook URL</label>
      <input class="input" id="jv-url" value="${s.jarvisWebhook||''}" placeholder="https://jarvis.example.com/hook/shop">
    </div>
    <div class="field"><label>Auth Header (optional)</label>
      <input type="password" class="input" id="jv-auth" value="${s.jarvisAuth||''}" placeholder="Bearer xxxx">
    </div>
    <div class="field">
      <label><input type="checkbox" id="jv-critical" ${s.jarvisCritical!==false?'checked':''}> Also POST critical alerts immediately</label>
    </div>
    <div class="field">
      <label><input type="checkbox" id="jv-9pm" ${s.jarvisDaily!==false?'checked':''}> Auto-send daily report at 9pm</label>
    </div>
    <div class="btn-row">
      <button class="btn" onclick="saveJarvisSettings()">Save</button>
      <button class="btn btn-outline" onclick="testJarvis()">🧪 Test Now</button>
    </div>
    <small class="mt-2" style="display:block;color:var(--muted)">All settings stay in this browser + Firestore. The webhook URL never leaves the app except when posting reports.</small>
  `);
}
function saveJarvisSettings() {
  state.settings.jarvisWebhook = document.getElementById('jv-url').value.trim();
  state.settings.jarvisAuth    = document.getElementById('jv-auth').value.trim();
  state.settings.jarvisCritical = document.getElementById('jv-critical').checked;
  state.settings.jarvisDaily    = document.getElementById('jv-9pm').checked;
  save(); closeModal();
  toast(state.settings.jarvisWebhook ? '🤝 Jarvis connected' : 'Jarvis disconnected');
}
async function testJarvis() {
  const url = document.getElementById('jv-url').value.trim();
  const auth = document.getElementById('jv-auth').value.trim();
  if (!url) { toast('Enter URL first'); return; }
  try {
    const resp = await fetch(url, {
      method:'POST',
      headers: { 'content-type':'application/json', ...(auth ? {'authorization':auth} : {}) },
      body: JSON.stringify({ source:'shop-ai', kind:'test', message:'Hello from Shop AI', shop: state.settings.shopName, ts: Date.now() })
    });
    if (resp.ok) toast('✅ Jarvis responded ' + resp.status);
    else toast('⚠️ Jarvis returned ' + resp.status);
    if (typeof RX !== 'undefined') RX.log('info','jarvis','test ping status=' + resp.status);
  } catch(e) {
    toast('❌ ' + e.message);
    if (typeof RX !== 'undefined') RX.log('error','jarvis','test ping failed: ' + e.message);
  }
}

// pushToJarvis — single fan-out to (a) Firestore jarvis_inbox (Jarvis app
// listens there in real-time) and (b) optional webhook. Used by all paths.
async function pushToJarvis(payload) {
  // payload: { kind, severity, title, message, data? }
  // 1. Firestore inbox — preferred, real-time, persistent
  if (window.firebaseWriteInbox && window._firebaseAuthed) {
    try {
      await window.firebaseWriteInbox(payload);
      if (typeof RX !== 'undefined') RX.log('info','jarvis','inbox write ok: '+payload.kind+'/'+payload.severity);
    } catch(e) {
      if (typeof RX !== 'undefined') RX.log('error','jarvis','inbox write failed: '+e.message);
    }
  }
  // 2. Optional webhook (legacy / extra)
  if (state.settings.jarvisWebhook) {
    try {
      await fetch(state.settings.jarvisWebhook, {
        method:'POST',
        headers: { 'content-type':'application/json', ...(state.settings.jarvisAuth ? {'authorization':state.settings.jarvisAuth} : {}) },
        body: JSON.stringify({ source:'shop-ai', shop: state.settings.shopName, ts: Date.now(), ...payload })
      });
    } catch(e) {
      if (typeof RX !== 'undefined') RX.log('error','jarvis','webhook failed: '+e.message);
    }
  }
}

// Push critical alert to Jarvis (called from agent when severity===critical)
async function rxPushCriticalToJarvis(alert) {
  if (state.settings.jarvisCritical === false) return;
  await pushToJarvis({
    kind: 'critical_alert',
    severity: 'critical',
    title: alert.category || 'Critical alert',
    message: alert.message || '',
    data: { tab: alert.tab, action: alert.action }
  });
}

// Push security event to Jarvis (login fail, halt triggered, PIN reset, etc.)
async function rxPushSecurityEvent(title, message, data) {
  await pushToJarvis({
    kind: 'data_security',
    severity: 'warning',
    title: title || 'Security event',
    message: message || '',
    data: data || null
  });
}

// Push general system event (sale milestone, halt resolved, etc.)
async function rxPushSystemEvent(severity, title, message, data) {
  await pushToJarvis({
    kind: 'system',
    severity: severity || 'info',
    title: title || 'System event',
    message: message || '',
    data: data || null
  });
}

function openCustomersList() {
  const html = `<button class="btn" onclick="openAddCustomer()">+ Add Customer</button>
    <div class="mt-2">${state.customers.map(c => `
      <div class="part-item" onclick="openAddCustomer('${c.id}')">
        <div class="part-row">
          <div><div class="bold">${c.name} ${c.trusted?'★':''}</div>
            <small>${c.phone}</small>
            <div class="part-meta"><span class="tag ${c.type==='wholesale'?'':'warn'}">${c.type}</span>${c.totalDue>0?`<span class="tag danger">Due: ${rs(c.totalDue)}</span>`:''}</div>
          </div>
        </div>
      </div>`).join('')}</div>`;
  showModal('Customers', html);
}
function openAddCustomer(id) {
  const c = id ? getCustomerById(id) : {};
  showModal(id?'Edit Customer':'Add Customer', `
    <div class="field"><label>Name *</label><input class="input" id="c-name" value="${c.name||''}"></div>
    <div class="field"><label>Phone (with country code) *</label><input class="input" id="c-phone" value="${c.phone||''}" placeholder="919876543210"></div>
    <div class="field"><label>Type</label>
      <select class="select" id="c-type">
        <option value="retail" ${c.type==='retail'?'selected':''}>Retail</option>
        <option value="wholesale" ${c.type==='wholesale'?'selected':''}>Wholesale</option>
      </select></div>
    <div class="field"><label><input type="checkbox" id="c-trusted" ${c.trusted?'checked':''}> Trusted (eligible for credit pricing)</label></div>
    <div class="row2">
      <div class="field"><label>Credit Days</label><input type="number" class="input" id="c-days" value="${c.creditDays||0}" placeholder="e.g. 30"></div>
      <div class="field"><label>Credit Limit ₹</label><input type="number" class="input" id="c-limit" value="${c.creditLimit||0}" placeholder="0 = no limit"></div>
    </div>
    <div class="field"><label>Total Due ₹</label><input type="number" class="input" id="c-due" value="${c.totalDue||0}"></div>
    <div class="field"><label>Notes</label><textarea class="input" id="c-notes" rows="2">${c.notes||''}</textarea></div>
    <button class="btn" onclick="saveCustomer('${id||''}')">${id?'Update':'Add'}</button>
    ${id?`<button class="btn btn-danger mt-1" onclick="deleteCustomer('${id}')">Delete</button>`:''}
  `);
}
async function saveCustomer(id) {
  const c = { id: id || uid(),
    name: document.getElementById('c-name').value.trim(),
    phone: document.getElementById('c-phone').value.trim(),
    type: document.getElementById('c-type').value,
    trusted: document.getElementById('c-trusted').checked,
    creditDays: +document.getElementById('c-days').value || 0,
    creditLimit: +(document.getElementById('c-limit')||{value:0}).value || 0,
    totalDue: +document.getElementById('c-due').value || 0,
    notes: document.getElementById('c-notes').value.trim() };
  if (!c.name || !c.phone) { toast('Name and phone required'); return; }
  if (id) state.customers[state.customers.findIndex(x=>x.id===id)] = c;
  else state.customers.push(c);
  invalidateCustomerIndex();
  save(); closeModal();
}
function deleteCustomer(id) {
  const c = getCustomerById(id);
  if (!c) return;
  const msg = (c.totalDue > 0)
    ? '⚠️ ' + c.name + ' has ₹' + Math.round(c.totalDue).toLocaleString('en-IN') + ' outstanding! Delete anyway?'
    : 'Delete customer ' + c.name + '?';
  if (!confirm(msg)) return;
  state.customers = state.customers.filter(x => x.id !== id);
  invalidateCustomerIndex();
  window._ownerDeleteInProgress = true;
  save().finally(() => { window._ownerDeleteInProgress = false; });
  logActivity && logActivity('Customer deleted: ' + c.name + (c.totalDue > 0 ? ' (had ₹' + Math.round(c.totalDue) + ' due)' : ''), 'warn');
  closeModal();
}

function openSalesHistory() {
  const sorted = [...state.sales].sort((a,b) => b.date.localeCompare(a.date)).slice(0, 30);
  showModal('Sales History (last 30)', sorted.map(s => {
    const cust = getCustomerById(s.customerId);
    return `<div class="sale-row" onclick="closeModal();showInvoice(state.sales.find(x=>x.id==='${s.id}'))" style="cursor:pointer;">
      <div class="sale-head">
        <div><b>${s.invoiceNo}</b><br><small>${cust?cust.name:'Walk-in'} • ${s.paymentMode}</small></div>
        <div class="sale-amt">${rs(s.total)}</div>
      </div>
      <small>${new Date(s.date).toLocaleString('en-IN')}</small>
    </div>`;
  }).join(''));
}

function openSuppliersList() {
  const sup = {};
  state.parts.forEach(p => {
    if (p.supplierName) {
      sup[p.supplierName] = sup[p.supplierName] || {phone:p.supplierPhone, count:0};
      sup[p.supplierName].count++;
    }
  });
  showModal('Suppliers', Object.entries(sup).map(([name, info]) => `
    <div class="part-item">
      <div class="part-row">
        <div><b>${name}</b><br><small>${info.phone||'-'} • ${info.count} parts</small></div>
        ${info.phone?`<a class="wa-btn btn-sm" style="text-decoration:none;width:auto;padding:8px 14px;" target="_blank" href="https://wa.me/${info.phone}">WA</a>`:''}
      </div>
    </div>`).join(''));
}

// ── PARTS MIGRATION: blob → per-doc Firestore ─────────────────
// Called once from the Cloud Sync Upgrade card in Reports → Backup & Safety.
// Delegates to window.migratePartsToDocuments (defined in Firebase module scope,
// which already has the correct db reference and batch logic).
async function runPartsMigration() {
  if (localStorage.getItem('_partsDocMigrated') === '2') {
    initMigrateCard(); return;
  }
  if (!window._firebaseAuthed) {
    toast('⚠️ Not signed in to Firebase — try again in a moment'); return;
  }
  if (!window.migratePartsToDocuments) {
    toast('⚠️ Migration function not loaded — reload the app and try again'); return;
  }

  const btn      = document.getElementById('migrate-run-btn');
  const progress = document.getElementById('migrate-progress');
  const bar      = document.getElementById('migrate-bar');
  const label    = document.getElementById('migrate-label');
  const total    = state.parts.length;

  if (btn)      { btn.disabled = true; btn.textContent = '⏳ Upgrading…'; }
  if (progress) progress.style.display = '';
  if (label)    label.textContent = 'Uploading ' + total + ' parts to Firestore…';

  // Animate progress bar based on expected time (~50 parts/sec on typical connection)
  const estMs = Math.max(3000, Math.ceil(total / 50) * 1000);
  const startTs = Date.now();
  const ticker = setInterval(function() {
    const elapsed = Date.now() - startTs;
    const pct = Math.min(90, Math.round(elapsed / estMs * 100)); // cap at 90 until done
    if (bar) bar.style.width = pct + '%';
    if (label) label.textContent = 'Uploading parts… (~' + Math.max(0, Math.ceil((estMs - elapsed) / 1000)) + 's left)';
  }, 400);

  try {
    await window.migratePartsToDocuments();
    clearInterval(ticker);
    if (bar)   bar.style.width = '100%';
    if (label) label.textContent = '✅ Done — ' + total + ' parts upgraded';
    toast('✅ Fast sync active — saves are now much faster');
    logActivity && logActivity('Parts migrated to per-doc Firestore (' + total + ' parts)', 'info');
    setTimeout(initMigrateCard, 1500);
  } catch(e) {
    clearInterval(ticker);
    console.error('[migration]', e);
    if (btn)   { btn.disabled = false; btn.textContent = '⚡ Retry upgrade'; }
    if (label) label.textContent = '❌ Failed: ' + e.message;
    toast('❌ Migration failed — check your internet connection and try again');
  }
}

// Initialises the migration card to show correct state when the backup tab opens
function initMigrateCard() {
  var done    = localStorage.getItem('_partsDocMigrated') === '2';
  var doneEl  = document.getElementById('migrate-done-msg');
  var needEl  = document.getElementById('migrate-needed-msg');
  var countEl = document.getElementById('migrate-part-count');
  if (doneEl)  doneEl.style.display  = done ? '' : 'none';
  if (needEl)  needEl.style.display  = done ? 'none' : '';
  if (!done && countEl) countEl.textContent = (state.parts.length || 0);
}

// exportData defined fully below (with backup timestamp + all data keys)
function exportData() {
  // ⚠️ ALL state keys — if you add a new state collection, add it here too
  const d = {
    parts:state.parts, customers:state.customers, sales:state.sales, orders:state.orders,
    staff:state.staff, users:state.users, wholesaleOrders:state.wholesaleOrders,
    salaryRecords:state.salaryRecords, advanceRecords:state.advanceRecords,
    staffScores:state.staffScores, mechanics:state.mechanics, retailCustomers:state.retailCustomers,
    collections:state.collections, expenses:state.expenses,
    attendanceRecords:state.attendanceRecords, settings:state.settings,
    // Financial & purchasing — added 2025
    purchases:state.purchases||[], suppliers:state.suppliers||[],
    supplierAdvances:state.supplierAdvances||[], dayClosings:state.dayClosings||[],
    bankAccounts:state.bankAccounts||[], manualPayables:state.manualPayables||[],
    // Operational
    demandInbox:state.demandInbox||[], incomingStock:state.incomingStock||[],
    brandMatrix:state.brandMatrix||[], recycleBin:state.recycleBin||[],
    activityLog:(state.activityLog||[]).slice(0,200)
  };
  const blob = new Blob([JSON.stringify(d,null,2)],{type:'application/json'});
  const a = document.createElement('a'); a.href=URL.createObjectURL(blob);
  a.download='shop-backup-'+todayStr()+'.json'; a.click();
  setTimeout(function(){ URL.revokeObjectURL(a.href); }, 5000); // free blob after download starts
  state.settings.lastBackup = new Date().toISOString();
  save(); if(typeof updateLastBackupDisplay==='function') updateLastBackupDisplay();
  toast('💾 Full backup downloaded — ' + Object.keys(d).length + ' collections');
}
function importData(e) {
  const f = e.target.files[0]; if (!f) return;
  const r = new FileReader();
  r.onload = ev => {
    if (!confirm('This will REPLACE all current data with the backup file. Continue?')) return;
    try {
      const d = JSON.parse(ev.target.result);
      // Restore every collection present in backup; keep current if not in file
      const keys = ['parts','customers','sales','orders','staff','users','wholesaleOrders',
        'salaryRecords','advanceRecords','staffScores','mechanics','retailCustomers',
        'collections','expenses','attendanceRecords',
        'purchases','suppliers','supplierAdvances','dayClosings',
        'bankAccounts','manualPayables','demandInbox','incomingStock','brandMatrix','activityLog','recycleBin'];
      keys.forEach(k => { if (d[k] !== undefined) state[k] = d[k]; });
      if (d.settings) state.settings = {...state.settings, ...d.settings};
      try { recomputeTotalDue(); } catch(e2) {}
      save(); renderHome(); renderInventory();
      toast('✅ Data imported — ' + keys.filter(k=>d[k]).length + ' collections restored');
      logActivity && logActivity('Data import from backup file', 'warn');
    } catch(err) { toast('❌ Invalid backup file: ' + err.message); }
  };
  r.readAsText(f);
}
// ── BULK CATEGORY FIX ─────────────────────────────────────────
function openBulkCategoryFix() {
  // Build a list of all existing categories for the dropdown hints
  var cats = [...new Set(state.parts.map(function(p){ return (p.category||'').trim(); }).filter(Boolean))].sort();
  var catOptions = cats.map(function(c){ return '<option value="' + c + '">' + c + '</option>'; }).join('');

  showModal('🔧 Bulk Category Fix', `
    <div style="font-size:13px;color:var(--muted);margin-bottom:14px;">Find parts with a wrong category and fix them all at once.</div>
    <div class="field">
      <label>Wrong Category (what staff typed)</label>
      <input class="input" id="bcf-wrong" placeholder="e.g. reel" list="bcf-cat-list" autocomplete="off">
      <datalist id="bcf-cat-list">${catOptions}</datalist>
    </div>
    <div class="field">
      <label>Correct Category (what it should be)</label>
      <input class="input" id="bcf-correct" placeholder="e.g. seal" list="bcf-cat-list2" autocomplete="off">
      <datalist id="bcf-cat-list2">${catOptions}</datalist>
    </div>
    <div class="field">
      <label>Only fix parts whose name contains <span style="color:var(--muted);font-weight:400;">(leave blank = fix all)</span></label>
      <input class="input" id="bcf-namefilter" placeholder="e.g. seal">
    </div>
    <button class="btn btn-outline" onclick="bcfPreview()">🔍 Preview Matches</button>
    <div id="bcf-result" style="margin-top:12px;"></div>
  `);
}


function bcfPreview() {
  var wrong      = (document.getElementById('bcf-wrong')      || {}).value.trim().toLowerCase();
  var correct    = (document.getElementById('bcf-correct')    || {}).value.trim();
  var nameFilter = (document.getElementById('bcf-namefilter') || {}).value.trim().toLowerCase();
  var res        = document.getElementById('bcf-result');
  if (!res) return;

  if (!wrong)   { res.innerHTML = '<div style="color:var(--danger);font-size:13px;">Enter the wrong category first.</div>'; return; }
  if (!correct) { res.innerHTML = '<div style="color:var(--danger);font-size:13px;">Enter the correct category to change to.</div>'; return; }

  var matches = state.parts.filter(function(p) {
    var catMatch  = (p.category||'').trim().toLowerCase() === wrong;
    var nameMatch = !nameFilter || (p.name||'').toLowerCase().includes(nameFilter);
    return catMatch && nameMatch;
  });

  if (!matches.length) {
    res.innerHTML = '<div style="color:var(--muted);font-size:13px;padding:10px 0;">No parts found with category <b>' + wrong + '</b>'
      + (nameFilter ? ' and name containing <b>' + nameFilter + '</b>' : '') + '.</div>';
    return;
  }

  var html = '<div style="font-size:13px;font-weight:600;margin-bottom:8px;color:var(--primary);">'
    + matches.length + ' part' + (matches.length > 1 ? 's' : '') + ' will change: '
    + '<b style="color:#dc2626">' + wrong + '</b> &rarr; <b style="color:#16a34a">' + correct + '</b></div>';

  html += '<div style="max-height:220px;overflow-y:auto;border:1px solid var(--border);border-radius:8px;margin-bottom:10px;">';
  matches.forEach(function(p) {
    html += '<div style="display:flex;align-items:center;gap:10px;padding:8px 12px;border-bottom:1px solid var(--border);font-size:13px;">'
      + '<span style="font-family:monospace;font-size:11px;color:var(--muted);flex-shrink:0;">' + p.partNo + '</span>'
      + '<span style="flex:1;">' + p.name + '</span>'
      + '<span style="font-size:11px;background:#fee2e2;color:#991b1b;padding:2px 7px;border-radius:5px;flex-shrink:0;">' + (p.category||'—') + '</span>'
      + '</div>';
  });
  html += '</div>';
  // Use data attributes to avoid any quote escaping issues in onclick
  html += '<button class="btn btn-success" id="bcf-fix-btn">&#x2705; Fix All ' + matches.length + ' Parts Now</button>';
  res.innerHTML = html;

  // Attach click handler directly — no inline JSON, no quote issues
  var btn = document.getElementById('bcf-fix-btn');
  if (btn) btn.onclick = function() { bcfApplyFix(wrong, correct, nameFilter); };
}

async function bcfApplyFix(wrong, correct, nameFilter) {
  var count = 0;
  state.parts.forEach(function(p) {
    var catMatch  = (p.category||'').trim().toLowerCase() === wrong.toLowerCase();
    var nameMatch = !nameFilter || (p.name||'').toLowerCase().includes(nameFilter.toLowerCase());
    if (catMatch && nameMatch) { p.category = correct; count++; }
  });
  try { await save(); } catch(e) { console.error('bcfApplyFix save error', e); }
  renderInventory();
  var res = document.getElementById('bcf-result');
  if (res) res.innerHTML = '<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:12px;font-size:13px;font-weight:600;color:#16a34a;">'
    + '&#x2705; Done &mdash; ' + count + ' part' + (count > 1 ? 's' : '') + ' updated to category <b>' + correct + '</b></div>';
  toast('✅ ' + count + ' part' + (count>1?'s':'') + ' fixed to "' + correct + '"');
  logActivity && logActivity('Bulk category fix: "' + wrong + '" → "' + correct + '" (' + count + ' parts)', 'info');
}
// ─────────────────────────────────────────────────────────────

function exportInventoryCSV() {
  const rows = [['PartNo','Name','Brand','Machine','Category','Models','Stock','Min','Cost','MRP','Instant','Credit','Velocity','DaysLeft','Location','Supplier','SupPhone']];
  state.parts.forEach(p => rows.push([p.partNo,p.name,p.brand,p.machineType,p.category,p.models,p.stock,p.minStock,p.costPrice,p.mrp,p.instantPrice,p.creditPrice,Smart.velocity(p.id).toFixed(2),Smart.daysUntilStockout(p),p.location,p.supplierName,p.supplierPhone]));
  downloadCSV(rows, 'inventory-'+todayStr()+'.csv');
}
function exportSalesCSV() {
  const rows = [['Invoice','Date','Customer','Mode','Subtotal','Discount','Total','Items']];
  state.sales.forEach(s => {
    const cust = getCustomerById(s.customerId);
    rows.push([s.invoiceNo, s.date, cust?cust.name:'Walk-in', s.paymentMode, s.subtotal, s.discount, s.total, s.items.map(i=>i.qty+'x '+i.name).join('; ')]);
  });
  downloadCSV(rows, 'sales-'+todayStr()+'.csv');
}
function downloadCSV(rows, fname) {
  const csv = rows.map(r => r.map(c => `"${(c==null?'':c).toString().replace(/"/g,'""')}"`).join(',')).join('\n');
  const blob = new Blob([csv], {type:'text/csv'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob); a.download = fname; a.click();
  setTimeout(function(){ URL.revokeObjectURL(a.href); }, 5000); // free blob after download starts
}
function resetData() {
  if (!confirm('Delete ALL data permanently? Make sure you exported a backup first.')) return;
  if (!confirm('Are you 100% sure? This cannot be undone.')) return;
  localStorage.clear(); location.reload();
}

// ============================================================
// MODAL
// ============================================================
// ============================================================
// PARTS DATE REVIEW — calendar strip to fix name mistakes
// ============================================================
function openPartsDateReview() {
  // Build a map of date-string → parts[]
  const byDate = {};
  (state.parts || []).forEach(p => {
    if (!p.addedAt) return;
    const d = new Date(p.addedAt);
    const key = d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0');
    (byDate[key] = byDate[key] || []).push(p);
  });

  const dates = Object.keys(byDate).sort((a,b) => b.localeCompare(a)); // newest first
  if (!dates.length) { showModal('📅 Parts by Date', '<div style="text-align:center;padding:32px;color:var(--muted);">No parts with timestamps yet.</div>'); return; }

  // Default selected = today if it has parts, else most recent
  const todayKey = (function(){ const n=new Date(); return n.getFullYear()+'-'+String(n.getMonth()+1).padStart(2,'0')+'-'+String(n.getDate()).padStart(2,'0'); })();
  window._pdrSelected = byDate[todayKey] ? todayKey : dates[0];
  window._pdrByDate   = byDate;

  showModal('📅 Parts by Date', _pdrHTML());
}

function _pdrFmtDate(key) {
  const [y,m,d] = key.split('-');
  const n = new Date(); const today = n.getFullYear()+'-'+String(n.getMonth()+1).padStart(2,'0')+'-'+String(n.getDate()).padStart(2,'0');
  const yest = (function(){ const x=new Date(n); x.setDate(x.getDate()-1); return x.getFullYear()+'-'+String(x.getMonth()+1).padStart(2,'0')+'-'+String(x.getDate()).padStart(2,'0'); })();
  if (key === today) return 'Today';
  if (key === yest)  return 'Yesterday';
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  return d + ' ' + months[+m-1];
}

function _pdrHTML() {
  const byDate = window._pdrByDate;
  const sel    = window._pdrSelected;
  const dates  = Object.keys(byDate).sort((a,b) => b.localeCompare(a));

  // Calendar strip
  const strip = `<div style="display:flex;gap:6px;overflow-x:auto;padding:4px 0 10px;-webkit-overflow-scrolling:touch;">` +
    dates.map(d => {
      const isActive = d === sel;
      const cnt = byDate[d].length;
      return `<div onclick="window._pdrSelected='${d}';document.getElementById('modal-body').innerHTML=_pdrHTML();" style="flex-shrink:0;min-width:58px;text-align:center;padding:7px 6px;border-radius:10px;cursor:pointer;border:2px solid ${isActive?'#0f766e':'var(--border)'};background:${isActive?'#0f766e':'var(--bg)'};">
        <div style="font-size:12px;font-weight:800;color:${isActive?'white':'var(--text)'};">${_pdrFmtDate(d)}</div>
        <div style="font-size:10px;font-weight:600;color:${isActive?'#bbf7d0':'var(--muted)'};">${cnt} part${cnt>1?'s':''}</div>
      </div>`;
    }).join('') + `</div>`;

  // Parts list for selected date
  const parts = (byDate[sel] || []).slice().sort((a,b) => b.addedAt - a.addedAt);
  const rows = parts.map(p => {
    const t = new Date(p.addedAt);
    const timeStr = t.toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit'});
    const pricingOk = catPricingDone && catPricingDone(p);
    const flags = [];
    if (!pricingOk)      flags.push('<span style="font-size:10px;background:#fef9c3;color:#854d0e;padding:1px 6px;border-radius:6px;font-weight:700;">No price</span>');
    if (!p.location)     flags.push('<span style="font-size:10px;background:#fef2f2;color:#dc2626;padding:1px 6px;border-radius:6px;font-weight:700;">No rack</span>');
    if (!p.supplierName) flags.push('<span style="font-size:10px;background:#fffbeb;color:#d97706;padding:1px 6px;border-radius:6px;font-weight:600;">No supplier</span>');

    return `<div style="border:1px solid var(--border);border-radius:10px;padding:10px 12px;margin-bottom:8px;background:var(--bg);">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:8px;">
        <div style="flex:1;min-width:0;">
          <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
            <input id="pdr-name-${p.id}" value="${p.name.replace(/"/g,'&quot;')}" style="font-size:13px;font-weight:700;color:var(--text);border:none;border-bottom:1.5px dashed var(--border);background:transparent;padding:2px 4px;width:100%;min-width:0;box-sizing:border-box;" onchange="window._pdrPendingEdit=window._pdrPendingEdit||{};window._pdrPendingEdit['${p.id}']=this.value;" onfocus="this.select();">
          </div>
          <div style="font-size:11px;color:var(--muted);margin-top:3px;">${p.partNo} · ${p.brand}${p.machineType?' · '+p.machineType:''}</div>
          ${flags.length ? `<div style="display:flex;gap:4px;flex-wrap:wrap;margin-top:4px;">${flags.join('')}</div>` : '<div style="font-size:10px;color:#16a34a;margin-top:3px;font-weight:600;">✅ Complete</div>'}
        </div>
        <div style="text-align:right;flex-shrink:0;">
          <div style="font-size:10px;color:var(--muted);">${timeStr}</div>
          <button onclick="window._pdrSaveName('${p.id}')" style="margin-top:6px;font-size:11px;font-weight:700;padding:4px 10px;border-radius:7px;background:#0f766e;color:white;border:none;cursor:pointer;">Save</button>
          <button onclick="closeModal();openAddPart('${p.id}')" style="margin-top:4px;display:block;font-size:11px;padding:4px 10px;border-radius:7px;background:var(--bg);border:1px solid var(--border);cursor:pointer;width:100%;">Full Edit →</button>
        </div>
      </div>
    </div>`;
  }).join('');

  return strip +
    `<div style="font-size:11px;font-weight:700;color:#0f766e;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px;">${_pdrFmtDate(sel)} — ${parts.length} part${parts.length!==1?'s':''}</div>` +
    rows +
    `<div style="text-align:center;margin-top:4px;"><button class="btn btn-outline" onclick="goTab('inventory');state.filterMachine='new';renderInventory();" style="font-size:12px;">Open in Inventory →</button></div>`;
}

window._pdrSaveName = function(partId) {
  const inp = document.getElementById('pdr-name-' + partId);
  if (!inp) return;
  const newName = inp.value.trim();
  if (!newName) { inp.style.borderColor='#dc2626'; return; }
  const idx = state.parts.findIndex(x => x.id === partId);
  if (idx < 0) return;
  state.parts[idx].name = newName;
  markChanged('parts'); // mid-array field edit is invisible to the parts fingerprint — force the durable write
  save();
  inp.style.borderBottom = '1.5px solid #0f766e';
  inp.style.color = '#0f766e';
  const btn = inp.closest('div[style]').querySelector('button');
  if (btn) { btn.textContent = '✅'; btn.style.background='#16a34a'; setTimeout(()=>{btn.textContent='Save';btn.style.background='#0f766e';},1500); }
  logActivity && logActivity('Name corrected: ' + partId + ' → "' + newName + '"', 'info');
  toast('✅ Name updated');
};

function showModal(title, body) {
  document.getElementById('modal-title').textContent = title;
  document.getElementById('modal-body').innerHTML = body;
  document.getElementById('modal-bg').classList.add('show');
}
function closeModal() { document.getElementById('modal-bg').classList.remove('show'); }

// ============================================================
// LOGIN SYSTEM
// ============================================================
function initLogin() {
  // If already have a valid session, skip login
  if (state.session) {
    document.getElementById('login-screen').style.display = 'none';
    applyRoleUI();
    showUserBar();
    // Refresh path: the login handler (which unlocks central AI) was skipped, so
    // re-hydrate the decrypted keys from this session's cache. Without this, AI
    // silently falls back to a stale/empty local key and 401s on "x-api-key".
    try { _restoreCentralFromSession(); } catch(_){}
    return;
  }

  // Show login screen
  document.getElementById('login-screen').style.display = 'flex';

  if (state.users.length === 0) {
    // First time — show setup panel
    document.getElementById('setup-panel').style.display = 'block';
    document.getElementById('login-panel').style.display = 'none';
  } else {
    // Returning — show login panel
    document.getElementById('setup-panel').style.display = 'none';
    document.getElementById('login-panel').style.display = 'block';
    const activeUsers = state.users.filter(u => u.active !== false);
    const sel = document.getElementById('login-staff-select');
    sel.innerHTML = '<option value="">— select your name —</option>' +
      activeUsers.map(u => `<option value="${u.id}">${u.name} (${u.role})</option>`).join('');
    const dl = document.getElementById('login-staff-list');
    if (dl) dl.innerHTML = activeUsers.map(u => `<option value="${u.name}" data-id="${u.id}">${u.name} (${u.role})</option>`).join('');
  }
}

function switchToSetup() {
  document.getElementById('setup-panel').style.display = 'block';
  document.getElementById('login-panel').style.display = 'none';
}

function selectRole(role) {
  document.querySelectorAll('.role-card').forEach(c => c.classList.remove('sel'));
  document.getElementById('role-'+role)?.classList.add('sel');
}

async function createFirstAdmin() {
  const errDiv = document.getElementById('setup-error');
  errDiv.textContent = '';
  const shopName = document.getElementById('setup-shop').value.trim();
  const name = document.getElementById('setup-admin-name').value.trim();
  const pin = document.getElementById('setup-pin').value.trim();
  const pin2 = document.getElementById('setup-pin2').value.trim();
  const sq1 = document.getElementById('setup-sq1').value.trim();
  const sa1 = document.getElementById('setup-sa1').value.trim().toLowerCase();
  const sq2 = document.getElementById('setup-sq2').value.trim();
  const sa2 = document.getElementById('setup-sa2').value.trim().toLowerCase();

  if (!shopName) { errDiv.textContent = '⚠️ Enter shop name'; return; }
  if (!name) { errDiv.textContent = '⚠️ Enter your name'; return; }
  if (!pin || pin.length < 4) { errDiv.textContent = '⚠️ PIN must be exactly 4 digits'; return; }
  if (!/^\d{4}$/.test(pin)) { errDiv.textContent = '⚠️ PIN must be 4 numbers only'; return; }
  if (pin !== pin2) { errDiv.textContent = '⚠️ PINs do not match'; return; }
  if (!sq1 || !sa1) { errDiv.textContent = '⚠️ Set Security Question 1 and answer'; return; }
  if (!sq2 || !sa2) { errDiv.textContent = '⚠️ Set Security Question 2 and answer'; return; }

  state.settings.shopName = shopName;
  const staffId = uid();
  const userId = uid();
  state.staff.push({id:staffId, name, role:'admin', baseSalary:0, commRate:state.settings.commRate||1.5, delivRate:state.settings.delivRate||50, joinDate:todayStr(), active:true});
  // Hash the PIN with a per-user salt so it never sits in plain text
  const pinSalt = makeSalt();
  const pinHash = await hashPin(pin, pinSalt);
  const sa1Hash = await sha256Hex(sa1);
  const sa2Hash = await sha256Hex(sa2);
  state.users.push({id:userId, name, role:'admin', pinHash, pinSalt, staffId, sq1, sa1Hash, sq2, sa2Hash, active:true, approvedBy:'self'});
  save();

  state.session = {userId, name, role:'admin', staffId};
  DB.set('session', state.session);
  document.getElementById('login-screen').style.display = 'none';
  document.getElementById('hdr-title').textContent = shopName;
  showUserBar();
  renderHome();
  toast('Welcome, ' + name + '! Shop created 👑');
}

// ---- FORGOT PIN FLOW ----
let _sqVerifiedUserId = null;
let _loginAttempts = {};

function showForgotPin() {
  ['login-panel','setup-panel','reset-panel'].forEach(id => document.getElementById(id).style.display='none');
  document.getElementById('forgot-panel').style.display = 'block';
  const sel = document.getElementById('forgot-staff-select');
  sel.innerHTML = '<option value="">— select —</option>' +
    state.users.map(u => `<option value="${u.id}">${u.name} (${u.role})</option>`).join('');
  document.getElementById('sq-fields').innerHTML = '';
  document.getElementById('forgot-error').textContent = '';
}

function loadSecurityQuestions() {
  const userId = document.getElementById('forgot-staff-select').value;
  const div = document.getElementById('sq-fields');
  if (!userId) { div.innerHTML = ''; return; }
  const user = state.users.find(u => u.id === userId);
  if (!user || !user.sq1) {
    div.innerHTML = '<div style="color:#dc2626;font-size:13px;">⚠️ This account has no security questions set. Contact admin.</div>';
    return;
  }
  div.innerHTML = `
    <div class="field"><label>${user.sq1}</label><input class="input" id="sq-ans1" placeholder="Your answer"></div>
    <div class="field"><label>${user.sq2}</label><input class="input" id="sq-ans2" placeholder="Your answer"></div>`;
}

async function verifySQ() {
  const errDiv = document.getElementById('forgot-error');
  errDiv.textContent = '';
  const userId = document.getElementById('forgot-staff-select').value;
  if (!userId) { errDiv.textContent = '⚠️ Select your name'; return; }
  const user = state.users.find(u => u.id === userId);
  const ans1 = (document.getElementById('sq-ans1')?.value || '').trim().toLowerCase();
  const ans2 = (document.getElementById('sq-ans2')?.value || '').trim().toLowerCase();

  // Track attempts per user (lockout after 3 wrong)
  _loginAttempts[userId] = (_loginAttempts[userId] || 0);
  if (_loginAttempts[userId] >= 3) {
    errDiv.textContent = '🔒 Too many attempts. Restart the app to try again.';
    if (typeof RX !== 'undefined') RX.log('warn','auth','SQ lockout for user ' + userId);
    return;
  }

  // Compare against either hashed or legacy plain answers
  const ans1Hash = await sha256Hex(ans1);
  const ans2Hash = await sha256Hex(ans2);
  const ok1 = user.sa1Hash ? (ans1Hash === user.sa1Hash) : (ans1 === user.sa1);
  const ok2 = user.sa2Hash ? (ans2Hash === user.sa2Hash) : (ans2 === user.sa2);
  if (!ok1 || !ok2) {
    _loginAttempts[userId]++;
    const left = 3 - _loginAttempts[userId];
    errDiv.textContent = `❌ Wrong answers. ${left > 0 ? left + ' attempt(s) left.' : 'Account locked — restart app.'}`;
    if (typeof RX !== 'undefined') RX.log('warn','auth','failed SQ for ' + user.name);
    return;
  }

  // Correct — show reset panel
  _sqVerifiedUserId = userId;
  _loginAttempts[userId] = 0;
  if (typeof RX !== 'undefined') RX.log('info','auth','SQ verified for ' + user.name);
  document.getElementById('forgot-panel').style.display = 'none';
  document.getElementById('reset-panel').style.display = 'block';
  document.getElementById('reset-error').textContent = '';
}

async function doResetPin() {
  const errDiv = document.getElementById('reset-error');
  errDiv.textContent = '';
  const newPin = document.getElementById('reset-pin').value.trim();
  const newPin2 = document.getElementById('reset-pin2').value.trim();
  if (!newPin || newPin.length < 4) { errDiv.textContent = '⚠️ PIN must be 4 digits'; return; }
  if (!/^\d{4}$/.test(newPin)) { errDiv.textContent = '⚠️ Digits only'; return; }
  if (newPin !== newPin2) { errDiv.textContent = '⚠️ PINs do not match'; return; }
  const user = state.users.find(u => u.id === _sqVerifiedUserId);
  if (!user) { errDiv.textContent = '⚠️ Session expired, restart'; return; }
  user.pinSalt = makeSalt();
  user.pinHash = await hashPin(newPin, user.pinSalt);
  delete user.pin;
  if (typeof RX !== 'undefined') RX.log('info','auth','PIN reset for ' + user.name);
  if (typeof logActivity === 'function') logActivity(`PIN reset for ${user.name}`, 'warn');
  if (typeof pushToJarvis === 'function') pushToJarvis({
    kind:'data_security', severity:'warning',
    title:'🔑 PIN Reset', message:'PIN was reset for '+user.name+' via security questions.',
    data:{ userName: user.name }
  });
  save();
  _sqVerifiedUserId = null;
  document.getElementById('reset-panel').style.display = 'none';
  document.getElementById('login-panel').style.display = 'block';
  const activeUsers = state.users.filter(u => u.active !== false);
  const sel = document.getElementById('login-staff-select');
  sel.innerHTML = '<option value="">— select your name —</option>' +
    activeUsers.map(u => `<option value="${u.id}">${u.name} (${u.role})</option>`).join('');
  const dl = document.getElementById('login-staff-list');
  if (dl) dl.innerHTML = activeUsers.map(u => `<option value="${u.name}" data-id="${u.id}">${u.name} (${u.role})</option>`).join('');
  const inp = document.getElementById('login-staff-input');
  if (inp) inp.value = user.name;
  // Pre-select in hidden select too
  sel.value = user.id;
  toast('✅ PIN reset successfully! Login with your new PIN.');
}

function backToLogin() {
  ['forgot-panel','reset-panel','setup-panel'].forEach(id => document.getElementById(id).style.display='none');
  document.getElementById('login-panel').style.display = 'block';
}

// Also update saveStaff to include SQ prompt for new staff
function openAddStaffSQ(userId) {
  const user = state.users.find(u => u.id === userId);
  if (!user) return;
  showModal('🔒 Set Security Questions for ' + user.name, `
    <p style="font-size:13px;color:var(--muted);">These are used if the staff member forgets their PIN.</p>
    <div class="field"><label>Question 1</label>
      <select class="select" id="msq1">
        <option value="">— choose —</option>
        <option>What is your mother's maiden name?</option>
        <option>What was the name of your first school?</option>
        <option>What is your childhood nickname?</option>
        <option>What is the name of your hometown?</option>
        <option>What was the name of your first pet?</option>
        <option>What is your father's middle name?</option>
      </select>
      <input class="input" id="msa1" placeholder="Answer" style="margin-top:6px;" value="${user.sa1||''}">
    </div>
    <div class="field"><label>Question 2</label>
      <select class="select" id="msq2">
        <option value="">— choose —</option>
        <option>What was the model of your first vehicle?</option>
        <option>What is your oldest sibling's name?</option>
        <option>In which city were you born?</option>
        <option>What is your lucky number?</option>
        <option>What was your first mobile number?</option>
        <option>What is your shop's street name?</option>
      </select>
      <input class="input" id="msa2" placeholder="Answer" style="margin-top:6px;" value="${user.sa2||''}">
    </div>
    <button class="btn" onclick="saveStaffSQ('${userId}')">Save Security Questions</button>
  `);
  // Pre-select saved questions
  if (user.sq1) { setTimeout(() => { const s=document.getElementById('msq1'); for(let i=0;i<s.options.length;i++) if(s.options[i].text===user.sq1){s.selectedIndex=i;break;} }, 100); }
  if (user.sq2) { setTimeout(() => { const s=document.getElementById('msq2'); for(let i=0;i<s.options.length;i++) if(s.options[i].text===user.sq2){s.selectedIndex=i;break;} }, 100); }
}

function saveStaffSQ(userId) {
  const user = state.users.find(u => u.id === userId);
  if (!user) return;
  const sq1 = document.getElementById('msq1').value;
  const sa1 = document.getElementById('msa1').value.trim().toLowerCase();
  const sq2 = document.getElementById('msq2').value;
  const sa2 = document.getElementById('msa2').value.trim().toLowerCase();
  if (!sq1||!sa1||!sq2||!sa2) { toast('Fill all questions and answers'); return; }
  user.sq1=sq1; user.sa1=sa1; user.sq2=sq2; user.sa2=sa2;
  save(); closeModal(); toast('Security questions saved ✅');
}

// Sync text input → hidden select when user picks from datalist or types
function loginInputChanged() {
  const inp = document.getElementById('login-staff-input');
  const sel = document.getElementById('login-staff-select');
  if (!inp || !sel) return;
  const typed = inp.value.trim().toLowerCase();
  // Try to match by name (case-insensitive)
  const matched = state.users.find(u => u.active !== false && u.name.toLowerCase() === typed);
  sel.value = matched ? matched.id : '';
}

async function doLogin() {
  const errDiv = document.getElementById('login-error');
  errDiv.textContent = '';

  // Resolve userId — from hidden select (synced) or fallback match by typed name
  let userId = document.getElementById('login-staff-select').value;
  const inp = document.getElementById('login-staff-input');
  if (!userId && inp) {
    const typed = inp.value.trim().toLowerCase();
    const matched = state.users.find(u => u.active !== false && u.name.toLowerCase() === typed);
    if (matched) userId = matched.id;
  }

  const pin = document.getElementById('login-pin').value.trim();
  if (!userId) { errDiv.textContent = '⚠️ Select or type your name first'; return; }
  if (!pin) { errDiv.textContent = '⚠️ Enter your PIN'; return; }

  _loginAttempts[userId] = _loginAttempts[userId] || 0;
  if (_loginAttempts[userId] >= 5) {
    errDiv.textContent = '🔒 Account locked after 5 wrong attempts. Use "Forgot PIN?" to reset.';
    if (typeof RX !== 'undefined') RX.log('warn','auth','login lockout for user ' + userId);
    var u = state.users.find(function(x){return x.id===userId;});
    if (typeof pushToJarvis === 'function') pushToJarvis({
      kind:'data_security', severity:'critical',
      title:'🔒 Account Locked', message:'5 wrong PIN attempts on '+(u?u.name:'unknown user')+'. Possible unauthorised access.',
      data:{ userId:userId, userName:u?u.name:null }
    });
    return;
  }

  const user = state.users.find(u => u.id === userId);
  if (!user) { errDiv.textContent = '⚠️ User not found'; return; }

  // Owner must explicitly activate accounts before they can login
  if (user.active === false) {
    errDiv.textContent = '🔒 Your account is deactivated. Ask the owner to activate it.';
    return;
  }

  // Compare PIN — supports both new hashed format and legacy plain-text
  let pinOk = false;
  if (user.pinHash) {
    const cmp = await hashPin(pin, user.pinSalt || '');
    pinOk = (cmp === user.pinHash);
  } else if (user.pin) {
    pinOk = (user.pin === pin);
    // Auto-migrate to hashed format on successful login
    if (pinOk) {
      user.pinSalt = makeSalt();
      user.pinHash = await hashPin(pin, user.pinSalt);
      delete user.pin;
      // Migrate security answers to hashed too
      if (user.sa1 && !user.sa1Hash) { user.sa1Hash = await sha256Hex(user.sa1); delete user.sa1; }
      if (user.sa2 && !user.sa2Hash) { user.sa2Hash = await sha256Hex(user.sa2); delete user.sa2; }
      if (typeof RX !== 'undefined') RX.log('info','auth','migrated user ' + user.name + ' to hashed PIN');
    }
  }

  if (!pinOk) {
    _loginAttempts[userId]++;
    const left = 5 - _loginAttempts[userId];
    errDiv.textContent = `❌ Wrong PIN. ${left} attempt${left!==1?'s':''} left before lockout.`;
    document.getElementById('login-pin').value = '';
    if (typeof RX !== 'undefined') RX.log('warn','auth','failed login for ' + user.name + ' (attempt ' + _loginAttempts[userId] + ')');
    if (typeof logActivity === 'function') logActivity(`Failed login attempt for ${user.name}`, 'warn');
    return;
  }

  _loginAttempts[userId] = 0;
  if (typeof RX !== 'undefined') RX.log('info','auth','login OK for ' + user.name);
  if (typeof logActivity === 'function') logActivity(`Login: ${user.name}`, 'info');
  state.session = {userId: user.id, name: user.name, role: user.role, staffId: user.staffId || null};
  DB.set('session', state.session);
  document.getElementById('login-screen').style.display = 'none';
  applyRoleUI();
  showUserBar();
  renderHome();
  toast('Welcome, ' + user.name + '! 👋');
  // If admin, decrypt central keys using the PIN they just entered
  if (user.role === 'admin' && typeof unlockCentralKeys === 'function') {
    unlockCentralKeys(pin).then(ok => {
      if (ok) {
        const el = document.getElementById('sync-indicator');
        if (el) { el.textContent = '🧠 Central AI unlocked'; el.className = 'synced'; setTimeout(()=>el.className='hidden',2500); }
      }
    }).catch(()=>{});
  }
}

function doLogout() {
  if (!confirm('Logout?')) return;
  state.session = null;
  DB.set('session', null);
  try { _clearCentralSession(); } catch(_){}   // drop cached AI keys — next user must unlock
  window._centralKeys = null;
  location.reload();
}

function showUserBar() {
  if (!state.session) return;
  const bar = document.getElementById('current-user-bar');
  bar.style.display = 'flex';
  document.getElementById('cur-user-label').textContent = `👤 ${state.session.name} · ${state.session.role}`;
}

function firstSetup() { switchToSetup(); }

// ============================================================
// STAFF TAB
// ============================================================
function showStaffSection(name, evt) {
  document.querySelectorAll('.staff-section').forEach(s => s.style.display = 'none');
  document.getElementById('staff-'+name).style.display = 'block';
  document.querySelectorAll('#staff-chips .chip').forEach(c => c.classList.remove('active'));
  if (evt && evt.target) evt.target.classList.add('active');
  if (name === 'performance') renderStaffPerformance();
  if (name === 'commission') renderCommissions();
  if (name === 'salary') renderSalaryIncrements();
  if (name === 'manage') renderManageStaff();
}

function renderStaffTab() { showStaffSection('performance'); }

function staffSalesMonth(staffId) {
  return state.sales.filter(s => isThisMonth(s.date) && s.soldById === staffId).reduce((a,s)=>a+s.total,0);
}
function staffDeliveriesMonth(staffId) {
  return state.sales.filter(s => isThisMonth(s.date) && s.deliveredById === staffId).length;
}

function renderStaffPerformance() {
  const target = state.settings.salesTarget || 50000;
  const salespeople = state.staff.filter(s => s.active && s.role !== 'delivery');
  const ranked = salespeople.map(s => ({s, sales: staffSalesMonth(s.id), delivers: staffDeliveriesMonth(s.id)})).sort((a,b) => b.sales - a.sales);
  let html = `<div class="card"><h2>🏆 Sales Leaderboard — ${new Date().toLocaleString('en-IN',{month:'long'})}</h2><small>Target: ${rs(target)} per salesperson</small>`;
  if (ranked.length === 0) html += '<div class="empty mt-2">No salespeople yet. Add staff in Manage Staff tab.</div>';
  ranked.forEach((r,i) => {
    const pct = Math.min(100, (r.sales/target*100)).toFixed(0);
    const rankClass = i===0?'rank-1':i===1?'rank-2':i===2?'rank-3':'';
    html += `<div class="comm-card mt-1">
      <div class="flex-between"><div class="name">${r.s.name} <span class="rank-badge ${rankClass}">#${i+1}</span></div><div class="bold text-success">${rs(r.sales)}</div></div>
      <div style="font-size:12px;color:var(--muted);">${r.s.role} · ${r.delivers} deliveries</div>
      <div class="comm-bar"><div class="comm-bar-fill" style="width:${pct}%"></div></div>
      <div style="font-size:11px;color:var(--muted);margin-top:3px;">${pct}% of target</div></div>`;
  });
  html += '</div>';
  const delivRanked = state.staff.filter(s=>s.active).map(s=>({s,count:staffDeliveriesMonth(s.id)})).filter(r=>r.count>0).sort((a,b)=>b.count-a.count);
  if (delivRanked.length) {
    html += `<div class="card"><h2>🛵 Delivery Stats</h2>`;
    delivRanked.forEach(r => { html += `<div class="cart-item"><div><b>${r.s.name}</b><br><small>${r.s.role}</small></div><div class="bold text-primary">${r.count} deliveries · ${rs(r.count*(r.s.delivRate||state.settings.delivRate))} earned</div></div>`; });
    html += '</div>';
  }
  document.getElementById('staff-perf-list').innerHTML = html;
}

function renderCommissions() {
  document.getElementById('comm-period').textContent = `Month: ${new Date().toLocaleString('en-IN',{month:'long',year:'numeric'})}`;
  const rows = state.staff.filter(s=>s.active).map(s => {
    const sales = staffSalesMonth(s.id);
    const delivers = staffDeliveriesMonth(s.id);
    const commRate = s.commRate ?? state.settings.commRate;
    const delivRate = s.delivRate ?? state.settings.delivRate;
    const salesComm = Math.round(sales * commRate / 100);
    const delivComm = delivers * delivRate;
    return {s, sales, delivers, salesComm, delivComm, total: salesComm+delivComm};
  }).filter(r => r.total > 0 || r.sales > 0);
  if (!rows.length) { document.getElementById('comm-list').innerHTML = '<div class="empty">No commissions yet. Record sales with "Sold By" to track.</div>'; return; }
  document.getElementById('comm-list').innerHTML = rows.map(r => `
    <div class="comm-card">
      <div class="flex-between"><div class="name">${r.s.name}</div><div class="bold text-success">Total: ${rs(r.total)}</div></div>
      <div class="perf-grid mt-1">
        <div><div class="label">Sales</div><div class="perf-num">${rs(r.sales)}</div></div>
        <div><div class="label">Sales Comm (${r.s.commRate??state.settings.commRate}%)</div><div class="perf-num text-success">${rs(r.salesComm)}</div></div>
        <div><div class="label">Delivery ×${r.delivers}</div><div class="perf-num">${rs(r.delivComm)}</div></div>
      </div>
      ${r.s.baseSalary?`<div style="margin-top:6px;font-size:12px;color:var(--muted);">Base ${rs(r.s.baseSalary)} + Comm ${rs(r.total)} = <b>${rs(r.s.baseSalary+r.total)}</b> total payable</div>`:''}
    </div>`).join('');
}

function renderSalaryIncrements() {
  const target = state.settings.salesTarget || 50000;
  const list = state.staff.filter(s=>s.active&&s.baseSalary>0).map(s => {
    const sales = staffSalesMonth(s.id);
    const pct = target > 0 ? sales/target*100 : 0;
    let increment = 0, badge = '', color = 'var(--muted)';
    if (pct >= 150) { increment=0.15; badge='🌟 Star Performer'; color='var(--success)'; }
    else if (pct >= 120) { increment=0.10; badge='🔥 Excellent'; color='var(--success)'; }
    else if (pct >= 100) { increment=0.07; badge='✅ Target Met'; color='var(--primary)'; }
    else if (pct >= 80) { increment=0.05; badge='👍 Good'; color='var(--primary)'; }
    else if (pct >= 60) { increment=0.03; badge='📈 Average'; color='var(--warning)'; }
    else { increment=0; badge='⚠️ Below Target'; color='var(--danger)'; }
    const incrAmt = Math.round(s.baseSalary * increment);
    return {s, sales, pct, increment, incrAmt, newSalary: s.baseSalary+incrAmt, badge, color};
  });
  if (!list.length) { document.getElementById('salary-list').innerHTML = '<div class="empty">Add base salary to staff members to see recommendations.</div>'; return; }
  document.getElementById('salary-list').innerHTML = list.map(r => `
    <div class="comm-card">
      <div class="flex-between"><div class="name">${r.s.name}</div><span style="color:${r.color};font-size:12px;font-weight:600;">${r.badge}</span></div>
      <div style="font-size:12px;color:var(--muted);">Achieved: ${rs(r.sales)} / ${rs(target)} (${r.pct.toFixed(0)}%)</div>
      <div class="comm-bar mt-1"><div class="comm-bar-fill" style="width:${Math.min(100,r.pct).toFixed(0)}%;background:${r.color}"></div></div>
      <div class="perf-grid mt-1">
        <div><div class="label">Current</div><div class="perf-num">${rs(r.s.baseSalary)}</div></div>
        <div><div class="label">Increment (${(r.increment*100).toFixed(0)}%)</div><div class="perf-num text-success">+${rs(r.incrAmt)}</div></div>
        <div><div class="label">New Salary</div><div class="perf-num" style="color:${r.color}">${rs(r.newSalary)}</div></div>
      </div>
      ${r.incrAmt > 0 ? `<button class="btn btn-sm btn-success mt-1" onclick="applyIncrement('${r.s.id}',${r.newSalary})">✓ Apply Increment</button>` : ''}
    </div>`).join('');
}

function applyIncrement(staffId, newSalary) {
  const s = state.staff.find(x=>x.id===staffId);
  if (!s || !confirm(`Set ${s.name}'s salary to ${rs(newSalary)}?`)) return;
  s.baseSalary = newSalary; save(); renderSalaryIncrements(); toast('Salary updated!');
}

function renderManageStaff() {
  document.getElementById('staff-manage-list').innerHTML = state.staff.length === 0
    ? '<div class="empty mt-2">No staff yet</div>'
    : state.staff.map(s => {
        const user = state.users.find(u => u.staffId === s.id);
        const hasSQ = user && user.sq1;
        return `<div class="part-item mt-1">
          <div class="part-row" onclick="openAddStaff('${s.id}')">
            <div style="flex:1;"><div class="bold">${s.name} ${s.active?'':'<span class="tag dead">Inactive</span>'}</div>
              <div class="part-meta"><span class="tag">${s.role}</span>${hasSQ?'<span class="tag" style="background:#dcfce7;color:#166534;">🔒 SQ Set</span>':'<span class="tag warn">No SQ</span>'}</div>
              <small>Base: ${rs(s.baseSalary||0)} · Comm: ${s.commRate??state.settings.commRate}% · Deliv: ₹${s.delivRate??state.settings.delivRate}/trip</small>
            </div>
          </div>
          ${user ? `<button class="btn btn-sm btn-outline mt-1" onclick="openAddStaffSQ('${user.id}')">🔒 ${hasSQ?'Update':'Set'} Security Questions</button>` : ''}
        </div>`;
      }).join('');
}

function openAddStaff(id) {
  const s = id ? state.staff.find(x=>x.id===id) : {};
  const existingUser = id ? state.users.find(u=>u.staffId===id) : null;
  showModal(id?'Edit Staff':'Add Staff Member', `
    <div class="field"><label>Name *</label><input class="input" id="sf-name" value="${s.name||''}"></div>
    <div class="field"><label>Role</label>
      <select class="select" id="sf-role">
        <option value="salesperson" ${s.role==='salesperson'?'selected':''}>Salesperson</option>
        <option value="delivery" ${s.role==='delivery'?'selected':''}>Delivery</option>
        <option value="manager" ${s.role==='manager'?'selected':''}>Manager</option>
        <option value="admin" ${s.role==='admin'?'selected':''}>Admin</option>
        <option value="manager" ${s.role==='manager'?'selected':''}>Manager</option>
        <option value="accounts" ${s.role==='accounts'?'selected':''}>Accounts</option>
        <option value="readonly" ${s.role==='readonly'?'selected':''}>Read Only</option>
      </select>
    </div>
    <div class="row2">
      <div class="field"><label>Base Salary ₹/mo</label><input type="number" class="input" id="sf-salary" value="${s.baseSalary||0}"></div>
      <div class="field"><label>Commission % on Sales</label><input type="number" step="0.1" class="input" id="sf-comm" value="${s.commRate??state.settings.commRate}"></div>
    </div>
    <div class="field"><label>Delivery Bonus ₹/trip</label><input type="number" class="input" id="sf-deliv" value="${s.delivRate??state.settings.delivRate}"></div>
    <div class="field"><label>Attendance / Employee Code <span style="color:var(--muted);font-weight:400;">(for live sync)</span></label><input class="input" id="sf-attcode" value="${s.attendanceCode||''}" placeholder="e.g. EMP001 — must match attendance software"></div>
    <div class="field"><label>Login PIN (4 digits)</label><input type="password" class="input" id="sf-pin" maxlength="4" inputmode="numeric" placeholder="${existingUser?'leave blank to keep current':' set 4-digit PIN'}"></div>
    <div class="field"><label><input type="checkbox" id="sf-active" ${s.active!==false?'checked':''}> Active</label></div>
    <button class="btn" onclick="saveStaff('${id||''}')">${id?'Update':'Add'} Staff</button>
    ${id?`<button class="btn btn-danger mt-1" onclick="deleteStaff('${id}')">Remove</button>`:''}
  `);
}

async function saveStaff(id) {
  const name = document.getElementById('sf-name').value.trim();
  const role = document.getElementById('sf-role').value;
  const pin = document.getElementById('sf-pin').value.trim();
  if (!name) { toast('Name required'); return; }
  if (!id && !pin) { toast('PIN required for new staff'); return; }
  if (pin && pin.length < 4) { toast('PIN must be 4 digits'); return; }
  const staffId = id || uid();
  const prev = id ? (state.staff.find(x=>x.id===id) || {}) : {};
  // Spread prev first so fields not in this form (attendanceCode sync data,
  // liveEarned, etc.) are preserved instead of being wiped on edit.
  const staffObj = Object.assign({}, prev, {id:staffId, name, role,
    baseSalary: +document.getElementById('sf-salary').value||0,
    commRate: +document.getElementById('sf-comm').value||state.settings.commRate,
    delivRate: +document.getElementById('sf-deliv').value||state.settings.delivRate,
    attendanceCode: (document.getElementById('sf-attcode')?.value||'').trim(),
    active: document.getElementById('sf-active').checked,
    joinDate: id ? (prev.joinDate||todayStr()) : todayStr()
  });
  if (id) state.staff[state.staff.findIndex(x=>x.id===id)] = staffObj;
  else state.staff.push(staffObj);
  const existingUser = state.users.find(u=>u.staffId===staffId);
  if (existingUser) {
    existingUser.name=name; existingUser.role=role;
    if (pin) {
      existingUser.pinSalt = makeSalt();
      existingUser.pinHash = await hashPin(pin, existingUser.pinSalt);
      delete existingUser.pin;
      if (typeof RX !== 'undefined') RX.log('info','auth','PIN updated for ' + name);
    }
  } else if (pin) {
    const newSalt = makeSalt();
    const newHash = await hashPin(pin, newSalt);
    state.users.push({id:uid(), name, role, pinHash:newHash, pinSalt:newSalt, staffId, active:true, approvedBy: state.session?.name || 'admin'});
  }
  save(); closeModal(); renderManageStaff();
  const sel = document.getElementById('login-staff-select');
  if (sel) sel.innerHTML = '<option value="">— select —</option>' + state.users.map(u=>`<option value="${u.id}">${u.name} (${u.role})</option>`).join('');
  toast(id?'Staff updated':'Staff added ✅');
}

function deleteStaff(id) {
  if (!confirm('Remove this staff member?')) return;
  state.staff = state.staff.filter(x=>x.id!==id);
  state.users = state.users.filter(u=>u.staffId!==id);
  window._ownerDeleteInProgress = true;
  save().finally(() => { window._ownerDeleteInProgress = false; });
  closeModal(); renderManageStaff();
}

function openCommSettings() {
  showModal('💰 Commission Settings', `
    <div class="field"><label>Default Commission % on Sales</label><input type="number" step="0.1" class="input" id="cs-rate" value="${state.settings.commRate}"></div>
    <div class="field"><label>Delivery Bonus ₹ per trip</label><input type="number" class="input" id="cs-deliv" value="${state.settings.delivRate}"></div>
    <div class="field"><label>Monthly Sales Target ₹ per Salesperson</label><input type="number" class="input" id="cs-target" value="${state.settings.salesTarget||50000}"></div>
    <small>These are defaults. Override per person in Manage Staff.</small>
    <button class="btn mt-2" onclick="saveCommSettings()">Save</button>
  `);
}
function saveCommSettings() {
  state.settings.commRate = +document.getElementById('cs-rate').value||1.5;
  state.settings.delivRate = +document.getElementById('cs-deliv').value||50;
  state.settings.salesTarget = +document.getElementById('cs-target').value||50000;
  save(); closeModal(); toast('Commission settings saved');
}

// ============================================================
// BULK IMPORT SYSTEM
// ============================================================
function handleBulkCSV(event) {
  const file = event.target.files[0]; if (!file) return;
  const name = file.name.toLowerCase();
  const isExcel = name.endsWith('.xlsx') || name.endsWith('.xls') || name.endsWith('.xlsm');

  if (isExcel) {
    // Excel path — use SheetJS
    if (typeof XLSX === 'undefined') {
      document.getElementById('bulk-import-status').innerHTML = '<div class="text-muted">Loading Excel reader…</div>';
      _loadLib('xlsx').then(function(){ if (typeof XLSX !== 'undefined') handleBulkCSV({ target: { files: [file] } }); else document.getElementById('bulk-import-status').innerHTML =
        '<div class="text-danger">Could not load the Excel reader — check your connection and try again, or save the file as CSV.</div>'; });
      event.target.value = ''; // allow re-picking the same file
      return;
    }
    const reader = new FileReader();
    reader.onload = e => {
      try {
        const wb = XLSX.read(e.target.result, { type: 'array' });
        const ws = wb.Sheets[wb.SheetNames[0]]; // use first sheet
        const csv = XLSX.utils.sheet_to_csv(ws);
        processBulkText(csv, file.name);
      } catch(err) {
        document.getElementById('bulk-import-status').innerHTML =
          `<div class="text-danger">❌ Could not read Excel file: ${err.message}</div>`;
      }
    };
    reader.readAsArrayBuffer(file);
  } else {
    // CSV / TSV path
    const reader = new FileReader();
    reader.onload = e => processBulkText(e.target.result, file.name);
    reader.readAsText(file);
  }
  event.target.value = '';
}

function parsePasteData() {
  const text = document.getElementById('paste-data').value.trim();
  if (!text) { toast('Paste some data first'); return; }
  processBulkText(text, 'pasted data');
}

function processBulkText(text, source) {
  const div = document.getElementById('bulk-import-status');
  div.innerHTML = '<div class="import-progress"><b>🔄 Processing…</b></div>';
  const firstLine = text.split('\n')[0];
  const delim = firstLine.includes('\t') ? '\t' : ',';
  const rows = text.trim().split('\n').map(r => r.split(delim).map(c => c.replace(/^"|"$/g,'').trim()));
  if (rows.length < 2) { div.innerHTML = '<div class="text-danger">No data rows found.</div>'; return; }
  const headers = rows[0].map(h => h.toLowerCase().replace(/[^a-z]/g,''));
  const colMap = {
    partno:['partno','partnum','sku','code','itemcode','part_no'],
    name:['name','partname','description','itemname','product'],
    brand:['brand','make','manufacturer'],
    machinetype:['machinetype','machine','type','equipment'],
    category:['category','cat','group'],
    models:['models','compatible','fits'],
    costprice:['costprice','cost','cp','purchaseprice'],
    mrp:['mrp','maxretail','maxprice'],
    instantprice:['instantprice','sellingprice','price','retailprice','ip','rate'],
    creditprice:['creditprice','creditrate','cr'],
    stock:['stock','qty','quantity','available'],
    minstock:['minstock','reorderlevel','minqty','rop'],
    location:['location','rack','racknumber','bin','shelf','loc'],
    suppliername:['suppliername','supplier','vendor'],
    supplierphone:['supplierphone','suppliercontact','vendorphone']
  };
  const cols = {}; Object.keys(colMap).forEach(k => { const aliases=colMap[k]; cols[k]=headers.findIndex(h=>aliases.includes(h)); });
  if (cols.partno===-1 || cols.name===-1) { div.innerHTML=`<div class="text-danger">⚠️ Missing "PartNo" or "Name" columns.<br><small>Found: ${rows[0].join(', ')}</small></div>`; return; }
  let added=0, updated=0, errors=0;
  const total = rows.length-1;
  div.innerHTML = `<div class="import-progress"><b>Processing ${total} parts…</b></div>`;
  // Build a fast lookup map so find() doesn't O(n²) for large imports
  const partsByNo = {};
  state.parts.forEach(function(p){ if(p.partNo) partsByNo[p.partNo] = p; });
  for (let idx = 1; idx < rows.length; idx++) {
    const row = rows[idx];
    const partNo = row[cols.partno]||''; const name = row[cols.name]||'';
    if (!partNo||!name){errors++;continue;}
    const get = (k) => cols[k]>=0 ? row[cols[k]]||'' : '';
    const getNum = (k) => cols[k]>=0 ? (+row[cols[k]]||0) : 0;
    const existing = partsByNo[partNo];
    const ip = getNum('instantprice'); const cp = getNum('costprice');
    const part = { id:existing?existing.id:uid(), partNo, name, brand:get('brand'), machineType:(get('machinetype')||'tractor').toLowerCase(), category:get('category'), models:get('models'), costPrice:cp, mrp:getNum('mrp')||Math.round(ip*1.2), instantPrice:ip, creditPrice:getNum('creditprice')||Math.round(ip*1.05), stock:getNum('stock'), minStock:getNum('minstock'), location:get('location'), supplierName:get('suppliername'), supplierPhone:get('supplierphone') };
    if (existing) { Object.assign(existing, part); partsByNo[partNo]=existing; updated++; } // in-place O(1): avoids indexOf scan per row, and preserves existing photoDataUrl/priceHistory/addedAt
    else { part.addedAt = Date.now(); state.parts.push(part); partsByNo[partNo]=part; added++; }
  }
  // Single save — parts no longer go through per-record RX outbox, so this is just
  // localStorage write + one firebasePush batch commit for all parts at once
  div.innerHTML = `<div class="import-progress"><b>💾 Saving ${added+updated} parts to cloud…</b></div>`;
  save().then(function() {
    div.innerHTML = `<div class="import-progress" style="background:#dcfce7;color:#166534;"><b>✅ Done! Added: ${added} · Updated: ${updated} · Errors: ${errors}</b><br>Total SKUs: <b>${state.parts.length}</b></div>`;
    toast(`✅ ${added+updated} parts saved`);
    renderHome(); renderInventory();
  }).catch(function(e) {
    div.innerHTML = `<div class="import-progress" style="background:#fef2f2;color:#991b1b;"><b>⚠️ Save error — ${e.message}</b></div>`;
  });
}

function downloadTemplate() {
  const csv = 'PartNo,Name,Brand,MachineType,Category,Models,CostPrice,MRP,InstantPrice,CreditPrice,Stock,MinStock,Rack,SupplierName,SupplierPhone\nBRG-6205,Ball Bearing 6205 ZZ,SKF,tractor,Bearing,Universal,85,160,130,140,200,30,A-22,Singh Auto Parts,919876543210';
  const a = document.createElement('a'); a.href = URL.createObjectURL(new Blob([csv],{type:'text/csv'})); a.download='inventory-template.csv'; a.click();
  setTimeout(function(){ URL.revokeObjectURL(a.href); }, 5000); // free blob after download starts
}

function quickAddPart() {
  // Read whatever is pre-filled in the Quick Add card, pass as prefill
  const pno   = (document.getElementById('qa-partno') || {}).value || '';
  const pname = (document.getElementById('qa-name')   || {}).value || '';
  const cost  = (document.getElementById('qa-cost')   || {}).value || '';
  const price = (document.getElementById('qa-price')  || {}).value || '';
  const stock = (document.getElementById('qa-stock')  || {}).value || '';
  const rack  = (document.getElementById('qa-rack')   || {}).value || '';
  const brand = (document.getElementById('qa-brand')  || {}).value || '';
  openAddPart({ context: 'Quick Add', prefill: { partNo:pno, name:pname, unitCost:+cost||0, stock:+stock||0, rack, brand } });
  // Pre-fill after modal opens
  setTimeout(() => {
    if (pno)   { const el=document.getElementById('uap-partNo');   if(el) el.value=pno.toUpperCase(); }
    if (pname) { const el=document.getElementById('uap-name');     if(el) el.value=pname; }
    if (cost)  { const el=document.getElementById('uap-cost');     if(el) el.value=cost; }
    if (price) { const el=document.getElementById('uap-instant');  if(el) el.value=price; }
    if (stock) { const el=document.getElementById('uap-stock');    if(el) el.value=stock; }
    if (rack)  { const el=document.getElementById('uap-loc');      if(el) el.value=rack.toUpperCase(); }
    if (brand) { const el=document.getElementById('uap-brand');    if(el) el.value=brand; }
    uapAutoPrice();
  }, 60);
}

// ============================================================
// RACK FINDER
// ============================================================
function openRackFinder() {
  const racks = [...new Set(state.parts.map(p=>p.location).filter(Boolean))].sort();
  showModal('📍 Rack Finder', `
    <div class="field"><label>Search by Rack or Part Name</label><input class="input" id="rack-search" placeholder="e.g. A-12 or part name" oninput="renderRackResults()"></div>
    <div style="margin-bottom:8px;"><div class="label">Available Racks</div><div class="chips" style="flex-wrap:wrap;">${racks.map(r=>`<div class="chip" onclick="document.getElementById('rack-search').value='${r}';renderRackResults()">${r}</div>`).join('')}</div></div>
    <div id="rack-results"></div>
  `);
}

function renderRackResults() {
  const q = document.getElementById('rack-search').value.toLowerCase().trim();
  const div = document.getElementById('rack-results'); if (!q){div.innerHTML='';return;}
  const found = state.parts.filter(p=>(p.location||'').toLowerCase().includes(q)||p.name.toLowerCase().includes(q)||p.partNo.toLowerCase().includes(q)).slice(0,20);
  if (!found.length){div.innerHTML='<div class="empty">No parts found</div>';return;}
  div.innerHTML = found.map(p=>`<div class="part-item" onclick="closeModal();openPartDetail('${p.id}')"><div class="part-row"><div style="flex:1;"><div class="part-name">${p.name}</div><div class="part-no">${p.partNo} · ${p.brand}</div></div><div style="text-align:right;"><div class="rack-badge">📍 ${p.location||'—'}</div><div style="font-size:12px;margin-top:4px;">${p.stock} pcs · ${rs(p.instantPrice)}</div></div></div></div>`).join('');
}

// ============================================================
// RENDER SOLD BY / DELIVERED BY in sale dropdowns
// ============================================================
// ============================================================
// WHOLESALE ORDERS
// ============================================================
// ── Payment progress bar for credit WOs ──────────────────────────────────
function woPaymentBar(o) {
  if (o.paymentMode !== 'credit') return '';
  const paid = o.paidAmount || 0;
  const total = o.total || 0;
  const due = total - paid;
  const pct = total > 0 ? Math.round((paid / total) * 100) : 0;
  const dueDateObj = o.dueDate ? new Date(o.dueDate) : null;
  const now = new Date();
  const overdueDays = dueDateObj ? Math.floor((now - dueDateObj) / 86400000) : 0;
  const isOverdue = overdueDays > 0;
  const daysLeft = dueDateObj ? Math.ceil((dueDateObj - now) / 86400000) : null;
  const dueTxt = dueDateObj
    ? (isOverdue
        ? `<span style="color:#dc2626;font-weight:700;">⚠ ${overdueDays}d overdue</span>`
        : `Due ${dueDateObj.toLocaleDateString('en-IN',{day:'2-digit',month:'short'})} (${daysLeft}d)`)
    : '';
  const settledTag = o.settled ? '<span style="color:#16a34a;font-weight:700;">✅ Paid</span>' : '';
  return `<div style="margin-top:6px;padding:6px 0;border-top:1px solid var(--border);">
    <div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:3px;">
      <span style="color:var(--muted);">${rs(paid)} paid · ${rs(due)} due ${dueTxt}${settledTag}</span>
      <span style="font-weight:700;color:${due<=0?'#16a34a':'#dc2626'};">${pct}%</span>
    </div>
    <div style="height:4px;background:var(--border);border-radius:2px;overflow:hidden;">
      <div style="height:100%;width:${pct}%;background:${isOverdue?'#ef4444':due<=0?'#16a34a':'#0ea5e9'};border-radius:2px;transition:width .4s;"></div>
    </div>
  </div>`;
}

function renderWholesaleOrders() {
  const filter = state.woFilter || 'all';
  let orders = [...state.wholesaleOrders].sort((a,b) => b.date.localeCompare(a.date));
  if (filter === 'credit_due') {
    orders = orders.filter(o => o.paymentMode === 'credit' && !o.settled);
  } else if (filter === 'overdue') {
    orders = orders.filter(o => o.paymentMode === 'credit' && !o.settled && o.dueDate && new Date(o.dueDate) < new Date());
  } else if (filter === 'previous_bills') {
    orders = orders.filter(o => o.historical);
  } else if (filter !== 'all') {
    orders = orders.filter(o => o.status === filter);
  }
  const div = document.getElementById('wo-list');
  if (!orders.length) { div.innerHTML = '<div class="empty"><div class="icon">📦</div>No orders yet</div>'; return; }

  const dispatchIcon = {bus:'🚌',private_bus:'🚌',courier:'📦',own_vehicle:'🚛',pickup:'🛍'};

  div.innerHTML = orders.map(o => {
    const cust    = getCustomerById(o.customerId);
    const soldBy  = state.staff.find(s => s.id === o.soldById);
    const delivBy = state.staff.find(s => s.id === o.deliveredById);
    const statusClass = {pending:'wo-pending',dispatched:'wo-dispatched',delivered:'wo-delivered',cancelled:'wo-cancelled'}[o.status]||'wo-pending';
    const d = o.dispatch || {};
    const dIcon = d.mode ? (dispatchIcon[d.mode]||'📦') : '';
    const dispatchBadge = d.mode && d.mode!=='pickup'
      ? `<span style="font-size:11px;background:#dbeafe;color:#1e40af;padding:2px 7px;border-radius:5px;font-weight:600;">${dIcon} ${d.busName||d.mode}</span>`
      : (d.mode==='pickup'?'<span style="font-size:11px;background:#dcfce7;color:#166534;padding:2px 7px;border-radius:5px;">🛍 Pickup</span>':'');
    const etaBadge = d.eta && o.status==='dispatched'
      ? `<span style="font-size:11px;background:#fef3c7;color:#92400e;padding:2px 7px;border-radius:5px;margin-left:4px;">ETA ${new Date(d.eta).toLocaleDateString('en-IN')}</span>`
      : '';
    const chargesNote = (o.shippingCharge||o.packagingCharge)
      ? `<small style="color:var(--muted);">incl. ${rs((o.shippingCharge||0)+(o.packagingCharge||0))} charges</small>`
      : '';
    const _gpPh = window.gpPhotosOf ? window.gpPhotosOf(o) : [];
    const hasPhoto = _gpPh.length > 0;
    const photoThumb = hasPhoto
      ? `<div style="position:relative;flex-shrink:0;"><img src="${_gpPh[0]}" onclick="gpViewPhotos('wo','${o.id}',0)" style="width:52px;height:52px;object-fit:cover;border-radius:8px;border:1px solid var(--border);cursor:pointer;">${_gpPh.length>1?`<span style="position:absolute;right:-4px;bottom:-4px;background:#0f766e;color:#fff;font-size:10px;font-weight:700;border-radius:8px;padding:1px 5px;">${_gpPh.length}</span>`:''}</div>`
      : '';
    const historicalBadge = o.historical
      ? `<span style="font-size:11px;background:#fef9c3;color:#92400e;padding:2px 6px;border-radius:4px;font-weight:600;">📜 Previous Bill${o.invoiceNo?' · '+o.invoiceNo:''}</span>`
      : '';

    return `<div class="wo-item">
      <div class="wo-head">
        <div style="display:flex;gap:8px;align-items:flex-start;">
          ${photoThumb}
          <div>
            <div class="bold">WO-${o.woNo} · ${cust?cust.name:'Walk-in'}</div>
            <small>${new Date(o.date).toLocaleString('en-IN')} · ${o.items.length} items${o.paymentMode?' · '+o.paymentMode.toUpperCase():''}</small>
            <div style="margin-top:4px;display:flex;flex-wrap:wrap;gap:4px;align-items:center;">
              ${historicalBadge}
              ${soldBy?`<span style="font-size:11px;background:#f1f5f9;padding:2px 6px;border-radius:4px;">📋 ${soldBy.name}</span>`:''}
              ${delivBy?`<span style="font-size:11px;background:#f1f5f9;padding:2px 6px;border-radius:4px;">🛵 ${delivBy.name}</span>`:''}
              ${dispatchBadge}${etaBadge}
            </div>
          </div>
        </div>
        <div style="text-align:right;">
          <div style="display:flex;align-items:center;justify-content:flex-end;gap:6px;">
            <button onclick="deleteWO('${o.id}')" title="Delete — moves to Recycle Bin" style="background:none;border:none;color:var(--muted);font-size:14px;cursor:pointer;padding:2px;line-height:1;">🗑</button>
            <span class="wo-status ${statusClass}">${o.status.toUpperCase()}</span>
          </div>
          <div class="bold mt-1" style="color:var(--primary);">${rs(o.total)}</div>
          ${chargesNote}
        </div>
      </div>
      <div class="wo-items-list">${o.items.slice(0,3).map(i=>`${i.qty}× ${i.name}`).join(' · ')}${o.items.length>3?` +${o.items.length-3} more`:''}</div>
      <div class="btn-row mt-1">
        <button class="btn btn-sm btn-outline" onclick="openWODetail('${o.id}')">👁 View</button>
        <button class="btn btn-sm btn-outline" onclick="gpOpenBillEditor('wo','${o.id}')">✏️ Edit</button>
        <button class="btn btn-sm btn-outline" onclick="gpPrintBill('wo','${o.id}')">🖨 Print</button>
        ${o.status==='pending'?`<button class="btn btn-sm btn-warn" onclick="openDispatchModal('${o.id}')">🚌 Dispatch</button>`:''}
        ${o.status==='dispatched'?`<button class="btn btn-sm btn-success" onclick="updateWOStatus('${o.id}','delivered')">✅ Mark Delivered</button>`:''}
        ${cust&&cust.phone?`<a class="btn btn-sm" style="background:#25d366;color:white;text-decoration:none;padding:8px 12px;border-radius:10px;" href="https://wa.me/${cust.phone}?text=${encodeURIComponent(buildWOMessage(o))}" target="_blank">📲 WA</a>`:''}
        ${o.paymentMode==='credit'&&!o.settled?`<button class="btn btn-sm btn-success" onclick="openWOPayment('${o.id}')">💰 Collect</button>`:''}
      </div>
      ${o.paymentMode==='credit'?woPaymentBar(o):''}
    </div>`;
  }).join('');
}

function filterWO(f, evt) {
  state.woFilter = f;
  document.querySelectorAll('#wo-chips .chip').forEach(c=>c.classList.remove('active'));
  if (evt&&evt.target) evt.target.classList.add('active');
  renderWholesaleOrders();
  const summary = document.getElementById('wo-credit-summary');
  if (summary) {
    if (f === 'credit_due' || f === 'overdue') {
      const now = Date.now();
      const creditWOs = (state.wholesaleOrders||[]).filter(o=>o.paymentMode==='credit'&&!o.settled);
      const totalDue = creditWOs.reduce((s,o)=>s+(o.total-(o.paidAmount||0)),0);
      const overdue = creditWOs.filter(o=>o.dueDate&&new Date(o.dueDate)<new Date());
      const dueSoon = creditWOs.filter(o=>o.dueDate&&new Date(o.dueDate)>new Date()&&new Date(o.dueDate)<new Date(now+7*86400000));
      summary.style.display='';
      summary.innerHTML=`<div style="display:flex;gap:8px;flex-wrap:wrap;">
        <div style="background:#fef2f2;border:1px solid #fecaca;border-radius:10px;padding:8px 12px;flex:1;min-width:100px;">
          <div style="font-size:11px;color:#991b1b;font-weight:700;">TOTAL DUE</div>
          <div style="font-size:18px;font-weight:800;color:#dc2626;">${rs(totalDue)}</div>
        </div>
        <div style="background:#fef9c3;border:1px solid #fde68a;border-radius:10px;padding:8px 12px;flex:1;min-width:100px;">
          <div style="font-size:11px;color:#92400e;font-weight:700;">OVERDUE</div>
          <div style="font-size:18px;font-weight:800;color:#d97706;">${overdue.length} orders</div>
        </div>
        <div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:10px;padding:8px 12px;flex:1;min-width:100px;">
          <div style="font-size:11px;color:#1e40af;font-weight:700;">DUE THIS WEEK</div>
          <div style="font-size:18px;font-weight:800;color:#1d4ed8;">${dueSoon.length} orders</div>
        </div>
      </div>`;
    } else {
      summary.style.display = 'none';
    }
  }
}

// ============================================================
// DISPATCH MODAL — fill bus details & send WA tracking to buyer
// ============================================================
function openDispatchModal(orderId) {
  const o = state.wholesaleOrders.find(x=>x.id===orderId);
  if (!o) return;
  const cust = getCustomerById(o.customerId);
  const d = o.dispatch || {};

  // Pre-fill ETA = today + 2 days
  const etaDefault = new Date(); etaDefault.setDate(etaDefault.getDate()+2);
  const etaStr = etaDefault.toISOString().slice(0,10);
  const nowStr = (() => { const n=new Date(); n.setMinutes(n.getMinutes()-n.getTimezoneOffset()); return n.toISOString().slice(0,16); })();

  showModal('🚌 Dispatch WO-' + o.woNo, `
    <div style="background:#fef3c7;border-radius:10px;padding:10px;margin-bottom:12px;font-size:13px;">
      <b>${cust?.name||'Customer'}</b> · ${o.items.length} items · ${rs(o.total)}
      ${cust?.phone?`<br>📞 ${cust.phone}`:''}
      ${d.destBusStand?`<br>🚌 Destination: ${d.destBusStand}`:''}
    </div>

    <div class="field"><label>Dispatch Mode *</label>
      <select class="select" id="dm-mode" onchange="dmToggleFields()">
        <option value="bus" ${d.mode==='bus'?'selected':''}>🚌 Local Bus (Roadways)</option>
        <option value="private_bus" ${d.mode==='private_bus'?'selected':''}>🚌 Private Bus / Express</option>
        <option value="courier" ${d.mode==='courier'?'selected':''}>📦 Courier (BlueDart/DTDC/Delhivery)</option>
        <option value="own_vehicle" ${d.mode==='own_vehicle'?'selected':''}>🚛 Our Own Vehicle</option>
        <option value="pickup" ${d.mode==='pickup'?'selected':''}>🛍 Customer Self Pickup</option>
      </select>
    </div>

    <div id="dm-transport-fields">
      <div class="row2">
        <div class="field"><label>Bus / Service Name</label>
          <input class="input" id="dm-busname" value="${d.busName||''}" placeholder="e.g. CG Roadways / Sharma Transport"></div>
        <div class="field"><label>Bus No / Booking / Docket No *</label>
          <input class="input" id="dm-busno" value="${d.busNo||''}" placeholder="e.g. CG-04-AB-1234 or TRK-9876"></div>
      </div>
      <div class="row2">
        <div class="field"><label>Departure Date & Time</label>
          <input type="datetime-local" class="input" id="dm-disptime" value="${d.dispatchTime||nowStr}"></div>
        <div class="field"><label>Expected Delivery Date</label>
          <input type="date" class="input" id="dm-eta" value="${d.eta||etaStr}"></div>
      </div>
      <div class="field"><label>Collect From (Bus Stand / Depot)</label>
        <input class="input" id="dm-busstand" value="${d.destBusStand||cust?.busStand||''}" placeholder="e.g. Raipur Central Bus Stand"></div>
      <div class="field"><label>Contact at Destination</label>
        <input class="input" id="dm-contact" value="${d.recvContact||((cust?.name||'')+' '+(cust?.phone||''))}" placeholder="Name + phone of receiver"></div>
    </div>

    <div id="dm-wa-preview" style="background:#f0fdf4;border-radius:10px;padding:12px;margin-top:10px;font-size:12px;font-family:monospace;white-space:pre-wrap;color:#166534;display:none;max-height:200px;overflow-y:auto;"></div>

    <div class="btn-row mt-2">
      <button class="btn btn-warn" onclick="previewDispatchWA('${orderId}')">👁 Preview WA Message</button>
    </div>
    <div class="btn-row mt-1">
      <button class="btn btn-success" onclick="confirmDispatch('${orderId}')">✅ Confirm Dispatch</button>
      ${cust?.phone?`<button class="btn" style="background:#25d366;" onclick="confirmAndSendWA('${orderId}')">📲 Dispatch & Send WA</button>`:''}
    </div>
  `);
}

function dmToggleFields() {
  const mode = document.getElementById('dm-mode').value;
  const tf = document.getElementById('dm-transport-fields');
  if (tf) tf.style.display = mode === 'pickup' ? 'none' : 'block';
}

function previewDispatchWA(orderId) {
  const o = state.wholesaleOrders.find(x=>x.id===orderId);
  if (!o) return;
  // Build a temp copy with the modal's current values
  const tempO = JSON.parse(JSON.stringify(o));
  tempO.dispatch = {
    mode:         document.getElementById('dm-mode').value,
    busName:      document.getElementById('dm-busname')?.value.trim()||'',
    busNo:        document.getElementById('dm-busno')?.value.trim()||'',
    dispatchTime: document.getElementById('dm-disptime')?.value||'',
    eta:          document.getElementById('dm-eta')?.value||'',
    destBusStand: document.getElementById('dm-busstand')?.value.trim()||'',
    recvContact:  document.getElementById('dm-contact')?.value.trim()||'',
  };
  const msg = buildWOMessage(tempO);
  const prev = document.getElementById('dm-wa-preview');
  prev.style.display = 'block';
  prev.textContent = msg;
}

function confirmDispatch(orderId) {
  const o = state.wholesaleOrders.find(x=>x.id===orderId);
  if (!o) return;
  const mode = document.getElementById('dm-mode').value;
  if (!mode) { toast('Select dispatch mode'); return; }
  if (mode !== 'pickup') {
    const busNo = document.getElementById('dm-busno')?.value.trim();
    if (!busNo) { toast('Enter Bus No / Booking No'); return; }
  }
  o.dispatch = {
    mode,
    busName:      document.getElementById('dm-busname')?.value.trim()||'',
    busNo:        document.getElementById('dm-busno')?.value.trim()||'',
    dispatchTime: document.getElementById('dm-disptime')?.value||'',
    eta:          document.getElementById('dm-eta')?.value||'',
    destBusStand: document.getElementById('dm-busstand')?.value.trim()||'',
    recvContact:  document.getElementById('dm-contact')?.value.trim()||'',
  };
  o.status = 'dispatched';
  o.dispatchedAt = new Date().toISOString();
  save();
  logActivity(`WO-${o.woNo} dispatched via ${mode} — Bus No: ${o.dispatch.busNo}`, 'info');
  closeModal();
  renderWholesaleOrders();
  toast('✅ WO-' + o.woNo + ' marked as Dispatched!');
}

function confirmAndSendWA(orderId) {
  const o = state.wholesaleOrders.find(x=>x.id===orderId);
  const cust = getCustomerById(o?.customerId);
  if (!cust?.phone) { toast('No customer phone'); return; }
  confirmDispatch(orderId);
  // After saving, build and send the message
  const updated = state.wholesaleOrders.find(x=>x.id===orderId);
  if (updated) {
    const msg = buildWOMessage(updated);
    window.open(`https://wa.me/${cust.phone}?text=${encodeURIComponent(msg)}`, '_blank');
  }
}

// ============================================================
// WO DETAIL MODAL — upgraded with charges + dispatch info
// ============================================================
function openWODetail(id) {
  const o = state.wholesaleOrders.find(x=>x.id===id);
  if (!o) return;
  const cust    = getCustomerById(o.customerId);
  const soldBy  = state.staff.find(s=>s.id===o.soldById);
  const delivBy = state.staff.find(s=>s.id===o.deliveredById);
  const d = o.dispatch || {};
  const modeLabel = {bus:'🚌 Local Bus',private_bus:'🚌 Private Bus',courier:'📦 Courier',own_vehicle:'🚛 Own Vehicle',pickup:'🛍 Customer Pickup'}[d.mode]||'';

  const itemsHTML = o.items.map(i=>`<tr><td>${i.name}<br><small style="color:var(--muted);">${i.partNo}</small></td><td style="text-align:center">${i.qty}</td><td style="text-align:right">${rs(i.price)}</td><td style="text-align:right">${rs(i.qty*i.price)}</td></tr>`).join('');

  showModal('WO-'+o.woNo, `
    <div class="flex-between mb-1">
      <div><b>${cust?.name||'—'}</b>${cust?.city?` · ${cust.city}`:''}<br><small>${new Date(o.date).toLocaleString('en-IN')}</small></div>
      <span class="wo-status wo-${o.status}">${o.status.toUpperCase()}</span>
    </div>
    <div style="font-size:12px;color:var(--muted);margin-bottom:8px;">
      ${soldBy?`📋 ${soldBy.name}  `:''}${delivBy?`🛵 ${delivBy.name}  `:''}${o.paymentMode?`💳 ${o.paymentMode.toUpperCase()}`:''}
    </div>

    ${o.historical ? `
    <div style="background:#fef9c3;border:1px solid #fde68a;border-radius:10px;padding:10px 12px;margin-bottom:10px;font-size:13px;">
      <div style="font-weight:700;color:#92400e;">📜 Previous Bill — manually recorded</div>
      ${o.invoiceNo?`<div style="margin-top:2px;">Bill / Invoice No: <b>${o.invoiceNo}</b></div>`:''}
    </div>` : ''}
    ${o.invoiceNo && !o.historical ? `<div style="font-size:13px;margin-bottom:6px;">Bill / Invoice No: <b>${o.invoiceNo}</b></div>` : ''}
    ${window.gpPhotoStripHtml ? window.gpPhotoStripHtml(o, 'wo') : ''}

    <table style="width:100%;font-size:13px;border-collapse:collapse;">
      <thead><tr style="background:#f1f5f9;"><th align="left" style="padding:6px 4px;">Item</th><th style="padding:6px 4px;">Qty</th><th align="right" style="padding:6px 4px;">Rate</th><th align="right" style="padding:6px 4px;">Amt</th></tr></thead>
      <tbody>${itemsHTML}</tbody>
    </table>

    <div class="divider"></div>
    <div class="flex-between" style="font-size:13px;"><span>Parts Subtotal</span><span>${rs(o.subtotal)}</span></div>
    ${o.discount?`<div class="flex-between" style="font-size:13px;color:var(--danger);"><span>Discount</span><span>−${rs(o.discount)}</span></div>`:''}
    ${o.shippingCharge?`<div class="flex-between" style="font-size:13px;"><span>🚌 Shipping / Freight</span><span>+${rs(o.shippingCharge)}</span></div>`:''}
    ${o.packagingCharge?`<div class="flex-between" style="font-size:13px;"><span>📦 Packaging</span><span>+${rs(o.packagingCharge)}</span></div>`:''}
    <div class="flex-between bold" style="font-size:18px;color:var(--primary);padding:6px 0;border-top:2px solid var(--border);margin-top:6px;"><span>TOTAL</span><span>${rs(o.total)}</span></div>

    ${d.mode ? `
    <div style="background:#eff6ff;border-radius:10px;padding:12px;margin-top:10px;font-size:13px;">
      <div style="font-weight:600;margin-bottom:6px;">🚌 Dispatch Info</div>
      ${modeLabel?`<div>Mode: <b>${modeLabel}</b></div>`:''}
      ${d.busName?`<div>Bus/Service: <b>${d.busName}</b></div>`:''}
      ${d.busNo?`<div>Bus No / Booking: <b>${d.busNo}</b></div>`:''}
      ${d.dispatchTime?`<div>Dispatched: ${new Date(d.dispatchTime).toLocaleString('en-IN')}</div>`:''}
      ${d.destBusStand?`<div>Collect From: <b>${d.destBusStand}</b></div>`:''}
      ${d.eta?`<div>Expected by: <b>${new Date(d.eta).toLocaleDateString('en-IN')}</b></div>`:''}
      ${d.recvContact?`<div>Receiver: ${d.recvContact}</div>`:''}
    </div>` : ''}

    ${o.notes?`<div class="mt-1" style="font-size:13px;"><b>Notes:</b> ${o.notes}</div>`:''}

    ${o.paymentMode==='credit'?`
    <div style="background:${o.settled?'#f0fdf4':'#fef2f2'};border-radius:10px;padding:12px;margin-top:10px;">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
        <div style="font-weight:700;font-size:14px;">${o.settled?'✅ Fully Paid':'💰 Credit Payment'}</div>
        <div style="font-size:13px;color:${o.settled?'#16a34a':'#dc2626'};font-weight:700;">${o.settled?rs(o.total):('Due: '+rs(o.total-(o.paidAmount||0)))}</div>
      </div>
      ${(o.payments&&o.payments.length)?`<div style="font-size:11px;font-weight:600;color:var(--muted);margin-bottom:6px;">PAYMENT HISTORY</div>
      ${o.payments.map(py=>`<div style="display:flex;justify-content:space-between;font-size:12px;padding:4px 0;border-bottom:1px solid rgba(0,0,0,.06);">
        <span>${new Date(py.date).toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'2-digit'})} · ${py.mode||'cash'}${py.ref?' · '+py.ref:''}</span>
        <span style="font-weight:700;color:var(--success);">${rs(py.amount)}</span>
      </div>`).join('')}`:'<div style="font-size:12px;color:var(--muted);">No payments recorded yet.</div>'}
      ${!o.settled?`<button class="btn btn-success mt-1" style="width:100%;" onclick="closeModal();openWOPayment('${o.id}')">💰 Record Payment</button>`:''}
    </div>`:''}
    <div class="btn-row mt-2">
      <button class="btn" onclick="closeModal();gpOpenBillEditor('wo','${o.id}')">✏️ Edit Bill</button>
      <button class="btn btn-outline" onclick="gpPrintBill('wo','${o.id}')">🖨️ Print</button>
    </div>
    <div class="btn-row mt-1">
      ${o.status==='pending'?`<button class="btn btn-warn" onclick="closeModal();openDispatchModal('${o.id}')">🚌 Dispatch Now</button>`:''}
      ${o.status==='dispatched'?`<button class="btn btn-success" onclick="closeModal();updateWOStatus('${o.id}','delivered')">✅ Mark Delivered</button>`:''}
      ${o.status!=='cancelled'&&o.status!=='delivered'?`<button class="btn btn-danger" onclick="closeModal();updateWOStatus('${o.id}','cancelled')">Cancel</button>`:''}
    </div>
    ${cust?.phone?`<a class="wa-btn mt-1" href="https://wa.me/${cust.phone}?text=${encodeURIComponent(buildWOMessage(o))}" target="_blank">📲 Send Update on WhatsApp</a>`:''}
    <button class="btn btn-outline mt-1" style="color:var(--danger);border-color:#fecaca;" onclick="deleteWO('${o.id}')">🗑 Delete — Move to Recycle Bin</button>
  `);
}

// ============================================================
// RECYCLE BIN — soft-delete for Wholesale Orders & Purchase Bills.
// Deleted records move here for 30 days (restorable) before being
// permanently purged.
// ============================================================
function pushToRecycleBin(type, record, label) {
  if (!state.recycleBin) state.recycleBin = [];
  state.recycleBin.push({
    id: uid(),
    type: type,               // 'wholesaleOrder' | 'purchase'
    label: label,
    record: JSON.parse(JSON.stringify(record)),
    deletedAt: new Date().toISOString()
  });
}

// Permanently purges anything that's sat in the bin for 30+ days
function purgeOldRecycleBinEntries() {
  if (!state.recycleBin || !state.recycleBin.length) return;
  var cutoff = Date.now() - 30*86400000;
  var kept = [], expired = [];
  state.recycleBin.forEach(function(e) {
    if (new Date(e.deletedAt).getTime() > cutoff) kept.push(e); else expired.push(e);
  });
  if (!expired.length) return;
  expired.forEach(function(e) {
    if (e.type === 'purchase') purgePurchaseForever(e.record);
    else if (e.type === 'wholesaleOrder') purgeWOForever(e.record);
  });
  state.recycleBin = kept;
  save();
}

// Soft-delete a wholesale order (incl. Previous Bills) — moves it to the Recycle Bin
function deleteWO(id) {
  var o = state.wholesaleOrders.find(function(x){ return x.id===id; });
  if (!o) return;
  var cust = getCustomerById(o.customerId);
  var label = 'WO-' + o.woNo + ' · ' + (cust?cust.name:'Walk-in') + ' · ' + rs(o.total);
  if (!confirm('Delete WO-' + o.woNo + '?\n\n' + label + '\n\nIt will be moved to the Recycle Bin for 30 days — you can restore it if this was a mistake.')) return;
  if (o.paymentMode === 'credit' && !o.settled && cust) {
    cust.totalDue = Math.max(0, (cust.totalDue||0) - (o.total - (o.paidAmount||0)));
  }
  pushToRecycleBin('wholesaleOrder', o, label);
  state.wholesaleOrders = state.wholesaleOrders.filter(function(x){ return x.id !== id; });
  save();
  renderWholesaleOrders();
  closeModal();
  logActivity('WO moved to recycle bin: ' + label, 'warn');
  toast('🗑 Moved to Recycle Bin — WO-' + o.woNo);
}

// Puts a wholesale order back from the Recycle Bin and re-applies its customer due
function restoreWOFromBin(o) {
  state.wholesaleOrders.push(o);
  var cust = getCustomerById(o.customerId);
  if (o.paymentMode === 'credit' && !o.settled && cust) {
    cust.totalDue = (cust.totalDue||0) + (o.total - (o.paidAmount||0));
  }
  renderWholesaleOrders();
}

// Irreversible — called only when a Recycle Bin entry is emptied (by hand or after 30 days)
function purgeWOForever(o) {
  if (o.photoDataUrl && o.photoDataUrl.startsWith('https://') && window.firebaseDeleteWOPhoto) {
    window.firebaseDeleteWOPhoto(o.id).catch(function(){});
  }
}

function openRecycleBinModal() {
  purgeOldRecycleBinEntries();
  showModal('🗑 Recycle Bin', `
    <div style="font-size:12px;color:var(--muted);margin-bottom:10px;">Deleted wholesale orders & purchase bills stay here for 30 days before being permanently removed.</div>
    <div id="recycle-bin-list">${renderRecycleBinList()}</div>
  `);
}

function renderRecycleBinList() {
  var entries = (state.recycleBin||[]).slice().sort(function(a,b){ return b.deletedAt.localeCompare(a.deletedAt); });
  if (!entries.length) return '<div class="empty"><div class="icon">🗑</div>Recycle Bin is empty</div>';
  return entries.map(function(e) {
    var icon = e.type === 'purchase' ? '📥' : '🏭';
    var typeLbl = e.type === 'purchase' ? 'Purchase Bill' : 'Wholesale Order';
    var daysLeft = Math.max(0, 30 - Math.floor((Date.now() - new Date(e.deletedAt).getTime())/86400000));
    return `<div style="display:flex;justify-content:space-between;align-items:center;gap:8px;padding:10px 0;border-bottom:1px solid var(--border);">
      <div style="min-width:0;flex:1;">
        <div style="font-size:11px;color:var(--muted);font-weight:600;text-transform:uppercase;">${icon} ${typeLbl}</div>
        <div style="font-weight:600;font-size:13px;">${e.label}</div>
        <div style="font-size:11px;color:var(--muted);">Deleted ${new Date(e.deletedAt).toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'2-digit'})} · ${daysLeft}d left</div>
      </div>
      <div style="display:flex;gap:6px;flex-shrink:0;">
        <button class="btn btn-sm btn-success" style="width:auto;" onclick="restoreRecycleBinEntry('${e.id}')">↩ Restore</button>
        <button class="btn btn-sm btn-danger" style="width:auto;" onclick="purgeRecycleBinEntry('${e.id}')">Delete Forever</button>
      </div>
    </div>`;
  }).join('');
}

function restoreRecycleBinEntry(binId) {
  var entry = (state.recycleBin||[]).find(function(x){ return x.id===binId; });
  if (!entry) return;
  if (entry.type === 'wholesaleOrder') restoreWOFromBin(entry.record);
  else if (entry.type === 'purchase') restorePurchaseFromBin(entry.record);
  state.recycleBin = state.recycleBin.filter(function(x){ return x.id !== binId; });
  save();
  var listDiv = document.getElementById('recycle-bin-list');
  if (listDiv) listDiv.innerHTML = renderRecycleBinList();
  logActivity('Restored from recycle bin: ' + entry.label, 'info');
  toast('↩️ Restored: ' + entry.label);
}

function purgeRecycleBinEntry(binId) {
  var entry = (state.recycleBin||[]).find(function(x){ return x.id===binId; });
  if (!entry) return;
  if (!confirm('Permanently delete this? This cannot be undone.\n\n' + entry.label)) return;
  if (entry.type === 'purchase') purgePurchaseForever(entry.record);
  else if (entry.type === 'wholesaleOrder') purgeWOForever(entry.record);
  state.recycleBin = state.recycleBin.filter(function(x){ return x.id !== binId; });
  save();
  var listDiv = document.getElementById('recycle-bin-list');
  if (listDiv) listDiv.innerHTML = renderRecycleBinList();
  toast('Permanently deleted');
}

function buildWOMessage(o) {
  const cust = getCustomerById(o.customerId);
  const d = o.dispatch || {};
  const modeLabel = {
    bus: '🚌 Local Bus', private_bus: '🚌 Express Bus',
    courier: '📦 Courier', own_vehicle: '🚛 Own Vehicle', pickup: '🛍 Self Pickup'
  }[d.mode] || '';

  let msg = `*${state.settings.shopName}*\n`;
  msg += `Order: *WO-${o.woNo}*\n`;
  msg += `Date: ${new Date(o.date).toLocaleString('en-IN')}\n`;
  if (cust) msg += `Customer: ${cust.name}\n`;
  msg += `\n*Items:*\n`;
  o.items.forEach(i => msg += `▸ ${i.name} (${i.partNo})\n  ${i.qty} × ${rs(i.price)} = ${rs(i.qty*i.price)}\n`);
  msg += `\n`;
  if (o.subtotal !== o.total) {
    msg += `Subtotal: ${rs(o.subtotal)}\n`;
    if (o.discount)         msg += `Discount: −${rs(o.discount)}\n`;
    if (o.shippingCharge)   msg += `Shipping: +${rs(o.shippingCharge)}\n`;
    if (o.packagingCharge)  msg += `Packaging: +${rs(o.packagingCharge)}\n`;
  }
  msg += `*Total: ${rs(o.total)}*\n`;
  if (o.paymentMode) msg += `Payment: ${o.paymentMode.toUpperCase()}\n`;

  // Dispatch / tracking info
  if (d.mode && d.mode !== 'pickup') {
    msg += `\n*📦 Dispatch Details:*\n`;
    if (modeLabel)      msg += `Mode: ${modeLabel}\n`;
    if (d.busName)      msg += `Bus/Service: ${d.busName}\n`;
    if (d.busNo)        msg += `Bus/Booking No: *${d.busNo}*\n`;
    if (d.dispatchTime) msg += `Dispatched: ${new Date(d.dispatchTime).toLocaleString('en-IN')}\n`;
    if (d.destBusStand) msg += `Collect from: *${d.destBusStand}*\n`;
    if (d.eta)          msg += `Expected by: ${new Date(d.eta).toLocaleDateString('en-IN')}\n`;
    if (d.recvContact)  msg += `Contact: ${d.recvContact}\n`;
    msg += `\nPlease collect from the above bus stand on the expected date. 📞 Call us if parcel doesn't arrive on time.`;
  } else if (d.mode === 'pickup') {
    msg += `\n✅ Your order is ready for pickup at our shop.`;
  }

  if (o.notes) msg += `\n\nNote: ${o.notes}`;
  if (state.settings.shopPhone) msg += `\n\n📞 ${state.settings.shopName}: ${state.settings.shopPhone}`;
  return msg;
}

// ============================================================
// ADD NEW WHOLESALE CUSTOMER (from within Wholesale tab)
// ============================================================
function openAddWholesaleCustomer() {
  showModal('+ New Wholesale Customer', `
    <div class="field"><label>Customer / Shop Name *</label><input class="input" id="wc-name" placeholder="e.g. Krishi Seva Kendra, Jabalpur"></div>
    <div class="row2">
      <div class="field"><label>Phone (with country code)</label><input class="input" id="wc-phone" placeholder="919876543210" inputmode="numeric"></div>
      <div class="field"><label>Credit Days</label>
        <select class="select" id="wc-credit">
          <option value="0">Cash only (no credit)</option>
          <option value="7">7 days</option>
          <option value="15" selected>15 days</option>
          <option value="30">30 days</option>
          <option value="45">45 days</option>
          <option value="60">60 days</option>
        </select>
      </div>
      <div class="field"><label>Credit Limit ₹</label>
        <input type="number" class="input" id="wc-limit" placeholder="0 = no limit" value="0">
        <small style="color:var(--muted);font-size:11px;">System warns if order would exceed this</small>
      </div>
    </div>
    <div class="row2">
      <div class="field"><label>City / District</label><input class="input" id="wc-city" placeholder="e.g. Raipur"></div>
      <div class="field"><label>State</label><input class="input" id="wc-state" placeholder="e.g. Chhattisgarh"></div>
    </div>
    <div class="field"><label>Full Address (for dispatching orders)</label><textarea class="input" id="wc-address" rows="2" placeholder="Shop/village address for bus or courier dispatch…"></textarea></div>
    <div class="field"><label>Nearest Bus Stand / Depot</label><input class="input" id="wc-busstand" placeholder="e.g. Raipur Central Bus Stand"></div>
    <div class="field"><label>Notes</label><input class="input" id="wc-notes" placeholder="e.g. Owns 5 tractors, buys monthly"></div>
    <div style="background:#f0fdf4;border-radius:10px;padding:10px;margin-bottom:10px;">
      <div style="font-size:12px;font-weight:600;color:#166534;margin-bottom:4px;">✅ Trust Status</div>
      <label style="font-size:13px;display:flex;align-items:center;gap:8px;cursor:pointer;">
        <input type="checkbox" id="wc-trusted" checked> Mark as Trusted — eligible for credit price & credit sales
      </label>
    </div>
    <button class="btn btn-success" onclick="saveWholesaleCustomer()">✓ Add to Customer Base</button>
  `);
}

async function saveWholesaleCustomer() {
  const name = document.getElementById('wc-name').value.trim();
  if (!name) { toast('Customer name required'); return; }
  const phone = document.getElementById('wc-phone').value.trim();
  const city   = document.getElementById('wc-city').value.trim();
  const state_ = document.getElementById('wc-state').value.trim();
  const address   = document.getElementById('wc-address').value.trim();
  const busStand  = document.getElementById('wc-busstand').value.trim();
  const creditDays = +document.getElementById('wc-credit').value;
  const creditLimit = +(document.getElementById('wc-limit')||{value:0}).value || 0;
  const trusted    = document.getElementById('wc-trusted').checked;
  const notes      = document.getElementById('wc-notes').value.trim();

  const cust = {
    id: uid(), name, phone,
    type: 'wholesale', trusted,
    creditDays, creditLimit, totalDue: 0,
    city, state: state_,
    address, busStand, notes,
    addedAt: new Date().toISOString()
  };
  state.customers.push(cust);
  invalidateCustomerIndex();
  save();
  closeModal();
  logActivity(`New wholesale customer added: ${name}`, 'info');
  toast(`✅ ${name} added to customer base!`);
  // refresh the WO customer dropdown if modal is re-opened
}

// ============================================================
// PREVIOUS BILL — log a wholesale customer's old/back-dated bill
// (paper bill from before this system was used, or one you just
// want on record) with a photo of the bill attached.
// ============================================================
function openAddPreviousBill() {
  const wsCusts = state.customers.filter(c=>c.type==='wholesale');
  state.pwbCart = [];
  showModal('📜 Log Previous Wholesale Bill', `
    <div style="background:#fef9c3;border-radius:10px;padding:10px 12px;margin-bottom:12px;font-size:12px;color:#92400e;">
      Use this to record a wholesale bill you already gave a customer — for a back-dated entry, an old paper bill, or one just to keep on file. It won't touch current stock.
    </div>

    <div class="field">
      <label>Wholesale Customer *</label>
      <select class="select" id="pwb-cust">
        <option value="">— select wholesale customer —</option>
        ${wsCusts.map(c=>`<option value="${c.id}">${c.name}${c.city?' — '+c.city:''}</option>`).join('')}
      </select>
      <small style="margin-top:5px;display:block;">
        <a href="#" onclick="event.preventDefault();closeModal();openAddWholesaleCustomer();" style="color:#0369a1;">+ Add new customer first</a>
      </small>
    </div>

    <div class="row2">
      <div class="field"><label>Bill Date *</label><input type="date" class="input" id="pwb-date" value="${new Date().toISOString().slice(0,10)}"></div>
      <div class="field"><label>Bill / Invoice No</label><input class="input" id="pwb-inv" placeholder="Optional"></div>
    </div>

    <!-- ITEMS PURCHASED -->
    <div style="background:#f8fafc;border-radius:10px;padding:12px;margin-bottom:12px;">
      <div style="font-size:13px;font-weight:600;margin-bottom:8px;">📦 Items Purchased <span style="color:var(--muted);font-weight:400;font-size:11px;">(what this customer bought — optional but recommended)</span></div>
      <div class="field" style="margin-bottom:6px;">
        <input class="input" id="pwb-search" placeholder="Search part no or name to add…" oninput="renderPWBSearch()">
      </div>
      <div id="pwb-search-results"></div>
      <div id="pwb-cart-items" class="mt-1"></div>
      <button type="button" class="btn btn-outline btn-sm mt-1" onclick="pwbToggleCustomForm()">✎ Add item not in inventory</button>
      <div id="pwb-custom-form" style="display:none;margin-top:8px;background:#fff;border:1px solid var(--border);border-radius:8px;padding:10px;">
        <div class="field" style="margin-bottom:6px;"><input class="input" id="pwb-custom-name" placeholder="Item name / description"></div>
        <div class="row2" style="margin-bottom:6px;">
          <input type="number" class="input" id="pwb-custom-qty" placeholder="Qty" value="1" min="0.01" step="0.01">
          <input type="number" class="input" id="pwb-custom-price" placeholder="Rate ₹" min="0">
        </div>
        <button type="button" class="btn btn-sm" style="width:100%;" onclick="pwbAddCustomItem()">+ Add Item</button>
      </div>
    </div>

    <div class="field"><label>Other / Unlisted Amount (₹) <span style="color:var(--muted);font-weight:400;font-size:12px;">(anything not listed above — or the full bill if you'd rather skip items)</span></label><input type="number" class="input" id="pwb-other" placeholder="0" oninput="renderPWBCart()"></div>

    <div id="pwb-total-row" style="font-size:17px;font-weight:700;color:var(--primary);margin:4px 0 12px;padding:10px;background:#f0fdf4;border-radius:10px;"></div>

    <div class="field"><label>Payment Status</label>
      <select class="select" id="pwb-paystatus" onchange="pwbTogglePayFields()">
        <option value="credit">Credit / Unpaid</option>
        <option value="partial">Partially Paid</option>
        <option value="paid">Paid in Full</option>
      </select>
    </div>

    <div id="pwb-paid-wrap" style="display:none;">
      <div class="row2">
        <div class="field"><label>Amount Received (₹)</label><input type="number" class="input" id="pwb-paid" placeholder="0"></div>
        <div class="field"><label>Received Via</label>
          <select class="select" id="pwb-mode"><option value="cash">Cash</option><option value="upi">UPI</option><option value="bank">Bank Transfer</option><option value="cheque">Cheque</option></select>
        </div>
      </div>
    </div>

    <div id="pwb-due-wrap" class="field">
      <label>Due Date <span style="color:var(--muted);font-weight:400;font-size:12px;">(optional)</span></label>
      <input type="date" class="input" id="pwb-duedate">
    </div>

    <div class="field"><label>Notes</label><textarea class="input" id="pwb-notes" rows="2" placeholder="Optional — items sold, reason for late entry, etc."></textarea></div>

    <div class="field"><label>📷 Bill Photo <span style="color:var(--muted);font-weight:400;font-size:12px;">(optional but recommended)</span></label>
      <div style="display:flex;align-items:center;gap:10px;margin-top:6px;">
        <input type="file" id="pwb-photo-input" accept="image/*" capture="environment" style="display:none;" onchange="pwbHandlePhoto(this)">
        <button type="button" class="btn btn-outline btn-sm" onclick="document.getElementById('pwb-photo-input').click()" style="white-space:nowrap;">📷 Take / Upload Photo</button>
        <span id="pwb-photo-name" style="font-size:12px;color:var(--muted);"></span>
      </div>
      <div id="pwb-photo-preview-wrap" style="display:none;margin-top:8px;">
        <img id="pwb-photo-preview" style="width:120px;height:120px;object-fit:cover;border-radius:8px;border:1px solid var(--border);">
      </div>
    </div>
    <input type="hidden" id="pwb-photo-data" value="">

    <button class="btn btn-success" onclick="savePreviousWholesaleBill()">✓ Save Previous Bill</button>
  `);
  pwbTogglePayFields();
  renderPWBCart();
}

function renderPWBSearch() {
  const q = (document.getElementById('pwb-search').value||'').toLowerCase().trim();
  const div = document.getElementById('pwb-search-results'); if(!div) return;
  if (!q) { div.innerHTML=''; return; }
  const res = state.parts.filter(p=>p.partNo.toLowerCase().includes(q)||p.name.toLowerCase().includes(q)).slice(0,8);
  div.innerHTML = res.map(p=>`<div class="part-item" onclick="addToPWBCart('${p.id}')">
    <div class="part-row"><div><div class="part-name">${p.name}</div><div class="part-no">${p.partNo} · Stock: ${p.stock}</div></div>
    <div class="part-prices"><div class="price-instant">${rs(p.creditPrice||p.instantPrice)}</div><small>Tap to add</small></div></div></div>`).join('');
}

function addToPWBCart(partId) {
  const p = getPartById(partId); if (!p) return;
  const existing = (state.pwbCart||[]).find(i=>i.partId===partId);
  if (existing) { existing.qty++; }
  else { state.pwbCart = state.pwbCart||[]; state.pwbCart.push({ partId: p.id, partNo: p.partNo, name: p.name, qty: 1, price: p.creditPrice||p.instantPrice||0 }); }
  document.getElementById('pwb-search').value='';
  document.getElementById('pwb-search-results').innerHTML='';
  renderPWBCart();
}

function pwbToggleCustomForm() {
  const f = document.getElementById('pwb-custom-form');
  if (f) f.style.display = f.style.display==='none' ? '' : 'none';
}

function pwbAddCustomItem() {
  const name = document.getElementById('pwb-custom-name').value.trim();
  const qty  = parseFloat(document.getElementById('pwb-custom-qty').value) || 0;
  const price= parseFloat(document.getElementById('pwb-custom-price').value) || 0;
  if (!name) { toast('Enter an item name'); return; }
  if (!qty)  { toast('Enter a quantity'); return; }
  state.pwbCart = state.pwbCart||[];
  state.pwbCart.push({ partId: null, partNo: '', name, qty, price });
  document.getElementById('pwb-custom-name').value = '';
  document.getElementById('pwb-custom-qty').value = '1';
  document.getElementById('pwb-custom-price').value = '';
  pwbToggleCustomForm();
  renderPWBCart();
}

function pwbSetQty(i, val) {
  const cart = state.pwbCart||[]; if (!cart[i]) return;
  const q = parseFloat(val) || 0;
  if (q <= 0) { cart.splice(i,1); } else { cart[i].qty = q; }
  renderPWBCart();
}

function pwbSetPrice(i, val) {
  const cart = state.pwbCart||[]; if (!cart[i]) return;
  cart[i].price = parseFloat(val) || 0;
  renderPWBCart();
}

function removePWBItem(i) {
  const cart = state.pwbCart||[]; if (!cart[i]) return;
  cart.splice(i,1);
  renderPWBCart();
}

function renderPWBCart() {
  const cart = state.pwbCart||[];
  const div = document.getElementById('pwb-cart-items');
  if (div) {
    div.innerHTML = !cart.length
      ? '<div style="color:var(--muted);font-size:13px;padding:6px 0;">No items added yet — search above, or add a custom item</div>'
      : cart.map((item,i) => `<div style="display:flex;gap:6px;align-items:center;padding:7px 0;border-bottom:1px solid var(--border);">
          <div style="flex:1;min-width:0;">
            <div style="font-weight:600;font-size:13px;">${item.name}</div>
            <div style="font-size:11px;color:var(--muted);">${item.partNo || 'Custom item'}</div>
            ${item.partId?`<button class="gp-rate-chip" onclick="gpOpenRateEditor('pwb',${i})">✎ H/C/R</button>`:''}
          </div>
          <input type="number" value="${item.qty}" min="0.01" step="0.01" style="width:52px;padding:5px 4px;border:1px solid var(--border);border-radius:6px;font-size:13px;text-align:center;" onchange="pwbSetQty(${i},this.value)">
          <span style="color:var(--muted);">×</span>
          <input type="number" value="${item.price}" min="0" style="width:70px;padding:5px 4px;border:1px solid var(--border);border-radius:6px;font-size:13px;text-align:right;" onchange="pwbSetPrice(${i},this.value)">
          <div style="min-width:66px;text-align:right;font-weight:700;font-size:13px;">${rs(item.qty*item.price)}</div>
          <button type="button" onclick="removePWBItem(${i})" style="background:none;border:none;color:var(--danger);font-size:16px;cursor:pointer;padding:0 2px;">✕</button>
        </div>`).join('');
  }

  const itemsTotal = cart.reduce((s,i)=>s+(i.qty*i.price),0);
  const other = parseFloat(document.getElementById('pwb-other')?.value) || 0;
  const grandTotal = itemsTotal + other;
  const totalRow = document.getElementById('pwb-total-row');
  if (totalRow) {
    totalRow.innerHTML = `
      <div style="font-size:13px;color:var(--muted);font-weight:400;">
        ${cart.length?`<div class="flex-between" style="margin-bottom:3px;"><span>Items Total</span><span>${rs(itemsTotal)}</span></div>`:''}
        ${other>0?`<div class="flex-between" style="margin-bottom:3px;"><span>Other / Unlisted</span><span>${rs(other)}</span></div>`:''}
      </div>
      <div class="flex-between" style="margin-top:${cart.length||other>0?'6px':'0'};padding-top:${cart.length||other>0?'6px':'0'};border-top:${cart.length||other>0?'1px solid var(--border)':'none'};">
        <span>BILL TOTAL</span><span>${rs(grandTotal)}</span>
      </div>`;
  }
}

function pwbTogglePayFields() {
  const status = document.getElementById('pwb-paystatus').value;
  const paidWrap = document.getElementById('pwb-paid-wrap');
  const dueWrap  = document.getElementById('pwb-due-wrap');
  if (paidWrap) paidWrap.style.display = status === 'partial' ? '' : 'none';
  if (dueWrap)  dueWrap.style.display  = status === 'paid' ? 'none' : '';
}

function pwbHandlePhoto(input) {
  if (!input.files || !input.files[0]) return;
  const file = input.files[0];
  const nameEl = document.getElementById('pwb-photo-name');
  if (nameEl) nameEl.textContent = file.name;
  const reader = new FileReader();
  reader.onload = function(e) {
    const dataUrl = e.target.result;
    const hidden = document.getElementById('pwb-photo-data');
    if (hidden) hidden.value = dataUrl;
    const preview = document.getElementById('pwb-photo-preview');
    const wrap = document.getElementById('pwb-photo-preview-wrap');
    if (preview) preview.src = dataUrl;
    if (wrap) wrap.style.display = '';
  };
  reader.readAsDataURL(file);
}

async function savePreviousWholesaleBill() {
  if (!(await rxGuardSale('Previous Bill'))) return;
  const custId = document.getElementById('pwb-cust').value;
  if (!custId) { toast('Select a customer'); return; }
  const cust = getCustomerById(custId);

  const cart = state.pwbCart||[];
  const itemsTotal = cart.reduce((s,i)=>s+(i.qty*i.price),0);
  const other = parseFloat(document.getElementById('pwb-other').value) || 0;
  const amt = itemsTotal + other;
  if (!amt) { toast('Add at least one item, or enter an amount'); return; }
  const items = cart.map(i => ({ partId: i.partId, partNo: i.partNo, name: i.name, qty: i.qty, price: i.price }));
  if (other > 0) items.push({ partId: null, partNo: '', name: cart.length ? 'Other / unlisted items' : 'Previous Bill (manual entry)', qty: 1, price: other });

  const dateStr = document.getElementById('pwb-date').value || new Date().toISOString().slice(0,10);
  const invNo   = document.getElementById('pwb-inv').value.trim();
  const notes   = document.getElementById('pwb-notes').value.trim();
  const payStatus = document.getElementById('pwb-paystatus').value;
  const mode      = document.getElementById('pwb-mode')?.value || 'cash';
  const paidInput = parseFloat(document.getElementById('pwb-paid')?.value) || 0;
  const dueDateInput = document.getElementById('pwb-duedate')?.value || '';
  const photoDataUrl = (document.getElementById('pwb-photo-data')||{}).value || null;

  const paid = payStatus === 'paid' ? amt : payStatus === 'partial' ? Math.min(paidInput, amt) : 0;
  const settled = paid >= amt;

  const wo = {
    id: uid(), woNo: state.settings.nextWO++,
    date: dateStr + 'T' + new Date().toTimeString().slice(0,8),
    customerId: custId,
    soldById: '', deliveredById: '',
    items,
    subtotal: amt, discount: 0, shippingCharge: 0, packagingCharge: 0, total: amt,
    paymentMode: settled ? mode : 'credit',
    settled,
    paidAmount: paid,
    payments: paid > 0 ? [{ date: new Date().toISOString(), amount: paid, mode, ref: '' }] : [],
    dueDate: !settled ? (dueDateInput || null) : null,
    notes,
    invoiceNo: invNo,
    dispatch: {},
    status: 'delivered',
    stockDeducted: true,
    historical: true,
    photoDataUrl: photoDataUrl || null
  };

  if (!settled && cust) {
    cust.totalDue = (cust.totalDue || 0) + (amt - paid);
  }

  state.wholesaleOrders.push(wo);
  state.pwbCart = [];
  await save();
  closeModal();
  renderWholesaleOrders();
  rxConfirmRecord('wholesaleOrders', wo.id, 12000).then(_ss => {
    if (!_ss.ok) {
      RX.log('warn','wo','Previous bill WO-' + wo.woNo + ' queued but not yet confirmed (' + _ss.reason + ')');
      toast('⚠️ Bill saved locally, syncing in background…');
    }
  });
  logActivity(`Previous bill logged: WO-${wo.woNo} for ${cust?cust.name:'—'} — ${rs(amt)}`, 'info');
  toast('✅ Previous bill saved — WO-' + wo.woNo);

  if (wo.photoDataUrl && wo.photoDataUrl.startsWith('data:') && window.firebaseUploadWOPhoto) {
    const _wid = wo.id, _durl = wo.photoDataUrl;
    window.firebaseUploadWOPhoto(_wid, _durl)
      .then(function(url) {
        const w2 = (state.wholesaleOrders||[]).find(function(x){ return x.id === _wid; });
        if (w2) { w2.photoDataUrl = url; save(); renderWholesaleOrders(); }
        toast('✅ Bill photo saved to cloud');
      })
      .catch(function(err) {
        console.warn('WO bill photo upload failed:', err.message);
        toast('Photo saved locally (cloud sync pending)');
      });
  }
}

function viewWOBillPhoto(woId) {
  const o = (state.wholesaleOrders||[]).find(x=>x.id===woId);
  if (!o || !o.photoDataUrl) return;
  const cust = getCustomerById(o.customerId);
  showModal('📷 Bill — ' + (cust?cust.name:'WO-'+o.woNo),
    '<img src="' + o.photoDataUrl + '" style="width:100%;border-radius:8px;border:1px solid var(--border);">'
    + '<div style="margin-top:10px;font-size:13px;">'
    + '<b>' + (cust?cust.name:'') + '</b> · ' + rs(o.total)
    + '<br><small>' + new Date(o.date).toLocaleDateString('en-IN') + (o.invoiceNo?' · Inv#'+o.invoiceNo:'') + '</small>'
    + '</div>'
  );
}

function attachWOBillPhoto(woId) {
  const input = document.createElement('input');
  input.type = 'file';
  input.accept = 'image/*';
  input.capture = 'environment';
  input.style.display = 'none';
  document.body.appendChild(input);
  input.onchange = function() {
    if (!input.files || !input.files[0]) { document.body.removeChild(input); return; }
    const file = input.files[0];
    document.body.removeChild(input);
    const reader = new FileReader();
    reader.onload = async function(e) {
      const dataUrl = e.target.result;
      const o = (state.wholesaleOrders||[]).find(x=>x.id===woId);
      if (!o) { toast('Order not found'); return; }
      o.photoDataUrl = dataUrl;
      try { await save(); } catch(err) { console.warn('[wo] photo-attach save failed:', err.message); if (typeof RX!=='undefined') RX.log('warn','wo','photo-attach save: '+err.message); }
      renderWholesaleOrders();
      closeModal();
      toast('📷 Photo attached — uploading to cloud…');
      if (window.firebaseUploadWOPhoto && window._firebaseAuthed) {
        window.firebaseUploadWOPhoto(woId, dataUrl)
          .then(function(url) {
            const o2 = (state.wholesaleOrders||[]).find(function(x){ return x.id === woId; });
            if (o2) { o2.photoDataUrl = url; save(); renderWholesaleOrders(); }
            toast('✅ Bill photo saved to cloud');
          })
          .catch(function(err) {
            console.warn('WO bill photo upload failed:', err.message);
            toast('Photo saved locally (cloud sync pending)');
          });
      }
    };
    reader.readAsDataURL(file);
  };
  input.click();
}

// ============================================================
// WHOLESALE ORDERS — New Order (Upgraded)
// ============================================================
function openNewWholesaleOrder() {
  state.woCart = [];
  const salespeople = state.staff.filter(s=>s.active&&s.role!=='delivery');
  const delivery = state.staff.filter(s=>s.active);
  const wsCusts = state.customers.filter(c=>c.type==='wholesale');
  showModal('New Wholesale Order', `
    <!-- CUSTOMER -->
    <div class="field">
      <label>Customer *</label>
      <select class="select" id="wo-cust" onchange="woFillCustomerDetails()">
        <option value="">— select wholesale customer —</option>
        ${wsCusts.map(c=>`<option value="${c.id}">${c.name}${c.city?' — '+c.city:''}</option>`).join('')}
      </select>
      <small style="margin-top:5px;display:block;">
        <a href="#" onclick="event.preventDefault();closeModal();openAddWholesaleCustomer();" style="color:#0369a1;">+ Add new customer first</a>
      </small>
    </div>

    <!-- Auto-filled customer details -->
    <div id="wo-cust-detail" style="display:none;background:#f0fdf4;border-radius:10px;padding:10px;margin-bottom:10px;font-size:13px;">
      <div id="wo-cust-detail-inner"></div>
    </div>

    <!-- STAFF -->
    <div class="row2">
      <div class="field"><label>📋 Order By</label>
        <select class="select" id="wo-soldby">
          <option value="">— select —</option>
          ${salespeople.map(s=>`<option value="${s.id}">${s.name}</option>`).join('')}
        </select>
      </div>
      <div class="field"><label>🛵 Dispatch By</label>
        <select class="select" id="wo-delivby">
          <option value="">TBD / Self</option>
          ${delivery.map(s=>`<option value="${s.id}">${s.name}</option>`).join('')}
        </select>
      </div>
    </div>

    <!-- PARTS SEARCH -->
    <div class="field"><label>Add Parts</label>
      <input class="input" id="wo-search" placeholder="Search part no or name…" oninput="renderWOSearch()">
    </div>
    <div id="wo-search-results"></div>
    <div id="wo-cart-items" class="mt-1"></div>

    <!-- CHARGES -->
    <div style="background:#f8fafc;border-radius:10px;padding:12px;margin-top:10px;">
      <div style="font-size:13px;font-weight:600;margin-bottom:8px;">💰 Order Charges</div>
      <div class="row2">
        <div class="field">
          <label>Discount ₹</label>
          <input type="number" class="input" id="wo-disc" value="0" min="0" oninput="renderWOCart()">
        </div>
        <div class="field">
          <label>Shipping / Freight ₹</label>
          <input type="number" class="input" id="wo-shipping" value="0" min="0" oninput="renderWOCart()" placeholder="Bus/courier charge">
        </div>
      </div>
      <div class="row2">
        <div class="field">
          <label>Packaging ₹</label>
          <input type="number" class="input" id="wo-packaging" value="0" min="0" oninput="renderWOCart()" placeholder="Box/tape/wrap cost">
        </div>
        <div class="field">
          <label>Payment Mode</label>
          <select class="select" id="wo-paymode">
            <option value="credit">Credit</option>
            <option value="advance">Advance (Paid)</option>
            <option value="upi">UPI</option>
            <option value="cash">Cash</option>
          </select>
        </div>
      </div>
    </div>

    <!-- DISPATCH / BUS DETAILS -->
    <div style="background:#eff6ff;border-radius:10px;padding:12px;margin-top:10px;">
      <div style="font-size:13px;font-weight:600;margin-bottom:8px;">🚌 Dispatch Details (Bus / Courier)</div>
      <div class="row2">
        <div class="field">
          <label>Dispatch Mode</label>
          <select class="select" id="wo-dispatch-mode" onchange="woToggleDispatchFields()">
            <option value="">— select later —</option>
            <option value="bus">Local Bus</option>
            <option value="private_bus">Private Bus (Luxury/Express)</option>
            <option value="courier">Courier (BlueDart/DTDC…)</option>
            <option value="own_vehicle">Own Vehicle</option>
            <option value="pickup">Customer Pickup</option>
          </select>
        </div>
        <div class="field">
          <label>Recipient Bus Stand / Depot</label>
          <input class="input" id="wo-dest-busstand" placeholder="e.g. Raipur Central Bus Stand">
        </div>
      </div>
      <div id="wo-bus-fields">
        <div class="row2">
          <div class="field">
            <label>Bus / Service Name</label>
            <input class="input" id="wo-bus-name" placeholder="e.g. Chhattisgarh Roadways / Sharma Transport">
          </div>
          <div class="field">
            <label>Bus No / Booking No</label>
            <input class="input" id="wo-bus-no" placeholder="e.g. CG-04-AB-1234 / TRK-567">
          </div>
        </div>
        <div class="row2">
          <div class="field">
            <label>Departure Date & Time</label>
            <input type="datetime-local" class="input" id="wo-dispatch-time">
          </div>
          <div class="field">
            <label>Expected Delivery Date</label>
            <input type="date" class="input" id="wo-eta">
          </div>
        </div>
        <div class="field">
          <label>Contact at Destination (name / phone)</label>
          <input class="input" id="wo-recv-contact" placeholder="Who to call when parcel arrives at bus stand">
        </div>
      </div>
    </div>

    <!-- TOTALS -->
    <div id="wo-total-row" style="font-size:17px;font-weight:700;color:var(--primary);margin:12px 0 4px;padding:10px;background:#f0fdf4;border-radius:10px;"></div>

    <!-- NOTES -->
    <div class="field">
      <label>Notes / Special Instructions</label>
      <textarea class="input" id="wo-notes" rows="2" placeholder="Fragile items, packing instructions, customer special requests…"></textarea>
    </div>

    <button class="btn btn-success" onclick="saveWholesaleOrder()">✓ Place Wholesale Order</button>
  `);
  // Pre-fill dispatch time to now
  const now = new Date();
  now.setMinutes(now.getMinutes() - now.getTimezoneOffset());
  const el = document.getElementById('wo-dispatch-time');
  if (el) el.value = now.toISOString().slice(0,16);
}

function woFillCustomerDetails() {
  const custId = document.getElementById('wo-cust').value;
  const cust = getCustomerById(custId);
  const detailBox = document.getElementById('wo-cust-detail');
  const inner = document.getElementById('wo-cust-detail-inner');
  if (!cust) { detailBox.style.display='none'; return; }
  detailBox.style.display = 'block';
  inner.innerHTML = `
    <div style="display:flex;justify-content:space-between;flex-wrap:wrap;gap:6px;">
      <div><b>${cust.name}</b>${cust.city?` · ${cust.city}`:''}${cust.state?`, ${cust.state}`:''}</div>
      <span class="tag ${cust.trusted?'':'warn'}">${cust.trusted?'✅ Trusted':'New'}</span>
    </div>
    ${cust.phone?`<div style="margin-top:4px;">📞 ${cust.phone}</div>`:''}
    ${cust.address?`<div style="margin-top:4px;">📍 ${cust.address}</div>`:''}
    ${cust.busStand?`<div style="margin-top:4px;">🚌 Bus Stand: <b>${cust.busStand}</b></div>`:''}
    ${cust.totalDue>0?`<div style="margin-top:4px;color:var(--warning);font-weight:600;">⚠️ Outstanding due: ${rs(cust.totalDue)}</div>`:''}
  `;
  // Auto-fill destination bus stand from customer record
  const busStandEl = document.getElementById('wo-dest-busstand');
  if (busStandEl && cust.busStand && !busStandEl.value) busStandEl.value = cust.busStand;
  const recvEl = document.getElementById('wo-recv-contact');
  if (recvEl && cust.phone && !recvEl.value) recvEl.value = cust.name + ' ' + cust.phone;
}

function woToggleDispatchFields() {
  const mode = document.getElementById('wo-dispatch-mode').value;
  const busFields = document.getElementById('wo-bus-fields');
  if (!busFields) return;
  busFields.style.display = (mode === 'pickup' || mode === '') ? 'none' : 'block';
}

function renderWOSearch() {
  const q = (document.getElementById('wo-search').value||'').toLowerCase().trim();
  const div = document.getElementById('wo-search-results'); if(!q){div.innerHTML='';return;}
  const res = state.parts.filter(p=>p.partNo.toLowerCase().includes(q)||p.name.toLowerCase().includes(q)).slice(0,8);
  div.innerHTML = res.map(p=>`<div class="part-item" onclick="addToWOCart('${p.id}')">
    <div class="part-row"><div><div class="part-name">${p.name}</div><div class="part-no">${p.partNo} · Stock: ${p.stock}</div></div>
    <div class="part-prices"><div class="price-instant">${rs(p.creditPrice||p.instantPrice)}</div><small>Tap to add</small></div></div></div>`).join('');
}

function addToWOCart(partId) {
  const existing = state.woCart.find(i=>i.partId===partId);
  if (existing) existing.qty++; else state.woCart.push({partId, qty:1});
  document.getElementById('wo-search').value='';
  document.getElementById('wo-search-results').innerHTML='';
  renderWOCart();
}

function renderWOCart() {
  const div = document.getElementById('wo-cart-items');
  if (!state.woCart.length) {
    div.innerHTML='<div style="color:var(--muted);font-size:13px;padding:8px 0;">No items added yet — search above to add parts</div>';
    document.getElementById('wo-total-row').innerHTML='';
    return;
  }
  let sub = 0;
  div.innerHTML = state.woCart.map((item,i) => {
    const p = getPartById(item.partId); if(!p) return '';
    const price = woItemPrice(item, p);
    sub += price * item.qty;
    const woCont=isContinuousUnit(p.unit); const woU=(p.unit&&p.unit!=='pcs')?p.unit:'';
    const woQ=woCont
      ? `<div class="qty-control"><button class="qty-btn" onclick="changeWOQty(${i},-1)">−</button><input type="number" min="0.5" step="0.5" value="${item.qty}" style="width:48px;text-align:center;font-size:14px;font-weight:700;border:none;background:transparent;" onchange="setWOQtyDirect(${i},this.value)"><span style="font-size:10px;color:var(--muted)">${woU}</span><button class="qty-btn" onclick="changeWOQty(${i},1)">+</button></div>`
      : `<div class="qty-control"><button class="qty-btn" onclick="changeWOQty(${i},-1)">−</button><span class="qty-num">${item.qty}</span><button class="qty-btn" onclick="changeWOQty(${i},1)">+</button></div>`;
    return `<div class="cart-item">
      <div style="flex:1;min-width:0;"><div class="bold" style="font-size:13px;">${p.name}</div><div class="part-no">${p.partNo} · ${rs(price)} / ${p.unit||'pcs'} · Stock: ${p.stock} ${p.unit||'pcs'}${item.customPrice!=null?' <span style="color:#b45309;font-weight:700;">(bill rate)</span>':''}</div><button class="gp-rate-chip" onclick="gpOpenRateEditor('wo',${i})">✎ Rate · H/C/R</button></div>
      ${woQ}
      <div style="min-width:70px;text-align:right;font-weight:600;">${rs(price*item.qty)}</div>
    </div>`;
  }).join('');

  const disc     = +document.getElementById('wo-disc')?.value     || 0;
  const shipping = +document.getElementById('wo-shipping')?.value  || 0;
  const packaging= +document.getElementById('wo-packaging')?.value || 0;
  const grandTotal = Math.max(0, sub - disc + shipping + packaging);

  document.getElementById('wo-total-row').innerHTML = `
    <div style="font-size:13px;color:var(--muted);">
      <div class="flex-between" style="margin-bottom:3px;"><span>Parts Subtotal</span><span>${rs(sub)}</span></div>
      ${disc>0?`<div class="flex-between" style="margin-bottom:3px;color:var(--danger);"><span>Discount</span><span>−${rs(disc)}</span></div>`:''}
      ${shipping>0?`<div class="flex-between" style="margin-bottom:3px;"><span>🚌 Shipping / Freight</span><span>+${rs(shipping)}</span></div>`:''}
      ${packaging>0?`<div class="flex-between" style="margin-bottom:3px;"><span>📦 Packaging</span><span>+${rs(packaging)}</span></div>`:''}
    </div>
    <div class="flex-between bold" style="font-size:18px;color:var(--primary);margin-top:6px;padding-top:6px;border-top:1px solid var(--border);">
      <span>GRAND TOTAL</span><span>${rs(grandTotal)}</span>
    </div>`;
}

function changeWOQty(idx, delta) {
  const item=state.woCart[idx];
  const p=getPartById(item.partId);
  const step=unitStep(p?p.unit:'pcs');
  item.qty=Math.round((item.qty+delta*step)*100)/100;
  if(item.qty<=0)state.woCart.splice(idx,1);
  renderWOCart();
}
function setWOQtyDirect(idx,val){
  const item=state.woCart[idx]; const p=getPartById(item.partId);
  var q=parseFloat(val)||0;
  if(p&&!isContinuousUnit(p.unit))q=Math.round(q);
  q=Math.round(q*100)/100;
  if(q<=0){state.woCart.splice(idx,1);renderWOCart();return;}
  item.qty=q; renderWOCart();
}

async function saveWholesaleOrder() {
  if (!state.woCart.length) { toast('Add items first'); return; }
  if (!(await rxGuardSale('Wholesale Order'))) return;
  const custId = document.getElementById('wo-cust').value;
  if (!custId) { toast('Select a customer'); return; }
  let sub = 0;
  const items = state.woCart.map(item => {
    const p = getPartById(item.partId);
    const price = woItemPrice(item, p);
    sub += price * item.qty;
    return {partId:p.id, partNo:p.partNo, name:p.name, qty:item.qty, price};
  });
  const disc      = +document.getElementById('wo-disc').value      || 0;
  const shipping  = +document.getElementById('wo-shipping').value   || 0;
  const packaging = +document.getElementById('wo-packaging').value  || 0;
  const grandTotal = Math.max(0, sub - disc + shipping + packaging);
  const payMode    = document.getElementById('wo-paymode').value    || 'credit';
  // Due date from the customer's credit days (computed BEFORE the order object uses it)
  const _custForDue = getCustomerById(custId);
  const dueDate = new Date(Date.now() + (((_custForDue && _custForDue.creditDays) ? _custForDue.creditDays : 30) * 86400000)).toISOString().slice(0,10);

  const dispatch = {
    mode:          document.getElementById('wo-dispatch-mode').value   || '',
    destBusStand:  document.getElementById('wo-dest-busstand').value.trim(),
    busName:       document.getElementById('wo-bus-name')?.value.trim()     || '',
    busNo:         document.getElementById('wo-bus-no')?.value.trim()       || '',
    dispatchTime:  document.getElementById('wo-dispatch-time')?.value       || '',
    eta:           document.getElementById('wo-eta')?.value                 || '',
    recvContact:   document.getElementById('wo-recv-contact')?.value.trim() || '',
  };

  const wo = {
    id: uid(), woNo: state.settings.nextWO++,
    date: new Date().toISOString(), customerId: custId,
    soldById:      document.getElementById('wo-soldby').value,
    deliveredById: document.getElementById('wo-delivby').value,
    items,
    subtotal: sub,
    discount: disc,
    shippingCharge: shipping,
    packagingCharge: packaging,
    total: grandTotal,
    paymentMode: payMode,
    settled: payMode !== 'credit',
    paidAmount: payMode !== 'credit' ? grandTotal : 0,
    payments: payMode !== 'credit' ? [{date:new Date().toISOString(),amount:grandTotal,mode:payMode,ref:''}] : [],
    dueDate: payMode === 'credit' ? dueDate : null,
    notes: document.getElementById('wo-notes').value.trim(),
    dispatch,
    status: 'pending'
  };

  // If payment mode is credit, add to customer's totalDue + enforce credit limit
  const cust = getCustomerById(custId);
  if (payMode === 'credit' && cust) {
    const limit = cust.creditLimit || 0;
    const currentDue = cust.totalDue || 0;
    if (limit > 0 && (currentDue + grandTotal) > limit) {
      const over = (currentDue + grandTotal) - limit;
      if (!confirm(`⚠️ Credit limit warning!\n\n${cust.name} will exceed their ₹${Math.round(limit).toLocaleString('en-IN')} limit by ₹${Math.round(over).toLocaleString('en-IN')}.\nCurrent due: ₹${Math.round(currentDue).toLocaleString('en-IN')}\nThis order: ₹${Math.round(grandTotal).toLocaleString('en-IN')}\n\nProceed anyway?`)) return;
    }
    cust.totalDue = (cust.totalDue||0) + grandTotal;
  }

  // Deduct stock immediately on WO creation (not on delivery)
  // This keeps inventory accurate — the goods are committed from this moment
  wo.stockDeducted = true;
  items.forEach(function(item) {
    const p = getPartById(item.partId);
    if (p) p.stock = Math.max(0, (p.stock || 0) - item.qty);
  });

  state.wholesaleOrders.push(wo);
  state.woCart = [];
  await save(); closeModal(); renderWholesaleOrders();
  // Per-record verification — non-blocking but warns on failure
  rxConfirmRecord('wholesaleOrders', wo.id, 12000).then(_ss => {
    if (!_ss.ok) {
      RX.log('warn','wo','WO-' + wo.woNo + ' queued but not yet confirmed (' + _ss.reason + ')');
      toast('⚠️ WO saved locally, syncing in background…');
    }
  });
  logActivity(`WO-${wo.woNo} created for ${cust?.name||'—'} — ${rs(grandTotal)}`, 'info');

  // Auto-send WhatsApp dispatch notification if dispatch mode is set
  if (dispatch.mode && dispatch.mode !== 'pickup' && cust?.phone) {
    const msg = buildWOMessage(wo);
    if (confirm(`Send order confirmation to ${cust.name} on WhatsApp?`)) {
      window.open(`https://wa.me/${cust.phone}?text=${encodeURIComponent(msg)}`, '_blank');
    }
  }
  toast('✅ Wholesale order WO-' + wo.woNo + ' created');
}

function updateWOStatus(id, status) {
  const o = state.wholesaleOrders.find(x=>x.id===id);
  if (!o) return;
  const prevStatus = o.status;
  o.status = status;
  if (status==='delivered') {
    o.deliveredAt = new Date().toISOString();
    if (prevStatus !== 'delivered') {
      // Stock was already deducted at order creation (wo.stockDeducted = true).
      // For legacy WOs created before this fix (stockDeducted not set), deduct now.
      const alerts = [];
      if (!o.stockDeducted) {
        o.items.forEach(item => {
          const p = getPartById(item.partId);
          if (p) {
            p.stock = Math.max(0, (p.stock||0) - item.qty);
            if (p.stock === 0) alerts.push({name:p.name, partNo:p.partNo, stock:0, minStock:p.minStock, status:'out'});
            else if (p.minStock > 0 && p.stock <= p.minStock) alerts.push({name:p.name, partNo:p.partNo, stock:p.stock, minStock:p.minStock, status:'low'});
          }
        });
        o.stockDeducted = true;
        logActivity && logActivity(`WO-${o.woNo} delivered — stock deducted (legacy)`, 'info');
      } else {
        // Stock already deducted — just check min-stock levels for alerts
        o.items.forEach(item => {
          const p = getPartById(item.partId);
          if (p) {
            if (p.stock === 0) alerts.push({name:p.name, partNo:p.partNo, stock:0, minStock:p.minStock, status:'out'});
            else if (p.minStock > 0 && p.stock <= p.minStock) alerts.push({name:p.name, partNo:p.partNo, stock:p.stock, minStock:p.minStock, status:'low'});
          }
        });
        logActivity && logActivity(`WO-${o.woNo} delivered`, 'info');
      }
      xlsRefreshStockCells(o.items.map(i=>i.partId));
      if (alerts.length) showStockAlertModal(alerts, `WO-${o.woNo} Delivered`);
    }
  }
  save(); renderWholesaleOrders();
  toast('Order marked ' + status);
}

// ============================================================
// WO PAYMENT SYSTEM — Per-order payment recording, collection workflow, cash flow
// ============================================================

function openWOPayment(woId) {
  const o = (state.wholesaleOrders||[]).find(x=>x.id===woId);
  if (!o) return;
  const cust = (state.customers||[]).find(c=>c.id===o.customerId);
  const paid = o.paidAmount || 0;
  const due  = (o.total||0) - paid;
  const dueDateObj = o.dueDate ? new Date(o.dueDate) : null;
  const now = new Date();
  const overdueDays = dueDateObj ? Math.floor((now-dueDateObj)/86400000) : 0;
  const overdueHtml = overdueDays > 0
    ? `<div style="background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:8px 12px;margin-bottom:10px;font-size:13px;color:#dc2626;font-weight:600;">⚠️ This order is ${overdueDays} days overdue (due ${dueDateObj.toLocaleDateString('en-IN')})</div>`
    : '';
  const profileHtml = buildPaymentProfileHtml(o.customerId);
  showModal(`💰 Collect — WO-${o.woNo} · ${cust?cust.name:''}`, `
    ${overdueHtml}
    <div style="background:#f8fafc;border-radius:10px;padding:10px 12px;margin-bottom:12px;font-size:13px;">
      <div style="display:flex;justify-content:space-between;margin-bottom:4px;">
        <span>Order Total</span><span style="font-weight:700;">${rs(o.total)}</span>
      </div>
      <div style="display:flex;justify-content:space-between;margin-bottom:4px;">
        <span>Paid so far</span><span style="font-weight:700;color:var(--success);">${rs(paid)}</span>
      </div>
      <div style="display:flex;justify-content:space-between;font-size:15px;font-weight:700;border-top:1px solid var(--border);padding-top:6px;margin-top:4px;">
        <span>Outstanding Due</span><span style="color:#dc2626;">${rs(due)}</span>
      </div>
    </div>
    ${profileHtml}
    <div class="field"><label>Amount Collecting Now (₹)</label>
      <input type="number" class="input" id="wop-amt" value="${Math.round(due)}" placeholder="${Math.round(due)}" inputmode="numeric">
    </div>
    <div class="row2">
      <div class="field"><label>Mode</label>
        <select class="select" id="wop-mode">
          <option>cash</option><option>upi</option><option>cheque</option><option>neft</option>
        </select>
      </div>
      <div class="field"><label>Ref / UTR</label>
        <input class="input" id="wop-ref" placeholder="Optional">
      </div>
    </div>
    <div class="field"><label>Date</label>
      <input type="date" class="input" id="wop-date" value="${new Date().toISOString().slice(0,10)}">
    </div>
    <button class="btn btn-success" onclick="recordWOPayment('${woId}')">✅ Record Payment</button>
    ${cust&&cust.phone?`<button class="btn btn-outline mt-1" onclick="sendWOPaymentReminder('${woId}')">📲 Send WA Reminder First</button>`:''}
  `);
}

async function recordWOPayment(woId) {
  const o = (state.wholesaleOrders||[]).find(x=>x.id===woId);
  if (!o) return;
  const amt  = parseFloat(document.getElementById('wop-amt').value) || 0;
  if (!amt) { toast('Enter amount'); return; }
  const mode = document.getElementById('wop-mode').value;
  const ref  = document.getElementById('wop-ref').value.trim();
  const date = document.getElementById('wop-date').value || new Date().toISOString().slice(0,10);
  const maxDue = (o.total||0) - (o.paidAmount||0);
  const paying = Math.min(amt, maxDue);
  if (!o.payments) o.payments = [];
  o.payments.push({ date: date+'T'+new Date().toTimeString().slice(0,8), amount: paying, mode, ref });
  o.paidAmount = (o.paidAmount||0) + paying;
  o.settled = o.paidAmount >= o.total;
  // Adjust customer totalDue
  const cust = (state.customers||[]).find(c=>c.id===o.customerId);
  if (cust) cust.totalDue = Math.max(0, (cust.totalDue||0) - paying);
  // Update payment behavior profile
  updatePaymentProfile(o.customerId, o, paying, date);
  logActivity && logActivity(`WO-${o.woNo} payment ${rs(paying)} via ${mode}${o.settled?' — FULLY SETTLED':''}`, 'info');
  await save();
  closeModal();
  renderWholesaleOrders();
  try { recomputeTotalDue(); } catch(e) {}
  toast(`${rs(paying)} collected${o.settled?' — Order fully settled ✅':''}` );
}

// ── Payment behavior profiling ─────────────────────────────
function updatePaymentProfile(custId, wo, amtPaid, payDate) {
  const cust = (state.customers||[]).find(c=>c.id===custId);
  if (!cust) return;
  if (!cust.paymentProfile) cust.paymentProfile = { totalOrders:0, onTimeCount:0, lateCount:0, totalDaysLate:0, avgOrderVal:0, lastPaidDate:null };
  const p = cust.paymentProfile;
  p.lastPaidDate = payDate;
  if (wo.dueDate) {
    const dueD = new Date(wo.dueDate);
    const payD = new Date(payDate);
    const diff = Math.floor((payD-dueD)/86400000);
    if (diff <= 0) p.onTimeCount = (p.onTimeCount||0) + 1;
    else { p.lateCount = (p.lateCount||0) + 1; p.totalDaysLate = (p.totalDaysLate||0) + diff; }
  }
  if (wo.settled) {
    p.totalOrders = (p.totalOrders||0) + 1;
    p.avgOrderVal = ((p.avgOrderVal||0) * ((p.totalOrders-1)/p.totalOrders)) + (wo.total/p.totalOrders);
  }
}

function buildPaymentProfileHtml(custId) {
  const cust = (state.customers||[]).find(c=>c.id===custId);
  if (!cust || !cust.paymentProfile || !cust.paymentProfile.totalOrders) return '';
  const p = cust.paymentProfile;
  const total = (p.onTimeCount||0) + (p.lateCount||0);
  if (!total) return '';
  const onTimePct = Math.round(((p.onTimeCount||0)/total)*100);
  const avgDelay = p.lateCount ? Math.round((p.totalDaysLate||0)/p.lateCount) : 0;
  const tag = onTimePct >= 80 ? '🟢 Reliable' : onTimePct >= 50 ? '🟡 Occasional delays' : '🔴 Frequent late payer';
  return `<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:8px 12px;margin-bottom:12px;font-size:12px;">
    <div style="font-weight:700;margin-bottom:4px;">Payment Profile: ${tag}</div>
    <div style="display:flex;gap:12px;flex-wrap:wrap;">
      <span>On time: <b>${onTimePct}%</b></span>
      ${avgDelay>0?`<span>Avg delay: <b>${avgDelay}d</b></span>`:''}
      <span>Orders settled: <b>${p.totalOrders}</b></span>
      ${p.lastPaidDate?`<span>Last paid: <b>${new Date(p.lastPaidDate).toLocaleDateString('en-IN',{day:'2-digit',month:'short'})}</b></span>`:''}
    </div>
  </div>`;
}

// ── WhatsApp collection messages ───────────────────────────
function sendWOPaymentReminder(woId) {
  const o = (state.wholesaleOrders||[]).find(x=>x.id===woId);
  if (!o) return;
  const cust = (state.customers||[]).find(c=>c.id===o.customerId);
  if (!cust || !cust.phone) { toast('No phone number for this customer'); return; }
  const due = (o.total||0) - (o.paidAmount||0);
  const dueDateObj = o.dueDate ? new Date(o.dueDate) : null;
  const overdueDays = dueDateObj ? Math.floor((new Date()-dueDateObj)/86400000) : 0;
  const msg = buildCollectionMessage(cust, o, due, overdueDays);
  window.open(`https://wa.me/${cust.phone}?text=${encodeURIComponent(msg)}`, '_blank');
}

function buildCollectionMessage(cust, wo, due, overdueDays) {
  const shop = (state.settings&&state.settings.shopName) || 'Goury Auto Parts';
  if (overdueDays > 30) {
    return `*${shop}* 🔴

Namaste ${cust.name} ji,

Humara WO-${wo.woNo} ka ₹${Math.round(due).toLocaleString('en-IN')} outstanding hai jo ${overdueDays} din se overdue hai.

Kripya aaj bhugtan karein warna aage credit band karna padega.

Dhanyavaad 🙏
_— ${shop}_`;
  } else if (overdueDays > 0) {
    return `*${shop}*

Namaste ${cust.name} ji,

WO-${wo.woNo} ka ₹${Math.round(due).toLocaleString('en-IN')} payment due tha. Kripya jald bhugtan karein.

Dhanyavaad 🙏
_— ${shop}_`;
  } else {
    const dueTxt = wo.dueDate ? new Date(wo.dueDate).toLocaleDateString('en-IN') : 'jald';
    return `*${shop}*

Namaste ${cust.name} ji,

WO-${wo.woNo} ka ₹${Math.round(due).toLocaleString('en-IN')} payment ${dueTxt} tak expected hai.

Dhanyavaad 🙏
_— ${shop}_`;
  }
}

// ── Bulk collection WhatsApp blast ─────────────────────────
function openCollectionBlast() {
  const now = new Date();
  const overdue = (state.wholesaleOrders||[])
    .filter(o=>o.paymentMode==='credit'&&!o.settled)
    .map(o=>{
      const cust = (state.customers||[]).find(c=>c.id===o.customerId);
      const due = (o.total||0)-(o.paidAmount||0);
      const overdueDays = o.dueDate ? Math.floor((now-new Date(o.dueDate))/86400000) : 0;
      return { o, cust, due, overdueDays };
    })
    .filter(x=>x.due>0&&x.cust)
    .sort((a,b)=>b.overdueDays-a.overdueDays);

  if (!overdue.length) { toast('No outstanding credit orders'); return; }

  const totalDue = overdue.reduce((s,x)=>s+x.due, 0);
  const rows = overdue.map((x,i)=>`
    <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--border);">
      <div>
        <div style="font-weight:600;font-size:13px;">${x.cust.name}</div>
        <div style="font-size:11px;color:var(--muted);">WO-${x.o.woNo} · ${x.overdueDays>0?`<span style="color:#dc2626;">${x.overdueDays}d overdue</span>`:'On time'}</div>
      </div>
      <div style="text-align:right;">
        <div style="font-weight:700;color:${x.overdueDays>0?'#dc2626':'var(--text)'};">₹${Math.round(x.due).toLocaleString('en-IN')}</div>
        ${x.cust.phone?`<a href="https://wa.me/${x.cust.phone}?text=${encodeURIComponent(buildCollectionMessage(x.cust,x.o,x.due,x.overdueDays))}" target="_blank" style="font-size:11px;color:#25d366;font-weight:600;">📲 Send WA</a>`:'<span style="font-size:11px;color:var(--muted);">No phone</span>'}
      </div>
    </div>`).join('');

  showModal('📋 Collection Blast', `
    <div style="background:#fef2f2;border-radius:10px;padding:10px 12px;margin-bottom:12px;">
      <div style="font-size:12px;color:#991b1b;font-weight:700;">TOTAL OUTSTANDING</div>
      <div style="font-size:24px;font-weight:800;color:#dc2626;">₹${Math.round(totalDue).toLocaleString('en-IN')}</div>
      <div style="font-size:12px;color:var(--muted);">${overdue.length} customers · ${overdue.filter(x=>x.overdueDays>0).length} overdue</div>
    </div>
    <div style="font-size:12px;color:var(--muted);margin-bottom:8px;">Tap 📲 to send individual reminders</div>
    ${rows}
  `);
}

// ── Cash Flow Panel ─────────────────────────────────────────
function openCashFlowPanel() {
  const now = new Date();
  const today = now.toISOString().slice(0,10);

  // Incoming: unsettled credit WOs grouped by week
  const creditWOs = (state.wholesaleOrders||[]).filter(o=>o.paymentMode==='credit'&&!o.settled);
  const totalIncoming = creditWOs.reduce((s,o)=>s+(o.total-(o.paidAmount||0)),0);

  // Group into: overdue / this week / next week / later
  function weekBucket(o) {
    if (!o.dueDate) return 'later';
    const d = new Date(o.dueDate);
    const diff = Math.floor((d-now)/86400000);
    if (diff < 0) return 'overdue';
    if (diff <= 7) return 'thisweek';
    if (diff <= 14) return 'nextweek';
    return 'later';
  }
  const buckets = { overdue:[], thisweek:[], nextweek:[], later:[] };
  creditWOs.forEach(o=>{
    const cust = (state.customers||[]).find(c=>c.id===o.customerId);
    buckets[weekBucket(o)].push({o,cust,due:(o.total||0)-(o.paidAmount||0)});
  });

  function bucketHtml(items, title, color) {
    if (!items.length) return '';
    const total = items.reduce((s,x)=>s+x.due,0);
    return `<div style="margin-bottom:14px;">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
        <div style="font-weight:700;font-size:13px;">${title}</div>
        <div style="font-weight:800;font-size:15px;color:${color};">₹${Math.round(total).toLocaleString('en-IN')}</div>
      </div>
      ${items.map(x=>`<div style="display:flex;justify-content:space-between;font-size:12px;padding:4px 0;border-bottom:1px solid var(--border);">
        <span>${x.cust?x.cust.name:'—'} · WO-${x.o.woNo}</span>
        <span style="font-weight:600;">₹${Math.round(x.due).toLocaleString('en-IN')}</span>
      </div>`).join('')}
    </div>`;
  }

  // Outgoing: unpaid purchases due in next 30d
  const outgoing = (state.purchases||[]).filter(p=>(p.amount-(p.paidAmount||0))>0);
  const totalOutgoing = outgoing.reduce((s,p)=>s+(p.amount-(p.paidAmount||0)),0);
  const netFlow = totalIncoming - totalOutgoing;

  showModal('📊 Cash Flow Forecast', `
    <div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;">
      <div style="flex:1;min-width:90px;background:#f0fdf4;border-radius:10px;padding:10px 12px;">
        <div style="font-size:11px;color:#166534;font-weight:700;">RECEIVABLE</div>
        <div style="font-size:20px;font-weight:800;color:#16a34a;">₹${Math.round(totalIncoming).toLocaleString('en-IN')}</div>
        <div style="font-size:11px;color:var(--muted);">${creditWOs.length} orders</div>
      </div>
      <div style="flex:1;min-width:90px;background:#fef2f2;border-radius:10px;padding:10px 12px;">
        <div style="font-size:11px;color:#991b1b;font-weight:700;">PAYABLE</div>
        <div style="font-size:20px;font-weight:800;color:#dc2626;">₹${Math.round(totalOutgoing).toLocaleString('en-IN')}</div>
        <div style="font-size:11px;color:var(--muted);">${outgoing.length} bills</div>
      </div>
      <div style="flex:1;min-width:90px;background:${netFlow>=0?'#eff6ff':'#fef9c3'};border-radius:10px;padding:10px 12px;">
        <div style="font-size:11px;color:${netFlow>=0?'#1e40af':'#92400e'};font-weight:700;">NET</div>
        <div style="font-size:20px;font-weight:800;color:${netFlow>=0?'#1d4ed8':'#d97706'};">${netFlow>=0?'+':''}₹${Math.round(Math.abs(netFlow)).toLocaleString('en-IN')}</div>
        <div style="font-size:11px;color:var(--muted);">${netFlow>=0?'Inflow':'Shortfall'}</div>
      </div>
    </div>
    <div style="font-weight:700;font-size:13px;margin-bottom:10px;">💰 Incoming Schedule</div>
    ${bucketHtml(buckets.overdue,'🔴 Overdue','#dc2626')}
    ${bucketHtml(buckets.thisweek,'📅 This Week','#d97706')}
    ${bucketHtml(buckets.nextweek,'📅 Next Week','#0369a1')}
    ${bucketHtml(buckets.later,'⏳ Later','var(--muted)')}
    ${!creditWOs.length?'<div style="text-align:center;padding:20px;color:var(--muted);">No outstanding credit orders</div>':''}
    <button class="btn btn-outline mt-2" onclick="openCollectionBlast()">📋 Send All Reminders</button>
  `);
}

// ── AI Collection Intelligence ─────────────────────────────
async function openWOCollectionAgent() {
  const key = getAIKey && getAIKey();
  if (!key || !key.startsWith('sk-')) { toast('Add AI key in Settings to use agent'); return; }
  const now = new Date();
  const creditWOs = (state.wholesaleOrders||[]).filter(o=>o.paymentMode==='credit'&&!o.settled);
  const totalDue = creditWOs.reduce((s,o)=>s+(o.total-(o.paidAmount||0)),0);
  const custSummaries = {};
  creditWOs.forEach(o=>{
    const cust = (state.customers||[]).find(c=>c.id===o.customerId);
    if (!cust) return;
    if (!custSummaries[cust.id]) custSummaries[cust.id] = {
      name: cust.name, phone: cust.phone||'', totalDue:0, orders:[],
      creditDays: cust.creditDays||30, creditLimit: cust.creditLimit||0,
      profile: cust.paymentProfile||null
    };
    const due = (o.total||0)-(o.paidAmount||0);
    const overdueDays = o.dueDate ? Math.floor((now-new Date(o.dueDate))/86400000) : 0;
    custSummaries[cust.id].totalDue += due;
    custSummaries[cust.id].orders.push({ woNo: o.woNo, total: o.total, due, overdueDays, dueDate: o.dueDate });
  });
  const context = JSON.stringify(Object.values(custSummaries), null, 2);
  showModal('🤖 Collection Agent', '<div class="ai-thinking">🤖 Analysing your receivables…</div>');
  try {
    const resp = await fetch('https://api.anthropic.com/v1/messages', {
      method:'POST',
      headers:{'content-type':'application/json','x-api-key':key,'anthropic-version':'2023-06-01','anthropic-dangerous-direct-browser-access':'true'},
      body: JSON.stringify({
        model: (getAIModel&&getAIModel()) || 'claude-haiku-4-5-20251001',
        max_tokens: 1200,
        messages:[{role:'user',content:`You are a collections advisor for a B2B automotive parts shop in India (Gwalior, MP). Analyse this outstanding credit data and give: (1) Priority action list — who to call today and why, (2) Risk flag — any customer showing dangerous pattern, (3) Cash flow gap — will we have a crunch this week. Be sharp and actionable. Data:

${context}

Total receivable: ₹${Math.round(totalDue).toLocaleString('en-IN')}
Respond in 3 crisp sections. Use INR amounts. Plain text, no markdown.`}]
      })
    });
    const data = await resp.json();
    const text = (data.content&&data.content[0]&&data.content[0].text) || 'No response';
    const modalBody = document.querySelector('.modal-body') || document.querySelector('[id^="modal"]');
    const el = document.querySelector('.ai-thinking');
    if (el) el.closest('.modal-body') ? el.closest('.modal-body').innerHTML = `<div style="font-size:13px;line-height:1.7;white-space:pre-wrap;">${text}</div><button class="btn btn-outline mt-2" onclick="openCollectionBlast()">📋 Send All Reminders</button>` : el.parentNode.innerHTML = `<div style="font-size:13px;line-height:1.7;white-space:pre-wrap;">${text}</div><button class="btn btn-outline mt-2" onclick="openCollectionBlast()">📋 Send All Reminders</button>`;
  } catch(e) {
    toast('Agent error: ' + e.message);
  }
}

// ============================================================
// GROWTH CHART
// ============================================================
const CATEGORIES = ['tractor','harvester','jcb','rotavator','other'];
const CAT_COLORS = {'tractor':'#0f766e','harvester':'#f59e0b','jcb':'#dc2626','rotavator':'#7c3aed','other':'#64748b'};

function showReportMenu() {
  document.querySelectorAll('.rpt-section').forEach(s=>s.style.display='none');
  document.getElementById('rpt-menu').style.display='block';
  document.getElementById('rpt-back-btn').style.display='none';
}

function showReport(name, evt) {
  document.querySelectorAll('.rpt-section').forEach(s=>s.style.display='none');
  document.getElementById('rpt-menu').style.display='none';
  document.getElementById('rpt-back-btn').style.display='block';
  const el = document.getElementById('rpt-'+name);
  if (el) el.style.display='block';
  if (name==='growth') renderGrowthChart(6);
  if (name==='customers') { renderCustomerBase(); }
  if (name==='po') { renderPOList(); }
  if (name==='staff') { renderRptStaffPerf(); renderMechComm(); }
  if (name==='salary') { populateSalarySelects(); renderSalaryPaidList(); renderAdvanceTracker(); }
  if (name==='scores') { populateScoreSelects(); renderScoreLeaderboard(); }
  if (name==='backup') { renderAIFeedbackList(); updateLastBackupDisplay(); try { initMigrateCard(); } catch(e) {} }
  if (name==='priceeditor') { xlsInit(); }
}

// Called on Reports tab open
function initReports() {
  showReportMenu();
}

// ============================================================
// LIVE PRICE EDITOR (Excel-style) — Admin Only
// ============================================================
let _xlsDirty = {}; // partId -> {costPrice, instantPrice, creditPrice, mrp}
let _xlsPage = 0;
const XLS_PAGE_SIZE = 50;

function xlsInit() {
  const isAdmin = state.session?.role === 'admin';
  document.getElementById('xls-locked-msg').style.display  = isAdmin ? 'none' : 'block';
  document.getElementById('xls-editor-wrap').style.display = isAdmin ? 'block' : 'none';
  if (!isAdmin) return;
  _xlsDirty = {};
  xlsUpdateUnsavedBadge();
  xlsRender();
}

function xlsGetFiltered() {
  const q = (document.getElementById('xls-search')?.value || '').toLowerCase().trim();
  const machine = document.getElementById('xls-machine-filter')?.value || '';
  const sortVal = document.getElementById('xls-sort')?.value || 'name';
  let parts = state.parts.slice();
  if (q) parts = parts.filter(p =>
    p.partNo.toLowerCase().includes(q) || p.name.toLowerCase().includes(q) ||
    (p.brand||'').toLowerCase().includes(q) || (p.category||'').toLowerCase().includes(q)
  );
  if (machine) parts = parts.filter(p => p.machineType === machine);
  parts.sort((a, b) => {
    if (sortVal === 'partno') return a.partNo.localeCompare(b.partNo);
    if (sortVal === 'cost')   return (a.costPrice||0) - (b.costPrice||0);
    if (sortVal === 'margin') {
      const mA = a.costPrice ? (a.instantPrice - a.costPrice)/a.costPrice : 0;
      const mB = b.costPrice ? (b.instantPrice - b.costPrice)/b.costPrice : 0;
      return mB - mA;
    }
    if (sortVal === 'stock') return (b.stock||0) - (a.stock||0);
    return a.name.localeCompare(b.name);
  });
  return parts;
}

function xlsRender() {
  const parts = xlsGetFiltered();
  const tbody = document.getElementById('xls-tbody');
  const empty = document.getElementById('xls-empty');
  if (!parts.length) { tbody.innerHTML = ''; empty.style.display = 'block'; return; }
  empty.style.display = 'none';

  // For scale: only render up to 200 rows in the SKU editor at a time
  // User must search/filter to narrow down — full list would freeze the browser
  const XLS_MAX_ROWS = window._isMobile ? 80 : 200;
  const visible = parts.slice(0, XLS_MAX_ROWS);
  const overflow = parts.length > XLS_MAX_ROWS;

  tbody.innerHTML = visible.map(p => {
    const d = _xlsDirty[p.id] || {};
    const cost    = d.costPrice    !== undefined ? d.costPrice    : p.costPrice;
    const retail  = d.instantPrice !== undefined ? d.instantPrice : p.instantPrice;
    const ws      = d.creditPrice  !== undefined ? d.creditPrice  : p.creditPrice;
    const mrp     = d.mrp          !== undefined ? d.mrp          : p.mrp;
    const rack    = d.location     !== undefined ? d.location     : (p.location || '');
    const isDirty = !!_xlsDirty[p.id];

    // Discount % from MRP (live computed, shown as read-only but editable to drive price)
    const retailDiscPct = mrp > 0 ? (((mrp - retail) / mrp) * 100).toFixed(1) : '0.0';
    const wsDiscPct     = mrp > 0 ? (((mrp - ws)     / mrp) * 100).toFixed(1) : '0.0';

    // Margin calculations — retail and wholesale separately
    const retMarginPct = cost > 0 ? ((retail - cost) / cost * 100) : 0;
    const wsMarginPct  = cost > 0 ? ((ws    - cost) / cost * 100) : 0;
    const retMarginAmt = retail - cost;
    const wsMarginAmt  = ws    - cost;
    const retCls = retMarginPct >= 30 ? 'xls-margin-high' : retMarginPct >= 15 ? 'xls-margin-mid' : 'xls-margin-low';
    const wsCls  = wsMarginPct  >= 25 ? 'xls-margin-ws-high' : wsMarginPct  >= 12 ? 'xls-margin-ws-mid'  : 'xls-margin-ws-low';

    return `<tr id="xlsrow-${p.id}" class="${isDirty ? 'xls-dirty' : ''}">
      <td><input class="xls-cell-input${d.partNo !== undefined ? ' changed' : ''}" type="text" value="${p.partNo.replace(/"/g,'&quot;')}"
        onchange="xlsCellChange('${p.id}','partNo',this)"
        onkeydown="xlsKeyNav(event,'${p.id}','partNo')"
        id="xlsc-${p.id}-partNo" data-orig="${p.partNo.replace(/"/g,'&quot;')}"
        style="min-width:90px;font-family:monospace;font-size:11px;font-weight:700;color:var(--primary-dark);text-transform:uppercase;"></td>
      <td><input class="xls-cell-input${d.name !== undefined ? ' changed' : ''}" type="text" value="${p.name.replace(/"/g,'&quot;')}"
        onchange="xlsCellChange('${p.id}','name',this)"
        onkeydown="xlsKeyNav(event,'${p.id}','name')"
        id="xlsc-${p.id}-name" data-orig="${p.name.replace(/"/g,'&quot;')}"
        style="min-width:120px;font-weight:500;"></td>
      <td><input class="xls-cell-input${d.costPrice !== undefined ? ' changed' : ''}" type="number" value="${cost}" min="0"
        onchange="xlsCellChange('${p.id}','costPrice',this)"
        onkeydown="xlsKeyNav(event,'${p.id}','costPrice')"
        id="xlsc-${p.id}-cost" data-orig="${p.costPrice}"></td>
      <td><input class="xls-cell-input${d.mrp !== undefined ? ' changed' : ''}" type="number" value="${mrp}" min="0"
        onchange="xlsCellChange('${p.id}','mrp',this)"
        onkeydown="xlsKeyNav(event,'${p.id}','mrp')"
        id="xlsc-${p.id}-mrp" data-orig="${p.mrp}"></td>
      <td><input class="xls-cell-input xls-disc-input${d.retailDiscPct !== undefined ? ' changed' : ''}" type="number" value="${retailDiscPct}" min="0" max="99" step="0.5"
        placeholder="0" title="Retail discount % off MRP"
        onchange="xlsDiscChange('${p.id}','retail',this)"
        onkeydown="xlsKeyNav(event,'${p.id}','retailDiscPct')"
        id="xlsc-${p.id}-retailDiscPct" style="min-width:52px;"></td>
      <td><input class="xls-cell-input${d.instantPrice !== undefined ? ' changed' : ''}" type="number" value="${retail}" min="0"
        onchange="xlsCellChange('${p.id}','instantPrice',this)"
        onkeydown="xlsKeyNav(event,'${p.id}','instantPrice')"
        id="xlsc-${p.id}-instantPrice" data-orig="${p.instantPrice}"></td>
      <td><input class="xls-cell-input xls-disc-input${d.wsDiscPct !== undefined ? ' changed' : ''}" type="number" value="${wsDiscPct}" min="0" max="99" step="0.5"
        placeholder="0" title="Wholesale discount % off MRP"
        onchange="xlsDiscChange('${p.id}','wholesale',this)"
        onkeydown="xlsKeyNav(event,'${p.id}','wsDiscPct')"
        id="xlsc-${p.id}-wsDiscPct" style="min-width:52px;"></td>
      <td><input class="xls-cell-input${d.creditPrice !== undefined ? ' changed' : ''}" type="number" value="${ws}" min="0"
        onchange="xlsCellChange('${p.id}','creditPrice',this)"
        onkeydown="xlsKeyNav(event,'${p.id}','creditPrice')"
        id="xlsc-${p.id}-creditPrice" data-orig="${p.creditPrice}"></td>
      <td style="white-space:nowrap;">
        <span class="xls-margin-pill ${retCls}" id="xls-retmargin-${p.id}">${retMarginPct.toFixed(0)}%<span class="xls-margin-amt">₹${retMarginAmt}</span></span>
      </td>
      <td style="white-space:nowrap;">
        <span class="xls-margin-pill ${wsCls}" id="xls-wsmargin-${p.id}">${wsMarginPct.toFixed(0)}%<span class="xls-margin-amt">₹${wsMarginAmt}</span></span>
      </td>
      <td id="xlsc-${p.id}-stockcell" style="white-space:nowrap;">
        <div class="xls-stock-wrap">
          <button class="xls-stk-btn minus" onclick="xlsStockAdj('${p.id}',-1)" title="Remove 1">−</button>
          <input class="xls-stock-num${d.stock !== undefined ? ' changed' : ''}" type="number" value="${d.stock !== undefined ? d.stock : p.stock}" min="0"
            onchange="xlsCellChange('${p.id}','stock',this)"
            onkeydown="xlsKeyNav(event,'${p.id}','stock')"
            id="xlsc-${p.id}-stock" data-orig="${p.stock}" title="Edit stock">
          <button class="xls-stk-btn plus" onclick="xlsStockAdj('${p.id}',1)" title="Add 1">+</button>
        </div>
      </td>
      <td>
        <input class="xls-cell-input${d.minStock !== undefined ? ' changed' : ''}" type="number" value="${d.minStock !== undefined ? d.minStock : p.minStock}" min="0"
          onchange="xlsCellChange('${p.id}','minStock',this)"
          onkeydown="xlsKeyNav(event,'${p.id}','minStock')"
          id="xlsc-${p.id}-minStock" data-orig="${p.minStock}" style="min-width:44px;" title="Min stock alert level">
      </td>
      <td><input class="xls-cell-input${d.location !== undefined ? ' changed' : ''}" type="text" value="${rack}"
        placeholder="—"
        onchange="xlsCellChange('${p.id}','location',this)"
        onkeydown="xlsKeyNav(event,'${p.id}','location')"
        id="xlsc-${p.id}-location" data-orig="${p.location||''}" style="min-width:60px;text-align:left;text-transform:uppercase;"></td>
      <td>${isDirty ? `<button class="xls-save-row-btn" onclick="xlsSaveRow('${p.id}')">✓</button>` : '<span style="font-size:16px;opacity:.25;">—</span>'}</td>
      <td style="white-space:nowrap;"><button class="xls-del-btn" onclick="xlsDeletePart('${p.id}')" title="Delete this part">🗑</button> <button style="background:none;border:none;cursor:pointer;font-size:14px;opacity:.6;" onclick="openPriceHistoryModal('${p.id}')" title="Price history">📈</button></td>
    </tr>`;
  }).join('');

  // Show overflow notice below table when results are truncated
  let noticeEl = document.getElementById('xls-overflow-notice');
  if (!noticeEl) {
    noticeEl = document.createElement('div');
    noticeEl.id = 'xls-overflow-notice';
    noticeEl.style.cssText = 'padding:10px 14px;background:#fef3c7;border-radius:0 0 10px 10px;font-size:13px;color:#92400e;text-align:center;';
    document.querySelector('.xls-wrap')?.after(noticeEl);
  }
  if (overflow) {
    noticeEl.style.display = 'block';
    noticeEl.innerHTML = `⚠️ Showing first ${XLS_MAX_ROWS} of <b>${parts.length}</b> results. <b>Use the search box above to narrow down by part no, name, brand, or category.</b>`;
  } else {
    noticeEl.style.display = 'none';
  }
  // Update count badge
  const badge = document.getElementById('xls-row-count');
  if (badge) badge.textContent = `${Math.min(visible.length, parts.length)} / ${parts.length} shown`;
}

function xlsCellChange(partId, field, input) {
  const isText = ['location', 'partNo', 'name'].includes(field);
  const rawVal = isText ? input.value.trim() : +input.value;
  const newVal = (field === 'location' || field === 'partNo') ? rawVal.toUpperCase() : rawVal;
  const origVal = isText ? (input.dataset.orig || '') : +input.dataset.orig;
  if (!_xlsDirty[partId]) _xlsDirty[partId] = {};
  if (newVal === origVal) {
    delete _xlsDirty[partId][field];
    if (Object.keys(_xlsDirty[partId]).length === 0) delete _xlsDirty[partId];
    input.classList.remove('changed');
  } else {
    _xlsDirty[partId][field] = newVal;
    input.classList.add('changed');
  }
  // Refresh stock badge colour when stock/minStock changes
  if (field === 'stock' || field === 'minStock') {
    const p = getPartById(partId);
    const d = _xlsDirty[partId] || {};
    const s  = d.stock    !== undefined ? d.stock    : p.stock;
    const ms = d.minStock !== undefined ? d.minStock : p.minStock;
    const inp = document.getElementById('xlsc-' + partId + '-stock');
    if (inp) {
      inp.style.color = s === 0 ? 'var(--danger)' : (ms > 0 && s <= ms) ? 'var(--warning)' : 'var(--success)';
    }
  }
  // Refresh margin pills live
  const row = document.getElementById('xlsrow-' + partId);
  if (row) {
    row.classList.toggle('xls-dirty', !!_xlsDirty[partId]);
    const p = getPartById(partId);
    const d = _xlsDirty[partId] || {};
    const cost   = d.costPrice    !== undefined ? d.costPrice    : p.costPrice;
    const retail = d.instantPrice !== undefined ? d.instantPrice : p.instantPrice;
    const ws     = d.creditPrice  !== undefined ? d.creditPrice  : p.creditPrice;
    // Retail margin
    const retPct = cost > 0 ? ((retail - cost) / cost * 100) : 0;
    const retAmt = retail - cost;
    const retCls = retPct >= 30 ? 'xls-margin-high' : retPct >= 15 ? 'xls-margin-mid' : 'xls-margin-low';
    const retPill = document.getElementById('xls-retmargin-' + partId);
    if (retPill) { retPill.className = 'xls-margin-pill ' + retCls; retPill.innerHTML = retPct.toFixed(0) + '%<span class="xls-margin-amt">₹' + retAmt + '</span>'; }
    // WS margin
    const wsPct = cost > 0 ? ((ws - cost) / cost * 100) : 0;
    const wsAmt = ws - cost;
    const wsCls = wsPct >= 25 ? 'xls-margin-ws-high' : wsPct >= 12 ? 'xls-margin-ws-mid' : 'xls-margin-ws-low';
    const wsPill = document.getElementById('xls-wsmargin-' + partId);
    if (wsPill) { wsPill.className = 'xls-margin-pill ' + wsCls; wsPill.innerHTML = wsPct.toFixed(0) + '%<span class="xls-margin-amt">₹' + wsAmt + '</span>'; }
    // Refresh save button in row
    const btn = row.querySelector('.xls-save-row-btn, td:last-child span');
    if (btn) btn.outerHTML = _xlsDirty[partId]
      ? `<button class="xls-save-row-btn" onclick="xlsSaveRow('${partId}')">✓</button>`
      : '<span style="font-size:16px;opacity:.25;">—</span>';
  }
  xlsUpdateUnsavedBadge();
}

function xlsKeyNav(e, partId, field) {
  const parts = xlsGetFiltered();
  const idx = parts.findIndex(p => p.id === partId);
  const fieldOrder = ['partNo', 'name', 'cost', 'mrp', 'retailDiscPct', 'instantPrice', 'wsDiscPct', 'creditPrice', 'stock', 'minStock', 'location'];
  const fi = fieldOrder.indexOf(field === 'costPrice' ? 'cost' : field);
  if (e.key === 'Enter' || e.key === 'Tab') {
    e.preventDefault();
    let nextPartId = partId, nextField = field;
    if (fi < fieldOrder.length - 1) {
      nextField = fieldOrder[fi + 1] === 'cost' ? 'costPrice' : fieldOrder[fi + 1];
    } else {
      nextField = 'costPrice';
      const nextPart = parts[idx + 1];
      if (nextPart) nextPartId = nextPart.id;
      else return;
    }
    const nextInput = document.getElementById(`xlsc-${nextPartId}-${nextField === 'costPrice' ? 'cost' : nextField}`);
    if (nextInput) { nextInput.focus(); nextInput.select(); }
  }
}


// ============================================================
// PRICE HISTORY — per-part audit trail of cost/price changes
// ============================================================
function recordPriceHistory(part, changedFields, changedBy) {
  if (!part || !changedFields || !Object.keys(changedFields).length) return;
  const priceFields = ['costPrice','instantPrice','creditPrice','mrp'];
  const relevant = priceFields.filter(f => changedFields[f] !== undefined && changedFields[f] !== part[f]);
  if (!relevant.length) return;
  if (!part.priceHistory) part.priceHistory = [];
  const entry = {
    ts: Date.now(),
    by: changedBy || state.session?.name || 'system',
    changes: {}
  };
  relevant.forEach(f => {
    entry.changes[f] = { from: part[f], to: changedFields[f] };
  });
  // Keep last 50 entries per part — prevents unbounded growth
  part.priceHistory.push(entry);
  if (part.priceHistory.length > 50) part.priceHistory = part.priceHistory.slice(-50);
}

function renderPriceHistory(partId) {
  const part = getPartById(partId);
  if (!part || !part.priceHistory || !part.priceHistory.length) {
    return '<div style="color:var(--muted);font-size:13px;padding:12px 0;">No price changes recorded yet.</div>';
  }
  var phRows = part.priceHistory.slice().reverse().map(function(entry) {
    var date = new Date(entry.ts).toLocaleDateString('en-IN', {day:'2-digit',month:'short',year:'2-digit',hour:'2-digit',minute:'2-digit'});
    var labels = {costPrice:'Cost',instantPrice:'Retail',creditPrice:'WS/Credit',mrp:'MRP'};
    var changes = Object.keys(entry.changes).map(function(field) {
      var val   = entry.changes[field];
      var label = labels[field] || field;
      var dir   = val.to > val.from ? '▲' : '▼';
      var color = field === 'costPrice' ? (val.to > val.from ? '#dc2626' : '#16a34a') : (val.to > val.from ? '#16a34a' : '#dc2626');
      return '<span style="margin-right:8px;"><b>' + label + ':</b> ₹' + val.from + ' <span style="color:' + color + ';font-weight:700;">' + dir + ' ₹' + val.to + '</span></span>';
    }).join('');
    return '<div style="padding:8px 0;border-bottom:1px solid #f1f5f9;font-size:12px;">'
      + '<div style="color:var(--muted);margin-bottom:3px;">' + date + ' · <b>' + escapeHtml(entry.by||'') + '</b></div>'
      + '<div>' + changes + '</div>'
      + '</div>';
  }).join('');
  return '<div style="max-height:280px;overflow-y:auto;">' + phRows + '</div>';
}

function openPriceHistoryModal(partId) {
  const part = getPartById(partId);
  if (!part) return;
  showModal(`📈 Price History — ${part.name} (${part.partNo})`,
    renderPriceHistory(partId) +
    '<button class="btn btn-outline mt-2" onclick="closeModal()">Close</button>'
  );
}

async function xlsSaveRow(partId) {
  const d = _xlsDirty[partId];
  if (!d) return;
  const p = getPartById(partId);
  if (!p) return;
  const patchFields = {};
  if (d.costPrice    !== undefined) patchFields.costPrice    = d.costPrice;
  if (d.mrp          !== undefined) patchFields.mrp          = d.mrp;
  if (d.instantPrice !== undefined) patchFields.instantPrice = d.instantPrice;
  if (d.creditPrice  !== undefined) patchFields.creditPrice  = d.creditPrice;
  if (d.location     !== undefined) patchFields.location     = d.location;
  if (d.minStock     !== undefined) patchFields.minStock     = d.minStock;
  if (d.stock        !== undefined) patchFields.stock        = d.stock;
  // Record price change history before applying
  recordPriceHistory(p, patchFields, state.session?.name);
  const cloud = Object.keys(patchFields).length
    ? await cloudPersistPartFields(partId, patchFields)
    : await cloudPersistPart(Object.assign({}, p, d, { updatedAt: Date.now() }));
  Object.assign(p, d);
  p.updatedAt = Date.now();
  delete _xlsDirty[partId];
  await save();
  const row = document.getElementById('xlsrow-' + partId);
  if (row) { row.className = 'xls-saved'; setTimeout(() => xlsRender(), 700); }
  xlsUpdateUnsavedBadge();
  logActivity(`SKU updated: ${p.partNo} "${p.name}" — Cost:₹${p.costPrice} Retail:₹${p.instantPrice} WS:₹${p.creditPrice}${p.location?' Rack:'+p.location:''}`, 'info');
  _partSaveToast(cloud, `✅ ${p.name} saved`);
}

async function xlsSaveAll() {
  const ids = Object.keys(_xlsDirty);
  if (!ids.length) return;
  let cloudOk = 0;
  let cloudPending = 0;
  for (const partId of ids) {
    const d = _xlsDirty[partId];
    const p = getPartById(partId);
    if (!p || !d) continue;
    const patchFields = {};
    if (d.costPrice    !== undefined) patchFields.costPrice    = d.costPrice;
    if (d.mrp          !== undefined) patchFields.mrp          = d.mrp;
    if (d.instantPrice !== undefined) patchFields.instantPrice = d.instantPrice;
    if (d.creditPrice  !== undefined) patchFields.creditPrice  = d.creditPrice;
    if (d.location     !== undefined) patchFields.location     = d.location;
    if (d.minStock     !== undefined) patchFields.minStock     = d.minStock;
    if (d.stock        !== undefined) patchFields.stock        = d.stock;
    const cloud = Object.keys(patchFields).length
      ? await cloudPersistPartFields(partId, patchFields)
      : await cloudPersistPart(Object.assign({}, p, d, { updatedAt: Date.now() }));
    if (cloud.cloud) cloudOk++; else cloudPending++;
    // Record price history for each updated part
    recordPriceHistory(p, patchFields, state.session?.name);
    Object.assign(p, d);
    p.updatedAt = Date.now();
  }
  _xlsDirty = {};
  await save();
  xlsRender();
  xlsUpdateUnsavedBadge();
  logActivity(`Bulk price update: ${ids.length} SKUs updated`, 'info');
  if (cloudPending === 0) toast(`✅ ${ids.length} SKUs saved to cloud`);
  else if (cloudOk === 0) toast(`✅ ${ids.length} SKUs saved on this device — cloud sync pending`);
  else toast(`✅ ${ids.length} SKUs saved (${cloudOk} cloud, ${cloudPending} pending)`);
}

function xlsUpdateUnsavedBadge() {
  const count = Object.keys(_xlsDirty).length;
  const badge = document.getElementById('xls-unsaved-count');
  const btn   = document.getElementById('xls-save-all-btn');
  if (badge) { badge.textContent = count + ' unsaved'; badge.style.display = count > 0 ? 'inline-block' : 'none'; }
  if (btn)   btn.disabled = count === 0;
}

// ---- Stock badge renderer for live SKU editor ----
function xlsStockBadge(p) {
  if (p.stock === 0)                             return `<span class="xls-stock-out">OUT</span>`;
  if (p.minStock > 0 && p.stock <= p.minStock)   return `<span class="xls-stock-low">⚠ ${p.stock}</span>`;
  return `<span class="xls-stock-ok">${p.stock}</span>`;
}

// ---- Refresh stock cells in SKU editor after a sale ----
function xlsRefreshStockCells(partIds) {
  if (!partIds || !partIds.length) return;
  partIds.forEach(partId => {
    const p = getPartById(partId);
    if (!p) return;
    // Update the editable stock input
    const inp = document.getElementById('xlsc-' + partId + '-stock');
    if (inp && !_xlsDirty[partId]?.stock) {
      inp.value = p.stock;
      inp.dataset.orig = p.stock;
      inp.style.color = p.stock === 0 ? 'var(--danger)' : (p.minStock > 0 && p.stock <= p.minStock) ? 'var(--warning)' : 'var(--success)';
    }
  });
}

// ---- Min-stock alert modal ----
function showStockAlertModal(alerts, trigger) {
  if (!alerts || !alerts.length) return;
  const outItems = alerts.filter(a => a.status === 'out');
  const lowItems = alerts.filter(a => a.status === 'low');
  const html = `
    <div style="background:linear-gradient(135deg,#fff1f2,#fee2e2);border-radius:10px;padding:12px;margin-bottom:12px;">
      <div style="font-size:13px;color:var(--muted);">After: <b>${trigger}</b></div>
      <div style="font-size:14px;font-weight:700;margin-top:4px;">
        ${outItems.length ? `🚨 ${outItems.length} SKU${outItems.length>1?'s':''} OUT OF STOCK` : ''}
        ${outItems.length && lowItems.length ? ' &nbsp;·&nbsp; ' : ''}
        ${lowItems.length ? `⚠️ ${lowItems.length} SKU${lowItems.length>1?'s':''} LOW STOCK` : ''}
      </div>
    </div>
    <div>
      ${alerts.map(a => `
        <div class="stock-alert-item ${a.status}">
          <div>
            <div style="font-weight:600;">${a.name}</div>
            <div style="font-size:11px;color:var(--muted);font-family:monospace;">${a.partNo}</div>
          </div>
          <div style="text-align:right;">
            ${a.status === 'out'
              ? `<span class="tag danger" style="font-size:12px;">OUT OF STOCK</span>`
              : `<span class="tag warn" style="font-size:12px;">Stock: ${a.stock} &nbsp;/&nbsp; Min: ${a.minStock}</span>`}
          </div>
        </div>`).join('')}
    </div>
    <div style="margin-top:12px;display:flex;gap:8px;flex-wrap:wrap;">
      <button class="btn btn-sm" style="background:var(--ai);" onclick="closeModal();goTab('brain');showBrainSection('reorder')">📦 Open Reorder List</button>
      <button class="btn btn-sm btn-outline" onclick="closeModal();goTab('reports');showReport('priceeditor');xlsSwitchTab('edit',document.querySelector('.xls-tab'))">📋 Open SKU Editor</button>
      <button class="btn btn-sm btn-outline" onclick="closeModal()">Dismiss</button>
    </div>`;
  showModal('⚠️ Stock Alert', html);
}

// ---- Quick stock +/- adjustment (saves immediately) ----
function xlsStockAdj(partId, delta) {
  const p = getPartById(partId);
  if (!p) return;
  const d = _xlsDirty[partId] || {};
  const current = d.stock !== undefined ? d.stock : p.stock;
  const newStock = Math.max(0, current + delta);
  if (!_xlsDirty[partId]) _xlsDirty[partId] = {};
  _xlsDirty[partId].stock = newStock;
  const inp = document.getElementById('xlsc-' + partId + '-stock');
  if (inp) {
    inp.value = newStock;
    inp.classList.add('changed');
    inp.style.color = newStock === 0 ? 'var(--danger)' : (p.minStock > 0 && newStock <= p.minStock) ? 'var(--warning)' : 'var(--success)';
  }
  const row = document.getElementById('xlsrow-' + partId);
  if (row) {
    row.classList.add('xls-dirty');
    const saveCell = row.querySelector('td:nth-last-child(2)');
    if (saveCell && !saveCell.querySelector('.xls-save-row-btn')) {
      saveCell.innerHTML = `<button class="xls-save-row-btn" onclick="xlsSaveRow('${partId}')">✓</button>`;
    }
  }
  xlsUpdateUnsavedBadge();
}

// ---- Delete a part from SKU editor ----
function xlsDeletePart(partId) {
  const p = getPartById(partId);
  if (!p) return;
  const hasSales = state.sales.some(s => s.items.some(i => i.partId === partId));
  const msg = hasSales
    ? `"${p.name}" has sales history. It will be removed from inventory but sale records are kept.\n\nDelete anyway?`
    : `Delete "${p.name}" (${p.partNo})? This cannot be undone.`;
  if (!confirm(msg)) return;
  delete _xlsDirty[partId];
  state.parts = state.parts.filter(x => x.id !== partId);
  save();
  xlsRender();
  xlsUpdateUnsavedBadge();
  logActivity(`Part deleted: ${p.partNo} "${p.name}"`, 'warn');
  toast(`🗑 ${p.name} removed`);
}

// ---- Add Part modal from SKU editor — delegates to unified modal ----
function xlsAddPartModal() {
  openAddPart({ context: 'SKU Editor' });
}

// ---- Tab switching ----
let _xlsSearchTimer = null;
function xlsSearchDebounced() {
  clearTimeout(_xlsSearchTimer);
  _xlsSearchTimer = setTimeout(() => xlsRender(), window._isMobile ? 500 : 250);
}

function xlsSwitchTab(name, el) {
  document.querySelectorAll('.xls-tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.xls-tab-panel').forEach(p => p.classList.remove('active'));
  if (el) el.classList.add('active');
  const panel = document.getElementById('xls-panel-' + name);
  if (panel) panel.classList.add('active');
  if (name === 'edit') xlsRender();
}

// ---- Download helpers ----
function xlsDownloadTemplate() {
  const header = 'PartNo,Name,Brand,MachineType,Category,CostPrice,MRP,InstantPrice,CreditPrice,Stock,Rack';
  const rows = state.parts.map(p =>
    [p.partNo,p.name,p.brand||'',p.machineType||'',p.category||'','','','','','',p.location||''].map(v => `"${String(v).replace(/"/g,'""')}"`).join(',')
  );
  impDownloadCSV([header, ...rows].join('\n'), 'sku_template.csv');
}

function xlsDownloadFilled() {
  const header = 'PartNo,Name,Brand,MachineType,Category,CostPrice,MRP,InstantPrice,CreditPrice,Stock,Rack';
  const rows = state.parts.map(p =>
    [p.partNo,p.name,p.brand||'',p.machineType||'',p.category||'',p.costPrice||0,p.mrp||0,p.instantPrice||0,p.creditPrice||0,p.stock||0,p.location||''].map(v => `"${String(v).replace(/"/g,'""')}"`).join(',')
  );
  impDownloadCSV([header, ...rows].join('\n'), 'current_prices.csv');
}

function impDownloadCSV(csv, filename) {
  const blob = new Blob([csv], {type:'text/csv'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
  setTimeout(function(){ URL.revokeObjectURL(a.href); }, 5000); // free blob after download starts
}

// ============================================================
// BULK IMPORT ENGINE
// ============================================================
let _impRawRows = [];    // [{col0:val, col1:val, ...}]
let _impHeaders = [];    // detected header names
let _impMapped  = [];    // cleaned + mapped rows ready for preview
let _impSource  = 'file'; // 'file' or 'paste'

// ---- Column name aliases (smart auto-mapping) ----
const IMP_ALIASES = {
  partno:       ['partno','part no','part_no','part number','part#','sku','item code','itemcode','code'],
  name:         ['name','part name','item name','description','desc','item description','product name'],
  brand:        ['brand','make','manufacturer','mfr'],
  machinetype:  ['machine','machinetype','machine type','type','vehicle','equipment'],
  category:     ['category','cat','group','item group','sub category'],
  costprice:    ['cost','costprice','cost price','purchase price','buy price','landed cost','cp','buying price'],
  mrp:          ['mrp','max retail price','maximum retail price','list price','msrp'],
  instantprice: ['retail','retail price','instantprice','selling price','sale price','price','sp','rate'],
  creditprice:  ['wholesale','wholesale price','creditprice','credit price','ws price','ws','dealer price','trade price'],
  stock:        ['stock','qty','quantity','current stock','opening stock','inventory'],
  location:     ['rack','location','bin','shelf','rack location','storage','warehouse location','bin location']
};

function impGuessField(colName) {
  const key = colName.toLowerCase().replace(/[^a-z0-9]/g,'');
  for (const [field, aliases] of Object.entries(IMP_ALIASES)) {
    if (aliases.some(a => a.replace(/[^a-z0-9]/g,'') === key || key.includes(a.replace(/[^a-z0-9]/g,'')) || a.replace(/[^a-z0-9]/g,'').includes(key))) {
      return field;
    }
  }
  return '';
}

// ---- Parse CSV text ----
function impParseCSV(text) {
  const lines = text.trim().split(/\r?\n/);
  if (!lines.length) return {headers:[], rows:[]};
  const sep = lines[0].includes('\t') ? '\t' : ',';
  function splitLine(line) {
    const cells = []; let cur = ''; let inQ = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (c === '"') { inQ = !inQ; continue; }
      if (c === sep && !inQ) { cells.push(cur.trim()); cur = ''; }
      else cur += c;
    }
    cells.push(cur.trim());
    return cells;
  }
  const headers = splitLine(lines[0]);
  const rows = lines.slice(1).filter(l => l.trim()).map(l => {
    const cells = splitLine(l);
    const obj = {};
    headers.forEach((h, i) => obj[h] = cells[i] || '');
    return obj;
  });
  return {headers, rows};
}

// ---- Handle file upload ----
async function impHandleFile(file) {
  if (!file) return;
  _impSource = 'file';
  impReset();
  toast('📂 Reading file…');

  let text = '';
  if (file.name.match(/\.xlsx?$/i)) {
    // Use SheetJS if available, else ask user to save as CSV
    await _loadLib('xlsx');
    if (typeof XLSX !== 'undefined') {
      const buf = await file.arrayBuffer();
      const wb = XLSX.read(buf, {type:'array'});
      const ws = wb.Sheets[wb.SheetNames[0]];
      text = XLSX.utils.sheet_to_csv(ws);
    } else {
      toast('⚠️ For .xlsx files, please save as CSV first or use the Paste tab');
      return;
    }
  } else {
    text = await file.text();
  }

  const {headers, rows} = impParseCSV(text);
  if (!headers.length || !rows.length) { toast('❌ Could not read file. Try CSV format.'); return; }
  _impHeaders = headers;
  _impRawRows = rows;
  impShowColMap();
}

// ---- Show column mapping UI ----
function impShowColMap() {
  const wrap = document.getElementById('imp-col-map-wrap');
  const table = document.getElementById('imp-col-map-table');
  wrap.style.display = 'block';

  const fields = Object.keys(IMP_ALIASES);
  const fieldLabels = {partno:'Part No *',name:'Name',brand:'Brand',machinetype:'Machine Type',category:'Category',costprice:'Cost Price ₹',mrp:'MRP ₹',instantprice:'Retail Price ₹',creditprice:'Wholesale Price ₹',stock:'Stock Qty',location:'Rack / Location'};

  table.innerHTML = `<thead><tr><th>App Field</th><th>Your Column</th><th>Sample Value</th></tr></thead><tbody>` +
    fields.map(field => {
      const guess = _impHeaders.find(h => impGuessField(h) === field) || '';
      const sample = guess && _impRawRows[0] ? (_impRawRows[0][guess] || '—') : '—';
      return `<tr>
        <td style="font-weight:600;color:var(--text);">${fieldLabels[field]}</td>
        <td><select id="imp-map-${field}" onchange="impUpdateSample('${field}')">
          <option value="">— skip —</option>
          ${_impHeaders.map(h => `<option value="${h}" ${h === guess ? 'selected' : ''}>${h}</option>`).join('')}
        </select></td>
        <td id="imp-sample-${field}" style="font-size:11px;color:var(--muted);">${sample}</td>
      </tr>`;
    }).join('') + `</tbody>`;
}

function impUpdateSample(field) {
  const sel = document.getElementById('imp-map-' + field);
  const col = sel?.value;
  const sample = col && _impRawRows[0] ? (_impRawRows[0][col] || '—') : '—';
  const el = document.getElementById('imp-sample-' + field);
  if (el) el.textContent = sample;
}

// ---- Run preview with data cleaning ----
function impRunPreview() {
  // Gather mapping
  const map = {};
  Object.keys(IMP_ALIASES).forEach(field => {
    const sel = document.getElementById('imp-map-' + field);
    if (sel?.value) map[field] = sel.value;
  });
  if (!map.partno) { toast('❌ Please map the Part No column'); return; }
  _impMapped = impCleanRows(_impRawRows, map);
  impShowPreview(_impMapped, 'imp-preview-wrap', 'imp-preview-head', 'imp-preview-body', 'imp-stat-new', 'imp-stat-update', 'imp-stat-skip');
}

// ---- Parse paste text ----
function impParsePaste() {
  const text = document.getElementById('imp-paste-area')?.value.trim();
  if (!text) { toast('Paste some data first'); return; }
  _impSource = 'paste';
  const {headers, rows} = impParseCSV(text);
  if (!headers.length || !rows.length) { toast('❌ Could not parse. Check format.'); return; }
  // Auto-map
  const map = {};
  headers.forEach(h => { const f = impGuessField(h); if (f) map[f] = h; });
  if (!map.partno) { toast('❌ No Part No column detected. Add a column named PartNo or Part No.'); return; }
  _impMapped = impCleanRows(rows, map);
  impShowPreview(_impMapped, 'imp-paste-preview', 'imp-paste-head', 'imp-paste-body', 'imp-paste-stat-new', 'imp-paste-stat-update', 'imp-paste-stat-skip');
}

// ---- Clean + classify rows ----
function impCleanRows(rawRows, map) {
  return rawRows.map(row => {
    const get = (f) => (map[f] ? (row[map[f]] || '') : '');
    const partNo = get('partno').toString().trim().replace(/\s+/g,' ');
    if (!partNo) return {_status:'skip', _reason:'No Part No', partNo:''};

    const name      = get('name').trim();
    const brand     = get('brand').trim();
    const machine   = get('machinetype').toLowerCase().trim();
    const category  = get('category').trim();
    const costPrice = Math.round(+get('costprice').toString().replace(/[^\d.]/g,'')) || 0;
    const mrp       = Math.round(+get('mrp').toString().replace(/[^\d.]/g,'')) || 0;
    const retail    = Math.round(+get('instantprice').toString().replace(/[^\d.]/g,'')) || 0;
    const ws        = Math.round(+get('creditprice').toString().replace(/[^\d.]/g,'')) || 0;
    const stock     = Math.round(+get('stock').toString().replace(/[^\d.]/g,'')) || undefined;
    const rack      = get('location').toUpperCase().trim();

    // Validate machine type
    const validMachines = ['tractor','harvester','jcb','rotavator','other'];
    const machineType = validMachines.includes(machine) ? machine : (machine ? 'other' : undefined);

    // Check if SKU exists
    const existing = state.parts.find(p => p.partNo.toLowerCase() === partNo.toLowerCase());
    const status = existing ? 'update' : 'new';

    return {_status: status, _partId: existing?.id, partNo, name, brand, machineType, category, costPrice, mrp, instantPrice: retail, creditPrice: ws, stock, location: rack};
  });
}

// ---- Show preview table ----
function impShowPreview(rows, wrapId, headId, bodyId, newId, updId, skipId) {
  const wrap = document.getElementById(wrapId);
  const head = document.getElementById(headId);
  const body = document.getElementById(bodyId);
  if (!wrap || !head || !body) return;

  const newCount  = rows.filter(r => r._status === 'new').length;
  const updCount  = rows.filter(r => r._status === 'update').length;
  const skipCount = rows.filter(r => r._status === 'skip').length;

  document.getElementById(newId).textContent  = newCount + ' New';
  document.getElementById(updId).textContent  = updCount + ' Updates';
  document.getElementById(skipId).textContent = skipCount + ' Skipped';

  head.innerHTML = '<th>Status</th><th>Part No</th><th>Name</th><th>Brand</th><th>Cost ₹</th><th>MRP ₹</th><th>Retail ₹</th><th>WS ₹</th><th>Rack</th><th>Stock</th>';
  body.innerHTML = rows.slice(0, 200).map(r => {
    if (r._status === 'skip') return `<tr class="imp-skip"><td><span class="imp-badge skip">SKIP</span></td><td colspan="9" style="color:var(--muted);">${r.partNo || '—'} · ${r._reason || 'Invalid'}</td></tr>`;
    const badge = r._status === 'new' ? '<span class="imp-badge new">NEW</span>' : '<span class="imp-badge update">UPDATE</span>';
    return `<tr class="imp-${r._status}">
      <td>${badge}</td>
      <td style="font-family:monospace;font-size:11px;font-weight:700;">${r.partNo}</td>
      <td>${r.name||'—'}</td>
      <td style="color:var(--muted);">${r.brand||'—'}</td>
      <td style="text-align:right;">${r.costPrice||'—'}</td>
      <td style="text-align:right;">${r.mrp||'—'}</td>
      <td style="text-align:right;color:var(--primary);font-weight:600;">${r.instantPrice||'—'}</td>
      <td style="text-align:right;">${r.creditPrice||'—'}</td>
      <td style="font-family:monospace;">${r.location||'—'}</td>
      <td style="text-align:center;">${r.stock !== undefined ? r.stock : '—'}</td>
    </tr>`;
  }).join('');

  wrap.style.display = 'block';
  wrap.scrollIntoView({behavior:'smooth', block:'nearest'});
}

// ---- Apply to price editor (marks as dirty for review) ----
function impApplyToEditor() { _impApply(_impMapped); }
function impApplyPaste()    { _impApply(_impMapped); }

function _impApply(rows) {
  let applied = 0, added = 0;
  const _newParts = [];
  rows.forEach(r => {
    if (r._status === 'skip') return;
    if (r._status === 'update' && r._partId) {
      // Mark as dirty in price editor
      const changes = {};
      if (r.costPrice)    changes.costPrice    = r.costPrice;
      if (r.mrp)          changes.mrp          = r.mrp;
      if (r.instantPrice) changes.instantPrice = r.instantPrice;
      if (r.creditPrice)  changes.creditPrice  = r.creditPrice;
      if (r.location)     changes.location     = r.location;
      if (r.stock !== undefined) changes.stock = r.stock;
      if (Object.keys(changes).length) { _xlsDirty[r._partId] = {...(_xlsDirty[r._partId]||{}), ...changes}; applied++; }
    } else if (r._status === 'new' && r.partNo) {
      // Add new part directly
      const newPart = {
        id: uid(), partNo: r.partNo, name: r.name || r.partNo,
        brand: r.brand || '', machineType: r.machineType || 'other',
        category: r.category || '', models: '',
        costPrice: r.costPrice || 0, mrp: r.mrp || 0,
        instantPrice: r.instantPrice || 0, creditPrice: r.creditPrice || 0,
        stock: r.stock !== undefined ? r.stock : 0, minStock: 0,
        location: r.location || '', supplierName: '', supplierPhone: '',
        addedAt: Date.now()
      };
      state.parts.push(newPart);
      _newParts.push(newPart);
      added++;
    }
  });
  if (applied || added) {
    save();
    // Write each new part to the per-doc cloud collection so other devices sync them.
    // save()/firebasePush skip the 'parts' blob in per-doc mode, so without this
    // bulk-imported parts would only exist on this device (same gap as the catalogue).
    if (_newParts.length && typeof cloudPersistPart === 'function') {
      _newParts.forEach(function(p){ try { cloudPersistPart(p); } catch(e){} });
    }
    xlsUpdateUnsavedBadge();
    xlsSwitchTab('edit', document.querySelector('.xls-tab'));
    setTimeout(() => xlsRender(), 100);
    toast(`✅ ${added} new SKUs added · ${applied} marked for price update`);
    logActivity(`Bulk import: ${added} new + ${applied} price updates applied`, 'info');
  } else {
    toast('Nothing to apply');
  }
  impReset();
}

function impReset() {
  _impRawRows = []; _impHeaders = []; _impMapped = []; _impSource = 'file';
  ['imp-col-map-wrap','imp-preview-wrap','imp-paste-preview'].forEach(id => {
    const el = document.getElementById(id); if (el) el.style.display = 'none';
  });
  const fi = document.getElementById('imp-file-input'); if (fi) fi.value = '';
}

// Called when Ret.Disc% or WS.Disc% cell changes — back-calculates price from MRP
function xlsDiscChange(partId, type, input) {
  const discPct = +input.value;
  if (isNaN(discPct) || discPct < 0 || discPct > 99) return;
  const p = getPartById(partId);
  if (!p) return;
  const d = _xlsDirty[partId] || {};
  const mrp = d.mrp !== undefined ? d.mrp : p.mrp;
  if (!mrp) return;
  const newPrice = Math.round(mrp * (1 - discPct / 100));
  const field = type === 'retail' ? 'instantPrice' : 'creditPrice';
  const priceInput = document.getElementById(`xlsc-${partId}-${field}`);
  if (priceInput) {
    priceInput.value = newPrice;
    priceInput.classList.add('changed');
  }
  if (!_xlsDirty[partId]) _xlsDirty[partId] = {};
  _xlsDirty[partId][field] = newPrice;
  // Mark disc% input as changed too
  const origDisc = type === 'retail'
    ? (p.mrp > 0 ? (((p.mrp - p.instantPrice) / p.mrp) * 100).toFixed(1) : '0.0')
    : (p.mrp > 0 ? (((p.mrp - p.creditPrice) / p.mrp) * 100).toFixed(1) : '0.0');
  if (+discPct !== +origDisc) input.classList.add('changed');
  else input.classList.remove('changed');
  // Refresh both margin pills
  const cost = d.costPrice !== undefined ? d.costPrice : p.costPrice;
  const curRetail = type === 'retail' ? newPrice : (d.instantPrice !== undefined ? d.instantPrice : p.instantPrice);
  const curWs     = type === 'wholesale' ? newPrice : (d.creditPrice !== undefined ? d.creditPrice : p.creditPrice);
  const retPct = cost > 0 ? ((curRetail - cost) / cost * 100) : 0;
  const retAmt = curRetail - cost;
  const retCls = retPct >= 30 ? 'xls-margin-high' : retPct >= 15 ? 'xls-margin-mid' : 'xls-margin-low';
  const retPill = document.getElementById('xls-retmargin-' + partId);
  if (retPill) { retPill.className = 'xls-margin-pill ' + retCls; retPill.innerHTML = retPct.toFixed(0) + '%<span class="xls-margin-amt">₹' + retAmt + '</span>'; }
  const wsPct = cost > 0 ? ((curWs - cost) / cost * 100) : 0;
  const wsAmt = curWs - cost;
  const wsCls = wsPct >= 25 ? 'xls-margin-ws-high' : wsPct >= 12 ? 'xls-margin-ws-mid' : 'xls-margin-ws-low';
  const wsPill = document.getElementById('xls-wsmargin-' + partId);
  if (wsPill) { wsPill.className = 'xls-margin-pill ' + wsCls; wsPill.innerHTML = wsPct.toFixed(0) + '%<span class="xls-margin-amt">₹' + wsAmt + '</span>'; }
  const row = document.getElementById('xlsrow-' + partId);
  if (row) row.classList.add('xls-dirty');
  // Refresh save btn in row
  const saveCell = row?.querySelector('td:last-child');
  if (saveCell) saveCell.innerHTML = `<button class="xls-save-row-btn" onclick="xlsSaveRow('${partId}')">✓</button>`;
  xlsUpdateUnsavedBadge();
}

// Bulk discount tool — applies % off MRP to all currently filtered SKUs
function xlsApplyDiscount(type) {
  const pctId = type === 'retail' ? 'disc-retail-pct' : 'disc-ws-pct';
  const discPct = +document.getElementById(pctId)?.value;
  if (!discPct || discPct <= 0 || discPct >= 100) { toast('Enter a valid discount % (1–99)'); return; }
  const parts = xlsGetFiltered();
  if (!parts.length) { toast('No parts visible to apply discount to'); return; }
  const field = type === 'retail' ? 'instantPrice' : 'creditPrice';
  let applied = 0;
  parts.forEach(p => {
    const d = _xlsDirty[p.id] || {};
    const mrp = d.mrp !== undefined ? d.mrp : p.mrp;
    if (!mrp) return;
    const newPrice = Math.round(mrp * (1 - discPct / 100));
    if (!_xlsDirty[p.id]) _xlsDirty[p.id] = {};
    _xlsDirty[p.id][field] = newPrice;
    applied++;
  });
  // Show preview
  const prev = document.getElementById('disc-preview');
  if (prev) {
    prev.style.display = 'block';
    prev.innerHTML = `✅ <b>${discPct}% off MRP</b> applied to <b>${applied} SKUs</b> (${type} price). Review rows highlighted in yellow, then click <b>💾 Save All</b>.`;
  }
  xlsRender();
  xlsUpdateUnsavedBadge();
  toast(`${discPct}% discount applied to ${applied} ${type} prices`);
}

// Toggle retail customer field based on whether wholesale customer is selected
function toggleRetailField() {
  const custSel = document.getElementById('sale-customer');
  const rcField = document.getElementById('retail-cust-field');
  if (!custSel || !rcField) return;
  rcField.style.display = custSel.value ? 'none' : 'block';
}

// Auto-fill mechanic rate when mechanic is selected
function autoFillMechRate() {
  const mechId = document.getElementById('sale-mechanic')?.value;
  const rateInput = document.getElementById('sale-mech-rate');
  const preview = document.getElementById('mech-comm-preview');
  if (!mechId) { if(rateInput) rateInput.value=''; if(preview) preview.textContent=''; return; }
  const mech = state.mechanics.find(m=>m.id===mechId);
  if (mech && rateInput) {
    rateInput.value = mech.defaultRate;
    updateMechPreview();
  }
}

function updateMechPreview() {
  const rate = +document.getElementById('sale-mech-rate')?.value || 0;
  const preview = document.getElementById('mech-comm-preview');
  if (!preview) return;
  if (!rate) { preview.textContent = ''; return; }
  const total = state.cart.reduce((a,i) => { const p=getPartById(i.partId); return a+(getPrice(p)*i.qty); }, 0);
  const comm = Math.round(total * rate / 100);
  preview.textContent = comm > 0 ? `= ${rs(comm)} commission` : '';
}

// Customer base filter
let _custFilter = 'all';
function setCustFilter(f, evt) {
  _custFilter = f;
  document.querySelectorAll('#rpt-customers .chip').forEach(c=>c.classList.remove('active'));
  if (evt&&evt.target) evt.target.classList.add('active');
  renderCustomerBase();
}

// Mechanic list
function openMechanicList() {
  if (!state.mechanics.length) { showModal('Mechanics', '<div class="empty">No mechanics added yet</div><button class="btn mt-2" onclick="closeModal();openAddMechanic()">+ Add First Mechanic</button>'); return; }
  showModal('🔧 All Mechanics', state.mechanics.map(m=>`
    <div class="part-item">
      <div class="part-row">
        <div><div class="bold">${m.name}</div><small>${m.phone||'No phone'} · Default: ${m.defaultRate}%</small></div>
        <div class="btn-row">
          ${m.phone?`<a href="https://wa.me/${m.phone}" target="_blank" style="background:#25d366;color:white;padding:6px 10px;border-radius:8px;font-size:12px;text-decoration:none;">📲</a>`:''}
          <button class="btn btn-sm btn-danger" onclick="deleteMechanic('${m.id}')">✕</button>
        </div>
      </div>
    </div>`).join('') + `<button class="btn mt-2" onclick="closeModal();openAddMechanic()">+ Add Mechanic</button>`);
}

function deleteMechanic(id) {
  if (!confirm('Remove this mechanic?')) return;
  state.mechanics = state.mechanics.filter(m=>m.id!==id);
  window._ownerDeleteInProgress = true;
  save().finally(() => { window._ownerDeleteInProgress = false; });
  closeModal(); toast('Mechanic removed');
}

// AI write broadcast from reports tab
async function aiWriteBroadcastRpt() {
  const ta = document.getElementById('bc-msg');
  const seg = document.getElementById('bc-segment')?.value || 'all';
  ta.value = 'Generating…';
  let msg;
  try {
    if (getAIKey() && getAIKey().startsWith('sk-')) {
      msg = await aiClaude(`Write a WhatsApp broadcast for ${seg} customers of ${state.settings.shopName} (spare parts shop). Keep under 80 words, friendly Hindi/English mix. Include shop phone ${state.settings.shopPhone||'[phone]'}.`);
    } else {
      msg = `*${state.settings.shopName}*\n\nNamaskar! 🙏\n\nNew stock available — tractor, harvester, JCB parts at best prices.\n\nCall or WhatsApp to order.\n📞 ${state.settings.shopPhone||'[phone]'}`;
    }
  } catch(e) { msg = `Error: ${e.message}`; }
  ta.value = msg;
}

function renderGrowthChart(months, evt) {
  if (evt) { document.querySelectorAll('#chart-period-chips .chip').forEach(c=>c.classList.remove('active')); evt.target.classList.add('active'); }
  const now = new Date();

  // Single-pass: build month→category→total index (was O(months×categories×sales×items))
  const monthCatTotals = {};
  state.sales.forEach(s => {
    const mo = s.date.slice(0,7);
    if (!monthCatTotals[mo]) monthCatTotals[mo] = {};
    s.items.forEach(i => {
      const p = getPartById(i.partId);
      const cat = p ? (CATEGORIES.slice(0,-1).includes(p.machineType) ? p.machineType : 'other') : 'other';
      monthCatTotals[mo][cat] = (monthCatTotals[mo][cat]||0) + i.price * i.qty;
    });
  });

  const labels = [];
  const data = {};
  CATEGORIES.forEach(c => data[c] = []);
  for (let m = months-1; m >= 0; m--) {
    const d = new Date(now.getFullYear(), now.getMonth()-m, 1);
    const key = d.toISOString().slice(0,7);
    labels.push(d.toLocaleString('en-IN',{month:'short',year:'2-digit'}));
    CATEGORIES.forEach(cat => { data[cat].push((monthCatTotals[key]||{})[cat] || 0); });
  }

  const maxVal = Math.max(...CATEGORIES.flatMap(c=>data[c]), 1);
  const chartH = 140;
  const barsHTML = labels.map((lbl, mi) => {
    const bars = CATEGORIES.map(cat => {
      const h = Math.max(4, Math.round(data[cat][mi]/maxVal*chartH));
      return `<div class="bar" style="height:${h}px;background:${CAT_COLORS[cat]};" title="${cat}: ${rs(data[cat][mi])}"></div>`;
    }).join('');
    return `<div class="bar-group">${bars}<div class="bar-label">${lbl}</div></div>`;
  }).join('');
  document.getElementById('growth-chart').innerHTML = `<div class="bar-chart">${barsHTML}</div>`;
  document.getElementById('chart-legend').innerHTML = CATEGORIES.map(c=>`<span><span class="legend-dot" style="background:${CAT_COLORS[c]};"></span> ${c.charAt(0).toUpperCase()+c.slice(1)}</span>`).join('');

  // Category breakdown this month — free since index is already built
  const thisMonth = now.toISOString().slice(0,7);
  const thisMonthData = monthCatTotals[thisMonth] || {};
  const breakdown = CATEGORIES.map(cat => ({cat, total: thisMonthData[cat]||0})).filter(x=>x.total>0).sort((a,b)=>b.total-a.total);
  const grandTotal = breakdown.reduce((a,x)=>a+x.total,0);
  document.getElementById('category-breakdown').innerHTML = breakdown.length === 0
    ? '<div class="empty">No sales this month yet</div>'
    : breakdown.map(x => {
        const pct = grandTotal>0?(x.total/grandTotal*100).toFixed(0):0;
        return `<div style="margin-bottom:10px;"><div class="flex-between" style="font-size:13px;"><span style="font-weight:600;">${x.cat.toUpperCase()}</span><span>${rs(x.total)} (${pct}%)</span></div><div class="comm-bar"><div class="comm-bar-fill" style="width:${pct}%;background:${CAT_COLORS[x.cat]||'var(--primary)'}"></div></div></div>`;
      }).join('');
}

function getCustomerTier(totalSpent, visitCount) {
  if (totalSpent >= 100000 || visitCount >= 20) return {label:'Platinum',cls:'tier-platinum'};
  if (totalSpent >= 50000 || visitCount >= 10) return {label:'Gold',cls:'tier-gold'};
  if (totalSpent >= 10000 || visitCount >= 3) return {label:'Silver',cls:'tier-silver'};
  return {label:'New',cls:'tier-new'};
}

function renderCustomerBase() {
  const q = (document.getElementById('cust-base-search')?.value||'').toLowerCase();
  const filter = _custFilter || 'all';

  // Single-pass sales scan — O(sales) not O(customers × sales)
  const custData = {};
  state.sales.forEach(s => {
    if (!s.customerId) return;
    if (!custData[s.customerId]) custData[s.customerId] = {totalSpent:0, visits:0, lastSale:null, type:'wholesale'};
    custData[s.customerId].totalSpent += s.total;
    custData[s.customerId].visits++;
    if (!custData[s.customerId].lastSale || s.date > custData[s.customerId].lastSale) {
      custData[s.customerId].lastSale = s.date;
    }
  });
  state.retailCustomers.forEach(c => {
    custData[c.id] = {totalSpent:c.totalSpent||0, visits:c.visits||0, lastSale:c.lastVisit, type:'retail'};
  });

  let allCusts = [
    ...state.customers.map(c=>({...c, ...custData[c.id], isWholesale:true})),
    ...state.retailCustomers.map(c=>({...c, ...custData[c.id], isWholesale:false, type:'retail'}))
  ];

  if (q) allCusts = allCusts.filter(c=>(c.name||'').toLowerCase().includes(q)||(c.phone||'').includes(q));
  if (filter==='wholesale') allCusts = allCusts.filter(c=>c.isWholesale);
  if (filter==='retail') allCusts = allCusts.filter(c=>!c.isWholesale);
  if (filter==='platinum') allCusts = allCusts.filter(c=>getCustomerTier(c.totalSpent||0,c.visits||0).label==='Platinum');
  if (filter==='inactive') allCusts = allCusts.filter(c=>c.lastSale&&daysAgo(c.lastSale)>=60);
  allCusts.sort((a,b)=>(b.totalSpent||0)-(a.totalSpent||0));

  // Stats bar
  const statsEl = document.getElementById('cust-base-stats');
  if (statsEl) {
    const total = allCusts.length;
    const repeats = allCusts.filter(c=>(c.visits||0)>=2).length;
    const totalRev = allCusts.reduce((a,c)=>a+(c.totalSpent||0),0);
    statsEl.innerHTML = `
      <div class="stat"><div class="num" style="font-size:18px;">${total}</div><div class="label">Customers</div></div>
      <div class="stat success"><div class="num" style="font-size:18px;">${repeats}</div><div class="label">Repeat</div></div>
      <div class="stat"><div class="num" style="font-size:14px;">${rs(totalRev)}</div><div class="label">Total Rev</div></div>`;
  }

  const div = document.getElementById('customer-base-list');
  if (!allCusts.length) { div.innerHTML='<div class="empty">No customers found</div>'; return; }
  div.innerHTML = allCusts.slice(0,50).map(c => {
    const tier = getCustomerTier(c.totalSpent||0, c.visits||0);
    const daysSince = c.lastSale ? daysAgo(c.lastSale) : null;
    const repeatLabel = c.visits >= 2 ? `🔁 ${c.visits} visits` : '🆕 New';
    return `<div class="part-item">
      <div class="part-row">
        <div style="flex:1;">
          <div class="bold">${c.name||'—'} <span class="cust-tier ${tier.cls}">${tier.label}</span></div>
          <div style="font-size:12px;color:var(--muted);">${c.phone||'—'} · ${c.type||'retail'} · ${repeatLabel}</div>
          <div style="font-size:12px;margin-top:3px;">
            <span class="text-success bold">${rs(c.totalSpent||0)}</span>
            <span style="color:var(--muted);"> · Last: ${daysSince!==null?daysSince+'d ago':'never'}</span>
          </div>
        </div>
        ${c.phone?`<a href="https://wa.me/${c.phone}" target="_blank" style="background:#25d366;color:white;padding:8px 12px;border-radius:8px;font-size:18px;text-decoration:none;">📲</a>`:''}
      </div>
    </div>`;
  }).join('');
}


function broadcastToSegment() {
  const seg = document.getElementById('bc-segment').value;
  const msg = document.getElementById('bc-msg').value.trim();
  if (!msg) { toast('Enter a message first'); return; }
  let targets = [];
  const now = Date.now();
  if (seg==='wholesale'||seg==='all'||seg==='platinum') {
    // Build single-pass customer totals for platinum filter
    const _bcTotals = {}, _bcCounts = {}, _bcLast = {};
    if (seg === 'platinum' || seg === 'inactive') {
      state.sales.forEach(s => {
        if (!s.customerId) return;
        _bcTotals[s.customerId] = (_bcTotals[s.customerId]||0) + s.total;
        _bcCounts[s.customerId] = (_bcCounts[s.customerId]||0) + 1;
        if (!_bcLast[s.customerId] || s.date > _bcLast[s.customerId]) _bcLast[s.customerId] = s.date;
      });
    }
    state.customers.forEach(c => {
      if (!c.phone) return;
      if (seg==='platinum') { if ((_bcTotals[c.id]||0)<100000 && (_bcCounts[c.id]||0)<20) return; }
      targets.push({phone:c.phone, name:c.name, message:msg});
    });
  }
  if (seg==='retail'||seg==='all') {
    state.retailCustomers.filter(c=>c.phone).forEach(c=>targets.push({phone:c.phone,name:c.name,message:msg}));
  }
  if (seg==='inactive') {
    [...state.customers,...state.retailCustomers].forEach(c => {
      if (!c.phone) return;
      const lastDate = _bcLast[c.id] || c.lastVisit;
      if (lastDate && daysAgo(lastDate) >= 60) targets.push({phone:c.phone,name:c.name,message:msg});
    });
  }
  if (!targets.length) { toast('No customers in this segment'); return; }
  openBulkQueue(targets);
}

// ============================================================
// RETAIL WALK-IN CUSTOMER DATABASE
// ============================================================
function openAddRetailCustomer() {
  showModal('+ Retail Walk-in Customer', `
    <div class="field"><label>Name *</label><input class="input" id="rc-name" placeholder="Customer name"></div>
    <div class="field"><label>Phone</label><input class="input" id="rc-phone" placeholder="919876543210"></div>
    <div class="field"><label>Village / Area</label><input class="input" id="rc-area" placeholder="e.g. Khanna, Ludhiana"></div>
    <div class="field"><label>Machine Type</label>
      <select class="select" id="rc-machine">
        <option value="">Unknown</option>
        <option value="tractor">Tractor</option>
        <option value="harvester">Harvester</option>
        <option value="jcb">JCB</option>
        <option value="rotavator">Rotavator</option>
      </select>
    </div>
    <div class="field"><label>Notes</label><textarea class="input" id="rc-notes" rows="2"></textarea></div>
    <button class="btn" onclick="saveRetailCustomer()">Save Customer</button>
  `);
}

async function saveRetailCustomer() {
  const name = document.getElementById('rc-name').value.trim();
  if (!name) { toast('Name required'); return; }
  state.retailCustomers.push({
    id:uid(), name, phone:document.getElementById('rc-phone').value.trim(),
    area:document.getElementById('rc-area').value.trim(),
    machineType:document.getElementById('rc-machine').value,
    notes:document.getElementById('rc-notes').value.trim(),
    visits:0, totalSpent:0, firstVisit:todayStr(), lastVisit:null
  });
  save(); closeModal(); toast('Retail customer saved ✅');
}

// ============================================================
// MECHANIC COMMISSION
// ============================================================
function renderSaleStaffDropdowns() {
  const salespeople = state.staff.filter(s=>s.active&&s.role!=='delivery');
  const delivery = state.staff.filter(s=>s.active);
  const sb = document.getElementById('sale-soldby');
  const db = document.getElementById('sale-deliveredby');
  const mb = document.getElementById('sale-mechanic');
  const rcb = document.getElementById('retail-cust-id');
  if (sb) {
    const cur = state.session;
    sb.innerHTML = '<option value="">— select staff —</option>' + salespeople.map(s=>`<option value="${s.id}" ${cur&&cur.staffId===s.id?'selected':''}>${s.name}</option>`).join('');
  }
  if (db) db.innerHTML = '<option value="">Walk-in / No delivery</option>' + delivery.map(s=>`<option value="${s.id}">${s.name}</option>`).join('');
  if (mb) mb.innerHTML = '<option value="">No mechanic referral</option>' + state.mechanics.map(m=>`<option value="${m.id}">${m.name} (default ${m.defaultRate}%)</option>`).join('');
  if (rcb) rcb.innerHTML = '<option value="">Anonymous walk-in</option>' + state.retailCustomers.map(c=>`<option value="${c.id}">${c.name}${c.phone?' - '+c.phone:''}</option>`).join('');
  // Show/hide retail customer field based on whether a wholesale customer is selected
  const saleCustomer = document.getElementById('sale-customer');
  if (saleCustomer) {
    saleCustomer.addEventListener('change', () => {
      const rcField = document.getElementById('retail-cust-field');
      if (rcField) rcField.style.display = saleCustomer.value ? 'none' : 'block';
    });
  }
}

function openAddMechanic() {
  showModal('+ Add Mechanic', `
    <div class="field"><label>Mechanic Name *</label><input class="input" id="mech-name" placeholder="e.g. Raju Mechanic"></div>
    <div class="field"><label>Phone</label><input class="input" id="mech-phone" placeholder="919876543210"></div>
    <div class="field"><label>Default Commission % on retail sales</label><input type="number" class="input" id="mech-rate" value="3" step="0.5" min="0" max="20"></div>
    <button class="btn" onclick="saveMechanic()">Save</button>
  `);
}
async function saveMechanic() {
  const name = document.getElementById('mech-name').value.trim();
  if (!name) { toast('Name required'); return; }
  state.mechanics.push({id:uid(), name, phone:document.getElementById('mech-phone').value.trim(), defaultRate:+document.getElementById('mech-rate').value||3});
  save(); closeModal();
  renderSaleStaffDropdowns();
  toast('Mechanic added ✅');
}

function renderMechComm() {
  const thisMonth = new Date().toISOString().slice(0,7);
  const mechSales = state.sales.filter(s=>s.date.slice(0,7)===thisMonth&&s.mechanicId);
  const byMech = {};
  mechSales.forEach(s => {
    byMech[s.mechanicId] = byMech[s.mechanicId] || {sales:0, comm:0};
    byMech[s.mechanicId].sales += s.total;
    byMech[s.mechanicId].comm += Math.round(s.total * (s.mechRate||0) / 100);
  });
  const div = document.getElementById('mech-comm-list');
  if (!Object.keys(byMech).length) { div.innerHTML='<div class="empty">No mechanic referrals this month</div>'; return; }
  div.innerHTML = Object.entries(byMech).map(([mechId,data]) => {
    const m = state.mechanics.find(x=>x.id===mechId);
    return `<div class="cart-item"><div><b>${m?.name||'Unknown'}</b><br><small>${m?.phone||''}</small></div>
    <div style="text-align:right;"><div class="bold text-success">${rs(data.comm)}</div><small>on ${rs(data.sales)}</small></div></div>`;
  }).join('');
}

function renderRptStaffPerf() {
  const target = state.settings.salesTarget||50000;
  const rows = state.staff.filter(s=>s.active).map(s=>({s,sales:staffSalesMonth(s.id),deliv:staffDeliveriesMonth(s.id)})).sort((a,b)=>b.sales-a.sales);
  document.getElementById('rpt-staff-perf').innerHTML = rows.map((r,i)=>{
    const pct=Math.min(100,(r.sales/target*100)).toFixed(0);
    return `<div class="comm-card"><div class="flex-between"><div class="name">#${i+1} ${r.s.name}</div><div class="bold text-success">${rs(r.sales)}</div></div>
    <div style="font-size:12px;color:var(--muted);">${r.s.role} · ${r.deliv} deliveries</div>
    <div class="comm-bar"><div class="comm-bar-fill" style="width:${pct}%"></div></div>
    <div style="font-size:11px;color:var(--muted);">${pct}% of target</div></div>`;
  }).join('');
}

// ============================================================
// SALARY CALCULATOR
// ============================================================
function populateSalarySelects() {
  const opts = '<option value="">— select staff —</option>' + state.staff.filter(s=>s.active).map(s=>`<option value="${s.id}">${s.name} (${s.role})</option>`).join('');
  ['sal-staff-sel','score-staff-sel'].forEach(id => { const el=document.getElementById(id); if(el) el.innerHTML=opts; });
}
function populateScoreSelects() { populateSalarySelects(); }

// ── MONTH PICKER HELPER ──────────────────────────────────────
function getMonthOptions(selectedKey) {
  const opts = [];
  const now = new Date();
  for (let i=0; i<12; i++) {
    const d = new Date(now.getFullYear(), now.getMonth()-i, 1);
    const key = d.toISOString().slice(0,7);
    const label = d.toLocaleString('en-IN',{month:'long',year:'numeric'});
    opts.push(`<option value="${key}" ${key===selectedKey?'selected':''}>${label}</option>`);
  }
  return opts.join('');
}

// ── MAIN SALARY VIEW ─────────────────────────────────────────
let _salMonth = new Date().toISOString().slice(0,7);

function loadSalaryCalc() {
  const staffId = document.getElementById('sal-staff-sel').value;
  const div = document.getElementById('sal-calc-body');
  if (!staffId) { div.innerHTML='<div class="empty" style="padding:20px;color:var(--muted);">Select a staff member above to view their salary details.</div>'; return; }
  const s = state.staff.find(x=>x.id===staffId);
  if (!s) return;
  renderSalaryView(staffId, _salMonth);
}

function renderSalaryView(staffId, monthKey) {
  _salMonth = monthKey;
  const div = document.getElementById('sal-calc-body');
  const s = state.staff.find(x=>x.id===staffId);
  if (!s || !div) return;

  // ── Attendance data ──
  const attKey = staffId+'-'+monthKey;
  const att = state.attendanceRecords[attKey] || {};
  const yr = parseInt(monthKey.slice(0,4));
  const mo = parseInt(monthKey.slice(5,7))-1;
  const daysInMonth = new Date(yr, mo+1, 0).getDate();
  const presentDays  = Object.values(att).filter(v=>v==='P').length;
  const halfDays     = Object.values(att).filter(v=>v==='H').length;
  const absentDays   = Object.values(att).filter(v=>v==='A').length;
  const unmarkedDays = daysInMonth - presentDays - halfDays - absentDays;
  const effectiveDays = presentDays + halfDays*0.5;

  // ── Pay components ──
  const basicPerDay = s.baseSalary > 0 ? (s.baseSalary/26) : 0;
  const basicPay    = Math.round(effectiveDays * basicPerDay);
  const commRate    = s.commRate ?? state.settings.commRate ?? 1.5;
  const delivRate   = s.delivRate ?? state.settings.delivRate ?? 50;

  // Sales for this specific month
  const salesAmt  = state.sales.filter(s2=>s2.date.slice(0,7)===monthKey&&s2.soldById===staffId).reduce((a,s2)=>a+s2.total,0);
  const delivCount= state.sales.filter(s2=>s2.date.slice(0,7)===monthKey&&s2.deliveredById===staffId).length;
  const salesComm = Math.round(salesAmt * commRate / 100);
  const delivComm = delivCount * delivRate;

  // Manual incentive / bonus entered for this month
  const manualKey = staffId+'-incentive-'+monthKey;
  const manualIncentive = +(state.attendanceRecords[manualKey]||0);
  const manualDeduction = +(state.attendanceRecords[staffId+'-deduct-'+monthKey]||0);

  // Advance outstanding (all unpaid)
  const advOutstanding = getStaffAdvanceBalance(staffId);
  // Advance taken THIS month
  const advThisMonth = state.advanceRecords.filter(a=>a.staffId===staffId&&a.date.slice(0,7)===monthKey).reduce((a,r)=>a+r.amount,0);

  const grossPay  = basicPay + salesComm + delivComm + manualIncentive;
  const netPay    = Math.max(0, grossPay - manualDeduction - advOutstanding);

  // Already paid this month?
  const paidThisMonth = state.salaryRecords.filter(r=>r.staffId===staffId&&r.month===monthKey&&r.type!=='advance').reduce((a,r)=>a+r.amount,0);
  const balance = Math.max(0, netPay - paidThisMonth);

  div.innerHTML = `
    <!-- Month selector -->
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:12px;">
      <label style="font-size:13px;color:var(--muted);white-space:nowrap;">Month:</label>
      <select class="select" id="sal-month-sel" onchange="renderSalaryView('${staffId}',this.value)" style="flex:1;">
        ${getMonthOptions(monthKey)}
      </select>
    </div>

    <!-- Staff info bar -->
    <div style="background:var(--primary-light);border-radius:10px;padding:10px 12px;margin-bottom:12px;display:flex;justify-content:space-between;align-items:center;">
      <div>
        <div class="bold">${s.name}</div>
        <div style="font-size:12px;color:var(--primary-dark);">${s.role} · Base: ${rs(s.baseSalary||0)}/mo · Comm: ${commRate}% · Deliv: ₹${delivRate}/trip</div>
      </div>
      <button class="btn btn-sm btn-outline" onclick="openAddStaff('${s.id}')" style="width:auto;">✏️ Edit</button>
    </div>

    <!-- Attendance grid -->
    <div style="background:#f8fafc;border-radius:12px;padding:12px;margin-bottom:12px;">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
        <div class="bold" style="font-size:13px;">📅 Attendance</div>
        <div style="font-size:11px;color:var(--muted);">Tap a day to mark: blank→P→½→A</div>
      </div>
      <!-- Day labels -->
      <div style="display:grid;grid-template-columns:repeat(7,1fr);gap:3px;margin-bottom:3px;">
        ${['S','M','T','W','T','F','S'].map(d=>`<div style="text-align:center;font-size:10px;color:var(--muted);font-weight:600;">${d}</div>`).join('')}
      </div>
      <div class="att-grid" id="att-grid-${staffId}" style="grid-template-columns:repeat(7,1fr);"></div>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin-top:8px;text-align:center;font-size:12px;">
        <div style="background:#dcfce7;border-radius:6px;padding:6px;"><div class="bold" style="color:#166534;">${presentDays}</div><div style="color:#166534;">Present</div></div>
        <div style="background:#fef3c7;border-radius:6px;padding:6px;"><div class="bold" style="color:#92400e;">${halfDays}</div><div style="color:#92400e;">Half-day</div></div>
        <div style="background:#fee2e2;border-radius:6px;padding:6px;"><div class="bold" style="color:#991b1b;">${absentDays}</div><div style="color:#991b1b;">Absent</div></div>
        <div style="background:#f3f4f6;border-radius:6px;padding:6px;"><div class="bold" style="color:var(--muted);">${unmarkedDays}</div><div style="color:var(--muted);">Unmarked</div></div>
      </div>
      <div style="font-size:12px;color:var(--muted);margin-top:6px;">Effective working days: <b>${effectiveDays}</b> of ${daysInMonth} (basis: 26 days/month)</div>
    </div>

    <!-- Salary breakdown -->
    <div style="background:white;border:1px solid var(--border);border-radius:12px;overflow:hidden;margin-bottom:12px;">
      <div style="background:var(--bg);padding:8px 12px;font-size:12px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;">Salary Breakdown</div>

      <div class="sal-row" style="padding:10px 12px;">
        <div><div>Basic Pay</div><div style="font-size:11px;color:var(--muted);">${effectiveDays} days × ${rs(Math.round(basicPerDay))}/day</div></div>
        <div class="bold">${rs(basicPay)}</div>
      </div>

      <div class="sal-row" style="padding:10px 12px;">
        <div><div>Sales Commission (${commRate}%)</div><div style="font-size:11px;color:var(--muted);">on ${rs(salesAmt)} sales this month</div></div>
        <div class="bold text-success">+${rs(salesComm)}</div>
      </div>

      <div class="sal-row" style="padding:10px 12px;">
        <div><div>Delivery Bonus</div><div style="font-size:11px;color:var(--muted);">${delivCount} deliveries × ₹${delivRate}</div></div>
        <div class="bold text-success">+${rs(delivComm)}</div>
      </div>

      <!-- Manual incentive / bonus entry -->
      <div class="sal-row" style="padding:10px 12px;background:#f0fdf4;">
        <div style="flex:1;">
          <div>Extra Incentive / Bonus</div>
          <div style="font-size:11px;color:var(--muted);">Festival bonus, special reward, etc.</div>
        </div>
        <div style="display:flex;align-items:center;gap:6px;">
          <span style="color:var(--success);font-weight:600;">+₹</span>
          <input type="number" style="width:90px;padding:6px 8px;border:1px solid var(--border);border-radius:8px;font-size:14px;text-align:right;" 
            id="incentive-input" value="${manualIncentive||''}" placeholder="0"
            onchange="saveSalaryComponent('${staffId}','${monthKey}','incentive',this.value)">
        </div>
      </div>

      <!-- Deduction entry -->
      <div class="sal-row" style="padding:10px 12px;background:#fff7f7;">
        <div style="flex:1;">
          <div>Deduction</div>
          <div style="font-size:11px;color:var(--muted);">Fine, uniform, tools, etc.</div>
        </div>
        <div style="display:flex;align-items:center;gap:6px;">
          <span style="color:var(--danger);font-weight:600;">−₹</span>
          <input type="number" style="width:90px;padding:6px 8px;border:1px solid var(--border);border-radius:8px;font-size:14px;text-align:right;"
            id="deduct-input" value="${manualDeduction||''}" placeholder="0"
            onchange="saveSalaryComponent('${staffId}','${monthKey}','deduct',this.value)">
        </div>
      </div>

      <div class="sal-row" style="padding:10px 12px;">
        <div><div>Advance Outstanding</div><div style="font-size:11px;color:var(--muted);">Taken this month: ${rs(advThisMonth)}</div></div>
        <div class="bold text-danger">−${rs(advOutstanding)}</div>
      </div>

      <div style="padding:12px;background:var(--primary-light);display:flex;justify-content:space-between;align-items:center;">
        <div>
          <div style="font-size:12px;color:var(--primary-dark);font-weight:600;">GROSS PAY</div>
          <div style="font-size:20px;font-weight:800;color:var(--primary);">${rs(grossPay)}</div>
        </div>
        <div style="text-align:right;">
          <div style="font-size:12px;color:var(--primary-dark);font-weight:600;">NET PAYABLE</div>
          <div style="font-size:24px;font-weight:900;color:var(--primary-dark);">${rs(netPay)}</div>
        </div>
      </div>
    </div>

    <!-- Payment status -->
    <div style="background:${paidThisMonth>=netPay&&netPay>0?'#dcfce7':'#fef9c3'};border-radius:12px;padding:12px;margin-bottom:12px;">
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <div>
          <div class="bold" style="font-size:14px;">${paidThisMonth>=netPay&&netPay>0?'✅ Fully Paid':'💳 Payment Status'}</div>
          <div style="font-size:12px;color:var(--muted);">Paid this month: <b>${rs(paidThisMonth)}</b> &nbsp; Remaining: <b style="color:${balance>0?'var(--danger)':'var(--success)'};">${rs(balance)}</b></div>
        </div>
        ${balance>0?`<button class="btn btn-success btn-sm" style="width:auto;" onclick="quickRecordPayment('${staffId}',${balance},'${monthKey}')">Pay ${rs(balance)}</button>`:''}
      </div>
    </div>

    <!-- Payment history for this month -->
    <div class="bold" style="font-size:13px;margin-bottom:6px;">📋 Payment History — ${new Date(yr,mo,1).toLocaleString('en-IN',{month:'long',year:'numeric'})}</div>
    <div id="month-pay-history"></div>
    <button class="btn btn-outline btn-sm mt-1" style="width:auto;" onclick="openRecordSalaryPaid()">+ Add Custom Payment</button>
  `;

  renderAttendanceGrid(staffId, monthKey, daysInMonth, att, yr, mo);
  renderMonthPayHistory(staffId, monthKey);
}

function renderAttendanceGrid(staffId, monthKey, days, att, yr, mo) {
  const grid = document.getElementById('att-grid-'+staffId);
  if (!grid) return;
  // Calculate day-of-week offset for first day
  const firstDow = new Date(yr, mo, 1).getDay(); // 0=Sun
  let html = '';
  // Empty cells before first day
  for (let i=0; i<firstDow; i++) html += `<div class="att-day" style="opacity:0;pointer-events:none;"></div>`;
  for (let d=1; d<=days; d++) {
    const key = String(d).padStart(2,'0');
    const status = att[key]||'';
    const cls = status==='P'?'att-present':status==='A'?'att-absent':status==='H'?'att-half':'att-holiday';
    const lbl = status==='P'?'P':status==='H'?'½':status==='A'?'A':d;
    html += `<div class="att-day ${cls}" onclick="cycleAttendance('${staffId}','${monthKey}','${key}')" title="${monthKey}-${key}">${lbl}</div>`;
  }
  grid.innerHTML = html;
}

function cycleAttendance(staffId, monthKey, day) {
  const key = staffId+'-'+monthKey;
  if (!state.attendanceRecords[key]) state.attendanceRecords[key] = {};
  const cur = state.attendanceRecords[key][day]||'';
  const next = cur===''?'P':cur==='P'?'H':cur==='H'?'A':'';
  if (next==='') delete state.attendanceRecords[key][day]; else state.attendanceRecords[key][day]=next;
  save();
  // Re-render just the grid + summary without resetting month selector
  const staffEl = document.getElementById('sal-staff-sel');
  if (staffEl) renderSalaryView(staffId, monthKey);
}

function saveSalaryComponent(staffId, monthKey, type, value) {
  const key = staffId+'-'+type+'-'+monthKey;
  state.attendanceRecords[key] = +value||0;
  save();
  // Refresh totals without full re-render (just update display)
  renderSalaryView(staffId, monthKey);
}

function renderMonthPayHistory(staffId, monthKey) {
  const div = document.getElementById('month-pay-history');
  if (!div) return;
  const records = state.salaryRecords.filter(r=>r.staffId===staffId&&r.month===monthKey).sort((a,b)=>b.date.localeCompare(a.date));
  if (!records.length) { div.innerHTML=`<div style="font-size:13px;color:var(--muted);padding:8px 0;">No payments recorded for this month yet.</div>`; return; }
  div.innerHTML = records.map(r=>`
    <div class="pay-record ${r.type}" style="margin-bottom:6px;">
      <div class="flex-between">
        <div>
          <span class="bold">${rs(r.amount)}</span>
          <span style="font-size:12px;color:var(--muted);"> · ${new Date(r.date).toLocaleDateString('en-IN')}</span>
          <div style="font-size:12px;color:var(--muted);">${r.note||r.type}</div>
        </div>
        <button class="btn btn-sm btn-danger" style="width:auto;padding:4px 8px;" onclick="deleteSalaryRecord('${r.id}')">✕</button>
      </div>
    </div>`).join('');
}

function quickRecordPayment(staffId, amount, monthKey) {
  const s = state.staff.find(x=>x.id===staffId);
  if (!confirm(`Pay ${rs(amount)} to ${s?.name}?`)) return;
  state.salaryRecords.push({id:uid(), staffId, amount, date:todayStr(), type:'salary', note:'Monthly salary', month:monthKey});
  save(); renderSalaryView(staffId, monthKey); renderSalaryPaidList(); toast('✅ Payment recorded');
}

function deleteSalaryRecord(id) {
  if (!confirm('Delete this payment record?')) return;
  state.salaryRecords = state.salaryRecords.filter(r=>r.id!==id);
  window._ownerDeleteInProgress = true;
  save().finally(() => { window._ownerDeleteInProgress = false; });
  const staffId = document.getElementById('sal-staff-sel')?.value;
  if (staffId) renderSalaryView(staffId, _salMonth);
  renderSalaryPaidList();
}

function recordSalaryPaid(staffId, amount) {
  if (!confirm(`Record salary payment of ${rs(amount)} for ${state.staff.find(s=>s.id===staffId)?.name}?`)) return;
  state.salaryRecords.push({id:uid(), staffId, amount, date:todayStr(), type:'salary', note:'Monthly salary', month:_salMonth});
  save(); renderSalaryView(staffId, _salMonth); renderSalaryPaidList(); toast('Payment recorded ✅');
}

function openRecordSalaryPaid() {
  const curStaff = document.getElementById('sal-staff-sel')?.value;
  showModal('+ Record Salary Payment', `
    <div class="field"><label>Staff Member</label>
      <select class="select" id="sp-staff">
        <option value="">— select —</option>
        ${state.staff.filter(s=>s.active).map(s=>`<option value="${s.id}" ${s.id===curStaff?'selected':''}>${s.name}</option>`).join('')}
      </select>
    </div>
    <div class="field"><label>Amount Paid ₹</label><input type="number" class="input" id="sp-amt" placeholder="0"></div>
    <div class="field"><label>Date</label><input type="date" class="input" id="sp-date" value="${todayStr()}"></div>
    <div class="field"><label>Type</label>
      <select class="select" id="sp-type">
        <option value="salary">Monthly Salary</option>
        <option value="advance">Advance</option>
        <option value="bonus">Bonus</option>
        <option value="manual">Other</option>
      </select>
    </div>
    <div class="field"><label>Note</label><input class="input" id="sp-note" placeholder="e.g. March salary, Diwali bonus…"></div>
    <button class="btn" onclick="saveManualSalaryPayment()">✓ Save Payment</button>
  `);
}

function saveManualSalaryPayment() {
  const staffId = document.getElementById('sp-staff').value;
  const amount  = +document.getElementById('sp-amt').value;
  const date    = document.getElementById('sp-date').value;
  const type    = document.getElementById('sp-type').value;
  const note    = document.getElementById('sp-note').value.trim();
  if (!staffId||!amount) { toast('Select staff and enter amount'); return; }
  // Advance-type payouts go through the SAME salary-relative limit as the advance
  // tracker — a staff member can't pull more than X% of salary in one go here either.
  let emergency = false, overrideReason = '';
  let ev = null;
  if (type === 'advance') {
    ev = evaluateAdvanceLimit(staffId, amount);
    if (advanceIsOverLimit(ev)) {
      const lbl = ev.verdict === 'over_tx_limit'
        ? `${rs(ev.txLimit)} limit (${ev.pct}% of ${rs(ev.salary)} salary)`
        : `${rs(ev.cap)} outstanding cap`;
      overrideReason = prompt(`🔒 ${ev.staff?.name}: ${rs(amount)} exceeds the ${lbl}.\n\nAllowed only in an EXTREME EMERGENCY. Type the reason to override, or press Cancel to stop:`);
      if (overrideReason === null) return;            // cancelled
      overrideReason = overrideReason.trim();
      if (!overrideReason) { toast('Override needs a reason — payment cancelled'); return; }
      emergency = true;
    }
  }
  const rec = {id:uid(), staffId, amount, date, type, note, month:date.slice(0,7)};
  if (emergency) { rec.emergency = true; rec.overrideReason = overrideReason; }
  state.salaryRecords.push(rec);
  markChanged('salaryRecords');
  // An advance-type salary payout is also an advance liability — record it so the
  // outstanding balance and the limit engine stay correct across both screens.
  if (type === 'advance') {
    state.advanceRecords.push({id:uid(), staffId, amount, date, reason:'salary-advance', repaid:false, emergency, overrideReason: emergency?overrideReason:''});
    markChanged('advanceRecords');
  }
  save(); closeModal(); renderSalaryPaidList();
  try { renderAdvanceTracker(); } catch(e) {}
  // Refresh salary view if on same staff
  const curStaff = document.getElementById('sal-staff-sel')?.value;
  if (curStaff===staffId) renderSalaryView(staffId, _salMonth);
  if (emergency) {
    logActivity && logActivity(`⚠️ EMERGENCY advance via salary ${rs(amount)} to ${ev?.staff?.name||staffId} (over limit) — reason: ${overrideReason}`, 'warn');
    toast('⚠️ Emergency advance recorded & logged');
  } else {
    toast('Payment recorded ✅');
  }
}

function renderSalaryPaidList() {
  const div = document.getElementById('salary-paid-list');
  if (!div) return;
  const recent = [...state.salaryRecords].sort((a,b)=>b.date.localeCompare(a.date)).slice(0,30);
  if (!recent.length) { div.innerHTML='<div class="empty" style="padding:16px;">No payments yet</div>'; return; }
  // Group by month
  const byMonth = {};
  recent.forEach(r => {
    const m = r.month||r.date.slice(0,7);
    if (!byMonth[m]) byMonth[m]=[];
    byMonth[m].push(r);
  });
  div.innerHTML = Object.entries(byMonth).sort((a,b)=>b[0].localeCompare(a[0])).map(([month, records]) => {
    const total = records.reduce((a,r)=>a+r.amount,0);
    const d = new Date(month+'-01');
    return `<div style="margin-bottom:12px;">
      <div style="font-size:12px;font-weight:600;color:var(--muted);text-transform:uppercase;margin-bottom:6px;">${d.toLocaleString('en-IN',{month:'long',year:'numeric'})} — ${rs(total)} total</div>
      ${records.map(r=>{
        const s=state.staff.find(x=>x.id===r.staffId);
        return `<div class="pay-record ${r.type}" style="margin-bottom:4px;">
          <div class="flex-between">
            <div><span class="bold">${s?.name||'—'}</span> <span style="font-size:11px;color:var(--muted);">· ${r.type}</span><br><small>${new Date(r.date).toLocaleDateString('en-IN')} ${r.note?'· '+r.note:''}</small></div>
            <span class="bold text-success">${rs(r.amount)}</span>
          </div>
        </div>`;
      }).join('')}
    </div>`;
  }).join('');
}

// ============================================================
// ADVANCE SYSTEM (capped ₹20k)
// ============================================================
function getStaffAdvanceBalance(staffId) {
  const advances = state.advanceRecords.filter(a=>a.staffId===staffId&&!a.repaid);
  return advances.reduce((a,r)=>a+r.amount,0);
}

function renderAdvanceTracker() {
  const div = document.getElementById('advance-tracker-list');
  if (!div) return;
  const cap = state.settings.advanceCap||20000;
  const pct = getAdvancePct();
  if (!state.staff.length) { div.innerHTML='<div class="empty">No staff added yet</div>'; return; }
  div.innerHTML = state.staff.filter(s=>s.active).map(s => {
    const bal = getStaffAdvanceBalance(s.id);
    const pctUsed = Math.min(100, (bal/cap*100)).toFixed(0);
    const cls = bal >= cap ? 'danger' : '';
    const salary = +s.baseSalary || 0;
    const txLimit = Math.round(salary * pct / 100);
    const limitLine = bal <= 0
      ? `<span style="color:var(--muted);">No running advance — next advance is unrestricted</span>`
      : (salary > 0
          ? `Max per advance: <b>${rs(txLimit)}</b> (${pct}% of ${rs(salary)})`
          : `<span style="color:#b45309;">Set base salary to enforce the ${pct}% per-advance limit</span>`);
    return `<div class="comm-card">
      <div class="flex-between"><div class="name">${s.name}</div><div class="bold ${bal>=cap?'text-danger':'text-warn'}">${rs(bal)} / ${rs(cap)}</div></div>
      <div class="advance-bar"><div class="advance-fill ${cls}" style="width:${pctUsed}%"></div></div>
      <div style="font-size:11px;color:var(--muted);">${pctUsed}% of cap used ${bal>=cap?'· 🔒 LOCKED':''}</div>
      <div style="font-size:11px;color:var(--muted);margin-top:2px;">${limitLine}</div>
    </div>`;
  }).join('');
}

// ── Advance limit engine ────────────────────────────────────────────────
// Two guardrails enforced on every advance AND every advance-type salary payout:
//   1. PER-PAYOUT limit = X% of that staff member's monthly salary (default 50%,
//      configurable via settings.advancePctOfSalary). Stops a single payout from
//      exceeding e.g. half a month's wage, so take-home stays livable.
//   2. OUTSTANDING cap = settings.advanceCap on total unpaid advances.
// The tighter limit binds. Either can be crossed ONLY through an explicit
// "extreme emergency" override, which forces a typed reason and is logged.
function getAdvancePct() { return +state.settings.advancePctOfSalary || 50; }

function evaluateAdvanceLimit(staffId, amount) {
  const s = state.staff.find(x => x.id === staffId);
  const salary = s ? (+s.baseSalary || 0) : 0;
  const pct = getAdvancePct();
  const txLimit = Math.round(salary * pct / 100);
  const outstanding = getStaffAdvanceBalance(staffId);
  const cap = +state.settings.advanceCap || 20000;
  const amt = +amount || 0;
  const r = { staff: s, salary, pct, txLimit, outstanding, cap, amount: amt, verdict: 'ok', overBy: 0 };
  // RULE: the limit only governs staff who already carry a RUNNING advance. A
  // staffer with nothing outstanding can take an advance freely — it becomes their
  // first advance, and the % limit then applies to every advance after that.
  if (outstanding <= 0) { r.verdict = 'ok'; r.noRunningAdvance = true; return r; }
  if (salary <= 0) {
    // No base salary on file → the % limit can't be computed. Enforce only the
    // outstanding cap and flag that salary should be set for full protection.
    if (outstanding + amt > cap) { r.verdict = 'over_cap'; r.overBy = (outstanding + amt) - cap; }
    else r.verdict = 'no_salary';
    return r;
  }
  if (amt > txLimit) { r.verdict = 'over_tx_limit'; r.overBy = amt - txLimit; return r; }
  if (outstanding + amt > cap) { r.verdict = 'over_cap'; r.overBy = (outstanding + amt) - cap; return r; }
  return r;
}
function advanceIsOverLimit(ev) { return ev.verdict === 'over_tx_limit' || ev.verdict === 'over_cap'; }

function advanceLimitMessage(ev) {
  if (!ev.staff) return '';
  if (ev.noRunningAdvance) return `<span style="color:var(--muted);">${ev.staff.name} has no running advance — no limit applies. ✅</span>`;
  if (ev.salary <= 0) {
    if (ev.verdict === 'over_cap') return `<span style="color:var(--danger);">⚠️ No base salary set for ${ev.staff.name}. This crosses the ${rs(ev.cap)} outstanding cap by ${rs(ev.overBy)}. Set a salary in Manage Staff to enforce the ${ev.pct}% rule.</span>`;
    return `<span style="color:#b45309;">ℹ️ No base salary set for ${ev.staff.name} — only the ${rs(ev.cap)} outstanding cap applies. Add a salary in Manage Staff to enforce the ${ev.pct}% limit.</span>`;
  }
  if (ev.verdict === 'over_tx_limit') return `<span style="color:var(--danger);">🔒 ${rs(ev.amount)} is over the ${rs(ev.txLimit)} limit (${ev.pct}% of ${rs(ev.salary)} salary) by ${rs(ev.overBy)}. Max in one go is ${rs(ev.txLimit)}. Use the extreme-emergency override only if unavoidable.</span>`;
  if (ev.verdict === 'over_cap') return `<span style="color:var(--danger);">🔒 Total outstanding would reach ${rs(ev.outstanding + ev.amount)} — over the ${rs(ev.cap)} cap by ${rs(ev.overBy)}. Use the extreme-emergency override only if unavoidable.</span>`;
  return `<span style="color:var(--muted);">Salary ${rs(ev.salary)} · Outstanding ${rs(ev.outstanding)} · Max this payout ${rs(ev.txLimit)} (${ev.pct}% of salary) ✅</span>`;
}

function openRecordAdvance() {
  showModal('Record Advance Payment', `
    <div class="field"><label>Staff Member</label>
      <select class="select" id="adv-staff" onchange="refreshAdvanceLimitUI()">
        <option value="">— select —</option>
        ${state.staff.filter(s=>s.active).map(s=>`<option value="${s.id}">${s.name}</option>`).join('')}
      </select>
    </div>
    <div id="adv-cap-msg" style="font-size:13px;margin-bottom:8px;"></div>
    <div class="field"><label>Amount ₹</label><input type="number" class="input" id="adv-amt" oninput="refreshAdvanceLimitUI()" placeholder="Enter amount"></div>
    <div class="field"><label>Reason</label>
      <select class="select" id="adv-reason">
        <option value="personal">Personal</option>
        <option value="medical">Medical 🏥</option>
        <option value="death">Family Emergency / Death</option>
        <option value="festival">Festival / Other</option>
      </select>
    </div>
    <!-- Extreme-emergency override — only shown when a limit is exceeded -->
    <div id="adv-override-box" style="display:none;background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:10px;margin-bottom:10px;">
      <label style="display:flex;align-items:center;gap:8px;font-size:13px;font-weight:700;color:#dc2626;cursor:pointer;">
        <input type="checkbox" id="adv-override" onchange="refreshAdvanceLimitUI()" style="width:auto;margin:0;"> ⚠️ Extreme emergency — override the limit
      </label>
      <div id="adv-override-reason-wrap" style="display:none;margin-top:8px;">
        <input class="input" id="adv-override-reason" placeholder="Why is this unavoidable? (required — logged)">
      </div>
    </div>
    <div class="field"><label>Date</label><input type="date" class="input" id="adv-date" value="${todayStr()}"></div>
    <button class="btn btn-warn" id="adv-save-btn" onclick="saveStaffAdvance()">Record Advance</button>
  `);
  refreshAdvanceLimitUI();
}

// Live limit feedback + button gating. Called on staff change, amount input, and
// override toggle. Replaces the old checkAdvanceCap().
function refreshAdvanceLimitUI() {
  const staffSel = document.getElementById('adv-staff');
  if (!staffSel) return;
  const staffId = staffSel.value;
  const amount  = +(document.getElementById('adv-amt')?.value) || 0;
  const msg       = document.getElementById('adv-cap-msg');
  const box       = document.getElementById('adv-override-box');
  const chk       = document.getElementById('adv-override');
  const reasonWrap= document.getElementById('adv-override-reason-wrap');
  const saveBtn   = document.getElementById('adv-save-btn');
  if (!staffId) { if(msg) msg.textContent=''; if(box) box.style.display='none'; if(saveBtn){saveBtn.disabled=false;saveBtn.style.opacity='1';saveBtn.textContent='Record Advance';} return; }
  const ev = evaluateAdvanceLimit(staffId, amount);
  if (msg) msg.innerHTML = advanceLimitMessage(ev);
  const over = advanceIsOverLimit(ev);
  if (box) box.style.display = over ? 'block' : 'none';
  if (!over && chk) chk.checked = false;
  if (reasonWrap) reasonWrap.style.display = (over && chk && chk.checked) ? 'block' : 'none';
  if (saveBtn) {
    const blocked = over && !(chk && chk.checked);
    saveBtn.disabled = blocked;
    saveBtn.style.opacity = blocked ? '0.5' : '1';
    saveBtn.textContent = blocked ? '🔒 Over limit — tick override to proceed'
                         : (over ? '⚠️ Record emergency advance' : 'Record Advance');
  }
}
// Back-compat alias (older callers / inline handlers)
function checkAdvanceCap() { refreshAdvanceLimitUI(); }

async function saveStaffAdvance() {
  const staffId = document.getElementById('adv-staff').value;
  const amount  = +document.getElementById('adv-amt').value;
  const reason  = document.getElementById('adv-reason').value;
  if (!staffId || !amount) { toast('Fill all fields'); return; }
  if (amount <= 0) { toast('Enter a valid amount'); return; }
  const ev = evaluateAdvanceLimit(staffId, amount);
  let emergency = false, overrideReason = '';
  if (advanceIsOverLimit(ev)) {
    const chk = document.getElementById('adv-override');
    if (!chk || !chk.checked) {
      toast(`🔒 ${ev.staff?.name}: amount is over the allowed limit. Tick the extreme-emergency override to proceed.`);
      return;
    }
    overrideReason = (document.getElementById('adv-override-reason')?.value || '').trim();
    if (!overrideReason) { toast('Enter the emergency reason (required for override)'); return; }
    const lbl = ev.verdict === 'over_tx_limit'
      ? `${rs(ev.txLimit)} limit (${ev.pct}% of salary)`
      : `${rs(ev.cap)} outstanding cap`;
    if (!confirm(`EXTREME EMERGENCY OVERRIDE\n\n${ev.staff?.name}: ${rs(amount)} exceeds the ${lbl}.\nThis will be recorded as an emergency and logged.\n\nProceed?`)) return;
    emergency = true;
  }
  state.advanceRecords.push({
    id: uid(), staffId, amount, date: document.getElementById('adv-date').value,
    reason, repaid: false, emergency, overrideReason: emergency ? overrideReason : ''
  });
  markChanged('advanceRecords');
  save(); closeModal(); renderAdvanceTracker();
  if (emergency) {
    logActivity && logActivity(`⚠️ EMERGENCY advance ${rs(amount)} to ${ev.staff?.name} (over limit) — reason: ${overrideReason}`, 'warn');
    toast('⚠️ Emergency advance recorded & logged');
  } else {
    toast('Advance recorded ✅');
  }
}

// ============================================================
// ATTENDANCE / SALARY LIVE SYNC (inbound from attendance software)
// ============================================================
// Goury LISTENS to the Firestore 'staffSync' collection. The attendance software
// writes one doc per employee; each change updates the matching staff member's
// salary (and live hours/earned) in real time — no manual entry. Matching tries
// staffId → attendanceCode → name (case-insensitive). Goury never writes back to
// staffSync, so there is no echo loop.
const _attSync = { connected:false, lastSync:null, applied:0, unmatched:[], unsub:null };
window._attSync = _attSync;

function _matchStaffForSync(data) {
  const sid = data.staffId || data.id;
  if (sid) { const m = state.staff.find(s=>s.id===sid); if (m) return m; }
  const code = (data.staffCode || data.code || data.employeeCode || data.attendanceCode || '').toString().trim().toLowerCase();
  if (code) { const m = state.staff.find(s=>(s.attendanceCode||'').toString().trim().toLowerCase()===code); if (m) return m; }
  const nm = (data.name||'').toString().trim().toLowerCase();
  if (nm) { const m = state.staff.find(s=>(s.name||'').toString().trim().toLowerCase()===nm); if (m) return m; }
  return null;
}

let _attCommitTimer = null;
function _scheduleAttCommit() {
  clearTimeout(_attCommitTimer);
  _attCommitTimer = setTimeout(function(){
    markChanged('staff'); save();
    try { renderAttendanceSyncPanel(); } catch(e){}
    try { renderAdvanceTracker(); } catch(e){}
    try { const cur=document.getElementById('sal-staff-sel')?.value; if(cur) renderSalaryView(cur, _salMonth); } catch(e){}
  }, 800);
}

// Apply a single staffSync doc onto its staff member. Liberal in the field names
// it accepts so it works with whatever the attendance software can emit.
function applyStaffSyncDoc(data) {
  const num = v => {
    if (v===undefined||v===null||v==='') return null;
    // Tolerate CSV values like "₹12,000", "12,000.50", " 272 hrs" → numeric
    const n = parseFloat(String(v).replace(/[^0-9.\-]/g,''));
    return isNaN(n) ? null : n;
  };
  const staff = _matchStaffForSync(data);
  if (!staff) {
    const label = data.name || data.staffCode || data.code || data._docId || 'unknown';
    if (!_attSync.unmatched.includes(label)) _attSync.unmatched.push(label);
    _attSync.lastSync = Date.now(); _scheduleAttCommit(); return;
  }
  const baseSalary = num(data.baseSalary ?? data.salary ?? data.monthlySalary);
  const earned     = num(data.earnedThisMonth ?? data.netSalary ?? data.payable ?? data.earned);
  const hours      = num(data.hoursWorked ?? data.hours ?? data.totalHours);
  const ot         = num(data.otHours ?? data.overtimeHours ?? data.ot);
  const monthKey   = (data.monthKey || data.month || '').toString();
  if (baseSalary !== null && baseSalary > 0) staff.baseSalary = baseSalary;
  if (earned !== null) staff.liveEarnedThisMonth = earned;
  if (hours !== null)  staff.liveHours = hours;
  if (ot !== null)     staff.liveOtHours = ot;
  if (monthKey)        staff.liveSyncMonth = monthKey;
  staff.lastAttSync = Date.now();
  _attSync.applied++; _attSync.lastSync = Date.now();
  _scheduleAttCommit();
}

function startStaffSyncListener() {
  if (!window.firestoreListenStaffSync || !window._firebaseAuthed) return;
  if (_attSync.unsub) return; // already listening
  try {
    _attSync.unsub = window.firestoreListenStaffSync(applyStaffSyncDoc);
    _attSync.connected = true;
    try { RX.log('info','attsync','staffSync listener started'); } catch(e){}
  } catch(e) { console.warn('startStaffSyncListener failed:', e.message); }
}

async function pullStaffSyncNow() {
  if (!window.firestorePullStaffSync) { toast('Sync not ready — reload with internet'); return; }
  toast('Pulling attendance data…');
  const rows = await window.firestorePullStaffSync();
  if (!rows) { toast('Could not reach cloud'); return; }
  _attSync.unmatched = [];
  rows.forEach(applyStaffSyncDoc);
  toast(`Synced ${rows.length} record(s) ✅`);
  renderAttendanceSyncPanel();
}

// ── CSV / EXCEL IMPORT from the biometric desktop software ────────────────
// Biometric machines export a file rather than pushing live, so this is the main
// path: read the export, map its columns, and apply each row through the same
// matching engine. Reuses impParseCSV + SheetJS (already loaded).
let _attImpRows = [], _attImpHeaders = [];

function _attGuessCol(field) {
  const H = _attImpHeaders.map(h => h.toLowerCase().replace(/[^a-z0-9]/g,''));
  const want = ({
    code:   ['staffcode','employeecode','empcode','empid','employeeid','attendancecode','enrollid','userid','acno','cardno','code','id'],
    name:   ['employeename','staffname','empname','fullname','name'],
    salary: ['basesalary','monthlysalary','grosssalary','salary','basic','wages','wage','pay'],
    hours:  ['hoursworked','totalhours','workinghours','workedhours','workhours','totalduration','duration','hours'],
    ot:     ['overtimehours','othours','overtime','extrahours','ot'],
    earned: ['earnedthismonth','netsalary','totalsalary','netamount','totalpay','payable','netpay','earned','amount'],
    month:  ['monthkey','salarymonth','wagemonth','payperiod','period','month']
  })[field] || [];
  for (const w of want) { const i = H.indexOf(w); if (i>=0) return _attImpHeaders[i]; }       // exact
  for (let i=0;i<H.length;i++){ if (want.some(w=>H[i].includes(w))) return _attImpHeaders[i]; } // partial
  return '';
}

async function attImportHandleFile(file) {
  if (!file) return;
  toast('📂 Reading file…');
  let text = '';
  try {
    if (file.name.match(/\.xlsx?$/i)) {
      await _loadLib('xlsx');
      if (typeof XLSX === 'undefined') { toast('Excel reader could not load — try again in a moment, or export as CSV'); return; }
      const buf = await file.arrayBuffer();
      const wb = XLSX.read(buf, {type:'array'});
      text = XLSX.utils.sheet_to_csv(wb.Sheets[wb.SheetNames[0]]);
    } else {
      text = await file.text();
    }
  } catch(e) { toast('Could not read file: '+e.message); return; }
  const parsed = impParseCSV(text);
  if (!parsed.headers.length || !parsed.rows.length) { toast('❌ Could not read rows. Try CSV format.'); return; }
  _attImpHeaders = parsed.headers; _attImpRows = parsed.rows;
  attImportShowMap();
}

function attImportShowMap() {
  const fields = [
    ['code','Employee Code'], ['name','Name'], ['salary','Monthly Salary ₹'],
    ['hours','Hours Worked'], ['ot','OT Hours'], ['earned','Earned This Month ₹'], ['month','Month (YYYY-MM)']
  ];
  const sel = (f) => { const g=_attGuessCol(f); return `<option value="">— none —</option>` + _attImpHeaders.map(h=>`<option value="${h.replace(/"/g,'&quot;')}" ${h===g?'selected':''}>${h}</option>`).join(''); };
  const rowsHtml = fields.map(([f,label])=>`<div class="field" style="margin-bottom:6px;"><label style="font-size:12px;">${label}</label><select class="select" id="attmap-${f}">${sel(f)}</select></div>`).join('');
  showModal('Map attendance columns', `
    <div style="font-size:12px;color:var(--muted);margin-bottom:8px;">Found <b>${_attImpRows.length}</b> row(s). Match your file's columns — we guessed where we could. Matching to staff is by Employee Code first, then Name.</div>
    ${rowsHtml}
    <div style="font-size:11px;color:var(--muted);margin:6px 0 10px;">Salary updates each staffer's pay. Hours / OT / Earned are stored for reference and shown on the salary screen.</div>
    <button class="btn" onclick="attImportApply()">✓ Apply to staff</button>
  `);
}

function attImportApply() {
  const g = f => document.getElementById('attmap-'+f)?.value || '';
  const cmap = { code:g('code'), name:g('name'), salary:g('salary'), hours:g('hours'), ot:g('ot'), earned:g('earned'), month:g('month') };
  if (!cmap.code && !cmap.name) { toast('Map at least Employee Code or Name'); return; }
  _attSync.unmatched = [];
  let applied = 0;
  _attImpRows.forEach(r => {
    const data = {};
    if (cmap.code)   data.staffCode = r[cmap.code];
    if (cmap.name)   data.name = r[cmap.name];
    if (cmap.salary) data.baseSalary = r[cmap.salary];
    if (cmap.hours)  data.hoursWorked = r[cmap.hours];
    if (cmap.ot)     data.otHours = r[cmap.ot];
    if (cmap.earned) data.earnedThisMonth = r[cmap.earned];
    if (cmap.month)  data.monthKey = r[cmap.month];
    if (!data.staffCode && !data.name) return;
    const before = _attSync.applied;
    applyStaffSyncDoc(data);
    if (_attSync.applied > before) applied++;
  });
  markChanged('staff'); save();           // force commit now (don't wait for debounce)
  closeModal();
  const unm = _attSync.unmatched.length;
  toast(`✅ Updated ${applied} staff${unm?` · ${unm} unmatched`:''}`);
  if (unm) setTimeout(function(){ toast(`⚠️ ${_attSync.unmatched.slice(0,4).join(', ')}${unm>4?'…':''} — set their Attendance Code in Manage Staff`); }, 1800);
  try { renderAdvanceTracker(); } catch(e){}
  try { const cur=document.getElementById('sal-staff-sel')?.value; if(cur) renderSalaryView(cur, _salMonth); } catch(e){}
}

function triggerAttImport() {
  let inp = document.getElementById('att-import-input');
  if (!inp) {
    inp = document.createElement('input');
    inp.type = 'file'; inp.id = 'att-import-input';
    inp.accept = '.csv,.xlsx,.xls,.tsv,.txt';
    inp.style.display = 'none';
    inp.onchange = function(){ if (this.files && this.files[0]) attImportHandleFile(this.files[0]); this.value=''; };
    document.body.appendChild(inp);
  }
  inp.click();
}

// Status block — rendered inside the settings modal
function renderAttendanceSyncPanel() {
  const el = document.getElementById('att-sync-status');
  if (!el) return;
  const dot = _attSync.connected ? '🟢' : '🔴';
  const last = _attSync.lastSync ? new Date(_attSync.lastSync).toLocaleString('en-IN') : 'never';
  const unmatched = _attSync.unmatched.length
    ? `<div style="color:#b45309;margin-top:4px;">⚠️ ${_attSync.unmatched.length} record(s) had no matching staff: ${_attSync.unmatched.slice(0,5).join(', ')}${_attSync.unmatched.length>5?'…':''}. Set their Attendance Code in Manage Staff.</div>`
    : '';
  el.innerHTML = `${dot} ${_attSync.connected?'Listening for live updates':'Not connected'} · applied ${_attSync.applied} · last ${last}${unmatched}`;
}

function openAttendanceSyncSetup() {
  const pid = window._firebaseProjectId || 'goury-spare-parts';
  const contract = `{
  "staffCode": "EMP001",          // must match the staff's Attendance Code in Goury
  "name": "Ramesh Kumar",         // fallback match if code is blank
  "baseSalary": 12000,            // monthly salary — becomes the salary in Goury
  "monthKey": "2026-06",
  "hoursWorked": 272,             // optional (your 260+ hr logic)
  "otHours": 12,                  // optional
  "earnedThisMonth": 12600,       // optional — salary incl. overtime for this month
  "updatedAt": 1718000000000
}`;
  const snippet = `// In your attendance software (Node + Firebase Admin SDK):
const admin = require('firebase-admin');
admin.initializeApp({ projectId: '${pid}',
  credential: admin.credential.cert(require('./serviceAccount.json')) });
const db = admin.firestore();

// Call this whenever an employee's salary/hours change:
async function pushToGoury(emp) {
  await db.collection('staffSync').doc(emp.code).set({
    staffCode: emp.code, name: emp.name,
    baseSalary: emp.salary, monthKey: emp.monthKey,
    hoursWorked: emp.hours, otHours: emp.ot,
    earnedThisMonth: emp.earned, updatedAt: Date.now()
  }, { merge: true });   // Goury updates that staff member instantly
}`;
  showModal('📡 Attendance → Salary Sync', `
    <div style="font-size:13px;line-height:1.55;color:var(--text,#111);">
      <div id="att-sync-status" style="background:#f1f5f9;border-radius:8px;padding:8px 10px;font-size:12px;margin-bottom:12px;"></div>

      <div style="border:1px solid #bbf7d0;background:#f0fdf4;border-radius:10px;padding:12px;margin-bottom:14px;">
        <div style="font-weight:800;color:#166534;margin-bottom:4px;">📂 Import attendance export</div>
        <div style="font-size:12px;color:#15803d;margin-bottom:10px;">Export the salary/attendance sheet from your biometric software (CSV or Excel), then import it here. Columns are auto-detected and you confirm the mapping.</div>
        <button class="btn" onclick="triggerAttImport()" style="width:100%;background:#16a34a;">Choose CSV / Excel file</button>
      </div>

      <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:8px;font-size:12px;margin-bottom:14px;">
        Set each person's <b>Attendance Code</b> in Manage Staff so it matches the employee code in your export. Rows that don't match are listed above so you can fix the code and re-import.
      </div>

      <details style="font-size:12px;color:var(--muted);">
        <summary style="cursor:pointer;font-weight:700;color:var(--text,#111);">⚙️ Automate it (advanced — for a developer)</summary>
        <p style="margin:8px 0;">To make it truly live, run a small watcher on the PC where the biometric software saves its export. The watcher reads new rows and writes them to Firestore; Goury then updates instantly (no manual import).</p>
        <div style="background:#0f172a;color:#e2e8f0;border-radius:8px;padding:10px;font-family:monospace;font-size:11px;">Firestore project: <b>${pid}</b><br>Collection: <b>staffSync</b> · Doc ID: employee code</div>
        <div style="font-weight:700;margin:10px 0 4px;color:var(--text,#111);">Document shape</div>
        <textarea readonly class="input" rows="8" style="font-family:monospace;font-size:11px;" onclick="this.select()">${contract.replace(/</g,'&lt;')}</textarea>
        <div style="font-weight:700;margin:10px 0 4px;color:var(--text,#111);">Writer code (Firebase Admin SDK)</div>
        <textarea readonly class="input" rows="11" style="font-family:monospace;font-size:11px;" onclick="this.select()">${snippet.replace(/</g,'&lt;')}</textarea>
        <button class="btn btn-outline" onclick="pullStaffSyncNow()" style="margin-top:8px;">🔄 Pull cloud data now</button>
      </details>

      <div style="margin-top:14px;"><button class="btn btn-outline" onclick="closeModal()" style="width:100%;">Done</button></div>
    </div>
  `);
  renderAttendanceSyncPanel();
}

// ============================================================
// STAFF SCORES (Year-End Review)
// ============================================================
function loadStaffScore() {
  const staffId = document.getElementById('score-staff-sel').value;
  const div = document.getElementById('score-body');
  if (!staffId) { div.innerHTML=''; return; }
  const s = state.staff.find(x=>x.id===staffId);
  const existing = state.staffScores[staffId] || {behaviour:0,output:0,effort:0,internalFeedback:'',year:new Date().getFullYear()};

  div.innerHTML = `
    <div class="bold mb-1">Internal Review — ${s?.name}</div>
    ${['behaviour','output','effort'].map(cat => `
      <div class="field">
        <label>${cat==='behaviour'?'😊 Behaviour & Attitude':cat==='output'?'📊 Output & Productivity':'🔧 Effort to Solve Problems'}</label>
        <div class="star-row" id="stars-${cat}">
          ${[1,2,3,4,5].map(n=>`<span class="star ${n<=(existing[cat]||0)?'lit':''}" onclick="setScore('${staffId}','${cat}',${n})" data-val="${n}">⭐</span>`).join('')}
        </div>
      </div>`).join('')}
    <div class="field"><label>Internal Feedback / Notes</label>
      <textarea class="input" id="score-feedback" rows="3" placeholder="Write internal review…">${existing.internalFeedback||''}</textarea>
    </div>
    <div class="field">
      <label>📋 Customer Feedback Ratings (from sales)</label>
      <div id="cust-feedback-avg" style="font-size:13px;color:var(--muted);">Loading…</div>
    </div>
    <button class="btn" onclick="saveStaffScore('${staffId}')">💾 Save Review</button>
  `;
  renderCustomerFeedbackAvg(staffId);
}

function setScore(staffId, cat, val) {
  if (!state.staffScores[staffId]) state.staffScores[staffId] = {};
  state.staffScores[staffId][cat] = val;
  document.querySelectorAll(`#stars-${cat} .star`).forEach(s => { s.classList.toggle('lit', +s.dataset.val <= val); });
}

function renderCustomerFeedbackAvg(staffId) {
  const div = document.getElementById('cust-feedback-avg');
  if (!div) return;
  const salesBySP = state.sales.filter(s=>s.soldById===staffId&&s.custRating);
  if (!salesBySP.length) { div.textContent='No customer ratings recorded yet'; return; }
  const avg = (salesBySP.reduce((a,s)=>a+(s.custRating||0),0)/salesBySP.length).toFixed(1);
  div.innerHTML = `Average: <b>${avg}/5</b> from ${salesBySP.length} customer ratings`;
}

function saveStaffScore(staffId) {
  if (!state.staffScores[staffId]) state.staffScores[staffId] = {};
  state.staffScores[staffId].internalFeedback = document.getElementById('score-feedback').value.trim();
  state.staffScores[staffId].year = new Date().getFullYear();
  ['behaviour','output','effort'].forEach(cat => {
    if (!state.staffScores[staffId][cat]) state.staffScores[staffId][cat]=0;
  });
  save(); toast('Review saved ✅');
  renderScoreLeaderboard();
}

function renderScoreLeaderboard() {
  const div = document.getElementById('score-leaderboard');
  if (!div) return;
  const ranked = state.staff.filter(s=>s.active).map(s => {
    const sc = state.staffScores[s.id]||{};
    const avg = ((sc.behaviour||0)+(sc.output||0)+(sc.effort||0))/3;
    const salesBySP = state.sales.filter(x=>x.soldById===s.id&&x.custRating);
    const custAvg = salesBySP.length ? salesBySP.reduce((a,x)=>a+(x.custRating||0),0)/salesBySP.length : 0;
    const overall = sc.behaviour ? (avg*0.6+custAvg*0.4) : 0;
    return {s, avg, custAvg, overall};
  }).sort((a,b)=>b.overall-a.overall);

  if (!ranked.length) { div.innerHTML='<div class="empty">No scores yet</div>'; return; }
  div.innerHTML = ranked.map((r,i) => `
    <div class="score-card">
      <div class="flex-between"><div class="name">#${i+1} ${r.s.name}</div><div class="bold" style="color:var(--ai);">${r.overall.toFixed(1)}/5</div></div>
      <div class="score-bar-wrap"><span style="font-size:12px;">Behaviour</span><div class="score-bar"><div class="score-fill" style="width:${(state.staffScores[r.s.id]?.behaviour||0)/5*100}%"></div></div><span style="font-size:12px;">${state.staffScores[r.s.id]?.behaviour||0}</span></div>
      <div class="score-bar-wrap"><span style="font-size:12px;">Output</span><div class="score-bar"><div class="score-fill" style="width:${(state.staffScores[r.s.id]?.output||0)/5*100}%"></div></div><span style="font-size:12px;">${state.staffScores[r.s.id]?.output||0}</span></div>
      <div class="score-bar-wrap"><span style="font-size:12px;">Effort</span><div class="score-bar"><div class="score-fill" style="width:${(state.staffScores[r.s.id]?.effort||0)/5*100}%"></div></div><span style="font-size:12px;">${state.staffScores[r.s.id]?.effort||0}</span></div>
      <div style="font-size:12px;color:var(--muted);">Customer avg: ${r.custAvg.toFixed(1)}/5</div>
    </div>`).join('');
}

// ============================================================
// AI FEEDBACK / CONTINUOUS IMPROVEMENT
// ============================================================
function renderAIFeedbackList() {
  const div = document.getElementById('ai-feedback-list');
  if (!div) return;
  const recent = state.aiFeedback.filter(f=>f.type==='ai-suggestion').slice(-10).reverse();
  if (!recent.length) { div.innerHTML='<div class="empty">No AI suggestions rated yet. Use the AI Brain tab and rate suggestions.</div>'; return; }
  div.innerHTML = recent.map(f=>`
    <div class="ai-insight">
      <div class="title">${f.suggestion||'AI Suggestion'}</div>
      <small>${new Date(f.date).toLocaleDateString('en-IN')}</small>
      ${f.rating ? `<div style="margin-top:4px;">${f.rating==='good'?'👍 Helpful':'👎 Not helpful'}</div>` : ''}
    </div>`).join('');
}

function exportAIFeedback() {
  const rows = [['Date','Type','Suggestion','Rating','Comment']];
  state.aiFeedback.forEach(f=>rows.push([f.date,f.type,f.suggestion||'',f.rating||'',f.comment||'']));
  downloadCSV(rows,'ai-feedback-'+todayStr()+'.csv');
}

// ============================================================
// BACKUP / DATA SAFETY
// ============================================================
function checkBackupReminder() {
  const freq = state.settings.backupFreq||3;
  const last = state.settings.lastBackup;
  if (!last) return; // no need to remind if never backed up
  const daysSince = daysAgo(last);
  if (daysSince >= freq) {
    // Show a subtle reminder toast
    setTimeout(()=>toast(`💾 Backup reminder: Last backup was ${daysSince} days ago. Go to Reports → Backup.`), 2000);
  }
}

function updateLastBackupDisplay() {
  const el = document.getElementById('last-backup-date');
  if (el) el.textContent = state.settings.lastBackup ? new Date(state.settings.lastBackup).toLocaleDateString('en-IN')+` (${daysAgo(state.settings.lastBackup)}d ago)` : 'Never';
}

function saveBackupPref() {
  state.settings.backupFreq = +document.getElementById('backup-freq').value||3;
  save(); toast('Backup preference saved');
}

function exportStaffCSV() {
  const rows = [['Name','Role','BaseSalary','CommRate','DelivRate','JoinDate','Active']];
  state.staff.forEach(s=>rows.push([s.name,s.role,s.baseSalary,s.commRate,s.delivRate,s.joinDate,s.active]));
  rows.push([]);
  rows.push(['--- SALARY RECORDS ---']);
  rows.push(['Staff','Amount','Date','Type','Note']);
  state.salaryRecords.forEach(r=>{const s=state.staff.find(x=>x.id===r.staffId);rows.push([s?.name||r.staffId,r.amount,r.date,r.type,r.note||'']);});
  rows.push([]);
  rows.push(['--- ADVANCE RECORDS ---']);
  rows.push(['Staff','Amount','Date','Reason','Repaid']);
  state.advanceRecords.forEach(r=>{const s=state.staff.find(x=>x.id===r.staffId);rows.push([s?.name||r.staffId,r.amount,r.date,r.reason,r.repaid?'Yes':'No']);});
  downloadCSV(rows,'staff-salary-'+todayStr()+'.csv');
}

// ============================================================
// ACTIVITY LOG
// ============================================================
function logActivity(msg, type='info') {
  if (!state.activityLog) state.activityLog = [];
  state.activityLog.unshift({ id:uid(), msg, type, user:state.session?.name||'System', date:new Date().toISOString() });
  if (state.activityLog.length > 200) state.activityLog = state.activityLog.slice(0,200);
}

// ============================================================
// OWNER CONTROL TOWER
// ============================================================
function renderOwnerDashboard() {
  document.getElementById('owner-date').textContent = new Date().toLocaleDateString('en-IN',{weekday:'long',day:'numeric',month:'long'});
  const role = state.session?.role;
  const isAdmin = role === 'admin';

  // Hide/show profit-sensitive cards based on role
  const profitCards = document.querySelectorAll('.ow-profit-gate');
  profitCards.forEach(el => el.style.display = isAdmin ? '' : 'none');

  const todaySalesList = state.sales.filter(s=>isToday(s.date));
  const monthSalesList = state.sales.filter(s=>isThisMonth(s.date));
  let todayRev=0,todayCost=0,todayCash=0,todayUPI=0,todayCredit=0;
  todaySalesList.forEach(s=>{
    todayRev+=s.total;
    if(s.paymentMode==='cash') todayCash+=s.total;
    else if(s.paymentMode==='upi') todayUPI+=s.total;
    else if(s.paymentMode==='credit') todayCredit+=s.total;
    s.items.forEach(it=>{const p=getPartById(it.partId);if(p)todayCost+=p.costPrice*it.qty;});
  });
  let monthRev=0,monthCost=0;
  monthSalesList.forEach(s=>{monthRev+=s.total;s.items.forEach(it=>{const p=getPartById(it.partId);if(p)monthCost+=p.costPrice*it.qty;});});
  const todayProfit=todayRev-todayCost, monthProfit=monthRev-monthCost;
  const due=state.customers.reduce((a,c)=>a+(c.totalDue||0),0);
  const dueCount=state.customers.filter(c=>(c.totalDue||0)>0).length;

  const setEl=(id,v)=>{const e=document.getElementById(id);if(e)e.textContent=v;};
  if (isAdmin) {
    setEl('ow-today-profit',rs(todayProfit));
    setEl('ow-today-margin',(todayRev?(todayProfit/todayRev*100):0).toFixed(1)+'% margin');
    setEl('ow-month-profit',rs(monthProfit));
    setEl('ow-month-margin',(monthRev?(monthProfit/monthRev*100):0).toFixed(1)+'% margin');
  }
  setEl('ow-today-rev',rs(todayRev));
  setEl('ow-today-bills',todaySalesList.length+' bills');
  setEl('ow-due',rs(due));
  setEl('ow-due-count',dueCount+' customers');
  setEl('ow-cash',rs(todayCash));
  setEl('ow-upi',rs(todayUPI));
  setEl('ow-credit',rs(todayCredit));

  // Top profit items — admin only
  if (isAdmin) {
    const itemProfit={};
    todaySalesList.forEach(s=>s.items.forEach(it=>{
      const p=getPartById(it.partId);if(!p)return;
      if(!itemProfit[it.partId])itemProfit[it.partId]={name:it.name,profit:0,qty:0,rev:0};
      itemProfit[it.partId].profit+=(it.price-p.costPrice)*it.qty;
      itemProfit[it.partId].qty+=it.qty;
      itemProfit[it.partId].rev+=it.price*it.qty;
    }));
    const topItems=Object.values(itemProfit).sort((a,b)=>b.profit-a.profit).slice(0,5);
    document.getElementById('ow-top-items').innerHTML=topItems.length===0
      ?'<div class="empty">No sales today yet</div>'
      :topItems.map((t,i)=>`<div class="top-item-row"><div><b>#${i+1} ${t.name}</b><br><small>Qty:${t.qty} · Rev:${rs(t.rev)}</small></div><div style="text-align:right;"><div style="color:var(--success);font-weight:700;">${rs(t.profit)}</div><small>${t.rev?(t.profit/t.rev*100).toFixed(0):0}%</small></div></div>`).join('');
    // Leakage
    const totalDiscount=monthSalesList.reduce((a,s)=>a+(s.discount||0),0);
    const deadValue=Smart.deadStock(90).reduce((a,p)=>a+p.costPrice*p.stock,0);
    document.getElementById('ow-leakage').innerHTML=`
      <div class="leakage-row"><span>📉 Discounts given (month)</span><span style="color:var(--danger);font-weight:700;">${rs(totalDiscount)}</span></div>
      <div class="leakage-row"><span>💀 Dead stock capital locked</span><span style="color:var(--danger);font-weight:700;">${rs(deadValue)}</span></div>
      <div class="leakage-row"><span>📋 Unpaid credit</span><span style="color:var(--warning);font-weight:700;">${rs(due)}</span></div>
      <div class="leakage-row bold" style="background:var(--bg);padding:8px;border-radius:8px;margin-top:4px;"><span>⚡ Total Leakage</span><span style="color:var(--danger);">${rs(totalDiscount+deadValue+due)}</span></div>`;
  }

  // Staff efficiency
  const staffEff=state.staff.filter(s=>s.active).map(s=>{
    const sales=monthSalesList.filter(x=>x.soldById===s.id).reduce((a,x)=>a+x.total,0);
    const bills=monthSalesList.filter(x=>x.soldById===s.id).length;
    return{s,sales,bills,avgBill:bills?sales/bills:0};
  }).sort((a,b)=>b.sales-a.sales);
  document.getElementById('ow-staff-eff').innerHTML=staffEff.length===0
    ?'<div class="empty">No staff data</div>'
    :staffEff.map(r=>`<div class="staff-eff-row"><div><b>${r.s.name}</b> <span class="tag">${r.s.role}</span><br><small>${r.bills} bills · Avg ${rs(r.avgBill)}</small></div><div style="text-align:right;"><div style="font-weight:700;color:var(--primary);">${rs(r.sales)}</div><small>this month</small></div></div>`).join('');

  // Activity log
  const logs=(state.activityLog||[]).slice(0,20);
  document.getElementById('ow-activity-log').innerHTML=logs.length===0
    ?'<div class="empty">No activity yet</div>'
    :logs.map(l=>`<div class="log-item"><div class="log-dot ${l.type==='warn'?'warn':l.type==='danger'?'danger':''}"></div><div><div>${l.msg}</div><small>${l.user} · ${new Date(l.date).toLocaleString('en-IN')}</small></div></div>`).join('');

  // User management (admin only)
  renderOwnerUserList();
  syncBizStatusUI();
}

function doClosing() {
  const sysCash=state.sales.filter(s=>isToday(s.date)&&s.paymentMode==='cash').reduce((a,s)=>a+s.total,0);
  const sysUPI=state.sales.filter(s=>isToday(s.date)&&s.paymentMode==='upi').reduce((a,s)=>a+s.total,0);
  const entCash=+document.getElementById('closing-cash').value||0;
  const entUPI=+document.getElementById('closing-upi').value||0;
  const diffCash=entCash-sysCash, diffUPI=entUPI-sysUPI;
  document.getElementById('closing-result').innerHTML=`
    <div class="closing-row ${Math.abs(diffCash)<1?'match':'mismatch'}"><span>💵 Cash: System ${rs(sysCash)} vs Physical ${rs(entCash)}</span><span>${diffCash>=0?'+':''}${rs(diffCash)}</span></div>
    <div class="closing-row ${Math.abs(diffUPI)<1?'match':'mismatch'}"><span>📱 UPI: System ${rs(sysUPI)} vs Entered ${rs(entUPI)}</span><span>${diffUPI>=0?'+':''}${rs(diffUPI)}</span></div>
    <div class="closing-row bold" style="color:var(--primary);"><span>📊 Total Today</span><span>${rs(sysCash+sysUPI)}</span></div>`;
  logActivity(`Day Closing — Cash diff:${rs(diffCash)}, UPI diff:${rs(diffUPI)}`, Math.abs(diffCash)>100||Math.abs(diffUPI)>100?'warn':'info');
  save(); toast('Closing recorded ✅');
}

// ============================================================
// BUSINESS ACTIVE / INACTIVE TOGGLE
// ============================================================
function syncBizStatusUI() {
  var active = state.settings.businessActive !== false; // default true
  var lbl = document.getElementById('biz-status-label');
  var sub = document.getElementById('biz-status-sub');
  var btn = document.getElementById('biz-toggle-btn');
  if (lbl) lbl.textContent = active ? '🟢 Business Active' : '🔴 Business Inactive';
  if (sub) sub.textContent = active ? 'Sales & billing are live' : 'Setup mode — data entry only, no sales';
  if (btn) {
    btn.textContent = active ? 'Active' : 'Inactive';
    btn.style.background = active ? '#16a34a' : '#dc2626';
  }
  var agOn=active&&state.settings.smartAgentsOn===true;
  var aBtn=document.getElementById('agents-toggle-btn'),aLbl=document.getElementById('agents-status-label'),aSub=document.getElementById('agents-status-sub');
  if(aBtn){aBtn.disabled=!active;aBtn.textContent=agOn?'On':'Off';aBtn.style.background=agOn?'#2563eb':'#6b7280';}
  if(aLbl)aLbl.textContent=agOn?'🤖 Smart Agents — ON':'🤖 Smart Agents';
  if(aSub)aSub.textContent=agOn?'Reorder · stockout · churn · pricing':(active?'Tap to enable':'Off — activate business first');
  var invEl=document.getElementById('dm-inv-count'),custEl=document.getElementById('dm-cust-count');
  if(invEl)invEl.textContent=(state.sales||[]).length;
  if(custEl)custEl.textContent=(state.customers||[]).length;
}
function toggleSmartAgents(){
  if(state.settings.businessActive===false){toast('⚠️ Activate business first');return;}
  var n=!(state.settings.smartAgentsOn===true);
  state.settings.smartAgentsOn=n;save();syncBizStatusUI();
  if(n){toast('🤖 Smart Agents ON');logActivity&&logActivity('Smart Agents ON','info');setTimeout(function(){try{runAgent();}catch(e){}},500);}
  else{toast('🤖 Smart Agents OFF');logActivity&&logActivity('Smart Agents OFF','info');}
}
function toggleBusinessActive() {
  var active = state.settings.businessActive !== false;
  var newState = !active;
  if (!confirm(newState
    ? 'Activate business? Sales & billing will be LIVE.'
    : 'Set to INACTIVE / Setup mode? Staff can still enter data but sales are hidden.')) return;
  state.settings.businessActive = newState;
  if (!newState) state.settings.smartAgentsOn = false;
  save();
  syncBizStatusUI();
  toast(newState ? '🟢 Business is now ACTIVE' : '🔴 Set to Inactive (setup mode)');
  logActivity('Business status set to: ' + (newState?'ACTIVE':'INACTIVE'), newState?'info':'warn');
}

// ============================================================
// DATA MANAGER — Invoice & Customer Management
// ============================================================
function openInvoiceManager() {
  var sales = [...(state.sales||[])].sort((a,b) => b.date.localeCompare(a.date));
  var html = `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
    <span style="font-size:13px;color:var(--muted);">${sales.length} invoices total</span>
    <button class="btn btn-sm btn-danger" onclick="confirmDeleteAllSales()">🗑 Delete All</button>
  </div>`;
  if (!sales.length) {
    html += '<div class="empty"><div class="icon">📄</div>No invoices yet</div>';
  } else {
    html += sales.map(s => {
      var cust = getCustomerById(s.customerId);
      return `<div style="background:#f8fafc;border-radius:10px;padding:10px 12px;margin-bottom:8px;display:flex;justify-content:space-between;align-items:center;gap:10px;">
        <div style="flex:1;cursor:pointer;" onclick="closeModal();showInvoice(state.sales.find(x=>x.id==='${s.id}'))">
          <div style="font-weight:700;font-size:14px;">${s.invoiceNo}</div>
          <div style="font-size:12px;color:var(--muted);">${cust?cust.name:'Walk-in'} · ${new Date(s.date).toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'2-digit'})}</div>
          <div style="font-size:13px;font-weight:600;color:var(--primary);">₹${s.total}</div>
        </div>
        <button onclick="closeModal();gpOpenBillEditor('sale','${s.id}')" style="background:#e0f2fe;color:#0369a1;border:none;border-radius:8px;padding:6px 10px;font-size:12px;font-weight:700;cursor:pointer;flex-shrink:0;">✏️ Edit</button>
        <button onclick="deleteSingleSale('${s.id}')" style="background:#fee2e2;color:#dc2626;border:none;border-radius:8px;padding:6px 10px;font-size:12px;font-weight:700;cursor:pointer;flex-shrink:0;">🗑 Delete</button>
      </div>`;
    }).join('');
  }
  showModal('📄 Invoice Manager', html);
}

function deleteSingleSale(id) {
  if (!confirm('Delete this invoice permanently? Stock will be restored.')) return;
  const sale = state.sales.find(s => s.id === id);
  if (!sale) return;
  // Restore stock for every item in this invoice
  (sale.items || []).forEach(function(item) {
    const p = getPartById(item.partId);
    if (p) p.stock = (p.stock || 0) + (item.qty || 0);
  });
  // Reverse credit totalDue if this was a credit sale
  if (sale.paymentMode === 'credit' && sale.customerId) {
    const c = getCustomerById(sale.customerId);
    if (c) c.totalDue = Math.max(0, (c.totalDue || 0) - sale.total);
  }
  state.sales = state.sales.filter(s => s.id !== id);
  window._ownerDeleteInProgress = true;
  save().finally(() => { window._ownerDeleteInProgress = false; });
  syncBizStatusUI();
  logActivity && logActivity('Invoice ' + (sale.invoiceNo||id) + ' deleted — stock restored', 'warn');
  toast('Invoice deleted — stock restored');
  openInvoiceManager();
}

function confirmDeleteAllSales() {
  if (!confirm('DELETE ALL ' + state.sales.length + ' invoices? This cannot be undone.')) return;
  if (!confirm('Are you absolutely sure? All sales history will be gone.')) return;
  state.sales = [];
  state.settings.nextInvoice = 1001;
  window._ownerDeleteInProgress = true;
  save().finally(() => { window._ownerDeleteInProgress = false; });
  syncBizStatusUI();
  closeModal();
  toast('All invoices deleted ✅');
  logActivity('Owner deleted all invoices (fresh start)', 'warn');
}

function openCustomerManager() {
  var custs = state.customers || [];
  var html = `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
    <span style="font-size:13px;color:var(--muted);">${custs.length} customers</span>
    <button class="btn btn-sm btn-danger" onclick="confirmDeleteAllCustomers()">🗑 Delete All</button>
  </div>`;
  if (!custs.length) {
    html += '<div class="empty"><div class="icon">👥</div>No customers yet</div>';
  } else {
    html += custs.map(c => {
      var lastSale = [...(state.sales||[])].filter(s=>s.customerId===c.id).sort((a,b)=>b.date.localeCompare(a.date))[0];
      return `<div style="background:#f8fafc;border-radius:10px;padding:10px 12px;margin-bottom:8px;display:flex;justify-content:space-between;align-items:center;gap:10px;">
        <div style="flex:1;">
          <div style="font-weight:700;font-size:14px;">${c.name} ${c.trusted?'★':''}</div>
          <div style="font-size:12px;color:var(--muted);">${c.phone} · <span class="tag ${c.type==='wholesale'?'':'warn'}">${c.type}</span></div>
          ${c.totalDue>0?`<div style="font-size:12px;color:var(--danger);font-weight:600;">Due: ₹${c.totalDue}</div>`:''}
          <div style="font-size:11px;color:var(--muted);">${lastSale ? 'Last sale: '+new Date(lastSale.date).toLocaleDateString('en-IN') : 'No sales'}</div>
        </div>
        <div style="display:flex;gap:6px;flex-shrink:0;">
          <button onclick="closeModal();openAddCustomer('${c.id}')" style="background:#e0f2fe;color:#0369a1;border:none;border-radius:8px;padding:6px 10px;font-size:12px;font-weight:700;cursor:pointer;">Edit</button>
          <button onclick="deleteCustomerFromManager('${c.id}')" style="background:#fee2e2;color:#dc2626;border:none;border-radius:8px;padding:6px 10px;font-size:12px;font-weight:700;cursor:pointer;">🗑</button>
        </div>
      </div>`;
    }).join('');
  }
  showModal('👥 Customer Manager', html);
}

function deleteCustomerFromManager(id) {
  if (!confirm('Delete this customer?')) return;
  state.customers = state.customers.filter(c => c.id !== id);
  window._ownerDeleteInProgress = true;
  save().finally(() => { window._ownerDeleteInProgress = false; });
  syncBizStatusUI();
  toast('Customer deleted');
  openCustomerManager(); // refresh
}

function confirmDeleteAllCustomers() {
  if (!confirm('DELETE ALL ' + state.customers.length + ' customers? This cannot be undone.')) return;
  if (!confirm('Are you absolutely sure?')) return;
  state.customers = [];
  window._ownerDeleteInProgress = true;
  save().finally(() => { window._ownerDeleteInProgress = false; });
  syncBizStatusUI();
  closeModal();
  toast('All customers deleted ✅');
  logActivity('Owner deleted all customers (fresh start)', 'warn');
}

function confirmClearAll() {
  if (!confirm('Clear ALL sales AND customers? This is a full fresh start.')) return;
  if (!confirm('Final check — this wipes all invoices and customer data permanently.')) return;
  state.sales = [];
  state.customers = [];
  state.settings.nextInvoice = 1001;
  window._ownerDeleteInProgress = true;
  save().finally(() => { window._ownerDeleteInProgress = false; });
  syncBizStatusUI();
  closeModal();
  toast('All sales & customers cleared ✅ Fresh start!');
  logActivity('Owner performed full data clear (fresh start)', 'warn');
}


// ============================================================
// USER ACCESS CONTROL — Owner only
// ============================================================
const ROLE_LABELS = {
  admin:       {icon:'👑', label:'Admin/Owner',  color:'#7c3aed'},
  manager:     {icon:'🔑', label:'Manager',      color:'#0f766e'},
  salesperson: {icon:'🧾', label:'Salesperson',  color:'#2563eb'},
  delivery:    {icon:'🛵', label:'Delivery',     color:'#d97706'}
};
const ROLE_ACCESS_DESC = {
  admin:       'Full access — all tabs, SKU editor, profit reports, owner dashboard',
  manager:     'Most tabs — no profit figures, no SKU price editor, no User Management',
  salesperson: 'Sale, Stock, Wholesale, WhatsApp, Collections, AI Brain',
  delivery:    'Sale entry, Stock view, Collections only'
};

function renderOwnerUserList() {
  const div = document.getElementById('owner-user-list');
  if (!div) return;
  const isAdmin = state.session?.role === 'admin';
  if (!isAdmin) { div.innerHTML = '<div style="color:var(--muted);font-size:13px;">Only Admin can manage users.</div>'; return; }
  if (!state.users.length) { div.innerHTML = '<div class="empty">No users yet</div>'; return; }

  div.innerHTML = state.users.map(u => {
    const rl = ROLE_LABELS[u.role] || {icon:'👤', label:u.role, color:'#64748b'};
    const isActive = u.active !== false;
    const isSelf = u.id === state.session?.userId;
    return `<div style="background:${isActive?'white':'#f8fafc'};border-radius:12px;padding:12px;margin-bottom:8px;border:1.5px solid ${isActive?'var(--border)':'#e2e8f0'};opacity:${isActive?1:.7};">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:8px;">
        <div style="flex:1;">
          <div style="font-weight:700;font-size:14px;">${u.name} ${isSelf?'<span class="tag" style="background:#dbeafe;color:#1e40af;">You</span>':''}</div>
          <div style="margin-top:4px;display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
            <span style="background:${rl.color}18;color:${rl.color};padding:2px 8px;border-radius:6px;font-size:11px;font-weight:700;">${rl.icon} ${rl.label}</span>
            <span class="${isActive?'tag':'tag dead'}">${isActive?'✅ Active':'🔒 Inactive'}</span>
          </div>
          <div style="font-size:11px;color:var(--muted);margin-top:4px;">${ROLE_ACCESS_DESC[u.role]||''}</div>
        </div>
        ${!isSelf ? `<button class="btn btn-sm btn-outline" style="white-space:nowrap;min-width:64px;" onclick="ownerEditUser('${u.id}')">Edit</button>` : ''}
      </div>
    </div>`;
  }).join('');
}

function ownerEditUser(userId) {
  const u = state.users.find(x => x.id === userId);
  if (!u) return;
  const isSelf = u.id === state.session?.userId;
  showModal('Edit User: ' + u.name, `
    <div class="field">
      <label>Role</label>
      <select class="select" id="eu-role">
        ${Object.entries(ROLE_LABELS).map(([r,d]) => `<option value="${r}" ${u.role===r?'selected':''}>${d.icon} ${d.label}</option>`).join('')}
      </select>
      <div id="eu-role-desc" style="font-size:11px;color:var(--muted);margin-top:4px;">${ROLE_ACCESS_DESC[u.role]||''}</div>
    </div>
    <div style="background:var(--bg);border-radius:10px;padding:12px;margin-bottom:10px;">
      <div style="font-weight:600;font-size:13px;margin-bottom:6px;">Account Status</div>
      <div style="display:flex;gap:8px;">
        <button id="eu-active-btn" class="btn btn-sm ${u.active!==false?'btn-success':'btn-outline'}" onclick="euToggleActive(true)" style="flex:1;">✅ Active</button>
        <button id="eu-inactive-btn" class="btn btn-sm ${u.active===false?'btn-danger':'btn-outline'}" onclick="euToggleActive(false)" style="flex:1;">🔒 Inactive</button>
      </div>
      <div style="font-size:11px;color:var(--muted);margin-top:6px;">Inactive users cannot login to the app.</div>
    </div>
    <div class="field">
      <label>Reset PIN (leave blank to keep current)</label>
      <input type="password" class="input" id="eu-pin" maxlength="4" inputmode="numeric" placeholder="New 4-digit PIN">
    </div>
    <button class="btn btn-success" onclick="ownerSaveUser('${userId}')">✓ Save Changes</button>
    ${!isSelf ? `<button class="btn btn-danger mt-1" onclick="ownerDeleteUser('${userId}')">🗑 Remove User</button>` : ''}
  `);
  // Live role description update
  document.getElementById('eu-role').addEventListener('change', function() {
    document.getElementById('eu-role-desc').textContent = ROLE_ACCESS_DESC[this.value] || '';
  });
  // Store temp active state
  window._euActive = u.active !== false;
}

function euToggleActive(val) {
  window._euActive = val;
  document.getElementById('eu-active-btn').className   = 'btn btn-sm ' + (val ? 'btn-success' : 'btn-outline');
  document.getElementById('eu-inactive-btn').className = 'btn btn-sm ' + (!val ? 'btn-danger'  : 'btn-outline');
}

async function ownerSaveUser(userId) {
  const u = state.users.find(x => x.id === userId);
  if (!u) return;
  const newRole = document.getElementById('eu-role').value;
  const newPin  = document.getElementById('eu-pin').value.trim();
  const newActive = window._euActive !== undefined ? window._euActive : (u.active !== false);

  if (newPin && (!/^\d{4}$/.test(newPin))) { toast('PIN must be exactly 4 digits'); return; }

  u.role   = newRole;
  u.active = newActive;
  if (newPin) {
    u.pinSalt = makeSalt();
    u.pinHash = await hashPin(newPin, u.pinSalt);
    delete u.pin;
    if (typeof RX !== 'undefined') RX.log('info','auth','admin reset PIN for ' + u.name);
  }

  // Sync role to staff record too
  const staffRec = state.staff.find(s => s.id === u.staffId);
  if (staffRec) staffRec.role = newRole;

  // If deactivating someone currently logged in on another session, note it
  logActivity(`User ${u.name} → role:${newRole}, active:${newActive}`, 'info');
  save();

  // Refresh login dropdown so newly activated users appear immediately
  const sel = document.getElementById('login-staff-select');
  if (sel) {
    sel.innerHTML = '<option value="">— select your name —</option>' +
      state.users.filter(u => u.active !== false)
        .map(u => `<option value="${u.id}">${u.name} (${u.role})</option>`)
        .join('');
  }
  // Also refresh datalist for text input
  const dl = document.getElementById('login-staff-list');
  if (dl) {
    dl.innerHTML = state.users.filter(u => u.active !== false)
      .map(u => `<option value="${u.name}" data-id="${u.id}">${u.name} (${u.role})</option>`)
      .join('');
  }

  // Force logout if the currently logged-in user was just deactivated
  if (!newActive && state.session && state.session.userId === userId) {
    closeModal();
    renderOwnerUserList();
    state.session = null;
    DB.set('session', null);
    toast('⚠️ Your account was deactivated. Logging out...');
    setTimeout(() => { location.reload(); }, 1200);
    return;
  }

  closeModal();
  renderOwnerUserList();
  applyRoleUI();
  toast(`✅ ${u.name} updated`);
}

function ownerDeleteUser(userId) {
  const u = state.users.find(x => x.id === userId);
  if (!u || !confirm(`Remove ${u.name} completely? They will not be able to login.`)) return;
  state.users = state.users.filter(x => x.id !== userId);
  // Keep staff record but deactivate it
  const staffRec = state.staff.find(s => s.id === u.staffId);
  if (staffRec) staffRec.active = false;
  save();
  closeModal();
  renderOwnerUserList();
  toast(`${u.name} removed`);
}

function ownerAddUser() {
  showModal('+ Add New User', `
    <div class="field"><label>Name *</label><input class="input" id="nu-name" placeholder="e.g. Ramesh Kumar"></div>
    <div class="field">
      <label>Role</label>
      <select class="select" id="nu-role" onchange="document.getElementById('nu-role-desc').textContent=ROLE_ACCESS_DESC[this.value]||''">
        ${Object.entries(ROLE_LABELS).map(([r,d]) => `<option value="${r}">${d.icon} ${d.label}</option>`).join('')}
      </select>
      <div id="nu-role-desc" style="font-size:11px;color:var(--muted);margin-top:4px;">${ROLE_ACCESS_DESC['admin']}</div>
    </div>
    <div class="field"><label>4-digit PIN *</label><input type="password" class="input" id="nu-pin" maxlength="4" inputmode="numeric" placeholder="••••"></div>
    <div style="background:#f0fdf4;border-radius:10px;padding:10px;margin-bottom:10px;font-size:12px;color:var(--success);">
      ✅ Account will be <b>Active</b> immediately. You can deactivate anytime from this panel.
    </div>
    <button class="btn btn-success" onclick="ownerCreateUser()">✓ Create & Activate</button>
  `);
}

async function ownerCreateUser() {
  const name = document.getElementById('nu-name').value.trim();
  const role = document.getElementById('nu-role').value;
  const pin  = document.getElementById('nu-pin').value.trim();
  if (!name) { toast('Name required'); return; }
  if (!pin || !/^\d{4}$/.test(pin)) { toast('PIN must be 4 digits'); return; }

  // Create staff + user records (PIN hashed)
  const staffId = uid(), userId = uid();
  const newSalt = makeSalt();
  const newHash = await hashPin(pin, newSalt);
  state.staff.push({id:staffId, name, role, baseSalary:0, commRate:state.settings.commRate||1.5, delivRate:state.settings.delivRate||50, joinDate:todayStr(), active:true});
  state.users.push({id:userId, name, role, pinHash:newHash, pinSalt:newSalt, staffId, active:true, approvedBy: state.session?.name || 'admin'});
  logActivity(`New user created: ${name} (${role})`, 'info');
  save();
  closeModal();
  renderOwnerUserList();
  // Refresh login dropdown
  const sel = document.getElementById('login-staff-select');
  const activeUsers = state.users.filter(u => u.active !== false);
  if (sel) sel.innerHTML = '<option value="">— select your name —</option>' + activeUsers.map(u=>`<option value="${u.id}">${u.name} (${u.role})</option>`).join('');
  const dl = document.getElementById('login-staff-list');
  if (dl) dl.innerHTML = activeUsers.map(u=>`<option value="${u.name}" data-id="${u.id}">${u.name} (${u.role})</option>`).join('');
  toast(`✅ ${name} added — they can now login`);
}

// ============================================================
// COLLECTIONS & EXPENSES
// ============================================================
function showCollSection(name, evt) {
  document.querySelectorAll('.coll-section').forEach(s=>s.style.display='none');
  const el=document.getElementById('coll-'+name); if(el)el.style.display='block';
  if(evt){document.querySelectorAll('#coll-chips .chip').forEach(c=>c.classList.remove('active'));evt.target.classList.add('active');}
  if(name==='summary')renderCollSummary();
  if(name==='history')renderCollHistory();
  if(name==='trips')renderCollTrips();
}

function initCollectionsTab() {
  const custSel=document.getElementById('coll-cust-sel');
  if(custSel)custSel.innerHTML='<option value="">— select existing customer —</option>'+
    [...state.customers,...(state.retailCustomers||[])].map(c=>`<option value="${c.id}" data-phone="${c.phone||''}">${c.name}</option>`).join('');
  ['coll-by','exp-by'].forEach(id=>{
    const sel=document.getElementById(id);
    if(sel)sel.innerHTML='<option value="">— staff —</option>'+state.staff.filter(s=>s.active).map(s=>`<option value="${s.id}">${s.name}</option>`).join('');
  });
  ['coll-date','exp-date'].forEach(id=>{const el=document.getElementById(id);if(el&&!el.value)el.value=todayStr();});
  renderCollSummary();
}

function prefillCollPhone() {
  const sel=document.getElementById('coll-cust-sel');
  const opt=sel.options[sel.selectedIndex];
  document.getElementById('coll-phone').value=opt?.dataset?.phone||'';
}

async function saveCollection() {
  const amount=+document.getElementById('coll-amount').value;
  if(!amount){toast('Enter amount');return;}
  const custSel=document.getElementById('coll-cust-sel').value;
  const custName=custSel?([...state.customers,...(state.retailCustomers||[])].find(c=>c.id===custSel)?.name||''):document.getElementById('coll-cust-name').value.trim();
  if(!custName){toast('Enter customer name');return;}
  const entry={id:uid(),type:'collection',custId:custSel||null,custName,phone:document.getElementById('coll-phone').value.trim(),amount,mode:document.getElementById('coll-mode').value,date:document.getElementById('coll-date').value||todayStr(),collectedBy:document.getElementById('coll-by').value,location:document.getElementById('coll-location').value.trim(),note:document.getElementById('coll-note').value.trim()};
  state.collections.push(entry);
  if(custSel){const c=getCustomerById(custSel);if(c)c.totalDue=Math.max(0,(c.totalDue||0)-amount);}
  logActivity(`Collection ₹${amount} from ${custName} via ${entry.mode}`,'info');
  try { recomputeTotalDue(); } catch(e) { console.warn('[totalDue] recomputeTotalDue failed:', e.message); }
  save(); toast('✅ Collection saved!');
  document.getElementById('coll-wa-btn').style.display=entry.phone?'block':'none';
  window._lastCollEntry=entry;
  document.getElementById('coll-amount').value='';
  document.getElementById('coll-cust-name').value='';
  document.getElementById('coll-note').value='';
  renderCollSummary(); renderHomeCollections();
}

async function saveExpense() {
  const amount=+document.getElementById('exp-amount').value;
  if(!amount){toast('Enter amount');return;}
  const entry={id:uid(),type:'expense',expType:document.getElementById('exp-type').value,amount,date:document.getElementById('exp-date').value||todayStr(),by:document.getElementById('exp-by').value,trip:document.getElementById('exp-trip').value.trim(),note:document.getElementById('exp-note').value.trim()};
  state.expenses.push(entry);
  logActivity(`Expense ${rs(amount)} — ${entry.expType}${entry.trip?' ('+entry.trip+')':''}`, 'warn');
  save(); toast('✅ Expense saved!');
  document.getElementById('exp-amount').value='';
  document.getElementById('exp-note').value='';
  document.getElementById('exp-trip').value='';
  renderCollSummary(); renderHomeCollections();
}

function sendCollectionWA() {
  const e=window._lastCollEntry; if(!e||!e.phone)return;
  const msg=`✅ *${state.settings.shopName}*\n\n₹${e.amount.toLocaleString('en-IN')} collected from you via *${e.mode.toUpperCase()}* on ${new Date(e.date).toLocaleDateString('en-IN')}.\n\nThank you for your payment! 🙏\n_— ${state.settings.shopName}_`;
  window.open(`https://wa.me/${e.phone}?text=${encodeURIComponent(msg)}`);
}

function renderCollSummary() {
  const today=todayStr();
  const todayColl=(state.collections||[]).filter(c=>c.date===today);
  const todayExp=(state.expenses||[]).filter(e=>e.date===today);
  const collTotal=todayColl.reduce((a,c)=>a+c.amount,0);
  const expTotal=todayExp.reduce((a,e)=>a+e.amount,0);
  const outstanding=state.customers.reduce((a,c)=>a+(c.totalDue||0),0);
  const setEl=(id,v)=>{const e=document.getElementById(id);if(e)e.textContent=v;};
  setEl('cs-collected',rs(collTotal)); setEl('cs-expense',rs(expTotal));
  setEl('cs-net',rs(collTotal-expTotal)); setEl('cs-outstanding',rs(outstanding));
  setEl('csm-cash',rs(todayColl.filter(c=>c.mode==='cash').reduce((a,c)=>a+c.amount,0)));
  setEl('csm-upi',rs(todayColl.filter(c=>c.mode==='upi').reduce((a,c)=>a+c.amount,0)));
  setEl('csm-cheque',rs(todayColl.filter(c=>c.mode==='cheque').reduce((a,c)=>a+c.amount,0)));
  const list=document.getElementById('cs-today-list'); if(!list)return;
  const all=[...todayColl.map(c=>({...c,_t:'c'})),...todayExp.map(e=>({...e,_t:'e'}))].sort((a,b)=>b.id.localeCompare(a.id));
  list.innerHTML=all.length===0?'<div class="empty">No entries today</div>':all.map(e=>e._t==='c'
    ?`<div class="coll-item"><div class="flex-between"><b>${e.custName}</b><b style="color:var(--success);">+${rs(e.amount)}</b></div><small>${e.mode.toUpperCase()} · ${e.location||'—'} · ${state.staff.find(s=>s.id===e.collectedBy)?.name||'—'}</small></div>`
    :`<div class="coll-item expense"><div class="flex-between"><b>${e.expType}${e.trip?' — '+e.trip:''}</b><b style="color:var(--danger);">−${rs(e.amount)}</b></div><small>${state.staff.find(s=>s.id===e.by)?.name||'—'} · ${e.note||'—'}</small></div>`
  ).join('');
}

function renderCollHistory() {
  const q=(document.getElementById('coll-search')?.value||'').toLowerCase();
  const all=[...(state.collections||[]).map(c=>({...c,_t:'c'})),...(state.expenses||[]).map(e=>({...e,_t:'e'}))]
    .filter(e=>!q||e.custName?.toLowerCase().includes(q)||e.location?.toLowerCase().includes(q)||e.trip?.toLowerCase().includes(q)||e.expType?.toLowerCase().includes(q))
    .sort((a,b)=>b.date.localeCompare(a.date)).slice(0,60);
  const div=document.getElementById('coll-history-list');if(!div)return;
  div.innerHTML=all.length===0?'<div class="empty">No entries</div>':all.map(e=>e._t==='c'
    ?`<div class="coll-item"><div class="flex-between"><b>${e.custName}</b><b style="color:var(--success);">+${rs(e.amount)}</b></div><small>${new Date(e.date).toLocaleDateString('en-IN')} · ${e.mode.toUpperCase()} · ${e.location||'—'}</small></div>`
    :`<div class="coll-item expense"><div class="flex-between"><b>${e.expType}${e.trip?' — '+e.trip:''}</b><b style="color:var(--danger);">−${rs(e.amount)}</b></div><small>${new Date(e.date).toLocaleDateString('en-IN')} · ${e.note||'—'}</small></div>`
  ).join('');
}

function renderCollTrips() {
  const trips={};
  (state.collections||[]).forEach(c=>{const k=c.location||'No Location';if(!trips[k])trips[k]={collected:0,expenses:0,count:0};trips[k].collected+=c.amount;trips[k].count++;});
  (state.expenses||[]).forEach(e=>{const k=e.trip||'No Trip';if(!trips[k])trips[k]={collected:0,expenses:0,count:0};trips[k].expenses+=e.amount;});
  const div=document.getElementById('coll-trip-list');if(!div)return;
  const sorted=Object.entries(trips).sort((a,b)=>(b[1].collected-b[1].expenses)-(a[1].collected-a[1].expenses));
  div.innerHTML=sorted.length===0?'<div class="empty">No trip data</div>':sorted.map(([area,d])=>{
    const net=d.collected-d.expenses;
    return`<div class="trip-card"><div class="flex-between"><b>📍 ${area}</b><div class="trip-profit ${net>=0?'pos':'neg'}">${net>=0?'+':''}${rs(net)}</div></div><div class="mode-split" style="margin-top:8px;"><div class="mode-pill"><div class="mp-num" style="color:var(--success);">${rs(d.collected)}</div><div>Collected</div></div><div class="mode-pill"><div class="mp-num" style="color:var(--danger);">${rs(d.expenses)}</div><div>Expenses</div></div><div class="mode-pill"><div class="mp-num">${d.count}</div><div>Shops</div></div></div></div>`;
  }).join('');
}

// ============================================================
// AI DATA CLEANER WIZARD
// ============================================================
window._cleanerParts = [];
window._cleanerIdx = 0;

function resetCleaner() {
  window._cleanerParts = [];
  window._cleanerIdx = 0;
  document.getElementById('cleaner-step1').style.display = 'block';
  document.getElementById('cleaner-step2').style.display = 'none';
  document.getElementById('cleaner-preview').style.display = 'none';
  document.getElementById('cleaner-input').value = '';
  document.getElementById('cleaner-progress').innerHTML = '';
}

async function startDataCleaner() {
  const raw = document.getElementById('cleaner-input').value.trim();
  if (!raw) { toast('Paste some part names first'); return; }
  if (!getAIKey() || !getAIKey().startsWith('sk-')) {
    toast('⚠️ Claude API key needed. Set it in ⚙ AI Settings.'); return;
  }

  const prefix = (document.getElementById('cleaner-prefix').value || 'GOURY').trim().toUpperCase();
  const startNum = parseInt(document.getElementById('cleaner-start-num').value) || 1;

  // Find highest existing part number with this prefix to avoid duplicates
  const existingNums = state.parts
    .map(p => p.partNo)
    .filter(pn => pn && pn.startsWith(prefix))
    .map(pn => parseInt(pn.replace(prefix, '')) || 0)
    .filter(n => !isNaN(n));
  const maxExisting = existingNums.length > 0 ? Math.max(...existingNums) : 0;
  const actualStart = Math.max(startNum, maxExisting + 1);

  const lines = raw.split('\n').map(l => l.trim()).filter(Boolean);
  const progDiv = document.getElementById('cleaner-progress');
  progDiv.innerHTML = `<div style="background:var(--ai-light);padding:10px;border-radius:8px;color:var(--ai);font-weight:600;">🧹 Cleaning ${lines.length} parts… please wait</div>`;

  const BATCH = 10;
  let allCleaned = [];
  for (let i = 0; i < lines.length; i += BATCH) {
    const batch = lines.slice(i, i + BATCH);
    try {
      const cleaned = await aiCleanNames(batch);
      allCleaned.push(...cleaned);
    } catch(e) {
      // fallback: use raw line as name
      batch.forEach(b => allCleaned.push({ name: b, brand: '', machineType: 'tractor', category: '' }));
    }
    const pct = Math.round(Math.min((i + BATCH) / lines.length * 100, 100));
    progDiv.innerHTML = `<div style="background:var(--ai-light);padding:10px;border-radius:8px;"><div style="color:var(--ai);font-weight:600;">🧹 Cleaning… ${pct}%</div><div class="cleaner-prog-bar"><div class="cleaner-prog-fill" style="width:${pct}%"></div></div></div>`;
    await new Promise(r => setTimeout(r, 100));
  }

  // Assign GOURY part numbers
  window._cleanerParts = allCleaned.map((p, i) => ({
    ...p,
    partNo: prefix + String(actualStart + i).padStart(3, '0'),
    costPrice: 0, mrp: 0, instantPrice: 0, creditPrice: 0, stock: 0,
    _done: false
  }));
  window._cleanerIdx = 0;

  progDiv.innerHTML = `<div style="background:#dcfce7;padding:10px;border-radius:8px;color:#166534;font-weight:600;">✅ ${allCleaned.length} parts cleaned! Now enter prices →</div>`;

  // Show cleaned preview
  renderCleanerPreview();

  // Start price wizard
  document.getElementById('cleaner-step1').style.display = 'none';
  document.getElementById('cleaner-step2').style.display = 'block';
  showCleanerPriceCard(0);
}

async function aiCleanNames(lines) {
  const key = getAIKey();
  const model = getAIModel();
  const prompt = `You are a spare parts standardization expert for Indian agricultural & construction machinery.

Clean these raw part name lines into proper standardized English.

Rules:
- Full proper English words: "asm"→"Assembly", "plt"→"Plate", "brng"/"brg"→"Bearing", "fil"/"filtr"→"Filter", "hyd"→"Hydraulic", "univrsl"→"Universal", "mtr"→"Motor", "cyl"→"Cylinder", "blde"→"Blade", "sft"→"Shaft", "pmp"→"Pump", "inj"→"Injector", "gsk"→"Gasket", "vlv"→"Valve", "spr"→"Spring", "bsh"→"Bush", "brkt"→"Bracket", "assy"→"Assembly"
- Title Case for all names
- Brand names: "jd"→"John Deere", "mf"→"Massey Ferguson", "m&m"/"mahindra"→"Mahindra", "jcb"→"JCB", "skf"→"SKF", "fiat"→"FIAT", "sonalika"→"Sonalika", "swaraj"→"Swaraj", "eicher"→"Eicher"
- Machine types: normalize to one of: tractor / harvester / jcb / rotavator / other
- Extract category if obvious (Bearing, Filter, Clutch, Hydraulic, Blade, Chain, etc.)
- Remove duplicate words

Return ONLY a JSON array. Each item: {"name":"Full Proper Name","brand":"Brand","machineType":"tractor","category":"Category"}

Lines:
${lines.map((l,i) => `${i+1}. ${l}`).join('\n')}

Return ONLY the JSON array, no explanation, no markdown.`;

  const resp = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'content-type':'application/json','x-api-key':key,'anthropic-version':'2023-06-01','anthropic-dangerous-direct-browser-access':'true' },
    body: JSON.stringify({ model, max_tokens: 1500, messages: [{ role:'user', content: prompt }] })
  });
  if (!resp.ok) throw new Error(`API ${resp.status}`);
  const data = await resp.json();
  let text = data.content[0].text.trim().replace(/```json|```/g, '').trim();
  return JSON.parse(text);
}

function renderCleanerPreview() {
  const wrap = document.getElementById('cleaner-preview');
  const list = document.getElementById('cleaner-preview-list');
  wrap.style.display = 'block';
  list.innerHTML = window._cleanerParts.map(p => `
    <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--border);font-size:13px;">
      <div><span class="tag ai" style="font-size:11px;">${p.partNo}</span> <b>${p.name}</b><br><small>${p.brand||''}${p.brand&&p.machineType?' · ':''}${p.machineType||''}${p.category?' · '+p.category:''}</small></div>
      <span class="${p._done?'tag':'tag warn'}">${p._done?'✓ Done':'Pending'}</span>
    </div>`).join('');
}

function showCleanerPriceCard(idx) {
  const parts = window._cleanerParts;
  const total = parts.length;

  // Update progress bar
  const done = parts.filter(p => p._done).length;
  document.getElementById('cleaner-price-progress-text').textContent = `Part ${idx + 1} of ${total}`;
  document.getElementById('cleaner-price-bar').style.width = (done / total * 100) + '%';
  document.getElementById('cleaner-price-count').textContent = `${done} saved · ${total - done} remaining`;

  if (idx >= total) {
    // All done
    document.getElementById('cleaner-price-card').style.display = 'none';
    document.getElementById('cleaner-price-done').style.display = 'block';
    const finalDone = parts.filter(p => p._done).length;
    document.getElementById('cleaner-done-summary').textContent = `${finalDone} parts ready to add to inventory (${total - finalDone} skipped)`;
    renderCleanerPreview();
    return;
  }

  const p = parts[idx];
  document.getElementById('cleaner-price-card').style.display = 'block';
  document.getElementById('cleaner-price-done').style.display = 'none';
  document.getElementById('cleaner-current-partno').textContent = p.partNo;
  document.getElementById('cleaner-current-name').textContent = p.name;
  document.getElementById('cleaner-current-meta').textContent = [p.brand, p.machineType, p.category].filter(Boolean).join(' · ');

  // Clear price fields
  ['cp-cost','cp-mrp','cp-retail','cp-wholesale','cp-stock'].forEach(id => {
    document.getElementById(id).value = '';
  });
  // Focus first field
  setTimeout(() => document.getElementById('cp-cost').focus(), 100);
}

function saveCleanerPrice() {
  const idx = window._cleanerIdx;
  const p = window._cleanerParts[idx];
  const cost = +document.getElementById('cp-cost').value || 0;
  const mrp = +document.getElementById('cp-mrp').value || 0;
  const retail = +document.getElementById('cp-retail').value || 0;
  const wholesale = +document.getElementById('cp-wholesale').value || 0;
  const stock = +document.getElementById('cp-stock').value || 0;

  p.costPrice = cost;
  p.mrp = mrp || Math.round(retail * 1.2);
  p.instantPrice = retail;
  p.creditPrice = wholesale || Math.round(retail * 0.95);
  p.stock = stock;
  p._done = true;

  window._cleanerIdx++;
  renderCleanerPreview();
  showCleanerPriceCard(window._cleanerIdx);
}

function skipCleanerPrice() {
  window._cleanerIdx++;
  showCleanerPriceCard(window._cleanerIdx);
}

function applyCleanerToInventory() {
  const toAdd = window._cleanerParts.filter(p => p._done && p.name);
  if (toAdd.length === 0) { toast('No parts to add'); return; }
  const _newParts = [];
  toAdd.forEach(p => {
    const np = {
      id: uid(), partNo: p.partNo, name: p.name,
      brand: p.brand || '', machineType: p.machineType || 'tractor',
      category: p.category || '', models: '',
      costPrice: p.costPrice, mrp: p.mrp,
      instantPrice: p.instantPrice, creditPrice: p.creditPrice,
      stock: p.stock, minStock: 0,
      location: '', supplierName: '', supplierPhone: '',
      addedAt: Date.now()
    };
    state.parts.push(np);
    _newParts.push(np);
  });
  logActivity(`Data Cleaner: ${toAdd.length} parts added to inventory`, 'info');
  save();
  // Write each new part to the per-doc cloud collection so other devices sync them
  // (firebasePush skips the parts blob in per-doc mode).
  if (typeof cloudPersistPart === 'function') _newParts.forEach(function(p){ try { cloudPersistPart(p); } catch(e){} });
  renderInventory();
  toast(`✅ ${toAdd.length} parts added to inventory!`);
  resetCleaner();
  showBrainSection('briefing');
}

// ============================================================
// DATA INTEGRITY — reconcile derived fields from source-of-truth
// Run at startup + after any bulk operation that touches totals
// ============================================================
function recomputeTotalDue() {
  // ── Customer totalDue ──────────────────────────────────
  // Source of truth: state.sales (credit, not yet settled) minus state.collections
  const custSalesDue = {};
  (state.sales || []).forEach(function(s) {
    if (s.paymentMode === 'credit' && s.customerId && !s.settled) {
      custSalesDue[s.customerId] = (custSalesDue[s.customerId] || 0) + (s.total || 0);
    }
  });
  (state.wholesaleOrders || []).forEach(function(wo) {
    if (wo.paymentMode === 'credit' && wo.customerId && !wo.settled) {
      // Use actual outstanding (total minus paidAmount) so partial payments are reflected
      const woDue = Math.max(0, (wo.total || 0) - (wo.paidAmount || 0));
      custSalesDue[wo.customerId] = (custSalesDue[wo.customerId] || 0) + woDue;
    }
  });
  (state.collections || []).forEach(function(col) {
    if (col.custId && col.amount) {
      custSalesDue[col.custId] = Math.max(0, (custSalesDue[col.custId] || 0) - col.amount);
    }
  });
  (state.customers || []).forEach(function(c) {
    c.totalDue = Math.max(0, Math.round(custSalesDue[c.id] || 0));
  });

  // ── Supplier totalDue ──────────────────────────────────
  // Source of truth: purchases minus paidAmount
  const supDue = {};
  (state.purchases || []).forEach(function(p) {
    if (p.supplierId) {
      supDue[p.supplierId] = (supDue[p.supplierId] || 0) + Math.max(0, (p.amount || 0) - (p.paidAmount || 0));
    }
  });
  (state.suppliers || []).forEach(function(s) {
    s.totalDue = Math.max(0, Math.round(supDue[s.id] || 0));
  });
}

// ── Integrity boot check ───────────────────────────────────
// Called once after cloud data is loaded. Silent — just fixes drift.
function runIntegrityChecks() {
  recomputeTotalDue();

  // Clamp negative stock (can happen from sync races or manual edits)
  (state.parts || []).forEach(function(p) {
    if (p.stock < 0) p.stock = 0;
    if (p.stockOnWay < 0) p.stockOnWay = 0;
  });

  // Heal any part left with a missing/undefined updatedAt by the old merge
  // bug — Firestore refuses to write a document containing an `undefined`
  // field, so a part in this state can never sync and jams the batch/queue
  // it's part of (including the once-per-device parts migration) on every
  // retry, forever, on every device that hasn't already fixed it locally.
  (state.parts || []).forEach(function(p) {
    if (p && (p.updatedAt === undefined || p.updatedAt === null)) {
      p.updatedAt = p.addedAt || Date.now();
    }
  });

  // Ensure supplier phone propagation — if a supplier record has a phone
  // and a part has the same supplier name but no phone, fill it in
  (state.suppliers || []).forEach(function(s) {
    if (!s.phone) return;
    (state.parts || []).forEach(function(p) {
      if (p.supplierName && p.supplierName.trim().toLowerCase() === s.name.trim().toLowerCase() && !p.supplierPhone) {
        p.supplierPhone = s.phone;
      }
    });
  });

  // Back-fill any supplier entry missing from state.suppliers
  // (parts added before the supplier tab existed)
  (state.parts || []).forEach(function(p) {
    if (!p.supplierName) return;
    ensureSupplierRegistered && ensureSupplierRegistered(p.supplierName, p.supplierPhone);
  });

  // Orphan guard: remove supplierAdvance entries whose supplierId no longer exists
  // (soft — just log, don't delete)
  const supIds = new Set((state.suppliers || []).map(function(s){ return s.id; }));
  (state.supplierAdvances || []).forEach(function(a) {
    if (a.supplierId && !supIds.has(a.supplierId)) {
      console.warn('[Integrity] orphan advance', a.id, 'supplierId', a.supplierId, 'not in suppliers');
    }
  });
}

// ============================================================
// ============================================================
// UNIVERSAL MERGE — used by all sync paths
// ============================================================
// mergeArrays(local, cloud, key) → merged array
// Rules:
//   Local-only record  → keep (not yet synced to cloud)
//   Cloud-only record  → take (added on another device)
//   Both exist         → field-level merge: newer updatedAt wins per record
//                        for parts: never wipe a field that has a value locally
//                        if timestamps equal or missing: union of non-empty fields
function mergeArrays(local, cloud, key) {
  const localById = {};
  (local||[]).forEach(function(r){ if(r&&r.id) localById[r.id]=r; });
  const cloudById = {};
  (cloud||[]).forEach(function(r){ if(r&&r.id) cloudById[r.id]=r; });
  const allIds = new Set([...Object.keys(localById), ...Object.keys(cloudById)]);
  const merged = [];

  allIds.forEach(function(id) {
    const l = localById[id];
    const c = cloudById[id];

    // Only one side has it
    if (!l) { merged.push(c); return; }
    if (!c) { merged.push(l); return; }

    // Both have it — determine winner per record
    const lTs = l.updatedAt || l.addedAt || 0;
    const cTs = c.updatedAt || c.addedAt || 0;

    if (key === 'parts') {
      // Parts: always field-level union — neither device should lose data
      // Base: take whichever was updated more recently
      const base = (cTs > lTs) ? Object.assign({}, l, c) : Object.assign({}, c, l);
      // Critical fields: always keep the non-zero local value (pricing set by owner)
      // This means: if local has a price and cloud doesn't (or has lower), local wins
      const result = Object.assign({}, base);
      if (l.costPrice    > 0) result.costPrice    = l.costPrice;
      if (l.instantPrice > 0) result.instantPrice = l.instantPrice;
      if (l.creditPrice  > 0) result.creditPrice  = l.creditPrice;
      if (l.mrp          > 0) result.mrp          = l.mrp;
      if (l.stock        >= 0 && lTs >= cTs) result.stock = l.stock; // stock: timestamp decides
      if (l.location)         result.location     = l.location;
      if (l.photoDataUrl && l.photoDataUrl.startsWith('https://')) result.photoDataUrl = l.photoDataUrl;
      // Keep the newer updatedAt. Firestore rejects `undefined` as a field value,
      // so when both sides are missing a timestamp (lTs===cTs===0), fall back to
      // Date.now() instead of undefined — otherwise every future write of this
      // part throws "Unsupported field value: undefined" and never syncs.
      result.updatedAt = Math.max(lTs, cTs) || Date.now();
      merged.push(result);
    } else {
      // Other arrays: simpler — newer timestamp wins the whole record
      if (cTs > lTs) { merged.push(c); }
      else { merged.push(l); }
    }
  });

  return merged;
}

// Real-time per-part Firestore handlers (shared by initial listener + visibility reconnect)
// Debounced re-render so a burst of incoming docs from another device repaints once.
let _partListenerRenderTimer = null;
function _scheduleListenerRender() {
  clearTimeout(_partListenerRenderTimer);
  _partListenerRenderTimer = setTimeout(function() {
    const activeTab = document.querySelector('.tab-content.active');
    const tabId = activeTab ? activeTab.id.replace('tab-','') : '';
    if (tabId === 'inventory') { try { renderInventory(); } catch(e){} }
    else if (tabId === 'home') { try { renderHome(); } catch(e){} }
    else if (tabId === 'catalogue') { try { catRenderDadQueue && catRenderDadQueue(); } catch(e){} }
  }, 250);
}

// Merge ONE incoming cloud part into state. Pure apply — no persist, no render, no DOM.
// idxMap: id → array index, built ONCE per batch (replaces per-doc findIndex O(n²)).
// Returns 'new' | 'merged' | 'skipped'.
// Firestore hands back _serverTs as a Timestamp class instance. Normalise to a plain
// millisecond number: class instances lose their prototype through IndexedDB's
// structured clone, which would leave a useless {seconds, nanoseconds} shell in state.
function _serverTsToMillis(ts) {
  if (!ts) return 0;
  if (typeof ts === 'number') return ts;
  if (typeof ts.toMillis === 'function') { try { return ts.toMillis(); } catch(e) {} }
  if (typeof ts.seconds === 'number') return ts.seconds * 1000;
  return Date.now();
}

function _applyPartChangeCore(updatedPart, idxMap) {
  if (!updatedPart || !updatedPart.id) return 'skipped';
  const idx = (updatedPart.id in idxMap) ? idxMap[updatedPart.id] : -1;
  if (idx < 0) {
    // A part from ANOTHER device. (This device's own newly-created parts are
    // already in state, so they take the branch below — never this one.)
    state.parts.push(updatedPart);
    idxMap[updatedPart.id] = state.parts.length - 1;
    return 'new';
  }
  const local = state.parts[idx];
  const lTs = local.updatedAt || local.addedAt || 0;
  const cTs = updatedPart.updatedAt || updatedPart.addedAt || 0;
  // ── ROOT-CAUSE FIX for the permanently-stuck "N parts syncing" pill ────────
  // _serverTs is written OUTBOUND on every cloud write (serverTimestamp()) but was
  // never read back into local state. The echo that carries it has the SAME
  // updatedAt as the local copy, so it hit the `cTs === lTs` skip below and the
  // local record never learned it had reached the server. _partNeedsHeal() checks
  // `!p._serverTs`, so every part this device ever wrote stayed "stranded" forever:
  // the healer re-enqueued them, the drain wrote them (successfully — which is why
  // they ARE in the cloud), and the next heal pass enqueued them all over again.
  // Stamping the flag here breaks that loop at its source. Stored as a plain number
  // so it survives the structured clone into IndexedDB.
  let _stamped = false;
  if (updatedPart._serverTs && !local._serverTs) {
    local._serverTs = _serverTsToMillis(updatedPart._serverTs);
    _stamped = true;
  }
  if (cTs < lTs) return _stamped ? 'stamped' : 'skipped'; // cloud copy older than local — protects an edit this device just made
  if (cTs === lTs) {
    // Echo of data we already hold — skipping it is what makes the full-collection
    // replay after every (re)subscribe nearly free. One exception: the cloud carries
    // a REAL photo URL while local still holds a placeholder — apply so the photo heals.
    const cloudReal = updatedPart.photoDataUrl && !(window.isPlaceholderPhoto && window.isPlaceholderPhoto(updatedPart.photoDataUrl));
    const localPlaceholder = window.isPlaceholderPhoto && window.isPlaceholderPhoto(local.photoDataUrl);
    if (!(cloudReal && localPlaceholder)) return _stamped ? 'stamped' : 'skipped';
  }
  const merged = Object.assign({}, updatedPart);
  merged._serverTs = _serverTsToMillis(updatedPart._serverTs) || local._serverTs;
  if (local.costPrice > 0 && !(updatedPart.costPrice > 0)) merged.costPrice = local.costPrice;
  if (local.instantPrice > 0 && !(updatedPart.instantPrice > 0)) merged.instantPrice = local.instantPrice;
  if (local.creditPrice > 0 && !(updatedPart.creditPrice > 0)) merged.creditPrice = local.creditPrice;
  // Don't let an incoming placeholder photo clobber a real local photo. If the JPG
  // upload is still pending, the local copy (base64, or a real https URL) is the only
  // intact image — keep it; the photo-upload queue will reconcile the cloud doc shortly.
  if (window.isPlaceholderPhoto && window.isPlaceholderPhoto(merged.photoDataUrl)
      && local.photoDataUrl && !window.isPlaceholderPhoto(local.photoDataUrl)) {
    merged.photoDataUrl = local.photoDataUrl;
  }
  state.parts[idx] = merged;
  return 'merged';
}

// Apply a WHOLE snapshot batch in one pass: one index build, one persist, one render.
// This is the mobile-hang fix — previously each of ~10,500 replayed docs did its own
// findIndex + DB.set + getElementById, freezing the main thread for seconds on every
// cold load, tab foreground, and reconnect.
window._applyPartsSnapshotBatch = function(changedParts, removedIds) {
  changedParts = changedParts || []; removedIds = removedIds || [];
  if (!changedParts.length && !removedIds.length) return;

  const idxMap = {};
  for (let i = 0; i < state.parts.length; i++) idxMap[state.parts[i].id] = i;

  let newCount = 0, mergedCount = 0, stampedCount = 0;
  const touchedIds = [];
  for (let i = 0; i < changedParts.length; i++) {
    const r = _applyPartChangeCore(changedParts[i], idxMap);
    if (r === 'new') newCount++;
    else if (r === 'merged') { mergedCount++; touchedIds.push(changedParts[i].id); }
    else if (r === 'stamped') stampedCount++; // _serverTs learned; content identical
  }

  // #3: Remote delete events from Firestore are authoritative — apply them
  // unconditionally so a delete on one device propagates to all others. The
  // owner-only guard belongs on the OUTBOUND delete (rxEnqueueChanged), not
  // here; gating the inbound event left "ghost parts" on every other device.
  let deletedCount = 0;
  if (removedIds.length) {
    const rm = new Set(removedIds);
    const before = state.parts.length;
    state.parts = state.parts.filter(function(p){ return !rm.has(p.id); });
    deletedCount = before - state.parts.length;
  }

  if (!newCount && !mergedCount && !deletedCount) {
    // Nothing visible changed. But if the echo taught us _serverTs for parts that
    // didn't have it, persist that — otherwise the flag is lost on reload and the
    // healer strands the same parts all over again. No render: pixels are identical.
    if (stampedCount) DB.set('parts', state.parts);
    return;
  }

  if (newCount || deletedCount) invalidateVelocityCache();
  DB.set('parts', state.parts); // ONE persist for the entire batch (was once per doc)
  if (window._markInbound) window._markInbound(); // remote change applied — keep pill time live

  // In-place card refresh only for a handful of merges (live edit from another device);
  // anything bigger goes through the debounced full re-render instead of thousands of
  // getElementById calls. Skipped when the array was reshaped (new/deleted) — idxMap
  // positions are only trusted while indexes are stable.
  if (mergedCount && mergedCount <= 20 && !newCount && !deletedCount) {
    for (let i = 0; i < touchedIds.length; i++) {
      const card = document.getElementById('pcard-' + touchedIds[i]);
      if (card) {
        const p = state.parts[idxMap[touchedIds[i]]];
        if (p) card.outerHTML = buildPartCard(p);
      }
    }
  }
  _scheduleListenerRender();
};

// Kept for the heartbeat delta path and any external callers — routes through the batch.
function _firestorePartChanged(updatedPart) {
  window._applyPartsSnapshotBatch([updatedPart], []);
}

function _firestorePartDeleted(deletedPartId) {
  if (!deletedPartId) return;
  window._applyPartsSnapshotBatch([], [deletedPartId]);
}

function _startPartsFirestoreListener() {
  if (!window.firestoreListenParts) return;
  // LEAK FIX: ALWAYS tear down the previous subscription before creating a new one.
  // Overwriting window._partsListener does NOT unsubscribe it — the old listener
  // stayed live forever, and the visibility/online/heartbeat restart race could
  // stack 3+ listeners, each replaying the entire ~10,500-doc collection on every
  // mobile tab-return (the "slows down over time" bug). Teardown here makes
  // duplicates structurally impossible regardless of which path calls this.
  if (window._partsListener) { try { window._partsListener(); } catch(e){} }
  window._partsListener = window.firestoreListenParts(_firestorePartChanged, _firestorePartDeleted);
  window._partsListenerActive = true;
  window._lastPartsSnapshotAt = Date.now();
}

// ── 2-MINUTE SYNC HEARTBEAT ──────────────────────────────────────────────
// The real-time onSnapshot listener is the primary, instant sync. This
// heartbeat is the safety net the owner asked for: every 2 minutes it
//   1) re-subscribes the listener if it has died (network blip, mobile
//      backgrounding, or an onSnapshot error), so "real-time" stays real, and
//   2) does a CHEAP delta pull of only the parts changed in the last few
//      minutes and applies them through the same field-preserving merge the
//      live listener uses — guaranteeing the device is never more than ~2
//      minutes stale, without re-reading the whole catalogue (which would
//      burn the Firestore read quota).
var _PARTS_SYNC_INTERVAL_MS = 120000; // 2 minutes
function _startPartsSyncHeartbeat() {
  if (window._partsSyncHeartbeat) return; // already running
  // First delta window starts now; the initial onSnapshot already loaded everything.
  window._lastPartsDeltaTs = Date.now();
  window._partsSyncHeartbeat = setInterval(async function() {
    if (!navigator.onLine || !window._firebaseAuthed) return;
    // (0) SILENT-DEATH FIX: the "active" flag is only cleared by the onSnapshot
    // ERROR callback, but mobile Firestore streams frequently die WITHOUT an error.
    // The flag then stays true forever and the restart check below never fires,
    // leaving the device stale until reload. We already record _lastPartsSnapshotAt
    // on every snapshot — if the listener claims active but hasn't produced a
    // snapshot in 10+ minutes while online, assume it silently died.
    if (window._partsListenerActive && Date.now() - (window._lastPartsSnapshotAt || 0) > 600000) {
      console.warn('[sync] heartbeat: listener claims active but no snapshot for 10+ min — marking dead');
      window._partsListenerActive = false;
    }
    // (1) Self-heal the live listener if it died.
    // (Manual teardown removed — _startPartsFirestoreListener unsubscribes the old one itself.)
    if (!window._partsListenerActive) {
      try {
        _startPartsFirestoreListener();
        console.log('[sync] heartbeat: real-time listener was down — restarted');
      } catch(e) { console.warn('[sync] heartbeat restart failed:', e && e.message); }
    }
    // (2) Cheap delta pull as a backup (clock-skew tolerant 5-min overlap).
    if (typeof window.firestorePullPartsSince === 'function') {
      var since = (window._lastPartsDeltaTs || Date.now()) - 300000; // 5-min overlap
      var changed = await window.firestorePullPartsSince(since);
      if (changed && changed.length) {
        try { window._applyPartsSnapshotBatch(changed, []); } catch(e){ console.warn('[sync] delta batch apply failed:', e && e.message); }
        console.log('[sync] heartbeat: pulled ' + changed.length + ' changed part(s) from cloud');
      }
      window._lastPartsDeltaTs = Date.now();
    }
    window._lastCloudSyncAt = Date.now();
  }, _PARTS_SYNC_INTERVAL_MS);
}

// FIREBASE STARTUP — pull cloud data, then start listening
// ============================================================
async function initFirebase() {
  // Load large arrays (parts/sales/orders/wholesale/activity/visualTraining) from
  // IndexedDB into memory — and, for users upgrading from the old localStorage
  // build, migrate them off the jammed 5 MB cap. Idempotent (safe if the standalone
  // boot hydrator already ran). Must happen before the cloud merge below so the
  // merge preserves local data, and before the first render.
  try {
    await DB.hydrateLargeKeys();
    DB_LARGE_KEYS.forEach(function(k){ if (Array.isArray(state[k]) || state[k] === undefined) state[k] = DB.get(k, []); });
    if (typeof invalidatePartsIndex === 'function') invalidatePartsIndex();
  } catch(e) { console.warn('[boot] large-key hydration failed:', e && e.message); }
  // Try pulling from cloud first
  const cloud = await window.firebasePull();
  const _allKeys=['parts','customers','sales','orders','staff','users','wholesaleOrders','salaryRecords','advanceRecords','staffScores','aiFeedback','attendanceRecords','mechanics','retailCustomers','collections','expenses','activityLog','settings','purchases','demandInbox','incomingStock','supplierAdvances','dayClosings','bankAccounts','manualPayables','suppliers','brandMatrix','visualTraining','saleReturns','purchaseOrders','locations','recycleBin','shopNotes'];
  // Filter deleted purchases from cloud data before merging
  if (cloud && Array.isArray(cloud.purchases) && _purchaseDeletedIds.size > 0) {
    cloud.purchases = cloud.purchases.filter(function(p){ return !p || !_purchaseDeletedIds.has(p.id); });
  }
  if(cloud){
    const _cTs = cloud._cloudTs || {};
    _allKeys.forEach(k=>{
      if(cloud[k]===undefined) return;
      // ── Timestamp-based merge: newer write wins ──
      // localTs: when we last wrote this key to localStorage on this device
      // cloudTs: when Firestore last received a push for this key
      const localTs = _keyLastSaved[k] || 0;
      const cloudTs = _cTs[k] || 0;
      const localIsNewer = localTs > cloudTs;
      const cloudIsNewer = cloudTs > localTs;

      if(Array.isArray(cloud[k])){
        let merged = mergeArrays(state[k], cloud[k], k);
        // Apply purchased deletions filter
        if (k === 'purchases' && _purchaseDeletedIds.size > 0) {
          merged = merged.filter(function(p){ return !p || !_purchaseDeletedIds.has(p.id); });
        }
        state[k]=merged; DB.set(k,merged);
        // Always push merged result to cloud so all devices converge
        // (only push if we actually changed something to avoid echo loops)
        if (window.firebasePush) {
          clearTimeout(window._fbSaveTimer);
          // Delay long enough for anonymous auth to complete before pushing merged state
          window._fbSaveTimer = setTimeout(function(){
            if (window._firebaseAuthed) {
              window.firebasePush(state).catch(function(e) {
                console.warn('[init] post-merge push failed:', e.message);
                if (typeof RX !== 'undefined') RX.log('warn','init','post-merge push: '+e.message);
              });
            } else {
              // Auth still pending — retry after additional wait
              setTimeout(function(){
                if (window.firebasePush && window._firebaseAuthed) window.firebasePush(state).catch(function(e){
                  console.warn('[init] post-merge push retry failed:', e.message);
                });
              }, 4000);
            }
          }, 1500);
        }
      } else {
        // Non-array (settings etc)
        if (k === 'settings') {
          // Always preserve locally-held secrets — they are intentionally stripped before
          // push, so the cloud copy never has them; merging cloud over local would wipe them
          const loc = state[k] || {};
          const keep = { jarvisAuth: loc.jarvisAuth }; // central-only: never carry local aiKey/aiVisionKey
          const merged = Object.assign({}, cloud[k], keep);
          merged.bsp = Object.assign({}, loc.bsp || {}, (cloud[k] && cloud[k].bsp) || {});
          if (loc.bsp && loc.bsp.key) merged.bsp.key = loc.bsp.key;
          if (cloudIsNewer) { state[k]=merged; DB.set(k,merged); }
          // else local is newer or same — keep local
        } else {
          if (cloudIsNewer) { state[k]=cloud[k]; DB.set(k,cloud[k]); }
        }
      }
    });
  }
  // If app crashed mid-push last session, parts in localStorage are newer than Firestore
  // Re-push immediately to close the gap before any other device can pull stale data
  try {
    if (localStorage.getItem('_partsPushPending') === '1' && window.firebasePush) {
      console.log('[boot] _partsPushPending flag set — re-pushing parts to Firestore');
      window.firebasePush(state).then(function() {
        try { localStorage.removeItem('_partsPushPending'); } catch(e) { console.warn('[boot] could not clear _partsPushPending:', e.message); }
      }).catch(function(e) {
        console.warn('[boot] re-push after crash failed, will retry next boot:', e.message);
        if (typeof RX !== 'undefined') RX.log('warn','boot','crash-recovery push failed: '+e.message);
      });
    }
  } catch(e) { console.warn('[boot] _partsPushPending check failed:', e.message); }
  // Only seed demo data if BOTH local AND cloud are empty
  if(state.parts.length===0 && (!cloud || !cloud.parts || cloud.parts.length===0)) seedSampleDataIfEmpty();
  // Run integrity checks — reconciles totalDue, clamps negative stock, back-fills supplier records
  try { runIntegrityChecks(); } catch(e) { console.warn('Integrity check failed:', e); }
  renderHome(); renderInventory();
  // Reliability: restore in-progress cart, take auto-snapshot, kick the outbox
  try { await rxRestoreDraft(); } catch(e) { console.warn('[boot] rxRestoreDraft failed:', e.message); }
  try { await rxAutoSnapshot(); } catch(e) { console.warn('[boot] rxAutoSnapshot failed:', e.message); }
  try { RX.checkHalt(); RX.drain(); drainPartsCloudPending(); renderRxStatusBar(); } catch(e) { console.warn('[boot] RX init failed:', e.message); }
  setInterval(() => { try { rxAutoSnapshot(); } catch(e) { console.warn('[snapshot] hourly auto-snapshot failed:', e.message); } }, 60 * 60 * 1000); // hourly check
  // Migrate any legacy base64 part photos to Firebase Storage
  setTimeout(function(){ try { migratePartPhotosToStorage(); } catch(e){} }, 4000);
  // Migrate parts from blob storage to individual Firestore docs (one-time, safe to re-run)
  // After this, every part lives at parts/<id> — concurrent edits never conflict
  setTimeout(function(){ try { window.migratePartsToDocuments && window.migratePartsToDocuments(); } catch(e){ console.warn('Parts migration failed:', e); } }, 6000);
  // Pre-warm velocity cache so first inventory render is instant
  setTimeout(function(){ try { _buildVelocityCache(); } catch(e){} }, 500);
  // Start real-time listener on individual parts documents
  // Any device changing a part triggers instant update on all other devices
  setTimeout(function(){
    try {
      _startPartsFirestoreListener();
      console.log('[sync] real-time parts listener active');
      try { _startPartsSyncHeartbeat(); console.log('[sync] 2-minute sync heartbeat active'); } catch(e) { console.warn('[sync] heartbeat start failed:', e && e.message); }
      // Start the inbound attendance/salary sync listener too
      try { startStaffSyncListener(); console.log('[sync] staffSync listener active'); } catch(e) { console.warn('[sync] staffSync listener failed:', e.message); }
      if (!window._partsVisibilityBound) {
        window._partsVisibilityBound = true;
        document.addEventListener('visibilitychange', function() {
          if (document.visibilityState === 'visible' && !window._partsListenerActive) {
            console.log('[sync] visibility restored — restarting parts listener');
            // Manual teardown removed — _startPartsFirestoreListener now unsubscribes
            // the old listener itself, so this path can't leave a dangling subscription.
            setTimeout(function(){
              // RACE FIX: re-check at fire time. The visibility, online, and heartbeat
              // paths can all schedule restarts in the same window; without this check
              // each timeout blindly created another listener (the duplicate-listener leak).
              if (window._partsListenerActive) return;
              try { _startPartsFirestoreListener(); } catch(e) { console.warn('[sync] listener restart failed:', e.message); }
            }, 1000);
          }
        });
        // Also restart listener when network comes back online (covers Firestore-level disconnects)
        window.addEventListener('online', function() {
          if (!window._partsListenerActive) {
            console.log('[sync] network online — restarting parts listener');
            setTimeout(function(){
              if (window._partsListenerActive) return; // another path already restarted it
              try { _startPartsFirestoreListener(); } catch(e) { console.warn('[sync] online restart failed:', e.message); }
            }, 1500);
          }
        });
      }
    } catch(e) { console.warn('Parts listener failed:', e); }
  }, 3000);
  // Migrate legacy standalone visual training localStorage → state (one-time)
  setTimeout(function(){
    try {
      const legacyKey = 'goury_visual_training';
      const raw = localStorage.getItem(legacyKey);
      if (!raw) return;
      const legacy = JSON.parse(raw);
      if (!Array.isArray(legacy) || legacy.length === 0) return;
      const existing = state.visualTraining || [];
      const existingIds = new Set(existing.map(r => r.id));
      const newRecords = legacy.filter(r => r.id && !existingIds.has(r.id));
      if (newRecords.length > 0) {
        state.visualTraining = [...existing, ...newRecords];
        save();
        console.log('[Migration] Moved', newRecords.length, 'visual training records from localStorage to state');
      }
      localStorage.removeItem(legacyKey);
    } catch(e) { console.warn('Visual training migration failed:', e); }
  }, 2000);
  window.firebaseListenAll((key, val, cloudTs) => {
    if (JSON.stringify(val) === JSON.stringify(state[key])) return; // no change
    if (window._markInbound) window._markInbound(); // real remote change received — keep pill time live
    const localTs = _keyLastSaved[key] || 0;
    // If local was saved more recently than this cloud snapshot, local wins — push to cloud
    if (localTs > (cloudTs || 0)) {
      if (window.firebasePush) {
        clearTimeout(window._fbSaveTimer);
        window.firebasePush(state);
      }
      return;
    }
    // Echo guard: if we saved within the last 10s, this is our own echo — ignore
    if (window._lastLocalSave && (Date.now() - window._lastLocalSave) < 10000) {
      return;
    }
    // Owner delete in progress — allow array to shrink
    const lL = Array.isArray(state[key]) ? state[key].length : 0;
    const cL = Array.isArray(val) ? val.length : 0;
    // Never let cloud shrink unless it's a deliberate owner delete
    if (Array.isArray(val) && cL < lL && !window._ownerDeleteInProgress) {
      if (window.firebasePush) window.firebasePush(state);
      return;
    }
    // All arrays: use universal field-level merge so concurrent edits on different devices
    // never wipe each other — your father editing prices + you adding parts = both survive
    if (Array.isArray(val)) {
      let merged = mergeArrays(state[key], val, key);
      if (key === 'purchases' && _purchaseDeletedIds.size > 0) {
        merged = merged.filter(function(p){ return !p || !_purchaseDeletedIds.has(p.id); });
      }
      state[key] = merged;
      DB.set(key, merged);
      // Push merged result so cloud converges (debounced to avoid echo loops)
      clearTimeout(window._fbSnapMergeTimer);
      window._fbSnapMergeTimer = setTimeout(function(){
        if (window.firebasePush) window.firebasePush(state);
      }, 2000);
      const activeTab = document.querySelector('.tab-content.active');
      const tabId = activeTab ? activeTab.id.replace('tab-','') : '';
      if (key === 'parts') {
        if (tabId === 'catalogue') { try { catRenderDadQueue && catRenderDadQueue(); catRenderProgress && catRenderProgress(); } catch(e){} }
        else if (tabId === 'home') renderHome();
        else if (tabId === 'inventory') renderInventory();
      } else {
        if (tabId === 'home') renderHome();
        else if (tabId === 'inventory') renderInventory();
      }
      return;
    }
    // Never let cloud overwrite locally-held secrets — they are intentionally stripped
    // before push, so the cloud doc never carries them
    if (key === 'settings' && state.settings) {
      const loc = state.settings, keep = {};
      if (loc.jarvisAuth) keep.jarvisAuth = loc.jarvisAuth;
      val = Object.assign({}, val, keep);
      if (loc.bsp && loc.bsp.key) {
        val.bsp = Object.assign({}, loc.bsp, val.bsp || {});
        val.bsp.key = loc.bsp.key;
      }
    }
    // Restore bill photos: if Firestore has __uploading__ placeholder but local already
    // has the real https:// Storage URL or base64, keep the local version
    if (key === 'purchases' && Array.isArray(val)) {
      // First: strip any purchases that were explicitly deleted on this device
      if (_purchaseDeletedIds.size > 0) {
        val = val.filter(function(p){ return !p || !_purchaseDeletedIds.has(p.id); });
      }
      const localById = {};
      (state.purchases || []).forEach(function(p){ if(p && p.id) localById[p.id] = p; });
      val = val.map(function(p) {
        if (!p || !p.id) return p;
        const local = localById[p.id];
        if (!local) return p;
        // Cloud has placeholder — restore from local (either base64 still uploading, or https:// already done)
        if (!p.photoDataUrl || p.photoDataUrl === '__uploading__' || p.photoDataUrl === '__local_only__') {
          if (local.photoDataUrl && (local.photoDataUrl.startsWith('data:') || local.photoDataUrl.startsWith('https://'))) {
            return Object.assign({}, p, { photoDataUrl: local.photoDataUrl });
          }
        }
        return p;
      });
    }
    state[key] = val;
    DB.set(key, val);
    const activeTab = document.querySelector('.tab-content.active');
    const tabId = activeTab ? activeTab.id.replace('tab-','') : '';
    if (tabId === 'home') renderHome();
    else if (tabId === 'inventory') renderInventory();
  });
}

// ============================================================
// INIT
// ============================================================
syncNextInvoice(); // self-heal invoice counter before anything else runs
initLogin();
// Hydrate large arrays from IndexedDB BEFORE the app settles — and migrate any
// upgrading user off the old localStorage cap. Runs regardless of network/Firebase
// so OFFLINE reloads restore the full local catalogue too (initFirebase may never
// fire when offline). DB.hydrateLargeKeys is idempotent, so initFirebase calling it
// again is a no-op.
(async function _bootHydrateLargeKeys(){
  try {
    await DB.hydrateLargeKeys();
    DB_LARGE_KEYS.forEach(function(k){ if (Array.isArray(state[k]) || state[k] === undefined) state[k] = DB.get(k, []); });
    if (typeof invalidatePartsIndex === 'function') invalidatePartsIndex();
    try { renderHome && renderHome(); } catch(_){}
    try { var at = document.querySelector('.tab-content.active'); if (at && at.id === 'tab-inventory') renderInventory && renderInventory(); } catch(_){}
  } catch(e) { console.warn('[boot] standalone hydrate failed:', e && e.message); }
})();
if(window._firebaseReady){initFirebase();}else{document.addEventListener('firebaseReady',()=>initFirebase());}

// ============================================================
// CONTINUOUS SAVE — flush on tab-hide, page-close, and interval
// ============================================================
// Problem: save() debounces the Firebase push by 1.2s. If the tab is
// closed or backgrounded before that timer fires, cloud never gets the
// latest state. The RX outbox covers transactional records, but we also
// want the full-state snapshot to be flushed immediately.
function _flushPending() {
  // Cancel the debounce and push to Firebase right now
  if (window._fbSaveTimer) {
    clearTimeout(window._fbSaveTimer);
    window._fbSaveTimer = null;
    try { if (window.firebasePush && window._firebaseAuthed) window.firebasePush(state).catch(function(e){ console.warn('[interval] heartbeat push failed:', e.message); }); } catch(e) { console.warn('[interval] firebasePush call error:', e.message); }
  }
  // Make sure the IDB outbox has the latest per-record changes
  try { rxEnqueueChanged(); } catch(e) { console.warn('[flush] rxEnqueueChanged failed:', e.message); if (typeof RX !== 'undefined') RX.log('warn','flush','rxEnqueueChanged: '+e.message); }
  // Re-persist cart draft so crash recovery has the latest cart state
  try {
    if (window.RX) RX.saveDraft('cart', {
      cart: state.cart, payMode: state.payMode,
      woCart: state.woCart, quoteCart: state.quoteCart, ts: Date.now()
    });
  } catch(e) { console.warn('[flush] saveDraft failed:', e.message); }
}

// 1. User switches tab or backgrounds the app (critical on mobile)
document.addEventListener('visibilitychange', () => { if (document.hidden) _flushPending(); });

// 2. Page is being unloaded (refresh, close, navigate away)
window.addEventListener('pagehide', _flushPending);
window.addEventListener('beforeunload', _flushPending);

// 3. Periodic flush every 30s — only pushes to Firebase if there is a pending timer.
//    Does NOT call save() (which would reset _fbSaveTimer and delay the push).
//    save() is called immediately whenever data changes; this just ensures nothing
//    is stranded in the 1.2s debounce window for more than 30s.
setInterval(() => {
  try {
    if (window._fbSaveTimer) {
      clearTimeout(window._fbSaveTimer);
      window._fbSaveTimer = null;
      if (window.firebasePush) window.firebasePush(state);
    }
    try { rxEnqueueChanged(); } catch(e) { if (typeof RX !== 'undefined') RX.log('warn','interval','rxEnqueueChanged: '+e.message); }
  } catch(e) { if (typeof RX !== 'undefined') RX.log('warn','interval','30s flush: '+e.message); }
}, 30 * 1000);
