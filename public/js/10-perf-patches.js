(function() {
  'use strict';

  // ─────────────────────────────────────────────────────────────────
  // PATCH 1 — Async DB.set for large arrays  [BIGGEST WIN]
  //
  // localStorage.setItem() is synchronous and blocks the main thread.
  // JSON.stringify(state.parts) on 10,500 parts is ~5-8MB of text.
  // Every save() froze the UI 200-600ms. Fix: defer large-array writes
  // 60ms and collapse rapid repeats into one write. Data is already in
  // memory + Firestore, so the localStorage copy lagging is harmless.
  // ─────────────────────────────────────────────────────────────────
  var _DB_WRITE_TIMERS = {};
  var _origDBset = DB.set.bind(DB);
  DB.set = function patchedDBset(k, v, preStr) {
    if (!Array.isArray(v) || v.length <= 100) return _origDBset(k, v, preStr);
    clearTimeout(_DB_WRITE_TIMERS[k]);
    _DB_WRITE_TIMERS[k] = setTimeout(function() {
      delete _DB_WRITE_TIMERS[k];
      try { _origDBset(k, v, preStr); } catch(e) { console.warn('[DB.set-deferred]', k, e.message); }
    }, 60);
  };
  // Flush any pending writes before the tab closes — never lose data
  window.addEventListener('beforeunload', function() {
    Object.keys(_DB_WRITE_TIMERS).forEach(function(k) {
      clearTimeout(_DB_WRITE_TIMERS[k]);
      try { _origDBset(k, state[k]); } catch(e) {}
    });
  });

  // ─────────────────────────────────────────────────────────────────
  // PATCH 2 — Scoped cloud push: stop re-uploading sales/orders on a
  //           parts-only edit.  [SAFE — only skips redundant work]
  //
  // In per-doc parts mode, every save() schedules a debounced
  // firebasePush(state) that re-serializes + re-uploads ALL large
  // arrays (sales, orders, wholesaleOrders, activityLog) — even when
  // only a price or stock number changed. That's slow and wastes
  // Firestore write quota. Parts themselves already wrote via
  // cloudPersistPart() -> parts/{id}.
  //
  // Strategy (safe): track the set of keys touched since the last
  // bulk push by wrapping markChanged(). When firebasePush fires and
  // EVERY touched key is parts/suppliers (and per-doc is active),
  // skip the bulk upload. The moment a sale/order/payment changes any
  // other key, the set contains it and the FULL push runs as before.
  //
  // completeSale() and other transaction paths do NOT use markChanged
  // (they use the full-scan save path), so _sawFullScanSave flips true
  // and their pushes are NEVER skipped — defaulting to "push all".
  // ─────────────────────────────────────────────────────────────────
  var _perDocActive = function() {
    try { return localStorage.getItem('_partsDocMigrated') === '2'; } catch(e) { return false; }
  };
  var _touchedKeys = new Set();
  var _sawFullScanSave = false;

  // markChanged and save are defined in a regular classic script (already
  // present by the time this runs). firebasePush is defined in a module-type
  // script which executes asynchronously AFTER this patch — so we wait for it.
  var _origMarkChanged = window.markChanged;
  if (typeof _origMarkChanged === 'function') {
    window.markChanged = function(key) {
      _touchedKeys.add(key);
      return _origMarkChanged(key);
    };
  }

  var _origSave = window.save;
  if (typeof _origSave === 'function') {
    window.save = function patchedSave() {
      if (_touchedKeys.size === 0) _sawFullScanSave = true;
      return _origSave.apply(this, arguments);
    };
  }

  // Wait for the async module to define window.firebasePush, then wrap it.
  // Without this guard the wrapper would capture `undefined` because the
  // Firebase module (type=module) runs after this classic script.
  function _installFirebasePushWrap() {
    var _origFirebasePush = window.firebasePush;
    if (typeof _origFirebasePush !== 'function') {
      return false; // not ready yet
    }
    if (_origFirebasePush.__gouryWrapped) return true; // already wrapped
    var wrapped = function patchedFirebasePush(stateObj) {
      var canSkip = _perDocActive()
        && !_sawFullScanSave
        && _touchedKeys.size > 0
        && Array.from(_touchedKeys).every(function(k){ return k === 'parts' || k === 'suppliers'; });
      _touchedKeys.clear();
      _sawFullScanSave = false;
      if (canSkip) {
        // Parts already synced via cloudPersistPart; suppliers via its own
        // per-record outbox (_RX_TRACKED). Nothing else changed — skip bulk.
        return Promise.resolve();
      }
      return _origFirebasePush.call(this, stateObj);
    };
    wrapped.__gouryWrapped = true;
    window.firebasePush = wrapped;
    console.log('[GOURY PATCH v3] firebasePush scoped-push wrapper installed');
    return true;
  }
  if (!_installFirebasePushWrap()) {
    // Poll up to ~10s for the module to finish loading
    var _fpTries = 0;
    var _fpPoll = setInterval(function() {
      _fpTries++;
      if (_installFirebasePushWrap() || _fpTries > 100) clearInterval(_fpPoll);
    }, 100);
  }

  // ─────────────────────────────────────────────────────────────────
  // PATCH 3 — xlsRender visibility guard
  // Was rebuilding a 200-row table after every savePart even when the
  // SKU Editor tab was closed. Skip when its container isn't visible.
  // ─────────────────────────────────────────────────────────────────
  var _xlsRenderOrig = window.xlsRender;
  window.xlsRender = function xlsRenderGuarded() {
    var tbody = document.getElementById('xls-tbody');
    if (!tbody || !tbody.offsetParent) return; // offsetParent null = hidden
    if (typeof _xlsRenderOrig === 'function') _xlsRenderOrig();
  };

  // ─────────────────────────────────────────────────────────────────
  // PATCH 4 — patchInventoryCard: fast prepend for new parts
  // New-part adds were forcing a full 10,500-card re-render. Prepend
  // the single new card instead (new parts sort to top anyway).
  // ─────────────────────────────────────────────────────────────────
  var _patchInventoryCardOrig = window.patchInventoryCard;
  window.patchInventoryCard = function patchInventoryCardFast(part, forceFullRender) {
    if (!forceFullRender) return _patchInventoryCardOrig(part, false);
    var list = document.getElementById('inv-list');
    if (!list || !list.offsetParent) return; // not visible — render later on tab open
    var empty = list.querySelector('.empty');
    if (empty) empty.remove();
    try {
      var tempDiv = document.createElement('div');
      tempDiv.innerHTML = buildPartCard(part);
      var newCard = tempDiv.firstElementChild;
      if (newCard) {
        list.insertBefore(newCard, list.firstChild);
        if (typeof _resolvePartPhotos === 'function') _resolvePartPhotos(list, [part]);
        var countEl = document.getElementById('inv-count');
        if (countEl && typeof _invFiltered !== 'undefined' && _invFiltered) {
          countEl.textContent = (_invFiltered.length + 1) + ' parts';
        }
      }
    } catch(e) {
      console.warn('[patchInventoryCard-patch] fallback to full render:', e.message);
      if (typeof renderInventory === 'function') renderInventory();
    }
  };

  // ─────────────────────────────────────────────────────────────────
  // PATCH 5 — renderSaleSearch: debounce + use precomputed index
  //
  // The billing search ran state.parts.filter(saleablePart) on EVERY
  // keystroke. saleablePart() is a function call per part — 10,500
  // calls per keypress. getPartsIndex().saleable is already a cached
  // precomputed array. Use it, and debounce 90ms.
  // ─────────────────────────────────────────────────────────────────
  var _saleSearchTimer = null;
  var _renderSaleSearchOrig = window.renderSaleSearch;
  window.renderSaleSearch = function renderSaleSearchFast() {
    clearTimeout(_saleSearchTimer);
    _saleSearchTimer = setTimeout(function() {
      var input = document.getElementById('sale-search');
      var div = document.getElementById('sale-search-results');
      if (!input || !div) return;
      var q = (input.value || '').toLowerCase().trim();
      if (!q) { div.innerHTML = ''; return; }

      // Use precomputed saleable index instead of filtering all parts
      var pool = (typeof getPartsIndex === 'function')
        ? getPartsIndex().saleable
        : state.parts.filter(function(p){ return saleablePart(p); });

      var results = [];
      for (var i = 0; i < pool.length && results.length < 8; i++) {
        var p = pool[i];
        if (p.partNo.toLowerCase().indexOf(q) !== -1 ||
            p.name.toLowerCase().indexOf(q) !== -1 ||
            (p.brand || '').toLowerCase().indexOf(q) !== -1) {
          results.push(p);
        }
      }

      if (!results.length) {
        div.innerHTML = '<div class="empty" style="padding:16px;">No parts found</div>';
        return;
      }
      if (typeof _renderSaleResults === 'function') {
        _renderSaleResults(div, results);
      }
      // Hindi translation pass (unchanged behaviour)
      if (typeof currentLang !== 'undefined' && currentLang === 'hi' &&
          typeof getAIKey === 'function' && getAIKey() && getAIKey().startsWith('sk-') &&
          typeof _translatePartNames === 'function') {
        var need = results.filter(function(p){ return !_hindiNameCache[p.id]; });
        if (need.length && !window._hindiTranslating) {
          _translatePartNames(need).then(function() {
            if (document.getElementById('sale-search-results') === div) _renderSaleResults(div, results);
          });
        }
      }
    }, 90);
  };

  // ─────────────────────────────────────────────────────────────────
  // PATCH 6 — mergeArrays: timestamp-correct parts merge  [SYNC FIX]
  //
  // Original always let the LOCAL price win if local price > 0, even
  // when the cloud copy was NEWER. Scenario: dad updates a price on
  // his phone -> you open your app (stale local cache) -> your old
  // price overwrites his new one on next merge. Silent data loss.
  //
  // Fix: for parts, the side with the newer updatedAt wins each
  // pricing field. Non-destructive union for everything else stays.
  // ─────────────────────────────────────────────────────────────────
  var _origMergeArrays = window.mergeArrays;
  window.mergeArrays = function mergeArraysFixed(local, cloud, key) {
    if (key !== 'parts') {
      // Non-parts arrays: original logic is fine
      return _origMergeArrays(local, cloud, key);
    }
    var localById = {}, cloudById = {};
    (local || []).forEach(function(r){ if (r && r.id) localById[r.id] = r; });
    (cloud || []).forEach(function(r){ if (r && r.id) cloudById[r.id] = r; });
    var allIds = {};
    Object.keys(localById).forEach(function(id){ allIds[id] = 1; });
    Object.keys(cloudById).forEach(function(id){ allIds[id] = 1; });

    var merged = [];
    Object.keys(allIds).forEach(function(id) {
      var l = localById[id], c = cloudById[id];
      if (!l) { merged.push(c); return; }
      if (!c) { merged.push(l); return; }

      var lTs = l.updatedAt || l.addedAt || 0;
      var cTs = c.updatedAt || c.addedAt || 0;
      // Whichever record is newer becomes the base / winner of conflicting fields
      var newer = (cTs > lTs) ? c : l;
      var older = (cTs > lTs) ? l : c;
      // Start from older, overlay newer — newer wins every populated field
      var result = Object.assign({}, older, newer);

      // For pricing fields: prefer the NEWER side's value if it's > 0,
      // otherwise fall back to whichever side has a non-zero value
      ['costPrice','instantPrice','creditPrice','mrp'].forEach(function(f) {
        if (newer[f] > 0)      result[f] = newer[f];
        else if (older[f] > 0) result[f] = older[f];
      });
      // Stock: newer timestamp wins (matches atomic-decrement intent)
      result.stock = (newer.stock !== undefined) ? newer.stock : older.stock;
      // Location: keep any non-empty, prefer newer
      result.location = newer.location || older.location || '';
      // Photo: prefer an https:// (uploaded) URL from either side
      if (newer.photoDataUrl && newer.photoDataUrl.indexOf('https://') === 0) result.photoDataUrl = newer.photoDataUrl;
      else if (older.photoDataUrl && older.photoDataUrl.indexOf('https://') === 0) result.photoDataUrl = older.photoDataUrl;
      // Firestore rejects `undefined` field values. Math.max(0,0) is falsy, so the
      // old `|| undefined` here poisoned any part with no timestamp on either side —
      // it would throw "Unsupported field value: undefined" on every write forever.
      result.updatedAt = Math.max(lTs, cTs) || Date.now();
      merged.push(result);
    });
    return merged;
  };

  // ─────────────────────────────────────────────────────────────────
  // PATCH 7 — catDadSave: per-doc Firestore write (pricing sync fix)
  // Original used save()+firebasePush which skips Firestore in per-doc
  // mode, so prices never reached the cloud and got echo-overwritten.
  // ─────────────────────────────────────────────────────────────────
  window.catDadSave = async function catDadSave(partId, andNext) {
    var costEl   = document.getElementById('cdcost-'   + partId);
    var retailEl = document.getElementById('cdretail-' + partId);
    var cost   = costEl   ? (parseFloat(costEl.value)   || 0) : 0;
    var retail = retailEl ? (parseFloat(retailEl.value) || 0) : 0;
    if (!cost || !retail) {
      typeof toast === 'function' && toast('\u26a0\ufe0f Cost and retail price required');
      return;
    }
    var idx = state.parts.findIndex(function(x) { return x.id === partId; });
    if (idx < 0) return;

    var p = Object.assign({}, state.parts[idx]);
    p.costPrice     = cost;
    p.instantPrice  = retail;
    p.mrp           = +(document.getElementById('cdmrp-'   + partId) || {}).value || p.mrp || 0;
    p.creditPrice   = +(document.getElementById('cdws-'    + partId) || {}).value || p.creditPrice || 0;
    p.minStock      = +(document.getElementById('cdmin-'   + partId) || {}).value || p.minStock || 0;
    p.supplierName  = ((document.getElementById('cdsup-'   + partId) || {}).value || '').trim();
    p.supplierPhone = ((document.getElementById('cdphone-' + partId) || {}).value || '').trim();
    p.updatedAt     = Date.now();

    var cardEl = document.getElementById('cqi-' + partId);
    if (cardEl) cardEl.remove();

    state.parts[idx] = p;
    window._lastLocalSave = Date.now();

    if (typeof markChanged === 'function') markChanged('parts');
    if (typeof save === 'function') save();

    var cloudResult = { ok: false };
    try {
      if (typeof cloudPersistPartFields === 'function') {
        // Owner stream writes ONLY pricing/supplier/minStock — the mirror of the
        // staff-stream fix. Pushing the whole part here could overwrite staff's
        // stock/name/photo in the cloud with a stale local copy.
        var _ownerPatch = {
          costPrice: p.costPrice, instantPrice: p.instantPrice,
          creditPrice: p.creditPrice, minStock: p.minStock
        };
        // MRP is staff-owned (read off the box). The owner sets only cost/
        // retail/wholesale, so the owner stream never writes mrp — that removes
        // the last field both streams could fight over.
        // Don't blank out an existing cloud supplier with an empty field.
        if (p.supplierName)  _ownerPatch.supplierName  = p.supplierName;
        if (p.supplierPhone) _ownerPatch.supplierPhone = p.supplierPhone;
        cloudResult = await cloudPersistPartFields(p.id, _ownerPatch);
      } else if (typeof cloudPersistPart === 'function') {
        cloudResult = await cloudPersistPart(p);
      } else if (window.firebasePush) {
        window.firebasePush(state);
        cloudResult = { ok: true, cloud: true };
      }
    } catch (e) { console.warn('[catDadSave] cloud error:', e.message); }

    if (typeof ensureSupplierRegistered === 'function') ensureSupplierRegistered(p.supplierName, p.supplierPhone);
    if (typeof invalidatePartsIndex === 'function') invalidatePartsIndex(); // saleable set changed

    if (typeof catRenderProgress === 'function') catRenderProgress();
    try { typeof patchInventoryCard === 'function' && patchInventoryCard(p, false); } catch(e) {}
    try { typeof xlsRender === 'function' && xlsRender(); } catch(e) {}
    try { typeof renderHomeDebounced === 'function' && renderHomeDebounced(); } catch(e) {}
    try { if (typeof logActivity === 'function') logActivity('Pricing filled: ' + p.partNo + ' "' + p.name + '"', 'info'); } catch(e) {}

    var stats = (typeof catCalcProgress === 'function') ? catCalcProgress() : { complete: '?', total: '?' };
    var cloudNote = cloudResult.cloud ? ' \u00b7 \u2601\ufe0f synced' : (cloudResult.queued ? ' \u00b7 queued' : '');
    typeof toast === 'function' && toast('\u2705 ' + p.name + ' saved' + cloudNote + ' \u00b7 ' + stats.complete + '/' + stats.total + ' done');

    var list = document.getElementById('cat-dad-list');
    if (list && !list.querySelector('.cat-qitem')) {
      list.innerHTML = '<div class="card" style="text-align:center;padding:24px;color:var(--muted);">\uD83C\uDF89 All parts have pricing filled in!</div>';
    }
    if (typeof catRenderProgressDebounced === 'function') catRenderProgressDebounced();

    if (andNext) {
      setTimeout(function() {
        var nextCard = document.querySelector('#cat-dad-list .cat-qitem');
        if (nextCard) {
          nextCard.scrollIntoView({ behavior: 'smooth', block: 'start' });
          var firstInput = nextCard.querySelector('input[type="number"]');
          if (firstInput) firstInput.focus();
        }
      }, 80);
    }
  };

  // ─────────────────────────────────────────────────────────────────
  // PATCH 8 — catDadAuto: debounce keystroke margin calculator
  // ─────────────────────────────────────────────────────────────────
  var _dadAutoTimers = {};
  var _catDadAutoOrig = window.catDadAuto;
  window.catDadAuto = function catDadAutoDebounced(partId) {
    clearTimeout(_dadAutoTimers[partId]);
    _dadAutoTimers[partId] = setTimeout(function() {
      delete _dadAutoTimers[partId];
      if (typeof _catDadAutoOrig === 'function') _catDadAutoOrig(partId);
    }, 80);
  };

  // ─────────────────────────────────────────────────────────────────
  // PATCH 9 — catRenderDadQueue: debounce rapid rebuilds
  // ─────────────────────────────────────────────────────────────────
  var _dadQueueTimer = null;
  var _dadQueueBase = window.catRenderDadQueue;
  window.catRenderDadQueue = function catRenderDadQueuePatched() {
    clearTimeout(_dadQueueTimer);
    _dadQueueTimer = setTimeout(function() { _dadQueueBase(); }, 120);
  };

  console.log('[GOURY PATCH v3] 9 fixes active: async DB.set, scoped push, xls guard, fast insert, sale-search index, merge-fix, catDadSave cloud, debounces \u2713');

})();
