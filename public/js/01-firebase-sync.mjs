  // ============================================================
  // ShopAI / Goury ERP — indexV32-sync
  // Changes from V30 (6 new features):
  //   1. Returns / Credit Notes — openReturnModal, processReturn, showReturnReceipt
  //   2. Price History Log — recordPriceHistory, renderPriceHistory, openPriceHistoryModal
  //   3. Granular Roles — accounts + readonly roles, isReadOnly() guard
  //   4. Purchase Orders — full PO flow: draft→sent→received, WhatsApp send, stock auto-update
  //   5. Multi-Location — branches/godowns, openManageLocations, openStockTransferModal
  //   6. Audit Trail — 📈 button in XLS editor, price history on savePart and xlsSaveRow
  // ============================================================
  import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.2/firebase-app.js";
  // Shop data now lives in Convex. convex-firestore.mjs speaks the same API as Firebase,
  // so the sync code below is unchanged. (staffSync + jarvis_inbox still go to Firebase.)
  import { getFirestore, initializeFirestore, persistentLocalCache, persistentMultipleTabManager, doc, setDoc, updateDoc, getDoc, getDocFromServer, getDocs, deleteDoc, onSnapshot, collection, writeBatch, serverTimestamp, increment as fsIncrement, query, where, orderBy } from "./convex-firestore.mjs";
  import { getAuth, signInAnonymously, onAuthStateChanged } from "./convex-firestore.mjs";
  import { getStorage, ref as storageRef, uploadBytes, getDownloadURL, deleteObject } from "./convex-firestore.mjs";

  const firebaseConfig = {
    apiKey: "AIzaSyCpRHhfAA7xiawuhKHR4tJmqxe8_1pa1Tk",
    authDomain: "goury-spare-parts.firebaseapp.com",
    projectId: "goury-spare-parts",
    storageBucket: "goury-spare-parts.firebasestorage.app",
    messagingSenderId: "1018605527254",
    appId: "1:1018605527254:web:4201b4045c26508578a977"
  };

  const app = initializeApp(firebaseConfig);
  window._firebaseProjectId = firebaseConfig.projectId;
  // Offline persistence: serve reads from a local IndexedDB cache and only fetch
  // deltas from the server. Turns cold start from "download all parts" into
  // "instant from cache + catch-up", and cuts Firestore read volume dramatically.
  // Falls back to the default in-memory cache if the browser can't enable it.
  let db;
  try {
    db = initializeFirestore(app, {
      localCache: persistentLocalCache({ tabManager: persistentMultipleTabManager() })
    });
  } catch (e) {
    console.warn('[firestore] persistent cache unavailable, using default:', e && e.message);
    db = getFirestore(app);
  }
  const auth = getAuth(app);
  const storage = getStorage(app);
  window._firebaseAuthed = false;
  signInAnonymously(auth).catch(e => console.warn("Anon auth failed:", e.message));
  onAuthStateChanged(auth, user => {
    window._firebaseAuthed = !!user;
    // Once signed in, recover any part photos that are stuck on a placeholder string
    // (the real image lives in Storage but the cloud record never got the https URL).
    if (user) {
      setTimeout(function(){ try { if (window.drainPhotoUploadQueue) window.drainPhotoUploadQueue(); } catch(e){} try { if (window.healPlaceholderPartPhotos) window.healPlaceholderPartPhotos(); } catch(e){} }, 4000);
    }
  });

  // Central AI key Firestore helpers — separate doc, never stripped by normal sync
  window._firestoreSetCentralKeys = async function(payload) {
    await setDoc(doc(db, 'shopdata', 'centralKeys'), payload);
  };
  window._firestoreGetCentralKeys = async function() {
    const snap = await getDoc(doc(db, 'shopdata', 'centralKeys'));
    return snap.exists() ? snap.data() : null;
  };
  // Force a read straight from the SERVER (bypasses offline cache) so we can
  // confirm a central-key write actually landed in the cloud, not just locally.
  window._firestoreGetCentralKeysFromServer = async function() {
    const snap = await getDocFromServer(doc(db, 'shopdata', 'centralKeys'));
    return snap.exists() ? snap.data() : null;
  };
  // Pull central key blob on auth (ciphertext only — no decryption without PIN)
  onAuthStateChanged(auth, async user => {
    if (user) {
      try {
        const blob = await window._firestoreGetCentralKeys();
        if (blob && blob.ct) {
          window._centralKeyBlob = blob;
          // CENTRAL-ONLY hygiene: a central key exists, so any leftover per-device
          // local key must be purged — it can never be used again and only causes
          // the stale-key confusion this mode was built to eliminate.
          try {
            if (window.state && state.settings && (state.settings.aiKey || state.settings.aiVisionKey)) {
              delete state.settings.aiKey; delete state.settings.aiVisionKey;
              if (window.DB) DB.set('settings', state.settings);
            }
          } catch(_){}
          // Blob is now known. Try a silent sessionStorage restore (refresh) or,
          // on a cold relaunch where the cache is gone, prompt the admin for their
          // PIN so central AI is re-activated instead of silently 401ing later.
          try { if (window.ensureCentralUnlockedOnBoot) setTimeout(window.ensureCentralUnlockedOnBoot, 0); } catch(_){}
        }
      } catch(e) { console.warn('[auth] centralKeys fetch failed:', e.message); }
    }
  });

  // ---- Sync helpers exposed to global scope ----
  // Small keys: stored as single Firestore docs (same as before)
  const SMALL_KEYS = ['customers','staff','users','salaryRecords','advanceRecords',
    'staffScores','aiFeedback','attendanceRecords','mechanics','retailCustomers',
    'collections','expenses','settings','suppliers','purchases',
    'supplierAdvances','dayClosings','bankAccounts','manualPayables',
    'demandInbox','incomingStock','brandMatrix',
    'saleReturns','purchaseOrders','locations','shopNotes'];
  // Large keys: stored as individual Firestore documents in sub-collections
  const LARGE_KEYS = ['parts','sales','orders','wholesaleOrders','activityLog','visualTraining'];

  function setSyncStatus(status, msg) {
    const el = document.getElementById('sync-indicator');
    if (!el) return;
    el.className = status;
    el.textContent = msg;
    if (status === 'synced') setTimeout(() => el.className = 'hidden', 2500);
  }

  // Push all state to Firestore — small keys as single docs, large keys as arrays in sub-docs
  window.firebasePush = async function(stateObj) {
    if (!window._firebaseAuthed) { setSyncStatus('offline','⚡ Not authenticated'); return; }
    // PERF (mobile hang fix): push ONLY the keys that actually changed since the last
    // successful push. Previously EVERY push re-serialised and re-uploaded the entire
    // database — editing one customer's phone number re-sent the whole sales history,
    // orders, wholesale orders and activity log, both as main-thread JSON.stringify work
    // and as a multi-MB upload over phone data. `_fbDirtyKeys === null` means "cloud state
    // unknown" (cold boot, forced resync, previous failure) and still does a full push.
    // null OR undefined => full push. Undefined can only happen if this module somehow
    // runs before the classic script that declares the tracker; defaulting to a full
    // upload there is the safe direction to fail.
    const _fullPush = !window._fbDirtyKeys;
    const _dirtySnap = _fullPush ? null : new Set(window._fbDirtyKeys);
    // Start accumulating immediately so a save() that lands mid-upload isn't swallowed:
    // the batch captured its values at build time, so a later edit must stay queued.
    if (_fullPush) window._fbDirtyKeys = new Set();
    const _wantsKey = function(k) { return !_dirtySnap || _dirtySnap.has(k); };
    if (_dirtySnap && _dirtySnap.size === 0) { setSyncStatus('synced', '✅ Synced'); return; }
    setSyncStatus('syncing', '☁ Syncing…');
    try {
      const batch = writeBatch(db);
      // Small keys: one doc each (same as before)
      SMALL_KEYS.forEach(k => {
        if (!_wantsKey(k)) return;
        // Never write the Claude API key to Firestore — strip it from settings before push
        let val = stateObj[k] ?? [];
        if (k === 'settings' && val && typeof val === 'object') {
          val = Object.assign({}, val);
          delete val.aiKey;
          delete val.aiVisionKey;
          delete val.jarvisAuth; // Bearer token — never to cloud
          if (val.bsp && typeof val.bsp === 'object') {
            val.bsp = Object.assign({}, val.bsp);
            delete val.bsp.key; // WhatsApp BSP API key — never to cloud
          }
        }
        // Strip base64 bill photos from purchases before Firestore push —
        // base64 images (200-500KB each) blow past Firestore's 1MB doc limit.
        // Photos are uploaded to Firebase Storage (bill_photos/) and the https:// URL
        // is stored instead. Only strip base64 — https:// URLs are small and stay.
        if (k === 'purchases' && Array.isArray(val)) {
          val = val.map(function(p) {
            if (p && p.photoDataUrl && p.photoDataUrl.startsWith('data:')) {
              return Object.assign({}, p, { photoDataUrl: '__uploading__' });
            }
            return p;
          });
        }
        batch.set(doc(db, 'shopdata', k), { data: JSON.stringify(val), ts: Date.now() });
      });
      // Large keys: stored as chunked docs (max ~900KB per doc, 500 items/chunk)
      // Skip 'parts' blob when per-doc store is active — parts/{id} is canonical after migration.
      // Writing the blob in parallel would create a stale copy that firebasePullParts could
      // merge back over individually-written parts, causing price/stock overwrites.
      const _perDocActive = localStorage.getItem('_partsDocMigrated') === '2';
      LARGE_KEYS.forEach(k => {
        if (k === 'parts' && _perDocActive) return; // parts live at parts/{id} — skip blob
        if (!_wantsKey(k)) return;                  // unchanged since last push — don't re-chunk or re-upload
        const arr = (window._gpStripBillPhotos ? window._gpStripBillPhotos(k, stateObj[k] || []) : (stateObj[k] || []));
        const CHUNK = 500;
        const numChunks = Math.max(1, Math.ceil(arr.length / CHUNK));
        // Store metadata doc
        batch.set(doc(db, 'shopdata', k + '__meta'), { chunks: numChunks, total: arr.length, ts: Date.now() });
        for (let i = 0; i < numChunks; i++) {
          const slice = arr.slice(i * CHUNK, (i + 1) * CHUNK);
          batch.set(doc(db, 'shopdata', k + '__' + i), { data: JSON.stringify(slice) });
        }
      });
      await batch.commit();
      // Clear ONLY the keys we actually pushed. Anything marked dirty *during* the upload
      // stays dirty and goes out on the next push. On failure nothing is cleared, so a
      // dropped push is retried rather than silently lost.
      if (_dirtySnap) { _dirtySnap.forEach(function(k){ if (window._fbDirtyKeys) window._fbDirtyKeys.delete(k); }); }
      setSyncStatus('synced', '✅ Synced');
      // Clear offline banner on successful push
      if (typeof window._syncFailCount !== 'undefined') window._syncFailCount = 0;
      if (typeof window._updateOfflineBanner === 'function') window._updateOfflineBanner();
    } catch(e) {
      // If batch is too large (>500 writes), fall back to sequential writes
      if (e.code === 'invalid-argument' || (e.message && e.message.includes('maximum'))) {
        await window.firebasePushSequential(stateObj, _dirtySnap);
      } else {
        // Failed push: nothing was confirmed in the cloud, so restore the dirty state.
        // A failed FULL push goes back to null (re-upload everything next time); a failed
        // partial push re-adds exactly the keys it tried to send.
        if (_fullPush) window._fbDirtyKeys = null;
        else if (_dirtySnap && window._fbDirtyKeys) _dirtySnap.forEach(function(k){ window._fbDirtyKeys.add(k); });
        setSyncStatus('offline', '⚡ Offline – saved locally');
        console.warn('Firebase push failed:', e.message);
        // Increment fail counter and show banner (if not already shown via navigator.onLine)
        if (typeof window._syncFailCount !== 'undefined') { window._syncFailCount++; }
        if (typeof window._updateOfflineBanner === 'function') window._updateOfflineBanner();
      }
    }
  };

  // Expose setSyncStatus so the offline banner patch can hook into it
  window._setSyncStatus = setSyncStatus;

  // Fallback: sequential writes for very large datasets
  window.firebasePushSequential = async function(stateObj, dirtySnap) {
    // Same dirty-key gate as the batch path — a fallback push must not turn into a
    // full-database re-upload just because the batch overflowed.
    const _wantsKeySeq = function(k) { return !dirtySnap || dirtySnap.has(k); };
    setSyncStatus('syncing', '☁ Syncing (large dataset)…');
    try {
      for (const k of SMALL_KEYS) {
        if (!_wantsKeySeq(k)) continue;
        // Never write the Claude API key to Firestore — strip it from settings before push
        let val = stateObj[k] ?? [];
        if (k === 'settings' && val && typeof val === 'object') {
          val = Object.assign({}, val);
          delete val.aiKey;
          delete val.aiVisionKey;
          delete val.jarvisAuth; // Bearer token — never to cloud
          if (val.bsp && typeof val.bsp === 'object') {
            val.bsp = Object.assign({}, val.bsp);
            delete val.bsp.key; // WhatsApp BSP API key — never to cloud
          }
        }
        // Strip base64 bill photos from purchases — same reason as batch push above
        if (k === 'purchases' && Array.isArray(val)) {
          val = val.map(function(p) {
            if (p && p.photoDataUrl && p.photoDataUrl.startsWith('data:')) {
              return Object.assign({}, p, { photoDataUrl: '__uploading__' });
            }
            return p;
          });
        }
        await setDoc(doc(db, 'shopdata', k), { data: JSON.stringify(val), ts: Date.now() });
      }
      const _perDocActiveSeq = localStorage.getItem('_partsDocMigrated') === '2';
      await Promise.all(LARGE_KEYS.map(async k => {
        if (k === 'parts' && _perDocActiveSeq) return; // skip parts blob — per-doc store is canonical
        if (!_wantsKeySeq(k)) return;
        const arr = (window._gpStripBillPhotos ? window._gpStripBillPhotos(k, stateObj[k] || []) : (stateObj[k] || []));
        const CHUNK = 500;
        const numChunks = Math.max(1, Math.ceil(arr.length / CHUNK));
        const writes = [setDoc(doc(db, 'shopdata', k + '__meta'), { chunks: numChunks, total: arr.length, ts: Date.now() })];
        for (let i = 0; i < numChunks; i++) {
          const slice = arr.slice(i * CHUNK, (i + 1) * CHUNK);
          writes.push(setDoc(doc(db, 'shopdata', k + '__' + i), { data: JSON.stringify(slice) }));
        }
        await Promise.all(writes);
      }));
      if (dirtySnap) { dirtySnap.forEach(function(k){ if (window._fbDirtyKeys) window._fbDirtyKeys.delete(k); }); }
      setSyncStatus('synced', '✅ Synced');
    } catch(e) {
      // Nothing confirmed — put the keys back so the next push retries them.
      if (!dirtySnap) window._fbDirtyKeys = null;
      else if (window._fbDirtyKeys) dirtySnap.forEach(function(k){ window._fbDirtyKeys.add(k); });
      setSyncStatus('offline', '⚡ Offline – saved locally');
      console.warn('Firebase sequential push failed:', e.message);
    }
  };

  // Escape hatch: force a complete re-upload of every key regardless of dirty state.
  // Use for "Sync now" buttons, after a restore, or any time cloud state is suspect.
  window.firebaseForceFullPush = function() {
    window._fbDirtyKeys = null;
    return window.firebasePush(window.state);
  };

  // Pull all state keys from Firestore on startup
  window.firebasePull = async function() {
    if (!window._firebaseAuthed) {
      await new Promise(r=>{let w=0;const iv=setInterval(()=>{w+=200;if(window._firebaseAuthed||(w>=5000&&!window._convexSigningIn)){clearInterval(iv);r();}},200);});
      if (!window._firebaseAuthed) { setSyncStatus('offline','⚡ Auth timeout'); return null; }
    }
    setSyncStatus('syncing', '☁ Loading cloud data…');
    try {
      const result = {};
      // Small keys
      await Promise.all(SMALL_KEYS.map(async k => {
        const snap = await getDoc(doc(db, 'shopdata', k));
        if (snap.exists()) result[k] = JSON.parse(snap.data().data);
      }));
      // Large keys: reassemble from chunks — also capture cloud ts for merge decisions
      const _cloudTs = {};
      await Promise.all(LARGE_KEYS.map(async k => {
        const metaSnap = await getDoc(doc(db, 'shopdata', k + '__meta'));
        if (!metaSnap.exists()) {
          const legacySnap = await getDoc(doc(db, 'shopdata', k));
          if (legacySnap.exists()) { result[k] = JSON.parse(legacySnap.data().data); _cloudTs[k] = legacySnap.data().ts || 0; }
          return;
        }
        _cloudTs[k] = metaSnap.data().ts || 0;
        const { chunks } = metaSnap.data();
        const chunkSnaps = await Promise.all(
          Array.from({length: chunks}, (_, i) => getDoc(doc(db, 'shopdata', k + '__' + i)))
        );
        result[k] = chunkSnaps.flatMap(s => s.exists() ? JSON.parse(s.data().data) : []);
      }));
      // Also capture ts for small keys
      await Promise.all(SMALL_KEYS.map(async k => {
        if (!result[k]) return;
        const snap = await getDoc(doc(db, 'shopdata', k));
        if (snap.exists()) _cloudTs[k] = snap.data().ts || 0;
      }));
      result._cloudTs = _cloudTs;
      // Filter out permanently deleted purchases from cloud pull
      if (Array.isArray(result.purchases) && _purchaseDeletedIds.size > 0) {
        result.purchases = result.purchases.filter(function(p){ return !p || !_purchaseDeletedIds.has(p.id); });
      }
      // Restore bill photos: if Firestore has __uploading__ placeholder but local already
      // has the https:// Storage URL or base64, prefer the local version
      try {
        const localPurchases = DB.get('purchases') || [];
        if (Array.isArray(result.purchases) && localPurchases.length) {
          const localById = {};
          localPurchases.forEach(function(p){ if(p && p.id) localById[p.id] = p; });
          result.purchases = result.purchases.map(function(p) {
            if (!p || !p.id) return p;
            const local = localById[p.id];
            if (!local) return p;
            if (!p.photoDataUrl || p.photoDataUrl === '__uploading__' || p.photoDataUrl === '__local_only__') {
              if (local.photoDataUrl && (local.photoDataUrl.startsWith('data:') || local.photoDataUrl.startsWith('https://'))) {
                return Object.assign({}, p, { photoDataUrl: local.photoDataUrl });
              }
            }
            return p;
          });
        }
      } catch(e) { console.warn('[pull] bill-photo restore failed:', e.message); if (typeof RX !== 'undefined') RX.log('warn','pull','bill-photo restore: '+e.message); }
      setSyncStatus('synced', '✅ Cloud data loaded');
      if (window._markInbound) window._markInbound(); // fresh cloud pull — stamp inbound time
      return result;
    } catch(e) {
      setSyncStatus('offline', '⚡ Offline mode');
      console.warn('Firebase pull failed:', e.message);
      return null;
    }
  };

  // Listen for real-time updates from other devices (small keys only — large keys too expensive)
  if (!window._shopdataUnsubs) window._shopdataUnsubs = {};
  window.firebaseListenAll = function(onUpdate) {
    SMALL_KEYS.forEach(k => {
      if (window._shopdataUnsubs[k]) { try { window._shopdataUnsubs[k](); } catch(_) {} }
      window._shopdataUnsubs[k] = onSnapshot(doc(db, 'shopdata', k), snap => {
        if (snap.exists() && snap.metadata.hasPendingWrites === false) {
          try {
            const val = JSON.parse(snap.data().data);
            const ts = snap.data().ts || 0;
            onUpdate(k, val, ts);
          } catch(e) { console.warn('[listen] onSnapshot parse failed for key:', k, e.message); }
        }
      }, (err) => { console.warn('[listen] onSnapshot error for key:', k, err.message); });
    });
    // Listen for parts meta changes to trigger a re-pull.
    // Only used on devices that haven't completed per-doc migration yet.
    // Once migration is done, the real-time parts collection listener (firestoreListenParts)
    // handles individual part changes — the blob meta listener would merge stale blob data
    // over correctly-written per-doc records, causing price/stock overwrites.
    if (localStorage.getItem('_partsDocMigrated') !== '2') {
      if (window._shopdataUnsubs['parts__meta']) { try { window._shopdataUnsubs['parts__meta'](); } catch(_) {} }
      window._shopdataUnsubs['parts__meta'] = onSnapshot(doc(db, 'shopdata', 'parts__meta'), snap => {
        if (snap.exists() && snap.metadata.hasPendingWrites === false) {
          // Another device updated parts — re-pull parts only
          window.firebasePullParts && window.firebasePullParts();
        }
      }, () => {});
    }
  };

  // Pull just the parts array (used when another device syncs)
  window.firebasePullParts = async function() {
    try {
      const metaSnap = await getDoc(doc(db, 'shopdata', 'parts__meta'));
      if (!metaSnap.exists()) return;
      const cloudTs = metaSnap.data().ts || 0;
      const localTs = _keyLastSaved['parts'] || 0;

      // If local was saved MORE recently than cloud, local is the source of truth
      // Push local up so other devices get the latest
      if (localTs > cloudTs) {
        console.log('[sync] parts: local newer (' + localTs + ' > ' + cloudTs + ') — pushing to cloud');
        if (window.firebasePush) window.firebasePush(state);
        return;
      }

      // Cloud is newer — pull and merge
      // Still guard against our own echo (push just completed and echo arrived)
      if (window._lastLocalSave && (Date.now() - window._lastLocalSave) < 10000) {
        console.log('[sync] parts: skipping echo — local save was ' + (Date.now()-window._lastLocalSave) + 'ms ago');
        return;
      }

      const { chunks } = metaSnap.data();
      const chunkSnaps = await Promise.all(
        Array.from({length: chunks}, (_, i) => getDoc(doc(db, 'shopdata', 'parts__' + i)))
      );
      const cloudParts = chunkSnaps.flatMap(s => s.exists() ? JSON.parse(s.data().data) : []);

      const merged = mergeArrays(state.parts, cloudParts, 'parts');
      state.parts = merged;
      DB.set('parts', merged);
      _keyLastSaved['parts'] = cloudTs; // align local ts to cloud so we don't keep re-pulling
      renderInventory();
      renderHome();
      console.log('[sync] parts merged from cloud: ' + merged.length + ' parts');
      // Recover any part photos stuck on a placeholder (real JPG is in Storage).
      try { if (window.healPlaceholderPartPhotos) window.healPlaceholderPartPhotos(); } catch(e){}
    } catch(e) { console.warn('Parts re-pull failed:', e.message); }
  };

  // ============================================================
  // PER-RECORD WRITES (append-only, no overwrite)
  // Each sale/order/payment becomes its own Firestore document at
  //   shop_records/{collection}_{recordId}
  // The recordId is the item.id (deterministic), so a retry of the
  // same write produces an idempotent set, not a duplicate.
  // ============================================================
  // Batch-write up to 500 ops in one Firestore commit (~50-100ms vs 500 sequential round-trips)
  window.firebaseBatchWrite = async function(ops) {
    // ops: [{type:'append'|'delete', collection, recordId, payload}]
    if (!window._firebaseAuthed) throw new Error('not_authenticated');
    // Firestore batch limit is 500 writes — chunk if needed
    const CHUNK = 490;
    for (let start = 0; start < ops.length; start += CHUNK) {
      const slice = ops.slice(start, start + CHUNK);
      const batch = writeBatch(db);
      for (const op of slice) {
        const ref = doc(db, 'shop_records/' + op.collection + '__' + op.recordId);
        if (op.type === 'delete') {
          batch.set(ref, {
            collection: op.collection, recordId: String(op.recordId),
            data: null, deleted: true, deletedAt: Date.now(),
            deletedBy: (window.state && window.state.session && window.state.session.userId) || 'system'
          }, { merge: true });
        } else {
          let payload = op.payload;
          // Strip base64 photos before Firestore write
          if (op.collection === 'parts' && payload && payload.photoDataUrl && payload.photoDataUrl.startsWith('data:')) {
            payload = Object.assign({}, payload, { photoDataUrl: '__pending_upload__' });
          }
          if (op.collection === 'purchases' && payload && payload.photoDataUrl && payload.photoDataUrl.startsWith('data:')) {
            payload = Object.assign({}, payload, { photoDataUrl: '__uploading__' });
          }
          batch.set(ref, {
            collection: op.collection, recordId: String(op.recordId),
            data: JSON.stringify(payload),
            version: (payload && payload._v) || 1,
            writtenAt: Date.now(),
            writtenBy: (window.state && window.state.session && window.state.session.userId) || 'system',
            shopId: (window.state && window.state.settings && window.state.settings.shopId) || 'default'
          });
        }
      }
      await batch.commit();
    }
  };

  window.firebaseAppendDoc = async function(collectionName, recordId, payload) {
    if (!window._firebaseAuthed) throw new Error('not_authenticated');
    if (!recordId) throw new Error('missing_record_id');
    const path = 'shop_records/' + collectionName + '__' + recordId;
    await setDoc(doc(db, path), {
      collection: collectionName,
      recordId: String(recordId),
      data: JSON.stringify(payload),
      version: (payload && payload._v) || 1,
      writtenAt: Date.now(),
      writtenBy: (window.state && window.state.session && window.state.session.userId) || 'system',
      shopId: (window.state && window.state.settings && window.state.settings.shopId) || 'default'
    });
    return path;
  };

  window.firebaseDeleteDoc = async function(collectionName, recordId) {
    if (!window._firebaseAuthed) throw new Error('not_authenticated');
    const path = 'shop_records/' + collectionName + '__' + recordId;
    // Soft delete — keep audit trail
    await setDoc(doc(db, path), {
      collection: collectionName,
      recordId: String(recordId),
      data: null,
      deleted: true,
      deletedAt: Date.now(),
      deletedBy: (window.state && window.state.session && window.state.session.userId) || 'system'
    }, { merge: true });
  };

  // ============================================================
  // PARTS AS INDIVIDUAL FIRESTORE DOCUMENTS
  // Each part lives at parts/<partId> — no more blobs.
  // This means concurrent writes from different devices never conflict:
  //   Father: updateDoc('parts/abc', {instantPrice:450}) 
  //   Staff:  setDoc('parts/xyz', {partNo:'X1', name:'Bearing',...})
  //   → completely independent, both succeed, no merge needed
  // ============================================================

  // Write a single part to Firestore (upsert)
  window.firestoreSetPart = async function(part) {
    // Throw, don't return. A silent return made every caller — including the pending-queue
    // drain — treat an unauthenticated no-op as a successful write, so the part was removed
    // from the retry queue having never reached Firestore. Auth can drop mid-drain (token
    // refresh, reconnect), so this window is real. Throwing routes it back to the retry path.
    if (!window._firebaseAuthed) throw new Error('not_authenticated');
    const p = Object.assign({}, part);
    // Never store base64 photos in Firestore docs
    if (p.photoDataUrl && p.photoDataUrl.startsWith('data:')) {
      try { if (window.photoUploadEnqueue) window.photoUploadEnqueue(p.id, p.photoDataUrl); } catch(e){} // ensure JPG is uploaded, not just stripped
      p.photoDataUrl = '__pending_upload__';
    }
    // Safety net: Firestore throws on ANY field set to `undefined` (not just
    // updatedAt). Strip them here so a bad merge/calc anywhere else can never
    // again permanently jam this part's sync with an "Unsupported field value"
    // error that keeps retrying and failing forever.
    Object.keys(p).forEach(function(k){ if (p[k] === undefined) delete p[k]; });
    if (p.updatedAt == null) p.updatedAt = Date.now();
    p._serverTs = serverTimestamp();
    await setDoc(doc(db, 'parts', p.id), p, { merge: true });
  };

  // Update only specific fields on a part (used for pricing — never overwrites stock/name etc.)
  window.firestoreUpdatePartFields = async function(partId, fields) {
    if (!window._firebaseAuthed) return;
    const update = Object.assign({}, fields, { _serverTs: serverTimestamp(), updatedAt: Date.now() });
    Object.keys(update).forEach(function(k){ if (update[k] === undefined) delete update[k]; });
    await updateDoc(doc(db, 'parts', partId), update);
  };

  // Atomic stock decrement — safe for concurrent sales on multiple devices
  window.firestoreDecrementStock = async function(partId, qty) {
    if (!window._firebaseAuthed) return;
    await updateDoc(doc(db, 'parts', partId), {
      stock: fsIncrement(-qty),
      _serverTs: serverTimestamp()
    });
  };

  // Atomic stock increment (receiving goods)
  window.firestoreIncrementStock = async function(partId, qty) {
    if (!window._firebaseAuthed) return;
    await updateDoc(doc(db, 'parts', partId), {
      stock: fsIncrement(qty),
      _serverTs: serverTimestamp()
    });
  };

  // Delete a part from Firestore
  window.firestoreDeletePart = async function(partId) {
    if (!window._firebaseAuthed) return;
    await deleteDoc(doc(db, 'parts', partId));
  };

  // Pull ALL parts from Firestore as individual documents
  window.firestorePullAllParts = async function() {
    if (!window._firebaseAuthed) return null;
    try {
      const snap = await getDocs(collection(db, 'parts'));
      const parts = [];
      snap.forEach(function(d){ if (d.exists()) parts.push(d.data()); });
      return parts;
    } catch(e) { console.warn('firestorePullAllParts failed:', e.message); return null; }
  };

  // Real-time listener on parts collection — fires whenever ANY part changes on ANY device.
  // PERF (mobile hang fix): the FIRST snapshot after every (re)subscribe replays the ENTIRE
  // collection (~10,500 docs) as docChanges — and mobile browsers kill the connection on
  // every backgrounding, so this replay happened on every tab-return. Applying docs
  // one-by-one (findIndex O(n) + DB.set + getElementById per doc) was O(n²) ≈ 55M ops on
  // the main thread = a multi-second freeze. Now the whole snapshot is applied as ONE
  // batch: one index build, one persist, one debounced render.
  window.firestoreListenParts = function(onPartChange, onPartDelete) {
    return onSnapshot(collection(db, 'parts'), function(snap) {
      window._lastPartsSnapshotAt = Date.now();   // liveness heartbeat
      window._partsListenerActive = true;
      var changed = [], removed = [];
      snap.docChanges().forEach(function(change) {
        if (change.type === 'added' || change.type === 'modified') changed.push(change.doc.data());
        else if (change.type === 'removed') removed.push(change.doc.id);
      });
      if (typeof window._applyPartsSnapshotBatch === 'function') {
        window._applyPartsSnapshotBatch(changed, removed);
      } else {
        // Batch applier not loaded yet (very early boot) — legacy per-doc path
        changed.forEach(function(p){ onPartChange(p); });
        removed.forEach(function(id){ onPartDelete(id); });
      }
    }, function(e){
      // Mark the listener dead so the heartbeat / visibility / online handlers
      // know to re-subscribe. Previously an onSnapshot error left the "active"
      // flag stuck true, so a silently-dropped listener never restarted and the
      // device went stale until a manual reload.
      window._partsListenerActive = false;
      console.warn('firestoreListenParts error:', e.message);
    });
  };

  // Cheap DELTA pull: only parts whose updatedAt is newer than `sinceTs`.
  // Used by the 2-minute heartbeat as a backup to the real-time listener, so a
  // device never stays stale even if the live stream drops a change. Returns
  // only recently-changed docs (a handful), not the whole catalogue.
  window.firestorePullPartsSince = async function(sinceTs) {
    if (!window._firebaseAuthed) return null;
    try {
      var q = query(collection(db, 'parts'), where('updatedAt', '>', sinceTs || 0), orderBy('updatedAt'));
      var snap = await getDocs(q);
      var parts = [];
      snap.forEach(function(d){ if (d.exists()) parts.push(d.data()); });
      return parts;
    } catch(e) {
      console.warn('firestorePullPartsSince failed:', e.message);
      return null;
    }
  };

  // ── ATTENDANCE / SALARY SYNC BRIDGE ──────────────────────────────────────
  // External attendance software writes one doc per employee into the
  // 'staffSync' collection; Goury listens here and updates the matching staff
  // member's salary in real time. This is the inbound counterpart to the parts
  // listener above and is the recommended way to "connect directly".
  window.firestoreListenStaffSync = function(onStaffSyncDoc) {
    return onSnapshot(collection(db, 'staffSync'), function(snap) {
      snap.docChanges().forEach(function(change) {
        if (change.type === 'added' || change.type === 'modified') {
          const data = change.doc.data() || {};
          data._docId = change.doc.id;
          onStaffSyncDoc(data);
        }
      });
    }, function(e){ console.warn('firestoreListenStaffSync error:', e.message); });
  };
  // One-shot pull of all staffSync docs (manual "sync now")
  window.firestorePullStaffSync = async function() {
    if (!window._firebaseAuthed) return null;
    try {
      const snap = await getDocs(collection(db, 'staffSync'));
      const rows = [];
      snap.forEach(function(d){ if (d.exists()) { const x=d.data()||{}; x._docId=d.id; rows.push(x); } });
      return rows;
    } catch(e) { console.warn('firestorePullStaffSync failed:', e.message); return null; }
  };

  // Migrate existing parts from blob storage to individual docs (one-time, safe to re-run)
  window.migratePartsToDocuments = async function() {
    if (!window._firebaseAuthed) return;
    if (localStorage.getItem('_partsDocMigrated') === '2') return; // already done
    const existing = state.parts || [];
    if (!existing.length) return;
    console.log('[migrate] writing', existing.length, 'parts as individual Firestore docs...');
    const CHUNK = 490;
    for (let i = 0; i < existing.length; i += CHUNK) {
      const batch = writeBatch(db);
      const slice = existing.slice(i, i + CHUNK);
      slice.forEach(function(p) {
        if (!p || !p.id) return;
        const payload = Object.assign({}, p, { _serverTs: serverTimestamp() });
        if (payload.photoDataUrl && payload.photoDataUrl.startsWith('data:')) {
          try { if (window.photoUploadEnqueue) window.photoUploadEnqueue(p.id, payload.photoDataUrl); } catch(e){} // ensure JPG is uploaded, not just stripped
          payload.photoDataUrl = '__pending_upload__';
        }
        // Firestore rejects `undefined` field values and throws "WriteBatch.set()
        // called with invalid data" for the WHOLE chunk — one bad legacy part
        // (e.g. updatedAt left undefined by an old merge bug) was silently
        // blocking this migration on every device that hadn't migrated yet.
        Object.keys(payload).forEach(function(k){ if (payload[k] === undefined) delete payload[k]; });
        if (payload.updatedAt == null) payload.updatedAt = Date.now();
        batch.set(doc(db, 'parts', p.id), payload, { merge: true });
      });
      await batch.commit();
      console.log('[migrate] wrote batch', i, '-', i + slice.length);
    }
    localStorage.setItem('_partsDocMigrated', '2');
    console.log('[migrate] done — all parts now individual Firestore docs');
  };

  // ---- JARVIS INBOX (shared Firestore relay to the JARVIS app) ----
  // Writes to a top-level `jarvis_inbox` collection. Jarvis listens on the
  // same collection and surfaces these in real-time.
  window.firebaseWriteInbox = async function(item) {
    if (!window._firebaseAuthed) throw new Error('not_authenticated');
    const id = (item && item.id) || ('inb_' + Date.now() + '_' + Math.random().toString(36).slice(2,8));
    const path = 'jarvis_inbox/' + id;
    await setDoc(doc(db, path), {
      id,
      source: 'shop-ai',
      kind: item.kind || 'system',           // critical_alert | daily_report | sale_milestone | data_security | system
      severity: item.severity || 'info',     // critical | warning | info
      title: item.title || '',
      message: item.message || '',
      data: item.data || null,
      shopId: (window.state && window.state.settings && window.state.settings.shopId) || (window.state && window.state.settings && window.state.settings.shopName) || 'shop',
      shopName: (window.state && window.state.settings && window.state.settings.shopName) || '',
      createdAt: Date.now(),
      serverTime: serverTimestamp(),
      read: false
    });
    return id;
  };

  // Verify a single doc exists in Firestore (used by save-confirmation)
  window.firebaseVerifyDoc = async function(collectionName, recordId) {
    if (!window._firebaseAuthed) return false;
    try {
      const snap = await getDoc(doc(db, 'shop_records/' + collectionName + '__' + recordId));
      return snap.exists() && snap.data().data !== null;
    } catch (e) { return false; }
  };

  // Pull all per-record docs for a collection (used for cloud-truth restore)
  // ── FIREBASE STORAGE — part photos ────────────────────────
  // Decode a base64 data URL into a Blob locally. BUG FIX: this used fetch(dataUrl), which the
  // page's CSP (connect-src 'self' https: wss:) blocks — so EVERY photo upload failed and was
  // retried forever, parts stayed on '__pending_upload__', and the retries lagged the app.
  function _dataUrlToBlob(dataUrl) {
    const comma = dataUrl.indexOf(',');
    const meta  = dataUrl.slice(5, comma); // e.g. "image/jpeg;base64"
    const bin   = atob(dataUrl.slice(comma + 1));
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    return new Blob([bytes], { type: meta.split(';')[0] || 'image/jpeg' });
  }
  window._dataUrlToBlob = _dataUrlToBlob;

  // Upload a base64 dataUrl to Storage, return the public download URL.
  window.firebaseUploadPartPhoto = async function(partId, dataUrl) {
    if (!window._firebaseAuthed) throw new Error('not_authenticated');
    // Convert base64 to Blob
    const blob  = _dataUrlToBlob(dataUrl);
    const path  = storageRef(storage, 'part_photos/' + partId + '.jpg');
    await uploadBytes(path, blob, { contentType: 'image/jpeg' });
    return await getDownloadURL(path);
  };

  // Delete a part photo from Storage (called when part is deleted).
  window.firebaseDeletePartPhoto = async function(partId) {
    try {
      const path = storageRef(storage, 'part_photos/' + partId + '.jpg');
      await deleteObject(path);
    } catch(e) {
      // File may not exist — ignore 'object-not-found'
      if (e.code !== 'storage/object-not-found') console.warn('deletePartPhoto:', e.message);
    }
  };

  // Get the download URL for an existing part photo (used on pull/restore).
  window.firebaseGetPartPhotoUrl = async function(partId) {
    try {
      const path = storageRef(storage, 'part_photos/' + partId + '.jpg');
      return await getDownloadURL(path);
    } catch(e) { return null; }
  };

  // ── Part-photo placeholder recovery (self-healing) ────────────
  // A part's photoDataUrl can be a PLACEHOLDER string ('__pending_upload__',
  // '__uploading__', '__local_only__') when the base64 was stripped before a
  // Firestore write but the real Storage URL was never written back to the cloud.
  // These placeholders are truthy, so old code rendered <img src="__pending_upload__">
  // → broken/blank image even though the JPG is safely in Storage at
  // part_photos/{id}.jpg. The helpers below detect placeholders and re-fetch the
  // real download URL, then backfill it to BOTH local state and Firestore so it
  // never regresses again.
  window._PHOTO_PLACEHOLDERS = { '__pending_upload__':1, '__uploading__':1, '__local_only__':1, '__pending__':1 };
  window.isPlaceholderPhoto = function(v) {
    return !!(v && typeof v === 'string' && v.charAt(0) === '_' && window._PHOTO_PLACEHOLDERS[v]);
  };
  // Returns a directly-displayable src ('' if there is no real photo yet).
  window.displayPhotoUrl = function(v) {
    if (!v) return '';
    if (window.isPlaceholderPhoto(v)) return '';
    return v;
  };

  window._healingPartPhotos = window._healingPartPhotos || {};
  // Fetch the real Storage URL for one part stuck on a placeholder, backfill local+cloud.
  // RENDER-THRASH FIX: session cache of parts whose photo lookup already came back
  // empty. Without it, every inventory render re-fired a doomed Storage fetch for
  // every placeholder card (the in-flight dedupe map is cleared when the promise
  // resolves), hammering the network on every scroll. Cleared naturally on reload,
  // and explicitly bypassed nowhere — a photo uploaded later arrives via the parts
  // listener updating photoDataUrl, so the negative cache never blocks real photos.
  window._noPhotoIds = window._noPhotoIds || new Set();
  // PERF: persist misses for 24h. The session-only cache meant EVERY app open re-probed Storage
  // for every placeholder part (2,500+ sequential 404s), keeping phones busy for minutes.
  const _NOPHOTO_KEY = '_noPhotoIds_v1', _NOPHOTO_TTL = 24 * 60 * 60 * 1000;
  let _noPhotoSaveTimer = null;
  try {
    const saved = JSON.parse(localStorage.getItem(_NOPHOTO_KEY) || 'null');
    if (saved && Array.isArray(saved.ids) && Date.now() - (saved.ts || 0) < _NOPHOTO_TTL) saved.ids.forEach(function(id){ window._noPhotoIds.add(id); });
    else localStorage.removeItem(_NOPHOTO_KEY);
  } catch(e) { /* cache is an optimisation only */ }
  function _noPhotoPersist() {
    if (_noPhotoSaveTimer) return; // throttle, not debounce — a steady stream of misses must not starve the write
    _noPhotoSaveTimer = setTimeout(function() {
      _noPhotoSaveTimer = null;
      try {
        const prev = JSON.parse(localStorage.getItem(_NOPHOTO_KEY) || 'null');
        localStorage.setItem(_NOPHOTO_KEY, JSON.stringify({ ts: (prev && prev.ts) || Date.now(), ids: Array.from(window._noPhotoIds) }));
      } catch(e) { /* quota — fine, falls back to per-session cache */ }
    }, 2000);
  }
  window._healPartPhoto = async function(partId) {
    if (!partId || !window.firebaseGetPartPhotoUrl || !window._firebaseAuthed) return null;
    if (window._noPhotoIds.has(partId)) return null; // known-missing this session — skip network
    if (window._healingPartPhotos[partId]) return window._healingPartPhotos[partId];
    const promise = (async function() {
      try {
        const url = await window.firebaseGetPartPhotoUrl(partId);
        // Only cache the miss when online — firebaseGetPartPhotoUrl returns null for
        // BOTH "photo doesn't exist" and transient network failures, and we must not
        // permanently mark a part photo-less because of a connectivity blip.
        if (!url && navigator.onLine) { window._noPhotoIds.add(partId); _noPhotoPersist(); }
        if (url && typeof getPartById === 'function') {
          const p = getPartById(partId);
          if (p && window.isPlaceholderPhoto(p.photoDataUrl)) {
            p.photoDataUrl = url;
            try { if (typeof save === 'function') save(); } catch(e){}
            // Field-level, AWAITED writeback — corrects only the doc's photoDataUrl,
            // never pushes the whole part (two-stream safe), and isn't fire-and-forget
            // so an auth flicker can't silently drop it. If auth is unstable this pass,
            // the part stays a placeholder and the periodic heal fixes it next run.
            if (window._firebaseAuthed && window.firestoreUpdatePartFields) {
              try { await window.firestoreUpdatePartFields(partId, { photoDataUrl: url }); }
              catch(e){ console.warn('[heal] writeback failed for', partId, e && e.message); }
            }
          }
          return url;
        }
        return null;
      } catch(e) { return null; }
      finally { delete window._healingPartPhotos[partId]; }
    })();
    window._healingPartPhotos[partId] = promise;
    return promise;
  };

  // Targeted recovery for ONE part, regardless of its current photoDataUrl
  // (empty OR placeholder). Probes Storage directly at part_photos/{id}.jpg and
  // tells you definitively whether the JPG exists in the cloud.
  window.recoverPartPhotoFromCloud = async function(partId) {
    if (!partId) return;
    if (!window._firebaseAuthed) { if (typeof toast==='function') toast('⚠️ Not signed in / offline — try again when online'); return; }
    if (typeof toast==='function') toast('Checking cloud…');
    try {
      const url = window.firebaseGetPartPhotoUrl ? await window.firebaseGetPartPhotoUrl(partId) : null;
      if (url) {
        const p = (typeof getPartById==='function') ? getPartById(partId) : null;
        if (p) {
          p.photoDataUrl = url;
          try { if (typeof save==='function') save(); } catch(e){}
          try { if (typeof cloudPersistPart==='function') cloudPersistPart(p); } catch(e){} // re-link in cloud so it sticks
        }
        if (typeof toast==='function') toast('✅ Photo recovered');
        try { if (typeof openPartDetail==='function' && p) openPartDetail(partId); } catch(e){}
        try { if (typeof renderInventory==='function') renderInventory(); } catch(e){}
      } else {
        if (typeof toast==='function') toast('No photo in cloud for this part — it was never uploaded. Add one with 📷.');
      }
    } catch(e) {
      if (typeof toast==='function') toast('Recovery failed: ' + (e && e.message ? e.message : 'unknown'));
    }
  };

  // Scan all parts for placeholder photos and heal them in small batches.
  // Safe to call repeatedly — only touches parts still on a placeholder.
  window._photoHealRan = false;
  window.healPlaceholderPartPhotos = async function(opts) {
    opts = opts || {};
    if (!window._firebaseAuthed || !window.state || !Array.isArray(window.state.parts)) return;
    const stuck = window.state.parts.filter(function(p){ return p && window.isPlaceholderPhoto(p.photoDataUrl); });
    if (!stuck.length) return;
    let healedAny = false;
    const BATCH = 8;
    for (let i = 0; i < stuck.length; i += BATCH) {
      const slice = stuck.slice(i, i + BATCH);
      const urls = await Promise.all(slice.map(function(p){ return window._healPartPhoto(p.id); }));
      if (urls.some(Boolean)) healedAny = true;
    }
    if (healedAny && !opts.silent) {
      try { if (typeof renderInventory === 'function') renderInventory(); } catch(e){}
      try { if (typeof renderHome === 'function') renderHome(); } catch(e){}
    }
  };

  // ── Photo Doctor: one-time sweep to fix broken / missing part photos ──────────
  // For every part that has a photo value:
  //   • base64 still on this device  → push it to Storage via the upload queue
  //   • placeholder OR an https URL  → re-resolve the canonical Storage file. If it
  //     exists, relink (fixes stale download tokens — the usual cause of 404s — and
  //     placeholders); if Storage genuinely has no file, clear the dead reference so
  //     the part shows a clean 📷 instead of a broken-image icon and the 404 console
  //     spam stops.
  window.photoDoctorRun = async function() {
    if (!window._firebaseAuthed) { if (typeof toast==='function') toast('⚠️ Sign in / go online first'); return; }
    if (!window.state || !Array.isArray(window.state.parts)) return;
    const parts = window.state.parts.filter(function(p){ return p && p.photoDataUrl; });
    if (!parts.length) { if (typeof toast==='function') toast('No photos to check'); return; }
    if (typeof confirm==='function' && !confirm('Photo Doctor will check ' + parts.length + ' part photo(s) against cloud storage:\n\n• Re-link photos whose link went stale\n• Upload photos still saved on this device\n• Clear links to photos that no longer exist (the part will show 📷)\n\nProceed?')) return;
    let queued=0, relinked=0, cleared=0, fine=0, errors=0, changed=false;
    const BATCH = 6;
    for (let i = 0; i < parts.length; i += BATCH) {
      if (typeof toast==='function') toast('🩺 Checking photos… ' + Math.min(i+BATCH, parts.length) + '/' + parts.length);
      const slice = parts.slice(i, i + BATCH);
      await Promise.all(slice.map(async function(p) {
        try {
          const v = p.photoDataUrl;
          if (typeof v === 'string' && v.startsWith('data:')) {
            if (window.photoUploadEnqueue) { window.photoUploadEnqueue(p.id, v); queued++; }
            return;
          }
          const url = window.firebaseGetPartPhotoUrl ? await window.firebaseGetPartPhotoUrl(p.id) : null;
          if (url) {
            if (url !== v) {
              p.photoDataUrl = url; changed = true;
              try { if (typeof cloudPersistPart==='function') cloudPersistPart(p); } catch(e){}
              relinked++;
            } else { fine++; }
          } else {
            p.photoDataUrl = ''; changed = true;            // Storage has no file → drop the dead reference
            try { if (typeof cloudPersistPart==='function') cloudPersistPart(p); } catch(e){}
            cleared++;
          }
        } catch(e) { errors++; }
      }));
    }
    if (changed) { try { if (typeof markChanged==='function') markChanged('parts'); if (typeof save==='function') save(); } catch(e){} }
    try { if (window.drainPhotoUploadQueue) window.drainPhotoUploadQueue(); } catch(e){}
    try { if (typeof renderInventory==='function') renderInventory(); } catch(e){}
    if (typeof alert==='function') alert('🩺 Photo Doctor done:\n• ' + relinked + ' re-linked\n• ' + queued + ' queued for upload\n• ' + cleared + ' dead links cleared\n• ' + fine + ' already OK' + (errors? '\n• ' + errors + ' errors' : ''));
    try { if (typeof logActivity==='function') logActivity('Photo Doctor: relinked '+relinked+', queued '+queued+', cleared '+cleared+', ok '+fine, 'info'); } catch(e){}
  };

  // ── Firebase Storage — bill photos ────────────────────────────
  window.firebaseUploadBillPhoto = async function(purchaseId, dataUrl) {
    if (!window._firebaseAuthed) throw new Error('not_authenticated');
    const blob = _dataUrlToBlob(dataUrl);
    const path = storageRef(storage, 'bill_photos/' + purchaseId + '.jpg');
    await uploadBytes(path, blob, { contentType: 'image/jpeg' });
    return await getDownloadURL(path);
  };

  window.firebaseDeleteBillPhoto = async function(purchaseId) {
    try {
      const path = storageRef(storage, 'bill_photos/' + purchaseId + '.jpg');
      await deleteObject(path);
    } catch(e) {
      if (e.code !== 'storage/object-not-found') console.warn('deleteBillPhoto:', e.message);
    }
  };
  // ────────────────────────────────────────────────────────────

  // ── Firebase Storage — wholesale (previous) bill photos ───────
  window.firebaseUploadWOPhoto = async function(woId, dataUrl) {
    if (!window._firebaseAuthed) throw new Error('not_authenticated');
    const blob = _dataUrlToBlob(dataUrl);
    const path = storageRef(storage, 'wholesale_bill_photos/' + woId + '.jpg');
    await uploadBytes(path, blob, { contentType: 'image/jpeg' });
    return await getDownloadURL(path);
  };

  window.firebaseDeleteWOPhoto = async function(woId) {
    try {
      const path = storageRef(storage, 'wholesale_bill_photos/' + woId + '.jpg');
      await deleteObject(path);
    } catch(e) {
      if (e.code !== 'storage/object-not-found') console.warn('deleteWOPhoto:', e.message);
    }
  };
  // ────────────────────────────────────────────────────────────

  window.firebasePullCollection = async function(collectionName) {
    if (!window._firebaseAuthed) throw new Error('not_authenticated');
    // Note: this is a heavy operation; use only for restore.
    const snap = await getDocs(collection(db, 'shop_records'));
    const out = [];
    snap.forEach(d => {
      const v = d.data();
      if (v.collection === collectionName && !v.deleted && v.data) {
        try { out.push(JSON.parse(v.data)); } catch(e) { /* skip malformed cloud doc */ }
      }
    });
    return out;
  };

  // Signal ready
  window._firebaseReady = true;
  document.dispatchEvent(new Event('firebaseReady'));
