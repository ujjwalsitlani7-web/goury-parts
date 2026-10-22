// ============================================================
// CONVEX ADAPTER — speaks the Firebase Firestore/Auth/Storage API used by
// 01-firebase-sync.mjs, but stores the shop's data in Convex.
//
// Routing:
//   shopdata/<key>                 -> Convex `kv`      (cached on the device by version)
//   parts/<id>, collection parts   -> Convex `parts`   (cached on the device, delta sync)
//   shop_records/<coll>__<id>      -> Convex `records`
//   staffSync, jarvis_inbox        -> real Firebase (attendance software + JARVIS still use it)
//   Storage uploads                -> Convex files; old photos are still read from Firebase
//
// Every Convex call carries the shop token (same one used for AI).
// ============================================================
import * as realFs from "https://www.gstatic.com/firebasejs/10.12.2/firebase-firestore.js";
import * as realAuth from "https://www.gstatic.com/firebasejs/10.12.2/firebase-auth.js";
import * as realSt from "https://www.gstatic.com/firebasejs/10.12.2/firebase-storage.js";

const CONVEX_URL = window.GOURY_CONVEX_URL || "https://terrific-eel-321.convex.cloud";
const TOKEN_KEY = "goury.shopToken";
const REAL_COLLECTIONS = { staffSync: 1, jarvis_inbox: 1 };

// ---------- Convex client ----------
let client = null;
let api = null;
function cx() {
  if (client) return client;
  if (!window.convex || !window.convex.ConvexClient) throw new Error("convex_unavailable");
  client = new window.convex.ConvexClient(CONVEX_URL);
  api = window.convex.anyApi;
  return client;
}
function token() { try { return localStorage.getItem(TOKEN_KEY) || ""; } catch (_) { return ""; } }
const q = (name, args) => cx().query(api.shop[name], Object.assign({ token: token() }, args || {}));
const m = (name, args) => cx().mutation(api.shop[name], Object.assign({ token: token() }, args || {}));
window._convexClient = () => { try { return cx(); } catch (_) { return null; } };

// ---------- tiny IndexedDB cache (saves download bandwidth) ----------
const CACHE_DB = "goury-convex-cache";
let _idb = null;
function idb() {
  if (_idb) return _idb;
  _idb = new Promise((res) => {
    try {
      const r = indexedDB.open(CACHE_DB, 1);
      r.onupgradeneeded = () => { r.result.createObjectStore("kv"); r.result.createObjectStore("meta"); r.result.createObjectStore("parts"); };
      r.onsuccess = () => res(r.result);
      r.onerror = () => res(null);
    } catch (_) { res(null); }
  });
  return _idb;
}
async function cget(store, key) {
  const db = await idb(); if (!db) return undefined;
  return new Promise((res) => {
    try { const r = db.transaction(store).objectStore(store).get(key); r.onsuccess = () => res(r.result); r.onerror = () => res(undefined); }
    catch (_) { res(undefined); }
  });
}
async function cgetAll(store) {
  const db = await idb(); if (!db) return [];
  return new Promise((res) => {
    try { const r = db.transaction(store).objectStore(store).getAll(); r.onsuccess = () => res(r.result || []); r.onerror = () => res([]); }
    catch (_) { res([]); }
  });
}
async function cput(store, entries) { // entries: [[key, value|undefined(delete)]]
  const db = await idb(); if (!db || !entries.length) return;
  return new Promise((res) => {
    try {
      const tx = db.transaction(store, "readwrite"); const os = tx.objectStore(store);
      entries.forEach(([k, v]) => { if (v === undefined) os.delete(k); else os.put(v, k); });
      tx.oncomplete = () => res(); tx.onerror = () => res(); tx.onabort = () => res();
    } catch (_) { res(); }
  });
}
async function cclear(store) {
  const db = await idb(); if (!db) return;
  return new Promise((res) => { try { const tx = db.transaction(store, "readwrite"); tx.objectStore(store).clear(); tx.oncomplete = () => res(); } catch (_) { res(); } });
}

// ---------- sentinels ----------
const SERVER_TS = { __goury: "serverTimestamp" };
const isServerTs = (v) => v && v.__goury === "serverTimestamp";
const isInc = (v) => v && v.__goury === "increment";
export function serverTimestamp() { return SERVER_TS; }
export function increment(n) { return { __goury: "increment", n: Number(n) || 0 }; }
function stripSentinels(obj, toReal) {
  const out = {};
  Object.keys(obj || {}).forEach((k) => {
    const v = obj[k];
    if (v === undefined) return;
    if (isServerTs(v)) { if (toReal) out[k] = realFs.serverTimestamp(); return; }
    if (isInc(v)) { if (toReal) out[k] = realFs.increment(v.n); return; }
    out[k] = v;
  });
  return out;
}

// ---------- refs & snapshots ----------
const DB = { __convexDb: true };
let _realApp = null;
let _realDb = null;
function realDb() { if (!_realDb && _realApp) _realDb = realFs.getFirestore(_realApp); return _realDb; }

export function getFirestore(app) { _realApp = app; return DB; }
export function initializeFirestore(app) { _realApp = app; return DB; }
export function persistentLocalCache() { return {}; }
export function persistentMultipleTabManager() { return {}; }

export function doc(_db, a, b) {
  const path = b === undefined ? String(a) : String(a) + "/" + String(b);
  const i = path.indexOf("/");
  return { __ref: "doc", path, coll: path.slice(0, i), id: path.slice(i + 1) };
}
export function collection(_db, name) { return { __ref: "coll", coll: String(name) }; }
export function where(field, op, value) { return { __c: "where", field, op, value }; }
export function orderBy(field) { return { __c: "orderBy", field }; }
export function query(coll, ...constraints) { return { __ref: "query", coll: coll.coll, constraints }; }

function snap(id, data) {
  return { id, exists: () => data !== null && data !== undefined, data: () => (data == null ? undefined : data), metadata: { hasPendingWrites: false, fromCache: false } };
}
function querySnap(docs, changes) {
  return {
    docs, size: docs.length, empty: docs.length === 0,
    forEach: (cb) => docs.forEach(cb),
    docChanges: () => changes || docs.map((d) => ({ type: "added", doc: d })),
  };
}
const isReal = (coll) => !!REAL_COLLECTIONS[coll];
function realRef(ref) {
  const d = realDb(); if (!d) throw new Error("firebase_unavailable");
  if (ref.__ref === "doc") return realFs.doc(d, ref.path);
  return realFs.collection(d, ref.coll);
}

// ---------- kv (shopdata) with version cache ----------
function kvToData(row) {
  if (!row) return null;
  const extra = row.extra ? JSON.parse(row.extra) : {};
  const out = Object.assign({}, extra);
  if (row.data !== null && row.data !== undefined) out.data = row.data;
  if (row.ts) out.ts = row.ts;
  return out;
}
function dataToKv(key, data) {
  const d = stripSentinels(data, false);
  const item = { key };
  if (typeof d.data === "string") { item.data = d.data; delete d.data; }
  if (typeof d.ts === "number") { item.ts = d.ts; delete d.ts; }
  if (Object.keys(d).length) item.extra = JSON.stringify(d);
  return item;
}
// Batch several getDoc(shopdata/…) calls made together into one version check + one fetch.
let _kvPending = null;
function kvGet(key) {
  if (!_kvPending) {
    _kvPending = { keys: new Map() };
    const batch = _kvPending;
    setTimeout(() => { _kvPending = null; kvFlush(batch); }, 0);
  }
  if (!_kvPending.keys.has(key)) {
    let resolve, reject; const p = new Promise((a, b) => { resolve = a; reject = b; });
    _kvPending.keys.set(key, { p, resolve, reject });
  }
  return _kvPending.keys.get(key).p;
}
async function kvFlush(batch) {
  const keys = Array.from(batch.keys.keys());
  try {
    const versions = await q("kvVersions", { keys });
    const cached = await Promise.all(keys.map((k) => cget("kv", k)));
    const need = keys.filter((k, i) => versions[k] !== null && !(cached[i] && cached[i].rev === versions[k]));
    const fresh = {};
    // Fetch changed keys in small groups so no single response gets huge.
    for (let i = 0; i < need.length; i += 4) {
      const got = await q("kvGetMany", { keys: need.slice(i, i + 4) });
      Object.assign(fresh, got);
    }
    const writes = [];
    keys.forEach((k, i) => {
      let row;
      if (versions[k] === null) { row = null; if (cached[i]) writes.push([k, undefined]); }
      else if (fresh[k]) { row = fresh[k]; writes.push([k, row]); }
      else row = cached[i];
      batch.keys.get(k).resolve(row || null);
    });
    cput("kv", writes);
  } catch (e) {
    batch.keys.forEach((v) => v.reject(e));
  }
}
async function kvSetItems(items) {
  if (!items.length) return;
  // Keep each call well under Convex's request size limit.
  let group = [], size = 0;
  const send = async () => {
    if (!group.length) return;
    const g = group; group = []; size = 0;
    const revs = await m("kvSetMany", { items: g });
    await cput("kv", g.map((it) => [it.key, { data: it.data ?? null, ts: it.ts ?? 0, extra: it.extra ?? null, rev: revs[it.key] }]));
  };
  for (const it of items) {
    const s = (it.data ? it.data.length : 0) + (it.extra ? it.extra.length : 0) + 100;
    if (size + s > 3500000 || group.length >= 40) await send();
    group.push(it); size += s;
  }
  await send();
}

// ---------- parts with local cache + delta sync ----------
async function partsClockGet() { return (await cget("meta", "partsClock")) || 0; }
async function partsSync() {
  let clock = await partsClockGet();
  if (!clock) {
    // First time on this device: full download, then catch up on anything written meanwhile.
    const startClock = await q("partsClock");
    await cclear("parts");
    let cursor = null, done = false;
    while (!done) {
      const r = await q("partsPage", { cursor, numItems: 800 });
      await cput("parts", r.items.map((j) => { const p = JSON.parse(j); return [String(p.id), p]; }));
      cursor = r.cursor; done = r.done;
    }
    clock = startClock;
    await cput("meta", [["partsClock", clock]]);
  }
  const changed = [], removed = [];
  for (let guard = 0; guard < 50; guard++) {
    const d = await q("partsSince", { since: clock });
    const ch = d.changed.map((j) => JSON.parse(j));
    await cput("parts", ch.map((p) => [String(p.id), p]).concat(d.deleted.map((id) => [id, undefined])));
    changed.push(...ch); removed.push(...d.deleted);
    clock = d.clock;
    await cput("meta", [["partsClock", clock]]);
    if (!d.more) break;
  }
  return { changed, removed, clock };
}
async function partsAll() { await partsSync(); return await cgetAll("parts"); }

// ---------- getDoc / getDocs ----------
export async function getDoc(ref) {
  if (isReal(ref.coll)) return realFs.getDoc(realRef(ref));
  if (ref.coll === "shopdata") return snap(ref.id, kvToData(await kvGet(ref.id)));
  if (ref.coll === "parts") { const j = await q("partGet", { pid: ref.id }); return snap(ref.id, j ? JSON.parse(j) : null); }
  if (ref.coll === "shop_records") {
    const r = await q("recordGet", { path: ref.id });
    if (!r) return snap(ref.id, null);
    const i = ref.id.indexOf("__");
    return snap(ref.id, { collection: r.coll, recordId: ref.id.slice(i + 2), data: r.json, deleted: r.deleted || undefined });
  }
  throw new Error("convex_adapter_unknown_path:" + ref.path);
}
export const getDocFromServer = getDoc;

export async function getDocs(target) {
  const coll = target.coll;
  if (isReal(coll)) {
    if (target.__ref === "query") throw new Error("convex_adapter_real_query_unsupported");
    return realFs.getDocs(realRef(target));
  }
  if (coll === "parts") {
    if (target.__ref === "query") {
      const w = target.constraints.find((c) => c.__c === "where");
      const since = w ? Number(w.value) || 0 : 0;
      const { changed } = await partsSync();
      const all = changed.length ? changed : [];
      // Callers ask for "changed since T"; cache already holds everything, so also include
      // cached parts whose updatedAt is newer than T (covers writes from this device).
      const extra = (await cgetAll("parts")).filter((p) => (p.updatedAt || 0) > since && !all.some((c) => c.id === p.id));
      return querySnap(all.concat(extra).map((p) => snap(String(p.id), p)));
    }
    const parts = await partsAll();
    return querySnap(parts.map((p) => snap(String(p.id), p)));
  }
  if (coll === "shop_records") {
    const docs = []; let cursor = null, done = false;
    while (!done) {
      const r = await q("recordsAllPage", { cursor, numItems: 1000 });
      r.items.forEach((it) => {
        const i = it.path.indexOf("__");
        docs.push(snap(it.path, { collection: it.coll, recordId: it.path.slice(i + 2), data: it.json, deleted: it.deleted || undefined }));
      });
      cursor = r.cursor; done = r.done;
    }
    return querySnap(docs);
  }
  throw new Error("convex_adapter_unknown_collection:" + coll);
}

// ---------- writes ----------
function recordOp(ref, data) {
  const i = ref.id.indexOf("__");
  const coll = data.collection || ref.id.slice(0, i);
  const recordId = String(data.recordId || ref.id.slice(i + 2));
  if (data.deleted) return { coll, recordId, json: null };
  return { coll, recordId, json: typeof data.data === "string" ? data.data : JSON.stringify(data.data ?? null) };
}
function partJson(ref, data) {
  const p = stripSentinels(data, false);
  if (!p.id) p.id = ref.id;
  return JSON.stringify(p);
}
export async function setDoc(ref, data, _opts) {
  if (isReal(ref.coll)) return realFs.setDoc(realRef(ref), stripSentinels(data, true), _opts || {});
  if (ref.coll === "shopdata") return kvSetItems([dataToKv(ref.id, data)]);
  if (ref.coll === "parts") return m("partsUpsert", { parts: [partJson(ref, data)] });
  if (ref.coll === "shop_records") return m("recordsWrite", { ops: [recordOp(ref, data)] });
  throw new Error("convex_adapter_unknown_path:" + ref.path);
}
export async function updateDoc(ref, fields) {
  if (isReal(ref.coll)) return realFs.updateDoc(realRef(ref), stripSentinels(fields, true));
  if (ref.coll !== "parts") throw new Error("convex_adapter_update_unsupported:" + ref.path);
  const inc = Object.keys(fields).find((k) => isInc(fields[k]));
  if (inc) {
    if (inc !== "stock") throw new Error("convex_adapter_increment_only_stock");
    const rest = stripSentinels(fields, false);
    await m("partAddStock", { pid: ref.id, delta: fields[inc].n });
    if (Object.keys(rest).length) await m("partUpdateFields", { pid: ref.id, fields: JSON.stringify(rest) });
    return;
  }
  return m("partUpdateFields", { pid: ref.id, fields: JSON.stringify(stripSentinels(fields, false)) });
}
export async function deleteDoc(ref) {
  if (isReal(ref.coll)) return realFs.deleteDoc(realRef(ref));
  if (ref.coll === "parts") return m("partDelete", { pid: ref.id });
  throw new Error("convex_adapter_delete_unsupported:" + ref.path);
}
export function writeBatch(_db) {
  const ops = [];
  return {
    set(ref, data, opts) { ops.push({ ref, data, opts }); return this; },
    async commit() {
      const kv = [], parts = [], recs = [], real = [];
      ops.forEach(({ ref, data, opts }) => {
        if (isReal(ref.coll)) real.push({ ref, data, opts });
        else if (ref.coll === "shopdata") kv.push(dataToKv(ref.id, data));
        else if (ref.coll === "parts") parts.push(partJson(ref, data));
        else if (ref.coll === "shop_records") recs.push(recordOp(ref, data));
        else throw new Error("convex_adapter_unknown_path:" + ref.path);
      });
      await kvSetItems(kv);
      for (let i = 0; i < parts.length; i += 200) await m("partsUpsert", { parts: parts.slice(i, i + 200) });
      for (let i = 0; i < recs.length; i += 200) await m("recordsWrite", { ops: recs.slice(i, i + 200) });
      for (const r of real) await realFs.setDoc(realRef(r.ref), stripSentinels(r.data, true), r.opts || {});
    },
  };
}

// ---------- live listeners ----------
// shopdata/<key>: watch a tiny version map; refetch a key only when its version changes.
const _kvWatch = new Map(); // key -> Set of {next, error}
let _kvUnsub = null, _kvResubTimer = null, _kvLastRev = {};
function kvResubscribe() {
  clearTimeout(_kvResubTimer);
  _kvResubTimer = setTimeout(() => {
    if (_kvUnsub) { try { _kvUnsub(); } catch (_) {} _kvUnsub = null; }
    const keys = Array.from(_kvWatch.keys());
    if (!keys.length) return;
    try {
      _kvUnsub = cx().onUpdate(api.shop.kvVersions, { token: token(), keys }, async (versions) => {
        for (const k of keys) {
          if (_kvLastRev[k] === versions[k]) continue;
          _kvLastRev[k] = versions[k];
          try {
            const row = await kvGet(k);
            const s = snap(k, kvToData(row));
            (_kvWatch.get(k) || []).forEach((w) => { try { w.next(s); } catch (_) {} });
          } catch (e) { (_kvWatch.get(k) || []).forEach((w) => { try { w.error && w.error(e); } catch (_) {} }); }
        }
      }, (e) => { _kvWatch.forEach((set) => set.forEach((w) => { try { w.error && w.error(e); } catch (_) {} })); });
    } catch (e) { _kvWatch.forEach((set) => set.forEach((w) => { try { w.error && w.error(e); } catch (_) {} })); }
  }, 0);
}
export function onSnapshot(target, next, error) {
  if (isReal(target.coll)) {
    try { return realFs.onSnapshot(realRef(target), next, error); }
    catch (e) { if (error) setTimeout(() => error(e), 0); return () => {}; }
  }
  if (target.__ref === "doc" && target.coll === "shopdata") {
    const w = { next, error };
    if (!_kvWatch.has(target.id)) _kvWatch.set(target.id, new Set());
    _kvWatch.get(target.id).add(w);
    delete _kvLastRev[target.id];
    kvResubscribe();
    return () => { const s = _kvWatch.get(target.id); if (s) { s.delete(w); if (!s.size) _kvWatch.delete(target.id); } kvResubscribe(); };
  }
  if (target.__ref === "coll" && target.coll === "parts") {
    let stopped = false, unsub = null, busy = false, again = false;
    const run = async (initial) => {
      if (busy) { again = true; return; }
      busy = true;
      try {
        if (initial) {
          const all = await partsAll();
          if (!stopped) next(querySnap(all.map((p) => snap(String(p.id), p))));
        } else {
          const { changed, removed } = await partsSync();
          if (!stopped && (changed.length || removed.length)) {
            const ch = changed.map((p) => ({ type: "modified", doc: snap(String(p.id), p) }))
              .concat(removed.map((id) => ({ type: "removed", doc: snap(id, null) })));
            next(querySnap(changed.map((p) => snap(String(p.id), p)), ch));
          }
        }
      } catch (e) { if (error && !stopped) error(e); }
      busy = false;
      if (again && !stopped) { again = false; run(false); }
    };
    run(true).then(() => {
      if (stopped) return;
      try {
        let first = true;
        unsub = cx().onUpdate(api.shop.partsClock, { token: token() }, () => { if (first) { first = false; } run(false); }, (e) => error && error(e));
      } catch (e) { error && error(e); }
    });
    return () => { stopped = true; if (unsub) try { unsub(); } catch (_) {} };
  }
  throw new Error("convex_adapter_unknown_listener:" + (target.path || target.coll));
}

// ---------- auth: "signed in" = this device has a valid shop token ----------
const _authCbs = new Set();
let _user = null;
let _realAuthObj = null;
export function getAuth(app) {
  _realApp = app;
  try { _realAuthObj = realAuth.getAuth(app); } catch (_) {}
  return { __convexAuth: true };
}
export function onAuthStateChanged(_auth, cb) {
  _authCbs.add(cb);
  setTimeout(() => { try { cb(_user); } catch (_) {} }, 0);
  return () => _authCbs.delete(cb);
}
function setUser(u) { _user = u; _authCbs.forEach((cb) => { try { cb(u); } catch (_) {} }); }
async function tokenWorks() {
  try { return (await q("ping")) === true; }
  catch (e) {
    // Production Convex hides error text ("Server Error"); the code is in e.data.
    const why = String(e && e.data) + " " + String(e && e.message);
    if (/bad_token|server_not_configured/.test(why)) return false;
    throw e;
  }
}
export async function signInAnonymously(_auth) {
  // While this is true, the startup cloud pull keeps waiting (however long the person
  // takes to type the shop token) instead of giving up after 5 s and running without data.
  window._convexSigningIn = true;
  try { return await _signIn(_auth); } finally { window._convexSigningIn = false; }
}
async function _signIn(_auth) {
  // Real Firebase sign-in is still needed for the attendance/JARVIS bridges and old photos.
  if (_realAuthObj) realAuth.signInAnonymously(_realAuthObj).catch((e) => console.warn("[firebase bridges] anon auth failed:", e.message));
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      if (token() && (await tokenWorks())) { setUser({ uid: "shop", isAnonymous: true }); return { user: _user }; }
    } catch (e) {
      // Network/offline: keep running on local data, retry when back online.
      console.warn("[convex] connect failed, will retry when online:", e.message);
      window.addEventListener("online", () => { signInAnonymously(_auth).catch(() => {}); }, { once: true });
      throw e;
    }
    const had = !!token();
    try { localStorage.removeItem(TOKEN_KEY); } catch (_) {}
    const t = window.askShopToken ? await window.askShopToken(had
      ? "That shop token didn\u2019t work. Check it and try again."
      : "Enter the shop token to connect this device to the shop\u2019s data.") : "";
    if (!t) throw new Error("no_shop_token");
  }
  throw new Error("no_shop_token");
}

// ---------- storage: new photos in Convex, old ones still readable from Firebase ----------
let _realStorage = null;
export function getStorage(app) { _realApp = app; try { _realStorage = realSt.getStorage(app); } catch (_) {} return { __convexStorage: true }; }
export function ref(_storage, path) { return { __sref: true, fullPath: String(path) }; }
export async function uploadBytes(sref, blob, meta) {
  const url = await m("uploadUrl");
  const res = await fetch(url, { method: "POST", headers: { "Content-Type": (meta && meta.contentType) || blob.type || "application/octet-stream" }, body: blob });
  if (!res.ok) throw new Error("upload_failed_" + res.status);
  const { storageId } = await res.json();
  sref._url = await m("fileSave", { path: sref.fullPath, storageId });
  return { ref: sref, metadata: meta || {} };
}
export async function getDownloadURL(sref) {
  if (sref._url) return sref._url;
  const u = await q("fileUrl", { path: sref.fullPath });
  if (u) return u;
  if (_realStorage) return realSt.getDownloadURL(realSt.ref(_realStorage, sref.fullPath)); // older photo, still in Firebase
  const e = new Error("object-not-found"); e.code = "storage/object-not-found"; throw e;
}
export async function deleteObject(sref) {
  await m("fileDelete", { path: sref.fullPath });
  if (_realStorage) { try { await realSt.deleteObject(realSt.ref(_realStorage, sref.fullPath)); } catch (_) {} }
}

// Never create demo sample data on a device that isn't connected to the shop yet:
// the app seeds demo data when it finds no cloud data, and that demo data would later
// be merged into the real shop once the device connects.
(function guardDemoSeed() {
  const orig = window.seedSampleDataIfEmpty;
  if (typeof orig !== "function") return;
  window.seedSampleDataIfEmpty = function () {
    if (!_user) { console.warn("[convex] not connected: skipped demo sample data"); return; }
    return orig.apply(this, arguments);
  };
})();

// For support/debugging from the console.
window._convexAdapter = { partsSync, cacheClear: async () => { await cclear("kv"); await cclear("parts"); await cclear("meta"); } };
