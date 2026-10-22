/* ══════════════════════════════════════════════════════════════════════════════
   GOURY OS  ·  CENTRAL NERVOUS SYSTEM  ·  v2.0
   ══════════════════════════════════════════════════════════════════════════════ */
(function(global) {
  'use strict';

  // ── Constants ──────────────────────────────────────────────────────────────
  const VERSION    = '2.0.0';
  const BOOT_AT    = Date.now();
  const CHANNEL_ID = 'goury_cns_channel';
  const STORE_APPS = 'goury_cns_apps_v2';
  const STORE_WHK  = 'goury_cns_webhooks_v2';
  const STORE_LOG  = 'goury_cns_log_v2';
  const MAX_EVENTS = 200;
  const MAX_LOG    = 500;

  // ── Internal state ─────────────────────────────────────────────────────────
  const _apps       = _loadJSON(STORE_APPS, {});
  const _webhooks   = _loadJSON(STORE_WHK,  []);
  const _subs       = {};           // eventType → [fn]
  const _events     = [];           // ring buffer of emitted events
  const _apiLog     = [];           // ring buffer of API calls
  let   _channel    = null;         // BroadcastChannel (cross-tab)
  let   _eventCount = 0;

  // ── Storage helpers ────────────────────────────────────────────────────────
  function _loadJSON(k, def) {
    try { return JSON.parse(localStorage.getItem(k) || 'null') || def; } catch(e) { return def; }
  }
  function _saveApps()    { try { localStorage.setItem(STORE_APPS, JSON.stringify(_apps));     } catch(e){} }
  function _saveWebhooks(){ try { localStorage.setItem(STORE_WHK,  JSON.stringify(_webhooks)); } catch(e){} }

  function _uid()  { return 'cns_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2,8); }
  function _ts()   { return Date.now(); }
  function _copy(v){ return v ? JSON.parse(JSON.stringify(v)) : null; }
  function _ok(d)  { return { ok:true,  data:d,     ts:_ts() }; }
  function _err(m) { return { ok:false, error:m,    ts:_ts() }; }

  // ── Auth ───────────────────────────────────────────────────────────────────
  // 'internal' is always allowed (same-page components)
  function _auth(key) {
    if (!key || key === 'internal') return { appId:'internal', name:'ShopAI', perms:['*'] };
    const a = Object.values(_apps).find(a => a.key === key && a.active);
    return a || null;
  }
  function _hasPerm(app, perm) {
    if (!app) return false;
    if (app.appId === 'internal') return true;
    return (app.perms||[]).includes('*') || (app.perms||[]).includes(perm);
  }

  // ── Safe state reader (deep copy, no mutation) ─────────────────────────────
  function _read(k) {
    const v = global.state && global.state[k];
    if (v === undefined || v === null) return null;
    try { return JSON.parse(JSON.stringify(v)); } catch(e) { return null; }
  }

  // ── Query engine ───────────────────────────────────────────────────────────
  // Supports: where (obj|fn), orderBy ([field,dir]|field), limit, offset,
  //           fields (projection), aggregate (count|sum|avg|min|max + field)
  function _runQuery(collection, opts) {
    let items = _read(collection);
    if (items === null) return _err('unknown_collection:' + collection);
    if (!Array.isArray(items)) return _ok({ item: items }); // settings etc.

    opts = opts || {};

    // WHERE
    if (opts.where) {
      if (typeof opts.where === 'function') {
        items = items.filter(opts.where);
      } else {
        items = items.filter(item => Object.entries(opts.where).every(([k,v]) => {
          if (v && typeof v === 'object') {
            if ('$gte'      in v) return item[k] >= v.$gte;
            if ('$lte'      in v) return item[k] <= v.$lte;
            if ('$gt'       in v) return item[k] >  v.$gt;
            if ('$lt'       in v) return item[k] <  v.$lt;
            if ('$ne'       in v) return item[k] !== v.$ne;
            if ('$in'       in v) return (v.$in).includes(item[k]);
            if ('$contains' in v) return String(item[k]||'').toLowerCase().includes(String(v.$contains).toLowerCase());
            if ('$startsWith' in v) return String(item[k]||'').toLowerCase().startsWith(String(v.$startsWith).toLowerCase());
          }
          return item[k] === v;
        }));
      }
    }

    // ORDER BY
    if (opts.orderBy) {
      const [field, dir] = Array.isArray(opts.orderBy) ? opts.orderBy : [opts.orderBy,'asc'];
      items = [...items].sort((a,b) => {
        const cmp = a[field] < b[field] ? -1 : a[field] > b[field] ? 1 : 0;
        return dir === 'desc' ? -cmp : cmp;
      });
    }

    const totalBeforeLimit = items.length;
    if (opts.offset) items = items.slice(opts.offset);
    if (opts.limit)  items = items.slice(0, opts.limit);

    // PROJECT
    if (opts.fields) {
      items = items.map(item => opts.fields.reduce((acc,f) => { acc[f]=item[f]; return acc; }, {}));
    }

    // AGGREGATE
    if (opts.aggregate) {
      const agg = { count: items.length };
      const f = opts.aggregate.field;
      if (f) {
        const nums = items.map(i => Number(i[f])||0);
        if (opts.aggregate.sum) agg.sum = nums.reduce((a,n)=>a+n,0);
        if (opts.aggregate.avg) agg.avg = (agg.sum||nums.reduce((a,n)=>a+n,0)) / (nums.length||1);
        if (opts.aggregate.min) agg.min = Math.min(...nums);
        if (opts.aggregate.max) agg.max = Math.max(...nums);
      }
      return _ok({ items, aggregate: agg, total: totalBeforeLimit });
    }

    return _ok({ items, total: totalBeforeLimit });
  }

  // ── Computed intelligence layer ────────────────────────────────────────────
  // Delegates to the existing Smart engine + adds cross-collection views
  const _computeFns = {

    'inventory.health': function() {
      const parts = _read('parts') || [];
      // Use EXACT same gate as the app: saleablePart() = catStaffDone() && catPricingDone()
      const staffDone   = p => !!(p.partNo && p.name && p.stock !== undefined && p.stock !== null);
      const pricingDone = p => !!(p.costPrice > 0 && p.instantPrice > 0);
      const sal = p => (typeof global.saleablePart === 'function') ? global.saleablePart(p) : (staffDone(p) && pricingDone(p));
      const saleable = parts.filter(sal);
      return {
        totalSKUs:      parts.length,
        saleableSKUs:   saleable.length,
        inStock:        saleable.filter(p => p.stock > 0).length,
        outOfStock:     saleable.filter(p => p.stock === 0).length,
        lowStock:       saleable.filter(p => p.stock > 0 && p.stock <= (p.minStock||0)).length,
        pendingPricing: parts.filter(p => staffDone(p) && !pricingDone(p)).length,
        draftOnly:      parts.filter(p => !staffDone(p)).length,
        inventoryValue: saleable.reduce((a,p)=>a+(p.stock||0)*(p.costPrice||0),0),
        retailValue:    saleable.reduce((a,p)=>a+(p.stock||0)*(p.instantPrice||0),0),
        withPhoto:      parts.filter(p=>p.photoDataUrl).length,
        photoCoverage:  parts.length ? Math.round(parts.filter(p=>p.photoDataUrl).length/parts.length*100) : 0,
        machineBreakdown: parts.reduce((acc,p)=>{ acc[p.machineType||'other']=(acc[p.machineType||'other']||0)+1; return acc; },{})
      };
    },

    'sales.summary': function(params) {
      const days   = (params&&params.days) || 30;
      const sales  = _read('sales') || [];
      const cutoff = _ts() - days*864e5;
      const recent = sales.filter(s => new Date(s.date).getTime() >= cutoff);
      const parts  = _read('parts') || [];
      let revenue=0, cost=0;
      const partTally = {};
      recent.forEach(s => {
        s.items.forEach(i => {
          revenue += (i.price||0)*(i.qty||1);
          const p = parts.find(x=>x.id===i.partId);
          if (p) cost += (p.costPrice||0)*(i.qty||1);
          partTally[i.partId] = (partTally[i.partId]||0)+(i.qty||1);
        });
      });
      const topParts = Object.entries(partTally)
        .sort((a,b)=>b[1]-a[1]).slice(0,5)
        .map(([id,qty])=>{ const p=parts.find(x=>x.id===id); return {id,name:p&&p.name||id,qty,partNo:p&&p.partNo||''}; });
      return { days, transactions:recent.length, revenue, cost, grossProfit:revenue-cost,
               margin: revenue ? Math.round((revenue-cost)/revenue*100) : 0, topParts };
    },

    'customer.health': function() {
      const custs = _read('customers') || [];
      const sales = _read('sales')     || [];
      const now   = _ts();
      const active30 = new Set(sales.filter(s=>now-new Date(s.date).getTime()<30*864e5).map(s=>s.customerId));
      const totalDue  = custs.reduce((a,c)=>a+(c.totalDue||0),0);
      return {
        total:      custs.length,
        trusted:    custs.filter(c=>c.trusted).length,
        withDue:    custs.filter(c=>(c.totalDue||0)>0).length,
        totalDue,
        active30d:  active30.size,
        wholesale:  custs.filter(c=>c.type==='wholesale').length,
        retail:     custs.filter(c=>c.type==='retail').length
      };
    },

    'supplier.exposure': function() {
      const sups  = _read('suppliers')        || [];
      const adv   = _read('supplierAdvances') || [];
      const purch = _read('purchases')        || [];
      return sups.map(s => ({
        id:s.id, name:s.name, phone:s.phone,
        netAdvance:  adv.filter(a=>a.supplierId===s.id&&!a.settled).reduce((x,a)=>x+(a.amount||0),0),
        totalBought: purch.filter(p=>p.supplierId===s.id).reduce((x,p)=>x+(p.totalAmount||0),0),
        partsCount:  (_read('parts')||[]).filter(p=>p.supplierName===s.name).length
      }));
    },

    'demand.signals': function() {
      const inbox = _read('demandInbox') || [];
      return {
        total:  inbox.length,
        high:   inbox.filter(d=>d.demandLevel==='high').length,
        medium: inbox.filter(d=>d.demandLevel==='medium').length,
        items:  inbox.filter(d=>d.demandLevel==='high'||d.demandLevel==='medium')
                     .slice(0,10)
                     .map(d=>({name:d.partName||d.query||'—',level:d.demandLevel,ts:d.ts||d.createdAt}))
      };
    },

    'briefing': function() {
      if (global.Smart && global.Smart.briefing) return global.Smart.briefing();
      return [];
    },

    'top.sellers': function(params) {
      if (global.Smart && global.Smart.topSellers) return global.Smart.topSellers((params&&params.days)||30,(params&&params.limit)||10);
      return [];
    },

    'dead.stock': function(params) {
      if (global.Smart && global.Smart.deadStock) return global.Smart.deadStock((params&&params.days)||90);
      return [];
    },

    'reorder.candidates': function() {
      const parts = _read('parts') || [];
      if (!global.Smart) return [];
      const isSaleable = p => (typeof global.saleablePart === 'function') ? global.saleablePart(p) : (!!(p.partNo && p.name && p.costPrice > 0 && p.instantPrice > 0));
      return parts.filter(p => {
        if (!isSaleable(p)) return false;
        const days = global.Smart.daysUntilStockout(p);
        return days <= 14 || p.stock === 0;
      }).map(p => ({
        id:p.id, name:p.name, partNo:p.partNo, stock:p.stock,
        minStock:p.minStock||0, machineType:p.machineType,
        supplierName:p.supplierName, supplierPhone:p.supplierPhone,
        velocity:global.Smart.velocity(p.id),
        daysLeft:global.Smart.daysUntilStockout(p),
        reorderQty:global.Smart.smartReorderQty(p)
      })).sort((a,b)=>a.daysLeft-b.daysLeft);
    },

    'full.snapshot': function() {
      // The one call that gives any external app everything it needs
      const inv    = _computeFns['inventory.health']();
      const sales  = _computeFns['sales.summary']({ days:30 });
      const cust   = _computeFns['customer.health']();
      const sups   = _computeFns['supplier.exposure']();
      const demand = _computeFns['demand.signals']();
      const reorder= _computeFns['reorder.candidates']();
      const s      = _copy(global.state && global.state.settings) || {};
      delete s.aiKey;       // never expose API keys
      delete s.aiVisionKey;  // never expose API keys
      return {
        schema:    '2.0', generatedAt: _ts(),
        shopName:  s.shopName || '—',
        inventory: inv, sales, customers: cust,
        suppliers: sups, demand, reorder,
        settings:  s,
        runtime: {
          online:    navigator.onLine,
          firebase:  !!global._firebaseAuthed,
          uptime:    Math.round((_ts()-BOOT_AT)/1000)+'s',
          version:   VERSION
        }
      };
    }
  };

  // ── Event Bus ──────────────────────────────────────────────────────────────
  function _emit(type, payload) {
    _eventCount++;
    const evt = { id:_uid(), type, payload, ts:_ts() };

    // Ring buffer
    _events.unshift(evt);
    if (_events.length > MAX_EVENTS) _events.pop();

    // Local subscribers
    const handlers = [...(_subs[type]||[]), ...(_subs['*']||[])];
    handlers.forEach(fn => { try { fn(_copy(evt)); } catch(e){} });

    // BroadcastChannel — push to other tabs/windows
    if (_channel) {
      try { _channel.postMessage(evt); } catch(e){}
    }

    // Webhooks (async, non-blocking)
    _fireWebhooks(type, payload);

    // Mirror critical events to Firestore jarvis_inbox
    const mirror = ['sale.completed','stockout.detected','reorder.triggered','daily.report','part.added','system.boot'];
    if (mirror.includes(type) && global.firebaseWriteInbox) {
      global.firebaseWriteInbox({
        kind:     type.includes('sale') ? 'sale_milestone' : type.includes('stockout') ? 'critical_alert' : 'system',
        severity: type.includes('stockout') ? 'critical' : 'info',
        title:    '[GouryOS] ' + type,
        message:  typeof payload === 'object' ? JSON.stringify(payload).slice(0,400) : String(payload),
        data:     payload
      }).catch(()=>{});
    }

    // Update UI badge — call via window so it works inside the IIFE
    try { if (typeof window._updateCNSBadge === 'function') window._updateCNSBadge(); } catch(e) { /* badge — non-critical */ }
  }

  // ── Webhook runner ─────────────────────────────────────────────────────────
  async function _fireWebhooks(type, payload) {
    const targets = _webhooks.filter(w => w.active && (w.events.includes('*') || w.events.includes(type)));
    for (const wh of targets) {
      const body = JSON.stringify({
        event: type, payload,
        shopId:   global.state&&global.state.settings&&global.state.settings.shopId || 'default',
        shopName: global.state&&global.state.settings&&global.state.settings.shopName || '',
        ts: _ts(), version: VERSION
      });
      try {
        const r = await fetch(wh.url, {
          method:'POST',
          headers:{ 'Content-Type':'application/json', 'X-GouryOS-Secret': wh.secret||'', 'X-GouryOS-Version': VERSION },
          body,
          signal: AbortSignal.timeout(8000)
        });
        wh.lastFired = _ts(); wh.lastStatus = r.status;
        wh.failCount = r.ok ? 0 : (wh.failCount||0)+1;
      } catch(e) {
        wh.failCount = (wh.failCount||0)+1; wh.lastError = e.message;
      }
      if ((wh.failCount||0) > 5) { wh.active = false; }
      _saveWebhooks();
    }
  }

  // ── Broadcast channel (cross-tab) ──────────────────────────────────────────
  function _initChannel() {
    try {
      _channel = new BroadcastChannel(CHANNEL_ID);
      _channel.onmessage = evt => {
        // Event received from another tab — run local subscribers only (not re-emit to avoid loops)
        const handlers = [...(_subs[evt.data.type]||[]), ...(_subs['*']||[])];
        handlers.forEach(fn => { try { fn(_copy(evt.data)); } catch(e){} });
      };
    } catch(e) { /* browser doesn't support — graceful degradation */ }
  }

  // ── Hook into existing app lifecycle (non-destructive) ─────────────────────
  function _wireHooks() {
    // Intercept save() to emit state.saved
    const _origSave = global.save;
    if (_origSave) {
      global.save = function() {
        const res = _origSave.apply(this, arguments);
        setTimeout(() => _emit('state.saved', { ts:_ts() }), 80);
        return res;
      };
    }

    // Watch sales array length for new sales
    let _lastSaleLen = 0;
    setInterval(() => {
      if (!global.state) return;
      if (document.hidden) return;
      const len = (global.state.sales||[]).length;
      if (len > _lastSaleLen) {
        const newest = global.state.sales[global.state.sales.length-1];
        if (newest) _emit('sale.completed', {
          invoiceNo: newest.invoiceNo, total: newest.total,
          items: newest.items&&newest.items.length,
          paymentMode: newest.paymentMode, date: newest.date
        });
      }
      _lastSaleLen = len;

      // Watch stockouts
      const outNow = (global.state.parts||[]).filter(p=>p.stock===0&&p.instantPrice>0).length;
      if (typeof global._cns_lastOut !== 'undefined' && outNow > global._cns_lastOut) {
        _emit('stockout.detected', { count:outNow, delta:outNow-global._cns_lastOut });
      }
      global._cns_lastOut = outNow;

      // Watch parts array length for new parts added
      const partLen = (global.state.parts||[]).length;
      if (typeof global._cns_lastParts !== 'undefined' && partLen > global._cns_lastParts) {
        const newest = global.state.parts[global.state.parts.length-1];
        if (newest) _emit('part.added', { name:newest.name, partNo:newest.partNo, machineType:newest.machineType });
      }
      global._cns_lastParts = partLen;
    }, window._isMobile ? 8000 : 2500); // mobile: 8s, desktop: 2.5s

    // Boot event
    window.addEventListener('load', () => {
      setTimeout(() => {
        global._cns_lastSaleLen  = (global.state&&global.state.sales||[]).length;
        global._cns_lastOut      = (global.state&&global.state.parts||[]).filter(p=>p.stock===0&&p.instantPrice>0).length;
        global._cns_lastParts    = (global.state&&global.state.parts||[]).length;
        _emit('system.boot', _computeFns['full.snapshot']());
      }, 1500);
    });

    // Firebase ready
    document.addEventListener('firebaseReady', () => _emit('system.firebase_ready', { ts:_ts() }));
  }

  // ── API log ────────────────────────────────────────────────────────────────
  function _logCall(appId, endpoint, ok) {
    _apiLog.unshift({ ts:_ts(), appId, endpoint, ok });
    if (_apiLog.length > MAX_LOG) _apiLog.pop();
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  PUBLIC API  — window.GouryOS
  // ══════════════════════════════════════════════════════════════════════════
  const GouryOS = {
    version: VERSION,

    // ── App Registry ──────────────────────────────────────────────────────
    register(name, perms, meta) {
      const key   = 'gapp_' + _uid();
      const appId = 'app_' + name.toLowerCase().replace(/[^a-z0-9]/g,'_');
      _apps[appId] = {
        appId, name, key,
        perms:  Array.isArray(perms) ? perms : (perms||'read').split(','),
        active: true,
        meta:   meta||{},
        registeredAt: _ts()
      };
      _saveApps();
      _emit('app.registered', { appId, name });
      return _ok({ appId, key, name, message:'Save this key — it will not be shown again.' });
    },

    revoke(appId, key) {
      const app = _auth(key||'internal');
      if (!_hasPerm(app,'admin') && app.appId !== 'internal') return _err('unauthorized');
      if (_apps[appId]) { _apps[appId].active = false; _saveApps(); return _ok(null); }
      return _err('app_not_found');
    },

    listApps(key) {
      const app = _auth(key||'internal');
      if (!app) return _err('unauthorized');
      return _ok(Object.values(_apps).map(a => ({
        appId:a.appId, name:a.name, active:a.active,
        perms:a.perms, meta:a.meta, registeredAt:a.registeredAt
      })));
    },

    // ── Data Queries ──────────────────────────────────────────────────────
    query(collection, opts, key) {
      const app = _auth(key||'internal');
      if (!app) return _err('unauthorized');
      const res = _runQuery(collection, opts);
      _logCall(app.appId, 'query:'+collection, res.ok);
      return res;
    },

    getById(collection, id, key) {
      const app = _auth(key||'internal');
      if (!app) return _err('unauthorized');
      const arr = _read(collection);
      if (!Array.isArray(arr)) return _err('not_an_array');
      const item = arr.find(i=>i.id===id);
      _logCall(app.appId, 'getById:'+collection, !!item);
      return item ? _ok(item) : _err('not_found');
    },

    // ── Computed Intelligence ─────────────────────────────────────────────
    compute(viewName, params, key) {
      const app = _auth(key||'internal');
      if (!app) return _err('unauthorized');
      const fn = _computeFns[viewName];
      if (!fn) return _err('unknown_view:'+viewName);
      try {
        const data = fn(params||{});
        _logCall(app.appId, 'compute:'+viewName, true);
        return _ok(data);
      } catch(e) {
        _logCall(app.appId, 'compute:'+viewName, false);
        return _err('compute_error:'+e.message);
      }
    },

    listViews() {
      return _ok(Object.keys(_computeFns));
    },

    listCollections() {
      return _ok(['parts','customers','sales','orders','wholesaleOrders','staff','suppliers',
        'purchases','collections','expenses','demandInbox','incomingStock',
        'supplierAdvances','dayClosings','bankAccounts','manualPayables',
        'mechanics','retailCustomers','activityLog','settings']);
    },

    // ── Event Bus ──────────────────────────────────────────────────────────
    on(eventType, handler, key) {
      if (key) { const app = _auth(key); if (!app) return _err('unauthorized'); }
      if (!_subs[eventType]) _subs[eventType] = [];
      _subs[eventType].push(handler);
      return _ok({ subscribed: eventType });
    },

    off(eventType, handler) {
      if (_subs[eventType]) _subs[eventType] = _subs[eventType].filter(h=>h!==handler);
    },

    emit(eventType, payload, key) {
      const app = _auth(key||'internal');
      if (!app) return _err('unauthorized');
      _emit(eventType, payload);
      return _ok({ emitted: eventType });
    },

    getEvents(limit, key) {
      const app = _auth(key||'internal');
      if (!app) return _err('unauthorized');
      return _ok(_events.slice(0, limit||50));
    },

    // ── Webhooks ───────────────────────────────────────────────────────────
    webhook: {
      register(url, events, secret, key) {
        const app = _auth(key||'internal');
        if (!app) return _err('unauthorized');
        if (!url||!url.startsWith('https://')) return _err('url must start with https://');
        const id = _uid();
        _webhooks.push({ id, url, events:events||['*'], secret:secret||'',
                         active:true, registeredAt:_ts(), failCount:0, lastFired:null });
        _saveWebhooks();
        _emit('webhook.registered', { id, url, events });
        return _ok({ id });
      },
      list(key) {
        const app = _auth(key||'internal');
        if (!app) return _err('unauthorized');
        return _ok(_webhooks.map(w=>({ id:w.id, url:w.url, events:w.events, active:w.active,
          lastFired:w.lastFired, lastStatus:w.lastStatus, failCount:w.failCount })));
      },
      delete(id, key) {
        const app = _auth(key||'internal');
        if (!app) return _err('unauthorized');
        const i = _webhooks.findIndex(w=>w.id===id);
        if (i<0) return _err('not_found');
        _webhooks.splice(i,1); _saveWebhooks();
        return _ok({ deleted:id });
      },
      test(id, key) {
        const app = _auth(key||'internal');
        if (!app) return _err('unauthorized');
        const wh = _webhooks.find(w=>w.id===id);
        if (!wh) return _err('not_found');
        _fireWebhooks('webhook.test', { message:'GouryOS CNS v'+VERSION+' test ping', ts:_ts() });
        return _ok({ sent:true });
      }
    },

    // ── Access Log ──────────────────────────────────────────────────────────
    getLog(limit) { return _ok(_apiLog.slice(0, limit||100)); },

    // ── System Health ────────────────────────────────────────────────────────
    health() {
      const p = global.state&&global.state.parts||[];
      const s = global.state&&global.state.sales||[];
      return _ok({
        version:    VERSION,
        uptime:     Math.round((_ts()-BOOT_AT)/1000)+'s',
        online:     navigator.onLine,
        firebaseOk: !!global._firebaseAuthed,
        partsCount: p.length,
        salesCount: s.length,
        appsCount:  Object.values(_apps).filter(a=>a.active).length,
        webhookCount: _webhooks.filter(w=>w.active).length,
        eventCount: _eventCount,
        shopName:   (global.state&&global.state.settings&&global.state.settings.shopName)||'—',
        ts:         _ts()
      });
    },

    // ── Live state pointers (for trusted same-page code) ───────────────────
    get state()  { return global.state;  },
    get Smart()  { return global.Smart;  },
    get _emit()  { return _emit; },   // allow internal hooks
    get _events(){ return _events; },
    get _apiLog(){ return _apiLog; }
  };

  // ── Expose globally ────────────────────────────────────────────────────────
  global.GouryOS = GouryOS;

  // ── Also expose reactive state for any consumer ────────────────────────────
  // window.__GOURY_STATE is a live reference — mutations are visible immediately
  Object.defineProperty(global, '__GOURY_STATE', {
    get: () => global.state,
    configurable: true
  });
  Object.defineProperty(global, '__GOURY_SMART', {
    get: () => global.Smart,
    configurable: true
  });

  // ── Initialise ─────────────────────────────────────────────────────────────
  _initChannel();
  _wireHooks();

  console.log(
    '%c GouryOS CNS v' + VERSION + ' ',
    'background:#064e3b;color:#34d399;padding:3px 10px;border-radius:4px;font-weight:800;font-family:monospace;',
    '— data platform ready'
  );

})(window);
