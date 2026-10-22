// ── waitForFirestore ──────────────────────────────────────────
async function waitForFirestore(label) {
  return new Promise(function(resolve) {
    if (!navigator.onLine) { showFsFail(label, 'No internet connection.'); resolve(false); return; }
    if (!window._firebaseAuthed) { showFsFail(label, 'Not authenticated. Reload app.'); resolve(false); return; }
    var tid = setTimeout(function() { showFsFail(label, 'Cloud did not respond. Check internet.'); resolve(false); }, 8000);
    var orig = window.firebasePush;
    window.firebasePush = async function(s) {
      clearTimeout(tid);
      window.firebasePush = orig;
      try {
        await orig(s);
        var el = document.getElementById('sync-indicator');
        if (el && el.className === 'offline') { showFsFail(label, 'Saved locally but cloud sync failed.'); resolve(false); }
        else resolve(true);
      } catch(e) { showFsFail(label, 'Cloud error: ' + e.message); resolve(false); }
    };
  });
}
function showFsFail(label, reason) {
  var ex = document.getElementById('fs-fail-modal');
  if (ex) ex.remove();
  var d = document.createElement('div');
  d.id = 'fs-fail-modal';
  d.style.cssText = 'position:fixed;inset:0;z-index:99999;background:rgba(0,0,0,.7);display:flex;align-items:center;justify-content:center;padding:20px;';
  var inner = document.createElement('div');
  inner.style.cssText = 'background:white;border-radius:16px;padding:24px;max-width:360px;width:100%;';
  inner.innerHTML = '<div style="font-size:36px;text-align:center;margin-bottom:12px">⚠️</div>'
    + '<div style="font-size:17px;font-weight:700;color:#dc2626;text-align:center;margin-bottom:8px">NOT SAVED TO CLOUD</div>'
    + '<div style="font-size:13px;font-weight:600;color:#0f172a;margin-bottom:6px">Action: ' + label + '</div>'
    + '<div style="font-size:13px;color:#64748b;margin-bottom:16px;line-height:1.5">' + reason + '</div>'
    + '<div style="font-size:12px;background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:10px;color:#991b1b;margin-bottom:16px">This action has been cancelled. Fix connection and try again.</div>';
  var btn = document.createElement('button');
  btn.textContent = 'OK, I will Try Again';
  btn.style.cssText = 'width:100%;padding:12px;background:#dc2626;color:white;border:none;border-radius:10px;font-size:15px;font-weight:700;cursor:pointer;';
  btn.onclick = function() { document.getElementById('fs-fail-modal').remove(); };
  inner.appendChild(btn);
  d.appendChild(inner);
  document.body.appendChild(d);
}

// ── AI AGENT ─────────────────────────────────────────────────
var _agAlerts = [];
var _agTab = 'all';
var _agRunning = false;

function toggleAgentPanel() {
  document.getElementById('agent-panel').classList.toggle('open');
}
function agentTab(t) {
  _agTab = t;
  ['all','data','ops','app'].forEach(function(x) {
    var el = document.getElementById('agtab-' + x);
    if (el) el.classList.toggle('active', x === t);
  });
  renderAgentBody();
}
function renderAgentBody() {
  var body = document.getElementById('agent-body');
  if (!body) return;
  if (_agRunning) {
    body.innerHTML = '<div class="ag-think"><div style="font-size:28px;display:inline-block;animation:agSpin .8s linear infinite">🤖</div><br><br>Reviewing your shop...</div>';
    return;
  }
  var list = _agTab === 'all' ? _agAlerts : _agAlerts.filter(function(a) { return a.tab === _agTab; });
  if (!list.length) { body.innerHTML = '<div class="ag-think">No issues in this category</div>'; return; }
  body.innerHTML = list.map(function(a) {
    var icon = a.severity === 'critical' ? 'CRITICAL' : a.severity === 'warning' ? 'WARNING' : 'INFO';
    return '<div class="ag-alert ' + a.severity + '">'
      + '<div class="ag-tag">' + icon + ' - ' + a.category + '</div>'
      + '<div class="ag-msg">' + a.message + '</div>'
      + (a.action ? '<div class="ag-act">Action: ' + a.action + '</div>' : '')
      + '</div>';
  }).join('');
}
function updateBadge() {
  var crit = _agAlerts.filter(function(a) { return a.severity === 'critical'; }).length;
  var warn = _agAlerts.filter(function(a) { return a.severity === 'warning'; }).length;
  var badge = document.getElementById('agent-badge');
  var fab = document.getElementById('agent-fab');
  if (!badge || !fab) return;
  if (crit > 0) { badge.textContent = crit; badge.classList.add('show'); fab.style.background = 'linear-gradient(135deg,#dc2626,#b91c1c)'; }
  else if (warn > 0) { badge.textContent = warn; badge.classList.add('show'); fab.style.background = 'linear-gradient(135deg,#f59e0b,#d97706)'; }
  else { badge.classList.remove('show'); fab.style.background = 'linear-gradient(135deg,#16a34a,#15803d)'; }
}
function showUrgent(urgents) {
  if (!urgents.length) return;
  var list = document.getElementById('urg-list');
  if (!list) return;
  list.innerHTML = urgents.map(function(u) {
    return '<div class="urg-item"><div class="urg-itag">' + u.category + '</div><div class="urg-imsg">' + u.message + '</div></div>';
  }).join('');
  document.getElementById('urg-overlay').classList.add('show');
}
function closeUrgent() { document.getElementById('urg-overlay').classList.remove('show'); }

async function localChecks() {
  var a = [];
  // Use the NEW reliability layer as source of truth (the old #sync-indicator
  // pill auto-hides after 2.5s, so checking its className gives misleading
  // "hidden" results — that caused the agent to falsely flag visibility issues).
  if (!navigator.onLine) a.push({severity:'critical',tab:'data',category:'Cloud Sync',message:'Device is OFFLINE. New writes will queue safely in IndexedDB and sync when internet returns.',action:'Check internet — outbox will retry automatically'});
  if (!window._firebaseAuthed) a.push({severity:'critical',tab:'data',category:'Authentication',message:'Firebase authentication failed. Cloud writes are paused — outbox is holding them in IndexedDB.',action:'Reload the app'});
  if (!window.firebasePush) a.push({severity:'critical',tab:'app',category:'App Integrity',message:'Firebase push function missing. Background sync disabled.',action:'Hard refresh (Ctrl+Shift+R)'});

  // Outbox / reliability layer health
  if (typeof RX !== 'undefined') {
    try {
      var pend = await RX.listPending();
      var failed = pend.filter(function(j){return j.attempts>=5;});
      var oldestAge = await RX.oldestPendingAgeMs();
      if (failed.length > 0) {
        a.push({severity:'critical',tab:'data',category:'Write Failures',message:failed.length + ' record(s) repeatedly failing to save: ' + failed.slice(0,3).map(function(j){return j.collection+'/'+j.recordId.slice(0,8);}).join(', '),action:'Open data health (status pill) and Retry, or save backup'});
      } else if (pend.length > 5 && oldestAge > 60000) {
        a.push({severity:'warning',tab:'data',category:'Sync Backlog',message:pend.length + ' write(s) pending for ' + Math.round(oldestAge/1000) + 's. Records are safe locally but not yet in cloud.',action:'Tap status pill, then Retry Now'});
      }
      var lastSync = await RX.getKey('lastSyncAt');
      if (lastSync) {
        var since = Date.now() - lastSync;
        if (since > 30*60*1000 && pend.length > 0) a.push({severity:'warning',tab:'data',category:'Sync Stale',message:'No successful cloud write in ' + Math.round(since/60000) + ' min.',action:'Check internet and retry'});
      }
      var halt = RX.getHalt();
      if (halt) a.push({severity:'critical',tab:'data',category:'Operations Halted',message:'Shop billing has been halted: ' + halt,action:'Resolve sync errors or override (risky)'});
    } catch(e) {
      a.push({severity:'warning',tab:'app',category:'Reliability Layer',message:'Could not query outbox: ' + e.message,action:''});
    }
  }

  var out = state.parts.filter(function(p) { return p.stock === 0 && p.minStock > 0; });
  if (out.length) a.push({severity:'critical',tab:'ops',category:'Out of Stock',message:out.length + ' part(s) out of stock: ' + out.slice(0,3).map(function(p){return p.name+' ('+p.partNo+')';}).join(', '),action:'Order from suppliers immediately'});

  var low = state.parts.filter(function(p) { return p.stock > 0 && p.minStock > 0 && p.stock <= p.minStock; });
  if (low.length) a.push({severity:'warning',tab:'ops',category:'Low Stock',message:low.length + ' part(s) below minimum: ' + low.slice(0,3).map(function(p){return p.name+' ('+p.stock+' left)';}).join(', '),action:'Reorder soon'});

  var overdue = state.customers.filter(function(c) { return (c.totalDue||0) > 0; });
  var totalCr = overdue.reduce(function(s,c){return s+(c.totalDue||0);},0);
  if (totalCr > 50000) a.push({severity:'critical',tab:'wholesale',category:'Credit Recovery',message:'Total credit outstanding: ₹' + Math.round(totalCr).toLocaleString('en-IN') + ' from ' + overdue.length + ' customers.',action:'Wholesale → 🤖 Agent for prioritised collection plan'});
  else if (totalCr > 10000) a.push({severity:'warning',tab:'wholesale',category:'Credit Outstanding',message:'₹' + Math.round(totalCr).toLocaleString('en-IN') + ' pending from ' + overdue.length + ' customers.',action:'Wholesale → 📋 Collect to send reminders'});
  // Credit limit breach warning
  var atLimit = (state.customers||[]).filter(function(c){return c.creditLimit>0&&(c.totalDue||0)>=(c.creditLimit*0.9);});
  if (atLimit.length) a.push({severity:'warning',tab:'wholesale',category:'Credit Limit',message:atLimit.length+' customer'+(atLimit.length>1?'s':'')+' at/near credit limit: '+atLimit.slice(0,2).map(function(c){return c.name+' (₹'+Math.round(c.totalDue).toLocaleString('en-IN')+'/₹'+Math.round(c.creditLimit).toLocaleString('en-IN')+')'}).join(', '),action:'Wholesale → collect before accepting new orders'});

  var dead = Smart.deadStock(90);
  if (dead.length) {
    var dv = dead.reduce(function(s,p){return s+p.costPrice*p.stock;},0);
    a.push({severity:'warning',tab:'ops',category:'Dead Stock',message:dead.length + ' SKUs not sold in 90 days, locking Rs.' + Math.round(dv).toLocaleString('en-IN') + ' capital.',action:'Consider clearance pricing'});
  }

  if (state.parts.length > 0 && state.parts.length < 5) a.push({severity:'warning',tab:'data',category:'Inventory',message:'Only ' + state.parts.length + ' SKUs loaded. Data may not have synced from cloud yet.',action:'Check internet and reload'});

  var noPrice = state.parts.filter(function(p){return !p.instantPrice||p.instantPrice<=0;});
  if (noPrice.length) a.push({severity:'warning',tab:'data',category:'Data Quality',message:noPrice.length + ' SKUs have no selling price and cannot be billed correctly.',action:'Edit these parts and set a price'});

  // BACKUP ALERTS — agent alerts only, owner must download manually
  if (!state.settings.lastBackup) a.push({severity:'critical',tab:'data',category:'Backup',message:'⚠️ No backup ever downloaded. If Firestore data is lost you have NO recovery. Agent cannot delete or restore data — only you can.',action:'Reports → Data Safety → Save HTML Snapshot NOW'});
  else {
    var days = Math.floor((Date.now()-new Date(state.settings.lastBackup).getTime())/86400000);
    if (days > 7)      a.push({severity:'critical',tab:'data',category:'Backup Overdue',message:'Last backup was ' + days + ' days ago. Cloud data not backed up — agent cannot protect this, only you can.',action:'Download a fresh backup now — Reports → Data Safety'});
    else if (days > 3) a.push({severity:'warning', tab:'data',category:'Backup',message:'Last backup was ' + days + ' days ago. Recommend downloading weekly.',action:'Reports → Data Safety → Save HTML Snapshot'});
  }

  if (!getAIKey() || !getAIKey().startsWith('sk-')) a.push({severity:'info',tab:'app',category:'AI',message:'No Claude API key. Agent running in local-only mode.',action:'Settings - AI Settings - add key for full AI review'});

  var todaySales = state.sales.filter(function(s){return isToday(s.date);});
  if (todaySales.length === 0 && new Date().getHours() >= 11) a.push({severity:'info',tab:'ops',category:'Sales',message:'No sales recorded today yet.',action:'Verify billing is working'});

  // Data completeness — missing rack locations (can't find parts)
  var noRack = (state.parts||[]).filter(function(p){ return saleablePart(p) && !p.location; });
  if (noRack.length >= 5) a.push({severity:'warning',tab:'data',category:'Missing Rack',message:noRack.length + ' parts have no rack location — staff cannot find them. Top: ' + noRack.slice(0,3).map(function(p){return p.name;}).join(', '),action:'Open Inventory → filter missing rack → add location'});

  // Supplier due alerts
  var supDue = (state.suppliers||[]).filter(function(s){ return (s.totalDue||0) > 0; });
  var totalSupDue = supDue.reduce(function(a,s){ return a+(s.totalDue||0); },0);
  if (totalSupDue > 100000) a.push({severity:'critical',tab:'ops',category:'Supplier Payables',message:'₹' + Math.round(totalSupDue).toLocaleString('en-IN') + ' due to ' + supDue.length + ' suppliers. Top: ' + supDue.sort(function(a,b){return (b.totalDue||0)-(a.totalDue||0);}).slice(0,2).map(function(s){return s.name+'(₹'+Math.round(s.totalDue).toLocaleString('en-IN')+')';}).join(', '),action:'Purchases → Suppliers → record payments'});
  else if (totalSupDue > 25000) a.push({severity:'warning',tab:'ops',category:'Supplier Payables',message:'₹' + Math.round(totalSupDue).toLocaleString('en-IN') + ' due to ' + supDue.length + ' supplier(s).',action:'Purchases → Suppliers to review and pay'});

  // Pending advances not yet adjusted
  var pendAdv = (state.supplierAdvances||[]).filter(function(a){ return a.status !== 'adjusted'; });
  var totalPendAdv = pendAdv.reduce(function(t,a){ return t+(a.amount||0)-(a.adjustedAmount||0); }, 0);
  if (totalPendAdv > 50000) a.push({severity:'warning',tab:'ops',category:'Advance Unadjusted',message:'₹' + Math.round(totalPendAdv).toLocaleString('en-IN') + ' in supplier advances not yet adjusted against invoices.',action:'Purchases → Advances → adjust against received bills'});

  // Unmatched incoming stock older than 14 days
  var oldIncoming = (state.incomingStock||[]).filter(function(i){ return daysAgo(i.createdAt||i.date||new Date().toISOString()) > 14; });
  if (oldIncoming.length) a.push({severity:'warning',tab:'ops',category:'Stale Incoming',message:oldIncoming.length + ' incoming stock entr' + (oldIncoming.length>1?'ies':'y') + ' older than 14 days not yet received.',action:'Purchases → Incoming → mark arrived or delete'});

  if (!a.length) a.push({severity:'info',tab:'all',category:'System',message:'All systems healthy. No critical issues found.',action:''});
  // Smart agent checks (only when businessActive + smartAgentsOn)
  checkHotStockout(a); checkWOChase(a); checkChurn(a); checkPricingStall(a);
  return a;
}

async function aiChecks() {
  var key = getAIKey();
  var model = getAIModel();
  var m = Smart.profitMonth();
  var top = Smart.topSellers(30,5);
  var dead = Smart.deadStock(90).slice(0,5);
  var low = state.parts.filter(function(p){return p.stock>0&&p.minStock>0&&p.stock<=p.minStock;});
  var out = state.parts.filter(function(p){return p.stock===0&&p.minStock>0;});
  var overdue = state.customers.filter(function(c){return (c.totalDue||0)>0;}).sort(function(a,b){return (b.totalDue||0)-(a.totalDue||0);});
  var rxPend = (typeof RX !== 'undefined') ? await RX.pendingCount() : 0;
  var rxLast = (typeof RX !== 'undefined') ? await RX.getKey('lastSyncAt') : null;
  var rxHalt = (typeof RX !== 'undefined') ? RX.getHalt() : null;
  var syncState = !navigator.onLine ? 'offline'
    : !window._firebaseAuthed ? 'unauthenticated'
    : rxHalt ? 'halted'
    : rxPend > 0 ? 'syncing(' + rxPend + ' pending)'
    : 'synced' + (rxLast ? ' ' + Math.round((Date.now()-rxLast)/1000) + 's ago' : '');

  var prompt = 'You are an AI agent monitoring a spare parts shop. Review this data and return ONLY a valid JSON array of issues. No explanation. No markdown.\n\n'
    + 'SHOP: ' + state.settings.shopName + '\n'
    + 'DATE: ' + todayStr() + '\n'
    + 'SYNC: ' + syncState + '\n'
    + 'SKUs: ' + state.parts.length + ' Customers: ' + state.customers.length + '\n'
    + 'Revenue: ' + rs(m.revenue) + ' Profit: ' + rs(m.profit) + ' Margin: ' + m.margin.toFixed(1) + '%\n'
    + 'OUT OF STOCK (' + out.length + '): ' + out.slice(0,4).map(function(p){return p.name+'('+p.partNo+')';}).join(', ') + '\n'
    + 'LOW STOCK (' + low.length + '): ' + low.slice(0,4).map(function(p){return p.name+'('+p.stock+' left)';}).join(', ') + '\n'
    + 'DEAD STOCK: ' + dead.map(function(p){return p.name+'('+p.stock+'pcs)';}).join(', ') + '\n'
    + 'CREDIT: Rs.' + Math.round(overdue.reduce(function(s,c){return s+(c.totalDue||0);},0)).toLocaleString('en-IN') + ' from ' + overdue.length + ' customers\n'
    + 'BACKUP: ' + (state.settings.lastBackup ? Math.floor((Date.now()-new Date(state.settings.lastBackup).getTime())/86400000)+'d ago' : 'NEVER') + '\n'
    + 'SALES TODAY: ' + state.sales.filter(function(s){return isToday(s.date);}).length + '\n\n'
    + 'Return JSON array. Each item: {"severity":"critical|warning|info","tab":"data|ops|app","category":"short name","message":"specific details with numbers","action":"what to do"}. Max 8 items.';

  var resp = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-api-key': key,
      'anthropic-version': '2023-06-01',
      'anthropic-dangerous-direct-browser-access': 'true'
    },
    body: JSON.stringify({model:model, max_tokens:1000, messages:[{role:'user', content:prompt}]})
  });
  if (!resp.ok) throw new Error('API ' + resp.status);
  var data = await resp.json();
  var text = data.content[0].text.trim();
  var start = text.indexOf('[');
  var end = text.lastIndexOf(']');
  if (start < 0 || end < 0) throw new Error('No JSON array found');
  return JSON.parse(text.slice(start, end+1));
}

async function runAgent() {
  if (_agRunning) return;
  _agRunning = true;
  var btn = document.getElementById('agent-run-btn');
  var stxt = document.getElementById('agent-status-txt');
  if (btn) { btn.disabled = true; btn.textContent = 'Reviewing...'; }
  if (stxt) stxt.textContent = 'Running local checks...';
  renderAgentBody();

  var alerts = await localChecks();

  if (getAIKey() && getAIKey().startsWith('sk-')) {
    try {
      if (stxt) stxt.textContent = 'Running AI deep review...';
      var aiA = await aiChecks();
      var cats = new Set(alerts.map(function(a){return a.category;}));
      aiA.forEach(function(a) {
        if (!cats.has(a.category)) alerts.push(a);
        else {
          var idx = alerts.findIndex(function(x){return x.category===a.category;});
          if (idx >= 0 && a.severity === 'critical' && alerts[idx].severity !== 'critical') alerts[idx] = a;
        }
      });
    } catch(e) {
      alerts.push({severity:'info',tab:'app',category:'AI Review',message:'AI review failed: ' + e.message,action:''});
    }
  }

  var ord = {critical:0,warning:1,info:2};
  alerts.sort(function(a,b){return ord[a.severity]-ord[b.severity];});
  _agAlerts = alerts;
  _agRunning = false;

  var now = new Date();
  if (btn) { btn.disabled = false; btn.textContent = 'Run Again'; }
  if (stxt) stxt.textContent = 'Last run: ' + now.toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit'});
  var lr = document.getElementById('agent-last-run');
  if (lr) lr.textContent = 'Last run: ' + now.toLocaleString('en-IN');

  updateBadge();
  renderAgentBody();
  var criticals = alerts.filter(function(a){return a.severity==='critical';});
  showUrgent(criticals);
  // Push new critical alerts to Jarvis (de-duped by category in last 30 min)
  try {
    var seen = JSON.parse(localStorage.getItem('rx_jarvis_seen')||'{}');
    var cutoff = Date.now() - 30*60*1000;
    Object.keys(seen).forEach(function(k){ if(seen[k]<cutoff) delete seen[k]; });
    criticals.forEach(function(c) {
      var key = c.category + '|' + c.message.slice(0,40);
      if (!seen[key]) { seen[key] = Date.now(); rxPushCriticalToJarvis(c); }
    });
    localStorage.setItem('rx_jarvis_seen', JSON.stringify(seen));
  } catch(e) { console.warn('[jarvis] seen-dedup failed:', e.message); }
}

setTimeout(function(){runAgent();}, window._isMobile ? 15000 : 8000);
setInterval(function(){ if(document.hidden) return; runAgent(); }, window._isMobile ? 10*60*1000 : 5*60*1000); // mobile: 10 min, desktop: 5 min — skipped while tab hidden (saves API tokens + battery)

// ============================================================
// AGENT PERMISSION BOUNDARY — READ CAREFULLY BEFORE EDITING
// ============================================================
// The AI agent is OBSERVER + ALERTER only. It MUST NOT delete,
// wipe, archive, or modify any business data (parts, sales,
// customers, purchases, suppliers, expenses, collections).
//
// AGENT CAN DO (safe, non-destructive only):
//   ✅ Read state to generate alerts and recommendations
//   ✅ Drain the outbox (retry pending cloud writes)
//   ✅ Trigger auto-snapshot (write-only, additive)
//   ✅ Log fix events for the daily report
//   ✅ Alert when backup is stale / data not syncing
//
// AGENT MUST NEVER DO (owner-only actions):
//   ❌ Delete or splice from state.parts / state.sales / etc.
//   ❌ clearDraft() — draft clearing is triggered by the user only
//   ❌ deleteDoc() on Firestore collections
//   ❌ Any state.xxx.splice() or state.xxx = [] operations
//   ❌ Any RX.clearAll() or localStorage.clear() or IDB wipe
//
// DATA DELETION IS OWNER-ONLY. If you need to add a cleanup
// feature, surface it as an ALERT with a manual action button —
// never execute it automatically.
// ============================================================
var _agentFixesToday = [];
function _agFixedToday() {
  var today = new Date().toISOString().slice(0,10);
  return _agentFixesToday.filter(function(f){return f.date===today;});
}
async function agentSelfHeal() {
  if (typeof RX === 'undefined') return;
  if (document.hidden) return;
  try {
    // 1. Drain outbox if there's pending writes and we're online
    //    (safe: this only retries cloud writes, never deletes data)
    var pend = await RX.pendingCount();
    if (pend > 0 && navigator.onLine && window._firebaseAuthed) {
      RX.drain();
      _agentFixesToday.push({date:new Date().toISOString().slice(0,10),ts:Date.now(),what:'Triggered outbox drain ('+pend+' pending)'});
    }
    // 2. Re-trigger a snapshot if last one was over 24h ago
    //    (safe: snapshot is additive, never removes data)
    var lastSnap = await RX.getKey('lastSnapshotAt');
    if (!lastSnap || Date.now()-lastSnap > 24*60*60*1000) {
      try { await rxAutoSnapshot(); _agentFixesToday.push({date:new Date().toISOString().slice(0,10),ts:Date.now(),what:'Created daily snapshot'}); } catch(e) { console.warn('[agent] auto-snapshot failed:', e.message); }
    }
    // 3. Alert if no backup taken recently — owner must act manually
    //    (NEVER auto-delete or auto-clear anything — alert only)
    if (!state.settings.lastBackup) {
      _agentFixesToday.push({date:new Date().toISOString().slice(0,10),ts:Date.now(),what:'ALERT: No backup on record — owner action required'});
    } else {
      var backupAge = Math.floor((Date.now()-new Date(state.settings.lastBackup).getTime())/86400000);
      if (backupAge > 3) {
        _agentFixesToday.push({date:new Date().toISOString().slice(0,10),ts:Date.now(),what:'ALERT: Backup is '+backupAge+' days old — owner should download snapshot'});
      }
    }
    // 4. Persist fixes log only if changed — avoids Firestore write every cycle
    var _newFixesJson = JSON.stringify(_agentFixesToday);
    if (_newFixesJson !== window._lastAgentFixesJson) {
      await RX.setKey('agentFixesToday', _agentFixesToday);
      window._lastAgentFixesJson = _newFixesJson;
    }
  } catch(e) { RX.log('error','agent','self-heal failed: '+e.message); }
}
setInterval(function(){ if(document.hidden) return; agentSelfHeal(); }, window._isMobile ? 5*60*1000 : 2*60*1000);

// ── SMART AGENTS ──
function _agentsEnabled(){return !!(state.settings&&state.settings.businessActive!==false&&state.settings.smartAgentsOn===true);}
function checkHotStockout(a){if(!_agentsEnabled())return;var u=[];state.parts.forEach(function(p){if(!saleablePart(p))return;var v=Smart.velocity(p.id);if(v<0.5)return;var d=Smart.daysUntilStockout(p);if(d<=5&&d!==Infinity)u.push(p.name+' ('+p.partNo+') ~'+d+'d');});if(u.length)a.push({severity:'critical',tab:'ops',category:'Hot Stockout Alert',message:u.length+' hot part(s) out in 5d: '+u.slice(0,3).join(', '),action:'Brain → Reorder'});}
function checkWOChase(a){
  if(!_agentsEnabled())return;
  const now=Date.now();
  const ov=(state.wholesaleOrders||[]).filter(function(w){
    if(w.paymentMode!=='credit'||w.settled)return false;
    if(w.dueDate) return new Date(w.dueDate)<new Date();
    return now - new Date(w.date||w.createdAt||0).getTime() > 30*864e5;
  });
  if(!ov.length)return;
  const t=ov.reduce(function(s,w){return s+((w.total||0)-(w.paidAmount||0));},0);
  const topCust=(state.customers||[]).filter(function(c){return (c.totalDue||0)>0;}).sort(function(a,b){return (b.totalDue||0)-(a.totalDue||0);}).slice(0,2).map(function(c){return c.name+'(₹'+Math.round(c.totalDue).toLocaleString('en-IN')+')'}).join(', ');
  a.push({severity:'warning',tab:'wholesale',category:'WO Collection Overdue',message:ov.length+' overdue WOs — ₹'+Math.round(t).toLocaleString('en-IN')+' outstanding'+(topCust?' · Top: '+topCust:''),action:'Wholesale → 🤖 Agent or 📋 Collect'});
}
var _cck=null;function checkChurn(a){if(!_agentsEnabled())return;var now=new Date();if(now.getDay()!==1&&_cck)return;_cck=now.toISOString().slice(0,10);var cut=Date.now()-45*864e5;var ch=[];(state.customers||[]).filter(function(c){return c.trusted;}).forEach(function(c){var l=state.sales.filter(function(s){return s.customerId===c.id;}).sort(function(a,b){return b.date.localeCompare(a.date);})[0];var ld=l?new Date(l.date).getTime():0;if(!ld||ld<cut)ch.push(c.name);});if(ch.length)a.push({severity:'warning',tab:'ops',category:'Trusted Customer Churn',message:ch.length+' trusted customer(s) 45+ days inactive: '+ch.slice(0,3).join(', '),action:'WhatsApp → Outreach'});}
function checkPricingStall(a){if(!_agentsEnabled())return;var s=state.parts.filter(function(p){return !!(p.partNo&&p.name&&p.stock!==undefined)&&!((p.costPrice||0)>0&&(p.instantPrice||0)>0);});if(s.length>20)a.push({severity:'warning',tab:'ops',category:'Pricing Queue Stalled',message:s.length+' parts pending pricing.',action:'Catalogue → Owner Pricing → Bulk Fill'});}
var _nf=null;function checkNightlyReorder(){if(!_agentsEnabled())return;var now=new Date();var k=now.toISOString().slice(0,10);if(_nf===k)return;if(now.getHours()<21)return;_nf=k;var bs={};state.parts.forEach(function(p){if(!saleablePart(p))return;var d=Smart.daysUntilStockout(p);if(d>14&&p.stock>(p.minStock||0))return;var n=Smart.smartReorderQty(p);if(n<=0)return;var s=p.supplierName||'Unknown';if(!bs[s])bs[s]=[];bs[s].push({name:p.name,partNo:p.partNo,need:n,stock:p.stock});});Object.keys(bs).forEach(function(sup){var pts=bs[sup];if(pts.length<2)return;var msg='*'+(state.settings.shopName||'Shop')+'* Reorder\n\n';pts.forEach(function(p){msg+='▸ '+p.name+' ('+p.partNo+') need '+p.need+' (stock:'+p.stock+')\n';});var so=(state.suppliers||[]).find(function(s){return s.name===sup;});_agAlerts.unshift({severity:'warning',tab:'ops',category:'🌙 Reorder: '+sup,message:pts.length+' parts need reorder from '+sup,action:so&&so.phone?'wa:'+so.phone+':'+encodeURIComponent(msg):'Draft ready'});});if(Object.keys(bs).length){updateBadge();renderAgentBody();toast('🌙 Nightly reorder done');}}
setInterval(checkNightlyReorder,10*60*1000);
setTimeout(checkNightlyReorder,15000);
// ── END SMART AGENTS ──
setTimeout(async function() {
  // hydrate fixes log on boot
  if (typeof RX !== 'undefined') {
    var saved = await RX.getKey('agentFixesToday');
    if (Array.isArray(saved)) _agentFixesToday = saved;
  }
}, 3000);

function notifyAgentOfHalt(reason) {
  // Append a critical alert and refresh badge so user sees red FAB immediately
  _agAlerts.unshift({severity:'critical',tab:'data',category:'Operations Halted',message:reason,action:'Tap status pill, then Retry Now'});
  try { updateBadge(); renderAgentBody(); } catch(e) { console.warn('[agent] render failed:', e.message); }
  RX.log('critical','agent','halt notified: '+reason);
  // Push to Jarvis immediately
  if (typeof pushToJarvis === 'function') {
    pushToJarvis({
      kind: 'data_security',
      severity: 'critical',
      title: '⛔ Shop Operations Halted',
      message: reason,
      data: { halt: true }
    });
  }
}

// ============================================================
// DAILY REPORT — summarises what the agent did + key shop metrics
// Sends to Jarvis webhook if configured; otherwise downloads JSON.
// ============================================================
async function rxBuildDailyReport() {
  var today = new Date().toISOString().slice(0,10);
  var todaySales = state.sales.filter(function(s){return isToday(s.date);});
  var todayWO = state.wholesaleOrders.filter(function(w){return isToday(w.date);});
  var todayColl = (state.collections||[]).filter(function(c){return c.date===today;});
  var todayExp  = (state.expenses||[]).filter(function(e){return e.date===today;});
  var pend = (typeof RX !== 'undefined') ? await RX.listPending() : [];
  var halt = (typeof RX !== 'undefined') ? RX.getHalt() : null;
  var lowStock = state.parts.filter(function(p){return p.stock>0&&p.minStock>0&&p.stock<=p.minStock;});
  var outStock = state.parts.filter(function(p){return p.stock===0&&p.minStock>0;});
  var totalDue = (state.customers||[]).reduce(function(s,c){return s+(c.totalDue||0);},0);

  return {
    shop: state.settings.shopName,
    date: today,
    generatedAt: new Date().toISOString(),
    sales: {
      count: todaySales.length,
      total: todaySales.reduce(function(s,x){return s+x.total;},0),
      cash: todaySales.filter(function(x){return x.paymentMode==='cash';}).reduce(function(s,x){return s+x.total;},0),
      credit: todaySales.filter(function(x){return x.paymentMode==='credit';}).reduce(function(s,x){return s+x.total;},0)
    },
    wholesale: { count: todayWO.length, total: todayWO.reduce(function(s,x){return s+x.total;},0) },
    collections: { count: todayColl.length, total: todayColl.reduce(function(s,x){return s+(x.amount||0);},0) },
    expenses: { count: todayExp.length, total: todayExp.reduce(function(s,x){return s+(x.amount||0);},0) },
    inventory: { skus: state.parts.length, lowStock: lowStock.length, outOfStock: outStock.length },
    creditOutstanding: Math.round(totalDue),
    dataHealth: {
      pendingWrites: pend.length,
      halted: !!halt, haltReason: halt,
      lastSync: (typeof RX !== 'undefined') ? await RX.getKey('lastSyncAt') : null
    },
    agentFixes: _agFixedToday(),
    topAlerts: (_agAlerts||[]).slice(0,8).map(function(x){return {sev:x.severity,cat:x.category,msg:x.message};})
  };
}

async function rxRunDailyReport() {
  var report = await rxBuildDailyReport();
  // Push to Firestore inbox (primary path — Jarvis listens here)
  var inboxOk = false;
  try {
    await pushToJarvis({
      kind: 'daily_report',
      severity: 'info',
      title: 'Daily Report — ' + report.date,
      message: report.sales.count + ' bills · ' + rs(report.sales.total) + ' · ' +
               report.dataHealth.pendingWrites + ' pending writes · ' +
               report.inventory.outOfStock + ' out of stock',
      data: report
    });
    inboxOk = true;
  } catch(e) { if (typeof RX !== 'undefined') RX.log('error','jarvis','daily inbox failed: '+e.message); }
  // Also POST to optional webhook
  var url = (state.settings && state.settings.jarvisWebhook) || '';
  var posted = inboxOk;
  if (url && /^https?:\/\//.test(url)) {
    try {
      var resp = await fetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json', ...(state.settings.jarvisAuth ? {'authorization': state.settings.jarvisAuth} : {}) },
        body: JSON.stringify({ source:'shop-ai', kind:'daily_report', report })
      });
      posted = posted || resp.ok;
      if (typeof RX !== 'undefined') RX.log(resp.ok?'info':'warn','jarvis','daily report POST status='+resp.status);
    } catch(e) {
      if (typeof RX !== 'undefined') RX.log('error','jarvis','daily report POST failed: '+e.message);
    }
  }
  // Always also offer the JSON for download / manual share
  var blob = new Blob([JSON.stringify(report,null,2)], { type:'application/json' });
  var a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'shop-daily-' + report.date + '.json';
  if (!posted) a.click();
  setTimeout(function(){ URL.revokeObjectURL(a.href); }, 5000); // free blob whether or not it was downloaded
  // Show a quick summary toast/modal
  showModal('📊 Daily Report — ' + report.date,
    '<div style="font-size:13px;line-height:1.6">' +
    (posted ? '<div style="background:#dcfce7;color:#166534;padding:8px;border-radius:8px;margin-bottom:10px"><b>✅ Sent to Jarvis</b></div>' :
              (url ? '<div style="background:#fee2e2;color:#991b1b;padding:8px;border-radius:8px;margin-bottom:10px"><b>⚠️ Could not POST to Jarvis</b> — JSON downloaded instead</div>' :
                     '<div style="background:#fef3c7;color:#92400e;padding:8px;border-radius:8px;margin-bottom:10px"><b>ℹ️ No Jarvis webhook set</b> — JSON downloaded. Add webhook in Settings → Integrations.</div>')) +
    '<table style="width:100%;font-size:13px"><tr><td>Sales</td><td style="text-align:right"><b>' + report.sales.count + ' bills · ' + rs(report.sales.total) + '</b></td></tr>' +
    '<tr><td>Wholesale</td><td style="text-align:right"><b>' + report.wholesale.count + ' · ' + rs(report.wholesale.total) + '</b></td></tr>' +
    '<tr><td>Collections</td><td style="text-align:right"><b>' + rs(report.collections.total) + '</b></td></tr>' +
    '<tr><td>Expenses</td><td style="text-align:right"><b>' + rs(report.expenses.total) + '</b></td></tr>' +
    '<tr><td>Pending writes</td><td style="text-align:right"><b>' + report.dataHealth.pendingWrites + '</b></td></tr>' +
    '<tr><td>Out of stock</td><td style="text-align:right"><b>' + report.inventory.outOfStock + '</b></td></tr>' +
    '<tr><td>Low stock</td><td style="text-align:right"><b>' + report.inventory.lowStock + '</b></td></tr>' +
    '<tr><td>Credit outstanding</td><td style="text-align:right"><b>' + rs(report.creditOutstanding) + '</b></td></tr>' +
    '<tr><td>Agent fixes today</td><td style="text-align:right"><b>' + report.agentFixes.length + '</b></td></tr></table>' +
    (report.agentFixes.length ? '<div style="margin-top:10px"><b>What I did today:</b><ul style="margin-left:20px">' + report.agentFixes.map(function(f){return '<li>'+new Date(f.ts).toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit'})+' · '+f.what+'</li>';}).join('') + '</ul></div>' : '') +
    '<button class="btn mt-2" onclick="rxRunDailyReport()" style="background:#0369a1">↻ Re-send / Re-download</button>' +
    '</div>'
  );
}

// Auto-send daily report at 9pm
function _scheduleDailyReport() {
  var now = new Date();
  var target = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 21, 0, 0);
  if (target <= now) target.setDate(target.getDate()+1);
  var ms = target - now;
  setTimeout(function() {
    try {
      if (state.settings && state.settings.jarvisDaily !== false) rxRunDailyReport();
    } catch(e) { console.warn('[daily] rxRunDailyReport failed:', e.message); }
    _scheduleDailyReport(); // schedule next day
  }, ms);
}
_scheduleDailyReport();

// ============================================================
// PURCHASES MODULE — Supplier Ledger + Bill Photo Scanner + Accounting
// ============================================================

// ---- Section toggling ----
function showPurchaseSection(sec) {
  ['ledger','advances','dayclosing','bankowner','bills','suppliers','incoming'].forEach(function(s) {
    var el = document.getElementById('psec-' + s);
    if (el) el.style.display = s === sec ? '' : 'none';
    var tb = document.getElementById('ptab-' + s);
    if (tb) tb.classList.toggle('active', s === sec);
  });
  if (sec === 'ledger') renderLedger();
  if (sec === 'advances') renderAdvances();
  if (sec === 'dayclosing') renderDayClosing();
  if (sec === 'bankowner') renderBankOwner();
  if (sec === 'bills') renderBillsList();
  if (sec === 'suppliers') renderSuppliersList();
  if (sec === 'incoming') renderIncomingStock();
}

function renderPurchasesTab() {
  populateSupplierFilter();
  renderLedger();
}

// ---- Supplier filter dropdown ----
function populateSupplierFilter() {
  ['pf-supplier','advf-supplier'].forEach(function(id) {
    var sel = document.getElementById(id);
    if (!sel) return;
    var cur = sel.value;
    sel.innerHTML = '<option value="">All Suppliers</option>' +
      (state.suppliers || []).map(function(s) {
        return '<option value="' + s.id + '" ' + (s.id === cur ? 'selected' : '') + '>' + s.name + '</option>';
      }).join('');
  });
}

// ---- Ledger render ----
function renderLedger() {
  var filterSup  = (document.getElementById('pf-supplier') || {}).value || '';
  var filterStat = (document.getElementById('pf-status')   || {}).value || '';
  var purchases  = (state.purchases || []).slice().reverse();

  if (filterSup)  purchases = purchases.filter(function(p){ return p.supplierId === filterSup; });
  if (filterStat) purchases = purchases.filter(function(p){ return p.payStatus === filterStat; });

  // Summary pills per supplier
  var sumDiv = document.getElementById('ledger-summary');
  if (sumDiv) {
    var totals = {};
    (state.purchases || []).forEach(function(p) {
      if (!totals[p.supplierId]) totals[p.supplierId] = { name: p.supplierName, due: 0, total: 0 };
      totals[p.supplierId].total += p.amount;
      totals[p.supplierId].due   += (p.amount - (p.paidAmount || 0));
    });
    sumDiv.innerHTML = Object.values(totals).filter(function(t){ return t.due > 0; }).map(function(t) {
      return '<div style="background:#fef2f2;border:1px solid #fecaca;border-radius:10px;padding:8px 12px;white-space:nowrap;flex-shrink:0;">'
        + '<div style="font-size:11px;color:#991b1b;font-weight:700;">' + t.name + '</div>'
        + '<div style="font-size:15px;font-weight:700;color:#dc2626;">₹' + Math.round(t.due).toLocaleString('en-IN') + ' due</div>'
        + '</div>';
    }).join('') || '<div style="font-size:13px;color:var(--muted);padding:6px;">No outstanding dues 🎉</div>';
  }

  var div = document.getElementById('ledger-list');
  if (!div) return;
  if (!purchases.length) {
    div.innerHTML = '<div class="empty"><div class="icon">📒</div>No purchases yet.<br><small>Tap + Add Purchase to start</small></div>';
    return;
  }
  div.innerHTML = purchases.map(function(p) {
    var due = p.amount - (p.paidAmount || 0);
    var statusCol = p.payStatus === 'paid' ? 'success' : p.payStatus === 'partial' ? 'warn' : 'danger';
    var statusLbl = p.payStatus === 'paid' ? '✓ Paid' : p.payStatus === 'partial' ? '½ Partial' : '⏳ Unpaid';
    var hasPhoto = p.photoDataUrl && !(window.isPlaceholderPhoto && window.isPlaceholderPhoto(p.photoDataUrl));
    return '<div class="sale-row" style="margin-bottom:10px;">'
      + '<div class="sale-head">'
      + '<div><b>' + p.supplierName + '</b><br><small>' + new Date(p.date).toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'2-digit'}) + (p.invoiceNo ? ' · Inv#' + p.invoiceNo : '') + '</small></div>'
      + '<span class="tag ' + statusCol + '">' + statusLbl + '</span>'
      + '</div>'
      + '<div style="font-size:13px;margin-top:4px;color:var(--muted);">' + (p.notes || '—') + '</div>'
      + '<div style="display:flex;justify-content:space-between;align-items:center;margin-top:8px;">'
      + '<div><div style="font-size:16px;font-weight:700;color:var(--text);">₹' + Math.round(p.amount).toLocaleString('en-IN') + '</div>'
      + (due > 0 ? '<div style="font-size:12px;color:#dc2626;font-weight:600;">₹' + Math.round(due).toLocaleString('en-IN') + ' remaining</div>' : '')
      + '</div>'
      + '<div style="display:flex;gap:6px;flex-wrap:wrap;">'
      + (hasPhoto ? '<button class="btn btn-sm btn-outline" onclick="viewBillPhoto(\'' + p.id + '\')">📷 Bill</button>' : '<button class="btn btn-sm btn-outline" onclick="attachBillPhoto(\'' + p.id + '\')" style="color:var(--muted);">📷 Add Photo</button>')
      + (due > 0 ? '<button class="btn btn-sm btn-success" onclick="openAddPayment(\'' + p.id + '\')">+ Pay</button>' : '')
      + '<button class="btn btn-sm" style="background:#f1f5f9;color:var(--text);" onclick="softDeletePurchase(\'' + p.id + '\')">🗑</button>'
      + '</div></div>'
      + ((p.payments && p.payments.length) ? '<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--border);">'
        + '<div style="font-size:11px;font-weight:600;color:var(--muted);margin-bottom:4px;">PAYMENTS</div>'
        + p.payments.map(function(py) {
            return '<div style="display:flex;justify-content:space-between;font-size:12px;padding:3px 0;">'
              + '<span>' + new Date(py.date).toLocaleDateString('en-IN',{day:'2-digit',month:'short'}) + ' · ' + (py.mode||'cash') + (py.ref?' · '+py.ref:'') + '</span>'
              + '<span style="font-weight:700;color:var(--success);">₹' + Math.round(py.amount).toLocaleString('en-IN') + '</span>'
              + '</div>';
          }).join('')
        + '</div>' : '')
      + '</div>';
  }).join('');
}

// ============================================================
// ===== SUPPLIER ADVANCES MODULE =====
// ============================================================

function renderAdvances() {
  populateSupplierFilter();
  var filterSup  = (document.getElementById('advf-supplier') || {}).value || '';
  var filterStat = (document.getElementById('advf-status')   || {}).value || '';
  var advances   = (state.supplierAdvances || []).slice().reverse();

  if (filterSup)  advances = advances.filter(function(a){ return a.supplierId === filterSup; });
  if (filterStat) advances = advances.filter(function(a){ return a.status === filterStat; });

  // Summary grid
  var totalAdvanced = (state.supplierAdvances||[]).reduce(function(s,a){ return s+a.amount; }, 0);
  var totalAdjusted = (state.supplierAdvances||[]).reduce(function(s,a){ return s+(a.adjustedAmount||0); }, 0);
  var totalPending  = totalAdvanced - totalAdjusted;
  var sg = document.getElementById('adv-summary-grid');
  if (sg) sg.innerHTML =
    '<div class="mf-card outflow"><div class="mf-num">₹' + Math.round(totalAdvanced).toLocaleString('en-IN') + '</div><div class="mf-label">Total Advanced</div></div>'
  + '<div class="mf-card ' + (totalPending > 0 ? 'net-neg' : 'inflow') + '"><div class="mf-num">₹' + Math.round(totalPending).toLocaleString('en-IN') + '</div><div class="mf-label">Pending Adjustment</div></div>';

  var div = document.getElementById('advances-list');
  if (!div) return;
  if (!advances.length) {
    div.innerHTML = '<div class="empty"><div class="icon">🤝</div>No advances given yet.<br><small>Tap + Give Advance to record one</small></div>';
    return;
  }
  div.innerHTML = advances.map(function(a) {
    var adjPct = a.amount > 0 ? Math.round(((a.adjustedAmount||0) / a.amount) * 100) : 0;
    var statusClass = a.status === 'adjusted' ? 'adv-adjusted' : a.status === 'partial' ? 'adv-partial' : 'adv-pending';
    var statusLbl   = a.status === 'adjusted' ? '✓ Fully Adjusted' : a.status === 'partial' ? '⟳ Partially Adjusted' : '⏳ Pending';
    var cardClass   = a.status === 'adjusted' ? 'advance-card adjusted' : a.status === 'partial' ? 'advance-card partial' : 'advance-card';
    return '<div class="' + cardClass + '">'
      + '<div class="adv-head">'
      + '<div><div class="adv-name">' + a.supplierName + '</div>'
      + '<div class="adv-meta">' + new Date(a.date).toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'2-digit'}) + (a.ref ? ' · Ref: ' + a.ref : '') + '</div>'
      + (a.notes ? '<div class="adv-meta" style="margin-top:2px;">' + a.notes + '</div>' : '')
      + '</div>'
      + '<span class="adv-badge ' + statusClass + '">' + statusLbl + '</span>'
      + '</div>'
      + '<div style="display:flex;justify-content:space-between;align-items:center;margin-top:8px;">'
      + '<div style="font-size:18px;font-weight:800;color:var(--text);">₹' + Math.round(a.amount).toLocaleString('en-IN')
      + '</div><div style="font-size:12px;color:var(--muted);text-align:right;">'
      + 'Adjusted: ₹' + Math.round(a.adjustedAmount||0).toLocaleString('en-IN') + '<br>'
      + '<span style="color:' + (a.amount-(a.adjustedAmount||0)>0?'#dc2626':'#16a34a') + ';font-weight:700;">'
      + '₹' + Math.round(a.amount-(a.adjustedAmount||0)).toLocaleString('en-IN') + ' left</span>'
      + '</div></div>'
      + '<div class="adv-progress-wrap">'
      + '<div style="display:flex;justify-content:space-between;font-size:10px;color:var(--muted);">'
      + '<span>Adjustment Progress</span><span>' + adjPct + '%</span></div>'
      + '<div class="adv-progress-bar"><div class="adv-progress-fill ' + (adjPct>=100?'done':'') + '" style="width:' + adjPct + '%;"></div></div>'
      + '</div>'
      + (a.status !== 'adjusted' ? '<div class="btn-row mt-1">'
        + '<button class="btn btn-sm btn-success" onclick="openAdjustAdvance(\'' + a.id + '\')">+ Mark Adjusted</button>'
        + '<button class="btn btn-sm btn-outline" onclick="deleteAdvance(\'' + a.id + '\')">🗑</button>'
        + '</div>' : '<div class="mt-1" style="text-align:right;"><button class="btn btn-sm" style="background:#f1f5f9;color:var(--muted);width:auto;" onclick="deleteAdvance(\'' + a.id + '\')">🗑</button></div>')
      + '</div>';
  }).join('');
}

function openAddAdvance() {
  var supOpts = (state.suppliers||[]).map(function(s){ return '<option value="'+s.id+'">'+s.name+'</option>'; }).join('');
  showModal('Give Supplier Advance', ''
    + '<div style="background:#fffbeb;border-radius:8px;padding:10px 12px;margin-bottom:12px;font-size:13px;color:#92400e;">⚠️ Advance = money given <b>before</b> goods arrive. It will be adjusted when supplier bills you.</div>'
    + '<div class="field"><label>Supplier *</label>'
    + '<select class="select" id="adv-sup">'
    + (supOpts || '<option value="">No suppliers — add one first</option>')
    + '<option value="__new__">＋ New supplier…</option>'
    + '</select></div>'
    + '<div id="adv-new-sup-wrap" style="display:none;">'
    + '<div class="field"><label>Supplier Name</label><input class="input" id="adv-new-sup-name" placeholder="Supplier name"></div>'
    + '</div>'
    + '<div class="row2">'
    + '<div class="field"><label>Date</label><input type="date" class="input" id="adv-date" value="'+new Date().toISOString().slice(0,10)+'"></div>'
    + '<div class="field"><label>Amount (₹) *</label><input type="number" class="input" id="adv-amt" placeholder="0"></div>'
    + '</div>'
    + '<div class="row2">'
    + '<div class="field"><label>Payment Mode</label><select class="select" id="adv-mode"><option>cash</option><option>upi</option><option>neft</option><option>cheque</option></select></div>'
    + '<div class="field"><label>Ref / UTR</label><input class="input" id="adv-ref" placeholder="Optional"></div>'
    + '</div>'
    + '<div class="field"><label>Purpose / Notes</label><input class="input" id="adv-notes" placeholder="e.g. Booking for Diwali stock"></div>'
    + '<div class="field"><label>Expected Delivery / Adjustment Date</label><input type="date" class="input" id="adv-expect"></div>'
    + '<button class="btn btn-warn" onclick="saveSupplierAdvance()">💰 Record Advance</button>'
  );
  setTimeout(function() {
    var sel = document.getElementById('adv-sup');
    if (sel) sel.onchange = function() {
      var nw = document.getElementById('adv-new-sup-wrap');
      if (nw) nw.style.display = sel.value === '__new__' ? '' : 'none';
    };
  }, 60);
}

async function saveSupplierAdvance() {
  var supId = (document.getElementById('adv-sup')||{}).value;
  var amt   = parseFloat((document.getElementById('adv-amt')||{}).value) || 0;
  var date  = (document.getElementById('adv-date')||{}).value || new Date().toISOString().slice(0,10);
  var mode  = (document.getElementById('adv-mode')||{}).value;
  var ref   = ((document.getElementById('adv-ref')||{}).value||'').trim();
  var notes = ((document.getElementById('adv-notes')||{}).value||'').trim();
  var expectDate = (document.getElementById('adv-expect')||{}).value || '';
  if (!amt) { toast('Enter amount'); return; }
  var sup;
  if (supId === '__new__') {
    var nName = ((document.getElementById('adv-new-sup-name')||{}).value||'').trim();
    if (!nName) { toast('Enter supplier name'); return; }
    sup = { id: uid(), name: nName, phone:'', city:'', gstin:'', notes:'', totalDue:0 };
    if (!state.suppliers) state.suppliers = [];
    state.suppliers.push(sup);
    supId = sup.id;
  } else {
    sup = (state.suppliers||[]).find(function(s){ return s.id === supId; });
  }
  if (!supId || !sup) { toast('Select supplier'); return; }
  if (!state.supplierAdvances) state.supplierAdvances = [];
  state.supplierAdvances.push({ id:uid(), supplierId:supId, supplierName:sup.name, date:date+'T'+new Date().toTimeString().slice(0,8), amount:amt, adjustedAmount:0, status:'pending', mode:mode, ref:ref, notes:notes, expectDate:expectDate, adjustments:[] });
  await save();
  closeModal();
  populateSupplierFilter();
  renderAdvances();
  toast('Advance recorded ✓');
}

function openAdjustAdvance(advId) {
  var a = (state.supplierAdvances||[]).find(function(x){ return x.id===advId; });
  if (!a) return;
  var remaining = a.amount - (a.adjustedAmount||0);
  showModal('Adjust Advance — ' + a.supplierName, ''
    + '<div style="background:#fef2f2;border-radius:8px;padding:10px 12px;margin-bottom:12px;font-size:13px;">'
    + '<b>' + a.supplierName + '</b> — Advance Given: ₹' + Math.round(a.amount).toLocaleString('en-IN')
    + '<br>Already adjusted: ₹' + Math.round(a.adjustedAmount||0).toLocaleString('en-IN')
    + ' &nbsp;|&nbsp; <span style="color:#dc2626;font-weight:700;">Remaining: ₹' + Math.round(remaining).toLocaleString('en-IN') + '</span></div>'
    + '<div class="field"><label>Amount Being Adjusted Now (₹) — against a bill</label>'
    + '<input type="number" class="input" id="adj-amt" placeholder="' + Math.round(remaining) + '" value="' + Math.round(remaining) + '"></div>'
    + '<div class="field"><label>Related Invoice No (optional)</label><input class="input" id="adj-inv" placeholder="e.g. INV-2024-055"></div>'
    + '<div class="field"><label>Remarks</label><input class="input" id="adj-notes" placeholder="e.g. Adjusted against March delivery"></div>'
    + '<button class="btn btn-success" onclick="saveAdjustAdvance(\'' + advId + '\')">✓ Record Adjustment</button>'
  );
}

async function saveAdjustAdvance(advId) {
  var a = (state.supplierAdvances||[]).find(function(x){ return x.id===advId; });
  if (!a) return;
  var remaining = a.amount - (a.adjustedAmount||0);
  var adjAmt = Math.min(parseFloat(document.getElementById('adj-amt').value)||0, remaining);
  if (!adjAmt) { toast('Enter adjustment amount'); return; }
  var inv   = ((document.getElementById('adj-inv')||{}).value||'').trim();
  var notes = ((document.getElementById('adj-notes')||{}).value||'').trim();
  a.adjustedAmount = (a.adjustedAmount||0) + adjAmt;
  a.status = a.adjustedAmount >= a.amount ? 'adjusted' : 'partial';
  if (!a.adjustments) a.adjustments = [];
  a.adjustments.push({ date:new Date().toISOString(), amount:adjAmt, invoiceNo:inv, notes:notes });
  await save();
  closeModal();
  renderAdvances();
  toast('Adjustment recorded ✓');
}

function deleteAdvance(id) {
  if (!confirm('Delete this advance record?')) return;
  state.supplierAdvances = (state.supplierAdvances||[]).filter(function(a){ return a.id!==id; });
  window._ownerDeleteInProgress = true;
  save().finally(() => { window._ownerDeleteInProgress = false; });
  renderAdvances();
}

// ============================================================
// ===== DAY CLOSING MODULE =====
// ============================================================

function renderDayClosing() {
  var today = new Date().toISOString().slice(0,10);
  var label = document.getElementById('dc-today-label');
  if (label) label.textContent = 'Today — ' + new Date().toLocaleDateString('en-IN', {weekday:'short',day:'2-digit',month:'short',year:'numeric'});

  var existing = (state.dayClosings||[]).find(function(d){ return d.date === today; });
  var statusEl = document.getElementById('dc-today-status');
  var form     = document.getElementById('dc-entry-form');
  var locked   = document.getElementById('dc-locked-view');

  if (existing && existing.locked) {
    if (statusEl) statusEl.innerHTML = '<span class="dc-status-closed">🔒 Closed</span>';
    if (form)   form.style.display = 'none';
    if (locked) { locked.style.display = ''; renderLockedView(existing); }
  } else {
    if (statusEl) statusEl.innerHTML = '<span class="dc-status-open">🟢 Open</span>';
    if (locked) locked.style.display = 'none';
    if (form)   form.style.display = '';
    // Pre-fill if draft exists
    if (existing) {
      var fi = function(id, val) { var e = document.getElementById(id); if(e) e.value = val||''; };
      fi('dc-cash', existing.cash||''); fi('dc-upi', existing.upi||''); fi('dc-neft', existing.neft||'');
      fi('dc-physical-cash', existing.physicalCash||''); fi('dc-notes', existing.notes||'');
      dcUpdateTotal();
    }
  }

  // Render history
  renderDayClosingHistory();
}

function renderLockedView(d) {
  var grid = document.getElementById('dc-locked-grid');
  if (grid) grid.innerHTML =
    '<div class="dc-pay-box"><div class="dpb-icon">💵</div><div class="dpb-label">Cash</div><div class="dpb-val">₹' + Math.round(d.cash||0).toLocaleString('en-IN') + '</div></div>'
  + '<div class="dc-pay-box"><div class="dpb-icon">📱</div><div class="dpb-label">UPI</div><div class="dpb-val">₹' + Math.round(d.upi||0).toLocaleString('en-IN') + '</div></div>'
  + '<div class="dc-pay-box"><div class="dpb-icon">🏦</div><div class="dpb-label">NEFT/RTGS</div><div class="dpb-val">₹' + Math.round(d.neft||0).toLocaleString('en-IN') + '</div></div>';
  var tot = (d.cash||0)+(d.upi||0)+(d.neft||0);
  var totEl = document.getElementById('dc-locked-total');
  if (totEl) totEl.textContent = '₹' + Math.round(tot).toLocaleString('en-IN');
  var mm = document.getElementById('dc-locked-mismatch');
  if (mm) {
    var diff = tot - (d.physicalCash||tot);
    if (d.physicalCash && Math.abs(diff) > 0) {
      mm.innerHTML = '<div class="dc-mismatch-row"><span>⚠️ Cash mismatch: digital ₹'+Math.round(d.cash||0).toLocaleString('en-IN')+' vs physical ₹'+Math.round(d.physicalCash).toLocaleString('en-IN')+'</span><span style="font-weight:700;color:#d97706;">Diff: ₹'+Math.round(Math.abs(d.cash-(d.physicalCash||d.cash))).toLocaleString('en-IN')+'</span></div>';
    } else mm.innerHTML = '';
  }
}

function dcUpdateTotal() {
  var cash  = parseFloat(document.getElementById('dc-cash').value)||0;
  var upi   = parseFloat(document.getElementById('dc-upi').value)||0;
  var neft  = parseFloat(document.getElementById('dc-neft').value)||0;
  var total = cash + upi + neft;
  var dispEl = document.getElementById('dc-total-display');
  if (dispEl) dispEl.textContent = '₹' + Math.round(total).toLocaleString('en-IN');
  // Mismatch check
  var physEl = document.getElementById('dc-physical-cash');
  var phys = physEl ? (parseFloat(physEl.value)||null) : null;
  var mismatchWrap = document.getElementById('dc-mismatch-warn');
  var mismatchAmt  = document.getElementById('dc-mismatch-amt');
  if (phys !== null && Math.abs(cash - phys) > 0) {
    if (mismatchWrap) mismatchWrap.style.display = '';
    if (mismatchAmt) mismatchAmt.textContent = 'Digital cash: ₹'+Math.round(cash).toLocaleString('en-IN')+' vs Physical: ₹'+Math.round(phys).toLocaleString('en-IN')+' → Diff ₹'+Math.round(Math.abs(cash-phys)).toLocaleString('en-IN');
  } else {
    if (mismatchWrap) mismatchWrap.style.display = 'none';
  }
}

async function saveDayClosing(draft) {
  var today = new Date().toISOString().slice(0,10);
  var cash  = parseFloat(document.getElementById('dc-cash').value)||0;
  var upi   = parseFloat(document.getElementById('dc-upi').value)||0;
  var neft  = parseFloat(document.getElementById('dc-neft').value)||0;
  var phys  = parseFloat(document.getElementById('dc-physical-cash').value)||null;
  var notes = ((document.getElementById('dc-notes')||{}).value||'').trim();
  if (!draft && (cash+upi+neft === 0)) { toast('Enter at least one amount before closing'); return; }
  if (!state.dayClosings) state.dayClosings = [];
  var idx = state.dayClosings.findIndex(function(d){ return d.date===today; });
  var record = { id:idx>=0?state.dayClosings[idx].id:uid(), date:today, cash:cash, upi:upi, neft:neft, physicalCash:phys, notes:notes, locked:!draft, closedAt:draft?null:new Date().toISOString() };
  if (idx >= 0) state.dayClosings[idx] = record;
  else state.dayClosings.push(record);
  await save();
  renderDayClosing();
  toast(draft ? 'Draft saved ✓' : '🔒 Day closed ✓');
}

async function saveDayClosingDraft() { saveDayClosing(true); }

async function dcReopen() {
  var today = new Date().toISOString().slice(0,10);
  var idx = (state.dayClosings||[]).findIndex(function(d){ return d.date===today; });
  if (idx>=0) { state.dayClosings[idx].locked = false; }
  await save();
  renderDayClosing();
}

function renderDayClosingHistory() {
  var div = document.getElementById('day-closings-history');
  if (!div) return;
  var records = (state.dayClosings||[]).filter(function(d){ return d.locked; }).slice().reverse();
  if (!records.length) {
    div.innerHTML = '<div style="font-size:13px;color:var(--muted);padding:8px 0;">No closed days yet.</div>';
    return;
  }
  div.innerHTML = records.slice(0,14).map(function(d) {
    var total = (d.cash||0)+(d.upi||0)+(d.neft||0);
    return '<div style="padding:10px 0;border-bottom:1px solid var(--border);">'
      + '<div style="display:flex;justify-content:space-between;align-items:center;">'
      + '<div><div style="font-weight:600;font-size:14px;">'+new Date(d.date).toLocaleDateString('en-IN',{weekday:'short',day:'2-digit',month:'short'})+'</div>'
      + '<div style="font-size:11px;color:var(--muted);margin-top:2px;">Cash ₹'+Math.round(d.cash||0).toLocaleString('en-IN')+' · UPI ₹'+Math.round(d.upi||0).toLocaleString('en-IN')+' · NEFT ₹'+Math.round(d.neft||0).toLocaleString('en-IN')+(d.notes?' · '+d.notes:'')+'</div></div>'
      + '<div style="font-size:18px;font-weight:800;color:var(--success);">₹'+Math.round(total).toLocaleString('en-IN')+'</div>'
      + '</div></div>';
  }).join('');
}

function dcExportSummary() {
  var rows = ['Date,Cash,UPI,NEFT/RTGS,Total,Notes'];
  (state.dayClosings||[]).filter(function(d){return d.locked;}).forEach(function(d) {
    rows.push([d.date, d.cash||0, d.upi||0, d.neft||0, (d.cash||0)+(d.upi||0)+(d.neft||0), d.notes||''].join(','));
  });
  var blob = new Blob([rows.join('\n')], {type:'text/csv'});
  var a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'day-closings.csv';
  a.click();
  setTimeout(function(){ URL.revokeObjectURL(a.href); }, 5000); // free blob after download starts
}

// ============================================================
// ===== BANK & PAYABLES MODULE =====
// ============================================================

function renderBankOwner() {
  // Money flow summary
  var totalSales30 = (state.sales||[]).filter(function(s){ return new Date(s.date) >= new Date(Date.now()-30*86400000); }).reduce(function(a,s){ return a+(s.total||0); }, 0);
  var totalPurchase30 = (state.purchases||[]).filter(function(p){ return new Date(p.date) >= new Date(Date.now()-30*86400000); }).reduce(function(a,p){ return a+(p.amount||0); }, 0);
  var net = totalSales30 - totalPurchase30;
  var fg = document.getElementById('bank-flow-summary');
  if (fg) fg.innerHTML =
    '<div class="mf-card inflow"><div class="mf-num">₹'+Math.round(totalSales30).toLocaleString('en-IN')+'</div><div class="mf-label">Sales (30d)</div></div>'
  + '<div class="mf-card outflow"><div class="mf-num">₹'+Math.round(totalPurchase30).toLocaleString('en-IN')+'</div><div class="mf-label">Purchases (30d)</div></div>'
  + '<div class="mf-card '+(net>=0?'net-pos':'net-neg')+'"><div class="mf-num">₹'+Math.round(Math.abs(net)).toLocaleString('en-IN')+'</div><div class="mf-label">'+(net>=0?'Net Inflow':'Net Outflow')+'</div></div>';

  // Bank accounts
  renderBankAccountsList();
  // Payables 30 days
  renderPayables30();
  // Leakage
  renderLeakageTracker();
}

function renderBankAccountsList() {
  var div = document.getElementById('bank-accounts-list');
  if (!div) return;
  if (!(state.bankAccounts||[]).length) {
    div.innerHTML = '<div style="background:#f8fafc;border-radius:var(--radius);padding:16px;text-align:center;font-size:13px;color:var(--muted);">No bank accounts added yet.<br>Add your accounts to track balances and OD limits.</div>';
    return;
  }
  div.innerHTML = (state.bankAccounts||[]).map(function(b) {
    var used = b.limit > 0 ? ((b.balance||0) / b.limit) * 100 : 0;
    var fillClass = used >= 85 ? 'danger' : used >= 60 ? 'warn' : 'safe';
    var available = b.limit > 0 ? b.limit - (b.balance||0) : null;
    return '<div class="bank-tracker-card">'
      + '<div style="display:flex;justify-content:space-between;align-items:flex-start;">'
      + '<div><div class="bank-name">🏦 '+b.name+'</div>'
      + '<div class="bank-account-no">'+(b.accountNo||'')+(b.ifsc?' · '+b.ifsc:'')+'</div></div>'
      + '<button onclick="editBankAccount(\''+b.id+'\')" style="background:none;border:none;font-size:18px;cursor:pointer;color:var(--muted);">✏️</button>'
      + '</div>'
      + (b.limit > 0 ? '<div class="bank-limit-bar-wrap">'
        + '<div style="display:flex;justify-content:space-between;font-size:11px;color:var(--muted);margin-bottom:4px;">'
        + '<span>OD/CC Limit Used</span><span style="font-weight:700;color:'+(used>=85?'#dc2626':used>=60?'#d97706':'#16a34a')+';">'+Math.round(used)+'%</span></div>'
        + '<div class="bank-limit-bar"><div class="bank-limit-fill '+fillClass+'" style="width:'+Math.min(used,100)+'%;"><span class="bank-limit-pct">'+Math.round(used)+'%</span></div></div>'
        + '</div>' : '')
      + '<div class="bank-stats-row">'
      + '<div class="bstat"><div class="bs-num" style="color:var(--success);">₹'+Math.round(b.balance||0).toLocaleString('en-IN')+'</div><div class="bs-label">Current Balance</div></div>'
      + (b.limit > 0 ? '<div class="bstat"><div class="bs-num" style="color:#1d4ed8;">₹'+Math.round(b.limit).toLocaleString('en-IN')+'</div><div class="bs-label">Total Limit</div></div>'
        + '<div class="bstat"><div class="bs-num" style="color:'+(available>0?'var(--success)':'var(--danger)')+';">₹'+Math.round(Math.abs(available||0)).toLocaleString('en-IN')+'</div><div class="bs-label">'+(available>=0?'Available':'Overlimit')+'</div></div>' : '')
      + '</div>'
      + (b.limit > 0 ? '<div style="margin-top:6px;"><button class="btn btn-sm btn-outline" style="width:auto;" onclick="openUpdateBalance(\''+b.id+'\')">Update Balance</button></div>' : '<div style="margin-top:6px;"><button class="btn btn-sm btn-outline" style="width:auto;" onclick="openUpdateBalance(\''+b.id+'\')">Update Balance</button></div>')
      + '</div>';
  }).join('');
}

function openAddBankAccount(id) {
  var b = id ? (state.bankAccounts||[]).find(function(x){return x.id===id;}) : {};
  b = b || {};
  showModal(id ? 'Edit Bank Account' : 'Add Bank Account', ''
    + '<div class="field"><label>Bank Name *</label><input class="input" id="ba-name" value="'+(b.name||'')+'" placeholder="e.g. SBI, HDFC, ICICI"></div>'
    + '<div class="row2">'
    + '<div class="field"><label>Account No</label><input class="input" id="ba-accno" value="'+(b.accountNo||'')+'" placeholder="Last 4 digits ok"></div>'
    + '<div class="field"><label>IFSC</label><input class="input" id="ba-ifsc" value="'+(b.ifsc||'')+'" placeholder="e.g. SBIN0001234"></div>'
    + '</div>'
    + '<div class="row2">'
    + '<div class="field"><label>Current Balance (₹)</label><input type="number" class="input" id="ba-bal" value="'+(b.balance||0)+'" placeholder="0"></div>'
    + '<div class="field"><label>OD / CC Limit (₹)</label><input type="number" class="input" id="ba-limit" value="'+(b.limit||0)+'" placeholder="0 if no limit"><small style="display:block;margin-top:4px;">Enter ₹1,00,00,000 for 1 Cr limit</small></div>'
    + '</div>'
    + '<div class="field"><label>Notes</label><input class="input" id="ba-notes" value="'+(b.notes||'')+'" placeholder="e.g. Current account, savings, CC"></div>'
    + '<button class="btn" onclick="saveBankAccount(\''+(id||'')+'\')">Save Account</button>'
  );
}

function editBankAccount(id) { openAddBankAccount(id); }

async function saveBankAccount(id) {
  var name = ((document.getElementById('ba-name')||{}).value||'').trim();
  if (!name) { toast('Enter bank name'); return; }
  var obj = { id: id||uid(), name:name, accountNo:((document.getElementById('ba-accno')||{}).value||'').trim(), ifsc:((document.getElementById('ba-ifsc')||{}).value||'').trim(), balance:parseFloat((document.getElementById('ba-bal')||{}).value)||0, limit:parseFloat((document.getElementById('ba-limit')||{}).value)||0, notes:((document.getElementById('ba-notes')||{}).value||'').trim() };
  if (!state.bankAccounts) state.bankAccounts = [];
  if (id) {
    var idx = state.bankAccounts.findIndex(function(b){return b.id===id;});
    if (idx>=0) state.bankAccounts[idx] = obj; else state.bankAccounts.push(obj);
  } else { state.bankAccounts.push(obj); }
  await save(); closeModal(); renderBankAccountsList();
  toast('Bank account saved ✓');
}

function openUpdateBalance(id) {
  var b = (state.bankAccounts||[]).find(function(x){return x.id===id;});
  if (!b) return;
  showModal('Update Balance — '+b.name, ''
    + '<div style="font-size:13px;color:var(--muted);margin-bottom:10px;">Current: ₹'+Math.round(b.balance||0).toLocaleString('en-IN')+(b.limit>0?' / Limit ₹'+Math.round(b.limit).toLocaleString('en-IN'):'')+' </div>'
    + '<div class="field"><label>New Balance (₹)</label><input type="number" class="input" id="ub-bal" value="'+(b.balance||0)+'" placeholder="Enter latest balance from bank app"></div>'
    + '<div class="field"><label>Remarks</label><input class="input" id="ub-notes" placeholder="e.g. Updated from bank statement 15 May"></div>'
    + '<button class="btn btn-success" onclick="saveBalance(\''+id+'\')">Update Balance</button>'
  );
}

async function saveBalance(id) {
  var b = (state.bankAccounts||[]).find(function(x){return x.id===id;});
  if (!b) return;
  b.balance = parseFloat((document.getElementById('ub-bal')||{}).value)||0;
  b.lastUpdated = new Date().toISOString();
  await save(); closeModal(); renderBankAccountsList();
  toast('Balance updated ✓');
}

function renderPayables30() {
  var now = new Date();
  var in30 = new Date(now.getTime() + 30*86400000);
  var items = [];

  // From unpaid/partial purchases
  (state.purchases||[]).forEach(function(p) {
    var due = p.amount - (p.paidAmount||0);
    if (due <= 0) return;
    // All unpaid purchases are "due" — show with inferred due date
    var pDate = new Date(p.date);
    var inferredDue = new Date(pDate.getTime() + 30*86400000); // assume 30-day credit
    items.push({ name:p.supplierName, type:'supplier', amount:due, dueDate:inferredDue, notes:'Inv: '+(p.invoiceNo||'—')+' · '+(p.notes||''), id:p.id });
  });

  // From pending advances (expected delivery)
  (state.supplierAdvances||[]).filter(function(a){ return a.status !== 'adjusted'; }).forEach(function(a) {
    var rem = a.amount - (a.adjustedAmount||0);
    if (rem <= 0) return;
    items.push({ name:a.supplierName+'  (advance adjustment)', type:'advance', amount:rem, dueDate:a.expectDate ? new Date(a.expectDate) : new Date(in30), notes:a.notes||'' });
  });

  // Manual payables
  (state.manualPayables||[]).filter(function(m){ return !m.paid && new Date(m.dueDate) <= in30; }).forEach(function(m) {
    items.push({ name:m.name, type:'manual', amount:m.amount, dueDate:new Date(m.dueDate), notes:m.notes||'', id:m.id });
  });

  items.sort(function(a,b){ return a.dueDate - b.dueDate; });

  var total30 = items.reduce(function(s,i){ return s+i.amount; }, 0);
  var totEl = document.getElementById('payable30-total');
  if (totEl) totEl.textContent = '₹' + Math.round(total30).toLocaleString('en-IN');

  var div = document.getElementById('payables-30-list');
  if (!div) return;
  if (!items.length) {
    div.innerHTML = '<div style="background:#f0fdf4;border-radius:var(--radius);padding:16px;text-align:center;font-size:13px;color:#166534;">🎉 No payables in next 30 days!</div>';
    return;
  }
  div.innerHTML = items.map(function(item) {
    var daysLeft = Math.ceil((item.dueDate - now) / 86400000);
    var urgency = daysLeft <= 5 ? 'urgent' : daysLeft <= 15 ? 'soon' : 'ok';
    var icon = item.type === 'advance' ? '🤝' : item.type === 'manual' ? '📋' : '🛒';
    return '<div class="payable-30-item '+urgency+'">'
      + '<div class="p30-left">'
      + '<div class="p30-name">'+icon+' '+item.name+'</div>'
      + '<div class="p30-meta">'+(item.notes||'')+' · '+(daysLeft<=0?'Overdue':daysLeft===0?'Due today':daysLeft+' days left')+'</div>'
      + '</div>'
      + '<div class="p30-right">'
      + '<div class="p30-amt">₹'+Math.round(item.amount).toLocaleString('en-IN')+'</div>'
      + '<div class="p30-due-date">'+item.dueDate.toLocaleDateString('en-IN',{day:'2-digit',month:'short'})+'</div>'
      + '</div>'
      + '</div>';
  }).join('');
}

function openAddManualPayable() {
  showModal('Add Manual Payable', ''
    + '<div class="field"><label>Payable Name *</label><input class="input" id="mp-name" placeholder="e.g. Shop Rent, EMI, Electricity Bill"></div>'
    + '<div class="row2">'
    + '<div class="field"><label>Amount (₹) *</label><input type="number" class="input" id="mp-amt" placeholder="0"></div>'
    + '<div class="field"><label>Due Date *</label><input type="date" class="input" id="mp-due"></div>'
    + '</div>'
    + '<div class="field"><label>Notes</label><input class="input" id="mp-notes" placeholder="e.g. Quarterly rent payment"></div>'
    + '<button class="btn" onclick="saveManualPayable()">Save</button>'
  );
}

async function saveManualPayable() {
  var name = ((document.getElementById('mp-name')||{}).value||'').trim();
  var amt  = parseFloat((document.getElementById('mp-amt')||{}).value)||0;
  var due  = (document.getElementById('mp-due')||{}).value;
  if (!name||!amt||!due) { toast('Fill all required fields'); return; }
  if (!state.manualPayables) state.manualPayables = [];
  state.manualPayables.push({ id:uid(), name:name, amount:amt, dueDate:due, notes:((document.getElementById('mp-notes')||{}).value||'').trim(), paid:false, createdAt:new Date().toISOString() });
  await save(); closeModal(); renderPayables30();
  toast('Payable added ✓');
}

function renderLeakageTracker() {
  var div = document.getElementById('leakage-tracker-list');
  if (!div) return;
  // Calculate spend categories from purchases and expenses
  var catTotals = {};
  var now = new Date();
  var m30 = new Date(now.getTime() - 30*86400000);

  (state.purchases||[]).filter(function(p){ return new Date(p.date)>=m30; }).forEach(function(p) {
    var cat = p.supplierName || 'Supplier';
    catTotals[cat] = (catTotals[cat]||0) + p.amount;
  });
  (state.expenses||[]).filter(function(e){ return new Date(e.date||e.createdAt)>=m30; }).forEach(function(e) {
    var cat = e.category || e.type || 'Other';
    catTotals[cat] = (catTotals[cat]||0) + (e.amount||0);
  });

  var total = Object.values(catTotals).reduce(function(a,v){ return a+v; }, 0);
  if (!total) {
    div.innerHTML = '<div style="font-size:13px;color:var(--muted);padding:8px 0;">No expense data in last 30 days yet.</div>';
    return;
  }
  var sorted = Object.entries(catTotals).sort(function(a,b){ return b[1]-a[1]; });
  div.innerHTML = '<div class="card" style="padding:12px;">'
    + sorted.slice(0,8).map(function(kv) {
      var pct = Math.round((kv[1]/total)*100);
      var barColor = pct>30?'#dc2626':pct>20?'#f59e0b':'#0f766e';
      return '<div class="leakage-row">'
        + '<div style="flex:1;min-width:0;"><div style="font-size:13px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">'+kv[0]+'</div>'
        + '<div style="height:5px;background:var(--border);border-radius:3px;margin-top:4px;overflow:hidden;"><div style="height:100%;width:'+pct+'%;background:'+barColor+';border-radius:3px;transition:width .4s;"></div></div>'
        + '</div>'
        + '<div style="text-align:right;padding-left:10px;flex-shrink:0;">'
        + '<div style="font-weight:700;font-size:14px;">₹'+Math.round(kv[1]).toLocaleString('en-IN')+'</div>'
        + '<div style="font-size:11px;color:var(--muted);">'+pct+'%</div>'
        + '</div></div>';
    }).join('')
    + '<div style="padding-top:8px;margin-top:4px;border-top:2px solid var(--border);display:flex;justify-content:space-between;font-weight:700;">'
    + '<span>Total Outflow (30d)</span><span style="color:var(--danger);">₹'+Math.round(total).toLocaleString('en-IN')+'</span></div>'
    + '</div>';
}

// ---- Add Purchase modal ----
function openAddPurchase(prefill) {
  prefill = prefill || {};
  var suppliers = state.suppliers || [];
  var supOpts = suppliers.map(function(s) {
    return '<option value="' + s.id + '">' + s.name + '</option>';
  }).join('');

  // Try to match a prefill supplierName to an existing supplier
  var matchedSupId = '';
  if (prefill.supplierName) {
    var sn = (prefill.supplierName||'').toLowerCase();
    var sf = suppliers.find(function(s){ return s.name.toLowerCase().includes(sn) || sn.includes(s.name.toLowerCase()); });
    if (sf) matchedSupId = sf.id;
  }

  showModal('Add Purchase Entry', ''
    + '<div class="field"><label>Supplier</label>'
    + '<select class="select" id="ap-sup" onchange="apToggleNewSupplier()">'
    + (supOpts ? supOpts : '')
    + '<option value="__new__">＋ New supplier…</option>'
    + '</select></div>'
    + '<div id="ap-new-sup-wrap" style="display:none;">'
    + '<div class="field"><label>New Supplier Name *</label><input class="input" id="ap-new-sup-name" placeholder="e.g. Goyal Auto Parts" value="' + (prefill.supplierName||'') + '"></div>'
    + '</div>'
    + '<div class="row2">'
    + '<div class="field"><label>Date</label><input type="date" class="input" id="ap-date" value="' + (prefill.date || new Date().toISOString().slice(0,10)) + '"></div>'
    + '<div class="field"><label>Invoice No</label><input class="input" id="ap-inv" placeholder="Optional" value="' + (prefill.invoiceNo||'') + '"></div>'
    + '</div>'
    + '<div class="field"><label>Total Amount (₹)</label><input type="number" class="input" id="ap-amt" placeholder="0" value="' + (prefill.amount||'') + '"></div>'
    + '<div class="field"><label>Items / Notes</label><textarea class="input" id="ap-notes" rows="3" placeholder="e.g. Brake pads x10, Filter x5...">' + (prefill.notes||'') + '</textarea></div>'
    + '<div class="row2">'
    + '<div class="field"><label>Amount Paid Now (₹)</label><input type="number" class="input" id="ap-paid" placeholder="0 if credit"></div>'
    + '<div class="field"><label>Payment Mode</label><select class="select" id="ap-mode"><option>cash</option><option>upi</option><option>cheque</option><option>neft</option></select></div>'
    + '</div>'
    + '<div class="field"><label>Payment Ref / UTR</label><input class="input" id="ap-ref" placeholder="Optional"></div>'
    + '<div class="field"><label>📷 Bill Photo <span style="color:var(--muted);font-weight:400;font-size:12px;">(optional)</span></label>'    + '<div style="display:flex;align-items:center;gap:10px;margin-top:6px;">'    + '<input type="file" id="ap-photo-input" accept="image/*" capture="environment" style="display:none;" onchange="apHandlePhoto(this)">'    + '<button type="button" class="btn btn-outline btn-sm" onclick="document.getElementById(\'ap-photo-input\').click()" style="white-space:nowrap;">📷 Attach Photo</button>'    + '<span id="ap-photo-name" style="font-size:12px;color:var(--muted);"></span>'    + '</div>'    + '<div id="ap-photo-preview-wrap" style="display:none;margin-top:8px;"><img id="ap-photo-preview" style="width:80px;height:80px;object-fit:cover;border-radius:8px;border:1px solid var(--border);"></div>'    + '</div>'    + '<input type="hidden" id="ap-photo-data" value="' + (prefill.photoDataUrl||'') + '">'    + '<button class="btn" onclick="savePurchase()">Save Purchase</button>'
  );

  // Set the matched supplier after modal renders
  setTimeout(function() {
    var sel = document.getElementById('ap-sup');
    if (!sel) return;
    if (matchedSupId) {
      sel.value = matchedSupId;
    } else if (!suppliers.length || prefill.supplierName) {
      // No suppliers yet, or supplier not found — show new supplier input
      sel.value = '__new__';
      apToggleNewSupplier();
    }
  }, 30);
}

function apToggleNewSupplier() {
  var sel = document.getElementById('ap-sup');
  var wrap = document.getElementById('ap-new-sup-wrap');
  if (!sel || !wrap) return;
  wrap.style.display = sel.value === '__new__' ? '' : 'none';
}

function apHandlePhoto(input) {
  if (!input.files || !input.files[0]) return;
  var file = input.files[0];
  var nameEl = document.getElementById('ap-photo-name');
  if (nameEl) nameEl.textContent = file.name;
  var reader = new FileReader();
  reader.onload = function(e) {
    var dataUrl = e.target.result;
    var hidden = document.getElementById('ap-photo-data');
    if (hidden) hidden.value = dataUrl;
    var preview = document.getElementById('ap-photo-preview');
    var wrap = document.getElementById('ap-photo-preview-wrap');
    if (preview) { preview.src = dataUrl; }
    if (wrap) wrap.style.display = '';
  };
  reader.readAsDataURL(file);
}

async function savePurchase() {
  var photoDataUrl = (document.getElementById('ap-photo-data') || {}).value || null;
  var supSel  = document.getElementById('ap-sup');
  var supId   = supSel ? supSel.value : '';
  var sup     = null;

  // Handle inline new supplier
  if (supId === '__new__') {
    var newSupName = (document.getElementById('ap-new-sup-name') || {}).value || '';
    newSupName = newSupName.trim();
    if (!newSupName) { toast('Enter supplier name'); return; }
    // Check if already exists
    sup = (state.suppliers||[]).find(function(s){ return s.name.toLowerCase() === newSupName.toLowerCase(); });
    if (!sup) {
      sup = { id: uid(), name: newSupName, phone: '', city: '', gstin: '', notes: '', totalDue: 0 };
      if (!state.suppliers) state.suppliers = [];
      state.suppliers.push(sup);
      populateSupplierFilter();
    }
    supId = sup.id;
  } else {
    sup = (state.suppliers || []).find(function(s){ return s.id === supId; });
  }

  if (!supId) { toast('Select or enter a supplier'); return; }

  var amt     = parseFloat(document.getElementById('ap-amt').value) || 0;
  var paid    = parseFloat(document.getElementById('ap-paid').value) || 0;
  var mode    = document.getElementById('ap-mode').value;
  var ref     = document.getElementById('ap-ref').value.trim();
  var notes   = document.getElementById('ap-notes').value.trim();
  var invNo   = document.getElementById('ap-inv').value.trim();
  var date    = document.getElementById('ap-date').value || new Date().toISOString().slice(0,10);
  if (!amt)   { toast('Enter amount'); return; }
  paid = Math.min(paid, amt);
  var status = paid >= amt ? 'paid' : paid > 0 ? 'partial' : 'unpaid';
  var p = {
    id: uid(), date: date + 'T' + new Date().toTimeString().slice(0,8),
    supplierId: supId, supplierName: sup ? sup.name : supId,
    amount: amt, paidAmount: paid, payStatus: status,
    invoiceNo: invNo, notes: notes,
    photoDataUrl: photoDataUrl || null,
    payments: paid > 0 ? [{ date: new Date().toISOString(), amount: paid, mode: mode, ref: ref }] : []
  };
  if (!state.purchases) state.purchases = [];
  state.purchases.push(p);
  // Update supplier totalDue
  if (sup) { sup.totalDue = (sup.totalDue || 0) + (amt - paid); }
  await save();
  closeModal();
  populateSupplierFilter();
  renderLedger();
  renderBillsList();
  toast('Purchase saved ✓');
  // Upload bill photo to Firebase Storage in background
  if (p.photoDataUrl && p.photoDataUrl.startsWith('data:') && window.firebaseUploadBillPhoto) {
    var _pid = p.id, _durl = p.photoDataUrl;
    window.firebaseUploadBillPhoto(_pid, _durl)
      .then(function(url) {
        var p2 = (state.purchases||[]).find(function(x){ return x.id === _pid; });
        if (p2) { p2.photoDataUrl = url; save(); renderBillsList(); renderLedger(); }
        toast('✅ Bill photo saved to cloud');
      })
      .catch(function(e) { console.warn('Bill photo upload failed:', e.message); });
  }
}

// ---- Add payment to existing purchase ----
function openAddPayment(purchaseId) {
  var p = (state.purchases || []).find(function(x){ return x.id === purchaseId; });
  if (!p) return;
  var due = p.amount - (p.paidAmount || 0);
  showModal('Record Payment — ' + p.supplierName, ''
    + '<div style="background:#fef2f2;border-radius:8px;padding:10px 12px;margin-bottom:12px;font-size:13px;">'
    + '<b>' + p.supplierName + '</b> — Total: ₹' + Math.round(p.amount).toLocaleString('en-IN')
    + '<br>Paid so far: ₹' + Math.round(p.paidAmount||0).toLocaleString('en-IN')
    + ' &nbsp;|&nbsp; <span style="color:#dc2626;font-weight:700;">Due: ₹' + Math.round(due).toLocaleString('en-IN') + '</span></div>'
    + '<div class="field"><label>Amount Paying Now (₹)</label><input type="number" class="input" id="pay-amt" placeholder="' + Math.round(due) + '" value="' + Math.round(due) + '"></div>'
    + '<div class="row2">'
    + '<div class="field"><label>Mode</label><select class="select" id="pay-mode"><option>cash</option><option>upi</option><option>cheque</option><option>neft</option></select></div>'
    + '<div class="field"><label>Ref / UTR</label><input class="input" id="pay-ref" placeholder="Optional"></div>'
    + '</div>'
    + '<button class="btn btn-success" onclick="recordPayment(\'' + purchaseId + '\')">Record Payment</button>'
  );
}

function recordPayment(purchaseId) {
  var p = (state.purchases || []).find(function(x){ return x.id === purchaseId; });
  if (!p) return;
  var amt  = parseFloat(document.getElementById('pay-amt').value) || 0;
  var mode = document.getElementById('pay-mode').value;
  var ref  = document.getElementById('pay-ref').value.trim();
  if (!amt) { toast('Enter amount'); return; }
  var due = p.amount - (p.paidAmount || 0);
  amt = Math.min(amt, due);
  p.paidAmount = (p.paidAmount || 0) + amt;
  p.payStatus  = p.paidAmount >= p.amount ? 'paid' : 'partial';
  if (!p.payments) p.payments = [];
  p.payments.push({ date: new Date().toISOString(), amount: amt, mode: mode, ref: ref });
  // Update supplier totalDue
  var sup = (state.suppliers || []).find(function(s){ return s.id === p.supplierId; });
  if (sup) { sup.totalDue = Math.max(0, (sup.totalDue || 0) - amt); }
  save();
  closeModal();
  renderLedger();
  toast('₹' + Math.round(amt).toLocaleString('en-IN') + ' recorded ✓');
}

// Soft-delete — moves the purchase bill to the Recycle Bin instead of erasing it.
// Nothing is tombstoned or removed from cloud storage yet, so it can still be restored.
function softDeletePurchase(id) {
  var p = (state.purchases||[]).find(function(x){ return x.id===id; });
  if (!p) return;
  var label = (p.supplierName||'Supplier') + ' · ₹' + Math.round(p.amount).toLocaleString('en-IN') + (p.invoiceNo?' · Inv#'+p.invoiceNo:'');
  if (!confirm('Delete this purchase bill?\n\n' + label + '\n\nIt will be moved to the Recycle Bin for 30 days — you can restore it if this was a mistake.')) return;
  // Reverse the unpaid portion from supplier totalDue — restored if this entry is undone
  var due = (p.amount||0) - (p.paidAmount||0);
  if (due > 0) {
    var sup = (state.suppliers||[]).find(function(s){ return s.id===p.supplierId; });
    if (sup) sup.totalDue = Math.max(0, (sup.totalDue||0) - due);
  }
  pushToRecycleBin('purchase', p, label);
  state.purchases = (state.purchases || []).filter(function(x){ return x.id !== id; });
  try { recomputeTotalDue(); } catch(e) { console.warn('[totalDue] recomputeTotalDue failed:', e.message); }
  save();
  logActivity && logActivity('Purchase bill moved to recycle bin: ' + label, 'warn');
  renderLedger();
  renderBillsList && renderBillsList();
  renderSuppliersList && renderSuppliersList();
  if (typeof _supDetailId !== 'undefined' && _supDetailId) renderSupplierDetail && renderSupplierDetail();
  toast('🗑 Moved to Recycle Bin');
}

// Puts a purchase back from the Recycle Bin and re-applies its supplier due
function restorePurchaseFromBin(p) {
  if (!state.purchases) state.purchases = [];
  state.purchases.push(p);
  var due = (p.amount||0) - (p.paidAmount||0);
  if (due > 0) {
    var sup = (state.suppliers||[]).find(function(s){ return s.id===p.supplierId; });
    if (sup) sup.totalDue = (sup.totalDue||0) + due;
  }
  try { recomputeTotalDue(); } catch(e) {}
  renderLedger();
  renderBillsList && renderBillsList();
  renderSuppliersList && renderSuppliersList();
}

// Irreversible — called only when a Recycle Bin entry is emptied (by hand or after 30 days).
// Tombstones the id (prevents cloud sync from ever resurrecting it) and drops its cloud photo.
function purgePurchaseForever(p) {
  _purchaseDeletedIds.add(p.id);
  try { localStorage.setItem('_deletedPurchaseIds', JSON.stringify([..._purchaseDeletedIds])); } catch(e) { /* delete tombstones persist — survivable if storage full */ }
  if (p.photoDataUrl && p.photoDataUrl.startsWith('https://') && window.firebaseDeleteBillPhoto) {
    window.firebaseDeleteBillPhoto(p.id).catch(function(){});
  }
  window._ownerDeleteInProgress = true;
  save().finally(() => {
    setTimeout(() => { window._ownerDeleteInProgress = false; }, 10000);
  });
  // Immediately push post-delete state to Firestore — bypasses the 1.2s debounce
  // so Firestore reflects the deletion before any echo snapshot can bounce it back
  if (window.firebasePush) {
    clearTimeout(window._fbSaveTimer);
    window.firebasePush(state);
  }
}

// ---- Suppliers CRUD ----
function renderSuppliersList() {
  // Refresh the global supplier datalist whenever this list is rendered
  refreshSupplierDatalist && refreshSupplierDatalist();

  var query = ((document.getElementById('sup-search')||{}).value||'').trim().toLowerCase();
  var suppliers = (state.suppliers||[]).filter(function(s){
    if (!query) return true;
    return (s.name||'').toLowerCase().includes(query) || (s.city||'').toLowerCase().includes(query);
  });

  // Stats bar
  var totalDue = (state.suppliers||[]).reduce(function(a,s){ return a+(s.totalDue||0); }, 0);
  var totalVol = (state.purchases||[]).reduce(function(a,p){ return a+(p.amount||0); }, 0);
  var totalAdv = (state.supplierAdvances||[]).filter(function(a){ return a.status!=='adjusted'; }).reduce(function(a,x){ return a+(x.amount||0)-(x.adjustedAmount||0); }, 0);
  var sb = document.getElementById('sup-stats-bar');
  if (sb) sb.innerHTML = ''
    + '<div class="sup-stat danger"><div class="sn">₹'+Math.round(totalDue).toLocaleString('en-IN')+'</div><div class="sl">Total Due</div></div>'
    + '<div class="sup-stat warn"><div class="sn">₹'+Math.round(totalAdv).toLocaleString('en-IN')+'</div><div class="sl">Advances Given</div></div>'
    + '<div class="sup-stat"><div class="sn">₹'+Math.round(totalVol).toLocaleString('en-IN')+'</div><div class="sl">All-time Purchases</div></div>'
    + '<div class="sup-stat parts"><div class="sn">'+(state.suppliers||[]).length+'</div><div class="sl">Suppliers</div></div>';

  var div = document.getElementById('suppliers-list');
  if (!div) return;
  var detail = document.getElementById('sup-detail-panel');

  if (!suppliers.length) {
    div.innerHTML = '<div class="empty"><div class="icon">🏢</div>'+(query?'No suppliers match your search.':'No suppliers yet — tap + Add Supplier to get started.')+'</div>';
    return;
  }

  div.innerHTML = suppliers.map(function(s) {
    var partCount = (state.parts||[]).filter(function(p){ return p.supplierName && p.supplierName.trim().toLowerCase()===s.name.trim().toLowerCase(); }).length;
    var invTotal  = (state.purchases||[]).filter(function(p){ return p.supplierId===s.id; }).reduce(function(a,p){return a+p.amount;},0);
    var pendAdv   = (state.supplierAdvances||[]).filter(function(a){ return a.supplierId===s.id && a.status!=='adjusted'; }).reduce(function(a,x){ return a+(x.amount||0)-(x.adjustedAmount||0); }, 0);
    var initials  = s.name.trim().split(' ').map(function(w){return w[0]||'';}).slice(0,2).join('').toUpperCase();
    return '<div class="sup-card" id="supcard-'+s.id+'" onclick="openSupplierDetail(\''+s.id+'\')">'
      + '<div class="sup-card-head">'
        + '<div class="sup-avatar">'+initials+'</div>'
        + '<div style="flex:1;min-width:0;">'
          + '<div class="sup-name">'+s.name+'</div>'
          + '<div class="sup-meta">'+(s.phone||'—')+(s.city?' · '+s.city:'')+(s.gstin?' · GSTIN: '+s.gstin:'')+'</div>'
        + '</div>'
        + '<div style="text-align:right;flex-shrink:0;">'
          + (s.totalDue>0 ? '<div style="font-size:15px;font-weight:700;color:#dc2626;">₹'+Math.round(s.totalDue).toLocaleString('en-IN')+'</div><div style="font-size:10px;color:#dc2626;">due</div>' : '<div style="font-size:12px;color:var(--success);font-weight:600;">✓ Clear</div>')
        + '</div>'
      + '</div>'
      + '<div class="sup-badges">'
        + (partCount>0 ? '<span class="sup-badge parts">📦 '+partCount+' part'+(partCount>1?'s':'')+'</span>' : '')
        + (invTotal>0  ? '<span class="sup-badge vol">🛒 ₹'+Math.round(invTotal).toLocaleString('en-IN')+' purchased</span>' : '')
        + (pendAdv>0   ? '<span class="sup-badge adv">🤝 ₹'+Math.round(pendAdv).toLocaleString('en-IN')+' advance</span>' : '')
        + (s.notes     ? '<span class="sup-badge">📝 '+s.notes+'</span>' : '')
      + '</div>'
    + '</div>';
  }).join('');
}

// ── Supplier detail panel ───────────────────────────────────
var _supDetailId = null;
var _supDetailTab = 'overview';

function openSupplierDetail(supId) {
  _supDetailId = supId;
  _supDetailTab = 'overview';
  // Mark active card
  document.querySelectorAll('.sup-card').forEach(function(c){ c.classList.remove('active'); });
  var card = document.getElementById('supcard-'+supId);
  if (card) { card.classList.add('active'); card.scrollIntoView({behavior:'smooth',block:'nearest'}); }
  renderSupplierDetail();
  var panel = document.getElementById('sup-detail-panel');
  if (panel) { panel.style.display=''; panel.scrollIntoView({behavior:'smooth',block:'start'}); }
}

function closeSupplierDetail() {
  _supDetailId = null;
  var panel = document.getElementById('sup-detail-panel');
  if (panel) panel.style.display = 'none';
  document.querySelectorAll('.sup-card').forEach(function(c){ c.classList.remove('active'); });
}

function switchSupDetailTab(tab) {
  _supDetailTab = tab;
  document.querySelectorAll('.sup-dtab').forEach(function(t){ t.classList.toggle('active', t.dataset.tab===tab); });
  document.querySelectorAll('.sup-dtab-body').forEach(function(b){ b.style.display = b.dataset.tab===tab ? '' : 'none'; });
  if (tab==='ai') renderSupAI(_supDetailId);
}

function renderSupplierDetail() {
  var panel = document.getElementById('sup-detail-panel');
  if (!panel || !_supDetailId) return;
  var s = (state.suppliers||[]).find(function(x){ return x.id===_supDetailId; });
  if (!s) return;

  var parts     = (state.parts||[]).filter(function(p){ return p.supplierName && p.supplierName.trim().toLowerCase()===s.name.trim().toLowerCase(); });
  var purchases = (state.purchases||[]).filter(function(p){ return p.supplierId===s.id; }).slice().reverse();
  var advances  = (state.supplierAdvances||[]).filter(function(a){ return a.supplierId===s.id; }).slice().reverse();

  var totalPurchased = purchases.reduce(function(a,p){ return a+p.amount; }, 0);
  var totalPaid      = purchases.reduce(function(a,p){ return a+(p.paidAmount||0); }, 0);
  var totalDue       = totalPurchased - totalPaid;
  var pendAdv        = advances.filter(function(a){ return a.status!=='adjusted'; }).reduce(function(a,x){ return a+(x.amount||0)-(x.adjustedAmount||0); }, 0);
  var invCount       = purchases.length;
  var unpaidCount    = purchases.filter(function(p){ return p.payStatus==='unpaid'||p.payStatus==='partial'; }).length;

  // Compute last order date
  var sortedP = purchases.slice().sort(function(a,b){ return b.date.localeCompare(a.date); });
  var lastOrder = sortedP.length ? new Date(sortedP[0].date).toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'2-digit'}) : '—';

  var initials = s.name.trim().split(' ').map(function(w){return w[0]||'';}).slice(0,2).join('').toUpperCase();

  // Invoices HTML
  var invHTML = purchases.length ? purchases.map(function(p){
    var due = p.amount - (p.paidAmount||0);
    var statusCls = p.payStatus==='paid'?'paid':p.payStatus==='partial'?'partial':'unpaid';
    var statusLbl = p.payStatus==='paid'?'✓ Paid':p.payStatus==='partial'?'½ Partial':'⏳ Unpaid';
    return '<div class="sup-inv-row">'
      + '<div class="sup-inv-top">'
        + '<div>'
          + '<div class="sup-inv-name">'+(p.invoiceNo?'Inv #'+p.invoiceNo : 'Purchase')+'</div>'
          + '<div class="sup-inv-meta">'+new Date(p.date).toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'2-digit'})+(p.notes?' · '+p.notes:'')+'</div>'
        + '</div>'
        + '<div style="text-align:right;">'
          + '<div class="sup-inv-amt">₹'+Math.round(p.amount).toLocaleString('en-IN')+'</div>'
          + (due>0 ? '<div style="font-size:11px;color:#dc2626;font-weight:600;">Due: ₹'+Math.round(due).toLocaleString('en-IN')+'</div>' : '')
        + '</div>'
      + '</div>'
      + '<div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:6px;margin-top:4px;">'
        + '<span class="sup-inv-status '+statusCls+'">'+statusLbl+'</span>'
        + (due>0 ? '<button class="btn btn-sm btn-success" style="padding:5px 10px;font-size:11px;" onclick="supRecordPayment(\''+p.id+'\')">💰 Pay</button>' : '')
      + '</div>'
    + '</div>';
  }).join('') : '<div style="text-align:center;padding:20px;color:var(--muted);font-size:13px;">No invoices yet for this supplier.<br><button class="btn btn-sm btn-ai" style="margin-top:10px;" onclick="openAddPurchaseForSupplier(\''+s.id+'\')">+ Add Invoice</button></div>';

  // Advances HTML
  var advHTML = advances.length ? advances.map(function(a){
    var remaining = (a.amount||0) - (a.adjustedAmount||0);
    var statusLbl = a.status==='adjusted'?'✓ Adjusted':a.status==='partial'?'½ Partial':'⏳ Pending';
    var statusCls = a.status==='adjusted'?'paid':a.status==='partial'?'partial':'unpaid';
    return '<div class="sup-inv-row">'
      + '<div class="sup-inv-top">'
        + '<div>'
          + '<div class="sup-inv-name">Advance · '+a.mode+'</div>'
          + '<div class="sup-inv-meta">'+new Date(a.date).toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'2-digit'})+(a.ref?' · Ref: '+a.ref:'')+(a.notes?' · '+a.notes:'')+'</div>'
        + '</div>'
        + '<div style="text-align:right;">'
          + '<div class="sup-inv-amt">₹'+Math.round(a.amount).toLocaleString('en-IN')+'</div>'
          + (remaining>0 && a.status!=='adjusted' ? '<div style="font-size:11px;color:#d97706;font-weight:600;">Bal: ₹'+Math.round(remaining).toLocaleString('en-IN')+'</div>' : '')
        + '</div>'
      + '</div>'
      + '<div style="margin-top:4px;"><span class="sup-inv-status '+statusCls+'">'+statusLbl+'</span></div>'
    + '</div>';
  }).join('') : '<div style="text-align:center;padding:20px;color:var(--muted);font-size:13px;">No advances recorded.<br><button class="btn btn-sm btn-warn" style="margin-top:10px;" onclick="openAddAdvanceForSupplier(\''+s.id+'\')">+ Give Advance</button></div>';

  // Parts chips HTML
  var partsHTML = parts.length ? '<div class="parts-chip-list">' + parts.map(function(p){
    return '<span class="parts-chip" onclick="openPartDetail(\''+p.id+'\')" style="cursor:pointer;">'+p.partNo+' — '+p.name+'</span>';
  }).join('') + '</div>' : '<div style="font-size:13px;color:var(--muted);margin-top:8px;">No parts linked yet. Set supplier name in any part\'s pricing to link them here.</div>';

  panel.innerHTML = '<div class="sup-detail">'
    // Header
    + '<div class="sup-detail-hdr">'
      + '<div class="sup-avatar" style="width:44px;height:44px;font-size:18px;">'+initials+'</div>'
      + '<div style="flex:1;">'
        + '<h3>'+s.name+'</h3>'
        + '<div style="font-size:12px;opacity:.85;">'+(s.phone||'—')+(s.city?' · '+s.city:'')+'</div>'
      + '</div>'
      + '<button class="btn btn-sm" style="background:rgba(255,255,255,.2);color:white;border:none;width:auto;padding:6px 10px;font-size:12px;" onclick="openAddSupplier(\''+s.id+'\')">✏️ Edit</button>'
      + '<button class="sup-detail-close" onclick="closeSupplierDetail()">✕</button>'
    + '</div>'
    // Stats row
    + '<div style="padding:12px 14px 0;">'
      + '<div class="sup-stat-row">'
        + '<div class="sup-stat '+(totalDue>0?'danger':'success')+'"><div class="sn">₹'+Math.round(totalDue).toLocaleString('en-IN')+'</div><div class="sl">Due</div></div>'
        + '<div class="sup-stat"><div class="sn">₹'+Math.round(totalPurchased).toLocaleString('en-IN')+'</div><div class="sl">Purchased</div></div>'
        + '<div class="sup-stat warn"><div class="sn">₹'+Math.round(pendAdv).toLocaleString('en-IN')+'</div><div class="sl">Advance</div></div>'
        + '<div class="sup-stat parts"><div class="sn">'+parts.length+'</div><div class="sl">Parts</div></div>'
        + '<div class="sup-stat"><div class="sn">'+lastOrder+'</div><div class="sl">Last Order</div></div>'
      + '</div>'
    + '</div>'
    // Sub-tabs
    + '<div class="sup-dtabs">'
      + '<button class="sup-dtab active" data-tab="overview" onclick="switchSupDetailTab(\'overview\')">Overview</button>'
      + '<button class="sup-dtab" data-tab="invoices" onclick="switchSupDetailTab(\'invoices\')">Invoices ('+invCount+')</button>'
      + '<button class="sup-dtab" data-tab="advances" onclick="switchSupDetailTab(\'advances\')">Advances ('+advances.length+')</button>'
      + '<button class="sup-dtab" data-tab="ai" onclick="switchSupDetailTab(\'ai\')">🤖 AI Insights</button>'
    + '</div>'
    // Tab bodies
    + '<div class="sup-dbody">'
      // Overview tab
      + '<div class="sup-dtab-body" data-tab="overview">'
        + '<div style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap;">'
          + '<button class="btn btn-sm btn-ai" style="flex:1;" onclick="openAddPurchaseForSupplier(\''+s.id+'\')">+ Add Invoice</button>'
          + '<button class="btn btn-sm btn-warn" style="flex:1;" onclick="openAddAdvanceForSupplier(\''+s.id+'\')">+ Give Advance</button>'
          + (s.phone ? '<a class="btn btn-sm btn-success" style="flex:1;text-decoration:none;text-align:center;" href="https://wa.me/'+s.phone+'" target="_blank">📲 WhatsApp</a>' : '')
        + '</div>'
        + (unpaidCount>0 ? '<div style="background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:10px 12px;margin-bottom:10px;font-size:13px;color:#dc2626;font-weight:600;">⚠️ '+unpaidCount+' unpaid invoice'+(unpaidCount>1?'s':'')+' totalling ₹'+Math.round(totalDue).toLocaleString('en-IN')+' due</div>' : '')
        + '<div style="font-size:12px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px;">Parts We Order From Them</div>'
        + partsHTML
        + (s.notes ? '<div style="margin-top:12px;font-size:13px;color:var(--muted);background:var(--bg);padding:10px;border-radius:8px;">📝 '+s.notes+'</div>' : '')
      + '</div>'
      // Invoices tab
      + '<div class="sup-dtab-body" data-tab="invoices" style="display:none;">'
        + '<button class="btn btn-sm btn-ai" style="margin-bottom:12px;" onclick="openAddPurchaseForSupplier(\''+s.id+'\')">+ Add Invoice</button>'
        + invHTML
      + '</div>'
      // Advances tab
      + '<div class="sup-dtab-body" data-tab="advances" style="display:none;">'
        + '<button class="btn btn-sm btn-warn" style="margin-bottom:12px;" onclick="openAddAdvanceForSupplier(\''+s.id+'\')">+ Give Advance</button>'
        + advHTML
      + '</div>'
      // AI tab
      + '<div class="sup-dtab-body" data-tab="ai" style="display:none;">'
        + '<div id="sup-ai-body-'+s.id+'"><div class="ai-thinking">🤖 Tap AI Insights to analyse ordering patterns…</div></div>'
      + '</div>'
    + '</div>'
  + '</div>';
}

// ── Record payment against an invoice ─────────────────────
function supRecordPayment(purchaseId) {
  var p = (state.purchases||[]).find(function(x){ return x.id===purchaseId; });
  if (!p) return;
  var due = p.amount - (p.paidAmount||0);
  showModal('Record Payment — '+(p.invoiceNo?'Inv #'+p.invoiceNo:'Purchase'),
    '<div style="font-size:13px;color:var(--muted);margin-bottom:12px;">Invoice: <b>₹'+Math.round(p.amount).toLocaleString('en-IN')+'</b> · Paid so far: <b>₹'+Math.round(p.paidAmount||0).toLocaleString('en-IN')+'</b> · Due: <b style="color:#dc2626;">₹'+Math.round(due).toLocaleString('en-IN')+'</b></div>'
    + '<div class="field"><label>Amount Paying Now (₹)</label><input type="number" class="input" id="spr-amt" value="'+Math.round(due)+'" placeholder="Amount"></div>'
    + '<div class="field"><label>Mode</label><select class="select" id="spr-mode"><option>Cash</option><option>UPI</option><option>NEFT/RTGS</option><option>Cheque</option></select></div>'
    + '<div class="field"><label>Date</label><input type="date" class="input" id="spr-date" value="'+todayStr()+'"></div>'
    + '<div class="field"><label>Reference / UTR (optional)</label><input class="input" id="spr-ref" placeholder="UPI ref, cheque no…"></div>'
    + '<button class="btn btn-success" onclick="confirmSupPayment(\''+purchaseId+'\')">✅ Save Payment</button>'
  );
}

function confirmSupPayment(purchaseId) {
  var p = (state.purchases||[]).find(function(x){ return x.id===purchaseId; });
  if (!p) return;
  var amt = parseFloat(document.getElementById('spr-amt').value) || 0;
  if (!amt) { toast('Enter amount'); return; }
  p.paidAmount = (p.paidAmount||0) + amt;
  p.payStatus  = p.paidAmount >= p.amount ? 'paid' : 'partial';
  // Update supplier totalDue
  var sup = (state.suppliers||[]).find(function(s){ return s.id===p.supplierId; });
  if (sup) sup.totalDue = Math.max(0, (sup.totalDue||0) - amt);
  save();
  closeModal();
  try { recomputeTotalDue(); } catch(e) { console.warn('[totalDue] recomputeTotalDue failed:', e.message); }
  logActivity && logActivity('Payment to '+p.supplierName+': ₹'+Math.round(amt).toLocaleString('en-IN')+' ('+document.getElementById('spr-mode').value+')', 'info');
  toast('✅ Payment recorded');
  renderSuppliersList();
  if (_supDetailId) renderSupplierDetail();
  renderLedger && renderLedger();
}

// ── Open advance modal pre-selected to a supplier ─────────
function openAddAdvanceForSupplier(supId) {
  openAddAdvance && openAddAdvance();
  setTimeout(function(){
    var sel = document.getElementById('adv-sup-id');
    if (sel) sel.value = supId;
  }, 80);
}

// ── AI Insights for supplier ───────────────────────────────
async function renderSupAI(supId) {
  var s = (state.suppliers||[]).find(function(x){ return x.id===supId; });
  if (!s) return;
  var el = document.getElementById('sup-ai-body-'+supId);
  if (!el) return;
  var key = getAIKey();
  if (!key || !key.startsWith('sk-')) {
    el.innerHTML = '<div style="text-align:center;padding:20px;font-size:13px;color:var(--muted);">No AI key configured.<br><small>Settings → AI Settings</small></div>';
    return;
  }
  el.innerHTML = '<div class="ai-thinking">🤖 Analysing ordering patterns…</div>';

  var parts     = (state.parts||[]).filter(function(p){ return p.supplierName && p.supplierName.trim().toLowerCase()===s.name.trim().toLowerCase(); });
  var purchases = (state.purchases||[]).filter(function(p){ return p.supplierId===supId; }).slice(-20);
  var totalSpend= purchases.reduce(function(a,p){ return a+p.amount; },0);
  var avgOrder  = purchases.length ? Math.round(totalSpend/purchases.length) : 0;

  // Build velocity data for parts from this supplier
  var partsData = parts.slice(0,20).map(function(p){
    var vel = Smart && Smart.velocity ? Smart.velocity(p.id).toFixed(2) : '?';
    return p.partNo+' '+p.name+' (stock: '+p.stock+', vel: '+vel+'/day, cost: ₹'+(p.costPrice||0)+')';
  }).join('\n');

  var purchaseHistory = purchases.slice(-10).map(function(p){
    return new Date(p.date).toLocaleDateString('en-IN')+' — ₹'+Math.round(p.amount).toLocaleString('en-IN')+(p.invoiceNo?' Inv#'+p.invoiceNo:'')+(p.notes?' · '+p.notes:'');
  }).join('\n');

  var prompt = 'You are an intelligent ERP assistant for Goury Parts, an auto parts shop in Gwalior, MP.\n\n'
    + 'SUPPLIER: '+s.name+(s.city?' ('+s.city+')':'')+'\n'
    + 'Total spend: ₹'+Math.round(totalSpend).toLocaleString('en-IN')+' across '+purchases.length+' purchases. Avg order: ₹'+avgOrder+'\n\n'
    + 'PARTS WE BUY FROM THEM ('+parts.length+' SKUs):\n'+(partsData||'None tagged yet')+'\n\n'
    + 'RECENT PURCHASE HISTORY:\n'+(purchaseHistory||'None recorded')+'\n\n'
    + 'Provide sharp, specific insights in 4 sections:\n'
    + '1. ORDER PATTERN: How often do we order, what triggers reorders, is there a pattern by season or machine type?\n'
    + '2. STOCK RISK: Which parts from this supplier are running low or at risk? Specific part names.\n'
    + '3. NEGOTIATION LEVERAGE: Based on spend volume and frequency, what terms/discounts should we ask for?\n'
    + '4. ACTION TODAY: One concrete thing to do right now with this supplier.\n\n'
    + 'Be direct and specific. Use ₹ amounts. Max 200 words total. No fluff.';

  try {
    var resp = await fetch('https://api.anthropic.com/v1/messages', {
      method:'POST',
      headers:{'Content-Type':'application/json','x-api-key':key,'anthropic-version':'2023-06-01','anthropic-dangerous-direct-browser-access':'true'},
      body:JSON.stringify({model:getAIModel(),max_tokens:500,messages:[{role:'user',content:prompt}]})
    });
    var data = await resp.json();
    var text = (data.content&&data.content[0]&&data.content[0].text)||'No response';
    // Format sections nicely
    var formatted = text
      .replace(/\*\*([^*]+)\*\*/g,'<b>$1</b>')
      .replace(/^(\d\.\s*[A-Z ]+:)/gm,'<div style="font-size:11px;font-weight:700;color:var(--ai);text-transform:uppercase;letter-spacing:.5px;margin-top:10px;margin-bottom:4px;">$1</div>')
      .replace(/\n/g,'<br>');
    el.innerHTML = '<div class="ai-insight-card">'
      + '<div class="ai-insight-title">🤖 AI Ordering Intelligence</div>'
      + '<div class="ai-insight-body">'+formatted+'</div>'
      + '</div>'
      + '<div style="font-size:11px;color:var(--muted);text-align:center;margin-top:6px;">Based on '+parts.length+' parts · '+purchases.length+' invoices · ₹'+Math.round(totalSpend).toLocaleString('en-IN')+' lifetime spend</div>'
      + '<button class="btn btn-outline btn-sm" style="margin-top:10px;" onclick="renderSupAI(\''+supId+'\')">🔄 Re-analyse</button>';
  } catch(e) {
    el.innerHTML = '<div style="color:var(--danger);font-size:13px;padding:12px;">AI error: '+e.message+'</div>';
  }
}

// ── Supplier CRUD ───────────────────────────────────────────
function openAddSupplier(id) {
  var s = id ? (state.suppliers||[]).find(function(x){return x.id===id;}) : {};
  s = s || {};
  showModal(id ? 'Edit Supplier' : 'Add Supplier', ''
    + '<div class="field"><label>Supplier Name *</label><input class="input" id="sup-name" value="'+(s.name||'')+'" placeholder="e.g. Goyal Auto Parts"></div>'
    + '<div class="row2">'
    + '<div class="field"><label>Phone</label><input class="input" id="sup-phone" value="'+(s.phone||'')+'" placeholder="9876543210"></div>'
    + '<div class="field"><label>City</label><input class="input" id="sup-city" value="'+(s.city||'')+'" placeholder="Gwalior"></div>'
    + '</div>'
    + '<div class="field"><label>GSTIN (optional)</label><input class="input" id="sup-gstin" value="'+(s.gstin||'')+'" placeholder="27AAAAA0000A1Z5"></div>'
    + '<div class="field"><label>Notes / Credit Terms</label><input class="input" id="sup-notes" value="'+(s.notes||'')+'" placeholder="e.g. 30 day credit, COD only"></div>'
    + '<button class="btn" onclick="saveSupplier(\''+(id||'')+'\')">✅ Save Supplier</button>'
    + (id ? '<button class="btn btn-danger" style="margin-top:8px;" onclick="deleteSupplier(\''+id+'\');closeModal()">🗑 Delete Supplier</button>' : '')
  );
}

function editSupplier(id) { openAddSupplier(id); }

function saveSupplier(id) {
  var name = (document.getElementById('sup-name').value||'').trim();
  if (!name) { toast('Enter supplier name'); return; }
  if (!state.suppliers) state.suppliers = [];
  var phone = (document.getElementById('sup-phone').value||'').trim();
  if (id) {
    var s = state.suppliers.find(function(x){return x.id===id;});
    if (s) { s.name=name; s.phone=phone; s.city=(document.getElementById('sup-city').value||'').trim(); s.gstin=(document.getElementById('sup-gstin').value||'').trim(); s.notes=(document.getElementById('sup-notes').value||'').trim(); }
    // Also update supplierName/Phone on all parts that reference this supplier (by old name)
    var oldName = s && s.name;
  } else {
    state.suppliers.push({ id:uid(), name:name, phone:phone, city:(document.getElementById('sup-city').value||'').trim(), gstin:(document.getElementById('sup-gstin').value||'').trim(), notes:(document.getElementById('sup-notes').value||'').trim(), totalDue:0 });
  }
  // If phone updated, propagate to all parts with matching supplier name
  if (phone) {
    (state.parts||[]).forEach(function(p){
      if (p.supplierName && p.supplierName.trim().toLowerCase()===name.toLowerCase() && !p.supplierPhone) {
        p.supplierPhone = phone;
      }
    });
  }
  save();
  closeModal();
  renderSuppliersList();
  if (_supDetailId===id) renderSupplierDetail();
  populateSupplierFilter && populateSupplierFilter();
  refreshSupplierDatalist && refreshSupplierDatalist();
  toast('✅ Supplier saved');
}

function deleteSupplier(id) {
  if (!confirm('Delete supplier? Their purchase records will remain.')) return;
  state.suppliers = (state.suppliers||[]).filter(function(s){return s.id!==id;});
  window._ownerDeleteInProgress = true;
  save().finally(function(){ window._ownerDeleteInProgress = false; });
  closeSupplierDetail();
  renderSuppliersList();
  populateSupplierFilter && populateSupplierFilter();
}

function openAddPurchaseForSupplier(supId) {
  var sup = (state.suppliers||[]).find(function(s){ return s.id === supId; });
  if (!sup) return;

  // Photo-first flow: open camera/gallery immediately, AI reads amount + invoice#,
  // then just ask how much was paid. No form filling needed.
  window._apfSupId = supId; // store for onclick handlers
  showModal('📷 Add Bill — ' + sup.name,
    '<div style="font-size:13px;color:var(--muted);margin-bottom:14px;">Take a photo of the bill. AI will read the total automatically.</div>'
    + '<div id="apf-preview-wrap" style="display:none;margin-bottom:12px;">'
    + '<img id="apf-preview-img" style="width:100%;max-height:200px;object-fit:contain;border-radius:10px;border:1px solid var(--border);">'
    + '<div id="apf-scan-status" style="font-size:13px;color:var(--ai);font-weight:600;text-align:center;margin-top:8px;"></div>'
    + '</div>'
    + '<input type="file" id="apf-file-input" accept="image/*" capture="environment" style="display:none;" onchange="apfHandlePhoto(this,window._apfSupId)">'
    + '<div id="apf-fields" style="display:none;">'
    + '<div class="field"><label>Total Amount (₹)</label><input type="number" class="input" id="apf-amt" placeholder="0" inputmode="numeric"></div>'
    + '<div class="field"><label>Invoice No</label><input class="input" id="apf-inv" placeholder="Optional"></div>'
    + '<div class="field"><label>Date</label><input type="date" class="input" id="apf-date" value="' + new Date().toISOString().slice(0,10) + '"></div>'
    + '<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:12px;margin-bottom:12px;">'
    + '<div style="font-size:13px;font-weight:700;margin-bottom:10px;">💰 How much paid now?</div>'
    + '<div style="display:flex;gap:8px;margin-bottom:10px;">'
    + '<button type="button" class="btn btn-outline btn-sm" onclick="apfSetPaid(0)" style="flex:1;">Credit (₹0)</button>'
    + '<button type="button" class="btn btn-outline btn-sm" onclick="apfSetPaidFull()" style="flex:1;">Full Amount</button>'
    + '<button type="button" class="btn btn-outline btn-sm" onclick="apfSetPaidCustom()" style="flex:1;">Custom</button>'
    + '</div>'
    + '<div id="apf-paid-wrap" style="display:none;">'
    + '<input type="number" class="input" id="apf-paid" placeholder="Amount paid" inputmode="numeric" style="margin-bottom:8px;">'
    + '<select class="select" id="apf-mode"><option>cash</option><option>upi</option><option>cheque</option><option>neft</option></select>'
    + '</div>'
    + '</div>'
    + '<button class="btn btn-success" onclick="apfSave(window._apfSupId)">✅ Save Bill</button>'
    + '</div>'
    + '<button class="btn" id="apf-camera-btn" onclick="document.getElementById(\'apf-file-input\').click()">📷 Take / Upload Photo</button>'
    + '<div style="margin-top:8px;text-align:center;"><button type="button" style="background:none;border:none;color:var(--muted);font-size:12px;cursor:pointer;text-decoration:underline;" onclick="apfSkipPhoto(window._apfSupId)">Skip photo, enter manually</button></div>'
  );
} // end openAddPurchaseForSupplier

function apfSetPaid(val) {
  document.getElementById('apf-paid-wrap').style.display = 'none';
  var el = document.getElementById('apf-paid'); if (el) el.value = val;
}
function apfSetPaidFull() {
  var amt = parseFloat((document.getElementById('apf-amt')||{}).value) || 0;
  document.getElementById('apf-paid-wrap').style.display = '';
  var el = document.getElementById('apf-paid'); if (el) el.value = amt;
}
function apfSetPaidCustom() {
  document.getElementById('apf-paid-wrap').style.display = '';
  var el = document.getElementById('apf-paid'); if (el) { el.value = ''; el.focus(); }
}
function apfSkipPhoto(supId) {
  document.getElementById('apf-preview-wrap').style.display = 'none';
  document.getElementById('apf-camera-btn').style.display = 'none';
  document.getElementById('apf-fields').style.display = '';
  window._apfPhotoDataUrl = null;
}

async function apfHandlePhoto(input, supId) {
  if (!input.files || !input.files[0]) return;
  var file = input.files[0];
  var reader = new FileReader();
  reader.onload = async function(e) {
    var dataUrl = e.target.result;
    window._apfPhotoDataUrl = dataUrl;

    // Show preview
    var wrap = document.getElementById('apf-preview-wrap');
    var img  = document.getElementById('apf-preview-img');
    var stat = document.getElementById('apf-scan-status');
    var btn  = document.getElementById('apf-camera-btn');
    if (wrap) wrap.style.display = '';
    if (img)  img.src = dataUrl;
    if (btn)  btn.style.display = 'none';
    if (stat) { stat.textContent = '🤖 Reading bill…'; stat.style.color = 'var(--ai)'; }

    var key = getAIKey();
    if (!key || !key.startsWith('sk-')) {
      // No AI key — just show the form
      if (stat) stat.textContent = 'No AI key — fill in manually';
      document.getElementById('apf-fields').style.display = '';
      return;
    }

    try {
      var base64   = dataUrl.split(',')[1];
      var mimeType = file.type || 'image/jpeg';
      var resp = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-api-key': key,
          'anthropic-version': '2023-06-01',
          'anthropic-dangerous-direct-browser-access': 'true'
        },
        body: JSON.stringify({
          model: getAIModel(),
          max_tokens: 300,
          messages: [{ role: 'user', content: [
            { type: 'image', source: { type: 'base64', media_type: mimeType, data: base64 } },
            { type: 'text', text: 'This is a supplier bill. Extract ONLY: totalAmount (number), invoiceNo (string), date (YYYY-MM-DD). Return ONLY valid JSON like {"totalAmount":1500,"invoiceNo":"INV-001","date":"2024-01-15"}. If not found use 0 or empty string.' }
          ]}]
        })
      });
      var data = await resp.json();
      var text = (data.content && data.content[0] && data.content[0].text) || '';
      var s = text.indexOf('{'), e2 = text.lastIndexOf('}');
      var parsed = {};
      if (s >= 0 && e2 >= 0) { try { parsed = JSON.parse(text.slice(s, e2+1)); } catch(ex) {} }

      if (stat) {
        stat.textContent = parsed.totalAmount ? '✅ Read: ₹' + parsed.totalAmount + (parsed.invoiceNo ? ' · Inv# ' + parsed.invoiceNo : '') : '⚠ Could not read total — please fill in';
        stat.style.color = parsed.totalAmount ? 'var(--success)' : 'var(--danger)';
      }

      var amtEl = document.getElementById('apf-amt');
      var invEl = document.getElementById('apf-inv');
      var dtEl  = document.getElementById('apf-date');
      if (amtEl && parsed.totalAmount) amtEl.value = parsed.totalAmount;
      if (invEl && parsed.invoiceNo)   invEl.value = parsed.invoiceNo;
      if (dtEl  && parsed.date && parsed.date.match(/^\d{4}-\d{2}-\d{2}$/)) dtEl.value = parsed.date;

    } catch(err) {
      if (stat) { stat.textContent = 'Scan failed — fill in manually'; stat.style.color = 'var(--muted)'; }
    }

    document.getElementById('apf-fields').style.display = '';
  };
  reader.readAsDataURL(file);
}

async function apfSave(supId) {
  var sup = (state.suppliers||[]).find(function(s){ return s.id === supId; });
  if (!sup) { toast('Supplier not found'); return; }

  var amt  = parseFloat((document.getElementById('apf-amt')||{}).value)  || 0;
  var paid = parseFloat((document.getElementById('apf-paid')||{}).value) || 0;
  var inv  = ((document.getElementById('apf-inv')||{}).value||'').trim();
  var date = ((document.getElementById('apf-date')||{}).value||'') || new Date().toISOString().slice(0,10);
  var mode = ((document.getElementById('apf-mode')||{}).value||'cash');

  if (!amt) { toast('Enter the bill amount'); return; }
  paid = Math.min(paid, amt);
  var status = paid >= amt ? 'paid' : paid > 0 ? 'partial' : 'unpaid';

  var purchase = {
    id: uid(),
    date: date + 'T' + new Date().toTimeString().slice(0,8),
    supplierId: supId,
    supplierName: sup.name,
    amount: amt,
    paidAmount: paid,
    payStatus: status,
    invoiceNo: inv,
    notes: '',
    photoDataUrl: window._apfPhotoDataUrl || null,
    payments: paid > 0 ? [{ date: new Date().toISOString(), amount: paid, mode: mode, ref: '' }] : []
  };

  if (!state.purchases) state.purchases = [];
  state.purchases.push(purchase);
  sup.totalDue = (sup.totalDue || 0) + (amt - paid);

  await save();
  closeModal();
  window._apfPhotoDataUrl = null;
  renderLedger && renderLedger();
  renderBillsList && renderBillsList();
  renderSuppliersList && renderSuppliersList();
  if (_supDetailId) renderSupplierDetail && renderSupplierDetail();
  toast('✅ Bill saved' + (status === 'unpaid' ? ' — ₹' + Math.round(amt).toLocaleString('en-IN') + ' added to dues' : ''));

  // Upload photo to Firebase Storage in background
  if (purchase.photoDataUrl && purchase.photoDataUrl.startsWith('data:') && window.firebaseUploadBillPhoto) {
    var _pid = purchase.id, _durl = purchase.photoDataUrl;
    window.firebaseUploadBillPhoto(_pid, _durl)
      .then(function(url) {
        var p2 = (state.purchases||[]).find(function(x){ return x.id === _pid; });
        if (p2) { p2.photoDataUrl = url; save(); renderBillsList && renderBillsList(); }
        toast('✅ Bill photo saved to cloud');
      })
      .catch(function(e) { console.warn('Bill photo upload failed:', e.message); });
  }
}


// ---- Bill Photo Scanner ----
// Attach a photo to an existing purchase (no re-scanning, no inventory changes)
function attachBillPhoto(purchaseId) {
  // Create a one-shot file input, trigger it, then clean up
  var input = document.createElement('input');
  input.type = 'file';
  input.accept = 'image/*';
  input.capture = 'environment';
  input.style.display = 'none';
  document.body.appendChild(input);
  input.onchange = function() {
    if (!input.files || !input.files[0]) { document.body.removeChild(input); return; }
    var file = input.files[0];
    document.body.removeChild(input);
    var reader = new FileReader();
    reader.onload = async function(e) {
      var dataUrl = e.target.result;
      var p = (state.purchases||[]).find(function(x){ return x.id === purchaseId; });
      if (!p) { toast('Purchase not found'); return; }
      p.photoDataUrl = dataUrl;
      try { await save(); } catch(err) { console.warn('[purchase] photo-attach save failed:', err.message); if(typeof RX!=='undefined') RX.log('warn','purchase','photo-attach save: '+err.message); }
      renderLedger();
      renderBillsList();
      toast('📷 Photo attached — uploading to cloud…');
      // Upload to Firebase Storage in background
      if (window.firebaseUploadBillPhoto && window._firebaseAuthed) {
        window.firebaseUploadBillPhoto(purchaseId, dataUrl)
          .then(function(url) {
            var p2 = (state.purchases||[]).find(function(x){ return x.id === purchaseId; });
            if (p2) { p2.photoDataUrl = url; save(); renderBillsList(); renderLedger(); }
            toast('✅ Bill photo saved to cloud');
          })
          .catch(function(err) {
            console.warn('Bill photo upload failed:', err.message);
            toast('Photo saved locally (cloud sync pending)');
          });
      }
    };
    reader.readAsDataURL(file);
  };
  input.click();
}

function renderBillsList() {
  var div = document.getElementById('bills-list');
  if (!div) return;
  var allPurchases = (state.purchases||[]).slice().reverse();
  if (!allPurchases.length) {
    div.innerHTML = '<div class="empty"><div class="icon">📷</div>No bill photos yet.<br><small>Use the scanner above to add one</small></div>';
    return;
  }
  div.innerHTML = allPurchases.map(function(p) {
    var hasPhoto = p.photoDataUrl && !(window.isPlaceholderPhoto && window.isPlaceholderPhoto(p.photoDataUrl));
    var imgSection = hasPhoto
      ? '<img src="' + p.photoDataUrl + '" onclick="viewBillPhoto(\'' + p.id + '\')" style="width:64px;height:64px;object-fit:cover;border-radius:8px;border:1px solid var(--border);cursor:pointer;flex-shrink:0;">'
      : '<div onclick="attachBillPhoto(\'' + p.id + '\')" style="width:64px;height:64px;border-radius:8px;border:2px dashed var(--border);display:flex;align-items:center;justify-content:center;flex-direction:column;cursor:pointer;flex-shrink:0;background:#fafbfc;">'
        + '<div style="font-size:22px;">📷</div>'
        + '<div style="font-size:9px;color:var(--muted);margin-top:2px;">Add</div>'
        + '</div>';
    return '<div style="display:flex;gap:10px;align-items:flex-start;padding:10px 0;border-bottom:1px solid var(--border);">'
      + imgSection
      + '<div style="flex:1;min-width:0;">'
      + '<div style="font-weight:600;font-size:14px;">' + p.supplierName + '</div>'
      + '<div style="font-size:12px;color:var(--muted);">' + new Date(p.date).toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'2-digit'}) + (p.invoiceNo?' · Inv#'+p.invoiceNo:'') + '</div>'
      + '<div style="font-size:13px;font-weight:700;color:var(--primary);margin-top:2px;">₹' + Math.round(p.amount).toLocaleString('en-IN') + '</div>'
      + '<div style="font-size:11px;color:var(--muted);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' + (p.notes||'') + '</div>'
      + '</div>'
      + '<button onclick="softDeletePurchase(\'' + p.id + '\')" title="Delete — moves to Recycle Bin" style="background:none;border:none;color:var(--muted);font-size:15px;cursor:pointer;padding:4px;line-height:1;flex-shrink:0;">🗑</button>'
      + '</div>';
  }).join('');
}

function viewBillPhoto(purchaseId) {
  var p = (state.purchases||[]).find(function(x){return x.id===purchaseId;});
  if (!p || !p.photoDataUrl) return;
  showModal('📷 Bill — ' + p.supplierName,
    '<img src="' + p.photoDataUrl + '" style="width:100%;border-radius:8px;border:1px solid var(--border);">'
    + '<div style="margin-top:10px;font-size:13px;">'
    + '<b>' + p.supplierName + '</b> · ₹' + Math.round(p.amount).toLocaleString('en-IN')
    + '<br><small>' + new Date(p.date).toLocaleDateString('en-IN') + (p.invoiceNo?' · Inv#'+p.invoiceNo:'') + '</small>'
    + '</div>'
  );
}

// ---- Bill Photo Scanner — scans bill, extracts line items with units, updates inventory ----

// Staging area — avoids passing huge base64 strings through onclick HTML attributes
window._billStage = null;

async function handleBillPhoto(input) {
  if (!input.files || !input.files[0]) return;
  var file = input.files[0];
  var reader = new FileReader();
  reader.onload = async function(e) {
    var dataUrl = e.target.result;
    var wrap = document.getElementById('bill-preview-wrap');
    var img  = document.getElementById('bill-preview-img');
    var stat = document.getElementById('bill-scan-status');
    var res  = document.getElementById('bill-scan-result');
    if (wrap) wrap.style.display = '';
    if (img)  img.src = dataUrl;
    if (stat) { stat.textContent = '🤖 Reading bill...'; stat.style.color = 'var(--ai)'; }
    if (res)  { res.style.display = 'none'; res.innerHTML = ''; }

    var key = getAIKey();
    if (!key || !key.startsWith('sk-')) {
      if (stat) { stat.textContent = 'No AI key — fill manually (Settings → AI Settings)'; stat.style.color = 'var(--muted)'; }
      window._billStage = { photoDataUrl: dataUrl, supplierName:'', invoiceNo:'', date: new Date().toISOString().slice(0,10), amount:0, notes:'', lineItems:[] };
      setTimeout(function() { openManualPurchaseFromStage(); }, 400);
      input.value = '';
      return;
    }

    try {
      var base64   = dataUrl.split(',')[1];
      var mimeType = file.type || 'image/jpeg';

      var catalogue = state.parts.slice(0, 150).map(function(p) {
        return p.partNo + '|' + p.name + (p.brand ? '|' + p.brand : '');
      }).join('\n');

      var prompt = 'This is a supplier purchase bill from an Indian spare parts / auto shop.\n\n'
        + 'Our inventory catalogue (partNo|name|brand):\n' + catalogue + '\n\n'
        + 'Extract and return ONLY valid JSON, no markdown, no explanation:\n'
        + '{"supplierName":"","invoiceNo":"","date":"YYYY-MM-DD","totalAmount":0,"lineItems":[{"partNo":"","name":"","qty":0,"unit":"","unitCost":0,"lineTotal":0,"matchedPartNo":""}]}\n\n'
        + 'Rules:\n'
        + '- unit: capture EXACTLY as written on bill e.g. "kg", "set", "pcs", "dozen", "ltr", "box", "pair". Leave empty if not mentioned.\n'
        + '- qty: the numeric quantity as on bill (e.g. 2.5 for "2.5 kg", 1 for "1 set").\n'
        + '- matchedPartNo: if item matches a part in catalogue by partNo or name, put that partNo. Else leave empty.\n'
        + '- Leave unclear fields as empty string or 0.';

      if (stat) stat.textContent = '🤖 Matching items to your inventory...';

      var resp = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-api-key': key,
          'anthropic-version': '2023-06-01',
          'anthropic-dangerous-direct-browser-access': 'true'
        },
        body: JSON.stringify({
          model: getAIModel(),
          max_tokens: 1400,
          messages: [{
            role: 'user',
            content: [
              { type: 'image', source: { type: 'base64', media_type: mimeType, data: base64 } },
              { type: 'text', text: prompt }
            ]
          }]
        })
      });

      var data = await resp.json();
      var text = (data.content && data.content[0] && data.content[0].text) || '';
      var s2 = text.indexOf('{'); var e2 = text.lastIndexOf('}');
      var parsed = {};
      if (s2 >= 0 && e2 >= 0) { try { parsed = JSON.parse(text.slice(s2, e2+1)); } catch(ex) {} }

      var lineItems = parsed.lineItems || [];
      var totalAmount = parsed.totalAmount || lineItems.reduce(function(s,l){ return s+(l.lineTotal||l.qty*l.unitCost||0);},0);

      // Match each line item to inventory
      lineItems.forEach(function(li) {
        var m = null;
        if (li.matchedPartNo) m = state.parts.find(function(p){ return p.partNo === li.matchedPartNo; });
        if (!m && li.partNo)  m = state.parts.find(function(p){ return p.partNo.toLowerCase() === (li.partNo||'').toLowerCase(); });
        if (!m && li.name) {
          var nm = (li.name||'').toLowerCase();
          m = state.parts.find(function(p){ return p.name.toLowerCase().includes(nm)||nm.includes(p.name.toLowerCase()); });
        }
        li._matched = m || null;
      });

      var matchedCount = lineItems.filter(function(li){ return li._matched; }).length;
      if (stat) { stat.textContent = '✅ Scanned — ' + matchedCount + '/' + lineItems.length + ' items matched to stock.'; stat.style.color = 'var(--success)'; }

      window._billStage = {
        supplierName: parsed.supplierName || '',
        invoiceNo:    parsed.invoiceNo    || '',
        date:         parsed.date         || new Date().toISOString().slice(0,10),
        amount:       totalAmount,
        notes:        lineItems.map(function(li){ return (li.name||li.partNo||'?')+' x'+(li.qty||0)+(li.unit?' '+li.unit:''); }).join(', '),
        photoDataUrl: dataUrl,
        lineItems:    lineItems
      };

      if (res) {
        res.style.display = '';
        var html = '<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:12px;margin-bottom:10px;font-size:13px;">'
          + '<b>' + (parsed.supplierName||'(Supplier not detected)') + '</b>'
          + (parsed.invoiceNo ? ' &nbsp;·&nbsp; Inv# ' + parsed.invoiceNo : '')
          + (parsed.date      ? ' &nbsp;·&nbsp; ' + parsed.date : '')
          + '<div style="font-size:15px;font-weight:700;margin-top:4px;">&#8377;' + Math.round(totalAmount).toLocaleString('en-IN') + '</div>'
          + '</div>';

        if (lineItems.length) {
          html += '<div style="font-weight:700;font-size:13px;margin-bottom:6px;">&#x1F4E6; Line Items — uncheck to skip</div>';
          lineItems.forEach(function(li, i) {
            var m   = li._matched;
            var bg  = m ? '#f0fdf4' : '#fffbeb';
            var bdr = m ? '#bbf7d0' : '#fde68a';
            // Pre-fill suggested prices: if matched part has existing prices use those, else suggest margin defaults
            var defRetail = m ? (m.instantPrice||0) : (li.unitCost ? Math.round(li.unitCost*1.3) : '');
            var defWhole  = m ? (m.creditPrice ||0) : (li.unitCost ? Math.round(li.unitCost*1.15): '');
            var defRack   = m ? (m.location    ||'') : '';
            html += '<div style="background:' + bg + ';border:1px solid ' + bdr + ';border-radius:8px;padding:10px 12px;margin-bottom:6px;">'
              + '<div style="display:flex;align-items:flex-start;gap:10px;">'
              + '<input type="checkbox" id="bli-chk-' + i + '" checked style="margin-top:3px;width:18px;height:18px;flex-shrink:0;">'
              + '<div style="flex:1;min-width:0;">'
              + '<div style="font-weight:600;font-size:13px;">' + (li.name||li.partNo||'Unknown item') + '</div>'
              + (li.partNo ? '<div style="font-size:11px;color:var(--muted);font-family:monospace;">' + li.partNo + '</div>' : '')
              // Row 1 — Qty, Unit, Cost
              + '<div style="display:flex;gap:6px;align-items:center;margin-top:8px;flex-wrap:wrap;">'
              + '<div style="display:flex;flex-direction:column;align-items:center;gap:2px;">'
              + '<label style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;">Qty</label>'
              + '<input type="number" id="bli-qty-' + i + '" value="' + (li.qty||0) + '" step="0.01" style="width:62px;padding:5px 4px;border:1px solid var(--border);border-radius:6px;font-size:13px;text-align:center;">'
              + '</div>'
              + '<div style="display:flex;flex-direction:column;align-items:center;gap:2px;">'
              + '<label style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;">Unit</label>'
              + '<input type="text" id="bli-unit-' + i + '" value="' + (li.unit||'') + '" placeholder="pcs/kg/set" style="width:62px;padding:5px 4px;border:1px solid var(--border);border-radius:6px;font-size:13px;">'
              + '</div>'
              + '<div style="display:flex;flex-direction:column;align-items:center;gap:2px;">'
              + '<label style="font-size:10px;color:#7c3aed;text-transform:uppercase;letter-spacing:.5px;">Cost &#8377;</label>'
              + '<input type="number" id="bli-cost-' + i + '" value="' + (li.unitCost||0) + '" step="0.01" style="width:72px;padding:5px 4px;border:1.5px solid #ddd6fe;border-radius:6px;font-size:13px;text-align:right;" oninput="bliAutoPrice(' + i + ')">'
              + '</div>'
              + '</div>'
              // Row 2 — Retail, Wholesale/Credit, Rack
              + '<div style="display:flex;gap:6px;align-items:center;margin-top:6px;flex-wrap:wrap;">'
              + '<div style="display:flex;flex-direction:column;align-items:center;gap:2px;">'
              + '<label style="font-size:10px;color:#0f766e;text-transform:uppercase;letter-spacing:.5px;">Retail &#8377;</label>'
              + '<input type="number" id="bli-retail-' + i + '" value="' + defRetail + '" step="0.01" placeholder="Instant" style="width:72px;padding:5px 4px;border:1.5px solid #99f6e4;border-radius:6px;font-size:13px;text-align:right;">'
              + '</div>'
              + '<div style="display:flex;flex-direction:column;align-items:center;gap:2px;">'
              + '<label style="font-size:10px;color:#d97706;text-transform:uppercase;letter-spacing:.5px;">Wholesale &#8377;</label>'
              + '<input type="number" id="bli-ws-' + i + '" value="' + defWhole + '" step="0.01" placeholder="Credit" style="width:72px;padding:5px 4px;border:1.5px solid #fde68a;border-radius:6px;font-size:13px;text-align:right;">'
              + '</div>'
              + '<div style="display:flex;flex-direction:column;align-items:center;gap:2px;">'
              + '<label style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;">Rack</label>'
              + '<input type="text" id="bli-rack-' + i + '" value="' + defRack + '" placeholder="A1/B3…" style="width:62px;padding:5px 4px;border:1px solid var(--border);border-radius:6px;font-size:13px;">'
              + '</div>'
              + '</div>'
              + (m
                  ? '<div style="font-size:11px;color:#16a34a;margin-top:5px;">&#x2713; Match: <b>' + m.name + '</b> &middot; stock now: <b>' + m.stock + '</b> &middot; existing retail: &#8377;' + (m.instantPrice||'—') + '</div>'
                  : '<div style="font-size:11px;color:#d97706;margin-top:5px;">&#x2728; New part &mdash; will be added to inventory on confirm</div>'
              )
              + '</div></div></div>';
          });
        }

        html += '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:12px;">'
          + '<button class="btn btn-ai" style="flex:1;" onclick="confirmBillAndUpdateInventory()">&#x2705; Confirm &amp; Update Inventory</button>'
          + '<button class="btn btn-outline" style="flex:1;" onclick="openManualPurchaseFromStage()">&#x270F; Edit Manually</button>'
          + '</div>';
        res.innerHTML = html;
      }

      if (parsed.supplierName) {
        var sn = (parsed.supplierName||'').toLowerCase();
        var sf = (state.suppliers||[]).find(function(s){ return s.name.toLowerCase().includes(sn)||sn.includes(s.name.toLowerCase()); });
        if (!sf && res) {
          res.innerHTML += '<div style="margin-top:8px;font-size:12px;color:var(--muted);">Supplier "<b>' + parsed.supplierName + '</b>" not in your list &mdash; <a href="#" onclick="openAddSupplier();return false;" style="color:var(--ai);">Add supplier</a></div>';
        }
      }

    } catch(err) {
      if (stat) { stat.textContent = 'Scan failed: ' + err.message; stat.style.color = 'var(--danger)'; }
      if (res) { res.style.display = ''; res.innerHTML = '<button class="btn btn-outline" onclick="openManualPurchaseFromStage()">Fill manually instead</button>'; }
      window._billStage = { photoDataUrl: dataUrl, supplierName:'', invoiceNo:'', date: new Date().toISOString().slice(0,10), amount:0, notes:'', lineItems:[] };
    }
    input.value = '';
  };
  reader.readAsDataURL(file);
}

function openManualPurchaseFromStage() {
  var b = window._billStage || {};
  openAddPurchase({ supplierName: b.supplierName||'', invoiceNo: b.invoiceNo||'', date: b.date||'', amount: b.amount||0, notes: b.notes||'', photoDataUrl: b.photoDataUrl||null });
}

// Open unified Add Part modal from an unmatched purchase bill line item
function bliOpenAddFromBill(i, qty, unit, partNo, name, cost) {
  openAddPart({
    context: 'Purchase Bill',
    prefill: { partNo: partNo||'', name: name||'', unitCost: cost||0, qty: qty||0, unit: unit||'pcs' }
  });
  // After modal renders, pre-fill fields and set pending bill index
  setTimeout(() => {
    if (partNo) { const el=document.getElementById('uap-partNo'); if(el) el.value=String(partNo).toUpperCase(); }
    if (name)   { const el=document.getElementById('uap-name');   if(el) el.value=name; }
    if (cost)   { const el=document.getElementById('uap-cost');   if(el) el.value=cost; }
    if (qty)    { const el=document.getElementById('uap-stock');  if(el) el.value=qty; }
    // Pick correct unit pill
    const u = (unit||'pcs').toLowerCase();
    const pills = document.querySelectorAll('.uap-unit-pill');
    pills.forEach(p => { if(p.dataset.unit === u) { p.classList.add('sel'); const h=document.getElementById('uap-unit'); if(h) h.value=u; } else p.classList.remove('sel'); });
    uapAutoPrice();
    // Store which bill line this is for — savePart reads this after save
    window._bliPendingIndex = i;
  }, 80);
}

// Show/hide the inline new-part form for an unmatched line item (kept for legacy compat)
function bliShowAddForm(i) {
  bliOpenAddFromBill(i, 0, 'pcs', '', '', 0);
}

// Auto-suggest prices in the inline add-part form when cost is typed (legacy compat)
function bafAutoPrice(i) { uapAutoPrice && uapAutoPrice(); }

// bliAddNewPart — replaced by bliOpenAddFromBill → unified openAddPart modal
// Kept as no-op stub to avoid errors from any remaining legacy references
async function bliAddNewPart(i, qty, unit) {
  bliOpenAddFromBill(i, qty, unit||'pcs', '', '', 0);
}

// Auto-suggest retail (30% margin) and wholesale (15% margin) when cost is edited on matched items
function bliAutoPrice(i) {
  var costEl   = document.getElementById('bli-cost-'   + i);
  var retailEl = document.getElementById('bli-retail-' + i);
  var wsEl     = document.getElementById('bli-ws-'     + i);
  if (!costEl) return;
  var cost = parseFloat(costEl.value) || 0;
  if (!cost) return;
  // Only auto-fill if the field is currently 0 or empty (don't overwrite what user typed)
  if (retailEl && (!retailEl.value || parseFloat(retailEl.value) === 0)) {
    retailEl.value = Math.round(cost * 1.30);
  }
  if (wsEl && (!wsEl.value || parseFloat(wsEl.value) === 0)) {
    wsEl.value = Math.round(cost * 1.15);
  }
}

async function confirmBillAndUpdateInventory() {
  var bill = window._billStage;
  if (!bill) { toast('No bill scanned — please try again'); return; }

  var lineItems = bill.lineItems || [];
  var updated   = [];
  var skipped   = [];
  var _affectedParts = []; // parts created/modified here → cloud-persist per-doc after save()
  function _trackPart(p){ if (p && p.id && _affectedParts.indexOf(p) < 0) _affectedParts.push(p); }

  lineItems.forEach(function(li, i) {
    var chk = document.getElementById('bli-chk-' + i);
    if (chk && !chk.checked) { skipped.push(li.name||li.partNo||'?'); return; }

    var qtyInput    = document.getElementById('bli-qty-'    + i);
    var unitInput   = document.getElementById('bli-unit-'   + i);
    var costInput   = document.getElementById('bli-cost-'   + i);
    var retailInput = document.getElementById('bli-retail-' + i);
    var wsInput     = document.getElementById('bli-ws-'     + i);
    var rackInput   = document.getElementById('bli-rack-'   + i);
    var qty    = qtyInput    ? (parseFloat(qtyInput.value)    || 0)  : (parseFloat(li.qty)    || 0);
    var unit   = unitInput   ? (unitInput.value.trim()         || li.unit || '') : (li.unit || '');
    var cost   = costInput   ? (parseFloat(costInput.value)   || 0)  : (li.unitCost || 0);
    var retail = retailInput ? (parseFloat(retailInput.value) || 0)  : 0;
    var ws     = wsInput     ? (parseFloat(wsInput.value)     || 0)  : 0;
    var rack   = rackInput   ? (rackInput.value.trim()         || '') : '';

    var matched = null;
    // First: use already-resolved match (covers newly added parts from "+ Add New Part")
    if (li._matched) matched = getPartById(li._matched.id) || li._matched;
    if (!matched && li.matchedPartNo) matched = state.parts.find(function(p){ return p.partNo === li.matchedPartNo; });
    if (!matched && li.partNo) matched = state.parts.find(function(p){ return p.partNo.toLowerCase()===(li.partNo||'').toLowerCase(); });
    if (!matched && li.name) {
      var nm = (li.name||'').toLowerCase();
      matched = state.parts.find(function(p){ return p.name.toLowerCase().includes(nm)||nm.includes(p.name.toLowerCase()); });
    }
    // Auto-create new part directly from bill data if no inventory match found
    if (!matched) {
      var newPart = {
        id:           uid(),
        partNo:       ((li.partNo || ('BILL-' + uid())).toString().toUpperCase()),
        name:         li.name || li.partNo || 'Unknown Part',
        machineType:  '',
        brand:        '',
        category:     '',
        models:       '',
        unit:         unit || li.unit || 'pcs',
        costPrice:    cost || li.unitCost || 0,
        mrp:          0,
        instantPrice: retail || (cost ? Math.round(cost * 1.30) : 0),
        creditPrice:  ws    || (cost ? Math.round(cost * 1.15) : 0),
        stock:        qty   || li.qty || 0,
        minStock:     0,
        location:     rack || '',
        supplierName: bill.supplierName || '',
        supplierPhone:'',
        photoDataUrl: '',
        addedAt:      Date.now()
      };
      state.parts.push(newPart);
      _trackPart(newPart);
      updated.push(newPart.name + ': ✨ NEW PART created — stock ' + newPart.stock + (newPart.unit?' '+newPart.unit:'')
        + ' | cost ₹' + (newPart.costPrice||0)
        + ' | retail ₹' + (newPart.instantPrice||'—')
        + (newPart.location ? ' | rack ' + newPart.location : ''));
      return;
    }

    // If this part was previously added via modal, stock already set — only update prices/rack
    if (li._newPartAdded) {
      if (cost   > 0) matched.costPrice    = cost;
      if (retail > 0) matched.instantPrice = retail;
      if (ws     > 0) matched.creditPrice  = ws;
      if (rack)       matched.location     = rack;
      if (unit && !matched.unit) matched.unit = unit;
      _trackPart(matched);
      updated.push(matched.name + ': NEW PART added with stock ' + matched.stock + (unit?' '+unit:'')
        + ' | retail ₹' + (retail||'—') + (rack?' | rack '+rack:''));
      return;
    }

    if (qty > 0) matched.stock = (matched.stock || 0) + qty;
    if (cost   > 0) matched.costPrice    = cost;
    if (retail > 0) matched.instantPrice = retail;
    if (ws     > 0) matched.creditPrice  = ws;
    if (rack)       matched.location     = rack;
    if (unit && !matched.unit) matched.unit = unit;
    _trackPart(matched);

    updated.push(matched.name + ': +' + qty + (unit?' '+unit:'')
      + ' | cost ₹' + (cost||matched.costPrice||0)
      + ' | retail ₹' + (retail||matched.instantPrice||'—')
      + (rack ? ' | rack ' + rack : '')
      + ' (stock now ' + matched.stock + ')');
  });

  // Save parts — await so IDB write completes before we add purchase entry
  try { await save(); } catch(e) { console.warn('[bill-scan] parts save failed:', e.message); if(typeof RX!=='undefined') RX.log('error','bill-scan','parts save: '+e.message); }

  // Cloud-persist every created/modified part per-doc so other devices sync them.
  // save()/firebasePush skip the parts blob in per-doc mode, so without this the
  // stock/price changes and new parts from a bill scan would stay on this device.
  if (typeof cloudPersistPart === 'function') {
    _affectedParts.forEach(function(p){ try { cloudPersistPart(p); } catch(e){} });
  }

  if (!state.purchases) state.purchases = [];
  var sup   = (state.suppliers||[]).find(function(s){ var sn=(bill.supplierName||'').toLowerCase(); return s.name.toLowerCase().includes(sn)||sn.includes(s.name.toLowerCase()); });
  // Auto-create supplier if not found and name is known
  if (!sup && bill.supplierName && bill.supplierName !== 'Unknown') {
    ensureSupplierRegistered && ensureSupplierRegistered(bill.supplierName, '');
    sup = (state.suppliers||[]).find(function(s){ var sn=(bill.supplierName||'').toLowerCase(); return s.name.toLowerCase().includes(sn)||sn.includes(s.name.toLowerCase()); });
  }
  var supId = sup ? sup.id : '';
  state.purchases.push({
    id:           uid(),
    date:         (bill.date||new Date().toISOString().slice(0,10)) + 'T' + new Date().toTimeString().slice(0,8),
    supplierId:   supId,
    supplierName: bill.supplierName || 'Unknown',
    amount:       bill.amount || 0,
    paidAmount:   0,
    payStatus:    'unpaid',
    invoiceNo:    bill.invoiceNo || '',
    notes:        bill.notes || '',
    photoDataUrl: bill.photoDataUrl || null,
    payments:     []
  });
  if (sup) sup.totalDue = (sup.totalDue||0) + (bill.amount||0);

  // Second save picks up the new purchase entry
  try { await save(); } catch(e) { console.warn('[bill-scan] purchase save failed:', e.message); if(typeof RX!=='undefined') RX.log('error','bill-scan','purchase save: '+e.message); }

  // Upload bill photo to Firebase Storage in background — replaces base64 with https:// URL
  // so Firestore document stays small and photo is centralized/accessible from any device
  var newPurchase = state.purchases[state.purchases.length - 1];
  if (newPurchase && newPurchase.photoDataUrl && newPurchase.photoDataUrl.startsWith('data:') && window.firebaseUploadBillPhoto) {
    var _uploadPurchaseId = newPurchase.id;
    var _uploadDataUrl    = newPurchase.photoDataUrl;
    window.firebaseUploadBillPhoto(_uploadPurchaseId, _uploadDataUrl)
      .then(function(url) {
        var p2 = (state.purchases||[]).find(function(x){ return x.id === _uploadPurchaseId; });
        if (p2) { p2.photoDataUrl = url; save(); renderBillsList(); }
        toast('✅ Bill photo saved to cloud');
      })
      .catch(function(e) {
        // Upload failed — keep base64 locally as fallback so photo is not lost
        console.warn('Bill photo upload failed:', e.message);
        toast('Bill photo saved locally (cloud upload failed — will retry on next save)');
      });
  }

  renderInventory();
  renderHome();
  populateSupplierFilter();
  renderLedger();
  renderBillsList();
  window._billStage = null;

  var wrap = document.getElementById('bill-preview-wrap');
  var res  = document.getElementById('bill-scan-result');
  var stat = document.getElementById('bill-scan-status');
  if (wrap) wrap.style.display = 'none';
  if (res)  { res.style.display = 'none'; res.innerHTML = ''; }
  if (stat) stat.textContent = '';

  var msg = updated.length
    ? 'Stock updated:\n' + updated.map(function(u){ return '- '+u; }).join('\n')
    : 'No items were matched to your inventory.';
  if (skipped.length) msg += '\n\nSkipped (no match):\n' + skipped.map(function(u){ return '- '+u; }).join('\n');
  msg += '\n\nPurchase saved in Supplier Ledger.';

  showModal('Inventory Updated', '<div style="white-space:pre-wrap;font-size:13px;line-height:1.8;">' + msg + '</div>'
    + '<button class="btn mt-2" onclick="closeModal();showPurchaseSection(\'ledger\')">View Ledger</button>'
  );
}


// ============================================================
// DEMAND INBOX — customer parts requests from WhatsApp bot
// ============================================================
var _demandFilter = 'all';

function demandFilter(f, el) {
  _demandFilter = f;
  document.querySelectorAll('#demand-filter-chips .chip').forEach(c => c.classList.remove('active'));
  if (el) el.classList.add('active');
  renderDemandInbox();
}

function renderDemandInbox() {
  if (!state.demandInbox) state.demandInbox = [];
  var list = _demandFilter === 'all' ? state.demandInbox
    : state.demandInbox.filter(function(d) { return d.demandLevel === _demandFilter; });
  list = list.slice().sort(function(a,b) {
    var rank = { high:0, medium:1, low:2 };
    return (rank[a.demandLevel]||2) - (rank[b.demandLevel]||2) || (b.createdAt||0) - (a.createdAt||0);
  });

  var badge = document.getElementById('demand-count-badge');
  var highMed = state.demandInbox.filter(function(d){ return d.demandLevel==='high'||d.demandLevel==='medium'; }).length;
  if (badge) badge.textContent = highMed > 0 ? highMed + ' actionable' : state.demandInbox.length + ' total';

  var div = document.getElementById('demand-list');
  if (!div) return;
  if (!list.length) {
    div.innerHTML = '<div class="empty"><div class="icon">📡</div><div>No demand signals yet.<br><small>They will appear here when the WhatsApp bot captures them.</small></div></div>';
    return;
  }

  div.innerHTML = list.map(function(d) {
    var lvlColor = d.demandLevel === 'high' ? '#dc2626' : d.demandLevel === 'medium' ? '#d97706' : '#64748b';
    var lvlBg    = d.demandLevel === 'high' ? '#fef2f2' : d.demandLevel === 'medium' ? '#fffbeb' : '#f8fafc';
    var lvlBorder= d.demandLevel === 'high' ? '#fecaca' : d.demandLevel === 'medium' ? '#fde68a' : '#e2e8f0';
    var matchedPart = d.partNo ? state.parts.find(function(p){ return p.partNo === d.partNo; }) : null;
    return '<div style="background:' + lvlBg + ';border:1px solid ' + lvlBorder + ';border-radius:10px;padding:12px 14px;margin-bottom:8px;">'
      + '<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:6px;">'
      + '<div style="font-size:14px;font-weight:600;color:var(--text);flex:1;">' + (d.partDescription || d.partName || 'Unknown part') + '</div>'
      + '<span style="background:' + lvlColor + ';color:white;font-size:10px;font-weight:700;padding:2px 8px;border-radius:10px;letter-spacing:1px;text-transform:uppercase;margin-left:8px;">' + (d.demandLevel||'?') + '</span>'
      + '</div>'
      + (d.customerPhone ? '<div style="font-size:12px;color:var(--muted);">From: ' + d.customerPhone + '</div>' : '')
      + (d.machineType   ? '<div style="font-size:12px;color:var(--muted);">Machine: ' + d.machineType + '</div>' : '')
      + (d.notes         ? '<div style="font-size:12px;color:var(--muted);margin-top:2px;">' + d.notes + '</div>' : '')
      + '<div style="font-size:11px;color:var(--muted);margin-top:4px;">' + (d.createdAt ? new Date(d.createdAt).toLocaleDateString('en-IN',{day:'numeric',month:'short',hour:'2-digit',minute:'2-digit'}) : '') + '</div>'
      + (matchedPart
          ? '<div style="margin-top:8px;padding:6px 8px;background:#dcfce7;border-radius:6px;font-size:12px;color:#166534;">Now in stock: ' + matchedPart.name + ' (' + matchedPart.stock + ' ' + (matchedPart.unit||'pcs') + ')</div>'
          : '<div style="display:flex;gap:6px;margin-top:8px;">'
            + '<button class="btn btn-sm" style="background:#0f766e;font-size:12px;" onclick="openAddPartFromDemand(\'' + d.id + '\')">+ Add to Inventory</button>'
            + '<button class="btn btn-sm btn-outline" style="font-size:12px;color:#dc2626;border-color:#dc2626;" onclick="dismissDemand(\'' + d.id + '\')">Dismiss</button>'
            + '</div>')
      + '</div>';
  }).join('');
}

function openAddDemandEntry() {
  showModal('Add Demand Signal', `
    <div class="field"><label>Part Description *</label>
      <input class="input" id="dmd-desc" placeholder="e.g. JCB 3DX hydraulic cylinder seal kit"></div>
    <div class="row2">
      <div class="field"><label>Machine Type</label>
        <input class="input" id="dmd-machine" placeholder="JCB, Tractor…"></div>
      <div class="field"><label>Demand Level</label>
        <select class="select" id="dmd-level">
          <option value="high">High</option>
          <option value="medium" selected>Medium</option>
          <option value="low">Low</option>
        </select></div>
    </div>
    <div class="field"><label>Customer Phone (optional)</label>
      <input class="input" id="dmd-phone" placeholder="9876543210"></div>
    <div class="field"><label>Notes</label>
      <input class="input" id="dmd-notes" placeholder="Any extra context"></div>
    <button class="btn mt-2" onclick="saveDemandEntry()">Save Signal</button>
  `);
}

function saveDemandEntry() {
  var desc = document.getElementById('dmd-desc').value.trim();
  if (!desc) { toast('Part description required'); return; }
  if (!state.demandInbox) state.demandInbox = [];
  state.demandInbox.push({
    id: uid(),
    partDescription: desc,
    machineType: document.getElementById('dmd-machine').value.trim(),
    demandLevel: document.getElementById('dmd-level').value,
    customerPhone: document.getElementById('dmd-phone').value.trim(),
    notes: document.getElementById('dmd-notes').value.trim(),
    source: 'manual',
    createdAt: Date.now()
  });
  window._ownerDeleteInProgress = false;
  save();
  closeModal();
  renderDemandInbox();
  toast('Demand signal saved');
}

function openAddPartFromDemand(demandId) {
  var d = (state.demandInbox||[]).find(function(x){ return x.id === demandId; });
  if (!d) return;
  openAddPart(null, { name: d.partDescription, machineType: d.machineType || 'tractor' });
}

function dismissDemand(id) {
  if (!confirm('Dismiss this demand signal?')) return;
  state.demandInbox = (state.demandInbox||[]).filter(function(d){ return d.id !== id; });
  window._ownerDeleteInProgress = true;
  save().finally(function(){ window._ownerDeleteInProgress = false; });
  renderDemandInbox();
  toast('Signal dismissed');
}

// Called by WhatsApp bot webhook — writes a demand signal from bot into Firestore
// then save() will sync it to state here via the outbox
function botWriteDemandSignal(entry) {
  if (!state.demandInbox) state.demandInbox = [];
  var existing = state.demandInbox.find(function(d){
    return d.partDescription && entry.partDescription &&
      d.partDescription.toLowerCase() === entry.partDescription.toLowerCase();
  });
  if (existing) {
    existing.requestCount = (existing.requestCount || 1) + 1;
    if (entry.demandLevel === 'high') existing.demandLevel = 'high';
    else if (entry.demandLevel === 'medium' && existing.demandLevel === 'low') existing.demandLevel = 'medium';
    existing.lastRequestAt = Date.now();
  } else {
    state.demandInbox.push(Object.assign({ id: uid(), createdAt: Date.now(), requestCount: 1 }, entry));
  }
  save();
  renderDemandInbox();
}



// ── PART PHOTO HANDLER ───────────────────────────────────────
// Photos are compressed locally then uploaded to Firebase Storage.
// uap-photo-data holds either:
//   - a base64 dataUrl (pending upload, set before part has an ID)
//   - an https:// Storage URL (already uploaded)
//   - '__CLEAR__' (user removed photo)
//   - '' (no change)
function uapHandlePhoto(input) {
  var file = input.files && input.files[0];
  if (!file) return;
  var preview = document.getElementById('uap-photo-preview');
  var clearBtn = document.getElementById('uap-photo-clear');
  if (preview) preview.innerHTML = '<div style="width:100%;height:100%;display:flex;align-items:center;justify-content:center;font-size:10px;color:#0f766e;font-weight:600;">🔍 Scanning…</div>';
  var reader = new FileReader();
  reader.onload = function(e) {
    var img = new Image();
    img.onload = function() {
      var MAX = 1600;
      var scale = img.width > MAX ? MAX / img.width : 1;
      var canvas = document.createElement('canvas');
      canvas.width  = Math.round(img.width  * scale);
      canvas.height = Math.round(img.height * scale);
      canvas.getContext('2d').drawImage(img, 0, 0, canvas.width, canvas.height);
      var dataUrl = canvas.toDataURL('image/jpeg', 0.92);
      var dataEl = document.getElementById('uap-photo-data');
      if (dataEl) dataEl.value = dataUrl;
      if (preview) preview.innerHTML = '<img src="'+dataUrl+'" style="width:100%;height:100%;object-fit:cover;border-radius:6px;">';
      if (clearBtn) clearBtn.style.display = 'inline-block';
      // Trigger AI extraction for owner form too
      uapAiExtractFromPhoto(dataUrl);
    };
    img.src = e.target.result;
  };
  reader.readAsDataURL(file);
}

// AI extraction for owner/edit add-part form (uap = Universal Add Part)
async function uapAiExtractFromPhoto(dataUrl) {
  var key = getAIKey();
  var hint = document.getElementById('uap-ai-hint');
  if (!key || !key.startsWith('sk-')) {
    if (hint) { hint.textContent = '⚠️ AI key not active — open Settings → AI and unlock with your PIN'; hint.style.color = 'var(--muted)'; hint.style.display = 'block'; }
    return;
  }

  if (hint) { hint.textContent = '🔍 Reading part box…'; hint.style.color = 'var(--primary)'; hint.style.display = 'block'; }

  async function callClaude(promptText, maxTokens) {
    return aiCall({ prompt: promptText, image: { dataUrl: dataUrl }, maxTokens: maxTokens || 600, vision: true, stripFences: true });
  }

  function parseJSON(raw) {
    try { var s=raw.indexOf('{'),e=raw.lastIndexOf('}'); return s>=0&&e>s?JSON.parse(raw.slice(s,e+1)):null; } catch(_){return null;}
  }
  function isEmpty(ex) { return !ex || (!ex.partNo && !ex.name && !ex.brand && !ex.mrp); }

  try {
    var knownBrands = [...new Set((state.parts||[]).map(function(p){return p.brand;}).filter(Boolean))].slice(0,30).join(', ');
    var knownCats   = [...new Set((state.parts||[]).map(function(p){return p.category||'';}).filter(Boolean))].slice(0,20).join(', ');

    var p1 = 'You are an expert in Indian agricultural and construction machinery spare parts.\n' +
      'Known brands: ' + (knownBrands||'SKF, FAG, NTN, Mahindra, John Deere, Bosch, Gates') + '\n' +
      'Known categories: ' + (knownCats||'Bearing, Filter, Belt, Clutch, Hydraulic, Seal, Gear, Chain, Blade') + '\n\n' +
      'Read ALL visible text on this part/box carefully.\n' +
      'Return ONLY JSON: {"partNo":"...","name":"...","brand":"...","mrp":null,"category":"...","machineType":"tractor|harvester|jcb|rotavator|other","models":"..."}\n' +
      'null for anything not visible — do NOT invent.';

    var ex = parseJSON(await callClaude(p1, 600));

    if (isEmpty(ex)) {
      if (hint) hint.textContent = '🔍 Looking closer…';
      var p2 = 'Look very carefully — check stamped codes, stickers, embossed numbers, all sides visible.\n' +
        'Part numbers look like: 6205, AL110864, 332/D2275, MS571\n' +
        'Known brands: ' + (knownBrands||'SKF, FAG, NTN, Mahindra') + '\n' +
        'Return ONLY JSON: {"partNo":"...","name":"...","brand":"...","mrp":null,"category":"...","machineType":"...","models":"..."}\n' +
        'null for fields you truly cannot see.';
      var ex2 = parseJSON(await callClaude(p2, 500));
      if (!isEmpty(ex2)) ex = ex2;
    }

    // ── PASS 3: visual inference ──────────────────────────────────────────────
    if (!ex || isEmpty(ex)) {
      if (hint) hint.textContent = '🔍 Inferring from shape…';
      try {
        var p3 = 'Cannot read text — describe what you see visually.\n' +
          'Known categories: ' + (knownCats || 'Bearing, Filter, Belt, Clutch, Hydraulic, Seal, Gear, Chain, Blade') + '\n' +
          'Return ONLY JSON: {"partNo":null,"name":"visual description e.g. Ball Bearing, Oil Filter","brand":null,"mrp":null,' +
          '"category":"category","machineType":"tractor|harvester|jcb|rotavator|other","models":null}\n' +
          'Fill name, category, machineType only — null for the rest.';
        var ex3 = parseJSON(await callClaude(p3, 300));
        if (ex3 && (ex3.name || ex3.category)) ex = ex3;
      } catch(e3) {}
    }

    if (!ex || (!ex.partNo && !ex.name && !ex.brand && !ex.category)) {
      if (hint) { hint.textContent = 'Could not read — retake the part label closer'; hint.style.color = 'var(--muted)'; }
      return;
    }

    var filled = [];
    function _f(id, val) {
      if (!val) return false;
      var el = document.getElementById(id);
      if (!el) return false;
      if ((el.tagName==='SELECT'?!el.value:!el.value.trim()) === false) return false;
      el.value = val; el.style.background = '#f0fdf4'; return true;
    }
    if (ex.partNo   && _f('uap-partno',   String(ex.partNo).toUpperCase())) filled.push('Part No');
    if (ex.name     && _f('uap-name',     ex.name))     filled.push('Name');
    if (ex.brand    && _f('uap-brand',    ex.brand))    filled.push('Brand');
    if (ex.mrp > 0  && _f('uap-mrp',     ex.mrp))      filled.push('MRP');
    if (ex.category && _f('uap-category', ex.category)) filled.push('Category');
    if (ex.models   && _f('uap-models',   ex.models))   filled.push('Models');
    if (ex.machineType) {
      var mtEl = document.getElementById('uap-machine');
      if (mtEl && !mtEl.value) {
        var mt = String(ex.machineType).toLowerCase();
        var match = Array.from(mtEl.options).map(function(o){return o.value;}).find(function(v){return v&&mt.includes(v);});
        if (match) { mtEl.value=match; mtEl.style.background='#f0fdf4'; filled.push('Machine'); }
      }
    }
    setTimeout(function(){
      ['uap-partno','uap-name','uap-brand','uap-mrp','uap-category','uap-models','uap-machine'].forEach(function(id){
        var el=document.getElementById(id); if(el) el.style.background='';
      });
    }, 4000);

    if (hint) {
      hint.style.color = filled.length ? 'var(--success)' : 'var(--muted)';
      hint.textContent = filled.length ? '✓ Filled: ' + filled.join(', ') : '✓ Photo saved — fill manually';
    }
  } catch(err) {
    if (hint) { hint.textContent = 'Scan failed: ' + (err.message||'network error'); hint.style.color = 'var(--muted)'; }
  }
}

function uapClearPhoto() {
  var dataEl = document.getElementById('uap-photo-data');
  if (dataEl) dataEl.value = '__CLEAR__';
  var preview = document.getElementById('uap-photo-preview');
  if (preview) preview.innerHTML = '📷';
  var clearBtn = document.getElementById('uap-photo-clear');
  if (clearBtn) clearBtn.style.display = 'none';
}


// ── HEAL & SYNC stranded parts → guarantee every part reaches the database ──
// A part is "stranded" when its photo is still a base64 data: URL (too big for
// Firestore's 1 MB doc limit) and/or the record never reached the server (no
// _serverTs). This routine, in order:
//   1) uploads any base64 photo to Storage and swaps in the https:// URL,
//   2) force-enqueues every stranded part into the parts-cloud queue,
//   3) drains that queue so each part is written to Firestore (with _serverTs).
// Unlike the old migrator it is AUTH-AWARE (waits & retries instead of bailing),
// idempotent, and reschedules itself until nothing is left local-only.
var _healPartsRunning = false;
var _healPartsDone = false;
var _healPartsAttempts = 0;
// Persisted across reloads. It was session-only, so every refresh re-marked every
// part as stranded and restarted the enqueue/drain cycle from scratch.
var _HEALED_KEY = '_partsHealed_v1';
var _healedThisSession = (function(){
  try { return new Set(JSON.parse(localStorage.getItem(_HEALED_KEY) || '[]')); }
  catch(e) { return new Set(); }
})();
function _healedPersist() {
  try {
    // Bounded so this can never become the thing that fills localStorage.
    var arr = Array.from(_healedThisSession).slice(-15000);
    localStorage.setItem(_HEALED_KEY, JSON.stringify(arr));
  } catch(e) { /* cache only — _serverTs in state is the real marker */ }
}

function _partNeedsHeal(p) {
  if (!p || !p.id) return false;
  if (p.photoDataUrl && String(p.photoDataUrl).startsWith('data:')) return true; // photo must move to Storage
  if (!p._serverTs && !_healedThisSession.has(p.id)) return true;                // never reached server
  return false;
}

async function _runHealStrandedParts(opts) {
  opts = opts || {};
  var verbose = opts.verbose !== false;
  if (_healPartsRunning) return;
  _healPartsRunning = true;
  try {
    if (!Array.isArray(state.parts)) return;

    var stranded = state.parts.filter(_partNeedsHeal);
    if (!stranded.length) {
      _healPartsDone = true;
      if (verbose) console.log('[heal] ✅ All parts are on the database.');
      return;
    }

    // Need auth + connectivity to do anything. If not ready, reschedule (don't give up).
    if (!navigator.onLine || !window._firebaseAuthed ||
        !window.firebaseUploadPartPhoto || !window.firestoreSetPart) {
      if (verbose) console.log('[heal] waiting for auth/online — ' + stranded.length + ' part(s) pending; will retry.');
      _scheduleHealRetry();
      return;
    }

    _healPartsAttempts++;
    if (verbose) console.log('[heal] pass ' + _healPartsAttempts + ' — ' + stranded.length + ' stranded part(s).');

    // ── Phase 1: base64 photos → Firebase Storage URL ──
    var b64 = stranded.filter(function(p){ return p.photoDataUrl && String(p.photoDataUrl).startsWith('data:'); });
    var photoOk = 0, photoFail = 0;
    for (var i = 0; i < b64.length; i++) {
      var bp = b64[i];
      try {
        var url = await window.firebaseUploadPartPhoto(bp.id, bp.photoDataUrl);
        var live = getPartById(bp.id) || bp;
        live.photoDataUrl = url;
        photoOk++;
      } catch (e) {
        photoFail++;
        console.warn('[heal] photo upload failed for ' + (bp.partNo || bp.id) + ': ' + (e && e.message));
      }
      await new Promise(function(r){ setTimeout(r, 120); }); // gentle on Storage
    }
    if (photoOk) { try { await save(); } catch(e) {} } // persist URL swaps locally first

    // ── Phase 2: enqueue stranded parts, in bounded batches ──
    // CAP: previously this force-enqueued EVERY stranded part on every pass. Combined
    // with the _serverTs bug (parts never lost their "stranded" flag, so the same
    // hundreds of records were re-added each pass while the drain could only clear
    // 25 per cycle), the pending count could never reach zero — which is exactly what
    // pinned the status pill at a near-constant "N parts syncing" indefinitely.
    // Enqueue at most one drain-cycle's worth of NEW work per pass; the retry timer
    // brings the rest along once the queue has actually drained.
    var _alreadyPending = _partsPendingLoad();
    var _toQueue = stranded.filter(function(p){ return !_alreadyPending[p.id]; }).slice(0, 100);
    _toQueue.forEach(function(p){ partsPendingEnqueue(p.id, 'heal'); });
    if (verbose) console.log('[heal] queued ' + _toQueue.length + ' of ' + stranded.length +
      ' stranded (' + Object.keys(_alreadyPending).length + ' already pending).');

    // ── Phase 3: drain the queue → Firestore (firestoreSetPart sets _serverTs) ──
    var pushed = 0, guard = 0;
    while (partsPendingCount() > 0 && guard < 25) {
      guard++;
      var res = await drainPartsCloudPending();
      pushed += (res && res.synced) || 0;
      if (!res || !res.synced) { await new Promise(function(r){ setTimeout(r, 900); }); } // contention/backoff
    }

    // Mark as healed exactly the parts that left the pending queue (i.e. wrote OK).
    var pendingNow = _partsPendingLoad();
    stranded.forEach(function(p){ if (!pendingNow[p.id]) _healedThisSession.add(p.id); });
    _healedPersist();

    var remaining = state.parts.filter(_partNeedsHeal).length;
    if (verbose) {
      console.log('[heal] photos ' + photoOk + ' ok / ' + photoFail + ' failed · pushed ' + pushed +
                  ' · still local-only: ' + remaining);
    }
    try { renderInventory(); } catch(e) {}

    if (remaining > 0 || photoFail > 0) {
      _scheduleHealRetry(); // keep going until everything lands
    } else {
      _healPartsDone = true;
      if (verbose) {
        console.log('[heal] ✅ Done — every part is now on the database.');
        try { toast('✅ ' + pushed + ' part(s) synced to cloud'); } catch(e) {}
      }
    }
  } finally {
    _healPartsRunning = false;
  }
}

function _scheduleHealRetry() {
  if (_healPartsDone || _healPartsAttempts > 40) return;
  var delay = Math.min(5000 + _healPartsAttempts * 3000, 30000);
  setTimeout(function(){ _runHealStrandedParts({ verbose: false }).catch(function(){}); }, delay);
}

// Back-compat: boot still calls migratePartPhotosToStorage()
async function migratePartPhotosToStorage() { return _runHealStrandedParts({ verbose: true }); }

// Manual controls (console / button):
//   healStrandedParts()  → force a run now
//   partsStillLocal()    → how many parts are not yet on the database
window.healStrandedParts = function(){ _healPartsDone = false; return _runHealStrandedParts({ verbose: true }); };
window.partsStillLocal = function(){
  var n = (state.parts || []).filter(_partNeedsHeal).length;
  console.log(n + ' part(s) still local-only (not on database).');
  return n;
};
// Silent count (used for the button label).
function partsStillLocalCount(){ return (state.parts || []).filter(_partNeedsHeal).length; }
window.partsStillLocalCount = partsStillLocalCount;

// Button handler: heal + sync, with before/after feedback and modal refresh.
async function healStrandedPartsUI(){
  var before = partsStillLocalCount();
  if (!before) { try { toast('✅ All parts already on the database'); } catch(e){} return; }
  if (!navigator.onLine || !window._firebaseAuthed) {
    try { toast('⚠️ Connect & sign in first — then tap again'); } catch(e){}
    return;
  }
  try { toast('🩹 Syncing ' + before + ' part(s)…'); } catch(e){}
  _healPartsDone = false;
  try { await _runHealStrandedParts({ verbose: true }); } catch(e){ console.warn("[heal] UI run error:", e && e.message); }
  var after = partsStillLocalCount();
  var done = before - after;
  try {
    toast(after === 0 ? ('✅ All ' + before + ' part(s) now on database')
                      : ('Synced ' + done + ' · ' + after + ' still pending — retrying'));
  } catch(e){}
  try { if (typeof rxOpenDataPanel === 'function') rxOpenDataPanel('actions'); } catch(e){}
}
window.healStrandedPartsUI = healStrandedPartsUI;

// Button handler: read the REAL cloud parts count and compare with this device.
async function cloudVsLocalCountUI(){
  var local = (state.parts || []).length;
  if (!navigator.onLine || !window._firebaseAuthed || !window.firestorePullAllParts) {
    var off = '<div style="padding:8px;font-size:14px;color:#b91c1c;line-height:1.6;">'
            + 'Can\'t read the cloud right now — the app is offline or not signed in, '
            + 'so sync is down. Fix the connection first, then tap again.<br><br>'
            + '📱 This device currently holds <b>' + local + '</b> parts locally (safe).</div>';
    try { showModal('☁ Cloud Parts Count', off); } catch(e){ try{ alert('Offline / not signed in. Local parts: ' + local); }catch(_){} }
    return;
  }
  try { toast('☁ Counting cloud parts…'); } catch(e){}
  var cloud = null;
  try { cloud = await window.firestorePullAllParts(); } catch(e){ cloud = null; }
  if (cloud === null) {
    var err = '<div style="padding:8px;font-size:14px;color:#b91c1c;line-height:1.6;">'
            + 'The cloud read failed — usually means authentication dropped or writes are '
            + 'blocked. This is the same thing stopping your sync.<br><br>'
            + '📱 Local parts on this device: <b>' + local + '</b> (nothing lost).</div>';
    try { showModal('☁ Cloud Parts Count', err); } catch(e){ try{ alert('Cloud read failed. Local parts: ' + local); }catch(_){} }
    return;
  }
  var cloudN = cloud.length;
  var diff = local - cloudN;
  var verdict = diff > 0 ? ('⚠️ <b>' + diff + '</b> part(s) on this device are <b>not on the cloud yet</b>.')
              : diff < 0 ? ('ℹ️ Cloud has <b>' + (-diff) + '</b> more than this device — refresh to pull them down.')
              : '✅ Cloud and this device match exactly.';
  var msg = '<div style="padding:6px 4px;font-size:15px;line-height:1.9;">'
          + '<div>☁ <b>Cloud (Firestore):</b> ' + cloudN + ' parts</div>'
          + '<div>📱 <b>This device:</b> ' + local + ' parts</div>'
          + '<div style="margin-top:10px;padding-top:10px;border-top:1px solid var(--border);font-size:14px;">'
          + verdict + '</div></div>';
  try { showModal('☁ Cloud Parts Count', msg); } catch(e){ try{ alert('Cloud: ' + cloudN + ' · Local: ' + local); }catch(_){} }
}
window.cloudVsLocalCountUI = cloudVsLocalCountUI;

// Button handler: attempt ONE write to a throwaway doc and show the exact
// Firestore error code. This is the decisive test for "reads work, writes don't".
async function cloudWriteTestUI(){
  if (!navigator.onLine || !window._firebaseAuthed || !window.firebaseAppendDoc) {
    try { showModal('🧪 Cloud Write Test', '<div style="padding:8px;font-size:14px;color:#b91c1c;">Offline or not signed in — can\'t test a write yet.</div>'); } catch(e){}
    return;
  }
  try { toast('🧪 Testing a cloud write…'); } catch(e){}
  var testId = '_wtest_' + Date.now();
  var meaning = function(code){
    if (code === 'resource-exhausted') return 'Daily Firestore quota is used up (free plan ≈ 20,000 writes/day). Reads still work, writes are blocked until it resets at midnight US-Pacific (early afternoon your time). Nothing is lost — queued writes flush after reset, or instantly if you move to the Blaze plan.';
    if (code === 'permission-denied')  return 'Your Firestore Security Rules are refusing writes. Check the Rules tab in the Firebase console.';
    if (code === 'unauthenticated')    return 'The sign-in session is not valid for writes. Fully close and reopen the app to re-authenticate.';
    if (code === 'unavailable' || code === 'deadline-exceeded') return 'Temporary network/backend hiccup. Try again in a moment on a stable connection.';
    return 'Unrecognised error — send this exact code and message to Claude.';
  };
  try {
    await window.firebaseAppendDoc('_diagnostics', testId, { ts: Date.now(), via: 'write-test' });
    try { window.firebaseDeleteDoc('_diagnostics', testId).catch(function(){}); } catch(e){}
    var okMsg = '<div style="padding:8px;font-size:14px;line-height:1.7;">'
              + '<div style="color:#16a34a;font-weight:700;font-size:16px;">✅ Write succeeded</div>'
              + 'Writes ARE reaching the cloud right now. So the stall is in the local queue, not the connection. '
              + 'Tap “🔁 Retry Pending Writes”, then “🩹 Heal &amp; Sync Stranded Parts”. If parts still don\'t go up, tell Claude — it\'s a queue issue.</div>';
    try { showModal('🧪 Cloud Write Test', okMsg); } catch(e){ try{ alert('Write OK — connection fine.'); }catch(_){} }
  } catch(e){
    var code = (e && e.code) || '(no code)';
    var msg  = (e && e.message) || String(e);
    var html = '<div style="padding:8px;font-size:14px;line-height:1.7;">'
             + '<div style="color:#b91c1c;font-weight:700;font-size:16px;">❌ Write blocked</div>'
             + '<div style="margin:6px 0;"><b>Error code:</b> <code style="background:#fee2e2;padding:2px 6px;border-radius:4px;">' + code + '</code></div>'
             + '<div style="margin:6px 0;color:#475569;font-size:12px;word-break:break-word;">' + msg + '</div>'
             + '<div style="margin-top:10px;padding-top:10px;border-top:1px solid var(--border);">' + meaning(code) + '</div></div>';
    try { showModal('🧪 Cloud Write Test', html); } catch(e2){ try{ alert('Write blocked: ' + code + ' — ' + msg); }catch(_){} }
  }
}
window.cloudWriteTestUI = cloudWriteTestUI;

// Decisive diagnostic for "writes work but the 98 don't go up".
// Writes ONE real stranded part to the parts collection, reads the cloud back
// to confirm it actually landed, and surfaces the error the queue recorded.
async function diagnosePartsSyncUI(){
  function row(k,v){ return '<div><b>'+k+':</b> '+v+'</div>'; }
  var L = [];
  L.push(row('Online', navigator.onLine));
  L.push(row('Authenticated', !!window._firebaseAuthed));
  var stranded = (state.parts||[]).filter(_partNeedsHeal);
  L.push(row('Detected as not-on-cloud', stranded.length));
  // queue state + recorded error from the last heal attempt
  var pmap = {};
  try { pmap = (typeof _partsPendingLoad==='function') ? _partsPendingLoad() : {}; } catch(e){}
  var pids = Object.keys(pmap);
  L.push(row('In parts-cloud queue', pids.length));
  var recErr = '';
  for (var i=0;i<pids.length;i++){ if (pmap[pids[i]] && pmap[pids[i]].lastError){ recErr = pmap[pids[i]].lastError; break; } }
  if (recErr) L.push(row('Queue recorded error', '<code style="background:#fee2e2;padding:1px 5px;border-radius:4px;word-break:break-word;">'+recErr+'</code>'));

  if (!navigator.onLine || !window._firebaseAuthed) {
    L.push('<div style="margin-top:8px;color:#b91c1c;">Offline / not signed in — fix that first.</div>');
    try { showModal('🔍 Parts Sync Diagnosis', '<div style="padding:8px;font-size:13px;line-height:1.8;">'+L.join('')+'</div>'); } catch(e){}
    return;
  }
  if (!stranded.length) {
    L.push('<div style="margin-top:8px;color:#16a34a;">Nothing is detected as missing on this device.</div>');
    try { showModal('🔍 Parts Sync Diagnosis', '<div style="padding:8px;font-size:13px;line-height:1.8;">'+L.join('')+'</div>'); } catch(e){}
    return;
  }

  // ── Live test: write ONE real stranded part, then read cloud to confirm ──
  try { toast('🔍 Writing one test part to cloud…'); } catch(e){}
  var sample = stranded[0];
  var writeErr = '';
  try {
    await window.firestoreSetPart(_partPayloadForCloud(sample));
  } catch(e){ writeErr = (e && e.code) || (e && e.message) || String(e); }

  var landed = null;
  try {
    var cloud = await window.firestorePullAllParts();
    if (cloud) landed = cloud.some(function(p){ return p && p.id === sample.id; });
  } catch(e){}

  L.push('<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--border);"><b>Live write test</b> (part '+(sample.partNo||sample.id)+')</div>');
  var verdict;
  if (writeErr) {
    L.push(row('Write threw', '<code style="background:#fee2e2;padding:1px 5px;border-radius:4px;word-break:break-word;">'+writeErr+'</code>'));
    if (writeErr.indexOf('permission-denied')>=0) verdict = '🔒 Your Firestore Security Rules are blocking writes to the parts collection. That\'s the cause — the rules need to allow it.';
    else if (writeErr.indexOf('resource-exhausted')>=0) verdict = '📊 Write quota is exhausted. Resets midnight US-Pacific, or move to Blaze plan.';
    else verdict = 'Send this exact code to Claude.';
  } else if (landed === true) {
    verdict = '✅ The parts-write path WORKS — that part is now on the cloud. The heal\'s push is being dropped before it writes (auth flickering, or queue marking it done without writing). This is fixable in code — tell Claude "direct write landed".';
  } else if (landed === false) {
    verdict = '⚠️ Write returned no error but the part did NOT appear in the cloud. That means auth silently dropped the write. Fully close & reopen the app, then retry — and tell Claude "silent no-op confirmed".';
  } else {
    verdict = 'Could not read the cloud back to confirm. Tell Claude what the queue error above says.';
  }
  L.push('<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--border);font-size:13px;">'+verdict+'</div>');
  try { showModal('🔍 Parts Sync Diagnosis', '<div style="padding:8px;font-size:13px;line-height:1.8;">'+L.join('')+'</div>'); }
  catch(e){ try{ alert('stranded '+stranded.length+' · queueErr '+(recErr||'-')+' · writeErr '+(writeErr||'-')+' · landed '+landed); }catch(_){} }
}
window.diagnosePartsSyncUI = diagnosePartsSyncUI;



// ============================================================
// INCOMING STOCK — parts ordered, not yet arrived
// Each entry: { id, partId, partName, partNo, qty, unit,
//               supplierName, orderedDate, expectedDays, notes }
// ============================================================

function renderIncomingStock() {
  if (!state.incomingStock) state.incomingStock = [];
  var div = document.getElementById('incoming-list');
  if (!div) return;

  if (!state.incomingStock.length) {
    div.innerHTML = '<div class="empty"><div class="icon">📦</div><div>No incoming stock.<br><small>Add an entry when you order parts from a supplier.</small></div></div>';
    return;
  }

  // Sort: soonest arriving first, then oldest order
  var list = state.incomingStock.slice().sort(function(a, b) {
    return (a.expectedDays || 99) - (b.expectedDays || 99);
  });

  div.innerHTML = list.map(function(item) {
    var daysText = item.expectedDays
      ? (item.expectedDays === 1 ? 'Arriving tomorrow' : 'Arriving in ' + item.expectedDays + ' days')
      : 'ETA not set';
    var daysColor = !item.expectedDays ? '#64748b'
      : item.expectedDays <= 1 ? '#16a34a'
      : item.expectedDays <= 3 ? '#d97706' : '#0e7490';
    var ordDate = item.orderedDate ? new Date(item.orderedDate).toLocaleDateString('en-IN', {day:'numeric',month:'short'}) : '';
    return (
      '<div style="background:var(--card);border:1px solid var(--border);border-left:4px solid #a5f3fc;border-radius:10px;padding:12px 14px;margin-bottom:8px;">' +
        '<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:6px;">' +
          '<div>' +
            '<div style="font-size:14px;font-weight:600;">' + (item.partName || 'Unknown part') + '</div>' +
            '<div style="font-size:11px;color:var(--muted);font-family:monospace;">' + (item.partNo || '') + '</div>' +
          '</div>' +
          '<div style="text-align:right;">' +
            '<div style="font-size:16px;font-weight:700;color:#0e7490;">+' + item.qty + ' ' + (item.unit || 'pcs') + '</div>' +
            '<div style="font-size:11px;font-weight:600;color:' + daysColor + ';">' + daysText + '</div>' +
          '</div>' +
        '</div>' +
        (item.supplierName ? '<div style="font-size:12px;color:var(--muted);">Supplier: ' + item.supplierName + '</div>' : '') +
        (ordDate ? '<div style="font-size:12px;color:var(--muted);">Ordered: ' + ordDate + '</div>' : '') +
        (item.notes ? '<div style="font-size:12px;color:var(--muted);margin-top:2px;">' + item.notes + '</div>' : '') +
        '<div style="display:flex;gap:8px;margin-top:10px;">' +
          '<button class="btn btn-sm btn-success" style="font-size:12px;" onclick="markIncomingArrived(\'' + item.id + '\')">✅ Arrived — add to stock</button>' +
          '<button class="btn btn-sm" style="font-size:12px;background:#f1f5f9;color:var(--muted);" onclick="editIncoming(\'' + item.id + '\')">Edit</button>' +
          '<button class="btn btn-sm" style="font-size:12px;background:none;color:var(--danger);border:1px solid var(--danger);" onclick="deleteIncoming(\'' + item.id + '\')">✕</button>' +
        '</div>' +
      '</div>'
    );
  }).join('');
}

function openAddIncoming() {
  // Build parts dropdown
  var partOptions = '<option value="">— select part —</option>' +
    state.parts.map(function(p) {
      return '<option value="' + p.id + '">' + p.name + ' (' + p.partNo + ')</option>';
    }).join('');
  var supplierOptions = '<option value="">— select supplier —</option>' +
    (state.suppliers || []).map(function(s) {
      return '<option value="' + s.name + '">' + s.name + '</option>';
    }).join('');

  showModal('📦 Add Incoming Stock', `
    <div class="field"><label>Part *</label>
      <select class="select" id="inc-part" onchange="incAutoFill(this.value)">${partOptions}</select>
    </div>
    <div class="row2">
      <div class="field"><label>Quantity *</label>
        <input type="number" class="input" id="inc-qty" placeholder="0" min="1"></div>
      <div class="field"><label>Unit</label>
        <input class="input" id="inc-unit" placeholder="pcs"></div>
    </div>
    <div class="row2">
      <div class="field"><label>Supplier</label>
        <select class="select" id="inc-supplier">${supplierOptions}</select></div>
      <div class="field"><label>Arriving in (days)</label>
        <input type="number" class="input" id="inc-days" placeholder="e.g. 3" min="1" max="60"></div>
    </div>
    <div class="field"><label>Notes</label>
      <input class="input" id="inc-notes" placeholder="PO number, batch, anything useful"></div>
    <button class="btn mt-2" onclick="saveIncoming()">Save</button>
  `);
}

function incAutoFill(partId) {
  var p = getPartById(partId);
  if (!p) return;
  var unitEl = document.getElementById('inc-unit');
  if (unitEl && !unitEl.value) unitEl.value = p.unit || 'pcs';
  // Auto-select supplier if part has one
  var supEl = document.getElementById('inc-supplier');
  if (supEl && p.supplierName) {
    for (var i = 0; i < supEl.options.length; i++) {
      if (supEl.options[i].value === p.supplierName) { supEl.selectedIndex = i; break; }
    }
  }
}

function saveIncoming() {
  var partId  = document.getElementById('inc-part').value;
  var qty     = +document.getElementById('inc-qty').value || 0;
  if (!partId) { toast('Select a part'); return; }
  if (!qty)    { toast('Enter quantity'); return; }
  var part = getPartById(partId);
  if (!state.incomingStock) state.incomingStock = [];
  var entry = {
    id:           uid(),
    partId:       partId,
    partName:     part ? part.name    : '',
    partNo:       part ? part.partNo  : '',
    qty:          qty,
    unit:         document.getElementById('inc-unit').value.trim() || (part ? part.unit : 'pcs') || 'pcs',
    supplierName: document.getElementById('inc-supplier').value,
    expectedDays: +document.getElementById('inc-days').value || 0,
    notes:        document.getElementById('inc-notes').value.trim(),
    orderedDate:  Date.now()
  };
  state.incomingStock.push(entry);
  // Also update the part's stockOnWay and expectedDays for badge display
  if (part) {
    part.stockOnWay   = (part.stockOnWay || 0) + qty;
    part.expectedDays = entry.expectedDays || part.expectedDays || 0;
  }
  save();
  closeModal();
  renderIncomingStock();
  renderInventory();
  toast('📦 Incoming stock recorded');
}

function editIncoming(id) {
  var item = (state.incomingStock || []).find(function(x){ return x.id === id; });
  if (!item) return;
  showModal('Edit Incoming', `
    <div class="row2">
      <div class="field"><label>Quantity</label>
        <input type="number" class="input" id="einc-qty" value="${item.qty}"></div>
      <div class="field"><label>Arriving in (days)</label>
        <input type="number" class="input" id="einc-days" value="${item.expectedDays || ''}"></div>
    </div>
    <div class="field"><label>Notes</label>
      <input class="input" id="einc-notes" value="${item.notes || ''}"></div>
    <button class="btn mt-2" onclick="saveEditIncoming('${id}')">Update</button>
  `);
}

function saveEditIncoming(id) {
  var item = (state.incomingStock || []).find(function(x){ return x.id === id; });
  if (!item) return;
  var oldQty = item.qty;
  var newQty = +document.getElementById('einc-qty').value || oldQty;
  item.qty          = newQty;
  item.expectedDays = +document.getElementById('einc-days').value || 0;
  item.notes        = document.getElementById('einc-notes').value.trim();
  // Adjust part stockOnWay
  var part = getPartById(item.partId);
  if (part) {
    part.stockOnWay = Math.max(0, (part.stockOnWay || 0) - oldQty + newQty);
    part.expectedDays = item.expectedDays || part.expectedDays;
  }
  save(); closeModal(); renderIncomingStock(); renderInventory();
  toast('Updated');
}

function markIncomingArrived(id) {
  var item = (state.incomingStock || []).find(function(x){ return x.id === id; });
  if (!item) return;
  var part = getPartById(item.partId);
  if (!part) { toast('Part not found in inventory'); return; }
  if (!confirm('Add ' + item.qty + ' ' + item.unit + ' of "' + item.partName + '" to stock?')) return;
  // Add to stock
  part.stock = (part.stock || 0) + item.qty;
  // Reduce stockOnWay
  part.stockOnWay = Math.max(0, (part.stockOnWay || 0) - item.qty);
  if (part.stockOnWay === 0) part.expectedDays = 0;
  // Remove from incoming list
  state.incomingStock = state.incomingStock.filter(function(x){ return x.id !== id; });
  window._ownerDeleteInProgress = true;
  save().finally(function(){ window._ownerDeleteInProgress = false; });
  renderIncomingStock();
  renderInventory();
  renderHome();
  toast('✅ Stock updated: ' + part.name + ' now ' + part.stock + ' ' + part.unit);
}

function deleteIncoming(id) {
  if (!confirm('Remove this incoming entry?')) return;
  var item = (state.incomingStock || []).find(function(x){ return x.id === id; });
  // Subtract from part stockOnWay
  if (item) {
    var part = getPartById(item.partId);
    if (part) {
      part.stockOnWay = Math.max(0, (part.stockOnWay || 0) - item.qty);
      if (part.stockOnWay === 0) part.expectedDays = 0;
    }
  }
  state.incomingStock = (state.incomingStock || []).filter(function(x){ return x.id !== id; });
  window._ownerDeleteInProgress = true;
  save().finally(function(){ window._ownerDeleteInProgress = false; });
  renderIncomingStock();
  renderInventory();
  toast('Removed');
}
