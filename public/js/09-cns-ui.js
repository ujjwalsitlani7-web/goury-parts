/* ═══════════════════════════════════════════════════════════════════════════
   CNS UI CONTROLLER
   ═══════════════════════════════════════════════════════════════════════════ */
const CNS_UI = (function() {
  let _tab     = 'status';
  let _open    = false;
  let _evtIdx  = 0; // for event feed rendering

  // Update FAB badge with live event count
  function _updateBadge() {
    const cnt = document.getElementById('cns-fab-count');
    if (!cnt || !window.GouryOS) return;
    const n = GouryOS._events.length;
    cnt.textContent = n > 99 ? '99+' : n;
    cnt.classList.toggle('show', n > 0);
  }

  // Subscribe to all events to trigger badge + re-render
  window.addEventListener('load', function() {
    setTimeout(function() {
      if (!window.GouryOS) return;
      GouryOS.on('*', function() {
        _updateBadge();
        if (_open && (_tab === 'status' || _tab === 'events' || _tab === 'intel')) {
          _render();
        }
      });
    }, 1800);
  });

  function toggle() {
    _open = !_open;
    document.getElementById('cns-panel').classList.toggle('open', _open);
    if (_open) {
      _updateVitals();
      _render();
    }
  }

  function tab(name) {
    _tab = name;
    document.querySelectorAll('.cns-tab-btn').forEach(b => b.classList.toggle('active', b.id === 'cnstab-' + name));
    _render();
  }

  function _updateVitals() {
    if (!window.GouryOS) return;
    const h  = GouryOS.health().data;
    const iv = GouryOS.compute('inventory.health').data;
    const sv = GouryOS.compute('sales.summary', {days:30}).data;
    const _$ = (id, v) => { const el=document.getElementById(id); if(el) el.textContent=v; };
    _$('cv-skus', iv.totalSKUs||0);  // ALL parts, not just saleable
    _$('cv-low', (iv.outOfStock||0) + (iv.lowStock||0));
    _$('cv-sales', sv.transactions||0);
    _$('cv-apps', h.appsCount||0);
    const sub = document.getElementById('cns-sub');
    if (sub) sub.textContent = 'v' + h.version + ' · ' + h.partsCount + ' SKUs · uptime ' + h.uptime + (h.firebaseOk ? ' · Firebase ✓' : ' · Firebase pending');
  }

  function _render() {
    const body = document.getElementById('cns-body');
    if (!body || !window.GouryOS) return;
    switch(_tab) {
      case 'status':   body.innerHTML = _renderStatus();   break;
      case 'intel':    body.innerHTML = _renderIntel();    break;
      case 'events':   body.innerHTML = _renderEvents();   break;
      case 'apps':     body.innerHTML = _renderApps();     break;
      case 'webhooks': body.innerHTML = _renderWebhooks(); break;
      case 'api':      body.innerHTML = _renderAPI();      break;
    }
  }

  // ── Status ────────────────────────────────────────────────────────────────
  function _renderStatus() {
    const h  = GouryOS.health().data;
    const iv = GouryOS.compute('inventory.health').data;
    const sv = GouryOS.compute('sales.summary',{days:30}).data;
    const cv = GouryOS.compute('customer.health').data;
    const dv = GouryOS.compute('demand.signals').data;
    const _r = (label, dot, pill, pillcls) => `
      <div class="cns-row">
        <div class="cns-row-left"><span class="cns-dot ${dot}"></span>${label}</div>
        <span class="cns-pill ${pillcls}">${pill}</span>
      </div>`;

    return `
    <div class="cns-card">
      <div class="cns-card-title green">System Heartbeat</div>
      ${_r('Network',    h.online?'green':'red',     h.online?'Online':'Offline',     h.online?'green':'red')}
      ${_r('Firebase',   h.firebaseOk?'green':'amber', h.firebaseOk?'Synced':'Pending', h.firebaseOk?'green':'amber')}
      ${_r('CNS v'+h.version,'green','Active · '+h.uptime,'green')}
      ${_r('Connected Apps','blue', h.appsCount+' registered','blue')}
      ${_r('Webhooks','blue', h.webhookCount+' active','blue')}
      ${_r('Events emitted','green', h.eventCount+' total','gray')}
    </div>

    <div class="cns-card">
      <div class="cns-card-title green">Inventory Core</div>
      <div class="cns-grid-3">
        <div class="cns-metric"><div class="cns-metric-n">${iv.totalSKUs}</div><div class="cns-metric-l">All Parts</div></div>
        <div class="cns-metric"><div class="cns-metric-n">${iv.saleableSKUs}</div><div class="cns-metric-l">Live / Saleable</div></div>
        <div class="cns-metric"><div class="cns-metric-n amber">${iv.pendingPricing}</div><div class="cns-metric-l">Needs Pricing</div></div>
        <div class="cns-metric"><div class="cns-metric-n red">${iv.outOfStock}</div><div class="cns-metric-l">Out of Stock</div></div>
        <div class="cns-metric"><div class="cns-metric-n amber">${iv.lowStock}</div><div class="cns-metric-l">Low Stock</div></div>
        <div class="cns-metric"><div class="cns-metric-n">₹${(iv.inventoryValue/100000).toFixed(1)}L</div><div class="cns-metric-l">Cost Value</div></div>
      </div>
    </div>

    <div class="cns-card">
      <div class="cns-card-title blue">Sales · 30 Days</div>
      <div class="cns-grid-2">
        <div class="cns-metric"><div class="cns-metric-n">₹${(sv.revenue/1000).toFixed(1)}K</div><div class="cns-metric-l">Revenue</div></div>
        <div class="cns-metric"><div class="cns-metric-n">${sv.transactions}</div><div class="cns-metric-l">Transactions</div></div>
        <div class="cns-metric"><div class="cns-metric-n">${sv.margin}%</div><div class="cns-metric-l">Gross Margin</div></div>
        <div class="cns-metric"><div class="cns-metric-n ${cv.totalDue>0?'amber':''}">${cv.withDue}</div><div class="cns-metric-l">Credit Debtors</div></div>
      </div>
    </div>

    ${sv.topParts&&sv.topParts.length ? `
    <div class="cns-card">
      <div class="cns-card-title amber">Top Selling Parts (30d)</div>
      <div class="cns-bar-chart">
        ${(()=>{const max=sv.topParts[0]&&sv.topParts[0].qty||1; return sv.topParts.map(p=>`
          <div class="cns-bar-item">
            <div class="cns-bar-name" title="${p.name}">${p.name}</div>
            <div class="cns-bar-track"><div class="cns-bar-fill" style="width:${Math.round(p.qty/max*100)}%"></div></div>
            <div class="cns-bar-val">${p.qty}</div>
          </div>`).join('');})()}
      </div>
    </div>` : ''}

    ${dv.high > 0 ? `
    <div class="cns-card">
      <div class="cns-card-title amber">Demand Signals (${dv.high} High)</div>
      ${dv.items.slice(0,5).map(d=>`
        <div class="cns-row">
          <div class="cns-row-left"><span class="cns-dot ${d.level==='high'?'red':'amber'}"></span>${d.name}</div>
          <span class="cns-pill ${d.level==='high'?'red':'amber'}">${d.level}</span>
        </div>`).join('')}
    </div>` : ''}`;
  }

  // ── Intel ─────────────────────────────────────────────────────────────────
  function _renderIntel() {
    const briefing = GouryOS.compute('briefing').data || [];
    const reorder  = GouryOS.compute('reorder.candidates').data || [];
    const dead     = GouryOS.compute('dead.stock').data || [];

    return `
    <div class="cns-card">
      <div class="cns-card-title green">Live Briefing (${briefing.length} actions)</div>
      ${briefing.length === 0
        ? '<div class="cns-empty">All clear — no urgent actions</div>'
        : briefing.map(a => `
          <div class="cns-intel-row ${a.type||'info'}">
            <div class="cns-intel-icon">${a.icon||'•'}</div>
            <div>
              <div class="cns-intel-title">${a.title}</div>
              <div class="cns-intel-detail">${a.detail}</div>
            </div>
          </div>`).join('')}
    </div>

    <div class="cns-card">
      <div class="cns-card-title amber">Reorder Candidates (${reorder.length})</div>
      ${reorder.length === 0
        ? '<div class="cns-empty">No urgent reorders</div>'
        : reorder.slice(0,8).map(p => `
          <div class="cns-row">
            <div class="cns-row-left">
              <span class="cns-dot ${p.stock===0?'red':'amber'}"></span>
              <div>
                <div style="font-size:11px;color:#e2e8f0;">${p.name}</div>
                <div style="font-size:9px;color:#475569;">${p.partNo} · stock:${p.stock} · ${p.velocity.toFixed(2)}/day</div>
              </div>
            </div>
            <div style="text-align:right;">
              <div style="font-size:10px;color:#f59e0b;font-weight:700;">${isFinite(p.daysLeft)?p.daysLeft+'d left':'Out'}</div>
              <div style="font-size:9px;color:#475569;">order ${p.reorderQty}</div>
            </div>
          </div>`).join('')}
    </div>

    <div class="cns-card">
      <div class="cns-card-title purple">Dead Stock — Capital Locked (${dead.length} parts)</div>
      ${dead.length === 0
        ? '<div class="cns-empty">No dead stock found</div>'
        : `<div style="font-size:12px;color:#a78bfa;font-weight:700;margin-bottom:8px;">
            ₹${dead.reduce((a,p)=>a+(p.costPrice||0)*(p.stock||0),0).toLocaleString('en-IN')} locked in dead inventory
           </div>`
          + dead.slice(0,6).map(p=>`
          <div class="cns-row">
            <div class="cns-row-left" style="font-size:11px;">${p.name}</div>
            <span class="cns-pill gray">${p.stock} ${p.unit||'pcs'} · ₹${((p.costPrice||0)*(p.stock||0)).toLocaleString('en-IN')}</span>
          </div>`).join('')}
    </div>`;
  }

  // ── Events ────────────────────────────────────────────────────────────────
  function _renderEvents() {
    const evts = GouryOS._events.slice(0, 30);
    return `
    <div class="cns-card">
      <div class="cns-card-title green">Live Event Stream (${GouryOS._events.length} total)</div>
      ${evts.length === 0
        ? '<div class="cns-empty">No events yet — use the app to generate events</div>'
        : evts.map(e => {
            const cat = e.type.startsWith('sale') ? 'sale' : e.type.startsWith('system') ? 'system' : e.type.startsWith('stock') ? 'alert' : '';
            return `<div class="cns-event ${cat}">
              <div class="cns-event-type">${e.type}</div>
              <div class="cns-event-payload">${e.payload ? JSON.stringify(e.payload).slice(0,120) : ''}</div>
              <div class="cns-event-ts">${new Date(e.ts).toLocaleTimeString('en-IN')}</div>
            </div>`;
          }).join('')}
    </div>

    <div class="cns-card">
      <div class="cns-card-title blue">Available Events</div>
      <div style="font-family:inherit;font-size:9px;color:#475569;line-height:2.2;">
        ${['sale.completed','part.added','stockout.detected','reorder.triggered',
           'state.saved','system.boot','system.firebase_ready','app.registered',
           'webhook.registered','webhook.test','daily.report','* (all)']
          .map(e=>`<span style="background:#0d1117;border:1px solid #1e293b;border-radius:4px;padding:2px 7px;margin:0 3px 3px 0;display:inline-block;">${e}</span>`)
          .join('')}
      </div>
    </div>`;
  }

  // ── Apps ──────────────────────────────────────────────────────────────────
  function _renderApps() {
    const apps = GouryOS.listApps().data || [];
    return `
    <div class="cns-card">
      <div class="cns-card-title green">Register External App</div>
      <input class="cns-input" id="cns-app-name" placeholder="App name — e.g. Orbion Dashboard">
      <select class="cns-input cns-select" id="cns-app-perms">
        <option value="read">Read only — query data, no writes</option>
        <option value="read,events">Read + Subscribe to events</option>
        <option value="read,events,webhooks">Read + Events + Webhooks</option>
        <option value="*">Full access (trusted internal apps)</option>
      </select>
      <button class="cns-btn" onclick="CNS_UI.registerApp()">⬡ Generate API Key</button>
      <div id="cns-new-key"></div>
    </div>

    <div class="cns-section-label">Connected Apps (${apps.length})</div>
    ${apps.length === 0
      ? '<div class="cns-empty">No external apps connected yet</div>'
      : apps.map(a => `
        <div class="cns-app-row">
          <div>
            <div class="cns-app-name">${a.name}</div>
            <div class="cns-app-meta">${(a.perms||[]).join(' · ')} · ${new Date(a.registeredAt).toLocaleDateString('en-IN')}</div>
          </div>
          <span class="cns-pill ${a.active?'green':'red'}">${a.active?'Active':'Revoked'}</span>
          ${a.active?`<button class="cns-wh-btn danger" onclick="CNS_UI.revokeApp('${a.appId}')" style="flex:0 0 auto;padding:5px 10px;">Revoke</button>`:''}
        </div>`).join('')}`;
  }

  // ── Webhooks ───────────────────────────────────────────────────────────────
  function _renderWebhooks() {
    const hooks = GouryOS.webhook.list().data || [];
    return `
    <div class="cns-card">
      <div class="cns-card-title blue">Register Webhook Endpoint</div>
      <input class="cns-input" id="cns-wh-url"    placeholder="https://your-app.com/api/goury-hook">
      <input class="cns-input" id="cns-wh-events" placeholder="Events: * or sale.completed,reorder.triggered">
      <input class="cns-input" id="cns-wh-secret" placeholder="Secret key (sent in X-GouryOS-Secret header)">
      <button class="cns-btn" onclick="CNS_UI.addWebhook()">↗ Register Webhook</button>
    </div>

    <div class="cns-section-label">Active Webhooks (${hooks.length})</div>
    ${hooks.length === 0
      ? '<div class="cns-empty">No webhooks registered.<br>Any app in the ecosystem can receive live events via HTTPS.</div>'
      : hooks.map(w => `
        <div class="cns-wh-row">
          <div class="cns-wh-url">${w.url}</div>
          <div class="cns-wh-meta">
            Events: ${(w.events||[]).join(', ')} ·
            ${w.lastFired ? 'Last: '+new Date(w.lastFired).toLocaleTimeString('en-IN') : 'Never fired'} ·
            <span style="color:${w.failCount>0?'#f87171':'#34d399'}">${w.failCount||0} failures</span> ·
            <span class="cns-pill ${w.active?'green':'red'}">${w.active?'Active':'Disabled'}</span>
          </div>
          <div class="cns-wh-actions">
            <button class="cns-wh-btn" onclick="CNS_UI.testWebhook('${w.id}')">Test Ping</button>
            <button class="cns-wh-btn danger" onclick="CNS_UI.deleteWebhook('${w.id}')">Delete</button>
          </div>
        </div>`).join('')}`;
  }

  // ── API ────────────────────────────────────────────────────────────────────
  function _renderAPI() {
    const colls = GouryOS.listCollections().data || [];
    const views = GouryOS.listViews().data || [];
    return `
    <div class="cns-card">
      <div class="cns-card-title green">Quick Reference</div>
      <div style="font-size:9px;color:#475569;line-height:2;font-family:inherit;">
        <div><span style="color:#34d399;">GouryOS.query(</span><span style="color:#fcd34d;">'parts'</span>, { where:{machineType:'tractor'}, limit:20 }<span style="color:#34d399;">)</span></div>
        <div><span style="color:#34d399;">GouryOS.compute(</span><span style="color:#fcd34d;">'inventory.health'</span><span style="color:#34d399;">)</span></div>
        <div><span style="color:#34d399;">GouryOS.compute(</span><span style="color:#fcd34d;">'full.snapshot'</span><span style="color:#34d399;">)</span></div>
        <div><span style="color:#34d399;">GouryOS.compute(</span><span style="color:#fcd34d;">'reorder.candidates'</span><span style="color:#34d399;">)</span></div>
        <div><span style="color:#34d399;">GouryOS.on(</span><span style="color:#fcd34d;">'sale.completed'</span>, handler<span style="color:#34d399;">)</span></div>
        <div><span style="color:#34d399;">GouryOS.webhook.register(</span>url, ['*'], secret<span style="color:#34d399;">)</span></div>
        <div><span style="color:#34d399;">GouryOS.register(</span><span style="color:#fcd34d;">'Orbion Dashboard'</span>, ['read','events']<span style="color:#34d399;">)</span></div>
      </div>
    </div>

    <div class="cns-card">
      <div class="cns-card-title green">Live Query Tester</div>
      <select class="cns-input cns-select" id="cns-q-coll">
        ${colls.map(c=>`<option>${c}</option>`).join('')}
      </select>
      <input class="cns-input" id="cns-q-limit" type="number" placeholder="Limit" value="5">
      <button class="cns-btn" onclick="CNS_UI.runQuery()">▶ Run Query</button>
      <div class="cns-console" id="cns-q-out" style="display:none;margin-top:8px;"></div>
    </div>

    <div class="cns-card">
      <div class="cns-card-title purple">Computed Views</div>
      <div class="cns-btn-row" style="flex-wrap:wrap;gap:5px;">
        ${views.map(v=>`<button class="cns-btn secondary" style="flex:0 0 auto;width:auto;padding:6px 10px;font-size:9px;" onclick="CNS_UI.runView('${v}')">${v}</button>`).join('')}
      </div>
      <div class="cns-console" id="cns-v-out" style="display:none;margin-top:8px;"></div>
    </div>

    <div class="cns-card">
      <div class="cns-card-title blue">Access Log (last 50 calls)</div>
      <div class="cns-console" style="max-height:160px;">
        ${GouryOS._apiLog.slice(0,50).map(l=>`
          <div class="cns-log-line">
            <span class="cns-log-ts">${new Date(l.ts).toLocaleTimeString('en-IN')}</span>
            <span class="cns-log-app">${(l.appId||'—').slice(0,14)}</span>
            <span class="cns-log-ep" style="color:${l.ok?'#34d399':'#f87171'}">${l.endpoint}</span>
          </div>`).join('')
        || '<div style="color:#334155;">No API calls yet</div>'}
      </div>
    </div>`;
  }

  // ── Public methods called by onclick ──────────────────────────────────────
  return {
    toggle, tab,

    registerApp() {
      const name  = (document.getElementById('cns-app-name').value||'').trim();
      const perms = (document.getElementById('cns-app-perms').value||'read').split(',');
      if (!name) { alert('Enter an app name'); return; }
      const res = GouryOS.register(name, perms);
      document.getElementById('cns-new-key').innerHTML =
        `<div style="font-size:10px;color:#f59e0b;font-weight:700;margin-top:8px;">⚠ Copy this key — shown only once</div>
         <div class="cns-key-box">${res.data.key}</div>
         <div style="font-size:9px;color:#334155;margin-top:4px;">App ID: ${res.data.appId}</div>`;
      document.getElementById('cns-app-name').value = '';
    },

    revokeApp(appId) {
      if (!confirm('Revoke this app? Its API key stops working immediately.')) return;
      GouryOS.revoke(appId);
      tab('apps');
    },

    addWebhook() {
      const url    = (document.getElementById('cns-wh-url').value||'').trim();
      const evts   = (document.getElementById('cns-wh-events').value||'*').split(',').map(s=>s.trim());
      const secret = (document.getElementById('cns-wh-secret').value||'').trim();
      const res    = GouryOS.webhook.register(url, evts, secret);
      if (!res.ok) { alert('Error: ' + res.error); return; }
      ['cns-wh-url','cns-wh-events','cns-wh-secret'].forEach(id => { const el=document.getElementById(id); if(el) el.value=''; });
      tab('webhooks');
    },

    testWebhook(id) {
      GouryOS.webhook.test(id);
      if (window.toast) toast('📡 Test ping sent');
    },

    deleteWebhook(id) {
      if (!confirm('Delete this webhook?')) return;
      GouryOS.webhook.delete(id);
      tab('webhooks');
    },

    runQuery() {
      const coll  = document.getElementById('cns-q-coll').value;
      const limit = parseInt(document.getElementById('cns-q-limit').value)||5;
      const res   = GouryOS.query(coll, { limit });
      const el    = document.getElementById('cns-q-out');
      el.style.display = 'block';
      el.textContent = JSON.stringify(res.ok ? { total:res.data.total, items:res.data.items.slice(0,limit) } : res, null, 2);
    },

    runView(name) {
      const res = GouryOS.compute(name, { days:30 });
      const el  = document.getElementById('cns-v-out');
      el.style.display = 'block';
      el.textContent = JSON.stringify(res.ok ? res.data : res, null, 2);
    }
  };
})();

// Wire badge updates from outside the closure
window._updateCNSBadge = function() {
  const cnt = document.getElementById('cns-fab-count');
  if (!cnt || !window.GouryOS) return;
  const n = GouryOS._events.length;
  cnt.textContent = n > 99 ? '99+' : n;
  cnt.classList.toggle('show', n > 0);
};
