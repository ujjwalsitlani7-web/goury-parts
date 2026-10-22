(function(){
  'use strict';
  var $ = function(id){ return document.getElementById(id); };
  function esc(v){ return (typeof escapeHtml === 'function') ? escapeHtml(v == null ? '' : String(v)) : String(v == null ? '' : v).replace(/[&<>"']/g, function(c){ return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }
  function num(v){ var n = parseFloat(v); return isFinite(n) ? n : 0; }
  function r2(n){ return Math.round(n * 100) / 100; }
  function who(){ return (state.session && state.session.name) || 'staff'; }
  function role(){ return (state.session && state.session.role) || ''; }
  function isOwnerish(){ return role() === 'admin' || role() === 'manager'; }
  function ro(){ return (typeof isReadOnly === 'function') && isReadOnly(); }
  function dateOnly(iso){ try { return new Date(iso).toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'numeric'}); } catch(e){ return ''; } }

  // ───────────────────────── Sheet overlays (stack above the main modal) ─────────────────────────
  var _sheets = [];
  function gpSheet(title, html, opts) {
    opts = opts || {};
    var bg = document.createElement('div');
    bg.className = 'gp-sheet-bg';
    bg.style.zIndex = String(1000 + _sheets.length * 10);
    bg.innerHTML = '<div class="gp-sheet"' + (opts.wide ? ' style="max-width:760px;"' : '') + '>'
      + '<div class="gp-sheet-hdr"><h3>' + title + '</h3><button class="gp-x" data-gpclose>✕</button></div>'
      + '<div class="gp-sheet-body">' + html + '</div></div>';
    bg.addEventListener('click', function(e){
      if (e.target === bg && !opts.sticky) gpCloseSheet();
      if (e.target && e.target.hasAttribute && e.target.hasAttribute('data-gpclose')) gpCloseSheet();
    });
    document.body.appendChild(bg);
    _sheets.push({ el: bg, onClose: opts.onClose });
    return bg;
  }
  function gpCloseSheet() {
    var s = _sheets.pop();
    if (!s) return;
    try { s.el.remove(); } catch(e){}
    if (s.onClose) try { s.onClose(); } catch(e){}
  }
  window.gpCloseSheet = gpCloseSheet;

  // ───────────────────────── Photos ─────────────────────────
  function isPh(v){ return !v || (window.isPlaceholderPhoto && window.isPlaceholderPhoto(v)); }
  function gpPhotosOf(b) {
    if (!b) return [];
    var list = Array.isArray(b.photos) ? b.photos.slice() : [];
    if (!list.length && b.photoDataUrl) list = [b.photoDataUrl];
    return list.filter(function(v){ return !isPh(v); });
  }
  window.gpPhotosOf = gpPhotosOf;

  function gpPhotoStripHtml(b, kind) {
    kind = kind || (b && b.woNo != null ? 'wo' : 'sale');
    var ph = gpPhotosOf(b);
    if (!ph.length) return '<div style="margin-top:8px;"><button class="btn btn-sm btn-outline" onclick="closeModal();gpOpenBillEditor(\'' + kind + '\',\'' + b.id + '\',\'photos\')">📷 Add Bill Photo(s)</button></div>';
    return '<div style="margin-top:10px;"><div style="font-size:12px;font-weight:700;color:var(--muted);">📷 BILL PHOTOS (' + ph.length + ')</div><div class="gp-photos">'
      + ph.map(function(src, i){ return '<div class="gp-photo"><img src="' + esc(src) + '" onclick="gpViewPhotos(\'' + kind + '\',\'' + b.id + '\',' + i + ')"></div>'; }).join('')
      + '</div></div>';
  }
  window.gpPhotoStripHtml = gpPhotoStripHtml;

  function findBill(kind, id) {
    var arr = kind === 'wo' ? (state.wholesaleOrders || []) : (state.sales || []);
    return arr.find(function(x){ return x.id === id; }) || null;
  }

  window.gpViewPhotos = function(kind, id, start) {
    var b = findBill(kind, id); if (!b) return;
    var ph = gpPhotosOf(b); if (!ph.length) return;
    var i = Math.max(0, Math.min(start || 0, ph.length - 1));
    var bg = gpSheet('📷 Bill photo', '<div id="gp-pv"></div>', { wide: true });
    function draw() {
      bg.querySelector('#gp-pv').innerHTML =
        '<img src="' + esc(ph[i]) + '" style="width:100%;border-radius:8px;border:1px solid #e2e8f0;background:#fff;">'
        + '<div style="display:flex;justify-content:space-between;align-items:center;margin-top:10px;">'
        + '<button class="btn btn-sm btn-outline" style="width:auto;" ' + (i <= 0 ? 'disabled' : '') + ' data-gpnav="-1">◀ Prev</button>'
        + '<span style="font-size:13px;color:var(--muted);">' + (i + 1) + ' / ' + ph.length + '</span>'
        + '<button class="btn btn-sm btn-outline" style="width:auto;" ' + (i >= ph.length - 1 ? 'disabled' : '') + ' data-gpnav="1">Next ▶</button></div>';
    }
    bg.addEventListener('click', function(e){
      var n = e.target && e.target.getAttribute && e.target.getAttribute('data-gpnav');
      if (n) { i = Math.max(0, Math.min(ph.length - 1, i + (+n))); draw(); }
    });
    draw();
  };
  // The old single-photo viewer now opens the multi-photo viewer
  window.viewWOBillPhoto = function(woId){ window.gpViewPhotos('wo', woId, 0); };

  // Shrink camera photos before storing — keeps bills far below Firestore's 1 MB doc limit
  function gpCompress(file, maxSide, quality) {
    maxSide = maxSide || 1400; quality = quality || 0.72;
    return new Promise(function(resolve, reject){
      var fr = new FileReader();
      fr.onerror = function(){ reject(new Error('read failed')); };
      fr.onload = function(){
        var img = new Image();
        img.onerror = function(){ resolve(fr.result); };
        img.onload = function(){
          var w = img.width, h = img.height, k = Math.min(1, maxSide / Math.max(w, h));
          var c = document.createElement('canvas');
          c.width = Math.round(w * k); c.height = Math.round(h * k);
          c.getContext('2d').drawImage(img, 0, 0, c.width, c.height);
          try { resolve(c.toDataURL('image/jpeg', quality)); } catch(e){ resolve(fr.result); }
        };
        img.src = fr.result;
      };
      fr.readAsDataURL(file);
    });
  }
  function gpPickPhotos(cb) {
    var input = document.createElement('input');
    input.type = 'file'; input.accept = 'image/*'; input.multiple = true; input.style.display = 'none';
    document.body.appendChild(input);
    input.onchange = async function(){
      var files = Array.prototype.slice.call(input.files || []);
      input.remove();
      if (!files.length) return;
      var out = [];
      for (var i = 0; i < files.length; i++) { try { out.push(await gpCompress(files[i])); } catch(e){} }
      cb(out);
    };
    input.click();
  }
  function gpPickCamera(cb) {
    var input = document.createElement('input');
    input.type = 'file'; input.accept = 'image/*'; input.capture = 'environment'; input.style.display = 'none';
    document.body.appendChild(input);
    input.onchange = async function(){
      var f = input.files && input.files[0]; input.remove();
      if (!f) return;
      try { cb([await gpCompress(f)]); } catch(e){}
    };
    input.click();
  }

  // Base64 bill photos must never ride inside the chunked cloud docs — they wait locally
  // until the uploader swaps in the https URL (same rule the purchases key already follows).
  window._gpStripBillPhotos = function(k, arr) {
    if ((k !== 'wholesaleOrders' && k !== 'sales') || !Array.isArray(arr)) return arr;
    var touched = false;
    var out = arr.map(function(b){
      if (!b) return b;
      var hasData = (b.photoDataUrl && String(b.photoDataUrl).indexOf('data:') === 0)
        || (Array.isArray(b.photos) && b.photos.some(function(v){ return v && String(v).indexOf('data:') === 0; }));
      if (!hasData) return b;
      touched = true;
      var c = Object.assign({}, b);
      if (c.photoDataUrl && String(c.photoDataUrl).indexOf('data:') === 0) c.photoDataUrl = '__uploading__';
      if (Array.isArray(c.photos)) c.photos = c.photos.filter(function(v){ return v && String(v).indexOf('data:') !== 0; });
      return c;
    });
    return touched ? out : arr;
  };

  var _uploading = {};
  async function gpUploadBillPhotos(kind, b) {
    if (!b || !window.firebaseUploadWOPhoto || !window._firebaseAuthed) return;
    var list = Array.isArray(b.photos) ? b.photos : (b.photoDataUrl ? [b.photoDataUrl] : []);
    var changed = false;
    for (var i = 0; i < list.length; i++) {
      var v = list[i];
      if (!v || String(v).indexOf('data:') !== 0) continue;
      var key = kind + '_' + b.id + '_' + i + '_' + v.length;
      if (_uploading[key]) continue;
      _uploading[key] = 1;
      try {
        var name = (kind === 'sale' ? 'sale_' : '') + b.id + '_p' + Date.now().toString(36) + i;
        var url = await window.firebaseUploadWOPhoto(name, v);
        var live = findBill(kind, b.id);
        if (live) {
          var lp = Array.isArray(live.photos) ? live.photos : (live.photoDataUrl ? [live.photoDataUrl] : []);
          var at = lp.indexOf(v);
          if (at >= 0) { lp[at] = url; live.photos = lp; live.photoDataUrl = lp[0] || null; live.updatedAt = Date.now(); changed = true; }
        }
      } catch(e) { console.warn('[gp] bill photo upload failed:', e && e.message); }
      finally { delete _uploading[key]; }
    }
    if (changed) { if (typeof markChanged === 'function') markChanged(kind === 'wo' ? 'wholesaleOrders' : 'sales'); save(); }
  }
  // Retry any bill photo still waiting as base64 on THIS device
  setInterval(function(){
    if (!window._firebaseAuthed || !navigator.onLine) return;
    (state.wholesaleOrders || []).forEach(function(o){ if (gpHasLocal(o)) gpUploadBillPhotos('wo', o); });
    (state.sales || []).forEach(function(s){ if (gpHasLocal(s)) gpUploadBillPhotos('sale', s); });
  }, 25000);
  function gpHasLocal(b){
    return b && ((Array.isArray(b.photos) && b.photos.some(function(v){ return v && String(v).indexOf('data:') === 0; }))
      || (b.photoDataUrl && String(b.photoDataUrl).indexOf('data:') === 0));
  }

  // ───────────────────────── Part rate save (H / C / R / MRP) ─────────────────────────
  async function gpSavePartRates(partId, rates) {
    var idx = state.parts.findIndex(function(x){ return x.id === partId; });
    if (idx < 0) return false;
    var old = state.parts[idx];
    var patch = {};
    ['creditPrice','costPrice','instantPrice','mrp'].forEach(function(f){
      if (rates[f] > 0 && rates[f] !== (old[f] || 0)) patch[f] = rates[f]; // blank field never wipes a rate
    });
    if (!Object.keys(patch).length) return false;
    var p = Object.assign({}, old);
    try { if (typeof recordPriceHistory === 'function') recordPriceHistory(p, patch, who()); } catch(e){}
    Object.assign(p, patch);
    p.updatedAt = Date.now();
    state.parts[idx] = p;
    window._lastLocalSave = Date.now();
    if (typeof markChanged === 'function') markChanged('parts');
    if (typeof invalidatePartsIndex === 'function') invalidatePartsIndex();
    save();
    try {
      var cloudPatch = Object.assign({}, patch, { updatedAt: p.updatedAt });
      if (p.priceHistory) cloudPatch.priceHistory = p.priceHistory;
      await cloudPersistPartFields(p.id, cloudPatch);
    } catch(e) { console.warn('[gp] rate cloud save:', e && e.message); }
    try { typeof patchInventoryCard === 'function' && patchInventoryCard(p, false); } catch(e){}
    try { if (typeof logActivity === 'function') logActivity('Rates changed while billing: ' + p.partNo + ' ' + Object.keys(patch).map(function(k){ return ({creditPrice:'H',costPrice:'C',instantPrice:'R',mrp:'MRP'})[k] + '=' + patch[k]; }).join(', '), 'info'); } catch(e){}
    return true;
  }

  // ───────────────────────── Rate editor (while making a bill) ─────────────────────────
  // ctx: 'sale' (counter cart) | 'wo' (new wholesale order) | 'pwb' (previous bill) | 'edit' (bill editor)
  function cartFor(ctx) {
    if (ctx === 'sale') return state.cart;
    if (ctx === 'wo') return state.woCart;
    if (ctx === 'pwb') return state.pwbCart || [];
    if (ctx === 'edit') return (_ed && _ed.items) || [];
    return [];
  }
  function currentLinePrice(ctx, item, p) {
    if (ctx === 'sale') return saleItemPrice(item, p);
    if (ctx === 'wo') return woItemPrice(item, p);
    return num(item.price);
  }
  function rerender(ctx) {
    if (ctx === 'sale') renderCart();
    else if (ctx === 'wo') renderWOCart();
    else if (ctx === 'pwb') renderPWBCart();
    else if (ctx === 'edit') edRender();
  }
  window.gpOpenRateEditor = function(ctx, idx) {
    var item = cartFor(ctx)[idx]; if (!item) return;
    var p = getPartById(item.partId); if (!p) { toast('Part not found in stock list'); return; }
    var cur = currentLinePrice(ctx, item, p);
    var bg = gpSheet('✎ Rate — ' + esc(p.name),
      '<div style="font-size:12px;color:var(--muted);margin-bottom:10px;">' + esc(p.partNo) + ' · ' + esc(p.brand || '') + ' · Stock: ' + (p.stock || 0) + ' ' + esc(p.unit || 'pcs') + (p.mrp ? ' · MRP ' + rs(p.mrp) : '') + '</div>'
      + '<div style="font-size:13px;font-weight:700;margin-bottom:6px;">Part rates (saved permanently)</div>'
      + '<div class="gp-hcr">'
      + '<div class="gp-box" style="border-color:#99f6e4;">H · Wholesale<input type="number" id="gpr-h" value="' + (p.creditPrice || '') + '" placeholder="0"></div>'
      + '<div class="gp-box" style="border-color:#fecaca;">C · Cost<input type="number" id="gpr-c" value="' + (p.costPrice || '') + '" placeholder="0"></div>'
      + '<div class="gp-box" style="border-color:#bfdbfe;">R · Retail<input type="number" id="gpr-r" value="' + (p.instantPrice || '') + '" placeholder="0"></div>'
      + '</div>'
      + '<div class="field mt-1"><label>MRP ₹</label><input type="number" class="input" id="gpr-mrp" value="' + (p.mrp || '') + '" placeholder="0"></div>'
      + '<div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:10px;padding:10px;margin-top:6px;">'
      + '<div style="font-size:13px;font-weight:700;color:#9a3412;">Rate on THIS bill ₹</div>'
      + '<input type="number" class="input" id="gpr-bill" value="' + cur + '" style="margin-top:6px;font-size:18px;font-weight:700;">'
      + '<div style="display:flex;gap:6px;margin-top:6px;flex-wrap:wrap;">'
      + '<button class="btn btn-sm btn-outline" style="width:auto;" data-gpuse="h">Use H</button>'
      + '<button class="btn btn-sm btn-outline" style="width:auto;" data-gpuse="r">Use R</button>'
      + '<button class="btn btn-sm btn-outline" style="width:auto;" data-gpuse="mrp">Use MRP</button>'
      + '</div>'
      + '<div id="gpr-margin" style="font-size:12px;margin-top:6px;"></div></div>'
      + '<button class="btn btn-success mt-2" id="gpr-save">✅ Save</button>'
      + '<div style="font-size:11px;color:var(--muted);margin-top:6px;">H/C/R/MRP changes are saved on the part for all future bills. The bill rate is used only on this bill.</div>'
    );
    function margin() {
      var b = num(bg.querySelector('#gpr-bill').value), c = num(bg.querySelector('#gpr-c').value);
      var el = bg.querySelector('#gpr-margin');
      if (!c || !b) { el.textContent = ''; return; }
      var m = b - c;
      el.innerHTML = 'Profit per unit: <b style="color:' + (m < 0 ? '#dc2626' : '#16a34a') + ';">' + rs(m) + '</b> (' + Math.round(m / b * 100) + '%)' + (m < 0 ? ' ⚠️ below cost' : '');
    }
    bg.addEventListener('input', margin);
    bg.addEventListener('click', function(e){
      var u = e.target && e.target.getAttribute && e.target.getAttribute('data-gpuse');
      if (u) { var v = num(bg.querySelector('#gpr-' + u).value); if (v) { bg.querySelector('#gpr-bill').value = v; margin(); } }
    });
    margin();
    bg.querySelector('#gpr-save').onclick = async function(){
      if (ro()) { toast('🔒 Read-only access'); return; }
      var rates = {
        creditPrice: num(bg.querySelector('#gpr-h').value),
        costPrice: num(bg.querySelector('#gpr-c').value),
        instantPrice: num(bg.querySelector('#gpr-r').value),
        mrp: num(bg.querySelector('#gpr-mrp').value)
      };
      var changed = await gpSavePartRates(p.id, rates);
      var billRate = num(bg.querySelector('#gpr-bill').value);
      var live = cartFor(ctx)[idx];
      if (live) {
        var np = getPartById(p.id) || p;
        if (ctx === 'sale' || ctx === 'wo') {
          var def = ctx === 'sale' ? getPrice(np) : (np.creditPrice || np.instantPrice);
          live.customPrice = (billRate > 0 && billRate !== def) ? billRate : null;
        } else {
          live.price = billRate;
        }
      }
      gpCloseSheet();
      rerender(ctx);
      toast(changed ? '✅ Rates saved on part + bill updated' : '✅ Bill rate updated');
    };
  };

  // ───────────────────────── New part while billing ─────────────────────────
  window.gpOpenNewPart = function(ctx, prefillName) {
    if (ro()) { toast('🔒 Read-only access'); return; }
    var q = prefillName || '';
    if (!q && ctx === 'sale' && $('sale-search')) q = $('sale-search').value || '';
    var units = ['pcs','set','box','pair','kg','ltr','mtr'];
    var bg = gpSheet('➕ New Part — save permanently',
      '<div class="field"><label>Part name *</label><input class="input" id="gpn-name" value="' + esc(q) + '" placeholder="e.g. Clutch Plate 280mm"></div>'
      + '<div class="row2"><div class="field"><label>Part number</label><input class="input" id="gpn-no" placeholder="Auto if blank" style="text-transform:uppercase;"></div>'
      + '<div class="field"><label>Brand</label><input class="input" id="gpn-brand" placeholder="SKF, Mahindra…"></div></div>'
      + '<div class="row2"><div class="field"><label>Category</label><input class="input" id="gpn-cat" placeholder="Bearing, Filter…"></div>'
      + '<div class="field"><label>Unit</label><select class="select" id="gpn-unit">' + units.map(function(u){ return '<option>' + u + '</option>'; }).join('') + '</select></div></div>'
      + '<div class="row2"><div class="field"><label>Stock in shop now *</label><input type="number" class="input" id="gpn-stock" value="1" min="0"></div>'
      + '<div class="field"><label>Rack / location</label><input class="input" id="gpn-loc" placeholder="e.g. A-7" style="text-transform:uppercase;"></div></div>'
      + '<div style="font-size:13px;font-weight:700;margin:4px 0 6px;">Rates</div>'
      + '<div class="gp-hcr">'
      + '<div class="gp-box" style="border-color:#99f6e4;">H · Wholesale<input type="number" id="gpn-h" placeholder="0"></div>'
      + '<div class="gp-box" style="border-color:#fecaca;">C · Cost<input type="number" id="gpn-c" placeholder="0"></div>'
      + '<div class="gp-box" style="border-color:#bfdbfe;">R · Retail *<input type="number" id="gpn-r" placeholder="0"></div>'
      + '</div>'
      + '<div class="field mt-1"><label>MRP ₹</label><input type="number" class="input" id="gpn-mrp" placeholder="0"></div>'
      + '<div style="font-size:11px;color:var(--muted);">Without C (cost) the part is added to this bill but also goes to the owner\'s pricing queue.</div>'
      + '<button class="btn btn-success mt-2" id="gpn-save">💾 Save part' + (ctx ? ' & add to bill' : '') + '</button>'
    );
    bg.querySelector('#gpn-save').onclick = async function(){
      var name = (bg.querySelector('#gpn-name').value || '').trim();
      var partNo = (bg.querySelector('#gpn-no').value || '').trim().toUpperCase();
      var H = num(bg.querySelector('#gpn-h').value), C = num(bg.querySelector('#gpn-c').value), R = num(bg.querySelector('#gpn-r').value);
      var stock = num(bg.querySelector('#gpn-stock').value);
      if (!name) { toast('⚠️ Part name required'); return; }
      if (!R && !H) { toast('⚠️ Enter R (retail) or H (wholesale) rate'); return; }
      if (!partNo) partNo = 'NP-' + Date.now().toString(36).toUpperCase().slice(-6);
      var dup = state.parts.find(function(x){ return (x.partNo || '').toUpperCase() === partNo; });
      if (dup) { toast('⚠️ Part no ' + partNo + ' already exists — search it instead'); return; }
      if (ctx === 'sale' && stock <= 0) { toast('⚠️ Stock must be at least 1 to sell'); return; }
      var part = {
        id: uid(), partNo: partNo, name: name,
        brand: (bg.querySelector('#gpn-brand').value || '').trim(),
        machineType: 'other',
        category: (bg.querySelector('#gpn-cat').value || '').trim(),
        models: '', unit: bg.querySelector('#gpn-unit').value || 'pcs',
        stock: stock, minStock: 0,
        location: (bg.querySelector('#gpn-loc').value || '').trim().toUpperCase(),
        mrp: num(bg.querySelector('#gpn-mrp').value),
        costPrice: C, instantPrice: R || H, creditPrice: H || R,
        supplierName: '', supplierPhone: '', photoDataUrl: '',
        addedAt: Date.now(), updatedAt: Date.now(), addedBy: who(), addedFrom: 'billing'
      };
      state.parts.push(part);
      if (typeof invalidatePartsIndex === 'function') invalidatePartsIndex();
      if (typeof markChanged === 'function') markChanged('parts');
      save();
      try { if (typeof cloudPersistPart === 'function') await cloudPersistPart(part); } catch(e){ console.warn('[gp] new part cloud:', e && e.message); }
      try { typeof patchInventoryCard === 'function' && patchInventoryCard(part, true); } catch(e){}
      try { typeof catRenderProgressDebounced === 'function' && catRenderProgressDebounced(); } catch(e){}
      try { logActivity('New part created while billing: ' + part.partNo + ' "' + part.name + '"', 'info'); } catch(e){}
      gpCloseSheet();
      if (ctx === 'sale') { if ($('sale-search')) $('sale-search').value = ''; addToCart(part.id); }
      else if (ctx === 'wo') addToWOCart(part.id);
      else if (ctx === 'pwb') addToPWBCart(part.id);
      else if (ctx === 'edit') edAddPart(part.id);
      toast('✅ ' + part.name + ' saved permanently' + (ctx ? ' & added to bill' : '') + (C ? '' : ' — cost pending with owner'));
    };
  };

  // ───────────────────────── Party search + party info ─────────────────────────
  function gpAttachPartySearch(selectId, holder) {
    var sel = $(selectId); if (!sel) return;
    if (!holder) {
      holder = document.createElement('div');
      sel.parentNode.insertBefore(holder, sel);
    }
    if (holder.getAttribute('data-gpdone')) return;
    holder.setAttribute('data-gpdone', '1');
    holder.className = 'gp-party-box';
    holder.innerHTML = '<input class="input" placeholder="🔍 Search party — name, phone or city…" autocomplete="off"><div class="gp-party-list" style="display:none;"></div>';
    var inp = holder.querySelector('input'), list = holder.querySelector('.gp-party-list');
    function draw() {
      var q = (inp.value || '').toLowerCase().trim();
      var opts = Array.prototype.slice.call(sel.options).filter(function(o){ return o.value; });
      var res = opts.filter(function(o){
        if (!q) return true;
        var c = getCustomerById(o.value);
        var hay = (o.textContent + ' ' + (c ? (c.phone || '') + ' ' + (c.city || '') : '')).toLowerCase();
        return hay.indexOf(q) >= 0;
      }).slice(0, 25);
      if (!res.length) { list.innerHTML = '<div style="color:var(--muted);cursor:default;">No party found</div>'; list.style.display = ''; return; }
      list.innerHTML = res.map(function(o){
        var c = getCustomerById(o.value);
        return '<div data-v="' + esc(o.value) + '"><b>' + esc(o.textContent) + '</b>'
          + (c && c.phone ? ' <span style="color:var(--muted);">· ' + esc(c.phone) + '</span>' : '')
          + (c && c.totalDue > 0 ? ' <span class="gp-tag" style="background:#fee2e2;color:#b91c1c;">Due ' + rs(c.totalDue) + '</span>' : '')
          + '</div>';
      }).join('');
      list.style.display = '';
    }
    inp.addEventListener('input', draw);
    inp.addEventListener('focus', draw);
    inp.addEventListener('blur', function(){ setTimeout(function(){ list.style.display = 'none'; }, 200); });
    list.addEventListener('mousedown', function(e){
      var d = e.target.closest('[data-v]'); if (!d) return;
      e.preventDefault();
      sel.value = d.getAttribute('data-v');
      sel.dispatchEvent(new Event('change', { bubbles: true }));
      inp.value = ''; list.style.display = 'none'; inp.blur();
    });
  }
  window.gpAttachPartySearch = gpAttachPartySearch;

  function billsOfParty(custId) {
    var out = [];
    (state.sales || []).forEach(function(s){ if (s.customerId === custId) out.push({ kind: 'sale', b: s, no: s.invoiceNo, date: s.date }); });
    (state.wholesaleOrders || []).forEach(function(o){ if (o.customerId === custId) out.push({ kind: 'wo', b: o, no: (o.invoiceNo ? o.invoiceNo + ' / ' : '') + 'WO-' + o.woNo, date: o.date }); });
    return out.sort(function(a, b){ return String(b.date).localeCompare(String(a.date)); });
  }
  function gpPartyInfoHtml(custId) {
    var c = getCustomerById(custId); if (!c) return '';
    var bills = billsOfParty(custId);
    var agg = {};
    bills.forEach(function(x){ (x.b.items || []).forEach(function(it){
      if (!it.partId) return;
      agg[it.partId] = agg[it.partId] || { qty: 0, times: 0, name: it.name };
      agg[it.partId].qty += num(it.qty); agg[it.partId].times++;
    }); });
    var top = Object.keys(agg).map(function(k){ return Object.assign({ id: k }, agg[k]); })
      .sort(function(a, b){ return b.times - a.times || b.qty - a.qty; }).slice(0, 8);
    var due = c.totalDue || 0;
    return '<div class="gp-info">'
      + '<div style="display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap;">'
      + '<div><b>' + esc(c.name) + '</b>' + (c.city ? ' · ' + esc(c.city) : '') + '<br>📞 <span>' + (c.phone ? esc(c.phone) : '<i style="color:var(--muted);">no phone</i>') + '</span> '
      + '<a href="#" onclick="event.preventDefault();gpEditPartyPhone(\'' + c.id + '\')" style="color:#0369a1;font-weight:700;">✏️ Change phone</a></div>'
      + '<div style="text-align:right;"><div style="font-size:11px;color:var(--muted);font-weight:700;">BAKI / DUE</div><div style="font-size:18px;font-weight:800;color:' + (due > 0 ? '#dc2626' : '#16a34a') + ';">' + rs(due) + '</div></div>'
      + '</div>'
      + '<h4>🧾 Purane bills (' + bills.length + ')</h4>'
      + (bills.length ? bills.slice(0, 5).map(function(x){
          var paid = x.kind === 'wo' ? (x.b.paymentMode === 'credit' && !x.b.settled ? '<span class="gp-tag" style="background:#fee2e2;color:#b91c1c;">Due ' + rs((x.b.total || 0) - (x.b.paidAmount || 0)) + '</span>' : '<span class="gp-tag" style="background:#dcfce7;color:#166534;">Paid</span>')
            : (x.b.paymentMode === 'credit' && !x.b.settled ? '<span class="gp-tag" style="background:#fee2e2;color:#b91c1c;">Credit</span>' : '<span class="gp-tag" style="background:#dcfce7;color:#166534;">' + esc((x.b.paymentMode || '').toUpperCase()) + '</span>');
          return '<div style="display:flex;justify-content:space-between;gap:6px;padding:4px 0;border-bottom:1px solid #e0f2fe;">'
            + '<span><b>' + esc(x.no) + '</b> · ' + dateOnly(x.date) + ' ' + paid + '</span>'
            + '<span style="white-space:nowrap;"><b>' + rs(x.b.total) + '</b> <a href="#" onclick="event.preventDefault();gpViewBill(\'' + x.kind + '\',\'' + x.b.id + '\')" style="color:#0369a1;">View</a></span></div>';
        }).join('') + (bills.length > 5 ? '<div style="margin-top:4px;"><a href="#" onclick="event.preventDefault();gpOpenBillSearch(' + JSON.stringify(c.name).replace(/"/g, '&quot;') + ')" style="color:#0369a1;">See all ' + bills.length + ' bills →</a></div>' : '')
        : '<div style="color:var(--muted);">No bills yet</div>')
      + '<h4>📦 Parts this party buys · current stock</h4>'
      + (top.length ? top.map(function(t){
          var p = getPartById(t.id);
          var st = p ? (p.stock || 0) : null;
          var col = st == null ? '#64748b' : (st <= 0 ? '#dc2626' : (p.minStock > 0 && st <= p.minStock ? '#d97706' : '#16a34a'));
          return '<div style="display:flex;justify-content:space-between;gap:6px;padding:3px 0;">'
            + '<span>' + esc(p ? p.name : t.name) + ' <small style="color:var(--muted);">(' + t.times + '× bought, ' + r2(t.qty) + ' total)</small></span>'
            + '<span style="font-weight:700;color:' + col + ';white-space:nowrap;">' + (st == null ? '—' : ('Stock ' + st)) + '</span></div>';
        }).join('') : '<div style="color:var(--muted);">No part history yet</div>')
      + '</div>';
  }
  window.gpPartyInfo = function(selectId) {
    var sel = $(selectId); if (!sel) return;
    var box = $(selectId + '-gpinfo');
    if (!box) { box = document.createElement('div'); box.id = selectId + '-gpinfo'; sel.parentNode.insertBefore(box, sel.nextSibling); }
    box.innerHTML = sel.value ? gpPartyInfoHtml(sel.value) : '';
  };

  window.gpEditPartyPhone = async function(custId) {
    if (ro()) { toast('🔒 Read-only access'); return; }
    var c = getCustomerById(custId); if (!c) return;
    var v = prompt('New phone number for ' + c.name + '\n(with country code, e.g. 919876543210)', c.phone || '');
    if (v === null) return;
    v = v.replace(/[^\d+]/g, '').replace(/^\+/, '');
    if (v && v.length === 10) v = '91' + v;
    var old = c.phone || '';
    c.phone = v;
    c.updatedAt = Date.now();
    if (typeof invalidateCustomerIndex === 'function') invalidateCustomerIndex();
    if (typeof markChanged === 'function') markChanged('customers');
    await save();
    try { logActivity('Party phone changed: ' + c.name + ' ' + (old || '—') + ' → ' + (v || '—'), 'info'); } catch(e){}
    ['sale-customer','wo-cust','pwb-cust','gped-cust'].forEach(function(id){ if ($(id) && $(id).value === custId) window.gpPartyInfo(id); });
    try { if (typeof woFillCustomerDetails === 'function' && $('wo-cust') && $('wo-cust').value === custId) woFillCustomerDetails(); } catch(e){}
    toast('✅ Phone updated for ' + c.name);
  };

  // ───────────────────────── Bill search ─────────────────────────
  window.gpViewBill = function(kind, id) {
    var b = findBill(kind, id); if (!b) { toast('Bill not found'); return; }
    while (_sheets.length) gpCloseSheet();
    if (kind === 'wo') openWODetail(id); else showInvoice(b);
  };
  window.gpOpenBillSearch = function(prefill) {
    var bg = gpSheet('🔎 Find Bill',
      '<input class="input" id="gps-q" placeholder="Bill no (INV-1001, WO-1005, 234…), party name or phone" autocomplete="off">'
      + '<div class="chips mt-1" id="gps-kind" style="display:flex;gap:6px;">'
      + '<div class="chip active" data-k="all">All</div><div class="chip" data-k="sale">Counter</div><div class="chip" data-k="wo">Wholesale</div></div>'
      + '<div id="gps-res" class="mt-1"></div>', { wide: true });
    var kind = 'all';
    var inp = bg.querySelector('#gps-q');
    if (prefill) inp.value = prefill;
    function draw() {
      var q = (inp.value || '').toLowerCase().trim();
      var qd = q.replace(/\D/g, '');
      var all = [];
      if (kind !== 'wo') (state.sales || []).forEach(function(s){ all.push({ kind: 'sale', b: s, no: s.invoiceNo || '', date: s.date }); });
      if (kind !== 'sale') (state.wholesaleOrders || []).forEach(function(o){ all.push({ kind: 'wo', b: o, no: 'WO-' + o.woNo + (o.invoiceNo ? ' · ' + o.invoiceNo : ''), date: o.date }); });
      var res = all.filter(function(x){
        if (!q) return true;
        var c = getCustomerById(x.b.customerId);
        var noStr = (x.no + ' ' + (x.b.invoiceNo || '')).toLowerCase();
        if (noStr.indexOf(q) >= 0) return true;
        if (qd && qd.length >= 2 && noStr.replace(/\D/g, ' ').split(' ').indexOf(qd) >= 0) return true;
        if (c && ((c.name || '').toLowerCase().indexOf(q) >= 0 || (c.phone || '').indexOf(q) >= 0)) return true;
        return false;
      }).sort(function(a, b){ return String(b.date).localeCompare(String(a.date)); });
      var box = bg.querySelector('#gps-res');
      if (!res.length) { box.innerHTML = '<div class="empty" style="padding:16px;">No bill found</div>'; return; }
      box.innerHTML = '<div style="font-size:12px;color:var(--muted);margin-bottom:6px;">' + res.length + ' bill(s)' + (res.length > 40 ? ' — showing latest 40' : '') + '</div>'
        + res.slice(0, 40).map(function(x){
          var c = getCustomerById(x.b.customerId);
          var ph = gpPhotosOf(x.b).length;
          return '<div style="background:#f8fafc;border-radius:10px;padding:10px 12px;margin-bottom:8px;">'
            + '<div style="display:flex;justify-content:space-between;gap:8px;">'
            + '<div><div style="font-weight:700;">' + esc(x.no) + ' <span class="gp-tag" style="background:' + (x.kind === 'wo' ? '#fef3c7;color:#92400e' : '#e0f2fe;color:#0369a1') + ';">' + (x.kind === 'wo' ? 'Wholesale' : 'Counter') + '</span>' + (ph ? ' <span class="gp-tag" style="background:#f1f5f9;color:#334155;">📷 ' + ph + '</span>' : '') + '</div>'
            + '<div style="font-size:12px;color:var(--muted);">' + esc(c ? c.name : 'Walk-in') + ' · ' + dateOnly(x.date) + ' · ' + (x.b.items || []).length + ' items</div></div>'
            + '<div style="font-weight:800;color:var(--primary);white-space:nowrap;">' + rs(x.b.total) + '</div></div>'
            + '<div style="display:flex;gap:6px;margin-top:8px;">'
            + '<button class="btn btn-sm btn-outline" style="flex:1;" onclick="gpViewBill(\'' + x.kind + '\',\'' + x.b.id + '\')">👁 View</button>'
            + '<button class="btn btn-sm btn-outline" style="flex:1;" onclick="gpOpenBillEditor(\'' + x.kind + '\',\'' + x.b.id + '\')">✏️ Edit</button>'
            + '<button class="btn btn-sm btn-outline" style="flex:1;" onclick="gpPrintBill(\'' + x.kind + '\',\'' + x.b.id + '\')">🖨 Print</button>'
            + '</div></div>';
        }).join('');
    }
    inp.addEventListener('input', draw);
    bg.querySelector('#gps-kind').addEventListener('click', function(e){
      var k = e.target.getAttribute('data-k'); if (!k) return;
      kind = k;
      bg.querySelectorAll('#gps-kind .chip').forEach(function(c){ c.classList.toggle('active', c === e.target); });
      draw();
    });
    draw();
    setTimeout(function(){ inp.focus(); }, 50);
  };

  // ───────────────────────── Print ─────────────────────────
  function billPrintHtml(kind, b, small) {
    var S = state.settings || {};
    var c = getCustomerById(b.customerId);
    var no = kind === 'wo' ? (b.invoiceNo || ('WO-' + b.woNo)) : b.invoiceNo;
    var rows = (b.items || []).map(function(it, i){
      var p = it.partId ? getPartById(it.partId) : null;
      var u = (p && p.unit && p.unit !== 'pcs') ? ' ' + p.unit : '';
      return '<tr><td>' + (i + 1) + '</td><td>' + esc(it.name) + (it.partNo ? '<br><small>' + esc(it.partNo) + '</small>' : '') + '</td><td class="r">' + r2(it.qty) + u + '</td><td class="r">' + rs(it.price) + '</td><td class="r">' + rs(it.qty * it.price) + '</td></tr>';
    }).join('');
    var soldBy = b.soldById && (state.staff || []).find(function(s){ return s.id === b.soldById; });
    var paid = kind === 'wo' ? (b.paidAmount || 0) : (b.paymentMode === 'credit' ? 0 : b.total);
    return '<div class="gpp' + (small ? ' small' : '') + '">'
      + '<h1>' + esc(S.shopName || 'Shop') + '</h1>'
      + (S.address ? '<div class="c">' + esc(S.address) + '</div>' : '')
      + (S.shopPhone ? '<div class="c">📞 ' + esc(S.shopPhone) + '</div>' : '')
      + (S.gstNo ? '<div class="c">GSTIN: ' + esc(S.gstNo) + '</div>' : '')
      + '<div class="line"></div>'
      + '<div class="c" style="font-weight:700;">' + (kind === 'wo' ? 'WHOLESALE BILL' : 'BILL / CASH MEMO') + '</div>'
      + '<div class="meta"><div>Bill No: <b>' + esc(no) + '</b>' + (kind === 'wo' && b.invoiceNo ? '<br><small>WO-' + b.woNo + '</small>' : '') + '</div><div>Date: <b>' + dateOnly(b.date) + '</b></div></div>'
      + '<div class="meta"><div>Party: <b>' + esc(c ? c.name : 'Walk-in Customer') + '</b>' + (c && c.city ? ', ' + esc(c.city) : '') + (c && c.phone ? '<br>📞 ' + esc(c.phone) : '') + '</div>'
      + '<div>Payment: <b>' + esc((b.paymentMode || '').toUpperCase()) + '</b>' + (soldBy ? '<br>Sold by: ' + esc(soldBy.name) : '') + '</div></div>'
      + '<table><thead><tr><th>#</th><th>Item</th><th class="r">Qty</th><th class="r">Rate</th><th class="r">Amount</th></tr></thead><tbody>' + rows + '</tbody></table>'
      + '<div style="margin-top:8px;">'
      + '<div class="meta"><span>Subtotal</span><span>' + rs(b.subtotal) + '</span></div>'
      + (b.discount ? '<div class="meta"><span>Discount</span><span>−' + rs(b.discount) + '</span></div>' : '')
      + (b.shippingCharge ? '<div class="meta"><span>Freight</span><span>+' + rs(b.shippingCharge) + '</span></div>' : '')
      + (b.packagingCharge ? '<div class="meta"><span>Packaging</span><span>+' + rs(b.packagingCharge) + '</span></div>' : '')
      + '<div class="line"></div>'
      + '<div class="meta tot"><span>TOTAL</span><span>' + rs(b.total) + '</span></div>'
      + (b.paymentMode === 'credit' ? '<div class="meta"><span>Paid</span><span>' + rs(paid) + '</span></div><div class="meta" style="font-weight:700;"><span>Balance Due</span><span>' + rs(Math.max(0, b.total - paid)) + '</span></div>' : '')
      + '</div>'
      + (b.notes ? '<div style="margin-top:8px;"><b>Note:</b> ' + esc(b.notes) + '</div>' : '')
      + (b.paymentMode === 'credit' && S.creditTerms ? '<div style="margin-top:6px;font-size:11px;">Credit terms: ' + esc(S.creditTerms) + '</div>' : '')
      + '<div class="line"></div><div class="c" style="font-size:11px;">Thank you! 🙏</div>'
      + (small ? '' : '<div style="display:flex;justify-content:flex-end;margin-top:40px;"><div style="border-top:1px solid #000;padding-top:4px;width:180px;text-align:center;">Signature</div></div>')
      + '</div>';
  }
  window.gpPrintBill = function(kind, id) {
    var b = findBill(kind, id); if (!b) { toast('Bill not found'); return; }
    var bg = gpSheet('🖨 Print Bill',
      '<div style="border:1px solid #e2e8f0;border-radius:10px;padding:12px;background:#fff;max-height:55vh;overflow:auto;">' + billPrintHtml(kind, b, false) + '</div>'
      + '<div class="row2 mt-2"><button class="btn" data-gpprint="a4">🖨 Print (A4 / A5)</button><button class="btn btn-outline" data-gpprint="small">🧾 Small (80mm)</button></div>', { wide: true });
    bg.addEventListener('click', function(e){
      var m = e.target && e.target.getAttribute && e.target.getAttribute('data-gpprint');
      if (!m) return;
      var area = $('gp-print-area');
      area.innerHTML = billPrintHtml(kind, b, m === 'small');
      document.body.classList.add('gp-printing');
      var done = function(){ document.body.classList.remove('gp-printing'); window.removeEventListener('afterprint', done); };
      window.addEventListener('afterprint', done);
      setTimeout(function(){ window.print(); setTimeout(done, 1500); }, 60);
    });
  };

  // ───────────────────────── BILL EDITOR ─────────────────────────
  var _ed = null; // { kind, id, orig, items, photos, ... }
  window.gpOpenBillEditor = function(kind, id, focus) {
    if (ro()) { toast('🔒 Read-only access — cannot edit bills'); return; }
    var b = findBill(kind, id); if (!b) { toast('Bill not found'); return; }
    while (_sheets.length) gpCloseSheet();
    try { closeModal(); } catch(e){}
    _ed = {
      kind: kind, id: id, orig: JSON.parse(JSON.stringify(b)),
      items: JSON.parse(JSON.stringify(b.items || [])),
      photos: (Array.isArray(b.photos) ? b.photos.slice() : (b.photoDataUrl && !isPh(b.photoDataUrl) ? [b.photoDataUrl] : [])).filter(function(v){ return !isPh(v); })
    };
    var custs = (state.customers || []).slice().sort(function(a, b){ return (a.name || '').localeCompare(b.name || ''); });
    var modes = kind === 'wo' ? ['credit','cash','upi','advance','bank','cheque'] : ['cash','upi','credit'];
    var dt = String(b.date || '').slice(0, 10);
    var hasReturn = kind === 'sale' && (state.saleReturns || []).some(function(r){ return r.originalSaleId === id; });
    var bg = gpSheet('✏️ Edit Bill — ' + esc(kind === 'wo' ? 'WO-' + b.woNo : b.invoiceNo),
      (hasReturn ? '<div style="background:#fef3c7;border-radius:8px;padding:8px 10px;font-size:12px;color:#92400e;margin-bottom:8px;">⚠️ This bill has a return recorded. Editing items here does not change the return.</div>' : '')
      + '<div class="row2"><div class="field"><label>Bill / Invoice No</label><input class="input" id="gped-inv" value="' + esc(b.invoiceNo || '') + '" placeholder="' + (kind === 'wo' ? 'e.g. 245 (WO-' + b.woNo + ' stays as reference)' : 'INV-1001') + '"></div>'
      + '<div class="field"><label>Bill Date</label><input type="date" class="input" id="gped-date" value="' + esc(dt) + '"></div></div>'
      + '<div class="field"><label>Party</label><div id="gped-cust-search"></div><select class="select" id="gped-cust" onchange="gpPartyInfo(\'gped-cust\')">'
      + '<option value="">' + (kind === 'wo' ? '— select party —' : 'Walk-in Customer') + '</option>'
      + custs.map(function(c){ return '<option value="' + esc(c.id) + '"' + (c.id === b.customerId ? ' selected' : '') + '>' + esc(c.name) + (c.city ? ' — ' + esc(c.city) : '') + '</option>'; }).join('')
      + '</select></div>'
      + '<div style="background:#f8fafc;border-radius:10px;padding:10px;margin-bottom:10px;">'
      + '<div style="font-size:13px;font-weight:700;margin-bottom:6px;">📦 Items</div>'
      + '<div id="gped-items"></div>'
      + '<input class="input mt-1" id="gped-search" placeholder="➕ Search part no / name to add…" autocomplete="off">'
      + '<div id="gped-sres"></div>'
      + '<div style="display:flex;gap:6px;margin-top:6px;flex-wrap:wrap;">'
      + '<button class="btn btn-sm btn-outline" style="width:auto;" onclick="gpOpenNewPart(\'edit\', (document.getElementById(\'gped-search\')||{}).value)">➕ New Part</button>'
      + '<button class="btn btn-sm btn-outline" style="width:auto;" id="gped-custom-btn">✎ Custom item</button></div>'
      + '<div id="gped-custom" style="display:none;margin-top:6px;"><input class="input" id="gped-cn" placeholder="Item name"><div class="row2 mt-1"><input type="number" class="input" id="gped-cq" placeholder="Qty" value="1"><input type="number" class="input" id="gped-cp" placeholder="Rate ₹"></div><button class="btn btn-sm mt-1" id="gped-cadd">+ Add</button></div>'
      + '</div>'
      + '<div class="row2"><div class="field"><label>Discount ₹</label><input type="number" class="input" id="gped-disc" value="' + (b.discount || 0) + '"></div>'
      + '<div class="field"><label>Payment Mode</label><select class="select" id="gped-mode">' + modes.map(function(m){ return '<option value="' + m + '"' + (m === b.paymentMode ? ' selected' : '') + '>' + m.toUpperCase() + '</option>'; }).join('') + '</select></div></div>'
      + (kind === 'wo' ? '<div class="row2"><div class="field"><label>Shipping / Freight ₹</label><input type="number" class="input" id="gped-ship" value="' + (b.shippingCharge || 0) + '"></div><div class="field"><label>Packaging ₹</label><input type="number" class="input" id="gped-pack" value="' + (b.packagingCharge || 0) + '"></div></div>' : '')
      + '<div id="gped-total" style="font-size:17px;font-weight:800;color:var(--primary);padding:10px;background:#f0fdf4;border-radius:10px;margin-bottom:10px;"></div>'
      + '<div class="field"><label>Notes</label><textarea class="input" id="gped-notes" rows="2">' + esc(b.notes || '') + '</textarea></div>'
      + '<div class="field" id="gped-photo-sec"><label>📷 Bill Photos</label><div class="gp-photos" id="gped-photos"></div>'
      + '<div style="display:flex;gap:6px;margin-top:8px;"><button class="btn btn-sm btn-outline" style="width:auto;" id="gped-cam">📸 Camera</button><button class="btn btn-sm btn-outline" style="width:auto;" id="gped-gal">🖼 Gallery (many)</button></div>'
      + '<small style="color:var(--muted);">Tap ✕ to remove a photo. Add new ones to replace.</small></div>'
      + '<button class="btn btn-success" id="gped-save">💾 Save Changes</button>'
      + '<button class="btn btn-outline mt-1" data-gpclose>Cancel</button>',
      { wide: true, sticky: true, onClose: function(){ _ed = null; } });
    _ed.bg = bg;
    gpAttachPartySearch('gped-cust', bg.querySelector('#gped-cust-search'));
    window.gpPartyInfo('gped-cust');
    bg.querySelector('#gped-search').addEventListener('input', edSearch);
    ['gped-disc','gped-ship','gped-pack'].forEach(function(i){ var el = bg.querySelector('#' + i); if (el) el.addEventListener('input', edTotals); });
    bg.querySelector('#gped-custom-btn').onclick = function(){ var f = bg.querySelector('#gped-custom'); f.style.display = f.style.display === 'none' ? '' : 'none'; };
    bg.querySelector('#gped-cadd').onclick = function(){
      var n = (bg.querySelector('#gped-cn').value || '').trim(), q = num(bg.querySelector('#gped-cq').value), pr = num(bg.querySelector('#gped-cp').value);
      if (!n || !q) { toast('Enter name and qty'); return; }
      _ed.items.push({ partId: null, partNo: '', name: n, qty: q, price: pr });
      bg.querySelector('#gped-cn').value = ''; bg.querySelector('#gped-cp').value = ''; bg.querySelector('#gped-custom').style.display = 'none';
      edRender();
    };
    bg.querySelector('#gped-cam').onclick = function(){ gpPickCamera(function(list){ _ed.photos = _ed.photos.concat(list); edPhotos(); }); };
    bg.querySelector('#gped-gal').onclick = function(){ gpPickPhotos(function(list){ _ed.photos = _ed.photos.concat(list); edPhotos(); }); };
    bg.querySelector('#gped-save').onclick = edSave;
    edRender(); edPhotos();
    if (focus === 'photos') setTimeout(function(){ var s = bg.querySelector('#gped-photo-sec'); if (s) s.scrollIntoView({ block: 'center' }); }, 80);
  };

  function edRender() {
    if (!_ed) return;
    var box = _ed.bg.querySelector('#gped-items');
    box.innerHTML = !_ed.items.length ? '<div style="color:var(--muted);font-size:13px;">No items — search below to add</div>'
      : _ed.items.map(function(it, i){
        var p = it.partId ? getPartById(it.partId) : null;
        return '<div class="gp-row">'
          + '<div style="flex:1;min-width:0;"><div style="font-weight:600;font-size:13px;">' + esc(it.name) + '</div>'
          + '<div style="font-size:11px;color:var(--muted);">' + esc(it.partNo || 'Custom item') + (p ? ' · Stock ' + (p.stock || 0) : '') + '</div>'
          + (it.partId ? '<button class="gp-rate-chip" onclick="gpOpenRateEditor(\'edit\',' + i + ')">✎ H/C/R</button>' : '') + '</div>'
          + '<input type="number" class="gp-mini" value="' + it.qty + '" step="any" onchange="gpEdSet(' + i + ',\'qty\',this.value)" title="Qty">'
          + '<span style="color:var(--muted);">×</span>'
          + '<input type="number" class="gp-mini" style="width:72px;" value="' + it.price + '" step="any" onchange="gpEdSet(' + i + ',\'price\',this.value)" title="Rate">'
          + '<div style="min-width:64px;text-align:right;font-weight:700;font-size:13px;">' + rs(it.qty * it.price) + '</div>'
          + '<button onclick="gpEdSet(' + i + ',\'del\')" style="background:none;border:none;color:#dc2626;font-size:16px;cursor:pointer;">✕</button></div>';
      }).join('');
    edTotals();
  }
  window.gpEdSet = function(i, f, v) {
    if (!_ed || !_ed.items[i]) return;
    if (f === 'del') _ed.items.splice(i, 1);
    else if (f === 'qty') { var q = num(v); if (q <= 0) _ed.items.splice(i, 1); else _ed.items[i].qty = r2(q); }
    else if (f === 'price') _ed.items[i].price = Math.max(0, num(v));
    edRender();
  };
  function edCalc() {
    var g = function(id){ var el = _ed.bg.querySelector('#' + id); return el ? num(el.value) : 0; };
    var sub = _ed.items.reduce(function(a, it){ return a + it.qty * it.price; }, 0);
    var disc = g('gped-disc'), ship = g('gped-ship'), pack = g('gped-pack');
    return { sub: r2(sub), disc: disc, ship: ship, pack: pack, total: r2(Math.max(0, sub - disc + ship + pack)) };
  }
  function edTotals() {
    if (!_ed) return;
    var t = edCalc(), o = _ed.orig;
    _ed.bg.querySelector('#gped-total').innerHTML = '<div class="flex-between" style="font-size:13px;font-weight:400;color:var(--muted);"><span>Items</span><span>' + rs(t.sub) + '</span></div>'
      + '<div class="flex-between"><span>NEW TOTAL</span><span>' + rs(t.total) + '</span></div>'
      + (Math.round(t.total) !== Math.round(o.total || 0) ? '<div style="font-size:12px;font-weight:600;color:#b45309;">Was ' + rs(o.total) + ' → change ' + (t.total > o.total ? '+' : '') + rs(t.total - o.total) + '</div>' : '');
  }
  function edSearch() {
    var q = (_ed.bg.querySelector('#gped-search').value || '').toLowerCase().trim();
    var box = _ed.bg.querySelector('#gped-sres');
    if (!q) { box.innerHTML = ''; return; }
    var res = [];
    for (var i = 0; i < state.parts.length && res.length < 8; i++) {
      var p = state.parts[i];
      if ((p.partNo || '').toLowerCase().indexOf(q) >= 0 || (p.name || '').toLowerCase().indexOf(q) >= 0 || (p.brand || '').toLowerCase().indexOf(q) >= 0) res.push(p);
    }
    box.innerHTML = res.length ? res.map(function(p){
      return '<div class="part-item" onclick="gpEdAddPart(\'' + p.id + '\')"><div class="part-row"><div><div class="part-name">' + esc(p.name) + '</div><div class="part-no">' + esc(p.partNo) + ' · Stock ' + (p.stock || 0) + '</div></div>'
        + '<div class="part-prices" style="font-size:11px;text-align:right;">H ' + rs(p.creditPrice) + '<br>R ' + rs(p.instantPrice) + '</div></div></div>';
    }).join('') : '<div style="color:var(--muted);font-size:13px;padding:6px 0;">Not found — use ➕ New Part</div>';
  }
  function edAddPart(partId) {
    if (!_ed) return;
    var p = getPartById(partId); if (!p) return;
    var ex = _ed.items.find(function(it){ return it.partId === partId; });
    var cust = getCustomerById((_ed.bg.querySelector('#gped-cust') || {}).value);
    var rate = _ed.kind === 'wo' ? (p.creditPrice || p.instantPrice)
      : ((cust && cust.trusted && (_ed.bg.querySelector('#gped-mode') || {}).value === 'credit') ? (p.creditPrice || p.instantPrice) : (p.instantPrice || p.creditPrice));
    if (ex) ex.qty = r2(ex.qty + 1);
    else _ed.items.push({ partId: p.id, partNo: p.partNo, name: p.name, qty: 1, price: rate || 0 });
    var s = _ed.bg.querySelector('#gped-search'); if (s) s.value = '';
    _ed.bg.querySelector('#gped-sres').innerHTML = '';
    edRender();
  }
  window.gpEdAddPart = edAddPart;
  function edPhotos() {
    if (!_ed) return;
    var box = _ed.bg.querySelector('#gped-photos');
    box.innerHTML = _ed.photos.length ? _ed.photos.map(function(src, i){
      return '<div class="gp-photo"><img src="' + esc(src) + '"><button onclick="gpEdDelPhoto(' + i + ')">✕</button></div>';
    }).join('') : '<div style="color:var(--muted);font-size:13px;">No photos</div>';
  }
  window.gpEdDelPhoto = function(i){ if (_ed) { _ed.photos.splice(i, 1); edPhotos(); } };

  async function edSave() {
    if (!_ed) return;
    var ed = _ed, bg = ed.bg, kind = ed.kind;
    var live = findBill(kind, ed.id);
    if (!live) { toast('Bill no longer exists'); return; }
    if (!ed.items.length && !confirm('This bill has no items. Save anyway?')) return;
    var inv = (bg.querySelector('#gped-inv').value || '').trim();
    var custId = bg.querySelector('#gped-cust').value || null;
    if (kind === 'wo' && !custId) { toast('Select a party'); return; }
    if (kind === 'sale' && !inv) { toast('Bill number cannot be empty'); return; }
    // Duplicate bill no warning
    if (inv && inv !== (live.invoiceNo || '')) {
      var clash = (state.sales || []).some(function(s){ return s.id !== ed.id && (s.invoiceNo || '').toLowerCase() === inv.toLowerCase(); })
        || (state.wholesaleOrders || []).some(function(o){ return o.id !== ed.id && ((o.invoiceNo || '').toLowerCase() === inv.toLowerCase() || ('wo-' + o.woNo) === inv.toLowerCase()); });
      if (clash && !confirm('⚠️ Bill no "' + inv + '" is already used on another bill. Save anyway?')) return;
    }
    var t = edCalc();
    var mode = bg.querySelector('#gped-mode').value;
    var newDate = bg.querySelector('#gped-date').value;

    // ── Stock: sale bills always moved stock; WOs only when stock was deducted and not a paper "previous bill"
    var movesStock = kind === 'sale' || (live.stockDeducted && !live.historical);
    var stockTouched = [];
    if (movesStock) {
      var oldQ = {}, newQ = {};
      (live.items || []).forEach(function(it){ if (it.partId) oldQ[it.partId] = (oldQ[it.partId] || 0) + num(it.qty); });
      ed.items.forEach(function(it){ if (it.partId) newQ[it.partId] = (newQ[it.partId] || 0) + num(it.qty); });
      var ids = {}; Object.keys(oldQ).concat(Object.keys(newQ)).forEach(function(k){ ids[k] = 1; });
      var short = [];
      Object.keys(ids).forEach(function(pid){
        var d = (newQ[pid] || 0) - (oldQ[pid] || 0);
        var p = getPartById(pid);
        if (d > 0 && p && d > (p.stock || 0)) short.push(p.name + ' (need ' + r2(d) + ' more, stock ' + (p.stock || 0) + ')');
      });
      if (short.length && !confirm('⚠️ Not enough stock for:\n' + short.join('\n') + '\n\nSave anyway? (stock will go to 0)')) return;
      Object.keys(ids).forEach(function(pid){
        var d = (newQ[pid] || 0) - (oldQ[pid] || 0);
        if (!d) return;
        var p = getPartById(pid); if (!p) return;
        p.stock = Math.round(Math.max(0, (p.stock || 0) - d) * 1000) / 1000;
        p.updatedAt = Date.now();
        stockTouched.push(p);
      });
    }

    // ── Customer due
    var oldCust = getCustomerById(live.customerId), newCust = getCustomerById(custId);
    var oldContrib, newContrib, paid = live.paidAmount || 0, payments = (live.payments || []).slice(), settled;
    if (kind === 'wo') {
      oldContrib = (live.paymentMode === 'credit' && !live.settled) ? Math.max(0, (live.total || 0) - paid) : 0;
      if (mode !== 'credit' && paid < t.total) {
        payments.push({ date: new Date().toISOString(), amount: r2(t.total - paid), mode: mode, ref: 'bill edit' });
        paid = t.total;
      }
      settled = paid >= t.total;
      newContrib = (mode === 'credit' && !settled) ? Math.max(0, t.total - paid) : 0;
    } else {
      oldContrib = (live.paymentMode === 'credit' && !live.settled) ? (live.total || 0) : 0;
      settled = mode !== 'credit' ? true : (live.paymentMode === 'credit' ? !!live.settled : false);
      newContrib = (mode === 'credit' && !settled) ? t.total : 0;
    }
    if (oldCust) oldCust.totalDue = Math.max(0, (oldCust.totalDue || 0) - oldContrib);
    if (newCust) newCust.totalDue = (newCust.totalDue || 0) + newContrib;

    // ── Apply the edit
    var changes = [];
    if ((live.invoiceNo || '') !== inv) changes.push('bill no ' + (live.invoiceNo || '—') + '→' + (inv || '—'));
    if (Math.round(live.total || 0) !== Math.round(t.total)) changes.push('total ' + rs(live.total) + '→' + rs(t.total));
    if ((live.customerId || null) !== custId) changes.push('party changed');
    if (JSON.stringify(live.items || []) !== JSON.stringify(ed.items)) changes.push('items changed');
    if (gpPhotosOf(live).length !== ed.photos.length || gpPhotosOf(live).some(function(v, i){ return v !== ed.photos[i]; })) changes.push('photos ' + ed.photos.length);
    live.invoiceNo = inv;
    if (newDate && newDate !== String(live.date || '').slice(0, 10)) live.date = newDate + String(live.date || 'T12:00:00').slice(10) || (newDate + 'T12:00:00');
    live.customerId = custId;
    live.items = ed.items.map(function(it){ return { partId: it.partId || null, partNo: it.partNo || '', name: it.name, qty: it.qty, price: it.price }; });
    live.subtotal = t.sub; live.discount = t.disc; live.total = t.total;
    if (kind === 'wo') { live.shippingCharge = t.ship; live.packagingCharge = t.pack; live.paidAmount = paid; live.payments = payments; }
    else live.saleType = custId ? 'wholesale' : 'retail';
    live.paymentMode = mode; live.settled = settled;
    live.notes = (bg.querySelector('#gped-notes').value || '').trim();
    live.photos = ed.photos.slice();
    live.photoDataUrl = ed.photos[0] || null;
    live.updatedAt = Date.now();
    live.editedAt = new Date().toISOString();
    live.editedBy = who();
    live.editLog = (live.editLog || []).concat([{ at: live.editedAt, by: who(), what: changes.join(', ') || 'saved' }]).slice(-30);

    if (kind === 'sale') { try { invalidateSalesIndex(); } catch(e){} }
    try { invalidateVelocityCache(); } catch(e){}
    if (typeof markChanged === 'function') { markChanged(kind === 'wo' ? 'wholesaleOrders' : 'sales'); markChanged('customers'); if (stockTouched.length) markChanged('parts'); }
    if (typeof invalidateCustomerIndex === 'function') invalidateCustomerIndex();
    await save();
    for (var i = 0; i < stockTouched.length; i++) {
      try { await cloudPersistPartFields(stockTouched[i].id, { stock: stockTouched[i].stock, updatedAt: stockTouched[i].updatedAt }); } catch(e){}
    }
    try { if (stockTouched.length && typeof xlsRefreshStockCells === 'function') xlsRefreshStockCells(stockTouched.map(function(p){ return p.id; })); } catch(e){}
    try { logActivity('Bill edited: ' + (kind === 'wo' ? 'WO-' + live.woNo : live.invoiceNo) + ' — ' + (changes.join(', ') || 'no change') + ' by ' + who(), 'warn'); } catch(e){}
    gpUploadBillPhotos(kind, live);
    _ed = null;
    while (_sheets.length) gpCloseSheet();
    try { if (kind === 'wo') renderWholesaleOrders(); } catch(e){}
    try { renderHomeDebounced(); } catch(e){}
    toast('✅ Bill saved' + (changes.length ? ' — ' + changes.join(', ') : ''));
    gpViewBill(kind, live.id);
  }

  // ───────────────────────── Hook existing screens ─────────────────────────
  function wrap(name, after) {
    var orig = window[name];
    if (typeof orig !== 'function') return;
    window[name] = function(){
      var r = orig.apply(this, arguments);
      try { after.apply(this, arguments); } catch(e){ console.warn('[gp] enhance ' + name + ':', e && e.message); }
      return r;
    };
  }
  function addNewPartBtn(afterId, ctx) {
    var anchor = $(afterId); if (!anchor || $('gp-np-' + ctx)) return;
    var b = document.createElement('button');
    b.type = 'button'; b.id = 'gp-np-' + ctx;
    b.className = 'btn btn-sm btn-outline mt-1';
    b.textContent = '➕ New Part — save permanently with H/C/R';
    b.onclick = function(){ var s = $(ctx === 'wo' ? 'wo-search' : 'pwb-search'); gpOpenNewPart(ctx, s ? s.value : ''); };
    anchor.parentNode.insertBefore(b, anchor.nextSibling);
  }
  wrap('openNewWholesaleOrder', function(){
    gpAttachPartySearch('wo-cust');
    $('wo-cust').addEventListener('change', function(){ window.gpPartyInfo('wo-cust'); });
    addNewPartBtn('wo-search-results', 'wo');
  });
  wrap('openAddPreviousBill', function(){
    gpAttachPartySearch('pwb-cust');
    $('pwb-cust').addEventListener('change', function(){ window.gpPartyInfo('pwb-cust'); });
    addNewPartBtn('pwb-search-results', 'pwb');
    // Multi-photo picker replaces the single photo field
    window._gpPwbPhotos = [];
    var wrapEl = $('pwb-photo-preview-wrap');
    if (wrapEl) {
      var field = wrapEl.parentNode;
      field.innerHTML = '<label>📷 Bill Photos <span style="color:var(--muted);font-weight:400;font-size:12px;">(add as many as you need)</span></label>'
        + '<div style="display:flex;gap:6px;margin-top:6px;"><button type="button" class="btn btn-sm btn-outline" style="width:auto;" id="gp-pwb-cam">📸 Camera</button><button type="button" class="btn btn-sm btn-outline" style="width:auto;" id="gp-pwb-gal">🖼 Gallery (many)</button></div>'
        + '<div class="gp-photos" id="gp-pwb-photos"></div>';
      var draw = function(){
        $('gp-pwb-photos').innerHTML = window._gpPwbPhotos.map(function(src, i){ return '<div class="gp-photo"><img src="' + src + '"><button type="button" onclick="window._gpPwbPhotos.splice(' + i + ',1);document.getElementById(\'gp-pwb-cam\').dispatchEvent(new Event(\'gpdraw\'))">✕</button></div>'; }).join('');
      };
      $('gp-pwb-cam').addEventListener('gpdraw', draw);
      $('gp-pwb-cam').onclick = function(){ gpPickCamera(function(l){ window._gpPwbPhotos = window._gpPwbPhotos.concat(l); draw(); }); };
      $('gp-pwb-gal').onclick = function(){ gpPickPhotos(function(l){ window._gpPwbPhotos = window._gpPwbPhotos.concat(l); draw(); }); };
    }
  });
  // After a previous bill is saved, attach ALL its photos (original handled only one)
  (function(){
    var orig = window.savePreviousWholesaleBill;
    if (typeof orig !== 'function') return;
    window.savePreviousWholesaleBill = async function(){
      var photos = (window._gpPwbPhotos || []).slice();
      var hidden = $('pwb-photo-data'); if (hidden) hidden.value = '';
      var before = {}; (state.wholesaleOrders || []).forEach(function(o){ before[o.id] = 1; });
      var r = await orig.apply(this, arguments);
      var wo = (state.wholesaleOrders || []).find(function(o){ return !before[o.id]; });
      if (!wo) { if (hidden && photos[0]) hidden.value = ''; return r; } // validation stopped the save — keep picked photos
      if (photos.length) {
        {
          wo.photos = photos; wo.photoDataUrl = photos[0]; wo.updatedAt = Date.now();
          if (typeof markChanged === 'function') markChanged('wholesaleOrders');
          await save();
          try { renderWholesaleOrders(); } catch(e){}
          gpUploadBillPhotos('wo', wo);
        }
      }
      window._gpPwbPhotos = [];
      return r;
    };
  })();
  // "Attach photo" on an old bill now opens the editor's photo section (supports many)
  window.attachWOBillPhoto = function(woId){ window.gpOpenBillEditor('wo', woId, 'photos'); };

  // ───────────────────────── NOTES (staff → owner) ─────────────────────────
  function notes(){ if (!Array.isArray(state.shopNotes)) state.shopNotes = []; return state.shopNotes; }
  function gpNotesBadge() {
    var n = notes().filter(function(x){ return !x.done && !x.hidden; }).length;
    var b = $('gp-notes-badge'); if (!b) return;
    b.textContent = n; b.style.display = n ? '' : 'none';
  }
  setInterval(gpNotesBadge, 4000);
  setTimeout(gpNotesBadge, 1500);
  var NOTE_TYPES = { out: '🔴 Part khatam', low: '🟠 Stock kam', order: '📦 Order karna', other: '📝 Other' };
  window.gpOpenNotes = function(prePartId) {
    var bg = gpSheet('📝 Notes for Sir',
      '<div style="background:#f8fafc;border-radius:10px;padding:10px;margin-bottom:12px;">'
      + '<div class="chips" id="gpnt-type" style="display:flex;gap:6px;flex-wrap:wrap;">' + Object.keys(NOTE_TYPES).map(function(k, i){ return '<div class="chip' + (i === 0 ? ' active' : '') + '" data-t="' + k + '">' + NOTE_TYPES[k] + '</div>'; }).join('') + '</div>'
      + '<input class="input mt-1" id="gpnt-part" placeholder="Part (optional) — search name / part no" autocomplete="off"><div id="gpnt-pres"></div>'
      + '<div id="gpnt-sel" style="font-size:13px;margin:4px 0;"></div>'
      + '<textarea class="input mt-1" id="gpnt-text" rows="2" placeholder="Likho… e.g. 6205 bearing khatam ho gaya, 2 customer ne maanga"></textarea>'
      + '<button class="btn btn-success mt-1" id="gpnt-add">➕ Add Note</button></div>'
      + '<div class="chips" id="gpnt-filter" style="display:flex;gap:6px;"><div class="chip active" data-f="open">Open</div><div class="chip" data-f="done">Done</div><div class="chip" data-f="all">All</div></div>'
      + '<div id="gpnt-list" class="mt-1"></div>');
    var type = 'out', partId = prePartId || null, filter = 'open';
    function drawSel() {
      var p = partId ? getPartById(partId) : null;
      bg.querySelector('#gpnt-sel').innerHTML = p ? '🔧 <b>' + esc(p.name) + '</b> (' + esc(p.partNo) + ') · stock ' + (p.stock || 0) + ' <a href="#" id="gpnt-unsel" style="color:#dc2626;">✕</a>' : '';
      var u = bg.querySelector('#gpnt-unsel'); if (u) u.onclick = function(e){ e.preventDefault(); partId = null; drawSel(); };
    }
    function drawList() {
      var list = notes().filter(function(n){ return !n.hidden && (filter === 'all' || (filter === 'done' ? n.done : !n.done)); })
        .sort(function(a, b){ return (b.createdAt || 0) - (a.createdAt || 0); });
      bg.querySelector('#gpnt-list').innerHTML = list.length ? list.map(function(n){
        var p = n.partId ? getPartById(n.partId) : null;
        return '<div class="gp-note' + (n.done ? ' done' : '') + '">'
          + '<div style="display:flex;justify-content:space-between;gap:8px;"><b style="font-size:13px;">' + (NOTE_TYPES[n.type] || '📝') + '</b><small style="color:var(--muted);">' + esc(n.by || '') + ' · ' + new Date(n.createdAt).toLocaleString('en-IN', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' }) + '</small></div>'
          + (p ? '<div style="font-size:13px;margin-top:3px;">🔧 ' + esc(p.name) + ' <small>(' + esc(p.partNo) + ')</small> · <b style="color:' + ((p.stock || 0) <= 0 ? '#dc2626' : '#16a34a') + ';">stock ' + (p.stock || 0) + '</b></div>' : (n.partName ? '<div style="font-size:13px;margin-top:3px;">🔧 ' + esc(n.partName) + '</div>' : ''))
          + (n.text ? '<div style="font-size:14px;margin-top:4px;">' + esc(n.text) + '</div>' : '')
          + (n.done ? '<div style="font-size:12px;color:#16a34a;margin-top:4px;">✓ Done by ' + esc(n.doneBy || '') + '</div>'
              : '<div style="display:flex;gap:6px;margin-top:8px;"><button class="btn btn-sm btn-success" style="width:auto;" data-done="' + n.id + '">✓ Done</button>' + (p && isOwnerish() && typeof openCreatePO === 'function' ? '<button class="btn btn-sm btn-outline" style="width:auto;" data-po="' + n.id + '">📋 Make PO</button>' : '') + '</div>')
          + (n.done && isOwnerish() ? '<a href="#" data-hide="' + n.id + '" style="font-size:11px;color:var(--muted);">Hide</a>' : '')
          + '</div>';
      }).join('') : '<div class="empty" style="padding:16px;">No notes</div>';
    }
    bg.querySelector('#gpnt-type').addEventListener('click', function(e){
      var t = e.target.getAttribute('data-t'); if (!t) return; type = t;
      bg.querySelectorAll('#gpnt-type .chip').forEach(function(c){ c.classList.toggle('active', c === e.target); });
    });
    bg.querySelector('#gpnt-filter').addEventListener('click', function(e){
      var f = e.target.getAttribute('data-f'); if (!f) return; filter = f;
      bg.querySelectorAll('#gpnt-filter .chip').forEach(function(c){ c.classList.toggle('active', c === e.target); });
      drawList();
    });
    bg.querySelector('#gpnt-part').addEventListener('input', function(){
      var q = (this.value || '').toLowerCase().trim(), box = bg.querySelector('#gpnt-pres');
      if (!q) { box.innerHTML = ''; return; }
      var res = state.parts.filter(function(p){ return (p.partNo || '').toLowerCase().indexOf(q) >= 0 || (p.name || '').toLowerCase().indexOf(q) >= 0; }).slice(0, 6);
      box.innerHTML = res.map(function(p){ return '<div class="part-item" data-pid="' + p.id + '" style="padding:8px 10px;"><b>' + esc(p.name) + '</b> <small>' + esc(p.partNo) + ' · stock ' + (p.stock || 0) + '</small></div>'; }).join('');
    });
    bg.querySelector('#gpnt-pres').addEventListener('click', function(e){
      var d = e.target.closest('[data-pid]'); if (!d) return;
      partId = d.getAttribute('data-pid'); bg.querySelector('#gpnt-part').value = ''; bg.querySelector('#gpnt-pres').innerHTML = ''; drawSel();
    });
    bg.querySelector('#gpnt-add').onclick = async function(){
      if (ro()) { toast('🔒 Read-only access'); return; }
      var text = (bg.querySelector('#gpnt-text').value || '').trim();
      var typed = (bg.querySelector('#gpnt-part').value || '').trim();
      if (!text && !partId && !typed) { toast('Write something or pick a part'); return; }
      var p = partId ? getPartById(partId) : null;
      notes().push({ id: uid(), type: type, text: text, partId: partId || null, partName: p ? p.name : typed, partNo: p ? p.partNo : '', by: who(), createdAt: Date.now(), updatedAt: Date.now(), done: false });
      if (typeof markChanged === 'function') markChanged('shopNotes');
      await save();
      bg.querySelector('#gpnt-text').value = ''; bg.querySelector('#gpnt-part').value = ''; partId = null; drawSel();
      drawList(); gpNotesBadge();
      toast('✅ Note saved — Sir will see it');
    };
    bg.querySelector('#gpnt-list').addEventListener('click', async function(e){
      var id = e.target.getAttribute('data-done') || e.target.getAttribute('data-hide') || e.target.getAttribute('data-po');
      if (!id) return;
      e.preventDefault();
      var n = notes().find(function(x){ return x.id === id; }); if (!n) return;
      if (e.target.getAttribute('data-po')) { while (_sheets.length) gpCloseSheet(); openCreatePO(); setTimeout(function(){ try { toast('Add ' + n.partName + ' to this PO'); } catch(_){} }, 300); return; }
      if (e.target.getAttribute('data-done')) { n.done = true; n.doneBy = who(); n.doneAt = Date.now(); }
      else n.hidden = true;
      n.updatedAt = Date.now();
      if (typeof markChanged === 'function') markChanged('shopNotes');
      await save(); drawList(); gpNotesBadge();
    });
    drawSel(); drawList();
  };

  // ───────────────────────── AI HELP ─────────────────────────
  var HELP_CONTEXT = [
    'You are the friendly help assistant inside "Goury Parts", a spare-parts shop app (tractor/harvester/JCB parts) used by the owner ("Sir") and shop staff in India.',
    'Answer in simple Hinglish (Hindi written in English letters) unless the user writes in English or Hindi script — then match them. Keep answers short, step by step, with the exact button names below.',
    'Rates: H = Wholesale rate, C = Cost price, R = Retail rate. MRP is printed price.',
    'APP FEATURES:',
    '- Bottom menu tabs: Home, Stock (inventory), Catalogue, Sale (counter sale), Wholesale, WhatsApp, AI Brain, Staff, Reports, Purchases, Owner.',
    '- Counter Sale: Sale tab → search party with "🔍 Search party" box (shows baki/due, old bills and parts this party buys with current stock) → search part → tap to add to cart → in cart tap "✎ Rate · H/C/R" to change the rate on this bill and/or save new H, C, R, MRP on the part permanently → choose Cash/UPI/Credit → "✓ Complete Sale & Print Bill".',
    '- New part while billing: "➕ New Part" button under the part search (Sale tab, New Wholesale Order, Previous Bill, Bill editor). Enter name, stock, rack, H/C/R rates → saves the part permanently and adds it to the bill.',
    '- Find a saved bill: "🔎 Find Bill" (Sale tab top, and Wholesale tab). Search by bill number (INV-1001, WO-1005 or your own invoice no), party name or phone. Each result has View / Edit / Print.',
    '- Edit a saved bill: open the bill → "✏️ Edit Bill" (also on Wholesale cards and in Find Bill). You can change bill/invoice number, date, party, add or remove parts, change qty and rate, discount, freight, payment mode, notes, and add/remove/replace multiple bill photos. Stock and party due are adjusted automatically. Edits are logged.',
    '- Print: "🖨 Print" on any bill → choose A4/A5 or Small 80mm receipt.',
    '- Party phone change: after selecting a party, in the blue party box tap "✏️ Change phone".',
    '- Bill photos: many photos per bill — in Previous Bill use Camera / Gallery (many); for any saved bill use Edit Bill → Bill Photos.',
    '- Wholesale tab: "+ New Order" (party order with dispatch/bus details), "📷 Previous Bill" (record an old paper bill with photos), "+ New Customer", Collect payments, Recycle Bin (deleted bills stay 30 days).',
    '- Notes for Sir: "📝" button at the top of the screen. Pick type (Part khatam / Stock kam / Order karna / Other), pick the part, write a note → Sir sees a red badge count. Sir taps "✓ Done" when handled.',
    '- Catalogue tab → "Add part to rack": select rack, fill part number, name, brand, unit, stock, rack, MRP and optional H/C/R rates. If C and R are filled the part is ready to sell immediately; otherwise it goes to the owner pricing queue ("Needs your pricing").',
    '- Stock tab: search and edit parts, low-stock and out-of-stock counts on Home.',
    '- Returns: open a counter bill → "↩️ Process Return".',
    '- WhatsApp: send bill to party with "📲 Send Bill on WhatsApp".',
    'If you are not sure a feature exists, say so honestly and suggest asking Sir. Never invent buttons. Do not give financial or legal advice.'
  ].join('\n');
  var FAQ = [
    { k: ['edit','badal','change bill','sudhar','galat'], a: 'Bill edit karne ke liye:\n1. "🔎 Find Bill" dabao (Sale tab ke upar ya Wholesale tab mein)\n2. Bill number ya party ka naam likho\n3. "✏️ Edit" dabao\n4. Part add/remove, qty, rate, invoice no., photo — sab badal sakte ho\n5. "💾 Save Changes" — stock aur baki apne aap theek ho jayega.' },
    { k: ['print'], a: 'Bill print: bill kholo → "🖨 Print" → A4/A5 ya Small (80mm) chuno.' },
    { k: ['h/c/r','hcr','rate','price','daam','keemat'], a: 'H = Wholesale, C = Cost, R = Retail.\nBill banate waqt cart mein part ke niche "✎ Rate · H/C/R" dabao. Upar H/C/R/MRP badlo (part pe hamesha ke liye save), niche "Rate on THIS bill" sirf is bill ke liye.' },
    { k: ['new part','naya part','nahi mil','not found'], a: 'Part list mein nahi hai? Part search ke niche "➕ New Part" dabao → naam, stock, rack, H/C/R rate bharo → "Save part & add to bill". Part hamesha ke liye save ho jayega.' },
    { k: ['phone','number','mobile'], a: 'Party ka phone badalna: party select karo → neeche blue box mein "✏️ Change phone" dabao → naya number likho.' },
    { k: ['photo','foto','image'], a: 'Bill ki photo: Previous Bill mein "📸 Camera" ya "🖼 Gallery (many)" se kai photo lagao. Saved bill mein: Edit Bill → Bill Photos → ✕ se hatao, naye add karo.' },
    { k: ['note','khatam','sir ko','bata'], a: 'Sir ko batana: upar "📝" button dabao → type chuno (Part khatam etc.) → part chuno → likho → "➕ Add Note". Sir ko red badge dikhega.' },
    { k: ['search','dhundh','find','bill no','invoice'], a: '"🔎 Find Bill" se bill number (jaise INV-1001 ya WO-1005), party naam ya phone se bill dhundho.' },
    { k: ['party','customer','baki','due'], a: 'Sale tab mein "🔍 Search party" box mein naam/phone likho. Party chunte hi baki (due), purane bills aur party ke parts ka stock dikhega.' },
    { k: ['catalogue','rack'], a: 'Catalogue → rack chuno → part details bharo → H/C/R rate (optional) → "💾 Save draft". C aur R dono bhare to part turant bikne ke liye ready.' }
  ];
  function faqAnswer(q) {
    q = (q || '').toLowerCase();
    var best = null, score = 0;
    FAQ.forEach(function(f){ var s = f.k.filter(function(k){ return q.indexOf(k) >= 0; }).length; if (s > score) { score = s; best = f; } });
    return best ? best.a : 'Mujhe iska pakka jawab nahi pata. Ye try karo: "bill edit kaise kare", "rate kaise badle", "naya part", "print", "note sir ko". Ya Sir se poochho.';
  }
  var _chat = [];
  window.gpOpenAIHelp = function() {
    var bg = gpSheet('🤖 AI Help — software ke baare mein poochho',
      '<div class="gp-chat" id="gpai-chat"></div>'
      + '<div class="gp-sugg" id="gpai-sugg">'
      + ['Bill edit kaise kare?', 'H C R rate kaise badle?', 'Naya part bill mein kaise jode?', 'Bill print kaise kare?', 'Sir ko note kaise bheje?', 'Party ka phone kaise badle?'].map(function(s){ return '<button>' + s + '</button>'; }).join('')
      + '</div>'
      + '<div style="display:flex;gap:6px;position:sticky;bottom:-22px;background:#fff;padding:8px 0;">'
      + '<input class="input" id="gpai-q" placeholder="Apna sawaal likho…" style="flex:1;">'
      + '<button class="btn" id="gpai-send" style="width:auto;">➤</button></div>');
    var chat = bg.querySelector('#gpai-chat'), inp = bg.querySelector('#gpai-q');
    function draw() {
      chat.innerHTML = (_chat.length ? '' : '<div class="gp-msg a">Namaste 🙏 Main is software ke baare mein madad karta hoon. Kuch bhi poochho — bill edit, rate, naya part, print, notes…</div>')
        + _chat.map(function(m){ return '<div class="gp-msg ' + (m.role === 'user' ? 'u' : 'a') + '">' + esc(m.content) + '</div>'; }).join('');
      chat.scrollTop = chat.scrollHeight;
      var sheet = bg.querySelector('.gp-sheet'); sheet.scrollTop = sheet.scrollHeight;
    }
    async function ask(q) {
      q = (q || '').trim(); if (!q) return;
      _chat.push({ role: 'user', content: q }); inp.value = ''; draw();
      var thinking = document.createElement('div'); thinking.className = 'gp-msg a'; thinking.textContent = '…soch raha hoon'; chat.appendChild(thinking);
      var answer;
      try {
        var stats = 'Current user: ' + who() + ' (role ' + role() + '). Parts: ' + (state.parts || []).length + ', customers: ' + (state.customers || []).length + ', open notes: ' + notes().filter(function(n){ return !n.done && !n.hidden; }).length + '.';
        var msgs = _chat.slice(-9).map(function(m){ return { role: m.role, content: m.content }; });
        while (msgs.length && msgs[0].role !== 'user') msgs.shift();
        msgs[0] = { role: 'user', content: HELP_CONTEXT + '\n' + stats + '\n\nUser question:\n' + msgs[0].content };
        answer = await aiCall({ messages: msgs, maxTokens: 700 });
        if (!answer) throw new Error('empty');
      } catch(e) {
        answer = faqAnswer(q) + (e && e.message && e.message !== 'empty' ? '\n\n(AI abhi available nahi: ' + e.message + ')' : '');
      }
      thinking.remove();
      _chat.push({ role: 'assistant', content: answer });
      if (_chat.length > 30) _chat = _chat.slice(-30);
      draw();
    }
    bg.querySelector('#gpai-send').onclick = function(){ ask(inp.value); };
    inp.addEventListener('keydown', function(e){ if (e.key === 'Enter') ask(inp.value); });
    bg.querySelector('#gpai-sugg').addEventListener('click', function(e){ if (e.target.tagName === 'BUTTON') ask(e.target.textContent); });
    draw();
    setTimeout(function(){ inp.focus(); }, 60);
  };

  // ───────────────────────── Boot ─────────────────────────
  function boot() {
    var h = document.querySelector('.gp-party-search[data-for="sale-customer"]');
    if (h) gpAttachPartySearch('sale-customer', h);
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();
  console.log('[GOURY BILLING UPGRADE] edit/print/search bills, H/C/R rates, new part, party info, multi-photo, notes, AI help ✓');
})();
