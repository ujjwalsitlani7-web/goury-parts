// ============================================================
// CATALOGUE REGISTRATION — Two-stream SKU entry system
// Staff stream: rack · photo · name · stock · location
// Owner/Dad stream: pricing · supplier · min stock
// ============================================================

// ── Completion logic ──────────────────────────────────────
function catStaffDone(p) {
  return !!(p.partNo && p.name && p.stock !== undefined && p.stock !== null);
}
function catPricingDone(p) {
  return !!(p.costPrice > 0 && p.instantPrice > 0);
}
function catFullyDone(p) { return catStaffDone(p) && catPricingDone(p); }
// ── GATE: only fully-done parts are visible in stock / sale / inventory ──
function saleablePart(p) { return catFullyDone(p); }

// ── Progress stats ─────────────────────────────────────────
function catCalcProgress() {
  var total    = state.parts.length;
  var complete = state.parts.filter(catFullyDone).length;
  var needDad  = state.parts.filter(function(p){ return catStaffDone(p) && !catPricingDone(p); }).length;
  var needStaff= state.parts.filter(function(p){ return !catStaffDone(p); }).length;
  var pct      = total > 0 ? Math.round(complete / total * 100) : 0;
  return { total, complete, needDad, needStaff, pct };
}

var _catProgressTimer = null;
function catRenderProgressDebounced() {
  clearTimeout(_catProgressTimer);
  _catProgressTimer = setTimeout(catRenderProgress, 150);
}

function catRenderProgress() {
  var s = catCalcProgress();
  var bar = document.getElementById('cat-prog-bar');
  if (bar) bar.style.width = s.pct + '%';
  var lbl = document.getElementById('cat-prog-label');
  if (lbl) lbl.textContent = s.complete + ' / ' + s.total + ' SKUs complete';
  var pctBadge = document.getElementById('cat-pct-badge');
  if (pctBadge) pctBadge.textContent = s.pct + '%';
  var nc = document.getElementById('cat-n-complete');  if (nc) nc.textContent = s.complete;
  var nd = document.getElementById('cat-n-dad');       if (nd) nd.textContent = s.needDad;
  var ns = document.getElementById('cat-n-staff');     if (ns) ns.textContent = s.needStaff;
  var dc = document.getElementById('cat-mode-dad-count'); if (dc) dc.textContent = s.needDad + ' pending';
  var db = document.getElementById('cat-dad-badge');   if (db) db.textContent = s.needDad + ' items';
  catPopulateDatalists();
  try { catRenderMissingLoc(); } catch(e) { /* catalogue render — non-critical */ }
}

function catPopulateDatalists() {
  var dl = document.getElementById('cat-cat-list');
  if (dl) dl.innerHTML = catCanonicalList('category').map(function(c){ return '<option value="'+c+'">'; }).join('');
  var bdl = document.getElementById('cat-brand-list');
  if (bdl) bdl.innerHTML = catCanonicalList('brand').map(function(b){ return '<option value="'+b+'">'; }).join('');
  // Part names — all existing names as suggestions
  var ndl = document.getElementById('cat-name-list');
  if (ndl) {
    var names = [...new Set(state.parts.map(function(p){ return p.name||''; }).filter(Boolean))].sort();
    ndl.innerHTML = names.map(function(n){ return '<option value="'+n.replace(/"/g,'&quot;')+'">'; }).join('');
  }
  // Supplier names — for pricing queue + edit part autofill
  refreshSupplierDatalist && refreshSupplierDatalist();
}

// ── Supplier autocomplete + autofill ──────────────────────
// Returns {name_lowercase -> {name, phone}} built from all parts that have a supplier
function getSupplierMap() {
  var map = {};
  state.parts.forEach(function(p) {
    if (!p.supplierName) return;
    var key = p.supplierName.trim().toLowerCase();
    if (!map[key]) map[key] = { name: p.supplierName.trim(), phone: p.supplierPhone || '' };
    // If this entry has a phone and stored one doesn't, prefer the one with a phone
    else if (!map[key].phone && p.supplierPhone) map[key].phone = p.supplierPhone.trim();
  });
  // Also pull from state.suppliers table if it exists
  (state.suppliers || []).forEach(function(s) {
    if (!s.name) return;
    var key = s.name.trim().toLowerCase();
    if (!map[key]) map[key] = { name: s.name.trim(), phone: s.phone || s.supplierPhone || '' };
    else if (!map[key].phone && (s.phone || s.supplierPhone)) map[key].phone = (s.phone || s.supplierPhone || '').trim();
  });
  return map;
}

// Called oninput on any supplier name field.
// nameInputId  — id of the text input being typed in
// phoneInputId — id of the paired phone input to autofill
function supAutofill(nameInputId, phoneInputId) {
  var nameEl  = document.getElementById(nameInputId);
  var phoneEl = document.getElementById(phoneInputId);
  if (!nameEl || !phoneEl) return;
  var typed = nameEl.value.trim().toLowerCase();
  if (!typed) return;
  var map = getSupplierMap();
  // Exact match on the typed value (datalist selection fires oninput with full value)
  var hit = map[typed];
  if (hit && hit.phone && !phoneEl.value.trim()) {
    phoneEl.value = hit.phone;
    // Flash the phone field green briefly so user notices it was autofilled
    phoneEl.style.transition = 'background 0.3s';
    phoneEl.style.background = '#dcfce7';
    setTimeout(function(){ phoneEl.style.background = ''; }, 1200);
  }
}

// Populate the global supplier datalist (called from catPopulateDatalists + openPartDetail)
function refreshSupplierDatalist() {
  var dl = document.getElementById('global-supplier-list');
  if (!dl) return;
  var map = getSupplierMap();
  dl.innerHTML = Object.values(map).sort(function(a,b){ return a.name.localeCompare(b.name); })
    .map(function(s){ return '<option value="'+s.name.replace(/"/g,'&quot;')+'">'; }).join('');
}

// ── Role-aware render entry point ──────────────────────────
var _catMode = 'staff';
var _catCurrentRack = '';
var _catSessionCount = 0;
var _catPendingPhoto = '';

// ── Fuzzy match helpers ────────────────────────────────────
// Simple edit-distance (Levenshtein) for short strings
function catEditDist(a, b) {
  a = (a||'').toLowerCase().trim();
  b = (b||'').toLowerCase().trim();
  if (a === b) return 0;
  var m = a.length, n = b.length;
  var prev = [], curr = [];
  for (var j = 0; j <= n; j++) prev[j] = j;
  for (var i = 1; i <= m; i++) {
    curr[0] = i;
    for (var j = 1; j <= n; j++) {
      curr[j] = a[i-1] === b[j-1] ? prev[j-1]
        : 1 + Math.min(prev[j], curr[j-1], prev[j-1]);
    }
    prev = curr.slice();
  }
  return prev[n];
}

function catNorm(s) { return (s||'').toLowerCase().replace(/\s+/g,' ').trim(); }

// Memoised canonical list — O(n²) fuzzy dedup only runs when parts array grows/shrinks
var _catCanonicalCache = {};
var _catCanonicalVersion = -1;
function catCanonicalList(field) {
  if (_catCanonicalVersion === state.parts.length && _catCanonicalCache[field]) {
    return _catCanonicalCache[field];
  }
  var counts = {};
  state.parts.forEach(function(p) {
    var v = (p[field]||'').trim();
    if (!v) return;
    counts[v] = (counts[v]||0) + 1;
  });
  var vals = Object.keys(counts).sort(function(a,b){ return counts[b]-counts[a]; });
  var canonical = [];
  vals.forEach(function(v) {
    var already = canonical.some(function(c){ return catEditDist(c,v) <= 2; });
    if (!already) canonical.push(v);
  });
  var result = canonical.sort();
  _catCanonicalCache[field] = result;
  _catCanonicalVersion = state.parts.length;
  return result;
}

function catFindCanonical(field, typed) {
  if (!typed || !typed.trim()) return null;
  var canonical = catCanonicalList(field);
  var best = null, bestDist = 99;
  canonical.forEach(function(c) {
    var d = catEditDist(c, typed);
    if (d > 0 && d <= 2 && d < bestDist) { bestDist = d; best = c; }
  });
  return best;
}

var CAT_MACHINE_WORDS = {
  tractor:   ['tractor'],
  harvester: ['harvester','combine'],
  jcb:       ['jcb','excavator','backhoe'],
  rotavator: ['rotavator','rotovator','tiller']
};

function catCheckCategoryMachineWord(category) {
  var norm = catNorm(category);
  for (var mtype in CAT_MACHINE_WORDS) {
    var words = CAT_MACHINE_WORDS[mtype];
    for (var i=0; i<words.length; i++) {
      if (norm.indexOf(words[i]) !== -1) return mtype;
    }
  }
  return null;
}

function catShowFieldWarn(fieldId, msg) {
  var warnId = fieldId + '-warn';
  var existing = document.getElementById(warnId);
  if (!msg) { if (existing) existing.style.display = 'none'; return; }
  var inp = document.getElementById(fieldId);
  if (!inp) return;
  if (!existing) {
    existing = document.createElement('div');
    existing.id = warnId;
    existing.style.cssText = 'font-size:11px;color:#92400e;background:#fef3c7;border:1px solid #fde68a;border-radius:6px;padding:6px 10px;margin-top:5px;display:flex;align-items:center;justify-content:space-between;gap:8px;line-height:1.4;';
    inp.insertAdjacentElement('afterend', existing);
  }
  existing.style.display = 'flex';
  existing.innerHTML = msg;
}

function catApplySuggestion(fieldId, value) {
  var el = document.getElementById(fieldId);
  if (el) el.value = value;
  catShowFieldWarn(fieldId, '');
}

function catApplyMachineFix(mtype, fieldId) {
  var mEl = document.getElementById('cat-machine');
  if (mEl) mEl.value = mtype;
  var fEl = document.getElementById(fieldId);
  if (fEl) fEl.value = '';
  catShowFieldWarn(fieldId, '');
}

function catLiveValidateName() {
  var inp = document.getElementById('cat-name');
  if (!inp) return;
  var typed = inp.value.trim();
  // Clear warn if too short
  var warnDiv = document.getElementById('cat-name-warn');
  if (typed.length < 4) { if (warnDiv) warnDiv.style.display = 'none'; return; }

  // Find closest existing part name by edit distance
  // Guard: skip for short names — CH4/CI4/CF4 are all 1 char apart
  var best = null, bestDist = 99;
  var typedNorm = catNorm(typed);
  if (typed.length >= 8) {
    state.parts.forEach(function(p) {
      if (!p.name) return;
      var pNorm = catNorm(p.name);
      if (pNorm === typedNorm) return; // exact match — not a typo
      var d = catEditDist(pNorm, typedNorm);
      var maxLen = Math.max(p.name.length, typed.length);
      if (d > 0 && d <= 3 && (d / maxLen) < 0.4 && d < bestDist) {
        bestDist = d; best = p;
      }
    });
  }

  if (!warnDiv) {
    warnDiv = document.createElement('div');
    warnDiv.id = 'cat-name-warn';
    warnDiv.style.cssText = 'font-size:11px;color:#92400e;background:#fef3c7;border:1px solid #fde68a;border-radius:6px;padding:6px 10px;margin-top:5px;display:flex;align-items:center;justify-content:space-between;gap:8px;line-height:1.4;';
    // Insert after the input, inside the field div
    inp.insertAdjacentElement('afterend', warnDiv);
  }

  if (best) {
    warnDiv.style.display = 'flex';
    warnDiv.innerHTML = '';
    var msgSpan = document.createElement('span');
    msgSpan.innerHTML = '⚠️ Similar: <b>' + best.name + '</b> (' + best.partNo + ')';
    warnDiv.appendChild(msgSpan);
    var useBtn = document.createElement('button');
    useBtn.textContent = 'Use this';
    useBtn.style.cssText = 'background:#0f766e;color:white;border:none;border-radius:5px;padding:3px 10px;font-size:11px;cursor:pointer;white-space:nowrap;flex-shrink:0;';
    useBtn.onclick = function() {
      inp.value = best.name;
      var pnEl = document.getElementById('cat-partno');
      if (pnEl && !pnEl.value.trim()) pnEl.value = best.partNo;
      warnDiv.style.display = 'none';
    };
    warnDiv.appendChild(useBtn);
    var keepBtn = document.createElement('button');
    keepBtn.textContent = 'Keep mine';
    keepBtn.style.cssText = 'background:transparent;color:#92400e;border:1px solid #fde68a;border-radius:5px;padding:3px 8px;font-size:11px;cursor:pointer;white-space:nowrap;flex-shrink:0;';
    keepBtn.onclick = function() { warnDiv.style.display = 'none'; };
    warnDiv.appendChild(keepBtn);
  } else {
    warnDiv.style.display = 'none';
  }
}

function catLiveValidate(fieldId, field) {
  var inp = document.getElementById(fieldId);
  if (!inp) return;
  var typed = inp.value.trim();
  if (!typed) { catShowFieldWarn(fieldId, ''); return; }

  // Check if category contains a machine-type word
  if (field === 'category') {
    var mword = catCheckCategoryMachineWord(typed);
    if (mword) {
      var warnDiv = document.getElementById(fieldId + '-warn');
      if (!warnDiv) {
        warnDiv = document.createElement('div');
        warnDiv.id = fieldId + '-warn';
        warnDiv.style.cssText = 'font-size:11px;color:#92400e;background:#fef3c7;border:1px solid #fde68a;border-radius:6px;padding:6px 10px;margin-top:5px;display:flex;align-items:center;justify-content:space-between;gap:8px;line-height:1.4;';
        inp.insertAdjacentElement('afterend', warnDiv);
      }
      warnDiv.style.display = 'flex';
      warnDiv.innerHTML = '';
      var msgSpan = document.createElement('span');
      msgSpan.textContent = '⚠️ "' + typed + '" is a machine type — use the dropdown above.';
      warnDiv.appendChild(msgSpan);
      var fixBtn = document.createElement('button');
      fixBtn.textContent = 'Fix it';
      fixBtn.style.cssText = 'background:#0f766e;color:white;border:none;border-radius:5px;padding:3px 10px;font-size:11px;cursor:pointer;white-space:nowrap;flex-shrink:0;';
      var mwordCopy = mword, fidCopy = fieldId;
      fixBtn.onclick = function() { catApplyMachineFix(mwordCopy, fidCopy); };
      warnDiv.appendChild(fixBtn);
      return;
    }
  }

  // Check for near-duplicate of existing canonical value
  var canon = catFindCanonical(field, typed);
  if (canon) {
    var warnDiv2 = document.getElementById(fieldId + '-warn');
    if (!warnDiv2) {
      warnDiv2 = document.createElement('div');
      warnDiv2.id = fieldId + '-warn';
      warnDiv2.style.cssText = 'font-size:11px;color:#92400e;background:#fef3c7;border:1px solid #fde68a;border-radius:6px;padding:6px 10px;margin-top:5px;display:flex;align-items:center;justify-content:space-between;gap:8px;line-height:1.4;';
      inp.insertAdjacentElement('afterend', warnDiv2);
    }
    warnDiv2.style.display = 'flex';
    warnDiv2.innerHTML = '';
    var msgSpan2 = document.createElement('span');
    msgSpan2.innerHTML = '⚠️ Did you mean <b>' + canon + '</b>?';
    warnDiv2.appendChild(msgSpan2);
    var useBtn = document.createElement('button');
    useBtn.textContent = 'Use this';
    useBtn.style.cssText = 'background:#0f766e;color:white;border:none;border-radius:5px;padding:3px 10px;font-size:11px;cursor:pointer;white-space:nowrap;flex-shrink:0;';
    var canonCopy = canon, fidCopy2 = fieldId;
    useBtn.onclick = function() { catApplySuggestion(fidCopy2, canonCopy); };
    warnDiv2.appendChild(useBtn);
    return;
  }

  catShowFieldWarn(fieldId, '');
}

function renderCatalogue() {
  var role = (state.session && state.session.role) || '';
  var ownerMode = (CAT_ROLE_MODE[role] === 'both'); // admin/manager only

  // Mode grid: completely hidden for staff/delivery
  var modeGrid = document.getElementById('cat-mode-grid');
  if (modeGrid) modeGrid.style.display = ownerMode ? '' : 'none';

  // Role gate: never shown (staff don't need to see a warning for something invisible)
  var gate = document.getElementById('cat-role-gate');
  if (gate) gate.style.display = 'none';

  if (ownerMode) {
    catSwitchMode(_catMode || 'staff');
  } else {
    // Staff/delivery: force staff panel, hide dad panel entirely
    document.getElementById('cat-staff-panel').style.display = '';
    document.getElementById('cat-dad-panel').style.display   = 'none';
  }

  catRenderProgress();
  catPopulateDatalists();
}

// ── Mode switch (owner only — staff never reach this) ──────
function catSwitchMode(mode) {
  var role = (state.session && state.session.role) || '';
  var ownerMode = (CAT_ROLE_MODE[role] === 'both');

  // Hard block: non-owners cannot switch to dad mode
  if (mode === 'dad' && !ownerMode) return;

  _catMode = mode;
  document.getElementById('cat-staff-panel').style.display = mode === 'staff' ? '' : 'none';
  document.getElementById('cat-dad-panel').style.display   = mode === 'dad'   ? '' : 'none';
  var sb = document.getElementById('cat-mode-staff-btn');
  var db = document.getElementById('cat-mode-dad-btn');
  if (sb) sb.className = 'cat-mode-btn' + (mode === 'staff' ? ' active' : '');
  if (db) db.className = 'cat-mode-btn' + (mode === 'dad'   ? ' active' : '');
  if (mode === 'dad') catRenderDadQueue();
}

// ── Rack selector ──────────────────────────────────────────
function catSelectRack() {
  var r = prompt('Enter rack number (e.g. A-7):');
  if (!r) return;
  r = r.toUpperCase().trim();
  _catCurrentRack = r;
  var badge = document.getElementById('cat-rack-badge');
  var lbl   = document.getElementById('cat-rack-lbl');
  if (badge) badge.textContent = r;
  if (lbl)   lbl.textContent   = 'Rack ' + r + ' — all entries auto-tagged';
  var locEl = document.getElementById('cat-loc');
  if (locEl) { locEl.value = r; }
  var hint = document.getElementById('cat-loc-hint');
  if (hint) hint.textContent = '← auto-filled';
}

// ── Unit pill ──────────────────────────────────────────────
function catSelectUnit(key, el) {
  document.querySelectorAll('#cat-unit-pills .uap-unit-pill').forEach(function(p){ p.classList.remove('sel'); });
  if (el) el.classList.add('sel');
  var hidden = document.getElementById('cat-unit');
  if (hidden) hidden.value = key;
}

// ── Photo handler (same canvas-resize pipeline as uapHandlePhoto) ──
function catHandlePhoto(input) {
  var file = input.files && input.files[0];
  if (!file) return;
  var reader = new FileReader();
  reader.onload = function(e) {
    var img = new Image();
    img.onload = function() {
      // Keep enough resolution to read small-print part numbers on boxes
      var MAX = 1600;
      var scale = img.width > MAX ? MAX / img.width : 1;
      var c = document.createElement('canvas');
      c.width  = Math.round(img.width  * scale);
      c.height = Math.round(img.height * scale);
      c.getContext('2d').drawImage(img, 0, 0, c.width, c.height);
      _catPendingPhoto = c.toDataURL('image/jpeg', 0.92);
      var dataEl = document.getElementById('cat-photo-data');
      if (dataEl) dataEl.value = _catPendingPhoto;
      // Render preview with scanning state
      var inner = document.getElementById('cat-photo-inner');
      var area  = document.getElementById('cat-photo-area');
      if (area) area.style.padding = '10px';
      if (inner) inner.innerHTML =
        '<div class="cat-photo-preview">'
        + '<img class="cat-photo-thumb" src="' + _catPendingPhoto + '">'
        + '<div style="flex:1;">'
        + '<div style="font-size:13px;font-weight:600;color:var(--primary);" id="cat-ai-scan-status">🔍 Reading part details…</div>'
        + '<div style="font-size:11px;color:var(--muted);margin-top:3px;" id="cat-ai-scan-sub">Reading: part no · name · brand · MRP · category · machine · models</div>'
        + '<div style="display:flex;gap:6px;margin-top:8px;">'
        + '<label class="btn btn-sm" style="font-size:12px;cursor:pointer;">Retake<input type="file" accept="image/*" capture="environment" style="display:none;" onchange="catHandlePhoto(this)"></label>'
        + '<button class="btn btn-outline btn-sm" style="font-size:12px;" onclick="catClearPhoto()">✕ Remove</button>'
        + '</div></div></div>';
      // Trigger AI extraction if key is available
      catAiExtractFromPhoto(_catPendingPhoto);
    };
    img.src = e.target.result;
  };
  reader.readAsDataURL(file);
}

async function catAiExtractFromPhoto(dataUrl) {
  var statusEl = document.getElementById('cat-ai-scan-status');
  var subEl    = document.getElementById('cat-ai-scan-sub');

  var key = getAIKey();
  if (!key || !key.startsWith('sk-')) {
    if (statusEl) { statusEl.textContent = '✓ Photo ready'; statusEl.style.color = 'var(--success)'; }
    if (subEl)    subEl.textContent = 'Add AI key in Settings to auto-fill fields';
    return;
  }

  // callClaude — defined locally so retry can also use it
  async function callClaude(promptText, maxTokens) {
    return aiCall({ prompt: promptText, image: { dataUrl: dataUrl }, maxTokens: maxTokens || 600, vision: true, stripFences: true });
  }

  function parseJSON(raw) {
    try {
      var s = raw.indexOf('{'), e = raw.lastIndexOf('}');
      return s >= 0 && e > s ? JSON.parse(raw.slice(s, e+1)) : null;
    } catch(_) { return null; }
  }

  function isEmpty(ex) {
    return !ex || (!ex.partNo && !ex.name && !ex.brand && !ex.mrp);
  }

  try {
    var knownBrands = [...new Set((state.parts||[]).map(function(p){return p.brand;}).filter(Boolean))].slice(0,30).join(', ');
    var knownCats   = [...new Set((state.parts||[]).map(function(p){return p.category||'';}).filter(Boolean))].slice(0,20).join(', ');

    // ── PASS 1: broad extraction ──────────────────────────────────────────
    if (statusEl) { statusEl.textContent = '🔍 Reading part box…'; statusEl.style.color = 'var(--primary)'; }

    var pass1prompt =
      'You are an expert in Indian agricultural and construction machinery spare parts.\n\n' +
      'This photo shows a spare part or its packaging box.\n' +
      'Known brands in this shop: ' + (knownBrands || 'SKF, FAG, NTN, Mahindra, John Deere, Bosch, Gates, Sonalika') + '\n' +
      'Known categories: ' + (knownCats || 'Bearing, Filter, Belt, Clutch, Hydraulic, Seal, Gear, Chain, Blade') + '\n\n' +
      'Read ALL visible text carefully — including small print, stickers, embossed markings.\n' +
      'Return ONLY this JSON (null for anything not visible — do NOT invent):\n' +
      '{"partNo":"exact code e.g. 6205ZZ","name":"full name","brand":"brand as printed","mrp":0,"category":"category","machineType":"tractor|harvester|jcb|rotavator|other","models":"compatible models","unit":"pcs|set|pair|kg|ltr|mtr"}\n' +
      'Return ONLY the JSON.';

    var raw1 = await callClaude(pass1prompt, 600);
    var extracted = parseJSON(raw1);

    // ── PASS 2: auto-retry if pass 1 got nothing ──────────────────────────
    if (isEmpty(extracted)) {
      if (statusEl) statusEl.textContent = '🔍 Looking closer…';

      var pass2prompt =
        'Examine this image very carefully. Look for:\n' +
        '• Any alphanumeric codes stamped or engraved on the metal\n' +
        '• Small stickers or labels anywhere on the part/box\n' +
        '• Embossed numbers on surfaces\n' +
        '• Text on ALL sides visible in the photo\n' +
        '• Part numbers often look like: 6205, AL110864, 332/D2275, MS571, BRG-6205\n\n' +
        'Known brands: ' + (knownBrands || 'SKF, FAG, NTN, Mahindra, John Deere, Bosch, Gates') + '\n\n' +
        'Return ONLY JSON: {"partNo":"...","name":"...","brand":"...","mrp":null,"category":"...","machineType":"...","models":"..."}\n' +
        'null for anything truly not visible.';

      var raw2 = await callClaude(pass2prompt, 500);
      var ex2  = parseJSON(raw2);
      if (!isEmpty(ex2)) extracted = ex2;
    }

    // ── PASS 3: visual inference — even if no text, fill what we can see ──
    if (!extracted || isEmpty(extracted)) {
      if (statusEl) statusEl.textContent = '🔍 Inferring from shape…';
      try {
        var p3 = 'You cannot read text on this part — that is OK. Instead, describe what you SEE visually.\n' +
          'Known categories: ' + (knownCats || 'Bearing, Filter, Belt, Clutch, Hydraulic, Seal, Gear, Chain, Blade') + '\n\n' +
          'Return ONLY JSON — fill what you can confidently infer from the visual shape/colour/material:\n' +
          '{"partNo":null,"name":"descriptive name from visual e.g. Ball Bearing, Oil Filter, V-Belt","brand":null,"mrp":null,' +
          '"category":"best match from known categories","machineType":"tractor|harvester|jcb|rotavator|other","models":null,"unit":"pcs|set|pair|kg|ltr|mtr"}\n' +
          'Only fill name, category, machineType, unit — leave others null.';
        var raw3 = await callClaude(p3, 300);
        var ex3 = parseJSON(raw3);
        if (ex3 && (ex3.name || ex3.category)) {
          extracted = ex3;
        }
      } catch(e3) { /* pass 3 failed silently */ }
    }

    if (!extracted || (!extracted.partNo && !extracted.name && !extracted.brand && !extracted.category)) {
      if (statusEl) { statusEl.textContent = '📷 Photo saved'; statusEl.style.color = 'var(--muted)'; }
      if (subEl)    subEl.textContent = 'Could not read — retake the part label closer';
      return;
    }

    // ── Fill fields ──────────────────────────────────────────────────────
    var filled = [];
    function _fill(id, val) {
      if (!val) return false;
      var el = document.getElementById(id);
      if (!el) return false;
      var empty = (el.tagName === 'SELECT') ? !el.value : !el.value.trim();
      if (!empty) return false;
      el.value = val;
      el.style.background = '#f0fdf4';
      return true;
    }

    if (extracted.partNo   && _fill('cat-partno',   String(extracted.partNo).toUpperCase())) { catCheckPartNoDuplicate&&catCheckPartNoDuplicate(); filled.push('Part No'); }
    if (extracted.name     && _fill('cat-name',     extracted.name))     filled.push('Name');
    if (extracted.brand    && _fill('cat-brand',    extracted.brand))    filled.push('Brand');
    if (extracted.mrp > 0  && _fill('cat-mrp',      extracted.mrp))      filled.push('MRP');
    if (extracted.category && _fill('cat-category', extracted.category)) filled.push('Category');
    if (extracted.models   && _fill('cat-models',   extracted.models))   filled.push('Models');

    if (extracted.machineType) {
      var mtEl = document.getElementById('cat-machine');
      if (mtEl && !mtEl.value) {
        var mt = String(extracted.machineType).toLowerCase();
        var match = Array.from(mtEl.options).map(function(o){return o.value;}).find(function(v){return v && mt.includes(v);});
        if (match) { mtEl.value = match; mtEl.style.background='#f0fdf4'; filled.push('Machine'); }
      }
    }
    if (extracted.unit) {
      var u = {pcs:'pcs',set:'set',pair:'pair',kg:'kg',ltr:'ltr',mtr:'mtr',box:'box'}[String(extracted.unit).toLowerCase()];
      if (u) { catSelectUnit && catSelectUnit(u, null); filled.push('Unit'); }
    }

    setTimeout(function() {
      ['cat-partno','cat-brand','cat-name','cat-mrp','cat-category','cat-models','cat-machine'].forEach(function(id){
        var el = document.getElementById(id); if (el) el.style.background = '';
      });
    }, 4000);

    if (statusEl) {
      statusEl.textContent = filled.length ? '✓ Filled: ' + filled.join(', ') : '✓ Photo saved';
      statusEl.style.color = filled.length ? 'var(--success)' : 'var(--muted)';
    }
    if (subEl) subEl.textContent = filled.length ? 'Check and complete remaining fields' : 'No text found — fill manually';

  } catch(err) {
    console.warn('[catAiExtract]', err.message);
    if (statusEl) { statusEl.textContent = '✓ Photo saved'; statusEl.style.color = 'var(--muted)'; }
    if (subEl)    subEl.textContent = 'Scan failed: ' + (err.message||'network error');
  }
}


// catCheckPartNoDuplicate — checks for duplicate part numbers in real time
// Shows a warning next to the field if the part no already exists
function catCheckPartNoDuplicate() {
  var el  = document.getElementById('cat-partno');
  var val = el && el.value && el.value.trim().toUpperCase();
  if (!val || val.length < 3) return;
  var hint = document.getElementById('cat-partno-dup-hint');
  var existing = (state.parts || []).find(function(p) {
    return p.partNo && p.partNo.toUpperCase() === val;
  });
  if (!hint) {
    // Create hint element next to the field
    hint = document.createElement('div');
    hint.id = 'cat-partno-dup-hint';
    hint.style.cssText = 'font-size:11px;font-weight:600;margin-top:3px;';
    el && el.parentNode && el.parentNode.appendChild(hint);
  }
  if (existing) {
    hint.textContent = '⚠️ Already in inventory: ' + existing.name;
    hint.style.color = '#d97706';
  } else {
    hint.textContent = '';
  }
}

function catClearPhoto() {
  _catPendingPhoto = '';
  var dataEl = document.getElementById('cat-photo-data');
  if (dataEl) dataEl.value = '';
  var area  = document.getElementById('cat-photo-area');
  var inner = document.getElementById('cat-photo-inner');
  if (area) area.style.padding = '18px 14px';
  if (inner) inner.innerHTML =
    '<div style="font-size:30px;">📷</div>'
    + '<div style="font-size:12px;color:var(--muted);margin:6px 0 10px;line-height:1.5;">Photo helps the WhatsApp bot identify this part<br>from customer pictures</div>'
    + '<div class="cat-photo-btns">'
    + '<label class="btn btn-sm" style="cursor:pointer;display:flex;align-items:center;justify-content:center;gap:5px;">📸 Camera<input type="file" accept="image/*" capture="environment" style="display:none;" onchange="catHandlePhoto(this)"></label>'
    + '<label class="btn btn-outline btn-sm" style="cursor:pointer;display:flex;align-items:center;justify-content:center;gap:5px;">🖼 Gallery<input type="file" accept="image/*" style="display:none;" onchange="catHandlePhoto(this)"></label>'
    + '</div>';
}

// ── Staff: save draft ──────────────────────────────────────
function catClearRackEntryForm() {
  // Clear all entry fields but keep the current rack selection intact
  ['cat-partno','cat-brand','cat-name','cat-category','cat-models','cat-stock','cat-minstock','cat-mrp','cat-rate-h','cat-rate-c','cat-rate-r','cat-sup-name','cat-sup-phone'].forEach(function(id) {
    var el = document.getElementById(id); if (el) el.value = '';
  });
  var locEl = document.getElementById('cat-loc');
  if (locEl) locEl.value = _catCurrentRack;
  catSelectUnit('pcs', document.querySelector('#cat-unit-pills .uap-unit-pill'));
  catClearPhoto();
  var firstField = document.getElementById('cat-partno');
  if (firstField) firstField.focus();
}

// ── Auto-register supplier when a new name+phone is entered anywhere ──
// Called from catSaveDraft (staff form) and catDadSave (pricing queue).
// If the name already exists in state.suppliers: fills in the phone if missing.
// If it doesn't exist: creates a new supplier record so it shows in the Suppliers tab.
function ensureSupplierRegistered(name, phone) {
  name  = (name  || '').trim();
  phone = (phone || '').trim();
  if (!name) return;
  if (!state.suppliers) state.suppliers = [];
  var existing = state.suppliers.find(function(s){
    return s.name.trim().toLowerCase() === name.toLowerCase();
  });
  if (existing) {
    // Fill phone if it was missing
    if (!existing.phone && phone) { existing.phone = phone; save(); }
    return;
  }
  // New supplier — register quietly
  state.suppliers.push({
    id: uid(), name: name, phone: phone,
    city: '', gstin: '', notes: '', totalDue: 0
  });
  // Don't call full save() here — callers already call save() right after
  refreshSupplierDatalist && refreshSupplierDatalist();
  // Keep supplier filter dropdowns in Ledger / Advances in sync
  try { populateSupplierFilter && populateSupplierFilter(); } catch(e) { /* filter render — non-critical */ }
}

function catSaveDraft(andNext) {
  var partNo = (document.getElementById('cat-partno').value || '').trim().toUpperCase();
  var name   = (document.getElementById('cat-name').value   || '').trim();
  if (!partNo || !name) { toast('⚠️ Part No and Name are required'); return; }

  var existing = state.parts.find(function(p){ return p.partNo === partNo; });
  // If this exact part already has pricing (owner filled it), warn staff but don't block flow
  if (existing && catPricingDone(existing)) {
    if (andNext) {
      // In rapid-entry mode: skip silently with a brief toast and move to next part
      toast('⚠️ ' + partNo + ' already priced — skipped. Enter next part.');
      catClearRackEntryForm();
      return;
    }
    if (!confirm('Part ' + partNo + ' already exists and has pricing. Update stock/location only?')) return;
  }
  var loc = (document.getElementById('cat-loc').value || '').trim().toUpperCase() || _catCurrentRack;

  var photo = (document.getElementById('cat-photo-data').value || '') || _catPendingPhoto;

  var part = {
    id:           existing ? existing.id : uid(),
    partNo:       partNo,
    name:         name,
    brand:        (function(){ var v=(document.getElementById('cat-brand').value||'').trim(); return catFindCanonical('brand',v)||v; })(),
    machineType:  document.getElementById('cat-machine').value,
    category:     (function(){ var v=(document.getElementById('cat-category').value||'').trim(); return catFindCanonical('category',v)||v; })(),
    models:       (document.getElementById('cat-models').value   || '').trim(),
    unit:         document.getElementById('cat-unit').value || 'pcs',
    stock:        +(document.getElementById('cat-stock').value    || 0),
    minStock:     +(document.getElementById('cat-minstock').value || 0),
    location:     loc,
    // MRP entered by staff (visible on box) — cost/retail/wholesale set by owner
    mrp:          +(document.getElementById('cat-mrp').value || 0) || (existing ? (existing.mrp || 0) : 0),
    // H/C/R typed on the rack form win; otherwise preserve existing pricing filled by owner
    costPrice:    (+((document.getElementById('cat-rate-c')||{}).value) || 0) || (existing ? (existing.costPrice    || 0) : 0),
    instantPrice: (+((document.getElementById('cat-rate-r')||{}).value) || 0) || (existing ? (existing.instantPrice || 0) : 0),
    creditPrice:  (+((document.getElementById('cat-rate-h')||{}).value) || 0) || (existing ? (existing.creditPrice  || 0) : 0),
    // Supplier: use form value if filled, else keep existing; never blank out an existing value
    supplierName: (function(){ var v=(document.getElementById('cat-sup-name')||{}).value||''; return v.trim() || (existing ? (existing.supplierName||'') : ''); })(),
    supplierPhone:(function(){ var v=(document.getElementById('cat-sup-phone')||{}).value||''; return v.trim() || (existing ? (existing.supplierPhone||'') : ''); })(),
    photoDataUrl: photo || (existing ? (existing.photoDataUrl || '') : '')
  };

  // ── STEP 1: Write to state ──
  if (existing) {
    var idx = state.parts.findIndex(x => x.id === existing.id);
    state.parts[idx] = part;
  } else {
    part.addedAt = Date.now();
    state.parts.push(part);
  }

  // ── STEP 2: Verify landed in state ──
  // Verify against the RAW array, not the cached index — the cache was built
  // before this write and a stale cache here previously caused a false
  // "state write failed" block error that aborted save() and lost the part.
  invalidatePartsIndex();
  var inState = state.parts.find(function(p){ return p.id === part.id; });
  if (!inState) {
    catSaveBlockError('❌ Part did not save — state write failed. Do NOT proceed. Tell owner.');
    return;
  }

  // ── STEP 3: Write to localStorage ──
  // Register supplier in state.suppliers if it's a new name
  ensureSupplierRegistered(part.supplierName, part.supplierPhone);
  markChanged('parts'); markChanged('suppliers');
  save();

  // ── STEP 3.5: Write THIS part to the per-document cloud collection ──
  // CRITICAL for multi-device: when the per-doc store is active, firebasePush()
  // intentionally skips the 'parts' blob (parts/{id} is canonical). save() above
  // therefore does NOT push this part to the cloud — only cloudPersistPart() writes
  // parts/{id}, which is the collection other devices listen to in real time.
  // savePart() already does this; the Catalogue path previously did not, so
  // catalogue-added parts saved locally but never appeared on other devices.
  // ── TWO-STREAM SYNC FIX ──────────────────────────────────────────────
  // The staff stream must NEVER write pricing to the cloud. Previously this
  // pushed the WHOLE part (with costPrice/instantPrice copied from a possibly
  // STALE local copy). If the owner had priced the part on another device and
  // this device hadn't received that echo yet, the stale 0-price overwrote the
  // owner's price in the cloud (merge:true still overwrites fields present in
  // the payload) — which is why catalogue pricing "came back unsaved".
  // Fix: for an EXISTING part, push only the staff-owned fields via a field-
  // level merge (updateDoc), leaving costPrice/instantPrice/creditPrice in the
  // cloud untouched. A brand-new part has no price yet, so a full create is safe.
  var _cloudPartPromise = null;
  if (existing && typeof cloudPersistPartFields === 'function') {
    var _staffPatch = {
      partNo: part.partNo, name: part.name, brand: part.brand,
      machineType: part.machineType, category: part.category, models: part.models,
      unit: part.unit, stock: part.stock, minStock: part.minStock,
      location: part.location, mrp: part.mrp,
      supplierName: part.supplierName, supplierPhone: part.supplierPhone
    };
    // Rates only travel when staff actually typed them on the rack form (never a stale 0)
    if (+((document.getElementById('cat-rate-c')||{}).value) > 0) _staffPatch.costPrice    = part.costPrice;
    if (+((document.getElementById('cat-rate-r')||{}).value) > 0) _staffPatch.instantPrice = part.instantPrice;
    if (+((document.getElementById('cat-rate-h')||{}).value) > 0) _staffPatch.creditPrice  = part.creditPrice;
    // Only sync the photo if it's already an uploaded URL — never push base64
    // (that bloats the doc past Firestore's 1 MB limit). Base64 photos are
    // handled by the background upload in STEP 5 below.
    if (part.photoDataUrl && !String(part.photoDataUrl).startsWith('data:')) {
      _staffPatch.photoDataUrl = part.photoDataUrl;
    }
    _cloudPartPromise = cloudPersistPartFields(part.id, _staffPatch);
  } else {
    _cloudPartPromise = (typeof cloudPersistPart === 'function') ? cloudPersistPart(part) : null;
  }

  // ── STEP 4: Verify persistence path ──
  // 'parts' uses a DEFERRED localStorage write (coalesced 1.2s after save()), so
  // an immediate DB.get here races the flush and previously fired a FALSE
  // "Device storage full" error. The part is on a guaranteed persistence path if
  // it's EITHER in the pending deferred-write queue (flushed on timer AND on tab
  // hide/close) OR already in localStorage. Real quota failures are now surfaced
  // by DB.set itself, not guessed at here.
  var _pendingParts = (typeof _dwPending !== 'undefined' && _dwPending['parts']) || null;
  var inPending = _pendingParts ? _pendingParts.find(function(p){ return p.id === part.id; }) : null;
  var inLocal = null;
  if (!inPending) {
    var localParts = DB.get('parts', []);
    inLocal = localParts.find(function(p){ return p.id === part.id; });
  }
  if (!inPending && !inLocal) {
    catSaveBlockError('❌ Part missing from storage queue — do NOT enter next part. Tell owner.');
    return;
  }

  // ── STEP 5: Photo upload (DURABLE — enqueue, never fire-and-forget) ──
  // OLD BUG: this used to call firebaseUploadPartPhoto() directly and only
  // console.warn() on failure. On a shop phone the direct upload regularly fails
  // mid-stream (network blip, tab switch, rapid Save&Next), so the JPG was lost
  // and the base64 sat in local state forever — that is exactly why photos went
  // missing in the cloud (100 → 500) AND why saves started hanging (the un-uploaded
  // base64 kept piling up inside the parts blob that save() serialises).
  // FIX: park the base64 in the IndexedDB photo-upload queue (idempotent per id)
  // and let the retry worker push it to Storage with backoff, then write the real
  // https URL back to state + cloud. This obeys the INVARIANT documented above the
  // queue: any path that holds a base64 photo MUST guarantee it reaches Storage.
  if (photo && photo.startsWith('data:') && window.photoUploadEnqueue) {
    try { window.photoUploadEnqueue(part.id, photo); }
    catch(e) { console.warn('[Catalogue] enqueue photo failed:', e && e.message); }
    if (window.drainPhotoUploadQueue) setTimeout(function(){ try { window.drainPhotoUploadQueue(); } catch(e){} }, 0);
    try { if (window.renderPhotoPendingBadge) window.renderPhotoPendingBadge(); } catch(e){}
  }

  _catSessionCount++;
  document.getElementById('cat-session-count').textContent = _catSessionCount;
  catRenderProgressDebounced();
  try { patchInventoryCard(part, !existing); } catch(e) { /* UI rerender — non-critical */ }
  try { renderHomeDebounced(); } catch(e) { /* UI rerender — non-critical */ }
  try { xlsRender && xlsRender(); } catch(e) { /* UI rerender — non-critical */ }
  logActivity && logActivity('Catalogue entry: ' + part.partNo + ' "' + part.name + '"', 'info');

  // ── STEP 6: Cloud sync with button lock ──
  // Reuse the push promise that save() already started — no second firebasePush call.
  var saveBtn = document.querySelector('.uap-save-btn');
  var nextBtn = document.querySelector('.cat-save-next');
  if (saveBtn) { saveBtn.disabled = true; saveBtn.textContent = '🔄 Syncing…'; }
  if (nextBtn) { nextBtn.disabled = true; }
  var syncTimeout = setTimeout(function() {
    if (saveBtn) { saveBtn.disabled = false; saveBtn.textContent = '💾 Save draft — pricing queued for owner'; }
    if (nextBtn) { nextBtn.disabled = false; }
    toast('⚠️ ' + part.name + ' saved on device. Cloud sync slow — check internet.');
    if (andNext) catClearRackEntryForm();
  }, 6000);
  var pushPromise = _cloudPartPromise || window._lastPartsPushPromise || (window.firebasePush ? window.firebasePush(state) : null);
  if (pushPromise) {
    pushPromise.then(function() {
      clearTimeout(syncTimeout);
      if (saveBtn) { saveBtn.disabled = false; saveBtn.textContent = '💾 Save draft — pricing queued for owner'; }
      if (nextBtn) { nextBtn.disabled = false; }
      var msg = '✅ ' + part.name + ' saved & synced';
      if (!catPricingDone(part)) msg += ' — pricing queued';
      toast(msg);
      if (andNext) catClearRackEntryForm();
    }).catch(function() {
      clearTimeout(syncTimeout);
      if (saveBtn) { saveBtn.disabled = false; saveBtn.textContent = '💾 Save draft — pricing queued for owner'; }
      if (nextBtn) { nextBtn.disabled = false; }
      toast('⚠️ ' + part.name + ' saved on device. Cloud sync failed — will retry.');
      if (andNext) catClearRackEntryForm();
    });
  } else {
    clearTimeout(syncTimeout);
    if (saveBtn) { saveBtn.disabled = false; saveBtn.textContent = '💾 Save draft — pricing queued for owner'; }
    if (nextBtn) { nextBtn.disabled = false; }
    var msg2 = '✅ ' + part.name + ' saved on device';
    if (!catPricingDone(part)) msg2 += ' — pricing queued';
    toast(msg2);
    if (andNext) catClearRackEntryForm();
  }
}

function catSaveBlockError(msg) {
  var banner = document.getElementById('cat-save-error-banner');
  if (!banner) {
    banner = document.createElement('div');
    banner.id = 'cat-save-error-banner';
    banner.style.cssText = 'position:sticky;top:60px;z-index:200;background:#dc2626;color:white;padding:14px 16px;font-size:14px;font-weight:700;line-height:1.5;border-radius:10px;margin:8px 0;display:flex;align-items:center;justify-content:space-between;gap:12px;';
    var panel = document.getElementById('cat-staff-panel');
    if (panel) panel.prepend(banner);
  }
  banner.style.display = 'flex';
  var dismissFn = "document.getElementById('cat-save-error-banner').style.display='none';document.querySelector('.uap-save-btn').disabled=false;document.querySelector('.uap-save-btn').textContent='💾 Save draft — pricing queued for owner';var nb=document.querySelector('.cat-save-next');if(nb)nb.disabled=false;";
  banner.innerHTML = '<span>' + msg + '</span><button onclick="' + dismissFn + '" style="background:rgba(255,255,255,0.2);border:none;color:white;padding:4px 10px;border-radius:6px;font-size:12px;cursor:pointer;flex-shrink:0;">Dismiss</button>';
  var saveBtn2 = document.querySelector('.uap-save-btn');
  var nextBtn2 = document.querySelector('.cat-save-next');
  if (saveBtn2) { saveBtn2.disabled = true; saveBtn2.style.opacity = '0.5'; }
  if (nextBtn2) { nextBtn2.disabled = true; nextBtn2.style.opacity = '0.5'; }
}

// ── Staff: missing location recovery ──────────────────────
function catRenderMissingLoc() {
  var panel = document.getElementById('cat-missing-loc-panel');
  var list  = document.getElementById('cat-missing-loc-list');
  var count = document.getElementById('cat-missing-loc-count');
  if (!panel || !list) return;

  var missing = state.parts.filter(function(p) { return p.partNo && p.name && !p.location; });
  count.textContent = missing.length;

  if (missing.length === 0) {
    panel.style.display = 'none';
    return;
  }
  panel.style.display = '';

  list.innerHTML = missing.map(function(p) {
    return '<div style="display:flex;align-items:center;gap:8px;padding:8px 0;border-bottom:1px solid #fde68a;">'
      + '<div style="flex:1;min-width:0;">'
        + '<div style="font-size:13px;font-weight:700;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">' + p.name + '</div>'
        + '<div style="font-size:11px;color:var(--muted);">' + p.partNo + (p.brand ? ' · ' + p.brand : '') + '</div>'
      + '</div>'
      + '<input type="text" id="cml-loc-' + p.id + '" class="input" style="width:90px;text-transform:uppercase;font-size:13px;font-weight:700;" placeholder="A-01" value="' + (p.location||'') + '" onkeydown="if(event.key===\'Enter\')catSaveMissingLoc(\'' + p.id + '\')">'
      + '<button class="btn" style="padding:8px 12px;font-size:12px;white-space:nowrap;" onclick="catSaveMissingLoc(\'' + p.id + '\')">Save</button>'
    + '</div>';
  }).join('');
}

function catSaveMissingLoc(partId) {
  var el  = document.getElementById('cml-loc-' + partId);
  var loc = el ? el.value.trim().toUpperCase() : '';
  if (!loc) { toast('⚠️ Enter a rack location first'); el && el.focus(); return; }
  var idx = state.parts.findIndex(x => x.id === partId);
  if (idx === -1) return;
  state.parts[idx].location = loc;
  markChanged('parts'); // mid-array field edit — fingerprint won't see it; force the durable write
  save();
  catRenderProgress();
  catRenderMissingLoc();
  toast('📍 ' + state.parts[idx].name + ' → ' + loc);
}

// ── Owner/Dad: render pricing queue ───────────────────────
function openBulkRateCard(){
  var h='<div><div style="font-size:12px;color:var(--muted);margin-bottom:10px;">Format: <code>PartNo TAB CostPrice</code> or <code>PartNo TAB CostPrice TAB RetailPrice</code></div><textarea id="bulk-rc-input" style="width:100%;height:160px;font-family:monospace;font-size:12px;padding:8px;border:1px solid var(--border);border-radius:8px;" placeholder="A230\t45\nA231\t48\t65"></textarea><div id="bulk-rc-preview" style="margin-top:8px;font-size:12px;"></div></div>';
  showModal('📋 Bulk Rate Card Fill',h,[{label:'Preview',cls:'btn-outline',fn:'bulkRateCardPreview()'},{label:'Apply All',cls:'btn-success',fn:'bulkRateCardApply()'}]);
}
function bulkRateCardPreview(){
  var raw=(document.getElementById('bulk-rc-input')||{}).value||'';
  var lines=raw.trim().split('\n').filter(function(l){return l.trim();});
  var ok=0,skip=0,rows=[];
  // O(1) lookup map (lower-cased partNo → part), built once — avoids a full find scan per line
  var _rcByNo={};
  state.parts.forEach(function(x){ if(x.partNo) _rcByNo[x.partNo.trim().toLowerCase()]=x; });
  lines.forEach(function(ln){
    var c=ln.split('\t'),pno=(c[0]||'').trim(),cost=parseFloat(c[1])||0,ret=parseFloat(c[2])||0;
    if(!pno||!cost){skip++;return;}
    var p=_rcByNo[pno.toLowerCase()];
    if(!p){skip++;rows.push('<span style="color:var(--danger);">✗ '+pno+'</span>');return;}
    if(!ret)ret=Math.round(cost*1.30);
    ok++;rows.push('<span style="color:var(--success);">✓ '+pno+' cost ₹'+cost+' retail ₹'+ret+'</span>');
  });
  var el=document.getElementById('bulk-rc-preview');
  if(el)el.innerHTML='<b>'+ok+' will update, '+skip+' skipped</b><br>'+rows.join('<br>');
}
function bulkRateCardApply(){
  var raw=(document.getElementById('bulk-rc-input')||{}).value||'';
  var lines=raw.trim().split('\n').filter(function(l){return l.trim();});
  var count=0;
  // O(1) lookup map (lower-cased partNo → part), built once — avoids a full findIndex scan per line
  var _rcByNo={};
  state.parts.forEach(function(x){ if(x.partNo) _rcByNo[x.partNo.trim().toLowerCase()]=x; });
  lines.forEach(function(ln){
    var c=ln.split('\t'),pno=(c[0]||'').trim(),cost=parseFloat(c[1])||0,ret=parseFloat(c[2])||0;
    if(!pno||!cost)return;
    var pt=_rcByNo[pno.toLowerCase()];
    if(!pt)return;
    if(!ret)ret=Math.round(cost*1.30);
    pt.costPrice=cost; pt.instantPrice=ret;
    if(!pt.creditPrice)pt.creditPrice=Math.round(ret*0.95);
    count++;
  });
  if (count) markChanged('parts'); // #1: bulk field edits are invisible to the parts fingerprint
  save();closeModal();catRenderDadQueue();catRenderProgress();
  try{renderInventory&&renderInventory();}catch(e){}
  toast('✅ '+count+' parts updated');logActivity&&logActivity('Bulk rate card: '+count+' parts','info');
}
function catRenderDadQueue() {
  var filter  = (document.getElementById('cat-dad-filter') || {}).value || 'all';
  var pending = state.parts.filter(function(p) { return catStaffDone(p) && !catPricingDone(p); });
  if (filter === 'noPhoto')    pending = pending.filter(function(p){ return !p.photoDataUrl; });
  if (filter === 'noSupplier') pending = pending.filter(function(p){ return !p.supplierName; });

  var list = document.getElementById('cat-dad-list');
  if (!list) return;

  if (!pending.length) {
    list.innerHTML = '<div class="card" style="text-align:center;padding:24px;color:var(--muted);">🎉 All parts have pricing filled in!</div>';
    return;
  }

  list.innerHTML = pending.map(function(p) {
    var _qPhoto = window.displayPhotoUrl ? window.displayPhotoUrl(p.photoDataUrl) : p.photoDataUrl;
    var photoHtml = _qPhoto
      ? '<img src="' + _qPhoto + '" style="width:44px;height:44px;object-fit:cover;border-radius:7px;">'
      : '<span style="font-size:20px;">📦</span>';
    var tags = [
      !p.costPrice    ? '<span class="cat-mtag">Cost price</span>'      : '<span class="cat-mtag ok">✓ Cost</span>',
      !p.instantPrice ? '<span class="cat-mtag">Retail price</span>'    : '<span class="cat-mtag ok">✓ Retail</span>',
      !p.mrp          ? '<span class="cat-mtag">MRP</span>'             : '<span class="cat-mtag ok">✓ MRP</span>',
      !p.supplierName ? '<span class="cat-mtag">Supplier</span>'        : '<span class="cat-mtag ok">✓ ' + p.supplierName + '</span>',
    ].join('');

    return '<div class="cat-qitem" id="cqi-' + p.id + '">'
      + '<div class="cat-qitem-top">'
        + '<div class="cat-qphoto">' + photoHtml + '</div>'
        + '<div class="cat-qinfo">'
          + '<div class="cat-qname">' + p.name + '</div>'
          + '<div class="cat-qmeta">' + p.partNo + ' · ' + (p.brand||'') + ' · 📍 ' + (p.location||'?') + '</div>'
        + '</div>'
      + '</div>'
      + '<div class="cat-missing-tags">' + tags + '</div>'
      + '<div class="cat-price-grid">'
        + '<div class="field"><label>C · Cost ₹</label><div class="cat-price-wrap"><span class="crs">₹</span><input type="number" class="input" id="cdcost-'+p.id+'" value="'+(p.costPrice||'')+'" placeholder="0" oninput="catDadAuto(\''+p.id+'\')"></div></div>'
        + '<div class="field"><label>MRP ₹</label><div class="cat-price-wrap"><span class="crs">₹</span><input type="number" class="input" id="cdmrp-'+p.id+'" value="'+(p.mrp||'')+'" placeholder="0"></div></div>'
        + '<div class="field"><label>R · Retail ₹</label><div class="cat-price-wrap"><span class="crs">₹</span><input type="number" class="input" id="cdretail-'+p.id+'" value="'+(p.instantPrice||'')+'" placeholder="0" oninput="catDadAuto(\''+p.id+'\')"></div></div>'
        + '<div class="field"><label>H · Wholesale ₹</label><div class="cat-price-wrap"><span class="crs">₹</span><input type="number" class="input" id="cdws-'+p.id+'" value="'+(p.creditPrice||'')+'" placeholder="0"></div></div>'
      + '</div>'
      + '<div id="cdecon-'+p.id+'" class="cat-econ-bar">'
        + '<div class="cat-ec"><div class="cv" id="cdm-'+p.id+'">—</div><div class="cl">Margin %</div></div>'
        + '<div class="cat-ec-div"></div>'
        + '<div class="cat-ec"><div class="cv" id="cdp-'+p.id+'">—</div><div class="cl">Profit/unit</div></div>'
        + '<div class="cat-ec-div"></div>'
        + '<div class="cat-ec"><div class="cv" id="cdw-'+p.id+'">—</div><div class="cl">Trade %</div></div>'
      + '</div>'
      + '<div class="row2" style="margin-top:8px;">'
        + '<div class="field"><label>Supplier name</label><input class="input" id="cdsup-'+p.id+'" value="'+(p.supplierName||'')+'" placeholder="e.g. Singh Auto Parts" list="global-supplier-list" oninput="supAutofill(\'cdsup-'+p.id+'\',\'cdphone-'+p.id+'\')" autocomplete="off"></div>'
        + '<div class="field"><label>Supplier phone</label><input class="input" id="cdphone-'+p.id+'" value="'+(p.supplierPhone||'')+'" placeholder="9876543210"></div>'
      + '</div>'
      + '<div class="field"><label>Min stock (reorder)</label><input type="number" class="input" id="cdmin-'+p.id+'" value="'+(p.minStock||'')+'" placeholder="0"></div>'
      + '<div class="cat-qsave-row">'
        + '<button class="btn" onclick="catDadSave(\''+p.id+'\')">✅ Save pricing</button>'
        + '<button class="btn btn-outline" style="flex:0 0 auto;padding:10px 12px;font-size:12px;" onclick="catDadSave(\''+p.id+'\',true)">Save + next →</button>'
        + '<button class="btn" style="flex:0 0 auto;padding:10px 12px;font-size:12px;background:#dc2626;" onclick="catDadDelete(\''+p.id+'\')">🗑</button>'
      + '</div>'
    + '</div>';
  }).join('');
}

function catDadAuto(partId) {
  var cost   = parseFloat((document.getElementById('cdcost-'+partId)  ||{}).value) || 0;
  var retail = parseFloat((document.getElementById('cdretail-'+partId)||{}).value) || 0;
  var ws     = parseFloat((document.getElementById('cdws-'+partId)    ||{}).value) || 0;
  var retEl  = document.getElementById('cdretail-'+partId);
  var wsEl   = document.getElementById('cdws-'+partId);
  if (cost > 0) {
    if (retEl && !parseFloat(retEl.value)) retEl.value = Math.round(cost * 1.30);
    if (wsEl  && !parseFloat(wsEl.value))  wsEl.value  = Math.round(cost * 1.15);
  }
  var retNow = parseFloat((document.getElementById('cdretail-'+partId)||{}).value) || 0;
  var wsNow  = parseFloat((document.getElementById('cdws-'+partId)    ||{}).value) || 0;
  var mEl    = document.getElementById('cdm-'+partId);
  var pEl    = document.getElementById('cdp-'+partId);
  var wEl    = document.getElementById('cdw-'+partId);
  var bar    = document.getElementById('cdecon-'+partId);
  if (cost > 0 && retNow > 0) {
    var mPct = ((retNow - cost) / cost * 100).toFixed(1);
    var mAbs = (retNow - cost).toFixed(0);
    var wPct = wsNow > 0 ? ((wsNow - cost) / cost * 100).toFixed(1) : '—';
    if (mEl) { mEl.textContent = mPct + '%'; mEl.className = 'cv' + (mPct < 5 ? ' danger' : mPct < 15 ? ' warn' : ''); }
    if (pEl) { pEl.textContent = '₹' + mAbs; pEl.className = 'cv' + (mAbs < 0 ? ' danger' : ''); }
    if (wEl) { wEl.textContent = wPct !== '—' ? wPct + '%' : '—'; }
    if (bar) bar.className = 'cat-econ-bar' + (mPct < 0 ? ' danger' : mPct < 10 ? ' warn' : '');
  } else {
    if (mEl) { mEl.textContent = '—'; mEl.className = 'cv'; }
    if (pEl) { pEl.textContent = '—'; pEl.className = 'cv'; }
    if (wEl)   wEl.textContent = '—';
  }
}

function catDadDelete(partId) {
  var p = getPartById(partId);
  if (!p) return;
  if (!confirm('Delete "' + p.name + '" from inventory? This cannot be undone.')) return;
  state.parts = state.parts.filter(function(x){ return x.id !== partId; });
  // Delete photo from Firebase Storage if exists
  if (window.firebaseDeletePartPhoto) window.firebaseDeletePartPhoto(partId).catch(function(){});
  // CRITICAL: _ownerDeleteInProgress must stay true until save() fully resolves
  // (save() is async — the RX cloud-delete queue runs after the promise resolves).
  // Setting it false synchronously after save() meant the flag was already off
  // by the time the RX loop ran, so Firebase never deleted the record and echoed it back.
  window._ownerDeleteInProgress = true;
  var saveResult = save();
  var afterSave = function() {
    window._ownerDeleteInProgress = false;
    catRenderProgress();
    catRenderDadQueue();
    try { renderInventory(); } catch(e) { /* UI rerender — non-critical */ }
    try { renderHome(); } catch(e) { /* UI rerender — non-critical */ }
  };
  if (saveResult && typeof saveResult.finally === 'function') {
    saveResult.finally(afterSave);
  } else {
    afterSave();
  }
  // Also force-push to Firebase immediately so the delete reaches the cloud
  // before any incoming snapshot echo can restore the record
  window._lastLocalSave = Date.now();
  if (window._fbSaveTimer) { clearTimeout(window._fbSaveTimer); window._fbSaveTimer = null; }
  if (window.firebasePush) window.firebasePush(state);
  logActivity && logActivity('Part deleted from pricing queue: ' + p.partNo + ' "' + p.name + '"', 'warn');
  toast('🗑 ' + p.name + ' deleted');
}

// ============================================================
// BRAND DISCOUNT MATRIX
// ============================================================
// state.brandMatrix entries:
//   { id, brand, category, costDisc, wsDisc, retailDisc,
//     supplierName, supplierPhone, createdAt }
// All disc = % off MRP.  Formulas:
//   costPrice    = round(MRP × (1 − costDisc/100))
//   creditPrice  = round(MRP × (1 − wsDisc/100))
//   instantPrice = round(MRP × (1 − retailDisc/100))

function bmLookup(brandName) {
  if (!brandName) return null;
  var q = brandName.trim().toLowerCase();
  return (state.brandMatrix || []).find(function(b) {
    return b.brand.trim().toLowerCase() === q;
  }) || null;
}

function bmApplyToMrp(entry, mrp) {
  mrp = parseFloat(mrp) || 0;
  return {
    costPrice:    Math.round(mrp * (1 - (entry.costDisc   || 0) / 100)),
    creditPrice:  Math.round(mrp * (1 - (entry.wsDisc     || 0) / 100)),
    instantPrice: Math.round(mrp * (1 - (entry.retailDisc || 0) / 100))
  };
}

// ── live calculator inside the brand form ─────────────────────
function bmLiveCalc() {
  var costDisc   = parseFloat(document.getElementById('bm-cost-disc')  ?.value) || 0;
  var wsDisc     = parseFloat(document.getElementById('bm-ws-disc')    ?.value) || 0;
  var retailDisc = parseFloat(document.getElementById('bm-retail-disc')?.value) || 0;
  var simMrp     = parseFloat(document.getElementById('bm-sim-mrp')    ?.value) || 0;

  var cp = document.getElementById('bm-cost-preview');
  var wp = document.getElementById('bm-ws-preview');
  var rp = document.getElementById('bm-retail-preview');
  if (cp) cp.textContent = costDisc   > 0 ? 'MRP − ' + costDisc   + '%' : '';
  if (wp) wp.textContent = wsDisc     > 0 ? 'MRP − ' + wsDisc     + '%' : '';
  if (rp) rp.textContent = retailDisc > 0 ? 'MRP − ' + retailDisc + '%' : '';

  var simEl = document.getElementById('bm-sim-result');
  if (simEl) {
    if (simMrp > 0 && (costDisc > 0 || wsDisc > 0 || retailDisc > 0)) {
      var sc = Math.round(simMrp * (1 - costDisc   / 100));
      var sw = Math.round(simMrp * (1 - wsDisc     / 100));
      var sr = Math.round(simMrp * (1 - retailDisc / 100));
      simEl.innerHTML =
        '<span style="color:#dc2626;font-weight:700;">Cost ₹' + sc.toLocaleString('en-IN') + '</span>'
        + ' &nbsp;·&nbsp; '
        + '<span style="color:#d97706;font-weight:700;">WS ₹' + sw.toLocaleString('en-IN') + '</span>'
        + ' &nbsp;·&nbsp; '
        + '<span style="color:#0f766e;font-weight:700;">Retail ₹' + sr.toLocaleString('en-IN') + '</span>';
    } else { simEl.innerHTML = ''; }
  }

  var ic = document.getElementById('bm-margin-insight');
  var ib = document.getElementById('bm-margin-body');
  if (ic && ib && costDisc > 0 && retailDisc > 0) {
    var rtM = (((1-retailDisc/100)-(1-costDisc/100))/(1-costDisc/100)*100);
    var wsM = wsDisc > 0 ? (((1-wsDisc/100)-(1-costDisc/100))/(1-costDisc/100)*100) : null;
    ib.innerHTML =
      '🛒 Retail margin on cost: <b>' + (rtM>=0?'+':'') + rtM.toFixed(1) + '%</b>'
      + (wsM !== null ? ' &nbsp;·&nbsp; 🏭 Wholesale margin on cost: <b>' + (wsM>=0?'+':'') + wsM.toFixed(1) + '%</b>' : '')
      + (simMrp > 0
        ? '<br>On MRP ₹' + simMrp.toLocaleString('en-IN')
          + ' → you earn <b>₹' + Math.round(simMrp*(1-retailDisc/100)-simMrp*(1-costDisc/100)).toLocaleString('en-IN')
          + ' per unit</b> at retail counter'
        : '');
    ic.style.display = '';
  } else if (ic) { ic.style.display = 'none'; }
}

// ── save / add / update ───────────────────────────────────────
function bmSave() {
  var id         = (document.getElementById('bm-edit-id')        ?.value || '').trim();
  var brand      = (document.getElementById('bm-brand')          ?.value || '').trim().toUpperCase();
  var category   = (document.getElementById('bm-category')       ?.value || '').trim();
  var supName    = (document.getElementById('bm-supplier')       ?.value || '').trim();
  var supPhone   = (document.getElementById('bm-supplier-phone') ?.value || '').trim();
  var costDisc   = parseFloat(document.getElementById('bm-cost-disc')  ?.value) || 0;
  var wsDisc     = parseFloat(document.getElementById('bm-ws-disc')    ?.value) || 0;
  var retailDisc = parseFloat(document.getElementById('bm-retail-disc')?.value) || 0;

  if (!brand) { toast('⚠️ Brand name is required'); return; }
  if (!costDisc && !wsDisc && !retailDisc) { toast('⚠️ Enter at least one discount %'); return; }

  var dup = (state.brandMatrix || []).find(function(b) {
    return b.brand.trim().toUpperCase() === brand && b.id !== id;
  });
  if (dup) { toast('⚠️ "' + brand + '" already exists — edit it instead'); return; }

  if (!state.brandMatrix) state.brandMatrix = [];
  var entry = { id: id || uid(), brand: brand, category: category,
    supplierName: supName, supplierPhone: supPhone,
    costDisc: costDisc, wsDisc: wsDisc, retailDisc: retailDisc,
    updatedAt: Date.now() };
  if (!id) entry.createdAt = Date.now();

  if (id) {
    var idx = state.brandMatrix.findIndex(function(b){ return b.id===id; });
    if (idx >= 0) state.brandMatrix[idx] = entry; else state.brandMatrix.push(entry);
    toast('✅ Brand updated');
  } else {
    state.brandMatrix.push(entry);
    toast('✅ Brand "' + brand + '" added');
  }
  if (supName) ensureSupplierRegistered(supName, supPhone);
  save();
  bmCancelEdit();
  renderBrandMatrixTab();
  logActivity && logActivity('Brand matrix saved: ' + brand + ' (' + costDisc + '/' + wsDisc + '/' + retailDisc + '%)', 'info');
}

function bmEdit(id) {
  var e = (state.brandMatrix || []).find(function(b){ return b.id===id; });
  if (!e) return;
  var fc = document.getElementById('bm-form-card');
  if (fc) fc.scrollIntoView({ behavior:'smooth', block:'start' });
  document.getElementById('bm-form-title').textContent       = '✏️ Edit Brand';
  document.getElementById('bm-cancel-btn').style.display     = '';
  document.getElementById('bm-edit-id').value                = e.id;
  document.getElementById('bm-brand').value                  = e.brand;
  document.getElementById('bm-category').value               = e.category      || '';
  document.getElementById('bm-supplier').value               = e.supplierName  || '';
  document.getElementById('bm-supplier-phone').value         = e.supplierPhone || '';
  document.getElementById('bm-cost-disc').value              = e.costDisc      || '';
  document.getElementById('bm-ws-disc').value                = e.wsDisc        || '';
  document.getElementById('bm-retail-disc').value            = e.retailDisc    || '';
  document.getElementById('bm-sim-mrp').value                = '';
  bmLiveCalc();
}

function bmCancelEdit() {
  document.getElementById('bm-form-title').textContent   = '➕ Add Brand';
  document.getElementById('bm-cancel-btn').style.display = 'none';
  ['bm-edit-id','bm-brand','bm-category','bm-supplier','bm-supplier-phone',
   'bm-cost-disc','bm-ws-disc','bm-retail-disc','bm-sim-mrp'].forEach(function(id) {
    var el = document.getElementById(id); if (el) el.value = '';
  });
  bmLiveCalc();
}

function bmDelete(id) {
  var e = (state.brandMatrix || []).find(function(b){ return b.id===id; });
  if (!e) return;
  if (!confirm('Delete brand "' + e.brand + '"?\n\nParts already priced won\'t change — only future auto-fill is removed.')) return;
  state.brandMatrix = state.brandMatrix.filter(function(b){ return b.id!==id; });
  save(); renderBrandMatrixTab(); toast('Brand removed');
}

// ── render the brand tab ──────────────────────────────────────
function renderBrandMatrixTab() {
  var all = state.brandMatrix || [];
  var q   = ((document.getElementById('bm-search') || {}).value || '').toLowerCase().trim();
  var list = q ? all.filter(function(b){
    return b.brand.toLowerCase().includes(q) || (b.supplierName||'').toLowerCase().includes(q) || (b.category||'').toLowerCase().includes(q);
  }) : all;

  // Count badge
  var badge = document.getElementById('bm-count-badge');
  if (badge) badge.textContent = all.length;

  // Queue alert — how many pending parts in catalogue have matrix matches with MRP set
  var readyParts = (state.parts || []).filter(function(p) {
    return catStaffDone(p) && !catPricingDone(p) && bmLookup(p.brand) && (p.mrp > 0);
  });
  var noMrpParts = (state.parts || []).filter(function(p) {
    return catStaffDone(p) && !catPricingDone(p) && bmLookup(p.brand) && !(p.mrp > 0);
  });
  var alertDiv  = document.getElementById('bm-queue-alert');
  var alertTitle = document.getElementById('bm-queue-alert-title');
  var alertBody  = document.getElementById('bm-queue-alert-body');
  if (alertDiv) {
    if (readyParts.length > 0) {
      alertDiv.style.display = '';
      if (alertTitle) alertTitle.textContent = '⚡ ' + readyParts.length + ' part' + (readyParts.length>1?'s':'') + ' ready to auto-price';
      if (alertBody) alertBody.textContent = 'Brand matrix set + MRP already entered by staff.'
        + (noMrpParts.length ? ' · ' + noMrpParts.length + ' more need MRP entered first.' : '');
    } else {
      alertDiv.style.display = 'none';
    }
  }

  // Profitability strip
  var strip = document.getElementById('bm-profit-strip');
  if (strip) {
    strip.innerHTML = all.slice(0,12).map(function(b) {
      var rm = (b.costDisc>0 && b.retailDisc>0)
        ? (((1-b.retailDisc/100)-(1-b.costDisc/100))/(1-b.costDisc/100)*100).toFixed(0) : null;
      var col = rm===null?'#94a3b8':rm<0?'#dc2626':rm<5?'#d97706':'#16a34a';
      return '<div style="background:white;border:1px solid var(--border);border-top:3px solid '+col
        +';border-radius:10px;padding:8px 12px;white-space:nowrap;flex-shrink:0;cursor:pointer;" onclick="bmEdit(\''+b.id+'\')">'
        + '<div style="font-size:12px;font-weight:700;color:var(--text);">' + b.brand + '</div>'
        + '<div style="font-size:11px;color:'+col+';font-weight:600;margin-top:2px;">'
        + (rm!==null ? (rm>=0?'+':'')+rm+'% margin' : '—') + '</div>'
        + '</div>';
    }).join('');
  }

  // Brand list
  var div = document.getElementById('bm-list');
  if (!div) return;
  if (!list.length) {
    div.innerHTML = q
      ? '<div class="empty" style="padding:20px;">No brands match "'+q+'"</div>'
      : '<div class="empty" style="padding:32px;text-align:center;"><div style="font-size:40px;margin-bottom:10px;">🏷️</div>No brands yet.<br><small>Add Oxid, SKF, Mahindra etc. above.</small></div>';
    return;
  }

  div.innerHTML = list.map(function(b) {
    var partTotal  = (state.parts||[]).filter(function(p){ return (p.brand||'').trim().toUpperCase()===b.brand; }).length;
    var pendingMrp = (state.parts||[]).filter(function(p){ return (p.brand||'').trim().toUpperCase()===b.brand && catStaffDone(p) && !catPricingDone(p) && (p.mrp>0); }).length;
    var pendingNoMrp=(state.parts||[]).filter(function(p){ return (p.brand||'').trim().toUpperCase()===b.brand && catStaffDone(p) && !catPricingDone(p) && !(p.mrp>0); }).length;

    var rtM = (b.costDisc>0&&b.retailDisc>0)
      ? (((1-b.retailDisc/100)-(1-b.costDisc/100))/(1-b.costDisc/100)*100).toFixed(1) : null;
    var wsM = (b.costDisc>0&&b.wsDisc>0)
      ? (((1-b.wsDisc/100)-(1-b.costDisc/100))/(1-b.costDisc/100)*100).toFixed(1) : null;
    var mCol = rtM===null?'#94a3b8':rtM<0?'#dc2626':rtM<5?'#d97706':'#16a34a';

    return '<div style="background:var(--bg);border-radius:12px;padding:12px 14px;margin-bottom:8px;border-left:4px solid '+mCol+';">'
      + '<div style="display:flex;justify-content:space-between;align-items:flex-start;gap:10px;">'
      +   '<div style="flex:1;min-width:0;">'
      +     '<div style="font-size:15px;font-weight:800;color:var(--text);">' + b.brand
      +       (b.category ? ' <span style="font-size:11px;font-weight:500;color:var(--muted);">· ' + b.category + '</span>' : '')
      +     '</div>'
      +     '<div style="display:flex;gap:5px;flex-wrap:wrap;margin-top:6px;">'
      +       (b.costDisc   ? '<span style="background:#fef2f2;color:#dc2626;font-size:11px;font-weight:700;padding:3px 8px;border-radius:6px;">Cost −'+b.costDisc+'%</span>' : '')
      +       (b.wsDisc     ? '<span style="background:#fffbeb;color:#d97706;font-size:11px;font-weight:700;padding:3px 8px;border-radius:6px;">WS −'+b.wsDisc+'%</span>' : '')
      +       (b.retailDisc ? '<span style="background:#f0fdf4;color:#166534;font-size:11px;font-weight:700;padding:3px 8px;border-radius:6px;">Retail −'+b.retailDisc+'%</span>' : '')
      +     '</div>'
      +     '<div style="font-size:12px;margin-top:5px;">'
      +       (rtM!==null ? '<span style="color:'+mCol+';font-weight:600;">Retail margin: '+(rtM>=0?'+':'')+rtM+'%</span>' : '')
      +       (wsM!==null ? ' <span style="color:var(--muted);">· WS: '+(wsM>=0?'+':'')+wsM+'%</span>' : '')
      +     '</div>'
      +     '<div style="font-size:11px;color:var(--muted);margin-top:3px;">'
      +       (b.supplierName ? '🏢 ' + b.supplierName + ' &nbsp;·&nbsp; ' : '')
      +       partTotal + ' part' + (partTotal!==1?'s':'') + ' in catalog'
      +       (pendingMrp ? ' &nbsp;·&nbsp; <span style="color:#0f766e;font-weight:700;">'+pendingMrp+' ready to auto-price</span>' : '')
      +       (pendingNoMrp ? ' &nbsp;·&nbsp; <span style="color:#d97706;">'+pendingNoMrp+' need MRP</span>' : '')
      +     '</div>'
      +   '</div>'
      +   '<div style="display:flex;flex-direction:column;gap:6px;align-items:flex-end;flex-shrink:0;">'
      +     (pendingMrp ? '<button class="btn btn-sm" onclick="bmAutoApproveByBrand(\''+b.id+'\')" style="width:auto;padding:6px 10px;font-size:12px;background:#0f766e;white-space:nowrap;">⚡ Auto-price '+pendingMrp+'</button>' : '')
      +     '<div style="display:flex;gap:5px;">'
      +       '<button class="btn btn-sm btn-outline" onclick="bmEdit(\''+b.id+'\')" style="width:auto;padding:6px 10px;">✏️</button>'
      +       '<button class="btn btn-sm" onclick="bmDelete(\''+b.id+'\')" style="width:auto;padding:6px 10px;background:var(--danger);">🗑</button>'
      +     '</div>'
      +   '</div>'
      + '</div>'
      + '</div>';
  }).join('');
}

// ── "Auto-price" — apply matrix to all ready pending parts for a brand ──
function bmAutoApproveByBrand(bmId) {
  var e = (state.brandMatrix||[]).find(function(b){return b.id===bmId;});
  if (!e) return;
  _bmApplyBrand(e);
}

// ── "Auto-price all" button — apply matrix across all brands ──
function bmAutoApproveAll() {
  var readyParts = (state.parts||[]).filter(function(p){
    return catStaffDone(p) && !catPricingDone(p) && bmLookup(p.brand) && (p.mrp>0);
  });
  if (!readyParts.length) { toast('No parts ready to auto-price'); return; }

  // Group by brand for the confirm message
  var brandCounts = {};
  readyParts.forEach(function(p){
    var b = (p.brand||'').trim().toUpperCase();
    brandCounts[b] = (brandCounts[b]||0)+1;
  });
  var summary = Object.keys(brandCounts).map(function(b){return b+' ('+brandCounts[b]+')';}).join(', ');

  if (!confirm('Auto-price ' + readyParts.length + ' parts from these brands?\n\n' + summary + '\n\nPrices will be calculated from MRP using each brand\'s matrix. Parts go live for sale immediately.')) return;

  var count = 0;
  (state.brandMatrix||[]).forEach(function(e) {
    count += _bmApplyBrand(e, true); // silent=true, batch mode
  });
  save();
  renderBrandMatrixTab();
  try { catRenderDadQueue && catRenderDadQueue(); catRenderProgress && catRenderProgress(); } catch(ex) {}
  try { renderInventory && renderInventory(); renderHome && renderHome(); } catch(ex) {}
  logActivity && logActivity('Brand matrix bulk auto-price: ' + count + ' parts', 'info');
  toast('✅ ' + count + ' parts priced — now live for sale!');
}

// Core apply function. Returns count of parts priced.
// silent=true skips the individual save/toast (caller will batch-save).
function _bmApplyBrand(e, silent) {
  var pending = (state.parts||[]).filter(function(p){
    return (p.brand||'').trim().toUpperCase() === e.brand
      && catStaffDone(p) && !catPricingDone(p) && (p.mrp > 0);
  });
  if (!pending.length) { if (!silent) toast('No ready parts for ' + e.brand); return 0; }

  var count = 0;
  pending.forEach(function(p) {
    var pt = getPartById(p.id); // O(1) via parts index — was a full findIndex scan per item
    if (!pt) return;
    var prices = bmApplyToMrp(e, p.mrp);
    pt.costPrice    = prices.costPrice;
    pt.creditPrice  = prices.creditPrice;
    pt.instantPrice = prices.instantPrice;
    if (e.supplierName  && !pt.supplierName)  pt.supplierName  = e.supplierName;
    if (e.supplierPhone && !pt.supplierPhone) pt.supplierPhone = e.supplierPhone;
    count++;
  });
  if (count) markChanged('parts'); // #1: mid-array field edits won't change the fingerprint — mark dirty even on the silent path

  if (!silent) {
    save();
    renderBrandMatrixTab();
    try { catRenderDadQueue && catRenderDadQueue(); catRenderProgress && catRenderProgress(); } catch(ex){}
    try { renderInventory && renderInventory(); renderHome && renderHome(); } catch(ex){}
    logActivity && logActivity('Brand matrix auto-price: ' + e.brand + ', ' + count + ' parts', 'info');
    toast('✅ ' + count + ' ' + e.brand + ' parts priced · now live!');
  }
  return count;
}

// ── Catalogue queue: auto-approve button ─────────────────────
// Called from the 🏷️ button in cat-dad-panel header
function catBmAutoApprove() {
  var readyParts = (state.parts||[]).filter(function(p){
    return catStaffDone(p) && !catPricingDone(p) && bmLookup(p.brand) && (p.mrp>0);
  });
  if (!readyParts.length) { toast('No matrix-matched parts with MRP set'); return; }

  var brandGroups = {};
  readyParts.forEach(function(p){
    var b = (p.brand||'').trim().toUpperCase();
    if (!brandGroups[b]) brandGroups[b] = 0;
    brandGroups[b]++;
  });
  var msg = 'Auto-price ' + readyParts.length + ' parts?\n\n'
    + Object.keys(brandGroups).map(function(b){ return '• ' + b + ': ' + brandGroups[b] + ' parts'; }).join('\n')
    + '\n\nPrices fill from brand matrix × MRP. They go live for sale immediately.';
  if (!confirm(msg)) return;

  var count = 0;
  (state.brandMatrix||[]).forEach(function(e){ count += _bmApplyBrand(e, true); });
  save();
  catRenderDadQueue(); catRenderProgress && catRenderProgress();
  try { renderInventory(); renderHome(); } catch(ex){}
  logActivity && logActivity('Catalogue matrix auto-approve: ' + count + ' parts', 'info');
  toast('✅ ' + count + ' parts priced · ' + count + ' now live!');
}

// ── inject matrix banner + auto-fill into each queue card ────
// Patches catRenderDadQueue so matched parts get a banner + prefilled prices
var _catRenderDadQueueOrig = catRenderDadQueue;
catRenderDadQueue = function() {
  _catRenderDadQueueOrig();

  // Show/hide the auto-approve button + match banner in header
  setTimeout(function() {
    var readyCount = (state.parts||[]).filter(function(p){
      return catStaffDone(p) && !catPricingDone(p) && bmLookup(p.brand) && (p.mrp>0);
    }).length;
    var autoBtn = document.getElementById('cat-bm-autoapprove-btn');
    var banner  = document.getElementById('cat-bm-match-banner');
    var bTitle  = document.getElementById('cat-bm-banner-title');
    var bBody   = document.getElementById('cat-bm-banner-body');
    var noMrpCount = (state.parts||[]).filter(function(p){
      return catStaffDone(p) && !catPricingDone(p) && bmLookup(p.brand) && !(p.mrp>0);
    }).length;

    if (autoBtn) { autoBtn.style.display = readyCount > 0 ? '' : 'none'; if (readyCount>0) autoBtn.textContent = '🏷️ Auto-price ' + readyCount + ' matched'; }
    if (banner) {
      if (readyCount > 0 || noMrpCount > 0) {
        banner.style.display = '';
        if (bTitle) bTitle.textContent = '🏷️ Brand matrix: ' + readyCount + ' parts can be auto-priced' + (noMrpCount ? ' · ' + noMrpCount + ' more need MRP' : '');
        if (bBody)  bBody.textContent  = readyCount > 0
          ? 'Tap "Auto-price" to fill all matched parts instantly. Only unmatched parts need manual pricing below.'
          : 'The ' + noMrpCount + ' remaining ' + (noMrpCount===1?'part':'parts') + ' need MRP entered — then auto-price will fill the rest.';
      } else { banner.style.display = 'none'; }
    }

    // Per-card: inject banner + prefill prices for matrix-matched parts
    var pending = (state.parts||[]).filter(function(p){
      return catStaffDone(p) && !catPricingDone(p);
    });
    pending.forEach(function(p) {
      var entry = bmLookup(p.brand);
      if (!entry) return;
      var qitem = document.getElementById('cqi-'+p.id);
      if (!qitem) return;

      var mrpEl    = document.getElementById('cdmrp-'   +p.id);
      var costEl   = document.getElementById('cdcost-'  +p.id);
      var wsEl     = document.getElementById('cdws-'    +p.id);
      var retailEl = document.getElementById('cdretail-'+p.id);
      var supEl    = document.getElementById('cdsup-'   +p.id);
      var phEl     = document.getElementById('cdphone-' +p.id);
      var mrp = parseFloat((mrpEl||{}).value) || p.mrp || 0;

      // Auto-fill if MRP is available
      if (mrp > 0) {
        var prices = bmApplyToMrp(entry, mrp);
        if (costEl   && !parseFloat(costEl.value))   costEl.value   = prices.costPrice;
        if (wsEl     && !parseFloat(wsEl.value))     wsEl.value     = prices.creditPrice;
        if (retailEl && !parseFloat(retailEl.value)) retailEl.value = prices.instantPrice;
        if (supEl    && !supEl.value && entry.supplierName)  supEl.value  = entry.supplierName;
        if (phEl     && !phEl.value  && entry.supplierPhone) phEl.value   = entry.supplierPhone;
        try { catDadAuto(p.id); } catch(ex) {}
      }

      // Inject card-level banner
      if (qitem.querySelector('.bm-card-banner')) return;
      var cardBanner = document.createElement('div');
      cardBanner.className = 'bm-card-banner';
      cardBanner.style.cssText = 'background:#f0fdf4;border:1px solid #86efac;border-radius:8px;padding:7px 10px;margin-bottom:8px;font-size:12px;color:#166534;display:flex;justify-content:space-between;align-items:center;gap:6px;';
      var filled = mrp > 0;
      cardBanner.innerHTML = '<span>🏷️ <b>' + entry.brand + '</b> matrix'
        + (filled ? ' — auto-filled from MRP ₹' + mrp.toLocaleString('en-IN') : ' — <span style="color:#d97706;font-weight:600;">enter MRP above to auto-fill</span>')
        + '</span>'
        + '<button onclick="bmReapplyCard(\''+p.id+'\',\''+entry.id+'\')" style="background:#16a34a;color:white;border:none;border-radius:5px;padding:3px 9px;font-size:11px;font-weight:700;cursor:pointer;flex-shrink:0;">↺ Re-fill</button>';
      var priceGrid = qitem.querySelector('.cat-price-grid');
      if (priceGrid) qitem.insertBefore(cardBanner, priceGrid);
      else qitem.prepend(cardBanner);

      // Wire MRP field to re-apply matrix when changed
      if (mrpEl && !mrpEl.dataset.bmWired) {
        mrpEl.dataset.bmWired = '1';
        mrpEl.addEventListener('input', function() {
          if (parseFloat(this.value) > 0) bmReapplyCard(p.id, entry.id, true);
        });
      }
    });
  }, 70);
};

function bmReapplyCard(partId, bmId, onlyEmpty) {
  var e = (state.brandMatrix||[]).find(function(b){return b.id===bmId;});
  if (!e) return;
  var mrpEl    = document.getElementById('cdmrp-'   +partId);
  var costEl   = document.getElementById('cdcost-'  +partId);
  var wsEl     = document.getElementById('cdws-'    +partId);
  var retailEl = document.getElementById('cdretail-'+partId);
  var supEl    = document.getElementById('cdsup-'   +partId);
  var phEl     = document.getElementById('cdphone-' +partId);
  var mrp = parseFloat((mrpEl||{}).value) || 0;
  if (!mrp) { toast('⚠️ Enter MRP first'); return; }
  var prices = bmApplyToMrp(e, mrp);
  if (onlyEmpty) {
    if (costEl   && !parseFloat(costEl.value))   costEl.value   = prices.costPrice;
    if (wsEl     && !parseFloat(wsEl.value))     wsEl.value     = prices.creditPrice;
    if (retailEl && !parseFloat(retailEl.value)) retailEl.value = prices.instantPrice;
  } else {
    if (costEl)   costEl.value   = prices.costPrice;
    if (wsEl)     wsEl.value     = prices.creditPrice;
    if (retailEl) retailEl.value = prices.instantPrice;
  }
  if (supEl && !supEl.value && e.supplierName)  supEl.value  = e.supplierName;
  if (phEl  && !phEl.value  && e.supplierPhone) phEl.value   = e.supplierPhone;
  try { catDadAuto(partId); } catch(ex) {}
  // Update card banner text
  var cb = document.querySelector('#cqi-'+partId+' .bm-card-banner span');
  if (cb) cb.innerHTML = '🏷️ <b>'+e.brand+'</b> matrix — auto-filled from MRP ₹'+mrp.toLocaleString('en-IN');
}

// ── hook into openAddPart: auto-fill when brand + MRP typed ──
var _uapAutoPriceOrig = uapAutoPrice;
uapAutoPrice = function() {
  _uapAutoPriceOrig();
  var brandEl  = document.getElementById('uap-brand');
  var mrpEl    = document.getElementById('uap-mrp');
  var costEl   = document.getElementById('uap-cost');
  var wsEl     = document.getElementById('uap-credit');
  var retailEl = document.getElementById('uap-instant');
  var hintEl   = document.getElementById('uap-bm-hint');
  if (!brandEl || !mrpEl) return;
  var brand = (brandEl.value || '').trim().toUpperCase();
  var mrp   = parseFloat(mrpEl.value) || 0;
  if (!brand || mrp <= 0) { if (hintEl) hintEl.style.display='none'; return; }
  var entry = bmLookup(brand);
  if (!entry) { if (hintEl) hintEl.style.display='none'; return; }
  var prices = bmApplyToMrp(entry, mrp);
  if (costEl   && (!costEl.value   || parseFloat(costEl.value)   === 0)) costEl.value   = prices.costPrice;
  if (wsEl     && (!wsEl.value     || parseFloat(wsEl.value)     === 0)) wsEl.value     = prices.creditPrice;
  if (retailEl && (!retailEl.value || parseFloat(retailEl.value) === 0)) retailEl.value = prices.instantPrice;
  if (hintEl) {
    hintEl.style.display = '';
    hintEl.innerHTML = '🏷️ <b>'+entry.brand+' matrix applied</b> — Cost −'+entry.costDisc+'% · WS −'+entry.wsDisc+'% · Retail −'+entry.retailDisc+'% off MRP'
      + ' &nbsp;<a href="#" onclick="bmClearUapFill();return false;" style="color:#64748b;font-size:11px;">clear</a>';
  }
};

var _openAddPartOrig = openAddPart;
openAddPart = function(idOrOpts) {
  _openAddPartOrig(idOrOpts);
  setTimeout(function() {
    if (document.getElementById('uap-bm-hint')) return;
    var mrpEl  = document.getElementById('uap-mrp');
    var brandEl= document.getElementById('uap-brand');
    var sec    = mrpEl ? mrpEl.closest('.uap-section') : null;
    if (!sec) return;
    var hint = document.createElement('div');
    hint.id = 'uap-bm-hint';
    hint.style.cssText = 'display:none;background:#f0fdf4;border:1px solid #86efac;border-radius:8px;padding:8px 12px;margin-bottom:8px;font-size:12px;color:#166534;';
    var anchor = sec.querySelector('.row3,.row2');
    sec.insertBefore(hint, anchor || sec.firstChild);
    if (mrpEl)   mrpEl.addEventListener('input', uapAutoPrice);
    if (brandEl) { brandEl.addEventListener('input', function(){ setTimeout(uapAutoPrice,60); }); brandEl.addEventListener('change', function(){ setTimeout(uapAutoPrice,60); }); }
    setTimeout(uapAutoPrice, 30);
  }, 80);
};

function bmClearUapFill() {
  var h = document.getElementById('uap-bm-hint'); if (h) h.style.display='none';
  ['uap-cost','uap-credit','uap-instant'].forEach(function(id){ var el=document.getElementById(id); if(el) el.value=''; });
  _uapAutoPriceOrig();
}

// ── bulk import: apply matrix when brand+MRP present ─────────
var _impCleanRowsOrig = typeof impCleanRows==='function' ? impCleanRows : null;
if (_impCleanRowsOrig) {
  impCleanRows = function(rawRows, map) {
    var rows = _impCleanRowsOrig(rawRows, map);
    rows.forEach(function(r) {
      if (r.brand && r.mrp > 0) {
        var e = bmLookup(r.brand);
        if (e) {
          if (!r.costPrice    || r.costPrice    === 0) r.costPrice    = Math.round(r.mrp*(1-e.costDisc/100));
          if (!r.creditPrice  || r.creditPrice  === 0) r.creditPrice  = Math.round(r.mrp*(1-e.wsDisc/100));
          if (!r.instantPrice || r.instantPrice === 0) r.instantPrice = Math.round(r.mrp*(1-e.retailDisc/100));
          if (!r.supplierName  && e.supplierName)  r.supplierName  = e.supplierName;
          if (!r.supplierPhone && e.supplierPhone) r.supplierPhone = e.supplierPhone;
        }
      }
    });
    return rows;
  };
}
// ============================================================
// END BRAND DISCOUNT MATRIX
// ============================================================

function catDadSave(partId, andNext) {
  var costEl   = document.getElementById('cdcost-'   + partId);
  var retailEl = document.getElementById('cdretail-' + partId);
  var cost   = costEl   ? (parseFloat(costEl.value)   || 0) : 0;
  var retail = retailEl ? (parseFloat(retailEl.value) || 0) : 0;
  if (!cost || !retail) {
    console.warn('[catDadSave] blocked — cost:', cost, 'retail:', retail, 'partId:', partId, 'costEl:', !!costEl, 'retailEl:', !!retailEl);
    toast('⚠️ Cost and retail price required'); return;
  }
  var idx = state.parts.findIndex(x => x.id === partId);
  if (idx < 0) return;
  var p = state.parts[idx];
  p.costPrice    = cost;
  p.mrp          = +(document.getElementById('cdmrp-'+partId)  ||{}).value || 0;
  p.instantPrice = retail;
  p.creditPrice  = +(document.getElementById('cdws-'+partId)   ||{}).value || 0;
  p.minStock     = +(document.getElementById('cdmin-'+partId)  ||{}).value || p.minStock || 0;
  p.supplierName = ((document.getElementById('cdsup-'+partId)  ||{}).value || '').trim();
  p.supplierPhone= ((document.getElementById('cdphone-'+partId)||{}).value || '').trim();
  state.parts[idx] = p;
  // Register supplier in state.suppliers if name is new
  ensureSupplierRegistered(p.supplierName, p.supplierPhone);
  // ── IMMEDIATELY remove the card from the DOM so it can't flash back ──
  // Do this BEFORE save/push so even a fast Firebase echo can't restore it
  // (the re-render below will also omit it since catPricingDone is now true).
  var cardEl = document.getElementById('cqi-' + partId);
  if (cardEl) cardEl.remove();
  // Stamp _lastLocalSave NOW before async push so the echo guard in firebaseListenAll
  // and firebasePullParts is always active when the confirmation snapshot arrives.
  window._lastLocalSave = Date.now();
  markChanged('parts');
  save();
  // Force-flush Firebase immediately — never rely on 1.2s debounce for pricing edits.
  // This ensures the cloud is updated before firebasePullParts can echo back a stale version.
  if (window._fbSaveTimer) { clearTimeout(window._fbSaveTimer); window._fbSaveTimer = null; }
  if (window.firebasePush) window.firebasePush(state);
  catRenderProgress();
  try { patchInventoryCard(p); } catch(e) { /* UI rerender — non-critical */ }
  try { renderHomeDebounced(); } catch(e) { /* UI rerender — non-critical */ }
  try { xlsRender && xlsRender(); } catch(e) { /* UI rerender — non-critical */ }
  var stats = catCalcProgress();
  logActivity && logActivity('Pricing filled: ' + p.partNo + ' "' + p.name + '"', 'info');
  toast('✅ ' + p.name + ' pricing saved · now live for sale · ' + stats.complete + ' of ' + stats.total + ' complete');
  // Card was already removed from DOM above — just check if queue is now empty
  var list = document.getElementById('cat-dad-list');
  if (list && !list.querySelector('.cat-qitem')) {
    list.innerHTML = '<div class="card" style="text-align:center;padding:24px;color:var(--muted);">🎉 All parts have pricing filled in!</div>';
  }
  catRenderProgressDebounced();
}
