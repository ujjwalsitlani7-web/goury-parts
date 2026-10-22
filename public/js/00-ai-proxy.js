// AI PROXY (client side). Loaded before every other script.
// Any request the app makes to api.anthropic.com is sent to our own /api/claude
// instead, with the API key header removed. The server adds the real key.
// Each device proves it belongs to the shop with a SHOP_TOKEN, asked for once.
(function () {
  'use strict';
  var TOKEN_KEY = 'goury.shopToken';
  var ANTHROPIC = 'https://api.anthropic.com/v1/messages';
  var realFetch = window.fetch.bind(window);

  function getToken() { try { return localStorage.getItem(TOKEN_KEY) || ''; } catch (_) { return ''; } }
  function setToken(t) { try { localStorage.setItem(TOKEN_KEY, t); } catch (_) {} }
  function clearToken() { try { localStorage.removeItem(TOKEN_KEY); } catch (_) {} }

  // One prompt at a time, even if several AI calls fail together.
  var pending = null;
  function askForToken(message) {
    if (pending) return pending;
    pending = new Promise(function (resolve) {
      var ov = document.createElement('div');
      ov.style.cssText = 'position:fixed;inset:0;z-index:100001;background:rgba(2,6,23,.72);backdrop-filter:blur(3px);display:flex;align-items:center;justify-content:center;padding:20px;';
      ov.innerHTML =
        '<div style="width:100%;max-width:340px;background:linear-gradient(160deg,#0f172a,#1e1b4b);border:1px solid rgba(165,243,252,.25);border-radius:16px;padding:20px;box-shadow:0 20px 60px rgba(0,0,0,.5);font-family:inherit;">' +
          '<div style="font-size:15px;font-weight:800;color:#a5f3fc;margin-bottom:4px;">\uD83D\uDD11 Connect this device to Shop AI</div>' +
          '<div id="_stMsg" style="font-size:12px;color:rgba(255,255,255,.65);line-height:1.5;margin-bottom:14px;"></div>' +
          '<input id="_stInput" type="password" autocomplete="off" placeholder="Shop token" style="width:100%;box-sizing:border-box;padding:11px 12px;border-radius:10px;border:1px solid rgba(255,255,255,.2);background:rgba(255,255,255,.08);color:white;font-size:16px;margin-bottom:12px;">' +
          '<div style="display:flex;gap:8px;">' +
            '<button id="_stSave" style="flex:1;padding:11px;border-radius:10px;border:none;background:#0f766e;color:white;font-size:14px;font-weight:700;cursor:pointer;">Save</button>' +
            '<button id="_stLater" style="padding:11px 14px;border-radius:10px;border:1px solid rgba(255,255,255,.18);background:rgba(255,255,255,.06);color:rgba(255,255,255,.6);font-size:13px;cursor:pointer;">Later</button>' +
          '</div>' +
        '</div>';
      ov.querySelector('#_stMsg').textContent = message;
      document.body.appendChild(ov);
      var input = ov.querySelector('#_stInput');
      function done(value) {
        try { document.body.removeChild(ov); } catch (_) {}
        pending = null;
        resolve(value);
      }
      ov.querySelector('#_stSave').onclick = function () {
        var v = (input.value || '').trim();
        if (!v) { input.focus(); return; }
        setToken(v); done(v);
      };
      ov.querySelector('#_stLater').onclick = function () { done(''); };
      input.addEventListener('keydown', function (e) { if (e.key === 'Enter') ov.querySelector('#_stSave').click(); });
      setTimeout(function () { input.focus(); }, 50);
    });
    return pending;
  }

  function toProxy(opts, token) {
    var h = {};
    var src = (opts && opts.headers) || {};
    if (typeof Headers !== 'undefined' && src instanceof Headers) src.forEach(function (v, k) { h[k] = v; });
    else Object.keys(src).forEach(function (k) { h[k] = src[k]; });
    Object.keys(h).forEach(function (k) {
      var lk = k.toLowerCase();
      if (lk === 'x-api-key' || lk === 'anthropic-version' || lk === 'anthropic-dangerous-direct-browser-access') delete h[k];
    });
    h['x-shop-token'] = token;
    return realFetch('/api/claude', Object.assign({}, opts, { headers: h }));
  }

  window.fetch = async function (input, opts) {
    var url = typeof input === 'string' ? input : (input && input.url) || '';
    if (url.indexOf(ANTHROPIC) !== 0) return realFetch(input, opts);

    var token = getToken();
    if (!token) token = await askForToken('Enter the shop token once to turn on AI features on this phone. Ask the owner if you don\u2019t have it.');
    if (!token) return realFetch('/api/claude', { method: 'POST' }); // yields a clean 401 the app already displays

    var res = await toProxy(opts, token);
    if (res.status === 401 && res.headers.get('x-proxy-auth') === 'failed') {
      clearToken();
      token = await askForToken('That shop token didn\u2019t work. Check it and try again.');
      if (token) res = await toProxy(opts, token);
    }
    return res;
  };

  window.resetShopToken = function () { clearToken(); };
})();
