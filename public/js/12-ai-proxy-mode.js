// AI PROXY MODE. Loaded after the app's own scripts.
// The real API key now lives on the server, so the old browser-side key system
// (PIN-encrypted central key, unlock prompts, local keys) is switched off here.
// getAIKey() returns a placeholder that passes the app's existing "sk-" checks;
// 00-ai-proxy.js strips it before anything leaves the device.
(function () {
  'use strict';
  var PLACEHOLDER = 'sk-server-proxy';
  window.getAIKey = function () { return PLACEHOLDER; };
  window.getAIVisionKey = function () { return PLACEHOLDER; };
  window.hasCentralKey = function () { return true; };
  window.ensureCentralUnlockedOnBoot = function () {};
  window.promptUnlockCentralAI = function () {};
})();

// LOGIN WAITS FOR CLOUD DATA.
// initLogin() runs once at boot using only what's saved on this device. On a new
// phone or a new web address (like the Vercel site) nothing is saved yet, so it
// showed "Set up your shop" even when the shop already exists in Firebase, and
// never re-checked after the cloud data arrived. Filling that form would create a
// second admin over the real shop.
// Now: while the device has no users, show "Loading your shop…", and re-run
// initLogin() as soon as users arrive from the cloud. Only if the cloud has
// finished loading with no users (or the device is offline, or ~20 s pass) is the
// setup form shown.
(function () {
  'use strict';
  if (typeof state === 'undefined' || state.session || (state.users && state.users.length)) return;

  var setup = document.getElementById('setup-panel');
  var login = document.getElementById('login-panel');
  if (!setup || !login) return;

  var pullDone = false;
  // The Firebase module runs after this file and assigns window.firebasePull;
  // wrap it on assignment so we know when the first cloud pull has finished.
  var _pull;
  try {
    Object.defineProperty(window, 'firebasePull', {
      configurable: true,
      get: function () { return _pull; },
      set: function (fn) {
        _pull = typeof fn !== 'function' ? fn : function () {
          return fn.apply(this, arguments).finally(function () {
            // Give initFirebase a moment to merge the result into state.
            setTimeout(function () { pullDone = true; }, 300);
          });
        };
      }
    });
  } catch (_) {}

  var wait = document.createElement('div');
  wait.id = 'cloud-wait-panel';
  wait.style.cssText = 'text-align:center;padding:28px 8px;color:var(--muted);font-size:14px;line-height:1.6;';
  wait.innerHTML = '<div style="font-size:26px;margin-bottom:6px;">\u23F3</div>Loading your shop\u2026<br><span style="font-size:12px;">Getting your data from the cloud</span>';
  setup.parentNode.insertBefore(wait, setup);
  setup.style.display = 'none';
  login.style.display = 'none';

  var started = Date.now();
  var timer = setInterval(function () {
    var haveUsers = state.users && state.users.length > 0;
    var giveUp = !navigator.onLine || pullDone || Date.now() - started > 20000;
    if (!haveUsers && !giveUp) return;
    clearInterval(timer);
    try { wait.remove(); } catch (_) {}
    if (haveUsers) {
      try { initLogin(); } catch (e) { console.warn('[login] refresh failed:', e && e.message); }
    } else {
      setup.style.display = 'block'; // genuinely a new shop (or no connection)
    }
  }, 250);
})();
