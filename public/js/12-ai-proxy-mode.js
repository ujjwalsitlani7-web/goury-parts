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
