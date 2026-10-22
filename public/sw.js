// Goury Parts service worker.
// Network-first for the app's own files, so every Vercel deploy reaches users on
// their next load, with the cached copy as an offline fallback. Requests to other
// origins (Firebase, AI APIs, CDNs) are never touched: they handle their own caching.
const CACHE = 'goury-shell-v5';
const SHELL = [
  '/', '/index.html', '/manifest.json',
  '/css/main.css', '/css/agent.css', '/css/extras.css', '/css/dark-theme.css',
  '/js/00-ai-proxy.js', '/js/convex-firestore.mjs', '/js/01-firebase-sync.mjs', '/js/02-lib-loader.js', '/js/03-app-core.js',
  '/js/04-app-features.js', '/js/05-i18n.js', '/js/06-catalogue.js',
  '/js/07-sw-register.js', '/js/08-goury-os-cns.js', '/js/09-cns-ui.js',
  '/js/10-perf-patches.js', '/js/11-app-extras.js', '/js/12-ai-proxy-mode.js',
  '/icons/icon-192.png', '/icons/icon-512.png'
];

// Third-party scripts the app can't start without, pinned to exact versions so they
// never change: served from cache first so the app also opens offline.
const CDN = [
  'https://cdn.jsdelivr.net/npm/convex@1.46.0/dist/browser.bundle.js'
];

self.addEventListener('install', (event) => {
  event.waitUntil(caches.open(CACHE).then((c) => Promise.all([
    c.addAll(SHELL),
    Promise.all(CDN.map((u) => fetch(u, { mode: 'cors' }).then((r) => r.ok && c.put(u, r)).catch(() => {})))
  ])).then(() => self.skipWaiting()));
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);
  if (CDN.includes(req.url)) {
    event.respondWith(caches.match(req.url).then((hit) => hit || fetch(req).then((res) => {
      if (res.ok) { const copy = res.clone(); caches.open(CACHE).then((c) => c.put(req.url, copy)); }
      return res;
    })));
    return;
  }
  if (url.origin !== self.location.origin) return;

  event.respondWith(
    fetch(req)
      .then((res) => {
        if (res.ok) {
          const copy = res.clone();
          caches.open(CACHE).then((c) => c.put(req, copy));
        }
        return res;
      })
      .catch(() =>
        caches.match(req).then((hit) => hit || (req.mode === 'navigate' ? caches.match('/index.html') : Response.error()))
      )
  );
});
