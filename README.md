# Goury Parts

Spare parts shop management app (stock, billing, staff, suppliers, purchase orders), installable as a PWA on phones.

This repo is the original single-file `index.html` split into separate CSS and JS files, with no change to app behaviour. The split version was checked against the original in a headless browser on desktop and mobile and renders pixel-identical, with no new errors.

## Project layout

```
public/                 everything Vercel serves
  index.html            page markup (was 1.4 MB, now ~135 KB)
  css/                  main.css, agent.css, extras.css
  js/                   scripts, loaded in the same order as the original
    00-ai-proxy.js          sends all AI calls to /api/claude instead of Anthropic
    01-firebase-sync.mjs    Firebase setup + all cloud sync  ← the file Convex will replace
    02-lib-loader.js        lazy loader for xlsx / zxing from jsDelivr
    03-app-core.js          main app (reliability layer, parts, sales, billing…)
    04-app-features.js      Firestore-dependent features
    05-i18n.js              English ↔ Hindi labels
    06-catalogue.js         catalogue registration
    07-sw-register.js       service worker registration
    08-goury-os-cns.js      Goury OS core
    09-cns-ui.js            Goury OS UI
    10-perf-patches.js      performance patches
    11-app-extras.js        extras
    12-ai-proxy-mode.js     switches off the old browser-side key system
  sw.js                 service worker (network-first, works offline)
  manifest.json         PWA manifest
  icons/                app icons
api/claude.js           server-side AI proxy (holds the Anthropic key)
vercel.json             Vercel config (no build step, cache + security headers)
scripts/check.mjs       syntax check for all scripts
```

The scripts are plain `<script>` tags, not bundled, because the app relies on ~580 inline `onclick` handlers that need functions to stay global. Order matters: keep the tags in `index.html` as they are.

## Run locally

```
npm i -g vercel        # once
cp .env.example .env   # then fill in the two values
npm run dev            # app + /api/claude at http://localhost:3000
npm run dev:static     # app only, no AI (no Vercel CLI needed)
npm run check          # syntax-checks every JS file
```

## Deploy

1. Create an empty repo on GitHub (no README), then from this folder:
   ```
   git init
   git add .
   git commit -m "Initial import of Goury Parts"
   git branch -M main
   git remote add origin https://github.com/<you>/goury-parts.git
   git push -u origin main
   ```
2. On vercel.com: Add New → Project → import the repo. Framework preset: **Other**. Leave build command empty; `vercel.json` already sets the output directory to `public`. Deploy.
3. In Vercel → Project → Settings → Environment Variables, add:
   - `ANTHROPIC_API_KEY`: your Anthropic key
   - `SHOP_TOKEN`: a long random passphrase (for example 4–5 random words). Every device enters it once.
   - `ALLOWED_ORIGINS` (optional): your site URL, e.g. `https://goury-parts.vercel.app`, so other websites can't use the endpoint.

   Redeploy after adding them (Deployments → ⋯ → Redeploy).
4. In the Firebase console → Authentication → Settings → Authorized domains, add your `*.vercel.app` domain (and any custom domain). Until you do, anonymous sign-in and sync will fail on the deployed site.

Every push to `main` redeploys. The service worker fetches fresh files first, so users get new versions on their next open, with the cached copy used only when offline.

## Moving to Convex (next step)

All cloud access is concentrated in `js/01-firebase-sync.mjs`. It uses:

- `shopdata/<key>` documents for small collections (customers, staff, settings, suppliers, purchases, expenses, etc.; see `SMALL_KEYS`)
- the `parts` collection, one document per part, with a live listener
- the `staffSync` collection
- `shopdata/centralKeys` for the PIN-encrypted AI key blob
- Firebase Storage for part photos
- anonymous auth

The rest of the app talks to it through `window._…` functions and the sync helpers it exposes, so the migration plan is to write a Convex schema matching these collections, then replace this one file with a Convex version that exposes the same functions.

## How AI works now

The browser never holds the Anthropic key. `00-ai-proxy.js` catches every request the app makes to `api.anthropic.com` and sends it to `/api/claude`, without any key header. The server checks the device's shop token, adds the real key, caps `max_tokens` at 4096, and forwards the request. The first time a device uses an AI feature it asks for the shop token and remembers it.

- To lock out a device or a former staff member, change `SHOP_TOKEN` in Vercel and redeploy. Every device will be asked for the new one.
- The old "Central AI key" in AI Settings is no longer used. Delete the `shopdata/centralKeys` document in Firestore so the old encrypted key isn't left lying around, and revoke that key in the Anthropic console.
- When the app moves to Convex, `/api/claude` becomes a Convex action and the shop token becomes a per-staff login check.

## Known issues worth fixing
- The Firebase `apiKey` in `01-firebase-sync.mjs` is public by design; security comes from Firestore/Storage rules, so make sure those aren't left open.
