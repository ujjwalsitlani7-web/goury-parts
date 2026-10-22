// POST /api/claude — server-side proxy for every AI feature in the app.
// The Anthropic key lives only in Vercel env vars; the browser never sees it.
//
// Env vars (Vercel → Project → Settings → Environment Variables):
//   ANTHROPIC_API_KEY  your Anthropic key (sk-ant-…)
//   SHOP_TOKEN         a long random passphrase each shop device enters once
//   ALLOWED_ORIGINS    optional, comma-separated, e.g. https://goury-parts.vercel.app
import { timingSafeEqual } from 'node:crypto';

const MAX_TOKENS_CAP = 4096;

function tokenOk(given) {
  const want = process.env.SHOP_TOKEN || '';
  if (!want || typeof given !== 'string') return false;
  const a = Buffer.from(given), b = Buffer.from(want);
  return a.length === b.length && timingSafeEqual(a, b);
}

function originOk(req) {
  const list = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
  if (!list.length) return true;
  const origin = req.headers.origin;
  return !origin || list.includes(origin);
}

function fail(res, status, message) {
  res.status(status).json({ type: 'error', error: { type: 'proxy_error', message } });
}

export default async function handler(req, res) {
  if (req.method !== 'POST') return fail(res, 405, 'Method not allowed');
  if (!originOk(req)) return fail(res, 403, 'Origin not allowed');
  if (!tokenOk(req.headers['x-shop-token'])) {
    res.setHeader('x-proxy-auth', 'failed'); // tells the app to ask for the token again
    return fail(res, 401, 'Shop token missing or wrong');
  }
  if (!process.env.ANTHROPIC_API_KEY) return fail(res, 500, 'Server has no ANTHROPIC_API_KEY set');

  const body = req.body && typeof req.body === 'object' ? req.body : null;
  if (!body || !Array.isArray(body.messages) || typeof body.model !== 'string') {
    return fail(res, 400, 'Expected { model, messages, max_tokens }');
  }
  if (!body.model.startsWith('claude-')) return fail(res, 400, 'Unsupported model');

  // Only forward the fields the app uses, and cap spend per request.
  const payload = {
    model: body.model,
    messages: body.messages,
    max_tokens: Math.min(Number(body.max_tokens) || 600, MAX_TOKENS_CAP),
  };
  if (typeof body.system === 'string' || Array.isArray(body.system)) payload.system = body.system;
  if (typeof body.temperature === 'number') payload.temperature = body.temperature;

  try {
    const upstream = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify(payload),
    });
    const text = await upstream.text();
    if (upstream.status === 401 || upstream.status === 403) {
      // The server's own Anthropic key was refused. Report it as a server problem,
      // not an auth failure, so devices don't think their shop token is wrong.
      let detail = '';
      try { detail = JSON.parse(text).error.message; } catch (_) {}
      return fail(res, 500, 'Anthropic rejected the server API key (' + upstream.status + '). Owner: check ANTHROPIC_API_KEY in Vercel.' + (detail ? ' ' + detail : ''));
    }
    res.status(upstream.status).setHeader('content-type', 'application/json').send(text);
  } catch (e) {
    fail(res, 502, 'Could not reach Anthropic: ' + (e && e.message));
  }
}

// Photos are sent as base64; allow a few MB (Vercel's hard limit is 4.5 MB).
export const config = { api: { bodyParser: { sizeLimit: '4.5mb' } } };
