import { ConvexError } from "convex/values";

// Every function requires the shop token. It's the same SHOP_TOKEN staff already
// enter for AI; set it in Convex → Settings → Environment Variables.
export function requireToken(token) {
  const want = process.env.SHOP_TOKEN;
  if (!want) throw new ConvexError("server_not_configured");
  if (typeof token !== "string" || token.length !== want.length) throw new ConvexError("bad_token");
  let diff = 0;
  for (let i = 0; i < want.length; i++) diff |= token.charCodeAt(i) ^ want.charCodeAt(i);
  if (diff !== 0) throw new ConvexError("bad_token");
}
