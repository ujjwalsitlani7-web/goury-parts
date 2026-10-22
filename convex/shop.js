// All server functions the Goury Parts app calls. Written with the "generic" helpers so
// they don't depend on generated code.
import { queryGeneric as query, mutationGeneric as mutation } from "convex/server";
import { v, ConvexError } from "convex/values";
import { requireToken } from "./auth.js";

const tok = v.string();

// ---------- session ----------
export const ping = query({
  args: { token: tok },
  handler: async (_ctx, { token }) => { requireToken(token); return true; },
});

// ---------- kv (was shopdata/<key>) ----------
async function kvRow(ctx, key) {
  return await ctx.db.query("kv").withIndex("by_key", (q) => q.eq("key", key)).unique();
}
function kvOut(row) {
  return row ? { data: row.data ?? null, ts: row.ts ?? 0, extra: row.extra ?? null, rev: row.rev ?? 0 } : null;
}

export const kvGetMany = query({
  args: { token: tok, keys: v.array(v.string()) },
  handler: async (ctx, { token, keys }) => {
    requireToken(token);
    const out = {};
    for (const k of keys) out[k] = kvOut(await kvRow(ctx, k));
    return out;
  },
});

// Small: just the server versions, so devices re-download only what changed.
export const kvVersions = query({
  args: { token: tok, keys: v.array(v.string()) },
  handler: async (ctx, { token, keys }) => {
    requireToken(token);
    const out = {};
    for (const k of keys) { const r = await kvRow(ctx, k); out[k] = r ? (r.rev ?? 0) : null; }
    return out;
  },
});

export const kvSetMany = mutation({
  args: {
    token: tok,
    items: v.array(v.object({
      key: v.string(),
      data: v.optional(v.string()),
      ts: v.optional(v.number()),
      extra: v.optional(v.string()),
    })),
  },
  handler: async (ctx, { token, items }) => {
    requireToken(token);
    const revs = {};
    let rev = Date.now();
    for (const it of items) {
      const row = await kvRow(ctx, it.key);
      const r = Math.max(rev, row && row.rev ? row.rev + 1 : 0);
      const doc = { key: it.key, data: it.data, ts: it.ts, extra: it.extra, rev: r };
      if (row) await ctx.db.replace(row._id, doc);
      else await ctx.db.insert("kv", doc);
      revs[it.key] = r;
    }
    return revs;
  },
});

// ---------- parts (was parts/<id>) ----------
async function partRow(ctx, pid) {
  return await ctx.db.query("parts").withIndex("by_pid", (q) => q.eq("pid", pid)).unique();
}
function clean(obj) {
  Object.keys(obj).forEach((k) => { if (obj[k] === undefined) delete obj[k]; });
  return obj;
}
async function writePart(ctx, pid, obj, now) {
  const row = await partRow(ctx, pid);
  const json = JSON.stringify(obj);
  if (row) await ctx.db.patch(row._id, { json, serverTs: now });
  else await ctx.db.insert("parts", { pid, json, serverTs: now });
  const tomb = await ctx.db.query("partDeletes").withIndex("by_pid", (q) => q.eq("pid", pid)).unique();
  if (tomb) await ctx.db.delete(tomb._id);
}

// Upsert with merge, like Firestore setDoc(..., {merge:true}). parts: JSON strings.
export const partsUpsert = mutation({
  args: { token: tok, parts: v.array(v.string()) },
  handler: async (ctx, { token, parts }) => {
    requireToken(token);
    const now = Date.now();
    for (const s of parts) {
      const p = JSON.parse(s);
      if (!p || !p.id) continue;
      const pid = String(p.id);
      const row = await partRow(ctx, pid);
      const merged = row ? Object.assign(JSON.parse(row.json), p) : p;
      merged._serverTs = now;
      await writePart(ctx, pid, clean(merged), now);
    }
    return parts.length;
  },
});

// Like Firestore updateDoc: fails if the part doesn't exist.
export const partUpdateFields = mutation({
  args: { token: tok, pid: v.string(), fields: v.string() },
  handler: async (ctx, { token, pid, fields }) => {
    requireToken(token);
    const row = await partRow(ctx, pid);
    if (!row) throw new ConvexError("not_found");
    const now = Date.now();
    const merged = Object.assign(JSON.parse(row.json), JSON.parse(fields), { _serverTs: now });
    await writePart(ctx, pid, clean(merged), now);
  },
});

// Atomic stock change (Convex mutations are transactions).
export const partAddStock = mutation({
  args: { token: tok, pid: v.string(), delta: v.number() },
  handler: async (ctx, { token, pid, delta }) => {
    requireToken(token);
    const row = await partRow(ctx, pid);
    if (!row) throw new ConvexError("not_found");
    const now = Date.now();
    const p = JSON.parse(row.json);
    p.stock = (Number(p.stock) || 0) + delta;
    p._serverTs = now;
    await writePart(ctx, pid, p, now);
    return p.stock;
  },
});

export const partDelete = mutation({
  args: { token: tok, pid: v.string() },
  handler: async (ctx, { token, pid }) => {
    requireToken(token);
    const row = await partRow(ctx, pid);
    if (row) await ctx.db.delete(row._id);
    const now = Date.now();
    const tomb = await ctx.db.query("partDeletes").withIndex("by_pid", (q) => q.eq("pid", pid)).unique();
    if (tomb) await ctx.db.patch(tomb._id, { serverTs: now });
    else await ctx.db.insert("partDeletes", { pid, serverTs: now });
  },
});

export const partGet = query({
  args: { token: tok, pid: v.string() },
  handler: async (ctx, { token, pid }) => {
    requireToken(token);
    const row = await partRow(ctx, pid);
    return row ? row.json : null;
  },
});

// Full download, a page at a time (5,000+ parts is too much for one response).
export const partsPage = query({
  args: { token: tok, cursor: v.union(v.string(), v.null()), numItems: v.number() },
  handler: async (ctx, { token, cursor, numItems }) => {
    requireToken(token);
    const res = await ctx.db.query("parts").withIndex("by_pid")
      .paginate({ cursor, numItems: Math.min(numItems, 1000) });
    return { items: res.page.map((r) => r.json), cursor: res.continueCursor, done: res.isDone };
  },
});

// Tiny live value: changes whenever any part is written or deleted.
export const partsClock = query({
  args: { token: tok },
  handler: async (ctx, { token }) => {
    requireToken(token);
    const a = await ctx.db.query("parts").withIndex("by_serverTs").order("desc").first();
    const b = await ctx.db.query("partDeletes").withIndex("by_serverTs").order("desc").first();
    return Math.max(a ? a.serverTs : 0, b ? b.serverTs : 0);
  },
});

// Delta since a server time: changed parts + deleted ids.
export const partsSince = query({
  args: { token: tok, since: v.number() },
  handler: async (ctx, { token, since }) => {
    requireToken(token);
    const changed = await ctx.db.query("parts").withIndex("by_serverTs", (q) => q.gt("serverTs", since)).take(2000);
    const deleted = await ctx.db.query("partDeletes").withIndex("by_serverTs", (q) => q.gt("serverTs", since)).take(2000);
    let clock = since;
    changed.forEach((r) => { if (r.serverTs > clock) clock = r.serverTs; });
    deleted.forEach((r) => { if (r.serverTs > clock) clock = r.serverTs; });
    return {
      changed: changed.map((r) => r.json),
      deleted: deleted.map((r) => r.pid),
      clock,
      more: changed.length === 2000 || deleted.length === 2000,
    };
  },
});

// ---------- records (was shop_records/<coll>__<id>) ----------
export const recordsWrite = mutation({
  args: {
    token: tok,
    ops: v.array(v.object({ coll: v.string(), recordId: v.string(), json: v.union(v.string(), v.null()) })),
  },
  handler: async (ctx, { token, ops }) => {
    requireToken(token);
    const now = Date.now();
    for (const op of ops) {
      const path = op.coll + "__" + op.recordId;
      const row = await ctx.db.query("records").withIndex("by_path", (q) => q.eq("path", path)).unique();
      const doc = op.json === null
        ? { path, coll: op.coll, json: row ? row.json : undefined, deleted: true, ts: now }  // soft delete keeps last copy
        : { path, coll: op.coll, json: op.json, deleted: false, ts: now };
      if (row) await ctx.db.replace(row._id, doc);
      else await ctx.db.insert("records", doc);
    }
    return ops.length;
  },
});

export const recordGet = query({
  args: { token: tok, path: v.string() },
  handler: async (ctx, { token, path }) => {
    requireToken(token);
    const row = await ctx.db.query("records").withIndex("by_path", (q) => q.eq("path", path)).unique();
    return row ? { coll: row.coll, json: row.json ?? null, deleted: row.deleted, ts: row.ts } : null;
  },
});

// Every record of every collection, a page at a time (restore tools only).
export const recordsAllPage = query({
  args: { token: tok, cursor: v.union(v.string(), v.null()), numItems: v.number() },
  handler: async (ctx, { token, cursor, numItems }) => {
    requireToken(token);
    const res = await ctx.db.query("records").paginate({ cursor, numItems: Math.min(numItems, 1000) });
    return {
      items: res.page.map((r) => ({ path: r.path, coll: r.coll, json: r.json ?? null, deleted: r.deleted })),
      cursor: res.continueCursor, done: res.isDone,
    };
  },
});

export const recordsPage = query({
  args: { token: tok, coll: v.string(), cursor: v.union(v.string(), v.null()), numItems: v.number() },
  handler: async (ctx, { token, coll, cursor, numItems }) => {
    requireToken(token);
    const res = await ctx.db.query("records").withIndex("by_coll", (q) => q.eq("coll", coll))
      .paginate({ cursor, numItems: Math.min(numItems, 1000) });
    return {
      items: res.page.filter((r) => !r.deleted && r.json).map((r) => r.json),
      cursor: res.continueCursor, done: res.isDone,
    };
  },
});

// ---------- files (was Firebase Storage) ----------
export const uploadUrl = mutation({
  args: { token: tok },
  handler: async (ctx, { token }) => { requireToken(token); return await ctx.storage.generateUploadUrl(); },
});

export const fileSave = mutation({
  args: { token: tok, path: v.string(), storageId: v.id("_storage") },
  handler: async (ctx, { token, path, storageId }) => {
    requireToken(token);
    const row = await ctx.db.query("files").withIndex("by_path", (q) => q.eq("path", path)).unique();
    if (row) {
      if (row.storageId !== storageId) await ctx.storage.delete(row.storageId);
      await ctx.db.patch(row._id, { storageId });
    } else {
      await ctx.db.insert("files", { path, storageId });
    }
    return await ctx.storage.getUrl(storageId);
  },
});

export const fileUrl = query({
  args: { token: tok, path: v.string() },
  handler: async (ctx, { token, path }) => {
    requireToken(token);
    const row = await ctx.db.query("files").withIndex("by_path", (q) => q.eq("path", path)).unique();
    return row ? await ctx.storage.getUrl(row.storageId) : null;
  },
});

export const fileDelete = mutation({
  args: { token: tok, path: v.string() },
  handler: async (ctx, { token, path }) => {
    requireToken(token);
    const row = await ctx.db.query("files").withIndex("by_path", (q) => q.eq("path", path)).unique();
    if (row) { await ctx.storage.delete(row.storageId); await ctx.db.delete(row._id); }
  },
});
