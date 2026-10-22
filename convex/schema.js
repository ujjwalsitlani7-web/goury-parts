// Convex schema for Goury Parts. Mirrors how the app stored data in Firestore so the
// browser code can switch over without changing the rest of the app.
// Payloads are stored as JSON strings: the app's objects use field names starting
// with "_" (e.g. _v, _serverTs), which Convex doesn't allow as document fields.
import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  // Was Firestore `shopdata/<key>`: small lists (customers, staff, settings…) as one
  // blob each, large lists (sales, orders…) split into `<key>__<n>` chunks plus a
  // `<key>__meta` entry.
  kv: defineTable({
    key: v.string(),
    data: v.optional(v.string()),   // the JSON blob
    ts: v.optional(v.number()),     // client timestamp of the write
    extra: v.optional(v.string()),  // other fields (e.g. chunks/total on __meta)
    rev: v.optional(v.number()),    // server version, bumped on every write (for caching)
  }).index("by_key", ["key"]),

  // Was Firestore `parts/<id>`: one row per part.
  parts: defineTable({
    pid: v.string(),
    json: v.string(),
    serverTs: v.number(),           // set by the server on every write, for delta sync
  }).index("by_pid", ["pid"]).index("by_serverTs", ["serverTs"]),

  // Deleted part ids, so other devices can remove them in their delta sync.
  partDeletes: defineTable({
    pid: v.string(),
    serverTs: v.number(),
  }).index("by_pid", ["pid"]).index("by_serverTs", ["serverTs"]),

  // Was Firestore `shop_records/<collection>__<recordId>`: per-record audit copies.
  records: defineTable({
    path: v.string(),               // "<collection>__<recordId>"
    coll: v.string(),
    json: v.optional(v.string()),
    deleted: v.boolean(),
    ts: v.number(),
  }).index("by_path", ["path"]).index("by_coll", ["coll"]),

  // Photos (was Firebase Storage). path like "part_photos/<id>.jpg".
  files: defineTable({
    path: v.string(),
    storageId: v.id("_storage"),
  }).index("by_path", ["path"]),
});
