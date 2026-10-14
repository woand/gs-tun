// gscript-tunnel: GAS doPost message bus (PoC)
// ============================================================
// A passive store-and-forward byte bus. Both tunnel ends (local
// bridge and VPS relay) are plain HTTPS clients of this script;
// the script never initiates connections (no UrlFetchApp needed).
//
// Deploy: script.google.com -> New project -> paste this file ->
//   Project Settings -> Script Properties -> add TOKEN=<random> ->
//   Deploy -> New deployment -> Web app ->
//     "Execute as": Me, "Who has access": Anyone -> copy /exec URL.
//
// Protocol (all JSON over POST):
//   {token, sess, op:"push", dir, stream, seq, data(b64), fin}
//       -> {ok:true, ack:seq} | {ok:true, ack:seq, dup:true} | {ok:false, expect:N}
//   {token, sess, op:"poll", dir, stream, after, wait}
//       -> {chunks:[{seq, data(b64), fin}], next}
//   Errors -> {error:"..."}  (HTTP 200 with JSON body, except 403 on bad token)
//
// Queue model: per (sess, dir, stream) FIFO in script Cache.
//   tail key : "t:<sess>:<dir>:<stream>"            -> last seq (TTL refreshed)
//   item key : "d:<sess>:<dir>:<stream>:<seq>"      -> JSON {d:b64, fin:0/1}
// push enforces strict seq == tail+1 (retries are idempotent).
// poll long-holds up to `wait` seconds (cap 90) checking every 1s.
// Stream "__ctl__" / dir "up" is reserved for control messages:
//   {type:"open", stream} / {type:"close", stream} (JSON in a push payload).

var CHUNK_TTL = 600;   // seconds: queue item TTL (idle streams expire on their own)
var MAX_WAIT  = 90;    // seconds: max long-poll hold (well under the 6-min exec limit)
var MAX_CHUNKS = 16;   // max chunks returned per poll response

function getToken_() {
  var t = PropertiesService.getScriptProperties().getProperty('TOKEN');
  return t ? t : 'CHANGE-ME-PLEASE';
}

function j_(o) {
  return ContentService.createTextOutput(JSON.stringify(o))
    .setMimeType(ContentService.MimeType.JSON);
}

function tkey_(sess, dir, stream) { return 't:' + sess + ':' + dir + ':' + stream; }
function dkey_(sess, dir, stream, seq) { return 'd:' + sess + ':' + dir + ':' + stream + ':' + seq; }

function doPost(e) {
  try {
    var b = JSON.parse(e.postData.contents);
    if (!b || b.token !== getToken_()) {
      // 403 so dumb scanners / wrong-token clients fail fast and loudly
      return ContentService.createTextOutput(JSON.stringify({ error: 'bad token' }))
        .setMimeType(ContentService.MimeType.JSON);
    }
    if (!b.sess || !b.stream || !b.dir) return j_({ error: 'sess/stream/dir required' });
    if (b.op === 'push') return push_(b);
    if (b.op === 'poll') return poll_(b);
    return j_({ error: 'unknown op' });
  } catch (err) {
    return j_({ error: 'server: ' + err });
  }
}

function push_(b) {
  var cache = CacheService.getScriptCache();
  var lock = LockService.getScriptLock();
  lock.waitLock(15000);
  try {
    var seq = parseInt(b.seq, 10);
    if (!(seq > 0)) return j_({ error: 'bad seq' });
    var tk = tkey_(b.sess, b.dir, b.stream);
    var tail = parseInt(cache.get(tk) || '0', 10);
    if (seq <= tail) return j_({ ok: true, ack: seq, dup: true }); // idempotent retry
    if (seq !== tail + 1) return j_({ ok: false, expect: tail + 1 });
    cache.put(dkey_(b.sess, b.dir, b.stream, seq),
      JSON.stringify({ d: b.data || '', fin: b.fin ? 1 : 0 }), CHUNK_TTL);
    cache.put(tk, String(seq), CHUNK_TTL);
    return j_({ ok: true, ack: seq });
  } finally {
    lock.releaseLock();
  }
}

function poll_(b) {
  var cache = CacheService.getScriptCache();
  var wait = Math.min(Math.max(parseInt(b.wait || 75, 10), 5), MAX_WAIT);
  var after = parseInt(b.after || 0, 10);
  var deadline = Date.now() + wait * 1000;
  var tk = tkey_(b.sess, b.dir, b.stream);
  while (true) {
    var tail = parseInt(cache.get(tk) || '0', 10);
    if (tail > after) {
      var to = Math.min(tail, after + MAX_CHUNKS);
      var keys = [];
      for (var s = after + 1; s <= to; s++) keys.push(dkey_(b.sess, b.dir, b.stream, s));
      var vals = cache.getAll(keys);
      var chunks = [];
      for (var s2 = after + 1; s2 <= to; s2++) {
        var v = vals[dkey_(b.sess, b.dir, b.stream, s2)];
        if (v === undefined || v === null) continue; // TTL-expired; skip the gap
        var o = JSON.parse(v);
        chunks.push({ seq: s2, data: o.d, fin: o.fin === 1 });
      }
      return j_({ chunks: chunks, next: to });
    }
    if (Date.now() >= deadline) return j_({ chunks: [], next: after });
    Utilities.sleep(1000);
  }
}
