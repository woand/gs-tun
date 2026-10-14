// gscript-tunnel: lossy GAS mailbox
// ============================================================
// Both tunnel ends are HTTPS clients of this script. GAS is deliberately a
// best-effort packet mailbox, not a transport protocol: packets can be lost,
// duplicated, or reordered. KCP above it supplies ordering, ACKs, retransmit,
// flow control, and congestion handling.
//
// Protocol:
//   {token, sess, op:"push", dir, stream, packets:[base64, ...]}
//       -> {ok:true}
//   {token, sess, op:"poll", dir, stream, wait}
//       -> {packets:[base64, ...]}
//
// CacheService is only a temporary mailbox. A poll destructively removes a
// batch before returning it. If the HTTP response is lost, KCP will resend
// packets whose own ACKs do not arrive. The queue index below is an internal
// CacheService implementation detail; it is not a wire-level FIFO/sequence.

var CHUNK_TTL = 600;             // seconds
var MAX_WAIT = 90;               // below the Apps Script execution limit
var MAX_PUSH_PACKETS = 16;       // keep one request comfortably small
var MAX_POLL_PACKETS = 32;
var MAX_QUEUE_PACKETS = 512;     // lossy backpressure: discard oldest entries

function getToken_() {
  var t = PropertiesService.getScriptProperties().getProperty('TOKEN');
  return t ? t : 'CHANGE-ME-PLEASE';
}

function j_(o) {
  return ContentService.createTextOutput(JSON.stringify(o))
    .setMimeType(ContentService.MimeType.JSON);
}

function qkey_(sess, dir, stream) {
  return 'q:' + sess + ':' + dir + ':' + stream;
}

function itemKey_(sess, dir, stream) {
  return 'p:' + sess + ':' + dir + ':' + stream + ':' + Utilities.getUuid();
}

function doPost(e) {
  try {
    var b = JSON.parse(e.postData.contents);
    if (!b || b.token !== getToken_()) {
      return ContentService.createTextOutput(JSON.stringify({error: 'bad token'}))
        .setMimeType(ContentService.MimeType.JSON);
    }
    if (!b.sess || !b.stream || !b.dir) return j_({error: 'sess/stream/dir required'});
    if (b.op === 'push') return push_(b);
    if (b.op === 'poll') return poll_(b);
    return j_({error: 'unknown op'});
  } catch (err) {
    return j_({error: 'server: ' + err});
  }
}

// Append opaque packets to a short-lived mailbox. There is no packet seq,
// ACK, retry, or duplicate suppression here.
function push_(b) {
  var packets = b.packets;
  if (!Array.isArray(packets)) return j_({error: 'packets required'});
  if (packets.length === 0) return j_({ok: true});
  if (packets.length > MAX_PUSH_PACKETS) {
    return j_({error: 'too many packets'});
  }

  var cache = CacheService.getScriptCache();
  var keys = [];
  var values = {};
  for (var i = 0; i < packets.length; i++) {
    if (typeof packets[i] !== 'string' || packets[i].length === 0) continue;
    var key = itemKey_(b.sess, b.dir, b.stream);
    keys.push(key);
    values[key] = packets[i];
  }
  if (keys.length === 0) return j_({ok: true});

  // Put packet values before publishing their keys. If CacheService evicts a
  // value, the poller simply drops that packet and KCP retransmits it.
  cache.putAll(values, CHUNK_TTL);

  var lock = LockService.getScriptLock();
  lock.waitLock(15000);
  try {
    var qkey = qkey_(b.sess, b.dir, b.stream);
    var queue = [];
    var raw = cache.get(qkey);
    if (raw) {
      try { queue = JSON.parse(raw); } catch (ignore) { queue = []; }
      if (!Array.isArray(queue)) queue = [];
    }
    queue = queue.concat(keys);
    var discarded = [];
    if (queue.length > MAX_QUEUE_PACKETS) {
      discarded = queue.slice(0, queue.length - MAX_QUEUE_PACKETS);
      queue = queue.slice(queue.length - MAX_QUEUE_PACKETS);
    }
    cache.put(qkey, JSON.stringify(queue), CHUNK_TTL);
    if (discarded.length > 0) cache.removeAll(discarded);
    return j_({ok: true});
  } finally {
    lock.releaseLock();
  }
}

// Atomically reserves and removes a batch from the mailbox index, then reads
// and deletes the packet values. The operation is intentionally destructive.
function pop_(b, maxPackets) {
  var cache = CacheService.getScriptCache();
  var lock = LockService.getScriptLock();
  lock.waitLock(15000);
  var keys;
  try {
    var qkey = qkey_(b.sess, b.dir, b.stream);
    var queue = [];
    var raw = cache.get(qkey);
    if (raw) {
      try { queue = JSON.parse(raw); } catch (ignore) { queue = []; }
      if (!Array.isArray(queue)) queue = [];
    }
    if (queue.length === 0) return [];
    keys = queue.splice(0, maxPackets);
    cache.put(qkey, JSON.stringify(queue), CHUNK_TTL);
  } finally {
    lock.releaseLock();
  }

  var values = cache.getAll(keys);
  var packets = [];
  for (var i = 0; i < keys.length; i++) {
    var value = values[keys[i]];
    if (value !== undefined && value !== null && value !== '') packets.push(value);
  }
  cache.removeAll(keys);
  return packets;
}

function poll_(b) {
  var wait = Math.min(Math.max(parseInt(b.wait || 75, 10), 1), MAX_WAIT);
  var deadline = Date.now() + wait * 1000;
  var sleepMs = 100;
  while (true) {
    var packets = pop_(b, MAX_POLL_PACKETS);
    if (packets.length > 0) return j_({packets: packets});
    if (Date.now() >= deadline) return j_({packets: []});
    Utilities.sleep(sleepMs);
    sleepMs = Math.min(sleepMs + 100, 500);
  }
}
