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
// packets whose own ACKs do not arrive. The mailbox key is an internal
// CacheService implementation detail; it is not a wire-level FIFO/sequence.

var CHUNK_TTL = 600;             // seconds
var MAX_WAIT = 90;               // below the Apps Script execution limit
var MAX_PUSH_PACKETS = 16;       // keep one request comfortably small
var MAX_POLL_PACKETS = 32;
// The mailbox is deliberately lossy: a newer batch may replace an older one
// before it is polled. KCP above this bus owns ordering and retransmission.
var POLL_SLEEP_MIN_MS = 50;
var POLL_SLEEP_MAX_MS = 250;

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

// Publish the newest opaque batch to a short-lived mailbox. There is no
// packet seq, ACK, retry, duplicate suppression, FIFO, or script lock here.
// A concurrent push may overwrite this batch; KCP retransmits what it needs.
function push_(b) {
  var packets = b.packets;
  if (!Array.isArray(packets)) return j_({error: 'packets required'});
  if (packets.length === 0) return j_({ok: true});
  if (packets.length > MAX_PUSH_PACKETS) {
    return j_({error: 'too many packets'});
  }

  var cache = CacheService.getScriptCache();
  var batch = [];
  for (var i = 0; i < packets.length; i++) {
    if (typeof packets[i] === 'string' && packets[i].length > 0) {
      batch.push(packets[i]);
    }
  }
  if (batch.length === 0) return j_({ok: true});

  // One CacheService write replaces the previous batch. This avoids the
  // global ScriptLock and the queue-index read/modify/write round trip.
  cache.put(qkey_(b.sess, b.dir, b.stream), JSON.stringify(batch), CHUNK_TTL);
  return j_({ok: true});
}

// Read and remove the newest mailbox batch. The get/remove race is
// intentionally lossy: if a push overlaps this operation, either batch may
// be dropped and KCP will retransmit missing data.
function pop_(b, maxPackets) {
  var cache = CacheService.getScriptCache();
  var qkey = qkey_(b.sess, b.dir, b.stream);
  var raw = cache.get(qkey);
  if (!raw) return [];
  cache.remove(qkey);
  try {
    var packets = JSON.parse(raw);
    if (!Array.isArray(packets)) return [];
    return packets.slice(0, maxPackets);
  } catch (ignore) {
    return [];
  }
}

function poll_(b) {
  var wait = Math.min(Math.max(parseInt(b.wait || 75, 10), 1), MAX_WAIT);
  var deadline = Date.now() + wait * 1000;
  var sleepMs = POLL_SLEEP_MIN_MS;
  while (true) {
    var packets = pop_(b, MAX_POLL_PACKETS);
    if (packets.length > 0) return j_({packets: packets});
    if (Date.now() >= deadline) return j_({packets: []});
    Utilities.sleep(sleepMs);
    sleepMs = Math.min(sleepMs + 50, POLL_SLEEP_MAX_MS);
  }
}
