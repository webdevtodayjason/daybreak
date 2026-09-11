#!/usr/bin/env python3
"""DAYBREAK ingest + enrichment daemon.

Four daemon threads, each with its own sqlite connection (WAL lets the server
read concurrently):

  fetch    feeds.fetch_all every FETCH_INTERVAL (300s), stamps meta.last_fetch_ts
  enrich   serial queue: pending_items -> enrich_item -> mark_enriched ->
           assign_cluster -> metrics/meta; labels clusters once they reach 3 items
  device   Tiiny.device_stats every 30s -> npu/cpu/mem metrics + queue_depth,
           latest snapshot cached in meta.device_last for the server
  janitor  db.prune hourly
  vault    daily intel digests + project brief into the device Knowledge Base
  idle     hourly SITREP, then jobs.run_due() -- the deep-work scheduler
           (dossiers, regional syntheses, cluster sweeps, image backfill, the
           daily brief). Never runs while the enrichment queue has work.
  r2       offsite sync (images, digests, daily snapshot) -- SPAWNED ONLY when
           R2_* credentials are configured; otherwise the thread does not exist.

Every loop iteration is wrapped: an exception logs one line and backs off, never
kills the thread. SIGTERM/SIGINT = clean exit.

Env: TIINY_HOST, TIINY_KEY, DAYBREAK_DB (default ./daybreak.db), R2_* (optional).
Run `python3 pipeline.py --once` for a single pass of every loop (integrator check).
"""

import json
import os
import re
import signal
import sqlite3
import sys
import threading
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import db  # noqa: E402
import enrich  # noqa: E402
import feeds  # noqa: E402
import jobs  # noqa: E402
import r2  # noqa: E402
import device

DB_PATH = os.environ.get("DAYBREAK_DB") or os.path.join(BASE_DIR, "daybreak.db")

FETCH_INTERVAL = float(os.environ.get("DAYBREAK_FETCH_INTERVAL", "300"))
DEVICE_INTERVAL = 5.0   # 8s image-gen spikes fall through a 30s net (Jason caught the dead needle)
JANITOR_INTERVAL = 3600.0
# The idle thread runs at most ONE background job per pass, so this is the real ceiling
# on deep work: at 300s that is twelve slots an hour shared between six job types, and no
# per-job interval can beat it. idle_body() already returns 30.0 after a job that did
# work, so a productive run cascades; this only governs how fast it recovers from a quiet
# spell. Env-configurable like FETCH_INTERVAL and R2_INTERVAL beside it.
IDLE_INTERVAL = float(os.environ.get("DAYBREAK_IDLE_INTERVAL", "300"))
R2_INTERVAL = float(os.environ.get("DAYBREAK_R2_INTERVAL", "900"))

ENRICH_BATCH = 20
IDLE_SLEEP = 2.0           # queue empty
CLUSTER_REFRESH_S = 60.0   # how often to re-read cluster labels/counts
CLUSTER_WINDOW_S = 72 * 3600.0
LABEL_MIN_ITEMS = 3
LABEL_PER_PASS = 3

STOP = threading.Event()

_LOG_LOCK = threading.Lock()


def log(msg):
    line = "%s %s\n" % (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), msg)
    with _LOG_LOCK:
        try:
            sys.stdout.write(line)
            sys.stdout.flush()
        except Exception:
            pass


class DeviceDown(Exception):
    """Tiiny is unreachable; back off instead of burning the pending queue."""


# --------------------------------------------------------------------------- #
# Tiiny handle (lazy: a missing TIINY_KEY must not stop the fetcher from running)
# --------------------------------------------------------------------------- #

_TIINY = None
_TIINY_LOCK = threading.Lock()


def tiiny():
    global _TIINY
    with _TIINY_LOCK:
        if _TIINY is None:
            _TIINY = enrich.Tiiny()
        return _TIINY


# --------------------------------------------------------------------------- #
# meta helpers
# --------------------------------------------------------------------------- #

_META_LOCK = threading.Lock()
_META_CACHE = {}


def _meta_float(con, key, default=0.0):
    try:
        v = db.get_meta(con, key)
    except Exception:
        return default
    if v in (None, ""):
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _bump_meta(con, key, amount):
    """Accumulate a running total in meta. Only the enricher thread writes these."""
    with _META_LOCK:
        if key not in _META_CACHE:
            _META_CACHE[key] = _meta_float(con, key, 0.0)
        _META_CACHE[key] += float(amount)
        val = _META_CACHE[key]
    db.set_meta(con, key, "%d" % int(val))


def _set_embeddings(con, on):
    want = "on" if on else "off"
    with _META_LOCK:
        if _META_CACHE.get("embeddings") == want:
            return
        _META_CACHE["embeddings"] = want
    try:
        db.set_meta(con, "embeddings", want)
        log("[meta] embeddings=%s" % want)
    except Exception as exc:
        log("[meta] embeddings write failed: %s" % exc)


# --------------------------------------------------------------------------- #
# generic supervised loop
# --------------------------------------------------------------------------- #

def _spawn(name, body, interval):
    """Run body(con) forever. body may return a float to override the next sleep."""

    def run():
        con = None
        fails = 0
        dev_fails = 0
        while not STOP.is_set():
            delay = interval
            try:
                if con is None:
                    con = db.connect(DB_PATH)
                    # long busy timeout: the server holds read connections on the same file
                    try:
                        con.execute("PRAGMA busy_timeout=15000")
                    except Exception:
                        pass
                hint = body(con)
                fails = 0
                dev_fails = 0
                if hint is not None:
                    delay = float(hint)
            except DeviceDown as exc:
                dev_fails += 1
                delay = min(15.0 * (2 ** min(dev_fails, 4)), 300.0)
                log("[%s] tiiny unreachable (%s); retry in %.0fs" % (name, exc, delay))
            except sqlite3.Error as exc:
                fails += 1
                delay = min(max(interval, 5.0) * (2 ** min(fails, 5)), 600.0)
                log("[%s] db error %s: %s; reconnect, retry in %.0fs"
                    % (name, type(exc).__name__, exc, delay))
                try:
                    con.close()
                except Exception:
                    pass
                con = None
            except Exception as exc:
                fails += 1
                delay = min(max(interval, 5.0) * (2 ** min(fails, 5)), 600.0)
                log("[%s] error %s: %s; retry in %.0fs"
                    % (name, type(exc).__name__, exc, delay))
            if STOP.wait(max(0.5, delay)):
                break
        try:
            if con is not None:
                con.close()
        except Exception:
            pass
        log("[%s] stopped" % name)

    return threading.Thread(target=run, name=name, daemon=True)


# --------------------------------------------------------------------------- #
# 1. fetcher
# --------------------------------------------------------------------------- #

_prev_feed_errors = set()


def fetch_body(con):
    t0 = time.time()
    res = feeds.fetch_all(con) or {}
    new = res.get("new", 0)
    checked = res.get("checked", 0)
    errors = res.get("errors") or {}
    ts = time.time()
    db.set_meta(con, "last_fetch_ts", "%.3f" % ts)
    try:
        if new:
            db.add_event(con, "FETCH", "+%d new items from %d sources"
                         % (new, checked))
        # log feed failures on state CHANGE only — a persistently-down feed
        # spamming the ops log every cycle buries real events
        global _prev_feed_errors
        cur = set(errors)
        for name in cur - _prev_feed_errors:
            db.add_event(con, "ERROR", "feed %s DOWN: %s" % (name, str(errors[name])[:80]))
        for name in _prev_feed_errors - cur:
            db.add_event(con, "FETCH", "feed %s recovered" % name)
        _prev_feed_errors = cur
    except Exception:
        pass
    detail = ""
    if errors:
        detail = " failed=" + ",".join(sorted(errors)[:6])
    log("[fetch] new=%s checked=%s errors=%d%s in %.1fs"
        % (new, checked, len(errors), detail, ts - t0))
    return None


# --------------------------------------------------------------------------- #
# 2. enricher (serial — the device does one inference at a time)
# --------------------------------------------------------------------------- #

_cluster_labels = {}   # cluster_id -> label (enricher thread only)
_last_cluster_refresh = 0.0


def _refresh_cluster_state(con):
    """Cache existing labels so upsert_cluster never writes a NULL over one."""
    global _last_cluster_refresh
    cutoff = time.time() - CLUSTER_WINDOW_S
    rows = con.execute(
        "SELECT id, label FROM clusters WHERE COALESCE(updated_at,0) >= ? "
        "ORDER BY updated_at DESC LIMIT 500", (cutoff,)).fetchall()
    for r in rows:
        _cluster_labels[r["id"]] = r["label"]
    _last_cluster_refresh = time.time()


def _reconcile_counts(con):
    """Recompute item_count/top_severity for recently touched clusters.

    Self-healing: independent of whatever assign_cluster/upsert_cluster already
    did, so counts can never drift or double-count. Labels are untouched.
    """
    cutoff = time.time() - CLUSTER_WINDOW_S
    with con:
        con.execute(
            "UPDATE clusters SET"
            "  item_count = (SELECT COUNT(*) FROM items WHERE items.cluster_id = clusters.id),"
            "  top_severity = COALESCE((SELECT MAX(severity) FROM items"
            "                           WHERE items.cluster_id = clusters.id), 1)"
            " WHERE id IN (SELECT DISTINCT cluster_id FROM items"
            "              WHERE cluster_id IS NOT NULL AND COALESCE(enriched_at,0) >= ?)",
            (cutoff,))


def _cluster_titles(con, cluster_id, limit=6):
    rows = con.execute(
        "SELECT title FROM items WHERE cluster_id = ? "
        "ORDER BY COALESCE(published, fetched_at) DESC LIMIT ?",
        (cluster_id, limit)).fetchall()
    return [r["title"] for r in rows if r["title"]]


def _label_clusters(con):
    rows = con.execute(
        "SELECT id, top_severity FROM clusters "
        "WHERE (label IS NULL OR label = '') AND item_count >= ? "
        "ORDER BY updated_at DESC LIMIT ?",
        (LABEL_MIN_ITEMS, LABEL_PER_PASS)).fetchall()
    for r in rows:
        if STOP.is_set():
            return
        cid = r["id"]
        titles = _cluster_titles(con, cid)
        if len(titles) < 2:
            continue
        # same NPU handshake as enrichment: labeling is a chat call too, and a
        # label fired mid-image-generation kills the image with device 150004
        while not STOP.is_set():
            try:
                hold = float(db.get_meta(con, "img_hold_until", "0") or 0)
            except (TypeError, ValueError):
                hold = 0
            if time.time() >= hold:
                break
            time.sleep(1.5)
        try:
            db.set_meta(con, "enrich_busy_until", "%.3f" % (time.time() + 60))
        except Exception:
            pass
        try:
            label = enrich.label_cluster(tiiny(), titles)
        except Exception as exc:
            log("[cluster] label %s failed: %s" % (cid, exc))
            return
        finally:
            try:
                db.set_meta(con, "enrich_busy_until", "0")
            except Exception:
                pass
        if not label or not str(label).strip():
            continue
        label = " ".join(str(label).split())[:120]
        _cluster_labels[cid] = label
        try:
            db.upsert_cluster(con, cid, label, r["top_severity"] or 1, time.time())
            log("[cluster] %s labelled: %s" % (cid, label))
            db.add_event(con, "CLUSTER", "CL-%s titled: %s" % (cid, label[:100]))
        except Exception as exc:
            log("[cluster] label write %s failed: %s" % (cid, exc))


# Failure strings that mean "the call never got a usable answer out of the box",
# not "this article confused the model". These must never park an item.
_TRANSPORT_MARKERS = (
    "HTTPError", "URLError", "timed out", "TimeoutError", "socket.timeout",
    "ConnectionError", "ConnectionResetError", "ConnectionRefused",
    "RemoteDisconnected", "IncompleteRead", "BadStatusLine", "OSError",
    "RuntimeError",  # TIINY_KEY unset
)

# A model that is stopped/erroring/still downloading cannot serve chat.
_BAD_MODEL_STATUS = ("stop", "error", "fail", "download", "unload", "pending", "crash")


def _transport_error(err):
    e = str(err or "")
    return any(m in e for m in _TRANSPORT_MARKERS)


def _device_ready():
    """True only if the device answers AND the chat model is loaded on it.

    `device_stats()` alone only proves the management API is alive: with the chat
    model evicted, /api/v1/npu/status still returns 200 while every enrichment
    fails, which would burn the whole pending queue into enrich_error.
    """
    try:
        st = tiiny().device_stats()
    except Exception:
        return False
    if not st:
        return False
    models = st.get("models")
    if not models:
        # models endpoint gave us nothing; we cannot prove the model is gone, and
        # the failure was not transport-level, so fall back to the old behaviour.
        # db.mark_error's retry budget is the backstop.
        return True
    for m in models:
        if str((m or {}).get("model_id") or "").strip() == enrich.CHAT_MODEL:
            status = str((m or {}).get("status") or "").strip().lower()
            return not any(bad in status for bad in _BAD_MODEL_STATUS)
    return False


def _enrich_one(con, row):
    item_id = row["id"]
    t0 = time.time()
    err = ""
    stats_out = {}
    res = None
    try:
        res = enrich.enrich_item(tiiny(), row, stats_out=stats_out)
    except Exception as exc:
        err = "%s: %s" % (type(exc).__name__, exc)
    if not err:
        # The real reason lives in the chat stats -- enrich_item returns None, it
        # does not raise, so without this every parked row reads "empty result".
        err = str(stats_out.get("error") or "").strip() or "empty result"

    if not res:
        # Tell an outage apart from one bad article: a bad article gets parked,
        # an outage must NOT burn the whole pending queue.
        if _transport_error(err) or not _device_ready():
            raise DeviceDown(err)
        db.mark_error(con, item_id, err[:300])
        log("[enrich] #%s FAILED %s" % (item_id, err[:160]))
        db.add_event(con, "ERROR", "enrich #%s failed: %s" % (item_id, err[:120]))
        return False

    stats = res.get("stats") or {}
    emb = res.get("embedding")
    try:
        sev = int(res.get("severity") or 1)
    except (TypeError, ValueError):
        sev = 1
    sev = max(1, min(5, sev))
    countries = res.get("countries")
    if not isinstance(countries, list):
        countries = [] if countries in (None, "") else [str(countries)]

    db.mark_enriched(con, item_id, res.get("summary"), res.get("category"),
                     res.get("region"), sev, json.dumps(countries), emb)

    ts = time.time()
    ms = _num(stats.get("ms"), (ts - t0) * 1000.0)
    tps = _num(stats.get("gen_tps"), 0.0)
    tok = _num(stats.get("tokens_out"), 0.0)
    try:
        db.record_metric(con, "enrich_ms", ms, ts)
        if tps > 0:
            db.record_metric(con, "gen_tps", tps, ts)
        # tokens_out is recorded centrally in device_body from the client drain, which
        # catches every generation path rather than only this one. Writing it here as
        # well would double count enrichment.
    except Exception as exc:
        log("[enrich] metric write failed: %s" % exc)

    # tokens are drained from the client in device_body now, so the enrich path must
    # not also add them or every enrichment would be counted twice
    _bump_meta(con, "items_enriched_total", 1)
    _set_embeddings(con, emb is not None)

    cid = None
    try:
        cid = enrich.assign_cluster(con, item_id, emb)
    except Exception as exc:
        log("[cluster] assign failed for #%s: %s" % (item_id, exc))
    if cid:
        try:
            db.set_cluster(con, item_id, cid)
            db.upsert_cluster(con, cid, _cluster_labels.get(cid), sev, ts)
        except Exception as exc:
            log("[cluster] upsert %s failed: %s" % (cid, exc))

    log("[enrich] #%s S%d %s/%s cl=%s %.1ftok/s %.0fms %s"
        % (item_id, sev, res.get("region"), res.get("category"), cid, tps, ms,
           (row["title"] or "")[:70]))
    db.add_event(con, "ENRICH", "#%s S%d %s/%s %.1f tok/s — %s"
                 % (item_id, sev, res.get("region"), res.get("category"), tps,
                    (row["title"] or "")[:90]))
    return True


def _num(v, default):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    if f != f or f in (float("inf"), float("-inf")):
        return default
    return f


def enrich_body(con):
    if time.time() - _last_cluster_refresh > CLUSTER_REFRESH_S:
        _refresh_cluster_state(con)

    rows = db.pending_items(con, limit=ENRICH_BATCH)
    if not rows:
        return IDLE_SLEEP

    done = 0
    for row in rows:
        if STOP.is_set():
            break
        # NPU handshake: image generation (server.py) needs a quiet NPU — a
        # concurrent Z-Image job fails with device error 150004 while chat is
        # running. The server takes a short lease in meta.img_hold_until; we
        # yield between items and advertise our own busy window while chatting.
        while not STOP.is_set():
            try:
                hold = float(db.get_meta(con, "img_hold_until", "0") or 0)
            except (TypeError, ValueError):
                hold = 0
            if time.time() >= hold:
                break
            time.sleep(1.5)
        try:
            db.set_meta(con, "enrich_busy_until", "%.3f" % (time.time() + 120))
            db.set_meta(con, "now_doing", "ENRICHING #%s — %s"
                        % (row["id"], (row["title"] or "")[:80]))
        except Exception:
            pass
        try:
            if _enrich_one(con, row):
                done += 1
        finally:
            try:
                db.set_meta(con, "enrich_busy_until", "0")
                db.set_meta(con, "now_doing", "")
            except Exception:
                pass

    if done:
        _reconcile_counts(con)
        _refresh_cluster_state(con)
        _label_clusters(con)
    return 0.5 if done else 5.0


# --------------------------------------------------------------------------- #
# 3. device poller
# --------------------------------------------------------------------------- #

_embed_probed = False
_device_misses = 0

_DEVICE_METRICS = (
    ("npu_util", "npu_util"),
    ("npu_mem_used_mb", "npu_mem_mb"),
    ("cpu_pct", "cpu_pct"),
    ("mem_pct", "mem_pct"),
)


def device_body(con):
    global _embed_probed, _device_misses
    # Flush whatever the device client generated since the last pass, from every thread
    # and every job. This runs every 5s, so the odometer is never far behind.
    try:
        drained, drained_out, dev_ms = enrich.drain_tokens()
        if dev_ms:
            db.add_device_ms(con, dev_ms)
        if drained:
            db.add_tokens(con, drained)
            # Record to the series too, not just the odometer. tokens_out was written
            # only by the enrich path, so the sparkline showed roughly a third of the
            # truth once images, geocoding, sitreps, dossiers and the analyst brief were
            # added - the odometer said ~4,950/hr while the series said ~1,667.
            # the series tracks total processed, matching the odometer
            db.record_metric(con, "tokens_out", float(drained), time.time())
    except Exception as exc:
        log("[device] token flush failed: %s" % str(exc)[:80])
    stats = None
    try:
        stats = tiiny().device_stats()
    except Exception as exc:
        log("[device] stats error %s: %s" % (type(exc).__name__, exc))

    ts = time.time()
    if not stats:
        # every 5th miss only — a week of downtime must not flood journald
        if _device_misses % 5 == 0:
            log("[device] offline (no stats; miss #%d)" % (_device_misses + 1))
        _device_misses += 1
    else:
        if _device_misses:
            log("[device] back online after %d misses" % _device_misses)
        _device_misses = 0

    if isinstance(stats, dict) and stats:
        for src, key in _DEVICE_METRICS:
            v = stats.get(src)
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                try:
                    db.record_metric(con, key, float(v), ts)
                except Exception as exc:
                    log("[device] metric %s failed: %s" % (key, exc))
        snap = dict(stats)
        snap["ts"] = ts
        try:
            blob = json.dumps(snap, default=str)
            if len(blob) > 32000:          # keep meta rows sane
                snap.pop("models", None)
                blob = json.dumps(snap, default=str)
            db.set_meta(con, "device_last", blob)
            db.set_meta(con, "device_last_ts", "%.3f" % ts)
        except Exception as exc:
            log("[device] snapshot write failed: %s" % exc)

    try:
        pending = db.counts(con).get("pending") or 0
        db.record_metric(con, "queue_depth", float(pending), ts)
    except Exception as exc:
        log("[device] queue_depth failed: %s" % exc)

    if stats and not _embed_probed:
        _embed_probed = True
        vec = None
        try:
            vec = tiiny().embed("daybreak embedding availability probe")
        except Exception as exc:
            log("[device] embed probe error: %s" % exc)
        _set_embeddings(con, bool(vec))
    return None


# --------------------------------------------------------------------------- #
# 4. janitor
# --------------------------------------------------------------------------- #

def janitor_body(con):
    t0 = time.time()
    db.prune(con)
    log("[janitor] prune ok in %.1fs" % (time.time() - t0))
    return None


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def _handle_signal(signum, _frame):
    log("signal %s -> shutting down" % signum)
    STOP.set()


def run_once():
    """One pass of every loop, sequential. For integrator verification."""
    global _R2
    if _R2 is None:
        _R2 = r2.from_env()
    con = db.connect(DB_PATH)
    try:
        for name, body in (("fetch", fetch_body), ("device", device_body),
                           ("enrich", enrich_body), ("janitor", janitor_body),
                           ("idle", idle_body), ("r2", r2_body)):
            try:
                body(con)
            except Exception as exc:
                log("[%s] once failed %s: %s" % (name, type(exc).__name__, exc))
    finally:
        con.close()



# --------------------------------------------------------------------------- #
# 5. vault filer — daily intel digests + project brief into the device KB
#    (exercises the Tiiny Knowledge Base / Vault SDK surface with real data)
# --------------------------------------------------------------------------- #

VAULT_CHECK_S = 900.0
KB_HOST = device.host()
KB_PORT = os.environ.get("TIINY_KB_PORT", "5003")
KB_KEY = os.environ.get("TIINY_KEY", "")

ABOUT_DOC = """# DAYBREAK — project brief

DAYBREAK (https://daybreak.example.com) is a live OSINT world-news intelligence
board whose AI runs entirely on a Tiiny Pocket edge device. It is a week-long
real-world endurance test of the device under sustained production load.

How it works: fourteen free news and security feeds (BBC, Guardian, Al Jazeera,
DW, France24, Kyiv Independent, Times of Israel, CISA cyber advisories, USGS
earthquakes, ReliefWeb and more) are ingested around the clock. Every item is
processed on the Tiiny Pocket: Ornith-1.0-35B writes a one-line analysis and
assigns a category, a combatant-command region (NORTHCOM, SOUTHCOM, EUCOM,
CENTCOM, AFRICOM, INDOPACOM or GLOBAL) and a severity from S1 (low signal) to
S5 (flash/critical). Qwen3-Embedding-0.6B embeds every item and cosine
clustering fuses cross-source coverage into developing stories, which Ornith
titles. Z-Image-Turbo paints photorealistic editorial images for articles on
demand. All three models are resident simultaneously on the device NPU.

The rig: the Tiiny Pocket does all AI inference. An Orange Pi 6 Plus in the
server rack runs ingestion, the dashboard, an infrared rack camera, and a
Cloudflare tunnel that publishes the board. The backend is pure Python stdlib.

Every day DAYBREAK files an intelligence digest of the day's enriched items
into this Knowledge Base, so the vault accumulates a searchable archive of
world events as analyzed on-device. The ASK THE ARCHIVE feature on the board
performs semantic retrieval against this vault.

Built by Jason Brashear (github.com/webdevtodayjason/daybreak). Equipment:
Tiiny AI Pocket. News content belongs to the cited sources; analyses are
AI-generated on-device and can be wrong.
"""


def _kb_call(path, payload=None, raw_body=None, content_type=None, timeout=60):
    import urllib.request
    url = "http://%s:%s%s" % (KB_HOST, KB_PORT, path)
    headers = {"Authorization": "Bearer " + KB_KEY}
    data = None
    if raw_body is not None:
        data = raw_body
        headers["Content-Type"] = content_type
    elif payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def _vault_file(con, filename, text):
    """Upload + index one markdown file. Returns True on success."""
    boundary = "----daybreakvault%d" % int(time.time() * 1000)
    payload = ("--%s\r\nContent-Disposition: form-data; name=\"file\"; "
               "filename=\"%s\"\r\nContent-Type: text/markdown\r\n\r\n"
               % (boundary, filename)).encode() + text.encode() \
              + ("\r\n--%s--\r\n" % boundary).encode()
    up = _kb_call("/kb/upload", raw_body=payload,
                  content_type="multipart/form-data; boundary=" + boundary,
                  timeout=120)
    sid = up.get("source_id")
    if not up.get("success") or not sid:
        raise RuntimeError("upload rejected: %s" % str(up)[:150])
    fin = _kb_call("/kb/finalize", payload={"source_id": sid}, timeout=300)
    if not fin.get("success"):
        raise RuntimeError("finalize rejected: %s" % str(fin)[:150])
    db.add_event(con, "VAULT", "filed %s to device Knowledge Base (%d entries indexed)"
                 % (filename, fin.get("generated_entries") or 0))
    # Drop a local copy so the offsite loop can ship it. Best-effort by design:
    # the device KB is the primary home, R2 is the backup, and a full disk here
    # must not fail a digest that already landed on the device.
    try:
        r2.write_digest_copy(text, filename, db_path=DB_PATH)
    except Exception as exc:
        log("[vault] digest copy for r2 failed: %s" % str(exc)[:100])
    _bump_meta(con, "vault_digests_total", 1)
    log("[vault] filed %s (%d entries)" % (filename, fin.get("generated_entries") or 0))
    return True


def _build_digest(con, start_ts, end_ts, day_label, partial=False):
    rows = con.execute(
        "SELECT * FROM items WHERE enriched_at IS NOT NULL "
        "AND COALESCE(published, fetched_at) >= ? AND COALESCE(published, fetched_at) < ? "
        "ORDER BY severity DESC, COALESCE(published, fetched_at) DESC LIMIT 250",
        (start_ts, end_ts)).fetchall()
    if not rows:
        return None
    out = ["# DAYBREAK daily intelligence digest — %s%s" % (day_label,
           " (partial: filed at bootstrap, covers the day so far)" if partial else ""),
           "", "AI analysis performed on-device by Ornith-1.0-35B on a Tiiny Pocket. "
           "%d enriched items. Severity scale S1 (low) to S5 (flash/critical)." % len(rows), ""]
    by_region = {}
    for r in rows:
        by_region.setdefault(r["region"] or "GLOBAL", []).append(r)
    for region in ("CENTCOM", "EUCOM", "INDOPACOM", "AFRICOM", "NORTHCOM", "SOUTHCOM", "GLOBAL"):
        items = by_region.get(region)
        if not items:
            continue
        out.append("## %s (%d items)" % (region, len(items)))
        for r in items[:40]:
            out.append("- [S%s/%s] %s — %s (%s)" % (
                r["severity"], r["category"], r["title"],
                (r["summary"] or "").strip(), r["source"]))
        out.append("")
    try:
        cl = con.execute(
            "SELECT label, item_count, top_severity FROM clusters "
            "WHERE label IS NOT NULL AND updated_at >= ? ORDER BY top_severity DESC, "
            "item_count DESC LIMIT 12", (start_ts,)).fetchall()
        if cl:
            out.append("## Developing stories")
            for c in cl:
                out.append("- %s (%d reports, peak S%s)"
                           % (c["label"], c["item_count"], c["top_severity"]))
    except Exception:
        pass
    return "\n".join(out)


def vault_body(con):
    if not KB_KEY:
        return None
    now = time.time()
    # one-time bootstrap: project brief + today-so-far partial digest
    if not db.get_meta(con, "vault_bootstrapped"):
        _vault_file(con, "daybreak-project-brief.md", ABOUT_DOC)
        day = time.strftime("%Y-%m-%d", time.gmtime(now))
        # jobs.day_bounds is the ONE definition of a UTC day (calendar.timegm).
        # mktime reads the struct as LOCAL time and time.timezone is the non-DST
        # offset, so on a host in a DST zone this window was an hour out for half
        # the year and the brief and the digest for the same nominal day covered
        # different spans.
        start = jobs.day_bounds(day)[0]
        text = _build_digest(con, start, now, day, partial=True)
        if text:
            _vault_file(con, "daybreak-intel-%s-partial.md" % day, text)
        db.set_meta(con, "vault_bootstrapped", "1")
        db.set_meta(con, "vault_last_day", day)
        return None
    # daily: after 00:15Z, file the full previous UTC day once
    gm = time.gmtime(now)
    if gm.tm_hour == 0 and gm.tm_min < 15:
        return None
    yday = time.strftime("%Y-%m-%d", time.gmtime(now - 86400))
    today = time.strftime("%Y-%m-%d", time.gmtime(now))
    if db.get_meta(con, "vault_last_day") not in (None, "", yday, today):
        pass  # fallthrough files yday below
    if db.get_meta(con, "vault_last_day") != today and yday != db.get_meta(con, "vault_last_day", ""):
        start = jobs.day_bounds(yday)[0]
        text = _build_digest(con, start, start + 86400, yday)
        if text:
            _vault_file(con, "daybreak-intel-%s.md" % yday, text)
        db.set_meta(con, "vault_last_day", today)
    return None



# --------------------------------------------------------------------------- #
# 6. idle work — hourly SITREP, then the jobs.py deep-work scheduler.
#    (The per-day image cap now lives in jobs.IMAGE_DAILY_CAP / DAYBREAK_IMAGE_DAILY_CAP;
#     the old inline _autoimage_one moved to jobs.job_image.)
# --------------------------------------------------------------------------- #

SITREP_INTERVAL_S = 3600.0
IMAGES_DIR = os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "images")


def _sitrep_due(con):
    try:
        last = float(db.get_meta(con, "latest_sitrep_ts", "0") or 0)
    except (TypeError, ValueError):
        last = 0
    return time.time() - last >= SITREP_INTERVAL_S


SITREP_LEVELS = ("RED", "AMBER", "GREEN")


def _sitrep_cards(raw):
    """Pull SITREP cards out of a labelled-line reply.

    Deliberately not nested JSON. Ornith reasons before it answers and that reasoning
    counts against max_tokens, so a nested array of objects reliably spent the budget
    deliberating about the schema. Flat labelled lines are cheap to emit, survive being
    buried in reasoning, and a malformed card can be dropped without losing the rest.
    """
    cards, cur = [], None
    for line in str(raw or "").splitlines():
        line = line.strip()
        if not line:
            continue
        head, _, rest = line.partition(":")
        key, rest = head.strip().upper(), rest.strip()
        if key == "CARD":
            if cur and cur.get("situation"):
                cards.append(cur)
            aor, _, lvl = rest.partition("/")
            lvl = lvl.strip().upper()
            cur = {"aor": (aor.strip().upper() or "GLOBAL")[:12],
                   "level": lvl if lvl in SITREP_LEVELS else "AMBER",
                   "title": "", "situation": "", "response": "", "forward": "",
                   "refs": []}
        elif cur is not None and key == "REFS":
            ids = []
            for tok in re.split(r"[^0-9]+", rest):
                if tok.isdigit():
                    ids.append(int(tok))
            cur["refs"] = ids[:8]
        elif cur is not None and key in ("TITLE", "SITUATION", "RESPONSE", "FORWARD"):
            cur[key.lower()] = " ".join(rest.split())[:400]
    if cur and cur.get("situation"):
        cards.append(cur)
    # A card with no forward look is still useful; one with no situation is not.
    return [c for c in cards if c["situation"]][:6]


def _write_sitrep(con):
    rows = con.execute(
        "SELECT id, title, summary, region, category, severity, url, source, countries, "
        "place, lat, lon FROM items "
        "WHERE enriched_at IS NOT NULL AND COALESCE(published, fetched_at) >= ? "
        "ORDER BY severity DESC, COALESCE(published, fetched_at) DESC LIMIT 30",
        (time.time() - 6 * 3600,)).fetchall()
    if len(rows) < 5:
        return
    # Ids go into the prompt so each card can cite the wire items behind it. Without
    # them a card is an assertion the reader cannot check; with them it is a claim with
    # its sources attached, which is the difference between a summary and a report.
    lines = ["#%d [S%s/%s/%s] %s: %s" % (r["id"], r["severity"], r["region"],
             r["category"], r["title"], (r["summary"] or "")[:140]) for r in rows]
    by_id = {r["id"]: r for r in rows}

    # The RED/AMBER/GREEN structure is the standard incident-reporting frame, and it
    # fits wire traffic well: situation is the event, response is what the actors are
    # doing about it, forward look is the trajectory. A watch officer reads three short
    # fields per theatre far faster than one paragraph covering all of them.
    system = (
        "You are the watch officer on an OSINT desk writing the hourly SITREP from the "
        "wire items given. Cover the 3 to 5 most significant developments, most severe "
        "first, at most one card per theatre.\n\n"
        "Reply as BLUF followed by cards, exactly in this form and nothing else:\n\n"
        "BLUF: one sentence, the single most important thing a commander must know\n"
        "CARD: <AOR>/<RED|AMBER|GREEN>\n"
        "TITLE: five to nine words naming the development\n"
        "SITUATION: what is happening and where, 1-2 plain sentences\n"
        "RESPONSE: what the actors involved are actually doing about it, 1 sentence. "
        "If nobody is visibly responding, say so.\n"
        "FORWARD: what is expected next and over what horizon, 1 sentence\n"
        "REFS: the # numbers of the wire items this card is based on, comma separated\n\n"
        "Repeat CARD/TITLE/SITUATION/RESPONSE/FORWARD per development. "
        "RED means strategically significant and worsening or capacity exceeded. "
        "AMBER means moderate impact being managed. GREEN means limited impact or "
        "improving. AOR is one of NORTHCOM SOUTHCOM EUCOM CENTCOM AFRICOM INDOPACOM "
        "GLOBAL. Plain declarative prose. No markdown, no preamble.")

    # One string field, not a nested structure: chat_json is the proven path here, and
    # asking it for an array of card objects is what burns the reasoning budget. The
    # labelled lines live inside that one string and are parsed in Python.
    obj, _stats = tiiny().chat_json(
        system + '\n\nReturn ONLY JSON: {"report": "<the whole report, newlines as \\n>"}',
        "\n".join(lines), max_tokens=2200)
    res = (obj or {}).get("report")

    cards = _sitrep_cards(res)
    bluf = ""
    for line in str(res or "").splitlines():
        if line.strip().upper().startswith("BLUF:"):
            bluf = " ".join(line.split(":", 1)[1].split())[:300]
            break

    if not cards:
        # Never regress: if the structured form did not come back, keep whatever prose
        # the model produced so the panel still says something.
        prose = " ".join(str(res or "").split())[:1900]
        if not prose:
            return
        db.set_meta(con, "latest_sitrep", prose)
        db.set_meta(con, "latest_sitrep_ts", "%.3f" % time.time())
        log("[sitrep] filed unstructured (%d chars from %d items)" % (len(prose), len(rows)))
        return

    # Resolve the citations here rather than in the browser: the client only holds the
    # most recent slice of the wire, and a card may well cite something that has already
    # scrolled out of it.
    for c in cards:
        srcs = []
        for rid in c.get("refs") or []:
            r = by_id.get(rid)
            if r is None:
                continue
            srcs.append({"id": r["id"], "title": r["title"], "source": r["source"],
                         "url": r["url"], "severity": r["severity"],
                         "region": r["region"], "category": r["category"],
                         # the enricher already tagged these; they become the map pins
                         "countries": r["countries"],
                         # a real coordinate when the geocoder found one, so the pin
                         # lands on the city rather than the middle of the country
                         "place": r["place"], "lat": r["lat"], "lon": r["lon"]})
        c["sources"] = srcs
        c.pop("refs", None)

    payload = {"dtg": time.strftime("%d%H%MZ %b %y", time.gmtime()).upper(),
               "bluf": bluf, "cards": cards, "items": len(rows)}
    db.set_meta(con, "latest_sitrep_json", json.dumps(payload))
    # Flattened prose kept for the RSS feed and anything else reading the old key.
    flat = ((bluf + " ") if bluf else "") + " ".join(
        "%s: %s %s" % (c["aor"], c["situation"], c["forward"]) for c in cards)
    db.set_meta(con, "latest_sitrep", " ".join(flat.split())[:1900])
    db.set_meta(con, "latest_sitrep_ts", "%.3f" % time.time())
    db.add_event(con, "SITREP", "watch officer filed hourly SITREP (%d cards, %d wire items)"
                 % (len(cards), len(rows)))
    log("[sitrep] filed %d cards from %d items" % (len(cards), len(rows)))


_JOB_CTX = None
_JOB_CTX_LOCK = threading.Lock()


def job_ctx():
    """The host services jobs.py runs against: our Tiiny handle, our log, our STOP.

    Built once. Sharing the Tiiny handle matters — it is the same serial device.
    """
    global _JOB_CTX
    with _JOB_CTX_LOCK:
        if _JOB_CTX is None:
            _JOB_CTX = jobs.Ctx(tiiny=tiiny, log=log, stop=STOP,
                                images_dir=IMAGES_DIR, db_path=DB_PATH)
        return _JOB_CTX


def idle_body(con):
    """Deep work while the wire is quiet: SITREP first, then the job scheduler.

    The hourly SITREP stays inline (it is the board's headline panel and must not
    queue behind a 90-second dossier). Everything else — entity dossiers, regional
    syntheses, the cluster sweep, image backfill and the daily brief — is
    jobs.run_due, which picks at most ONE job per pass, re-checks the pending
    queue itself and takes the NPU lease around every device call.
    """
    if not KB_KEY:
        return None
    # only work when the enrichment queue is quiet — enrichment always wins
    pending = 0
    try:
        pending = int((db.counts(con) or {}).get("pending") or 0)
    except Exception:
        pass
    if pending > 0:
        return 60.0
    if _sitrep_due(con):
        while not STOP.is_set():
            try:
                hold = float(db.get_meta(con, "img_hold_until", "0") or 0)
            except (TypeError, ValueError):
                hold = 0
            if time.time() >= hold:
                break
            time.sleep(2)
        try:
            db.set_meta(con, "enrich_busy_until", "%.3f" % (time.time() + 120))
            db.set_meta(con, "now_doing", "WRITING HOURLY SITREP — ORNITH-35B")
            _write_sitrep(con)
        finally:
            try:
                db.set_meta(con, "enrich_busy_until", "0")
                db.set_meta(con, "now_doing", "")
            except Exception:
                pass
        return 30.0

    res = jobs.run_due(con, job_ctx()) or {}
    if res.get("ran"):
        # one line per job run; the oplog carries the detail for the board
        log("[jobs] %s -> %s" % (res["ran"], json.dumps(
            {k: v for k, v in res.items() if k not in ("ran", "due_count")},
            default=str)[:220]))
        # a job that did real work leaves more due behind it — come back sooner
        return 30.0 if res.get("did_work") else None
    return None


# --------------------------------------------------------------------------- #
# 7. offsite sync — only exists when R2 is configured
# --------------------------------------------------------------------------- #

_R2 = None


def r2_body(con):
    """One bounded offsite pass: new images, new digests, the daily snapshot.

    No NPU involvement, so this is deliberately NOT gated on the enrichment
    queue — it is network and disk only. r2.sync_all runs each leg independently
    and never raises.
    """
    if _R2 is None:
        return 3600.0
    res = r2.sync_all(con, DB_PATH, IMAGES_DIR, _R2, log=log) or {}
    shipped = 0
    parts = []
    for leg in ("images", "digests", "snapshot"):
        out = res.get(leg) or {}
        if not isinstance(out, dict):
            continue
        if out.get("error"):
            parts.append("%s=ERR" % leg)
            continue
        n = int(out.get("uploaded") or 0)
        shipped += n
        parts.append("%s=%d" % (leg, n))
    if shipped:
        db.add_event(con, "R2", "offsite sync shipped %d object(s) to R2 (%s)"
                     % (shipped, ", ".join(parts)))
        log("[r2] %s" % ", ".join(parts))
    try:
        # r2_last_sync_ts means "a pass completed", NOT "a pass shipped something".
        # Stamping it only inside `if shipped` meant a healthy loop in its steady
        # state (nothing new to upload) let its own freshness field age forever --
        # indistinguishable on the board from an offsite backup that has been dead
        # for a week, which is the one thing this field exists to tell an operator.
        # r2_last_ship_ts carries "when something actually moved".
        db.set_meta(con, "r2_last_sync_ts", "%.3f" % time.time())
        if shipped:
            db.set_meta(con, "r2_last_ship_ts", "%.3f" % time.time())
            db.set_meta(con, "r2_objects_total", "%d" % (
                int(float(db.get_meta(con, "r2_objects_total", "0") or 0)) + shipped))
    except Exception:
        pass
    return None


def main():
    global _R2
    log("daybreak pipeline start db=%s tiiny=%s"
        % (DB_PATH, device.host()))
    if not os.environ.get("TIINY_KEY"):
        log("WARNING TIINY_KEY unset — enrichment will fail until it is set")

    # Offsite sync is opt-in: no R2_* env, no thread, no DB writes, nothing.
    _R2 = r2.from_env()
    if _R2 is None:
        gaps = r2.missing_env()
        if gaps and len(gaps) < 4:
            # a PARTIAL config is a typo in /etc/daybreak.env, not a decision
            log("WARNING R2 partially configured — offsite sync OFF, missing: %s"
                % ", ".join(gaps))
        else:
            log("r2 offsite sync not configured (inert)")
    else:
        log("r2 offsite sync armed: bucket=%s every %.0fs" % (_R2.bucket, R2_INTERVAL))

    try:
        con = db.connect(DB_PATH)
        db.set_meta(con, "pipeline_started_ts", "%.3f" % time.time())
        con.close()
    except Exception as exc:
        log("startup db init failed %s: %s (threads will retry)"
            % (type(exc).__name__, exc))

    if "--once" in sys.argv[1:]:
        run_once()
        return 0

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _handle_signal)
        except (ValueError, OSError):
            pass

    threads = [
        _spawn("fetch", fetch_body, FETCH_INTERVAL),
        _spawn("enrich", enrich_body, IDLE_SLEEP),
        _spawn("device", device_body, DEVICE_INTERVAL),
        _spawn("janitor", janitor_body, JANITOR_INTERVAL),
        _spawn("vault", vault_body, VAULT_CHECK_S),
        _spawn("idle", idle_body, IDLE_INTERVAL),
    ]
    if _R2 is not None:
        threads.append(_spawn("r2", r2_body, R2_INTERVAL))
    for th in threads:
        th.start()
    peers = device.prove_shared("pipeline", quiet=True)
    log("device %s; locks %s; peers visible: %s"
        % (device.host(), device.LOCK_DIR, ", ".join(sorted(peers)) or "none yet"))
    log("threads up: %s" % ", ".join(t.name for t in threads))

    while not STOP.wait(1.0):
        pass

    for th in threads:
        th.join(timeout=10.0)
    # The enricher can be parked in a 20-60s device call when SIGTERM lands. It is a
    # daemon thread so the process still exits, but say so: otherwise journald just
    # shows three of four threads stopping and an operator cannot tell a clean drain
    # from a cut-off inference. (Unit gives us TimeoutStopSec=45 either way.)
    stragglers = [t.name for t in threads if t.is_alive()]
    if stragglers:
        log("abandoning mid-flight thread(s): %s (daemon; no write in progress)"
            % ", ".join(stragglers))
    log("daybreak pipeline stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
