#!/usr/bin/env python3
"""DAYBREAK read-only HTTP API + static host + camera proxy.

stdlib ThreadingHTTPServer on PORT (default 8811). One sqlite connection per
request (WAL, read-only usage) so nothing is shared across handler threads.

Routes:
  GET /            static/index.html
  GET /healthz     {"ok":true,...}
  GET /api/items?region&category&since&limit
  GET /api/clusters
  GET /api/stats
  GET /cam.mjpg    streaming proxy -> CAM_URL/stream   (fast 503 when down)
  GET /cam.jpg     snapshot proxy  -> CAM_URL/snapshot (fast 503 when down)

The Tiiny API key never appears here; the browser only ever talks to /api/*.
`python3 server.py --selfcheck` boots on an ephemeral port, hits every route and
prints the status codes.
"""

import http.client
import json
import os
import platform
import re
import shutil
import signal
import socket
import sqlite3
import sys
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import db  # noqa: E402
import device
from version import VERSION

PORT = int(os.environ.get("PORT", "8811"))
# Loopback by default: the board is unauthenticated and the cloudflared tunnel is the
# only intended path in (deploy/TUNNEL.md). Set BIND=0.0.0.0 to expose it on the LAN.
BIND = os.environ.get("BIND", "127.0.0.1")
DB_PATH = os.environ.get("DAYBREAK_DB") or os.path.join(BASE_DIR, "daybreak.db")
CAM_URL = (os.environ.get("CAM_URL") or "http://127.0.0.1:8812").rstrip("/")
TIINY_BASE = device.base_url()
IMAGE_MODEL = os.environ.get("IMAGE_MODEL", "Tongyi-MAI/Z-Image-Turbo")
PUBLIC_BASE = (os.environ.get("DAYBREAK_PUBLIC_BASE") or "https://daybreak.example.com").rstrip("/")
SITE_DESC_FALLBACK = "Daily world-news brief, written and spoken on a Tiiny Pocket edge device."
IMAGES_DIR = os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "images")
INDEX_PATH = os.path.join(BASE_DIR, "static", "index.html")

DOC_KINDS = {"dossier", "synthesis", "brief"}
REGIONS = {"NORTHCOM", "SOUTHCOM", "EUCOM", "CENTCOM", "AFRICOM", "INDOPACOM", "GLOBAL"}
CATEGORIES = {"conflict", "terrorism", "cyber", "diplomacy", "economy", "disaster",
              "health", "crime", "politics", "tech", "energy"}

SERIES_KEYS = ("npu_util", "gen_tps", "queue_depth")
METRIC_KEYS = ("npu_util", "npu_mem_mb", "cpu_pct", "mem_pct",
               "gen_tps", "queue_depth", "enrich_ms", "tokens_out")

CAM_CONNECT_TIMEOUT = 2.0    # fast 503 when ustreamer is down
CAM_READ_TIMEOUT = 15.0      # stalled camera drops the stream instead of pinning a thread
CAM_FAIL_COOLDOWN = 3.0      # breaker: skip the connect entirely right after a failure
CAM_MAX_STREAMS = 4
CAM_MAX_SNAPSHOT = 8 * 1024 * 1024
DEVICE_STALE_S = 120.0

_LOG_LOCK = threading.Lock()


def log(msg):
    line = "%s %s\n" % (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), msg)
    with _LOG_LOCK:
        try:
            sys.stdout.write(line)
            sys.stdout.flush()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

_index_lock = threading.Lock()
_index_cache = {"key": None, "data": b""}


def read_index():
    try:
        st = os.stat(INDEX_PATH)
    except OSError:
        return None
    key = (st.st_mtime_ns, st.st_size)
    with _index_lock:
        if _index_cache["key"] != key:
            try:
                with open(INDEX_PATH, "rb") as fh:
                    _index_cache["data"] = fh.read()
            except OSError:
                return None
            _index_cache["key"] = key
        return _index_cache["data"]


_schema_ready = False
_schema_lock = threading.Lock()


# Stories in flight, so a second click does not start a second write of the same item.
_STORY_RUNNING = set()
_STORY_LOCK = threading.Lock()


def open_db():
    """A connection per request, with the schema applied exactly ONCE per process.

    db.connect() executescripts the whole of schema.sql and runs _migrate on every
    call. That was cheap when the schema was small; it now creates a table and six
    indexes -- two of them expression indexes over COALESCE(published, fetched_at)
    across the entire items table -- plus a conditional DROP/CREATE INDEX. Doing
    that inside an HTTP handler means the first post-deploy request builds every
    index while holding the write lock with busy_timeout=30000, and every request
    after it re-parses the script. The cost scales with the archive, which is now
    unbounded. So: full connect once, bare connect thereafter.
    """
    global _schema_ready
    if not _schema_ready:
        with _schema_lock:
            if not _schema_ready:
                con = db.connect(DB_PATH)
                _schema_ready = True
                try:
                    con.execute("PRAGMA busy_timeout=5000")
                except Exception:
                    pass
                return con
    con = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("PRAGMA busy_timeout=5000")
    except Exception:
        pass
    return con


# Kept as the bound on how long generate_image() will queue for the real lock. No
# browser sits on a seven-minute request, so a device held longer than this answers 202
# with a retry hint and the client comes back.
IMG_CHAT_WAIT_S = 90.0


def rget(row, key, default=None):
    try:
        v = row[key]
    except (IndexError, KeyError):
        return default
    return default if v is None else v


def as_float(v, default=None):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    if f != f:
        return default
    return f


def as_int(v, default=None):
    f = as_float(v, None)
    return default if f is None else int(f)


def json_countries(v):
    if not v:
        return []
    try:
        out = json.loads(v)
    except (TypeError, ValueError):
        return []
    if isinstance(out, list):
        return [str(x) for x in out if x is not None][:12]
    return []


_cam_lock = threading.Lock()
_cam_fail_until = 0.0
_cam_slots = threading.BoundedSemaphore(CAM_MAX_STREAMS)
# Long-lived connections each pin a handler thread, so they are capped like the camera.
LEASE_MAX_STREAMS = 6
LEASE_STREAM_MAX_S = 900.0
_lease_slots = threading.BoundedSemaphore(LEASE_MAX_STREAMS)


# On-click article imagery via the device's Z-Image-Turbo. One generation at a
# time (the NPU is shared with enrichment); results cached forever on disk so a
# popular story costs one generation. Lock holders release in finally, always.
_IMG_LOCK = threading.Lock()


def image_path(item_id):
    return os.path.join(IMAGES_DIR, "%d.png" % item_id)


def generate_image(item_id, title, summary):
    """Blocking call to the device's native image route. Returns (bytes|None, err|None).

    The native /v1/image/generate returns raw image bytes on success and a small
    JSON {"code","message"} on device errors. seed = item id, so a story's image
    is reproducible. The device fails with code 150004 when the NPU is busy with
    chat — callers must hold the img_hold lease first (see r_item_image)."""
    import urllib.request
    # "headline/news" wording invites the model to paint signage; describe the
    # SCENE instead and ban writing surfaces in the negative prompt
    prompt = (
        "Photorealistic documentary photograph of the scene: %s. %s "
        "Dramatic natural lighting, cinematic composition, shallow depth of "
        "field." % (title, (summary or "")[:300]))
    body = json.dumps({
        "model": IMAGE_MODEL, "prompt": prompt,
        "negative_prompt": "text, letters, words, signage, signs, billboards, posters, newspaper, captions, watermark, subtitles, logos, low quality",
        # ponytail: 512x512 is the ONLY resolution this device generates (bug #039);
        # the drawer crops it to 16:9 with object-fit
        "width": 512, "height": 512, "seed": int(item_id) % 2147483647,
        "steps": 8}).encode()
    req = urllib.request.Request(
        TIINY_BASE + "/v1/image/generate", data=body,
        headers={"Authorization": "Bearer " + device.key(),
                 "Content-Type": "application/json"})
    # THE collision this whole exercise is about: a reader clicking an article fires
    # this from the server process while the pipeline may be 200 seconds into a story.
    # The timestamp scheme could only ask it to wait politely; this one makes it.
    #
    # But the wait is BOUNDED, and deliberately not the module default. This runs inside
    # an HTTP handler with a reader on the other end. The whole design of this endpoint
    # is to answer honestly and let the client poll back rather than hold a browser
    # request open -- see IMG_CHAT_WAIT_S. A desk report holds the device for minutes,
    # and the default lease wait would sit on the socket for as long as ten.
    try:
        with device.lease("%s on-demand image #%s" % (device.UNIT, item_id),
                          wait=IMG_CHAT_WAIT_S):
            with urllib.request.urlopen(req, timeout=300) as resp:
                ctype = resp.headers.get("Content-Type", "")
                raw = resp.read()
    except device.DeviceBusy:
        return None, "DEVICE_BUSY"
    except Exception as exc:
        return None, str(exc)[:200]
    if "image" in ctype and raw[:1] != b"{":
        return raw, None
    try:
        err = json.loads(raw)
        return None, "device %s: %s" % (err.get("code"), err.get("message"))
    except Exception:
        return None, "unexpected response (%s, %d bytes)" % (ctype, len(raw))


RERANK_MODEL = os.environ.get("RERANK_MODEL", "Qwen/Qwen3-Reranker-0.6B")


def rerank_passages(question, results, keep=12):
    """Reorder vault passages by cross-encoder relevance. Best-effort: any
    failure (model not loaded, timeout) returns the input order untouched."""
    import urllib.request
    docs = []
    for r in results[:keep]:
        t = str(r.get("lossless_restatement") or "")[:1200]
        if t:
            docs.append((r, t))
    if len(docs) < 2:
        return results
    body = json.dumps({"model": RERANK_MODEL, "query": question,
                       "documents": [d for _, d in docs]}).encode()
    req = urllib.request.Request(
        TIINY_BASE + "/v1/rerank", data=body,
        headers={"Authorization": "Bearer " + device.key(),
                 "Content-Type": "application/json"})
    try:
        with device.lease("%s rerank" % device.UNIT):
            with urllib.request.urlopen(req, timeout=45) as resp:
                scored = json.loads(resp.read()).get("results") or []
    except Exception as exc:
        log("[rerank] unavailable (%s); keeping vault order" % str(exc)[:80])
        return results
    order = []
    for s_ in scored:
        i = s_.get("index")
        if isinstance(i, int) and 0 <= i < len(docs):
            row = dict(docs[i][0])
            row["_rerank"] = round(float(s_.get("relevance_score") or 0), 4)
            order.append(row)
    if not order:
        return results
    tail = results[len(docs):]
    return order + list(tail)


# Host (Orange Pi) stats, read from local /proc//sys — the server runs on the box
# it reports on. Identity is immutable per boot; cpu% needs a previous /proc/stat
# sample, kept in _HOST. Every field degrades to None off-Linux (dev on macOS).
_HOST = {"ident": None, "stat": None, "lock": threading.Lock()}


def _read(path):
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return None


def _host_ident():
    ident = {"model": None, "soc": None, "os": None, "arch": platform.machine(),
             "cores": os.cpu_count(), "mem_total_mb": None, "hostname": socket.gethostname()}
    # HOST_MODEL env wins (marketing name), then device-tree, then ACPI DMI —
    # this board boots ACPI, where DMI reports the CIX reference design name
    ident["model"] = os.environ.get("HOST_MODEL") or None
    if not ident["model"]:
        raw = _read("/proc/device-tree/model")
        if raw:
            ident["model"] = raw.replace("\x00", "").strip()
    if not ident["model"]:
        raw = _read("/sys/class/dmi/id/product_name")
        if raw:
            ident["model"] = raw.strip()
    raw = _read("/proc/device-tree/compatible")
    if raw:
        # e.g. "orangepi,6-plus\0cix,sky1\0" — last entry names the SoC family
        parts = [p for p in raw.split("\x00") if p]
        if parts:
            ident["soc"] = parts[-1].replace(",", " ").upper()
    if not ident["soc"]:
        raw = _read("/sys/class/dmi/id/product_name")
        if raw and "cix" in raw.lower():
            ident["soc"] = raw.strip().upper()
    raw = _read("/etc/os-release")
    if raw:
        for line in raw.splitlines():
            if line.startswith("PRETTY_NAME="):
                ident["os"] = line.split("=", 1)[1].strip().strip('"')
    raw = _read("/proc/meminfo")
    if raw:
        for line in raw.splitlines():
            if line.startswith("MemTotal:"):
                ident["mem_total_mb"] = round(int(line.split()[1]) / 1024)
                break
    return ident


def _host_cpu_pct():
    raw = _read("/proc/stat")
    if not raw:
        return None
    parts = raw.splitlines()[0].split()[1:]
    vals = [int(x) for x in parts[:8]]
    total = sum(vals)
    idle = vals[3] + (vals[4] if len(vals) > 4 else 0)
    with _HOST["lock"]:
        prev = _HOST["stat"]
        _HOST["stat"] = (total, idle)
    if not prev or total <= prev[0]:
        return None
    dt, di = total - prev[0], idle - prev[1]
    return round(100.0 * (dt - di) / dt, 1) if dt > 0 else None


try:
    import devtherm
except Exception:                        # thermals are a nicety, never a dependency
    devtherm = None

try:
    import leasewatch
except Exception:                        # same: the board must run without it
    leasewatch = None


def host_stats():
    with _HOST["lock"]:
        if _HOST["ident"] is None:
            _HOST["ident"] = _host_ident()
        ident = _HOST["ident"]
    out = {"ident": ident, "cpu_pct": _host_cpu_pct(), "mem_pct": None,
           "mem_used_mb": None, "temp_c": None, "load1": None,
           "disk_used_gb": None, "disk_total_gb": None, "uptime_s": None}
    raw = _read("/proc/meminfo")
    if raw:
        mi = {}
        for line in raw.splitlines():
            k = line.split(":")[0]
            if k in ("MemTotal", "MemAvailable"):
                mi[k] = int(line.split()[1])
        if "MemTotal" in mi and "MemAvailable" in mi and mi["MemTotal"]:
            used = mi["MemTotal"] - mi["MemAvailable"]
            out["mem_used_mb"] = round(used / 1024)
            out["mem_pct"] = round(100.0 * used / mi["MemTotal"], 1)
    temps = []
    zones = []
    try:
        for zone in sorted(os.listdir("/sys/class/thermal")):
            if zone.startswith("thermal_zone"):
                raw = _read("/sys/class/thermal/%s/temp" % zone)
                name = (_read("/sys/class/thermal/%s/type" % zone) or zone).strip()
                if raw and raw.strip().lstrip("-").isdigit():
                    t = int(raw.strip()) / 1000.0
                    temps.append(t)
                    zones.append({"name": name[:24], "c": round(t, 1)})
    except OSError:
        pass
    if temps:
        out["temp_c"] = round(max(temps), 1)
    out["temp_zones"] = zones[:12]

    # Clock domains, so the Orange Pi tab can show the same dials as the device tab.
    # The Pi enumerates its engines through ACPI rather than device-tree, so the
    # devfreq nodes are named by ACPI HID (CIXH3010 is the VPU, driver amvx_dev)
    # rather than by an address-and-function string. Map the ones we can name.
    ACPI_NAMES = {"CIXH3010": "vpu", "CIXH5000": "gpu"}
    clocks = {}
    try:
        for node in sorted(os.listdir("/sys/class/devfreq")):
            base = "/sys/class/devfreq/" + node
            hid = node.split(":")[0]
            name = ACPI_NAMES.get(hid) or node.split(".")[-1]
            try:
                cur = int(open(base + "/cur_freq").read().strip())
                mn = int(open(base + "/min_freq").read().strip())
                mx = int(open(base + "/max_freq").read().strip())
            except (OSError, ValueError):
                continue
            clocks[name] = {"cur_mhz": cur // 1000000, "min_mhz": mn // 1000000,
                            "max_mhz": mx // 1000000, "node": node}
    except OSError:
        pass
    out["clocks"] = clocks

    cpus = []
    try:
        for pol in sorted(os.listdir("/sys/devices/system/cpu/cpufreq")):
            if not pol.startswith("policy"):
                continue
            base = "/sys/devices/system/cpu/cpufreq/" + pol
            try:
                cur = int(open(base + "/scaling_cur_freq").read().strip()) // 1000
                mx = int(open(base + "/cpuinfo_max_freq").read().strip()) // 1000
                aff = open(base + "/affected_cpus").read().split()
            except (OSError, ValueError):
                continue
            cpus.append({"policy": pol.replace("policy", "p"), "cur_mhz": cur,
                         "max_mhz": mx, "cpus": len(aff)})
    except OSError:
        pass
    cpus.sort(key=lambda c: -c["max_mhz"])
    out["cpu_clocks"] = cpus
    # drive identity
    model = _read("/sys/block/nvme0n1/device/model")
    out["disk_model"] = model.strip() if model else None
    # network: default-route iface, addr, link speed, live rx/tx rates
    out["net"] = None
    try:
        iface = None
        raw = _read("/proc/net/route") or ""
        for line in raw.splitlines()[1:]:
            f = line.split()
            if len(f) > 1 and f[1] == "00000000":
                iface = f[0]
                break
        if iface:
            rx = int(_read("/sys/class/net/%s/statistics/rx_bytes" % iface) or 0)
            tx = int(_read("/sys/class/net/%s/statistics/tx_bytes" % iface) or 0)
            now = time.time()
            with _HOST["lock"]:
                prev = _HOST.get("net")
                _HOST["net"] = (now, rx, tx)
            rate_rx = rate_tx = None
            if prev and now > prev[0]:
                dt = now - prev[0]
                rate_rx = max(0, (rx - prev[1]) / dt)
                rate_tx = max(0, (tx - prev[2]) / dt)
            speed = _read("/sys/class/net/%s/speed" % iface)
            addr = None
            try:
                probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                probe.settimeout(1)
                probe.connect(("8.8.8.8", 53))
                addr = probe.getsockname()[0]
                probe.close()
            except OSError:
                pass
            out["net"] = {
                "iface": iface, "addr": addr,
                "wireless": os.path.isdir("/sys/class/net/%s/wireless" % iface),
                "speed_mb": int(speed.strip()) if speed and speed.strip().lstrip("-").isdigit() else None,
                "rx_bps": round(rate_rx) if rate_rx is not None else None,
                "tx_bps": round(rate_tx) if rate_tx is not None else None,
            }
    except Exception:
        pass
    # archive growth: how big is this thing getting
    grow = {}
    try:
        grow["db_mb"] = round(os.path.getsize(DB_PATH) / 1e6, 1)
        wal = DB_PATH + "-wal"
        if os.path.exists(wal):
            grow["db_mb"] = round(grow["db_mb"] + os.path.getsize(wal) / 1e6, 1)
    except OSError:
        pass
    try:
        n = tot = 0
        with os.scandir(IMAGES_DIR) as it:
            for e in it:
                if e.name.endswith(".png"):
                    n += 1
                    tot += e.stat().st_size
        grow["images_n"] = n
        grow["images_mb"] = round(tot / 1e6, 1)
    except OSError:
        grow["images_n"] = 0
        grow["images_mb"] = 0.0
    out["growth"] = grow
    try:
        out["load1"] = round(os.getloadavg()[0], 2)
    except OSError:
        pass
    try:
        du = shutil.disk_usage("/")
        out["disk_used_gb"] = round((du.total - du.free) / 1e9, 1)
        out["disk_total_gb"] = round(du.total / 1e9, 1)
    except OSError:
        pass
    raw = _read("/proc/uptime")
    if raw:
        out["uptime_s"] = float(raw.split()[0])
    return out


def cam_recently_failed():
    with _cam_lock:
        return time.time() < _cam_fail_until


def cam_note_fail():
    global _cam_fail_until
    with _cam_lock:
        _cam_fail_until = time.time() + CAM_FAIL_COOLDOWN


def cam_note_ok():
    global _cam_fail_until
    with _cam_lock:
        _cam_fail_until = 0.0


def cam_connect(timeout):
    u = urlparse(CAM_URL)
    host = u.hostname or "127.0.0.1"
    port = u.port or (443 if u.scheme == "https" else 80)
    prefix = (u.path or "").rstrip("/")
    conn = http.client.HTTPConnection(host, port, timeout=timeout)
    return conn, prefix


# --------------------------------------------------------------------------- #
# handler
# --------------------------------------------------------------------------- #

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "daybreak"
    sys_version = ""
    # Idle keep-alive connections each pin a thread; recycle them promptly.
    timeout = 15.0

    # keep journald readable over a week: only failures get a line
    def log_message(self, fmt, *args):
        try:
            code = args[1] if len(args) > 1 else ""
        except Exception:
            code = ""
        if str(code).startswith(("4", "5")):
            log("[http] %s %s" % (self.address_string(), fmt % args))

    def log_error(self, fmt, *args):  # noqa: A003
        log("[http] %s %s" % (self.address_string(), fmt % args))

    # ---- plumbing -------------------------------------------------------- #

    def r_story(self, qs):
        """?id=<n> -> our own article, written from the source.

        Asynchronous on purpose. Prefilling several thousand characters of source at
        ~24 tok/s and then generating 800 words is a three minute job - it timed out the
        tunnel when it ran inline. The request starts the work and returns; the page
        polls with cached=1 and shows it when it lands.
        """
        try:
            import research, enrich
        except Exception as exc:
            return self.json(200, {"ok": False, "reason": "module: %s" % exc})
        if not research.available():
            return self.json(200, {"ok": False, "reason": "no fetch key configured"})
        try:
            iid = int((qs.get("id") or ["0"])[0])
        except (TypeError, ValueError):
            iid = 0
        con = open_db()
        row = con.execute(
            "SELECT id, title, url, source, summary FROM items WHERE id=?", (iid,)).fetchone()
        if row is None:
            return self.json(200, {"ok": False, "reason": "no such item"})
        item = {"id": row["id"], "title": row["title"], "url": row["url"],
                "source": row["source"]}
        if (qs.get("cached") or [""])[0] == "1":
            research.story_ensure(con)
            got = research.story_as_dict(
                con.execute("SELECT * FROM story WHERE item_id=?", (iid,)).fetchone())
            return self.json(200, {"ok": bool(got), "story": got,
                                   "reason": None if got else "not written yet"})
        with _STORY_LOCK:
            if iid in _STORY_RUNNING:
                return self.json(200, {"ok": False, "queued": True,
                                       "reason": "already being written"})
            _STORY_RUNNING.add(iid)

        def work():
            c2 = None
            try:
                c2 = open_db()
                st = research.run_story(c2, enrich.Tiiny(), item, say=log)
                _t, _o, _m = enrich.drain_tokens()
                db.add_tokens(c2, _t)
                db.add_device_ms(c2, _m)
                if not st:
                    log("[story] #%d produced nothing" % iid)
            except Exception as exc:
                log("[story] #%d failed: %s" % (iid, str(exc)[:160]))
            finally:
                with _STORY_LOCK:
                    _STORY_RUNNING.discard(iid)

        threading.Thread(target=work, name="story-%d" % iid, daemon=True).start()
        return self.json(200, {"ok": False, "queued": True,
                               "reason": "writing - this takes a couple of minutes"})

    def r_deepdive(self, qs):
        """?card=<n> -> the analyst read for that SITREP card.

        Cached per subject: a second request inside the freshness window returns the
        stored dive rather than searching again, so clicking about the board is cheap.
        """
        try:
            import research
        except Exception as exc:
            return self.json(200, {"ok": False, "reason": "research module: %s" % exc})
        if not research.available():
            return self.json(200, {"ok": False, "reason": "no search key configured"})
        try:
            idx = int((qs.get("card") or ["0"])[0])
        except (TypeError, ValueError):
            idx = 0
        con = open_db()
        try:
            raw = db.get_meta(con, "latest_sitrep_json", "") or "{}"
            cards = (json.loads(raw).get("cards") or [])
        except (ValueError, TypeError):
            cards = []
        if idx < 0 or idx >= len(cards):
            return self.json(200, {"ok": False, "reason": "no such card"})
        card = cards[idx]
        key = research.subject_key(str(card.get("aor") or "GLOBAL").upper(),
                                   str(card.get("title") or "")[:220])
        if (qs.get("cached") or [""])[0] == "1":
            research.ensure(con)
            got = research.as_dict(research.latest(con, key, research.DIVE_FRESH_S))
            return self.json(200, {"ok": bool(got), "dive": got,
                                   "reason": None if got else "not cached"})
        try:
            import enrich
            dive = research.run_dive(con, enrich.Tiiny(), card, say=log)
            _tot, _out, _ms = enrich.drain_tokens()
            db.add_tokens(con, _tot)
            db.add_device_ms(con, _ms)
        except Exception as exc:
            log("[dive] failed: %s" % str(exc)[:160])
            return self.json(200, {"ok": False, "reason": str(exc)[:160]})
        return self.json(200, {"ok": bool(dive), "dive": dive,
                               "reason": None if dive else "no usable result"})

    def send_bytes(self, code, body, ctype, headers=None, close=False):
        try:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            if close:
                self.send_header("Connection", "close")
                self.close_connection = True
            self.end_headers()
            if body and self.command != "HEAD":
                self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, socket.timeout, TimeoutError):
            self.close_connection = True

    def json(self, code, payload):
        body = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
        self.send_bytes(code, body, "application/json; charset=utf-8")

    # ---- routing --------------------------------------------------------- #

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = parse_qs(parsed.query or "")
            if path in ("/", "/index.html"):
                return self.r_index()
            if path == "/healthz":
                return self.r_health()
            if path == "/api/items":
                return self.r_items(query)
            if path == "/api/clusters":
                return self.r_clusters(query)
            if path == "/api/stats":
                return self.r_stats(query)
            if path == "/api/lease":
                return self.r_lease(query)
            if path == "/api/lease/stream":
                return self.r_lease_stream()
            if path == "/api/item":
                return self.r_item(query)
            if path == "/api/log":
                return self.r_log(query)
            if path == "/api/docs":
                return self.r_docs(query)
            if path == "/api/ask":
                return self.r_ask(query)
            if path == "/api/item/image":
                return self.r_item_image(query)
            if path == "/cam.mjpg":
                return self.r_cam_stream()
            if path == "/cam.jpg":
                return self.r_cam_snapshot()
            if path == "/api/episodes":
                return self.r_episodes()
            if path == "/feed.xml" or path == "/podcast.xml":
                return self.r_feed()
            if path.startswith("/episodes/"):
                return self.r_episode(path)
            if path == "/og.png":
                try:
                    with open(os.path.join(BASE_DIR, "static", "og.png"), "rb") as f:
                        return self.send_bytes(200, f.read(), "image/png")
                except OSError:
                    return self.json(404, {"error": "not found"})
            # On-demand deep dive for one SITREP card. Synchronous because the reader
            # is watching a spinner; the device lease inside keeps it off enrichment.
            if path == "/api/story":
                return self.r_story(query)
            if path == "/api/deepdive":
                return self.r_deepdive(query)
            # Vendored map data: Natural Earth coastlines (public domain, simplified to
            # 23 KB) and a country centroid table. Served from disk rather than a CDN so
            # the board keeps working with no outbound network at all.
            if path in ("/world.json", "/places.json"):
                try:
                    with open(os.path.join(BASE_DIR, "static", path[1:]), "rb") as f:
                        return self.send_bytes(200, f.read(), "application/json",
                                               headers={"Cache-Control": "public, max-age=604800"})
                except OSError:
                    return self.json(404, {"error": "not found"})
            if path == "/favicon.ico":
                return self.send_bytes(204, b"", "image/x-icon")
            return self.json(404, {"error": "not found"})
        except (BrokenPipeError, ConnectionResetError, socket.timeout, TimeoutError):
            self.close_connection = True
        except Exception:
            log("[http] 500 %s\n%s" % (self.path, traceback.format_exc()))
            try:
                self.json(500, {"error": "internal error"})
            except Exception:
                self.close_connection = True

    def do_HEAD(self):
        # never open an upstream stream for a HEAD probe
        if urlparse(self.path).path.rstrip("/") == "/cam.mjpg":
            return self.send_bytes(200, b"", "multipart/x-mixed-replace")
        self.do_GET()

    def do_POST(self):
        self.json(405, {"error": "method not allowed"})

    do_PUT = do_DELETE = do_PATCH = do_POST

    # ---- routes ---------------------------------------------------------- #

    def r_index(self):
        data = read_index()
        if data is None:
            return self.json(503, {"error": "index.html not built yet"})
        self.send_bytes(200, data, "text/html; charset=utf-8")

    def r_health(self):
        ok = True
        try:
            con = open_db()
            try:
                con.execute("SELECT 1").fetchone()
            finally:
                con.close()
        except Exception:
            ok = False
        # version is what tiinyapp.farm reads off this route to say which build is
        # running; device says whether there is a Tiiny to talk to at all, which is
        # the one thing a fresh install most often gets wrong.
        self.json(200 if ok else 503,
                  {"ok": ok, "ts": time.time(), "db": DB_PATH,
                   "version": VERSION, "device": device.configured()})

    def r_items(self, q):
        region = _first(q, "region")
        if region:
            region = region.upper()
        if region not in REGIONS:
            region = None
        category = _first(q, "category")
        if category:
            category = category.lower()
        if category not in CATEGORIES:
            category = None
        since = as_float(_first(q, "since"), None)
        limit = as_int(_first(q, "limit"), 100) or 100
        limit = max(1, min(200, limit))

        con = open_db()
        try:
            rows = db.recent_items(con, region=region, category=category,
                                   since=since, limit=limit)
        finally:
            con.close()

        items = []
        for r in rows:
            items.append({
                "id": rget(r, "id"),
                "url": rget(r, "url", ""),
                "source": rget(r, "source", ""),
                "title": rget(r, "title", ""),
                "published": as_float(rget(r, "published"), None),
                "fetched_at": as_float(rget(r, "fetched_at"), None),
                "summary": rget(r, "summary"),
                "category": rget(r, "category"),
                "region": rget(r, "region"),
                "severity": as_int(rget(r, "severity"), 1),
                "countries": json_countries(rget(r, "countries")),
                "cluster_id": rget(r, "cluster_id"),
            })
        self.json(200, {"items": items})

    def r_clusters(self, q):
        limit = as_int(_first(q, "limit"), 12) or 12
        limit = max(1, min(50, limit))
        con = open_db()
        try:
            rows = db.top_clusters(con, limit=limit)
            out = []
            for r in rows:
                cid = rget(r, "id")
                titles = []
                members = []
                try:
                    trows = con.execute(
                        "SELECT id, title FROM items WHERE cluster_id = ? "
                        "ORDER BY COALESCE(published, fetched_at) DESC LIMIT 3",
                        (cid,)).fetchall()
                    titles = [t["title"] for t in trows if t["title"]]
                    members = [{"id": t["id"], "title": t["title"]}
                               for t in trows if t["title"]]
                except Exception:
                    titles = []
                out.append({
                    "items": members,
                    "id": cid,
                    "label": rget(r, "label"),
                    "item_count": as_int(rget(r, "item_count"), 0),
                    "top_severity": as_int(rget(r, "top_severity"), 1),
                    "updated_at": as_float(rget(r, "updated_at"), None),
                    "titles": titles,
                })
        finally:
            con.close()
        self.json(200, {"clusters": out})

    def r_ask(self, q):
        import urllib.request
        question = (_first(q, "q") or "").strip()
        if not (3 <= len(question) <= 200):
            return self.json(400, {"error": "q must be 3-200 chars"})
        kb_port = os.environ.get("TIINY_KB_PORT", "5003")
        host = device.host()
        req = urllib.request.Request(
            "http://%s:%s/kb/retrieve" % (host, kb_port),
            data=json.dumps({"question": question}).encode(),
            headers={"Authorization": "Bearer " + device.key(),
                     "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                data = json.loads(resp.read())
        except Exception as exc:
            log("[ask] kb retrieve failed: %s" % str(exc)[:120])
            return self.json(502, {"error": "vault unreachable"})
        # Retrieve-then-rerank: the vault returns semantic neighbours, then the
        # device's cross-encoder scores each passage against the actual question.
        # Vector search is recall-oriented; the reranker is precision-oriented,
        # and it is only 2 NPU units. If it is not loaded we simply keep the
        # vault's own ordering.
        raw_results = [r for r in (data.get("results") or []) if isinstance(r, dict)]
        raw_results = rerank_passages(question, raw_results)
        out = []
        for r in raw_results:
            if not isinstance(r, dict):
                continue
            text = str(r.get("lossless_restatement") or "")
            # PRIVACY BOUNDARY: the device vault also holds the owner's chat-history
            # summaries and other personal files. The public board may only surface
            # what daybreak itself filed.
            if "daybreak-" not in text[:80].lower():
                continue
            if len(out) >= 5:
                break
            out.append({"score": r.get("_rerank"),
                        "text": text[:700],
                        "topic": str(r.get("topic") or "")[:40],
                        "entities": [str(e)[:40] for e in (r.get("entities") or [])[:6]],
                        "keywords": [str(k)[:40] for k in (r.get("keywords") or [])[:6]]})
        try:
            ec = open_db()
            db.add_event(ec, "VAULT", "archive query: %s" % question[:90])
            ec.close()
        except Exception:
            pass
        self.json(200, {"question": question, "results": out})

    EPISODES_DIR = os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "episodes")

    def _episode_list(self):
        """Newest-first episodes on disk, paired with their brief metadata."""
        out = []
        try:
            names = sorted((n for n in os.listdir(self.EPISODES_DIR)
                            if n.endswith(".mp3")), reverse=True)
        except OSError:
            return out
        con = open_db()
        try:
            for n in names[:60]:
                day = n[:-4]
                full = os.path.join(self.EPISODES_DIR, n)
                try:
                    st = os.stat(full)
                except OSError:
                    continue
                title = "DAYBREAK Brief — %s" % day
                desc = SITE_DESC_FALLBACK
                try:
                    row = con.execute(
                        "SELECT title, body FROM docs WHERE kind='brief' AND subject=? LIMIT 1",
                        (day,)).fetchone()
                    if row:
                        title = (rget(row, "title") or title)
                        desc = (rget(row, "body") or desc)
                except Exception:
                    pass
                # podcast clients show duration from the feed; probe it once
                try:
                    import audio as _a
                    secs = _a._duration(full)
                except Exception:
                    secs = 0.0
                out.append({"day": day, "title": title, "desc": desc,
                            "bytes": st.st_size, "ts": st.st_mtime,
                            "seconds": secs,
                            "url": "%s/episodes/%s" % (PUBLIC_BASE, n)})
        finally:
            con.close()
        return out

    def r_episodes(self):
        """Episode list for the on-page player (newest first)."""
        eps = []
        for e in self._episode_list()[:20]:
            eps.append({"day": e["day"], "title": e["title"],
                        "desc": (e.get("desc") or "")[:400],
                        "script": (e.get("desc") or ""),
                        "seconds": round(e.get("seconds") or 0, 1),
                        "bytes": e.get("bytes"),
                        "url": "/episodes/%s.mp3" % e["day"],
                        "cdn": e.get("url")})
        self.json(200, {"episodes": eps, "feed": "/feed.xml"})

    def r_feed(self):
        try:
            import audio
            body = audio.build_feed(self._episode_list(), PUBLIC_BASE)
        except Exception as exc:
            log("[feed] %s" % str(exc)[:120])
            return self.json(500, {"error": "feed unavailable"})
        self.send_bytes(200, body.encode("utf-8"), "application/rss+xml; charset=utf-8")

    def r_episode(self, path):
        name = os.path.basename(path)
        if not re.match(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}\.mp3$", name):
            return self.json(404, {"error": "not found"})
        full = os.path.join(self.EPISODES_DIR, name)
        try:
            with open(full, "rb") as fh:
                data = fh.read()
        except OSError:
            return self.json(404, {"error": "not found"})
        self.send_bytes(200, data, "audio/mpeg")

    def r_log(self, q):
        limit = as_int(_first(q, "limit"), 50) or 50
        con = open_db()
        try:
            rows = db.recent_events(con, limit=limit)
            now = db.get_meta(con, "now_doing", "") or ""
            # now_doing is cleared in a finally, but a SIGKILL (or a power cut)
            # mid-inference leaves the last one stranded and the board would keep
            # claiming the NPU is busy forever. The lease keys are timestamps for
            # exactly this reason: once both have expired, nobody is working.
            busy = max(as_float(db.get_meta(con, "enrich_busy_until"), 0.0) or 0.0,
                       as_float(db.get_meta(con, "img_hold_until"), 0.0) or 0.0)
            if now and time.time() > busy:
                now = ""
            imgs = as_int(db.get_meta(con, "images_total"), 0) or 0
        finally:
            con.close()
        self.json(200, {"now": now,
                        "images_total": imgs,
                        "events": [{"ts": as_float(rget(r, "ts"), None),
                                    "kind": rget(r, "kind", ""),
                                    "msg": rget(r, "msg", "")} for r in rows]})

    def r_docs(self, q):
        """Long-form AI output from jobs.py: dossiers, regional syntheses, briefs.

        `?kind=` filters, `?id=` returns one document with its body, otherwise a
        newest-first index (bodies omitted, they run to 2 KB each).
        """
        doc_id = as_int(_first(q, "id"), None)
        kind = (_first(q, "kind") or "").strip().lower() or None
        if kind and kind not in DOC_KINDS:
            return self.json(400, {"error": "unknown kind"})
        limit = max(1, min(100, as_int(_first(q, "limit"), 20) or 20))
        con = open_db()
        try:
            if doc_id:
                row = con.execute("SELECT * FROM docs WHERE id = ?", (doc_id,)).fetchone()
                if row is None:
                    return self.json(404, {"error": "no such doc"})
                return self.json(200, {"doc": {
                    "id": rget(row, "id"), "kind": rget(row, "kind"),
                    "subject": rget(row, "subject"), "title": rget(row, "title"),
                    "body": rget(row, "body", ""),
                    "created_at": as_float(rget(row, "created_at"), None),
                    "item_count": as_int(rget(row, "item_count"), 0)}})
            rows = db.recent_docs(con, kind=kind, limit=limit)
            counts = db.doc_counts(con)
        finally:
            con.close()
        self.json(200, {"counts": counts, "docs": [{
            "id": rget(r, "id"), "kind": rget(r, "kind"),
            "subject": rget(r, "subject"), "title": rget(r, "title"),
            "created_at": as_float(rget(r, "created_at"), None),
            "item_count": as_int(rget(r, "item_count"), 0)} for r in rows]})

    def r_item(self, q):
        item_id = as_int(_first(q, "id"), None)
        if not item_id:
            return self.json(400, {"error": "id required"})
        con = open_db()
        try:
            row = con.execute("SELECT * FROM items WHERE id = ?",
                              (item_id,)).fetchone()
            if row is None:
                return self.json(404, {"error": "no such item"})
            siblings = []
            cid = rget(row, "cluster_id")
            label = None
            if cid:
                try:
                    crow = con.execute("SELECT label FROM clusters WHERE id = ?",
                                       (cid,)).fetchone()
                    label = rget(crow, "label") if crow else None
                    srows = con.execute(
                        "SELECT id, title, source, severity FROM items "
                        "WHERE cluster_id = ? AND id != ? "
                        "ORDER BY COALESCE(published, fetched_at) DESC LIMIT 6",
                        (cid, item_id)).fetchall()
                    siblings = [{"id": rget(s, "id"), "title": rget(s, "title"),
                                 "source": rget(s, "source"),
                                 "severity": as_int(rget(s, "severity"), 1)}
                                for s in srows]
                except Exception:
                    siblings = []
        finally:
            con.close()
        self.json(200, {
            "id": rget(row, "id"), "url": rget(row, "url", ""),
            "source": rget(row, "source", ""), "title": rget(row, "title", ""),
            "published": as_float(rget(row, "published"), None),
            "fetched_at": as_float(rget(row, "fetched_at"), None),
            "raw_summary": rget(row, "raw_summary"),
            "summary": rget(row, "summary"),
            "category": rget(row, "category"), "region": rget(row, "region"),
            "severity": as_int(rget(row, "severity"), None),
            "countries": json_countries(rget(row, "countries")),
            "cluster_id": cid, "cluster_label": label, "siblings": siblings,
            "image_ready": os.path.exists(image_path(item_id)),
        })

    def r_item_image(self, q):
        item_id = as_int(_first(q, "id"), None)
        if not item_id:
            return self.json(400, {"error": "id required"})
        p = image_path(item_id)
        if os.path.exists(p):
            try:
                with open(p, "rb") as f:
                    data = f.read()
                return self.send_bytes(200, data, "image/png")
            except OSError:
                pass
        con = open_db()
        try:
            row = con.execute(
                "SELECT title, summary, raw_summary FROM items WHERE id = ?",
                (item_id,)).fetchone()
        finally:
            con.close()
        if row is None:
            return self.json(404, {"error": "no such item"})
        if not _IMG_LOCK.acquire(blocking=False):
            # one generation at a time; the client polls back
            return self.json(202, {"status": "busy", "retry_s": 4})
        hold_token = None
        try:
            # The advertised timestamps no longer gate this path, and that is the whole
            # point of the onelane.
            #
            # _wait_for_chat() honoured meta.enrich_busy_until, which is a PESSIMISTIC
            # reservation: a job advertises 120s and then typically uses three. A reader
            # who clicked an article was turned away for a lease nobody was holding, and
            # since the enricher re-advertises every cycle, the answer never changed.
            # Measured: "#3174 deferred: chat lease held for another 118s", twice, while
            # the device was idle between three-second gradings.
            #
            # generate_image() takes the real lock with a bounded wait, so contention is
            # now settled by the thing that actually knows: whoever holds the device.
            # A fair queue was verified before removing this - a tight acquire/work/
            # release loop against an occasional waiter gave the waiter a 0.55s median
            # wait, no timeouts in 8 attempts.
            hold_con = open_db()
            try:
                # Disk cap. This path had none: it writes into IMAGES_DIR with no
                # severity filter, and every render it produces for an S4/S5 story
                # is a file prune_images protects, so a viewer clicking through hot
                # stories could grow the directory past DAYBREAK_IMAGE_CAP_GB
                # without bound. `over_cap` is None when there is no fresh janitor
                # reading, which means "not known to be over" -- proceed.
                if db.image_cap_state(hold_con).get("over_cap"):
                    log("[image] #%d refused: image cache over disk cap" % item_id)
                    return self.json(507, {
                        "error": "image cache is over its disk cap",
                        "detail": "raise DAYBREAK_IMAGE_CAP_GB or lower "
                                  "DAYBREAK_IMAGE_PROTECT_DAYS"})
                hold_token = "%.3f" % (time.time() + 180)
                db.set_meta(hold_con, "img_hold_until", hold_token)
                db.set_meta(hold_con, "now_doing",
                            "GENERATING IMAGE #%d — Z-IMAGE-TURBO" % item_id)
                db.add_event(hold_con, "IMAGE",
                             "tasking Z-Image-Turbo for #%d — %s"
                             % (item_id, str(rget(row, "title", ""))[:80]))
            finally:
                hold_con.close()
            # 150004 = NPU busy on the device; a stray chat call can still slip
            # into our window, so retry into the gaps rather than failing fast
            data = err = None
            for attempt in range(3):
                data, err = generate_image(
                    item_id, rget(row, "title", ""),
                    rget(row, "summary") or rget(row, "raw_summary") or "")
                if data is not None or "150004" not in str(err):
                    break
                log("[image] #%d attempt %d hit busy NPU; retrying" %
                    (item_id, attempt + 1))
                time.sleep(6)
            if data is None and err == "DEVICE_BUSY":
                # Somebody else had the device for the whole wait. That is not a
                # failure, it is a queue, and the client already knows how to come back.
                log("[image] #%d deferred: device held for the full %.0fs wait"
                    % (item_id, IMG_CHAT_WAIT_S))
                return self.json(202, {"status": "busy", "retry_s": 20,
                                       "detail": "device is busy with another job"})
            if data is None:
                log("[image] #%d failed: %s" % (item_id, err))
                try:
                    ec = open_db()
                    db.add_event(ec, "ERROR", "image #%d failed: %s"
                                 % (item_id, str(err)[:100]))
                    ec.close()
                except Exception:
                    pass
                return self.json(502, {"error": "generation failed",
                                       "detail": err})
            try:
                os.makedirs(IMAGES_DIR, exist_ok=True)
                tmp = p + ".tmp"
                with open(tmp, "wb") as f:
                    f.write(data)
                os.replace(tmp, p)
            except OSError as exc:
                log("[image] cache write failed: %s" % exc)
            log("[image] #%d generated (%d bytes)" % (item_id, len(data)))
            try:
                done_con = open_db()
                db.add_event(done_con, "IMAGE",
                             "#%d rendered (%d KB, 512x512, 8 steps)"
                             % (item_id, len(data) // 1024))
                cur = as_int(db.get_meta(done_con, "images_total"), 0) or 0
                db.set_meta(done_con, "images_total", str(cur + 1))
                done_con.close()
            except Exception:
                pass
            return self.send_bytes(200, data, "image/png")
        finally:
            if hold_token is not None:
                try:
                    rel = open_db()
                    # Compare-and-clear: img_hold_until is shared with jobs.py and
                    # pipeline.py, and a blind "0" here would drop a lease somebody
                    # else took after us -- which is the 150004 collision the lease
                    # exists to prevent, caused by the release rather than avoided.
                    if str(db.get_meta(rel, "img_hold_until", "0")) == hold_token:
                        db.set_meta(rel, "img_hold_until", "0")
                        db.set_meta(rel, "now_doing", "")
                    rel.close()
                except Exception:
                    pass
            _IMG_LOCK.release()

    def r_lease(self, q):
        """What the device is doing right now, and what it just finished.

        Cheap by construction: leasewatch samples the holder record, and reading that
        never takes the lock, so polling this cannot delay an inference. Every process
        appears here - the pipeline, this server, and chorus - because the lock file is
        the only place all of them are visible at once.
        """
        if leasewatch is None:
            return self.json(200, {"now": None, "queue": 0, "recent": [], "totals": {}})
        n = as_int(_first(q, "n"), 24) or 24
        return self.json(200, leasewatch.get(max(1, min(60, n))))

    def r_lease_stream(self):
        """Server-sent events at the sampler's own resolution.

        A one-second poll is not real time and an engineer will say so. leasewatch already
        samples the holder record at 4Hz on a background thread, so this pushes at that
        rate and only when something actually changed - the version counter makes an
        unchanged device cost one comparison, not a payload.

        Reading the holder record never takes the lock, so however many people watch this,
        none of them can delay an inference.
        """
        if leasewatch is None:
            return self.json(503, {"error": "lease watch unavailable"})
        if not _lease_slots.acquire(blocking=False):
            return self.json(503, {"error": "too many watchers"})
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache, no-store")
            self.send_header("Connection", "keep-alive")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()
            sent_ver, sent_seq = -1, None
            t0, beat = time.time(), 0.0
            while time.time() - t0 < LEASE_STREAM_MAX_S:
                snap = leasewatch.get(12, since=sent_seq)
                if snap["version"] != sent_ver:
                    sent_ver = snap["version"]
                    sent_seq = snap["seq"]
                    snap["t"] = time.time()
                    self.wfile.write(("data: %s\n\n" % json.dumps(snap)).encode())
                    self.wfile.flush()
                    beat = time.time()
                elif time.time() - beat > 12.0:
                    # A comment keeps the tunnel and any proxy from closing a quiet
                    # stream, and costs nothing on the client.
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    beat = time.time()
                time.sleep(leasewatch.INTERVAL_S)
        except (BrokenPipeError, ConnectionResetError):
            pass                                   # the watcher went away; normal
        except Exception as exc:
            log("[lease] stream ended: %s" % str(exc)[:90])
        finally:
            _lease_slots.release()
        return None

    def r_stats(self, q):
        # Shape is ADDITIVE only: counts/meta/device/series/host keep their exact
        # existing contents and types, `archive` and `docs` are new top-level keys.
        # An older index.html ignores them; nothing it reads moved.
        payload = {"counts": {}, "meta": {}, "device": {}, "series": {},
                   "archive": {}, "docs": {}}
        # Additive, like archive and docs before it: whether a Tiiny is configured at
        # all, and where we were told about it. Never the key. A board with no device
        # showed six empty gauges and an amber telemetry light, which reads as a
        # broken device rather than an absent one.
        payload["tiiny"] = {"configured": device.configured(),
                            "base": TIINY_BASE if device.configured() else "",
                            "source": device.source(),
                            "version": VERSION}
        try:
            payload["host"] = host_stats()
        except Exception as exc:
            log("[stats] host stats failed: %s" % exc)
            payload["host"] = {}
        hourly = None
        long_days = 30.0
        con = open_db()
        try:
            try:
                payload["counts"] = dict(db.counts(con) or {})
            except Exception as exc:
                log("[stats] counts failed: %s" % exc)

            # The archive is the product: how far back it reaches, how many
            # analyses it holds, what it costs on disk.
            try:
                payload["archive"] = dict(db.archive_stats(con) or {})
            except Exception as exc:
                log("[stats] archive failed: %s" % exc)
            try:
                payload["docs"] = dict(db.doc_counts(con) or {})
            except Exception as exc:
                log("[stats] doc counts failed: %s" % exc)

            meta = {}
            try:
                meta["embeddings"] = db.get_meta(con, "embeddings", "off") or "off"
                meta["last_fetch_ts"] = as_float(db.get_meta(con, "last_fetch_ts"), None)
                meta["pipeline_started_ts"] = as_float(
                    db.get_meta(con, "pipeline_started_ts"), None)
                meta["tokens_total"] = as_int(db.get_meta(con, "tokens_total"), 0)
                meta["device_ms_total"] = as_int(db.get_meta(con, "device_ms_total"), 0)
                # Burn rate computed here, over a real hour of recorded generations.
                # The browser used to derive this by watching the odometer, which meant
                # nothing displayed until it had two samples far enough apart - so every
                # page load showed a blank rate and a dead token feed for a minute and
                # a half. The server already has the series; it should just say.
                try:
                    trow = con.execute(
                        "SELECT SUM(value) s, MIN(ts) a, MAX(ts) b FROM metrics "
                        "WHERE key='tokens_out' AND ts >= ?",
                        (time.time() - 3600,)).fetchone()
                    if trow and trow["s"] and trow["b"] and trow["a"]:
                        span = max(300.0, float(trow["b"]) - float(trow["a"]))
                        meta["tokens_rate_hr"] = round(float(trow["s"]) / (span / 3600.0))
                except (sqlite3.Error, TypeError, ValueError):
                    pass
                # The standing analyst brief: an assessment across every theatre,
                # written against its own previous one.
                try:
                    arow = con.execute(
                        "SELECT body, created_at, item_count FROM docs WHERE kind='analyst' "
                        "ORDER BY created_at DESC LIMIT 1").fetchone()
                    if arow:
                        meta["analyst"] = json.loads(arow["body"] or "{}")
                        meta["analyst"]["ts"] = arow["created_at"]
                        meta["analyst"]["items"] = arow["item_count"]
                except (ValueError, TypeError, sqlite3.Error):
                    pass
                meta["items_enriched_total"] = as_int(
                    db.get_meta(con, "items_enriched_total"), 0)
                meta["vault_digests_total"] = as_int(
                    db.get_meta(con, "vault_digests_total"), 0)
                meta["latest_sitrep"] = str(db.get_meta(con, "latest_sitrep", "") or "")[:2000]
                meta["latest_sitrep_ts"] = as_float(db.get_meta(con, "latest_sitrep_ts"), None)
                # Structured form. The flat prose above stays for the RSS feed and any
                # other consumer that predates the cards.
                try:
                    meta["sitrep"] = json.loads(
                        db.get_meta(con, "latest_sitrep_json", "") or "{}")
                except (ValueError, TypeError):
                    meta["sitrep"] = {}
                meta["images_total"] = as_int(db.get_meta(con, "images_total"), 0)
                # jobs.py run counters — how much deep work the idle NPU has done
                for name in ("dossier", "synthesis", "brief", "recluster", "image"):
                    meta["job_%s_total" % name] = as_int(
                        db.get_meta(con, "job_%s_total" % name), 0)
                meta["r2_last_sync_ts"] = as_float(db.get_meta(con, "r2_last_sync_ts"), None)
                meta["r2_last_ship_ts"] = as_float(db.get_meta(con, "r2_last_ship_ts"), None)
                meta["r2_objects_total"] = as_int(db.get_meta(con, "r2_objects_total"), 0)
            except Exception as exc:
                log("[stats] meta failed: %s" % exc)
            payload["meta"] = meta

            metrics = {}
            try:
                metrics = db.latest_metrics(con, list(METRIC_KEYS), window_s=3600) or {}
            except Exception as exc:
                log("[stats] metrics failed: %s" % exc)

            snap = {}
            snap_ts = None
            try:
                raw = db.get_meta(con, "device_last")
                if raw:
                    snap = json.loads(raw)
                    if not isinstance(snap, dict):
                        snap = {}
                snap_ts = as_float(snap.get("ts"), None)
                if snap_ts is None:
                    snap_ts = as_float(db.get_meta(con, "device_last_ts"), None)
            except Exception as exc:
                log("[stats] device snapshot failed: %s" % exc)

            # ?series=long reaches the hourly rollup. db.rollup_metrics folds every
            # completed hour into metrics_hourly precisely so the trend survives
            # the 8-day raw-metric retention -- but nothing read it, so past 8 days
            # the data existed with no way to see it and the retention claim in
            # db.py's docstring was unbacked. Opt-in: the default payload is
            # byte-for-byte what it was.
            if (_first(q, "series") or "").strip().lower() in ("long", "hourly"):
                long_days = max(1.0, min(400.0, as_float(_first(q, "days"), 30.0) or 30.0))
                try:
                    hourly = db.hourly_metrics(con, list(SERIES_KEYS),
                                               window_s=long_days * 86400) or {}
                except Exception as exc:
                    log("[stats] hourly series failed: %s" % exc)
                    hourly = {}
        finally:
            con.close()

        def latest(key, fallback=None):
            entry = metrics.get(key) or {}
            v = as_float(entry.get("latest"), None)
            return fallback if v is None else v

        models = snap.get("models")
        if not isinstance(models, list):
            models = []
        payload["device"] = {
            "ts": snap_ts,
            "online": bool(snap_ts is not None and (time.time() - snap_ts) < DEVICE_STALE_S),
            "npu_util": latest("npu_util", as_float(snap.get("npu_util"), None)),
            "npu_mem_mb": latest("npu_mem_mb", as_float(snap.get("npu_mem_used_mb"), None)),
            "npu_mem_used_mb": as_float(snap.get("npu_mem_used_mb"),
                                        latest("npu_mem_mb", None)),
            "npu_mem_total_mb": as_float(snap.get("npu_mem_total_mb"), None),
            "cpu_pct": latest("cpu_pct", as_float(snap.get("cpu_pct"), None)),
            "mem_pct": latest("mem_pct", as_float(snap.get("mem_pct"), None)),
            "npu_used": as_float(snap.get("npu_used"), None),
            "npu_available": as_float(snap.get("npu_available"), None),
            "models": models,
            "gen_tps": latest("gen_tps", None),
            "queue_depth": latest("queue_depth", None),
            "enrich_ms": latest("enrich_ms", None),
            # Device thermals are not on the REST surface at all: /api/v1/npu/status
            # returns temp_c and power_w as null while /sys/class/thermal holds fourteen
            # live zones. devtherm samples them over SSH on a background thread, so this
            # is a cached read and never delays a page.
            "thermal": devtherm.get() if devtherm is not None else {},
        }
        for key in SERIES_KEYS:
            entry = metrics.get(key) or {}
            series = entry.get("series") or []
            payload["series"][key] = series if isinstance(series, list) else []

        if hourly is not None:
            payload["series_long"] = {"window_days": long_days, "keys": {}}
            for key in SERIES_KEYS:
                entry = hourly.get(key) or {}
                rows = entry.get("series") or []
                payload["series_long"]["keys"][key] = rows if isinstance(rows, list) else []
        self.json(200, payload)

    # ---- camera ---------------------------------------------------------- #

    def r_cam_snapshot(self):
        if cam_recently_failed():
            return self.json(503, {"error": "camera offline"})
        conn = None
        try:
            conn, prefix = cam_connect(CAM_CONNECT_TIMEOUT)
            conn.request("GET", prefix + "/snapshot",
                         headers={"User-Agent": "daybreak/1.0"})
            resp = conn.getresponse()
            if resp.status != 200:
                raise OSError("upstream %s" % resp.status)
            data = resp.read(CAM_MAX_SNAPSHOT)
            ctype = resp.getheader("Content-Type") or "image/jpeg"
        except Exception as exc:
            cam_note_fail()
            log("[cam] snapshot unavailable: %s" % exc)
            self._close_quiet(conn)
            return self.json(503, {"error": "camera offline"})
        cam_note_ok()
        self.send_bytes(200, data, ctype)
        self._close_quiet(conn)

    def r_cam_stream(self):
        if cam_recently_failed():
            return self.json(503, {"error": "camera offline"})
        if not _cam_slots.acquire(blocking=False):
            return self.json(503, {"error": "camera busy"})
        conn = None
        try:
            try:
                conn, prefix = cam_connect(CAM_CONNECT_TIMEOUT)
                conn.request("GET", prefix + "/stream",
                             headers={"User-Agent": "daybreak/1.0"})
                resp = conn.getresponse()
                if resp.status != 200:
                    raise OSError("upstream %s" % resp.status)
            except Exception as exc:
                cam_note_fail()
                log("[cam] stream unavailable: %s" % exc)
                self._close_quiet(conn)
                return self.json(503, {"error": "camera offline"})

            cam_note_ok()
            ctype = (resp.getheader("Content-Type")
                     or "multipart/x-mixed-replace; boundary=boundarydonotcross")
            # a stalled camera must not pin this thread forever
            try:
                if conn.sock is not None:
                    conn.sock.settimeout(CAM_READ_TIMEOUT)
            except Exception:
                pass

            self.close_connection = True
            try:
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
                self.send_header("Pragma", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
                while True:
                    chunk = resp.read(16384)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
            except (BrokenPipeError, ConnectionResetError):
                pass  # viewer navigated away
            except (socket.timeout, TimeoutError, OSError, http.client.HTTPException) as exc:
                cam_note_fail()
                log("[cam] stream ended: %s" % exc)
            self._close_quiet(conn)
        finally:
            _cam_slots.release()

    @staticmethod
    def _close_quiet(conn):
        if conn is None:
            return
        try:
            conn.close()
        except Exception:
            pass


def _first(q, key, default=None):
    v = q.get(key)
    if not v:
        return default
    v = v[0].strip()
    return v or default


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 64


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def selfcheck():
    import urllib.request
    srv = Server(("127.0.0.1", 0), Handler)
    host, port = srv.server_address[0], srv.server_address[1]
    th = threading.Thread(target=srv.serve_forever, kwargs={"poll_interval": 0.2},
                          daemon=True)
    th.start()
    rc = 0
    # "/" first: the wall itself is the thing being checked, and a board that
    # serves every API route and then 500s on the page is a passing check and a
    # blank screen. /cam.jpg and /nope are the two that are meant to fail.
    for path in ("/", "/healthz", "/api/items?limit=5", "/api/clusters", "/api/stats",
                 "/cam.jpg", "/nope"):
        url = "http://%s:%d%s" % (host, port, path)
        try:
            with urllib.request.urlopen(url, timeout=10) as r:
                body = r.read()
                print("  %-24s %s %d bytes" % (path, r.status, len(body)))
                if path == "/api/stats":
                    print("    %s" % json.dumps(json.loads(body))[:400])
        except Exception as exc:
            code = getattr(exc, "code", None)
            expected = (path in ("/cam.jpg", "/nope"))
            print("  %-24s %s%s" % (path, code or exc, "" if expected else "  <-- UNEXPECTED"))
            if not expected:
                rc = 1
    srv.shutdown()
    srv.server_close()
    return rc


def main():
    if "--selfcheck" in sys.argv[1:]:
        return selfcheck()

    srv = Server((BIND, PORT), Handler)

    def stop(signum, _frame):
        log("signal %s -> shutting down" % signum)
        threading.Thread(target=srv.shutdown, daemon=True).start()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, stop)
        except (ValueError, OSError):
            pass

    if leasewatch is not None:
        def _lw_load():
            con = open_db()
            try:
                return json.loads(db.get_meta(con, "lease_totals") or "{}")
            finally:
                con.close()

        def _lw_save(totals):
            con = open_db()
            try:
                db.set_meta(con, "lease_totals", json.dumps(totals))
                con.commit()
            finally:
                con.close()

        try:
            # The tally is what the DEVICE has done, not what this process has seen
            # since it started. A deploy used to reset it silently, which reads as the
            # board losing work rather than the counter losing its memory.
            leasewatch.start(device.holder, _lw_load, _lw_save)
            log("lease watch: sampling the onelane every %.2fs, totals persisted"
                % leasewatch.INTERVAL_S)
        except Exception as exc:
            log("lease watch: disabled (%s)" % str(exc)[:80])

    if devtherm is not None:
        if devtherm.start() is not None:
            log("device thermals: sampling %s every %.0fs over ssh"
                % (devtherm.HOST, devtherm.INTERVAL_S))
        else:
            # Not an error worth failing over: the board simply omits the panel.
            log("device thermals: disabled (%s)" % (devtherm.get().get("error") or "no key"))

    # Positive proof the lock directory is shared with the other unit. Nothing a single
    # process can observe proves sharedness -- onelane's own check only spots known
    # ways it is NOT shared. Two units seeing each other's marker is the real evidence,
    # and a permanent "peers visible: none" is the symptom of the namespace bug this
    # directory exists to avoid.
    peers = device.prove_shared("server", quiet=True)
    log("device %s; locks %s; peers visible: %s"
        % (device.host(), device.LOCK_DIR, ", ".join(sorted(peers)) or "none yet"))
    log("daybreak server on %s:%d db=%s cam=%s" % (BIND, PORT, DB_PATH, CAM_URL))
    try:
        srv.serve_forever(poll_interval=0.5)
    finally:
        srv.server_close()
        log("daybreak server stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
