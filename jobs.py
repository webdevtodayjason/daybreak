#!/usr/bin/env python3
"""DAYBREAK idle-time deep work — the scheduler that keeps the NPU earning.

The enrichment queue is the device's day job: every incoming wire item gets a
summary, a category, a region and a severity. But the queue drains. On a quiet
feed cycle the Tiiny sat idle apart from one ad-hoc image render bolted onto the
sitrep loop. This module is what it does instead, and it is where every new
long-form job type lives:

  recluster   DB-only sweep: enriched items that never landed in a cluster get
              one, and clusters that grew past the label threshold get an
              AI-written headline. Cheap; runs most often.
  synthesis   One COCOM region at a time, round-robin: 24h of that AOR's wire
              read into a regional assessment.
  dossier     The hottest entity (country/actor) on the wire that has not been
              written up recently: 14 days of its coverage read into a dossier.
  image       Backfill one missing S4+ story render (the old _autoimage_one),
              honouring BOTH caps: DAYBREAK_IMAGE_DAILY_CAP renders/day and the
              DAYBREAK_IMAGE_CAP_GB disk ceiling db.prune_images enforces.
  brief       Once per UTC day: the executive brief for the day just ended,
              built from that day's syntheses, dossiers and top wire items. Also
              dropped into digests/ so the R2 loop ships it offsite.

Three invariants, enforced here so pipeline.py cannot get them wrong:

  1. NOTHING runs while db.counts()['pending'] > 0. Enrichment always wins.
  2. Every device call takes the lease -- BOTH keys, chat work included.
     `img_hold_until` is the one every consumer actually polls (the enricher's
     between-item yield, the cluster labeller, wait_for_quiet below);
     `enrich_busy_until` is what server.py's on-demand image endpoint waits out.
     Advertising only enrich_busy_until for a 250s dossier left the enricher free
     to start a second chat call on the same device, so a job now takes both.
     Release is compare-and-clear (_release): a blind "0" would zero somebody
     else's live lease.
  3. SIGTERM exits cleanly: jobs check ctx.stopped() between units of work and a
     job never holds a lease across a stop.

Output lands in the `docs` table (kind = dossier|synthesis|brief) and every job
narrates itself into the oplog, so the AI OPS LOG on the board shows the device
working even when the wire is quiet.

CLI (this is also the operator's "brief script"):
    python3 jobs.py                     one scheduler pass, gates respected
    python3 jobs.py --status            what is due, what ran last, what exists
    python3 jobs.py --brief             file today's brief for the previous UTC day
    python3 jobs.py --brief --day 2026-08-20 --force     re-run a specific day
    python3 jobs.py --job synthesis --force              force one job now
    python3 jobs.py --selfcheck         no-device wiring check on a temp DB

Env: TIINY_HOST, TIINY_KEY, DAYBREAK_DB, IMAGE_MODEL, plus the DAYBREAK_*_INTERVAL
overrides below. Stdlib only.
"""

import calendar
import json
import os
import re
import unicodedata
import sqlite3
import sys
import time
import urllib.request
from contextlib import contextmanager

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import db  # noqa: E402
import enrich  # noqa: E402
import device

try:
    import r2 as _r2  # optional: only used to drop a digest copy for the offsite loop
except Exception:  # pragma: no cover - r2.py is stdlib-only, but never hard-fail
    _r2 = None


# --------------------------------------------------------------------------- #
# tunables
# --------------------------------------------------------------------------- #

def _env_float(name, default):
    try:
        v = float(os.environ.get(name, "") or "")
    except (TypeError, ValueError):
        return default
    return v if v > 0 else default


def _env_int(name, default):
    try:
        v = int(float(os.environ.get(name, "") or ""))
    except (TypeError, ValueError):
        return default
    return v if v >= 0 else default


RECLUSTER_INTERVAL_S = _env_float("DAYBREAK_RECLUSTER_INTERVAL", 1800.0)
SYNTHESIS_INTERVAL_S = _env_float("DAYBREAK_SYNTHESIS_INTERVAL", 3600.0)
DOSSIER_INTERVAL_S = _env_float("DAYBREAK_DOSSIER_INTERVAL", 5400.0)
IMAGE_INTERVAL_S = _env_float("DAYBREAK_IMAGE_INTERVAL", 600.0)
BRIEF_CHECK_S = _env_float("DAYBREAK_BRIEF_CHECK", 900.0)

IMAGE_DAILY_CAP = _env_int("DAYBREAK_IMAGE_DAILY_CAP", 12)
IMAGE_MODEL = os.environ.get("IMAGE_MODEL", "Tongyi-MAI/Z-Image-Turbo")

# A brief covers a finished UTC day, filed once the day is comfortably over.
BRIEF_AFTER_MIN = _env_float("DAYBREAK_BRIEF_AFTER_MIN", 30.0)

REGIONS = ("NORTHCOM", "SOUTHCOM", "EUCOM", "CENTCOM", "AFRICOM", "INDOPACOM", "GLOBAL")

SYNTHESIS_MIN_ITEMS = 5        # below this an AOR has nothing worth synthesising
SYNTHESIS_WINDOW_S = 24 * 3600
SYNTHESIS_COOLDOWN_S = 6 * 3600    # same region, no more often than this

DOSSIER_WINDOW_S = 14 * 86400
DOSSIER_MIN_ITEMS = 4
DOSSIER_COOLDOWN_S = 3 * 86400     # same entity, no more often than this

RECLUSTER_WINDOW_S = 72 * 3600
RECLUSTER_BATCH = 40
LABEL_MIN_ITEMS = 3
LABEL_PER_PASS = 3

IMAGE_TIMEOUT_S = 120.0        # urlopen timeout on /v1/image/generate

# A lease shorter than the call it guards is not a lease. Both of these are
# derived from the actual worst-case call time, not guessed:
#   chat  : enrich.chat_json makes up to TWO attempts, each capped at
#           enrich.TIMEOUT (TIINY_TIMEOUT, default 180s). Measured normal case is
#           18-31s for a 1200-token generation at ~22 tok/s, but a second client
#           on the device stretches that -- an integrator run alongside the live
#           pipeline took 250s, which would have expired a 240s lease MID-CALL
#           and let the next worker start a second inference.
#   image : the render request itself waits up to IMAGE_TIMEOUT_S, so a 90s lease
#           (what the old inline _autoimage_one used) could expire with the NPU
#           still rendering -- which is precisely the collision that produces
#           device error 150004.
CHAT_LEASE_S = max(240.0, 2 * float(getattr(enrich, "TIMEOUT", 180.0)) + 30.0)
IMAGE_LEASE_S = IMAGE_TIMEOUT_S + 30.0
LEASE_WAIT_S = 120.0           # max wait for someone else's image lease to clear

# Country strings the model emits that are not entities worth a dossier.
_ENTITY_SKIP = frozenset({"", "n/a", "na", "none", "unknown", "global", "worldwide",
                          "world", "international", "various", "multiple", "-"})

_WS_RE = re.compile(r"\s+")


# --------------------------------------------------------------------------- #
# context: everything a job needs from its host, injected so jobs.py never
# imports pipeline.py (that import would be circular).
# --------------------------------------------------------------------------- #

class Ctx:
    """Host services for a job run.

    tiiny   callable returning an enrich.Tiiny (pipeline shares one handle)
    log     callable(str) -- pipeline's log(), or print
    stop    threading.Event -- set on SIGTERM
    """

    def __init__(self, tiiny=None, log=None, stop=None, images_dir=None, db_path=None):
        self._tiiny = tiiny
        self._handle = None
        self._log = log
        self.stop = stop
        self.db_path = db_path or os.environ.get("DAYBREAK_DB") or \
            os.path.join(BASE_DIR, "daybreak.db")
        self.images_dir = images_dir or os.path.join(
            os.path.dirname(os.path.abspath(self.db_path)), "images")

    def device(self):
        if self._tiiny is not None:
            return self._tiiny() if callable(self._tiiny) else self._tiiny
        if self._handle is None:
            self._handle = enrich.Tiiny()
        return self._handle

    def stopped(self):
        return bool(self.stop is not None and self.stop.is_set())

    def sleep(self, seconds):
        """Interruptible sleep. -> True if we were asked to stop."""
        if self.stop is not None:
            return bool(self.stop.wait(seconds))
        time.sleep(seconds)
        return False

    def say(self, msg):
        if self._log:
            try:
                self._log(msg)
                return
            except Exception:
                pass
        try:
            sys.stdout.write("%s %s\n" % (
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), msg))
            sys.stdout.flush()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# the NPU lease (same meta keys server.py and pipeline.py already use)
# --------------------------------------------------------------------------- #

def _meta_float(con, key, default=0.0):
    try:
        return float(db.get_meta(con, key, "0") or 0)
    except (TypeError, ValueError, sqlite3.Error):
        return default


def _set(con, key, value):
    try:
        db.set_meta(con, key, value)
    except Exception:
        pass


def wait_for_quiet(con, ctx, timeout=LEASE_WAIT_S):
    """Block until nobody else holds the NPU. -> True if it is ours.

    BOTH keys, not just img_hold_until: pipeline._label_clusters and the hourly
    sitrep advertise only enrich_busy_until, and the pending==0 gate in run_due
    does not cover them -- _label_clusters runs after the batch that emptied the
    queue, so a job could take the device while a label generation was in flight.
    """
    deadline = time.time() + max(0.0, timeout)
    while True:
        if ctx.stopped():
            return False
        busy = max(_meta_float(con, "img_hold_until"),
                   _meta_float(con, "enrich_busy_until"))
        if time.time() >= busy:
            return True
        if time.time() >= deadline:
            return False
        if ctx.sleep(1.5):
            return False


def _release(con, key, mine):
    """Clear a lease key only if we still hold it.

    These keys are shared across processes -- server.py's on-demand render and
    pipeline.py's enricher write them from their own connections. A blind write of
    "0" in a finally block zeroes out somebody else's live lease: a job finishing
    while server.py is mid-Z-Image would drop img_hold_until, the enricher (which
    polls that key) would fire a chat call into the running render, and that is
    exactly the device 150004 collision the lease exists to prevent."""
    try:
        cur = db.get_meta(con, key, "0")
    except Exception:
        cur = None
    if cur is not None and str(cur) != mine:
        return False              # somebody else took it after us; leave it alone
    _set(con, key, "0")
    return True


@contextmanager
def lease(con, ctx, label, seconds=CHAT_LEASE_S):
    """Hold the NPU for one job. Always released, even on exception.

    BOTH keys are taken, for chat work as well as image work. `img_hold_until` is
    the key every consumer actually honours -- pipeline.enrich_body,
    pipeline._label_clusters and jobs.wait_for_quiet all poll it and nothing else.
    `enrich_busy_until` is advertised for server.py's on-demand image endpoint (and
    the board's now_doing staleness check), which is the only reader it has.
    Setting only enrich_busy_until for a 40-250s generation left the enricher free
    to start a second chat call on the same device handle the moment a fetch cycle
    dropped new items in the queue.
    """
    until = "%.3f" % (time.time() + float(seconds))
    _set(con, "enrich_busy_until", until)
    _set(con, "img_hold_until", until)
    _set(con, "now_doing", str(label)[:120])
    try:
        yield
    finally:
        _release(con, "enrich_busy_until", until)
        _release(con, "img_hold_until", until)
        _set(con, "now_doing", "")


# --------------------------------------------------------------------------- #
# small helpers
# --------------------------------------------------------------------------- #

def _clean(text, limit=4000):
    return _WS_RE.sub(" ", str(text or "")).strip()[:limit]


def _body(res, field, limit):
    """The model's prose for `field`, or "" if it did not return a string.

    str() on a list or dict would stringify the repr straight into a stored
    document body; an empty result is what the callers already handle correctly.
    """
    val = (res or {}).get(field) if isinstance(res, dict) else None
    return _clean(val, limit) if isinstance(val, str) else ""


def _strlist(res, field, count, limit):
    """Up to `count` cleaned strings from a model-supplied list field.

    Slicing whatever the model returned was wrong in both directions: a string
    ("a, b, c")[:3] yields "a, " and iterates into three one-character "watch
    items"; a dict raises TypeError: unhashable type 'slice', which run_due
    swallows but the --job / --brief CLI paths turn into a traceback. Anything
    that is not a list is treated as absent, same as _countries already does.
    """
    val = (res or {}).get(field) if isinstance(res, dict) else None
    if not isinstance(val, list):
        return []
    out = []
    for item in val[:max(0, int(count))]:
        if not isinstance(item, (str, int, float)) or isinstance(item, bool):
            continue
        text = _clean(item, limit)
        if text:
            out.append(text)
    return out


def _sev(row, default=1):
    try:
        return max(1, min(5, int(row["severity"])))
    except (TypeError, ValueError, KeyError, IndexError):
        return default


def _countries(raw):
    """items.countries is a JSON array written by the model. Never trust it."""
    if not raw:
        return []
    try:
        val = json.loads(raw)
    except (TypeError, ValueError):
        return []
    if not isinstance(val, list):
        return []
    out = []
    for c in val[:12]:
        name = _clean(c, 60)
        if name and name.lower() not in _ENTITY_SKIP:
            out.append(name)
    return out


def _wire_lines(rows, limit=None):
    lines = []
    for r in rows[:limit or len(rows)]:
        lines.append("- [S%d/%s/%s] %s :: %s (%s)" % (
            _sev(r), r["region"] or "GLOBAL", r["category"] or "politics",
            _clean(r["title"], 180), _clean(r["summary"], 220), r["source"]))
    return "\n".join(lines)


def _ask(ctx, con, system, user, max_tokens, label):
    """One leased chat call. -> (parsed_dict|None, stats)."""
    if not wait_for_quiet(con, ctx):
        return None, {"error": "npu busy"}
    with lease(con, ctx, label):
        try:
            return ctx.device().chat_json(system, user, max_tokens=max_tokens)
        except Exception as exc:
            return None, {"error": "%s: %s" % (type(exc).__name__, exc)}


def _doc_stats(stats):
    stats = stats if isinstance(stats, dict) else {}
    out = {}
    for k in ("gen_tps", "ms", "tokens_out"):
        v = stats.get(k)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            out[k] = round(float(v), 2)
    return out


def _bump(con, key, amount=1):
    """Accumulate a meta counter. Only jobs.py writes the job_* totals."""
    try:
        cur = float(db.get_meta(con, key, "0") or 0)
    except (TypeError, ValueError):
        cur = 0.0
    _set(con, key, "%d" % int(cur + amount))


def _attempt_key(kind, subject):
    return "job_try_%s_%s" % (kind, str(subject).lower().replace(" ", "_")[:60])


def _last_touch(con, kind, subject, doc_ts=0.0):
    """When this subject was last WORKED, whether or not the model produced text.

    Rotation must key off attempts, not successes. A region whose generation keeps
    timing out never gets a doc, so a success-only clock leaves its last_ts pinned
    at 0 -- it wins the "oldest" race every single pass and the other six AORs are
    never synthesised at all. (Seen live: SOUTHCOM failed twice and was re-picked
    both times while EUCOM, with six times the traffic, never got a turn.)
    """
    return max(float(doc_ts or 0.0), _meta_float(con, _attempt_key(kind, subject), 0.0))


def _mark_attempt(con, kind, subject):
    _set(con, _attempt_key(kind, subject), "%.3f" % time.time())


def utc_day(ts=None):
    return time.strftime("%Y-%m-%d", time.gmtime(ts if ts is not None else time.time()))


def day_bounds(day):
    """UTC midnight-to-midnight epoch bounds for 'YYYY-MM-DD'."""
    t = time.strptime(day, "%Y-%m-%d")
    # calendar is imported at the top rather than inline: a dynamic __import__ is
    # one of the things tiinyapp.farm's archive scanner refuses, and it was only
    # ever inline to keep the import list short.
    start = int(calendar.timegm(t))
    return float(start), float(start + 86400)


# --------------------------------------------------------------------------- #
# job 1: recluster — DB sweep + cluster labelling
# --------------------------------------------------------------------------- #

def _already_labelled(con, cluster_id):
    """True if some other thread has written this cluster's label meanwhile."""
    try:
        row = con.execute("SELECT label FROM clusters WHERE id=?",
                          (cluster_id,)).fetchone()
    except sqlite3.Error:
        return False
    return bool(row is not None and (row["label"] or "").strip())


def job_recluster(con, ctx, force=False):
    """Give orphaned enriched items a cluster, then label clusters that earned one.

    Enrichment assigns a cluster inline, but items enriched during a device blip,
    or before the embedding model came up, land with cluster_id NULL and stay
    invisible to the DEVELOPING panel forever. This is the sweeper.
    """
    since = time.time() - RECLUSTER_WINDOW_S
    rows = con.execute(
        "SELECT id, embedding FROM items WHERE enriched_at IS NOT NULL"
        " AND cluster_id IS NULL AND COALESCE(published, fetched_at) >= ?"
        " ORDER BY COALESCE(published, fetched_at) DESC LIMIT ?",
        (since, RECLUSTER_BATCH)).fetchall()

    # Load the comparison window ONCE for the whole batch. assign_cluster would
    # otherwise re-read every clustered item of the last 72h, embedding blobs and
    # all, for each of up to RECLUSTER_BATCH=40 orphans -- the same multi-megabyte
    # result set forty times, scored with a pure-Python cosine, every 30 minutes,
    # competing with the device poller and the enricher for the Pi's CPU.
    window = None
    if any(r["embedding"] for r in rows):
        try:
            window = list(db.clustered_embeddings(
                con, time.time() - enrich.CLUSTER_WINDOW_S) or [])
        except Exception:
            window = None            # fall back to per-call loading

    assigned = 0
    for row in rows:
        if ctx.stopped():
            break
        try:
            cid = enrich.assign_cluster(con, row["id"], row["embedding"],
                                        members=window)
            if cid is not None:
                assigned += 1
                if window is not None and row["embedding"]:
                    # keep the local window current so later orphans in this batch
                    # can cluster onto earlier ones, exactly as they would have
                    window.append((row["id"], cid, row["embedding"]))
        except Exception as exc:
            ctx.say("[recluster] item %s failed: %s" % (row["id"], str(exc)[:100]))

    # Labelling is the only device work here, and only for clusters that grew.
    labeled = 0
    try:
        cands = con.execute(
            "SELECT id, item_count FROM clusters WHERE (label IS NULL OR label='')"
            " AND item_count >= ? ORDER BY top_severity DESC, updated_at DESC LIMIT ?",
            (LABEL_MIN_ITEMS, LABEL_PER_PASS)).fetchall()
    except sqlite3.Error:
        cands = []
    for c in cands:
        if ctx.stopped():
            break
        titles = [r["title"] for r in con.execute(
            "SELECT title FROM items WHERE cluster_id=?"
            " ORDER BY COALESCE(published, fetched_at) DESC LIMIT 6",
            (c["id"],)).fetchall()]
        if len(titles) < 2:
            continue
        if not wait_for_quiet(con, ctx):
            break
        # pipeline._label_clusters runs the same candidate query from the enricher
        # thread after every batch. wait_for_quiet above can block for up to
        # LEASE_WAIT_S, which is plenty of time for it to have labelled this very
        # cluster -- re-read before paying the device for a generation whose only
        # effect would be to overwrite the label that already exists.
        if _already_labelled(con, c["id"]):
            continue
        with lease(con, ctx, "LABELLING DEVELOPING STORY #%d" % c["id"], seconds=120):
            try:
                label = enrich.label_cluster(ctx.device(), titles)
            except Exception as exc:
                ctx.say("[recluster] label #%s failed: %s" % (c["id"], str(exc)[:100]))
                label = None
        if label:
            try:
                db.upsert_cluster(con, c["id"], label, None, time.time())
                labeled += 1
                db.add_event(con, "CLUSTER", "labelled developing story #%d: %s"
                             % (c["id"], _clean(label, 90)))
            except Exception as exc:
                ctx.say("[recluster] label write #%s failed: %s" % (c["id"], exc))

    if assigned or labeled:
        db.add_event(con, "RECLUSTER",
                     "recluster sweep: %d orphaned item(s) joined a story, %d story label(s) written"
                     % (assigned, labeled))
        _bump(con, "job_recluster_total", 1)
        ctx.say("[recluster] assigned=%d labeled=%d" % (assigned, labeled))
    return {"job": "recluster", "assigned": assigned, "labeled": labeled,
            "did_work": bool(assigned or labeled)}


# --------------------------------------------------------------------------- #
# job 2: regional synthesis
# --------------------------------------------------------------------------- #

_SYNTH_SYSTEM = (
    "You are the senior analyst on a 24/7 OSINT watch floor. You write regional "
    "assessments for a wall board: dense, declarative, no hedging, no preamble, "
    "no markdown. You never invent facts that are not in the wire items given."
)

_SYNTH_PROMPT = (
    "Wire items from the last 24 hours in the %s area of responsibility.\n\n%s\n\n"
    "Write the regional assessment. Return ONE JSON object and nothing else:\n"
    '{"headline": "<= 9 words, the single dominant development">, '
    '"assessment": "<180-260 words: what happened, what connects, what it means>", '
    '"watch": ["<= 3 short items to watch next"]}'
)


def _pick_region(con, force=False):
    """The AOR with enough traffic whose last synthesis is oldest. -> (region, rows)."""
    now = time.time()
    since = now - SYNTHESIS_WINDOW_S
    best = None
    for region in REGIONS:
        rows = con.execute(
            "SELECT id, title, summary, source, region, category, severity"
            " FROM items WHERE enriched_at IS NOT NULL AND region = ?"
            " AND COALESCE(published, fetched_at) >= ?"
            " ORDER BY severity DESC, COALESCE(published, fetched_at) DESC LIMIT 40",
            (region, since)).fetchall()
        if len(rows) < SYNTHESIS_MIN_ITEMS:
            continue
        last = db.latest_doc(con, "synthesis", region)
        doc_ts = 0.0
        if last is not None:
            try:
                doc_ts = float(last["created_at"] or 0)
            except (TypeError, ValueError, KeyError):
                doc_ts = 0.0
        last_ts = _last_touch(con, "synthesis", region, doc_ts)
        if not force and (now - last_ts) < SYNTHESIS_COOLDOWN_S:
            continue
        if best is None or last_ts < best[2]:
            best = (region, rows, last_ts)
    if best is None:
        return None, []
    return best[0], best[1]


def job_synthesis(con, ctx, force=False, region=None):
    """Read one AOR's last 24h into a regional assessment."""
    if region:
        rows = con.execute(
            "SELECT id, title, summary, source, region, category, severity"
            " FROM items WHERE enriched_at IS NOT NULL AND region = ?"
            " AND COALESCE(published, fetched_at) >= ?"
            " ORDER BY severity DESC, COALESCE(published, fetched_at) DESC LIMIT 40",
            (region, time.time() - SYNTHESIS_WINDOW_S)).fetchall()
    else:
        region, rows = _pick_region(con, force=force)
    if not region or len(rows) < (1 if force else SYNTHESIS_MIN_ITEMS):
        return {"job": "synthesis", "did_work": False, "reason": "no region with traffic"}

    _mark_attempt(con, "synthesis", region)   # before the call: a crash still rotates
    res, stats = _ask(ctx, con, _SYNTH_SYSTEM,
                      _SYNTH_PROMPT % (region, _wire_lines(rows)),
                      1200, "SYNTHESISING %s — ORNITH-35B" % region)
    body = _body(res, "assessment", 4000)
    if not body:
        ctx.say("[synthesis] %s produced nothing (%s)"
                % (region, (stats or {}).get("error", "empty content")))
        return {"job": "synthesis", "did_work": False, "region": region,
                "reason": (stats or {}).get("error") or "empty"}

    headline = _body(res, "headline", 120) or ("%s assessment" % region)
    watch = _strlist(res, "watch", 3, 140)
    if watch:
        body = body + "\n\nWatch: " + "; ".join(watch)

    meta = _doc_stats(stats)
    meta["ids"] = [r["id"] for r in rows[:40]]
    doc_id = db.put_doc(con, "synthesis", region, headline, body,
                        item_count=len(rows), meta=meta)
    db.add_event(con, "SYNTH", "%s regional synthesis: %s (%d wire items)"
                 % (region, headline, len(rows)))
    _bump(con, "job_synthesis_total", 1)
    ctx.say("[synthesis] %s doc=%s %d chars from %d items"
            % (region, doc_id, len(body), len(rows)))
    return {"job": "synthesis", "did_work": True, "region": region, "doc_id": doc_id,
            "headline": headline, "chars": len(body), "items": len(rows)}


# --------------------------------------------------------------------------- #
# job 3: entity dossier
# --------------------------------------------------------------------------- #

_DOSSIER_SYSTEM = (
    "You are a targeting-desk analyst compiling an entity dossier from open "
    "sources. You are precise, unhedged and you never state anything the supplied "
    "reporting does not support. No preamble, no markdown."
)

_DOSSIER_PROMPT = (
    "Entity: %s\nOpen-source reporting from the last 14 days that mentions it:\n\n%s\n\n"
    "Compile the dossier. Return ONE JSON object and nothing else:\n"
    '{"headline": "<= 9 words on this entity\'s current posture", '
    '"dossier": "<200-320 words: current situation, actors involved, trajectory>", '
    '"threads": ["<= 3 named developing threads"]}'
)


def _hot_entity(con, force=False):
    """Most severity-weighted entity on the wire that is off cooldown.

    Weight is sum(severity^2) over the window, so one S5 outranks a drizzle of
    S1 mentions -- the dossier should follow the crisis, not the news volume.
    """
    now = time.time()
    rows = con.execute(
        "SELECT countries, severity FROM items WHERE enriched_at IS NOT NULL"
        " AND countries IS NOT NULL AND COALESCE(published, fetched_at) >= ?"
        " ORDER BY COALESCE(published, fetched_at) DESC LIMIT 1200",
        (now - DOSSIER_WINDOW_S,)).fetchall()
    weight, seen = {}, {}
    for r in rows:
        sev = _sev(r)
        for name in _countries(r["countries"]):
            key = name.title()
            weight[key] = weight.get(key, 0) + sev * sev
            seen[key] = seen.get(key, 0) + 1
    ranked = sorted(weight.items(), key=lambda kv: (-kv[1], kv[0]))
    for name, _w in ranked:
        if seen.get(name, 0) < DOSSIER_MIN_ITEMS and not force:
            continue
        last = db.latest_doc(con, "dossier", name)
        doc_ts = 0.0
        if last is not None:
            try:
                doc_ts = float(last["created_at"] or 0)
            except (TypeError, ValueError, KeyError):
                doc_ts = 0.0
        # attempt-aware, same reason as _pick_region: an entity whose generation
        # keeps failing must not monopolise the dossier slot forever
        if not force and (now - _last_touch(con, "dossier", name, doc_ts)) < DOSSIER_COOLDOWN_S:
            continue
        return name
    return ranked[0][0] if (force and ranked) else None


def _entity_items(con, entity, limit=32):
    """Items whose countries array names the entity. LIKE on the JSON text is a
    prefilter only -- every row is re-checked against the parsed array."""
    rows = con.execute(
        "SELECT id, title, summary, source, region, category, severity, countries"
        " FROM items WHERE enriched_at IS NOT NULL"
        " AND COALESCE(published, fetched_at) >= ? AND countries LIKE ?"
        " ORDER BY severity DESC, COALESCE(published, fetched_at) DESC LIMIT 200",
        (time.time() - DOSSIER_WINDOW_S, "%" + entity + "%")).fetchall()
    want = entity.lower()
    out = [r for r in rows if any(c.lower() == want for c in _countries(r["countries"]))]
    return out[:limit]


def job_dossier(con, ctx, force=False, entity=None):
    """Write up the entity the wire is most worked up about."""
    entity = entity or _hot_entity(con, force=force)
    if not entity:
        return {"job": "dossier", "did_work": False, "reason": "no entity off cooldown"}
    rows = _entity_items(con, entity)
    if len(rows) < (1 if force else DOSSIER_MIN_ITEMS):
        return {"job": "dossier", "did_work": False, "entity": entity,
                "reason": "only %d item(s)" % len(rows)}

    _mark_attempt(con, "dossier", entity)     # before the call: a crash still rotates
    res, stats = _ask(ctx, con, _DOSSIER_SYSTEM,
                      _DOSSIER_PROMPT % (entity, _wire_lines(rows)),
                      1400, "COMPILING DOSSIER: %s — ORNITH-35B" % entity.upper())
    body = _body(res, "dossier", 5000)
    if not body:
        ctx.say("[dossier] %s produced nothing (%s)"
                % (entity, (stats or {}).get("error", "empty content")))
        return {"job": "dossier", "did_work": False, "entity": entity,
                "reason": (stats or {}).get("error") or "empty"}

    headline = _body(res, "headline", 120) or ("%s dossier" % entity)
    threads = _strlist(res, "threads", 3, 140)
    if threads:
        body = body + "\n\nThreads: " + "; ".join(threads)

    meta = _doc_stats(stats)
    meta["ids"] = [r["id"] for r in rows]
    doc_id = db.put_doc(con, "dossier", entity, headline, body,
                        item_count=len(rows), meta=meta)
    db.add_event(con, "DOSSIER", "entity dossier compiled: %s — %s (%d reports)"
                 % (entity, headline, len(rows)))
    _bump(con, "job_dossier_total", 1)
    ctx.say("[dossier] %s doc=%s %d chars from %d items"
            % (entity, doc_id, len(body), len(rows)))
    return {"job": "dossier", "did_work": True, "entity": entity, "doc_id": doc_id,
            "headline": headline, "chars": len(body), "items": len(rows)}


# --------------------------------------------------------------------------- #
# job 4: image backfill  (was pipeline._autoimage_one)
# --------------------------------------------------------------------------- #

IMAGE_RETRY_AFTER_S = _env_float("DAYBREAK_IMAGE_RETRY_AFTER", 6 * 3600.0)

# Severity floor for AUTOMATIC rendering. The click-to-render path in the server ignores
# this — a reader asking for a picture always gets one. At the old hardcoded 4 nothing on
# the visible wire qualified, because a normal hour is mostly S2 and S3.
IMAGE_MIN_SEVERITY = _env_int("DAYBREAK_IMAGE_MIN_SEVERITY", 3)

# How far back the picker will look. Newest-first ordering means this is a backfill depth,
# not a deadline: the top of the board is always covered first and the window only decides
# how far down it keeps going once it is caught up.
IMAGE_WINDOW_S = _env_float("DAYBREAK_IMAGE_WINDOW", 7 * 86400.0)

# Candidates examined per pass. Rows already holding a rendered file are skipped in
# Python, so once the recent stretch is covered the scan has to reach past it to find the
# next gap. Three small columns, so a generous limit is cheaper than missing the gap.
IMAGE_SCAN_LIMIT = _env_int("DAYBREAK_IMAGE_SCAN_LIMIT", 400)
_OVER_CAP_EVENT_S = 3600.0     # oplog rate limit for the over-cap notice


def _daily_budget(con):
    """(remaining_today, reason) -- the cheap half. Rolls the day counter over."""
    today = utc_day()
    if db.get_meta(con, "autoimg_day") != today:
        _set(con, "autoimg_day", today)
        _set(con, "autoimg_count", "0")
    try:
        done = int(db.get_meta(con, "autoimg_count", "0") or 0)
    except (TypeError, ValueError):
        done = 0
    if done >= IMAGE_DAILY_CAP:
        return 0, "daily cap %d reached" % IMAGE_DAILY_CAP
    return IMAGE_DAILY_CAP - done, ""


def _disk_over_cap(con, ctx):
    """"" when there is room, else the reason the disk cap says no.

    Reads the janitor's cached verdict (db.image_cap_state) rather than running a
    prune of its own: a full scandir + stat + chunked severity lookup -- which can
    also DELETE files -- has no business inside a budget check that runs ~79 times
    a day. Only when there is no fresh reading does it fall back to the sweep."""
    try:
        state = db.image_cap_state(con)
        if state.get("over_cap") is None:
            state = None
    except Exception:
        state = None
    if state is None:
        try:
            img = db.prune_images(con, ctx.images_dir) or {}
            state = {"over_cap": bool(img.get("over_cap")),
                     "bytes": img.get("bytes_after") or 0}
        except Exception:
            return ""
    if not state.get("over_cap"):
        return ""
    return "image cache over disk cap (%.1f GB)" % ((state.get("bytes") or 0) / 1e9)


def _note_over_cap(con, ctx, reason):
    """Make the over-cap stall visible. It used to live only in a dict nobody
    surfaced: job_image returned did_work=False, the board showed nothing, and the
    operator found out when the disk filled."""
    last = _meta_float(con, "img_over_cap_event_ts", 0.0)
    if time.time() - last < _OVER_CAP_EVENT_S:
        return
    _set(con, "img_over_cap_event_ts", "%.3f" % time.time())
    try:
        db.add_event(con, "IMAGE", "image backfill paused: %s — raise "
                     "DAYBREAK_IMAGE_CAP_GB or lower DAYBREAK_IMAGE_PROTECT_DAYS"
                     % reason)
    except Exception:
        pass
    ctx.say("[image] %s" % reason)


def _image_fail_key(item_id):
    return "img_fail_%d" % int(item_id)


# --------------------------------------------------------------------------- #
# geocode: put the incident where it actually happened
# --------------------------------------------------------------------------- #

GEO_BATCH = _env_int("DAYBREAK_GEO_BATCH", 6)
GEO_INTERVAL_S = _env_float("DAYBREAK_GEO_INTERVAL", 240.0)
GEO_MIN_SEVERITY = _env_int("DAYBREAK_GEO_MIN_SEVERITY", 2)
_GAZ = {"loaded": False, "iso": {}, "cities": {}}

# Suffixes a newsroom writes and a gazetteer does not: "Gaza City", "Idlib Province".
GEO_SUFFIX = (" city", " province", " governorate", " district", " region", " oblast",
              " county", " prefecture", " municipality", " metropolitan area")


def _geo_norm(s):
    """Collapse transliteration variance. See gazetteer build notes."""
    s = unicodedata.normalize("NFKD", str(s or "").lower())
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = s.replace("y", "i")
    s = re.sub(r"(.)\1+", r"\1", s)
    return " ".join(s.split())


def _gaz():
    if not _GAZ["loaded"]:
        _GAZ["loaded"] = True
        try:
            with open(os.path.join(BASE_DIR, "gazetteer.json")) as fh:
                d = json.load(fh)
            _GAZ["iso"] = d.get("iso") or {}
            _GAZ["cities"] = d.get("cities") or {}
        except (OSError, ValueError):
            pass                          # no gazetteer: the job simply finds nothing
    return _GAZ


def geo_lookup(place, countries):
    """(lat, lon) for a place name, constrained to the countries already tagged.

    The country constraint is what makes this safe. 'Nice', 'Reading' and 'Mobile' are
    all cities and all ordinary English words; scoping the search to countries the
    enricher already identified removes almost every way to be confidently wrong.
    """
    g = _gaz()
    if not g["cities"] or not place:
        return None
    ccs = []
    for c in (countries or []):
        iso = g["iso"].get(str(c).strip().lower())
        if iso and iso not in ccs:
            ccs.append(iso)
    if not ccs:
        return None
    cand = _geo_norm(place)
    tries = [cand]
    for suf in GEO_SUFFIX:
        if cand.endswith(_geo_norm(suf)):
            tries.append(cand[: -len(_geo_norm(suf))].strip())
    # "Kryvyi Rih, Dnipropetrovsk" -> try the leading component too
    if "," in str(place):
        tries.append(_geo_norm(str(place).split(",")[0]))
    for cc in ccs:
        table = g["cities"].get(cc) or {}
        for t in tries:
            if t and t in table:
                return table[t]
    return None


def geo_scan(text, countries):
    """Find a city name in the text itself, scoped to the tagged countries.

    The model is asked not to answer with a country and sometimes does anyway, and
    sometimes returns NONE for a headline that plainly names a city. Scanning the text
    catches those. It is only safe because of two constraints: the candidate must be
    Capitalised in the original (so "nice" the adjective cannot match Nice the city) and
    the search is limited to countries the enricher already identified.
    """
    g = _gaz()
    if not g["cities"] or not text:
        return None, None
    ccs = [g["iso"].get(str(c).strip().lower()) for c in (countries or [])]
    ccs = [c for c in ccs if c]
    if not ccs:
        return None, None
    # {2,} not {3,}: at four characters minimum, "Rih" is never captured, so the pair
    # "Kryvyi Rih" can never form. Short tokens are filtered by the key length below.
    words = re.findall(r"\b[A-Z][A-Za-z'’\-]{2,}\b", str(text))
    # try two-word names first: "Kryvyi Rih" must beat "Kryvyi"
    cands = []
    for i, w in enumerate(words):
        if i + 1 < len(words):
            cands.append(w + " " + words[i + 1])
    cands.extend(words)
    for cc in ccs:
        table = g["cities"].get(cc) or {}
        for cand in cands:
            # Length is judged on the candidate, not the normalised key: normalising
            # "Kyiv" collapses it to "kiv", which a key-length guard would reject.
            if len(cand) < 4:
                continue
            key = _geo_norm(cand)
            if key and key in table:
                return cand, table[key]
    return None, None


GEO_SYSTEM = (
    "You are given a news headline and summary. Name the single most specific place the "
    "event happened: a city or town if one is identifiable, otherwise a province or "
    "region. Do not name a country on its own. If no place below country level can be "
    "determined, reply NONE.\n\n"
    "Reply with exactly one line and nothing else:\nPLACE: <name>")


def job_geocode(con, ctx, force=False):
    """Give recent significant items a real coordinate.

    Deliberately a background job rather than part of enrichment: enrichment is the
    critical path that keeps the wire moving, and a geocode failure must never be able
    to hold up an article being read and graded.
    """
    if not _gaz()["cities"]:
        return {"job": "geocode", "did_work": False, "reason": "no gazetteer"}
    rows = con.execute(
        "SELECT id, title, summary, countries FROM items "
        "WHERE enriched_at IS NOT NULL AND geo_at IS NULL AND severity >= ? "
        "ORDER BY severity DESC, COALESCE(published, fetched_at) DESC LIMIT ?",
        (GEO_MIN_SEVERITY, GEO_BATCH)).fetchall()
    if not rows:
        return {"job": "geocode", "did_work": False, "reason": "nothing to place"}

    dev = ctx.device()
    done = hit = 0
    for r in rows:
        try:
            countries = json.loads(r["countries"] or "[]")
        except (ValueError, TypeError):
            countries = []
        place = None
        if countries:                     # no country tag means nothing to constrain to
            try:
                with lease(con, ctx, "PLACING #%d" % r["id"], seconds=90):
                    obj, _s = dev.chat_json(
                        GEO_SYSTEM + '\n\nReturn ONLY JSON: {"line": "<the PLACE line>"}',
                        "%s\n%s" % (_clean(r["title"], 200),
                                     _clean(r["summary"] or "", 300)), max_tokens=700)
                    out = (obj or {}).get("line")
            except Exception as exc:
                ctx.say("[geo] #%d failed: %s" % (r["id"], str(exc)[:80]))
                out = None
            for line in str(out or "").splitlines():
                if line.strip().upper().startswith("PLACE:"):
                    place = line.split(":", 1)[1].strip()
                    break
        coord = geo_lookup(place, countries) if place and place.upper() != "NONE" else None
        if not coord and countries:
            # Second pass: the model either declined or handed back a country. Look for
            # a city named in the copy itself.
            found, coord2 = geo_scan("%s. %s" % (r["title"] or "", r["summary"] or ""),
                                     countries)
            if coord2:
                place, coord = found, coord2
        if coord:
            con.execute("UPDATE items SET place=?, lat=?, lon=?, geo_at=? WHERE id=?",
                        (place[:80], coord[0], coord[1], time.time(), r["id"]))
            hit += 1
        else:
            # Stamp anyway so an unplaceable item is not retried forever.
            con.execute("UPDATE items SET geo_at=?, place=? WHERE id=?",
                        (time.time(), (place or "")[:80] or None, r["id"]))
        done += 1
    con.commit()
    if hit:
        db.add_event(con, "GEO", "placed %d of %d items on the map" % (hit, done))
    return {"job": "geocode", "did_work": bool(done), "checked": done, "placed": hit}


# --------------------------------------------------------------------------- #
# analyst: the standing read across the whole board
# --------------------------------------------------------------------------- #

ANALYST_INTERVAL_S = _env_float("DAYBREAK_ANALYST_INTERVAL", 5400.0)
ANALYST_WINDOW_S = _env_float("DAYBREAK_ANALYST_WINDOW", 24 * 3600.0)

ANALYST_SYSTEM = (
    "You are the senior analyst on an OSINT desk. You are given the last day of graded "
    "wire traffic, the standing regional assessments, and your OWN previous brief. "
    "Write the analyst's brief: not what happened, but what it means.\n\n"
    "Reply as labelled lines, exactly this shape and nothing else:\n\n"
    "BLUF: one sentence. The single judgement that matters most today.\n"
    "PICTURE: 3-4 sentences on the overall situation, connecting developments across "
    "theatres where they are actually connected. Do not simply list regions.\n"
    "DRIVER: the one dynamic doing most to shape events right now, named plainly.\n"
    "SHIFT: what has moved since your previous brief. If a previous brief is supplied "
    "and nothing material has moved, say so. Omit this line if there is no previous "
    "brief.\n"
    "RISK: the most consequential thing that could plausibly happen next, with a rough "
    "likelihood in words, not numbers you cannot support.\n"
    "WATCH: two or three specific indicators worth checking tomorrow, semicolon "
    "separated. Each must be observable, not a mood.\n"
    "BLIND: what this desk cannot see that would change the assessment - a region with "
    "thin sourcing, a story with only one source, an obvious gap.\n\n"
    "Be concrete and commit to a judgement. An analyst who only hedges is useless. "
    "Do not invent facts that are not in the material.")

ANALYST_FIELDS = ("bluf", "picture", "driver", "shift", "risk", "watch", "blind")


def _analyst_parse(raw):
    out, cur = {}, None
    for line in str(raw or "").splitlines():
        line = line.strip()
        if not line:
            continue
        head, sep, rest = line.partition(":")
        key = head.strip().lower()
        if sep and key in ANALYST_FIELDS:
            cur = key
            out[cur] = " ".join(rest.split())
        elif cur and len(out.get(cur, "")) < 900:
            out[cur] = (out[cur] + " " + " ".join(line.split())).strip()
    for k in list(out):
        out[k] = out[k][:900]
    return out if out.get("bluf") and out.get("picture") else None


def job_analyst(con, ctx, force=False):
    """The standing brief across every theatre, written against its own last one.

    Distinct from the SITREP, which reports; this one assesses. It is also the only job
    that is shown its own previous output, which is what lets it say what has shifted
    rather than restating the same picture every ninety minutes.
    """
    since = time.time() - ANALYST_WINDOW_S
    rows = con.execute(
        "SELECT title, summary, region, category, severity FROM items "
        "WHERE enriched_at IS NOT NULL AND COALESCE(published, fetched_at) >= ? "
        "ORDER BY severity DESC, COALESCE(published, fetched_at) DESC LIMIT 60",
        (since,)).fetchall()
    if len(rows) < 10:
        return {"job": "analyst", "did_work": False, "reason": "not enough traffic"}

    wire = "\n".join(
        "- [S%s/%s/%s] %s: %s" % (r["severity"], r["region"], r["category"], r["title"],
                                  (r["summary"] or "")[:120]) for r in rows)
    synth = ""
    for d in con.execute(
            "SELECT subject, title, body FROM docs WHERE kind='synthesis' "
            "AND created_at >= ? ORDER BY created_at DESC LIMIT 6", (since,)).fetchall():
        synth += "- %s: %s\n  %s\n" % (d["subject"], d["title"] or "",
                                        (d["body"] or "")[:260])

    prev = con.execute(
        "SELECT title, body, created_at FROM docs WHERE kind='analyst' "
        "ORDER BY created_at DESC LIMIT 1").fetchone()
    prior = ""
    if prev is not None:
        try:
            pj = json.loads(prev["body"] or "{}")
        except (ValueError, TypeError):
            pj = {}
        if pj.get("bluf"):
            hrs = (time.time() - float(prev["created_at"])) / 3600.0
            prior = ("\n\nYOUR PREVIOUS BRIEF (%.0f hours ago):\nBLUF: %s\nPICTURE: %s"
                     % (hrs, pj.get("bluf", ""), pj.get("picture", "")[:500]))

    user = ("WIRE TRAFFIC, LAST 24 HOURS:\n%s\n\nSTANDING REGIONAL ASSESSMENTS:\n%s%s"
            % (wire, synth or "(none filed)", prior))

    with lease(con, ctx, "WRITING ANALYST BRIEF — ORNITH-35B", seconds=420):
        obj, stats = ctx.device().chat_json(
            ANALYST_SYSTEM + '\n\nReturn ONLY JSON: {"report": "<the labelled lines, '
            'newlines as \\n>"}', user, max_tokens=3000)
    parsed = _analyst_parse((obj or {}).get("report"))
    if not parsed:
        return {"job": "analyst", "did_work": False, "reason": "no usable brief"}

    meta = _doc_stats(stats)
    meta["items"] = len(rows)
    meta["prior"] = bool(prior)
    db.put_doc(con, "analyst", time.strftime("%Y-%m-%dT%H:%MZ", time.gmtime()),
               parsed.get("bluf", "")[:200], json.dumps(parsed), len(rows),
               json.dumps(meta))
    db.add_event(con, "ANALYST", "analyst brief filed (%d items, %s prior)"
                 % (len(rows), "with" if prior else "no"))
    ctx.say("[analyst] brief filed from %d items%s" % (len(rows), " (with prior)" if prior else ""))
    return {"job": "analyst", "did_work": True, "items": len(rows),
            "bluf": parsed.get("bluf", "")[:120]}


# Desk reports are the filler this board was missing. 59 feeds deliver ~42 stories an
# hour, which costs the device about two minutes of grading, leaving it idle for the
# other fifty-eight. Meanwhile 885 graded stories have no written report and each one is
# ~215s of genuine work. So: when nothing else is due and the queue is empty, write one.
# A reader who clicks an article then finds it already written instead of waiting three
# minutes for it, which is the same argument as pre-rendering the artwork.
STORY_INTERVAL_S = float(_env_int("DAYBREAK_STORY_INTERVAL", 90))
STORY_DAILY_CAP = _env_int("DAYBREAK_STORY_DAILY_CAP", 120)
STORY_MIN_SEV = _env_int("DAYBREAK_STORY_MIN_SEV", 3)


def job_story(con, ctx, force=False):
    """Write one missing desk report. The lowest-priority job on the board.

    Deliberately yields to everything: a report holds the device for ~215s across three
    sectioned calls, which is an age next to a 3s grading. It only runs with an empty
    enrichment queue, so live traffic always wins, and a newly arrived batch preempts
    the backfill on the next pass rather than queueing behind a report already running.
    """
    try:
        import research
    except Exception as exc:
        return {"job": "story", "did_work": False, "reason": "research unavailable: %s" % exc}
    if not research.available():
        return {"job": "story", "did_work": False, "reason": "no TinyFish key"}

    if not force and queue_pending(con) > 0:
        return {"job": "story", "did_work": False, "reason": "live traffic first"}

    since = time.time() - 86400
    done = con.execute("SELECT COUNT(*) FROM story WHERE created_at >= ?", (since,)).fetchone()[0]
    if done >= STORY_DAILY_CAP and not force:
        return {"job": "story", "did_work": False,
                "reason": "daily cap %d reached" % STORY_DAILY_CAP}

    # Newest first, same reasoning as the image backfill: what a reader opens the page
    # to is the top of the wire, so that is what earns a report first.
    row = con.execute(
        "SELECT i.* FROM items i WHERE i.severity >= ? AND i.url LIKE 'http%' "
        "AND NOT EXISTS (SELECT 1 FROM story s WHERE s.item_id = i.id) "
        "ORDER BY COALESCE(i.published, i.fetched_at) DESC LIMIT 1",
        (STORY_MIN_SEV,)).fetchone()
    if row is None:
        return {"job": "story", "did_work": False, "reason": "nothing missing a report"}

    iid = int(row["id"])
    # run_story reads the item with .get(), so it wants a plain dict. A sqlite3.Row
    # supports indexing but not .get, and the failure surfaces only after the outbound
    # fetch has already been paid for.
    item = {k: row[k] for k in row.keys()}
    dev = enrich.Tiiny()
    t0 = time.time()
    try:
        out = research.run_story(con, dev, item, say=ctx.say)
    except Exception as exc:
        ctx.say("[story] #%d failed: %s" % (iid, str(exc)[:110]))
        return {"job": "story", "did_work": False, "item": iid,
                "error": "%s: %s" % (type(exc).__name__, str(exc)[:90])}
    if not out:
        return {"job": "story", "did_work": False, "item": iid, "reason": "no usable source"}
    ctx.say("[story] #%d backfilled: %d words in %.0fs (%d/%d today)"
            % (iid, out.get("words") or 0, time.time() - t0, done + 1, STORY_DAILY_CAP))
    return {"job": "story", "did_work": True, "item": iid,
            "words": out.get("words") or 0, "ms": int((time.time() - t0) * 1000)}


def job_image(con, ctx, force=False):
    """Render one missing S4+ story image. Honours both caps and the image lease."""
    remaining, reason = _daily_budget(con)
    if remaining <= 0 and not force:
        return {"job": "image", "did_work": False, "reason": reason}

    # Candidate first, disk cap second: neither check is worth paying for when
    # there is nothing to render.
    row = None
    now = time.time()
    # Recency first, severity only as a tiebreaker. Sorting by severity first meant the
    # job worked through every S4 in the window - oldest included - before recency
    # mattered at all, so the board filled in backwards while the stories actually on
    # screen stayed blank. What a reader sees when they open the page is the newest
    # items, so that is what earns a picture first; older gaps get filled once the top
    # is covered, and a newly arrived story preempts that backfill on the next pass.
    for r in con.execute(
            "SELECT id, title, summary, severity FROM items WHERE enriched_at IS NOT NULL"
            " AND severity >= ? AND COALESCE(published, fetched_at) >= ?"
            " ORDER BY COALESCE(published, fetched_at) DESC, severity DESC LIMIT ?",
            (IMAGE_MIN_SEVERITY, now - IMAGE_WINDOW_S, IMAGE_SCAN_LIMIT)).fetchall():
        if os.path.exists(os.path.join(ctx.images_dir, "%d.png" % r["id"])):
            continue
        # Per-item failure memory. The candidate order is deterministic, so one
        # item the device will not render (declined body, rejected prompt) was
        # re-picked every pass for a full 24h -- each attempt taking the image
        # lease for IMAGE_LEASE_S and stalling the enricher, producing nothing and
        # blocking every other backfill behind it.
        if not force and (now - _meta_float(con, _image_fail_key(r["id"]), 0.0)) \
                < IMAGE_RETRY_AFTER_S:
            continue
        row = r
        break
    if row is None:
        return {"job": "image", "did_work": False, "reason": "no S4+ story missing a render"}

    over = _disk_over_cap(con, ctx)
    if over and not force:
        _note_over_cap(con, ctx, over)
        return {"job": "image", "did_work": False, "reason": over, "over_cap": True}

    if not wait_for_quiet(con, ctx):
        return {"job": "image", "did_work": False, "reason": "npu busy"}

    dev = ctx.device()
    prompt = ("Photorealistic documentary photograph of the scene: %s. %s "
              "Dramatic natural lighting, cinematic composition."
              % (_clean(row["title"], 200), _clean(row["summary"], 200)))
    body = json.dumps({
        "model": IMAGE_MODEL, "prompt": prompt,
        "negative_prompt": "text, letters, words, signage, watermark, low quality",
        "width": 512, "height": 512,
        "seed": int(row["id"]) % 2147483647, "steps": 8}).encode()
    req = urllib.request.Request(
        dev.base_url + "/v1/image/generate", data=body,
        headers={"Authorization": "Bearer " + dev.key,
                 "Content-Type": "application/json"})

    # Both leases, deliberately. The timestamp one stays until commit 3 so a rollback
    # needs no data migration; the onelane one is what actually excludes the other
    # process. Image generation is the longest device hold daybreak has.
    with lease(con, ctx, "AUTO-IMAGING S4+ STORY #%d" % row["id"],
               seconds=IMAGE_LEASE_S), \
            device.lease("%s image #%d" % (device.UNIT, row["id"])):
        try:
            with urllib.request.urlopen(req, timeout=IMAGE_TIMEOUT_S) as resp:
                raw = resp.read()
        except Exception as exc:
            ctx.say("[image] #%d failed: %s" % (row["id"], str(exc)[:120]))
            _set(con, _image_fail_key(row["id"]), "%.3f" % time.time())
            return {"job": "image", "did_work": False, "item": row["id"],
                    "reason": str(exc)[:120]}
        if not raw or raw[:1] == b"{":
            # device busy / declined -> JSON error body, not PNG bytes
            _set(con, _image_fail_key(row["id"]), "%.3f" % time.time())
            return {"job": "image", "did_work": False, "item": row["id"],
                    "reason": "device declined"}
        try:
            os.makedirs(ctx.images_dir, exist_ok=True)
            tmp = os.path.join(ctx.images_dir, "%d.png.tmp" % row["id"])
            with open(tmp, "wb") as fh:
                fh.write(raw)
            os.replace(tmp, os.path.join(ctx.images_dir, "%d.png" % row["id"]))
        except OSError as exc:
            # NOT recorded as an item failure: a full/read-only disk is not this
            # item's fault and skipping it for 6h would hide the real problem.
            ctx.say("[image] #%d write failed: %s" % (row["id"], exc))
            return {"job": "image", "did_work": False, "item": row["id"],
                    "reason": "write failed"}

    try:
        done = int(db.get_meta(con, "autoimg_count", "0") or 0)
    except (TypeError, ValueError):
        done = 0
    _set(con, "autoimg_count", str(done + 1))
    _set(con, _image_fail_key(row["id"]), "0")     # it rendered; forget the misses
    # Diffusion produces no tokens, so this work is invisible to the odometer. Record
    # the device time instead - three images is a minute of NPU at full tilt.
    try:
        enrich.count_device_ms((time.time() - t_img0) * 1000.0)
    except Exception:
        pass
    _bump(con, "images_total", 1)
    _bump(con, "job_image_total", 1)
    # Report the severity actually rendered. This said "S4+" from when the floor was
    # hardcoded at 4, so once the floor moved it was captioning S3 stories as S4 in the
    # ops log — a panel that gets read at a glance and trusted.
    db.add_event(con, "IMAGE", "auto-rendered S%s story #%d — %s (%d/%d today)"
                 % (row["severity"], row["id"], _clean(row["title"], 80),
                    done + 1, IMAGE_DAILY_CAP))
    ctx.say("[image] #%d rendered (%d bytes, %d/%d today)"
            % (row["id"], len(raw), done + 1, IMAGE_DAILY_CAP))
    return {"job": "image", "did_work": True, "item": row["id"], "bytes": len(raw),
            "today": done + 1, "cap": IMAGE_DAILY_CAP}


# --------------------------------------------------------------------------- #
# job 5: daily brief
# --------------------------------------------------------------------------- #

_BRIEF_SYSTEM = (
    "You are the watch commander writing the morning brief for a command staff. "
    "You lead with what matters, you are terse, you do not hedge and you do not "
    "repeat the inputs verbatim. No preamble, no markdown headers."
)

_BRIEF_PROMPT = (
    "UTC day: %s\n\nTop wire items of the day:\n%s\n\n%s\n"
    "Write the daily brief. Return ONE JSON object and nothing else:\n"
    '{"headline": "<= 10 words, the day in one line", '
    '"brief": "<250-400 words: the day\'s significant developments, by weight not by region>", '
    '"watch": ["<= 4 things to watch tomorrow"]}'
)


BRIEF_BACKFILL_DAYS = 7        # how far back a gap in the archive is still filled


def brief_day_due(con, now=None):
    """The newest UTC day that still needs a brief, or None.

    A day is briefable once it is over plus BRIEF_AFTER_MIN minutes, so the last
    hour's items are all in and enriched.

    It walks back up to BRIEF_BACKFILL_DAYS rather than only ever considering
    yesterday: a pipeline down Friday to Sunday used to leave Friday and Saturday
    permanently unbriefed, with nothing reporting the hole. Newest-first, so a
    backlog fills in reverse order and today's reader gets the freshest first.
    """
    now = now if now is not None else time.time()
    # Absolute epoch, not (hour == 0 and minute < N): the clock comparison was only
    # correct while BRIEF_AFTER_MIN <= 60. At 90 it was true for all of hour 0 and
    # no truer for hour 1, silently capping the delay at 60 minutes.
    newest = utc_day(now - 86400)
    if now < day_bounds(newest)[1] + float(BRIEF_AFTER_MIN) * 60.0:
        # yesterday is not ripe yet; older days may still have holes
        start_offset = 2
    else:
        start_offset = 1
    for back in range(start_offset, int(BRIEF_BACKFILL_DAYS) + 1):
        day = utc_day(now - back * 86400)
        if db.latest_doc(con, "brief", day) is not None:
            continue                  # already filed; keep looking for a hole
        start, end = day_bounds(day)
        try:
            n = con.execute(
                "SELECT COUNT(*) FROM items WHERE enriched_at IS NOT NULL"
                " AND COALESCE(published, fetched_at) >= ?"
                " AND COALESCE(published, fetched_at) < ?",
                (start, end)).fetchone()[0] or 0
        except sqlite3.Error:
            return None
        if n >= SYNTHESIS_MIN_ITEMS:
            return day
    return None


def _brief_context(con, start, end):
    """The day's syntheses and dossiers, folded in as analyst input.

    `end` is already the day boundary. It used to be bound as `end + 86400`, a
    two-day window: harmless for the live run (which fires ~30 min after midnight)
    but `--brief --day X --force` then folded a whole extra day of unrelated
    products into a brief that does not cover them. The grace below is the actual
    intent -- products filed shortly after midnight about the day just ended.
    """
    lines = []
    grace = max(0.0, float(BRIEF_AFTER_MIN)) * 60.0
    try:
        rows = con.execute(
            "SELECT kind, subject, title FROM docs WHERE kind IN ('synthesis','dossier')"
            " AND created_at >= ? AND created_at < ? ORDER BY created_at DESC LIMIT 20",
            (start, end + grace)).fetchall()
    except sqlite3.Error:
        rows = []
    if rows:
        lines.append("Analyst products filed during this period:")
        for r in rows:
            lines.append("- (%s) %s: %s" % (r["kind"], r["subject"], _clean(r["title"], 120)))
    try:
        cl = con.execute(
            "SELECT label, item_count, top_severity FROM clusters"
            " WHERE label IS NOT NULL AND label != '' AND updated_at >= ?"
            " ORDER BY top_severity DESC, item_count DESC LIMIT 10",
            (start,)).fetchall()
    except sqlite3.Error:
        cl = []
    if cl:
        lines.append("")
        lines.append("Developing stories:")
        for c in cl:
            lines.append("- %s (%d reports, peak S%s)"
                         % (_clean(c["label"], 120), c["item_count"] or 0,
                            c["top_severity"] or 1))
    return "\n".join(lines)


def job_brief(con, ctx, force=False, day=None):
    """File the executive brief for a finished UTC day, and stage it for R2."""
    day = day or brief_day_due(con)
    if not day:
        if not force:
            return {"job": "brief", "did_work": False, "reason": "no day due"}
        day = utc_day(time.time() - 86400)
    start, end = day_bounds(day)
    rows = con.execute(
        "SELECT id, title, summary, source, region, category, severity FROM items"
        " WHERE enriched_at IS NOT NULL AND COALESCE(published, fetched_at) >= ?"
        " AND COALESCE(published, fetched_at) < ?"
        " ORDER BY severity DESC, COALESCE(published, fetched_at) DESC LIMIT 60",
        (start, end)).fetchall()
    if not rows:
        return {"job": "brief", "did_work": False, "day": day, "reason": "no items that day"}

    res, stats = _ask(ctx, con, _BRIEF_SYSTEM,
                      _BRIEF_PROMPT % (day, _wire_lines(rows), _brief_context(con, start, end)),
                      1600, "WRITING DAILY BRIEF %s — ORNITH-35B" % day)
    body = _body(res, "brief", 6000)
    if not body:
        ctx.say("[brief] %s produced nothing (%s)"
                % (day, (stats or {}).get("error", "empty content")))
        return {"job": "brief", "did_work": False, "day": day,
                "reason": (stats or {}).get("error") or "empty"}

    headline = _body(res, "headline", 140) or ("Daily brief %s" % day)
    watch = _strlist(res, "watch", 4, 160)
    # The DB doc has no section structure, so the watch list is appended inline
    # there. The markdown digest gets a proper "## Watch" section instead and must
    # be built from the UNSUFFIXED prose, or every offsite brief ends with the same
    # four items twice: once as a run-on line, once as the bullet list.
    body_prose = body
    if watch:
        body = body + "\n\nWatch: " + "; ".join(watch)

    meta = _doc_stats(stats)
    meta["ids"] = [r["id"] for r in rows]
    doc_id = db.put_doc(con, "brief", day, headline, body,
                        item_count=len(rows), meta=meta)
    _bump(con, "job_brief_total", 1)
    db.add_event(con, "BRIEF", "daily brief filed for %s: %s (%d items reviewed)"
                 % (day, headline, len(rows)))

    # Stage a markdown copy for the offsite loop. Best-effort: R2 may be unset,
    # in which case the file just sits in digests/ until it is configured.
    staged = None
    if _r2 is not None:
        try:
            staged = _r2.write_digest_copy(
                _brief_markdown(day, headline, body_prose, rows, watch),
                "daybreak-brief-%s.md" % day, db_path=ctx.db_path)
        except Exception as exc:
            ctx.say("[brief] digest copy failed: %s" % str(exc)[:100])
    ctx.say("[brief] %s doc=%s %d chars from %d items%s"
            % (day, doc_id, len(body), len(rows), " staged=%s" % staged if staged else ""))
    return {"job": "brief", "did_work": True, "day": day, "doc_id": doc_id,
            "headline": headline, "chars": len(body), "items": len(rows),
            "staged": staged}


def _brief_markdown(day, headline, body, rows, watch):
    out = ["# DAYBREAK daily brief — %s" % day, "",
           "**%s**" % headline, "", body, "",
           "---", "",
           "AI analysis performed on-device by Ornith-1.0-35B on a Tiiny Pocket. "
           "%d enriched wire items reviewed. Severity S1 (low) to S5 (flash/critical)."
           % len(rows), "", "## Top reporting", ""]
    for r in rows[:30]:
        out.append("- [S%d/%s] %s — %s (%s)" % (
            _sev(r), r["region"] or "GLOBAL", _clean(r["title"], 200),
            _clean(r["summary"], 240), r["source"]))
    if watch:
        out += ["", "## Watch", ""] + ["- %s" % w for w in watch]
    return "\n".join(out)


# --------------------------------------------------------------------------- #
# the scheduler
# --------------------------------------------------------------------------- #

class Job:
    __slots__ = ("name", "interval", "fn", "device", "label")

    def __init__(self, name, interval, fn, device=True, label=""):
        self.name = name
        self.interval = interval
        self.fn = fn
        self.device = device
        self.label = label or name


# Priority order. The first DUE job in this list runs; each run stamps its own
# clock, so a cheap frequent job cannot starve behind an expensive rare one.

# --------------------------------------------------------------------------- #
# audio brief — narrate the day's text brief on the device itself
# --------------------------------------------------------------------------- #

AUDIO_INTERVAL_S = _env_float("DAYBREAK_AUDIO_INTERVAL", 1800.0)


def _episodes_dir(con):
    base = db.default_images_dir(con)
    root = os.path.dirname(base) if base else "."
    return os.path.join(root, "episodes")


def job_audio(con, ctx, force=False, day=None):
    """Render the most recent text brief to a spoken mp3 episode.

    Runs AFTER job_brief (which writes the text). Costs 7 NPU units for the
    duration of the run only — audio.Speech starts the TTS model and stops it
    again, so the three permanent models keep their 83 units.
    """
    try:
        import audio
    except Exception as exc:
        return {"job": "audio", "did_work": False, "reason": "audio module: %s" % exc}

    out_dir = _episodes_dir(con)
    if day:
        row = db.latest_doc(con, "brief", day)
    else:
        # Newest brief BY DAY, not by creation time, and the newest one that still has
        # no episode. latest_doc() orders by created_at, which is exactly backwards
        # here: job_brief backfills newest-day-first, so the most recently written brief
        # is the OLDEST day. Audio therefore locked onto the oldest day in the backlog,
        # rendered it, and every run after that answered "already rendered" — the
        # episode never advanced again. Walking by subject descending also fills holes,
        # matching how brief_day_due already behaves.
        row = None
        for cand in db.briefs_by_day(con, limit=int(BRIEF_BACKFILL_DAYS) + 1):
            if force or not os.path.exists(os.path.join(out_dir, "%s.mp3" % cand["subject"])):
                row = cand
                break
    if not row:
        return {"job": "audio", "did_work": False, "reason": "no brief needs audio"}
    day = day or row["subject"]
    if not day:
        return {"job": "audio", "did_work": False, "reason": "brief has no day"}

    out = os.path.join(out_dir, "%s.mp3" % day)
    if os.path.exists(out) and not force:
        return {"job": "audio", "did_work": False, "day": day, "reason": "already rendered"}

    body = row["body"] if hasattr(row, "keys") else ""
    title = (row["title"] if hasattr(row, "keys") else "") or ("Brief for %s" % day)

    # lease() is a module function, like every other call site. ctx.lease() raised
    # AttributeError on every run, so this job has never once completed under the
    # scheduler — the only episode on disk came from a hand-run that bypassed it.
    with lease(con, ctx, "NARRATING BRIEF %s — QWEN3-TTS" % day, seconds=900):
        path, res = audio.render_episode(body, out, log=ctx.say)
    if not path:
        db.add_event(con, "ERROR", "audio brief %s failed: %s" % (day, str(res)[:110]))
        return {"job": "audio", "did_work": False, "day": day, "error": str(res)[:160]}

    size = os.path.getsize(path)
    _set(con, "audio_last_day", day)
    db.add_event(con, "BRIEF", "narrated %s on-device — %.1f min, %d KB"
                 % (day, (res or 0) / 60.0, size // 1024))
    return {"job": "audio", "did_work": True, "day": day, "path": path,
            "seconds": round(res or 0, 1), "bytes": size, "title": title}


JOBS = (
    Job("brief", BRIEF_CHECK_S, job_brief, label="daily brief"),
    Job("audio", AUDIO_INTERVAL_S, job_audio, label="audio narration"),
    Job("recluster", RECLUSTER_INTERVAL_S, job_recluster, label="cluster sweep"),
    Job("synthesis", SYNTHESIS_INTERVAL_S, job_synthesis, label="regional synthesis"),
    Job("dossier", DOSSIER_INTERVAL_S, job_dossier, label="entity dossier"),
    Job("image", IMAGE_INTERVAL_S, job_image, label="image backfill"),
    Job("geocode", GEO_INTERVAL_S, job_geocode, label="incident geocoding"),
    Job("analyst", ANALYST_INTERVAL_S, job_analyst, label="analyst brief"),
    Job("story", STORY_INTERVAL_S, job_story, label="desk report backfill"),
)

BY_NAME = {j.name: j for j in JOBS}


def _last_run(con, name):
    return _meta_float(con, "job_last_" + name, 0.0)


def _stamp(con, name, ts=None):
    _set(con, "job_last_" + name, "%.3f" % (ts if ts is not None else time.time()))


def queue_pending(con):
    try:
        return int((db.counts(con) or {}).get("pending") or 0)
    except Exception:
        return 0


def due_jobs(con, now=None):
    """[(overdue_seconds, Job)] in priority order, most overdue first per priority."""
    now = now if now is not None else time.time()
    out = []
    for job in JOBS:
        last = _last_run(con, job.name)
        if last <= 0 or (now - last) >= job.interval:
            out.append((now - last if last > 0 else float("inf"), job))
    return out


# How long background work may be starved by a busy enrichment queue before it gets a
# turn anyway, and the queue depth below which it does not have to wait at all.
BG_STARVE_S = _env_int("DAYBREAK_BG_STARVE_S", 150)
BG_PENDING_OK = _env_int("DAYBREAK_BG_PENDING_OK", 25)


def run_due(con, ctx, now=None):
    """One scheduler pass: run AT MOST one job. -> result dict.

    Enrichment has priority, but no longer absolute priority.

    The original rule was: a non-empty enrichment queue means no background job may
    take the NPU. That made sense when nothing arbitrated the device and two callers
    meant a 150004. It does not survive 59 feeds. The queue is now almost never empty,
    so image backfill, dossiers and syntheses ran only in the seconds between a batch
    finishing and the next fetch landing - which in practice meant they stopped. The
    board went a day with 211 of the last 400 graded stories missing artwork while the
    device sat idle 84% of the time.

    The onelane settles device contention now, so the scheduler only has to settle
    FAIRNESS. Background work waits while the queue is deep, and gets a turn anyway
    once it has been starved long enough. One job per pass still applies, so this
    yields interleaving rather than a stampede.
    """
    if ctx.stopped():
        return {"ran": None, "skipped": "stopping"}
    pending = queue_pending(con)
    starved = time.time() - _meta_float(con, "jobs_last_bg")
    if pending > BG_PENDING_OK and starved < BG_STARVE_S:
        return {"ran": None, "skipped": "queue", "pending": pending,
                "starved_s": round(starved)}

    due = due_jobs(con, now)
    if not due:
        return {"ran": None, "skipped": "nothing due"}

    _overdue, job = due[0]
    t0 = time.time()
    try:
        res = job.fn(con, ctx) or {}
    except Exception as exc:
        ctx.say("[jobs] %s crashed %s: %s" % (job.name, type(exc).__name__, exc))
        db.add_event(con, "ERROR", "job %s failed: %s" % (job.name, str(exc)[:160]))
        res = {"job": job.name, "did_work": False, "error": "%s: %s"
               % (type(exc).__name__, exc)}
    finally:
        # Stamp even on failure: the interval is also the backoff. Without this a
        # job that always errors would be re-picked every single pass forever.
        _stamp(con, job.name, t0)
        # The starvation clock is global, not per job: what matters is that SOMETHING
        # other than enrichment got a turn recently.
        try:
            db.set_meta(con, "jobs_last_bg", "%.3f" % time.time())
        except Exception:
            pass
    res["ran"] = job.name
    res["ms"] = int((time.time() - t0) * 1000)
    res["due_count"] = len(due)
    return res


def status(con, now=None):
    """What the scheduler thinks, for --status and for the operator."""
    now = now if now is not None else time.time()
    jobs = []
    for job in JOBS:
        last = _last_run(con, job.name)
        jobs.append({"name": job.name, "label": job.label,
                     "interval_s": job.interval,
                     "last_run_ts": last or None,
                     "age_s": round(now - last, 1) if last else None,
                     "due": bool(last <= 0 or (now - last) >= job.interval),
                     "total": int(_meta_float(con, "job_%s_total" % job.name, 0))})
    return {"pending": queue_pending(con), "jobs": jobs,
            "docs": db.doc_counts(con),
            "brief_day_due": brief_day_due(con, now)}


# --------------------------------------------------------------------------- #
# CLI / self-check
# --------------------------------------------------------------------------- #

def _selfcheck():
    """Wiring check with no device: schema, gating, doc round-trip, lease release."""
    import tempfile

    path = os.path.join(tempfile.mkdtemp(prefix="daybreak-jobs-"), "smoke.db")
    con = db.connect(path)
    now = time.time()

    class _Dev:
        base_url = "http://127.0.0.1:1"
        key = "x"

        def chat_json(self, system, user, max_tokens=800):
            if "dossier" in user:
                return {"headline": "Test posture", "dossier": "D" * 400,
                        "threads": ["t1"]}, {"gen_tps": 22.0, "ms": 900, "tokens_out": 120}
            if "assessment" in user:
                return {"headline": "Test regional", "assessment": "A" * 400,
                        "watch": ["w1"]}, {"gen_tps": 21.0, "ms": 800, "tokens_out": 110}
            return {"headline": "Test day", "brief": "B" * 500, "watch": ["w"]}, \
                   {"gen_tps": 20.0, "ms": 1000, "tokens_out": 200}

    ctx = Ctx(tiiny=_Dev(), log=lambda m: None, db_path=path)

    # job_audio starts the TTS model on a real device and streams a wav back, so it
    # cannot run in a check that has no device. _Dev answers chat and nothing else, and
    # audio is second in the priority order, so leaving it due made this pass spend 38
    # seconds timing out against whatever address the box thinks the Tiiny is at, then
    # fail the "brief then recluster" assertion because audio had taken recluster's
    # turn. It is held not-due for the whole check instead.
    DEVICE_FREE = tuple(job.name for job in JOBS if job.name != "audio")

    def reset_clocks(value=0.0):
        """Every job this check can actually run, back to `value`; audio stays parked."""
        for job in JOBS:
            _stamp(con, job.name, value if job.name in DEVICE_FREE else time.time())

    # seed a finished UTC day (for the brief) plus a live 24h window (for the
    # synthesis/dossier jobs, which both read the last day only)
    day = utc_day(now - 86400)
    start, _end = day_bounds(day)
    for i in range(12):
        iid = db.insert_item(con, "https://ex.test/%d" % i, "SMOKE",
                             "Headline %d about Ruritania" % i, start + 60 * i, "raw")
        db.mark_enriched(con, iid, "Summary %d." % i, "conflict", "CENTCOM",
                         4 if i % 3 == 0 else 2, json.dumps(["Ruritania", "Global"]), None)
    for i in range(10):
        iid = db.insert_item(con, "https://ex.test/live/%d" % i, "SMOKE",
                             "Live headline %d from Ruritania" % i, now - 3600 * (i + 1), "raw")
        db.mark_enriched(con, iid, "Live summary %d." % i, "conflict", "CENTCOM",
                         5 if i % 4 == 0 else 3, json.dumps(["Ruritania"]), None)

    # 1. the fairness gate. This used to assert the original rule, that ANY pending
    # item stops every background job. run_due stopped working that way when the feed
    # list reached 59 sources and the queue stopped ever being empty: the rule is now
    # that a DEEP queue defers background work, and background work gets a turn anyway
    # once it has been starved for BG_STARVE_S. The test kept asserting the old rule
    # and had been failing ever since, on a fresh database, for two reasons at once.
    #
    # The second one is worth naming. meta.jobs_last_bg is absent on a new database,
    # _meta_float reads that as 0.0, and starved is then the whole Unix epoch, so the
    # starvation escape fires on the first pass however deep the queue is. That is the
    # right behaviour for a real box and it means a test of the queue gate has to stamp
    # the clock first, which is the part that made this look like a code bug.
    for i in range(BG_PENDING_OK + 1):
        assert db.insert_item(con, "https://ex.test/pending/%d" % i, "SMOKE",
                              "Pending %d" % i, now, "r")
    assert queue_pending(con) == BG_PENDING_OK + 1, queue_pending(con)
    db.set_meta(con, "jobs_last_bg", "%.3f" % time.time())
    res = run_due(con, ctx)
    assert res["ran"] is None and res["skipped"] == "queue", res

    # Starved long enough, and the same deep queue no longer defers anything. Every
    # job is stamped as just run for this one pass, so the gate is what is measured
    # and no document is written that the steps below would then find already done.
    reset_clocks(time.time())
    db.set_meta(con, "jobs_last_bg", "%.3f" % (time.time() - BG_STARVE_S - 1))
    res = run_due(con, ctx)
    assert res["ran"] is None and res["skipped"] == "nothing due", res
    reset_clocks()

    # A shallow queue never defers anything, whatever the clock says. The gate rows go
    # away rather than getting enriched: as GLOBAL/politics they would have been a
    # second region with enough material, and the synthesis cooldown check below asks
    # for exactly one.
    with con:
        con.execute("DELETE FROM items WHERE url LIKE 'https://ex.test/pending/%'")
    assert queue_pending(con) == 0
    db.set_meta(con, "jobs_last_bg", "%.3f" % time.time())
    reset_clocks(time.time())
    assert run_due(con, ctx).get("skipped") == "nothing due"
    reset_clocks()

    # 2. brief
    assert brief_day_due(con) == day, brief_day_due(con)
    r = job_brief(con, ctx)
    assert r["did_work"] and r["day"] == day, r
    assert db.latest_doc(con, "brief", day) is not None
    assert brief_day_due(con) is None, "brief must not re-fire the same day"

    # 3. synthesis + dossier
    r = job_synthesis(con, ctx)
    assert r["did_work"] and r["region"] == "CENTCOM", r
    assert job_synthesis(con, ctx)["did_work"] is False, "region cooldown not honoured"
    r = job_dossier(con, ctx)
    assert r["did_work"] and r["entity"] == "Ruritania", r
    assert job_dossier(con, ctx)["did_work"] is False, "entity cooldown not honoured"

    # 3b. a FAILING region must still rotate out of the way (regression: a
    #     success-only clock pinned last_ts at 0 and re-picked it every pass)
    for i in range(8):
        iid = db.insert_item(con, "https://ex.test/eu/%d" % i, "SMOKE",
                             "Euro headline %d" % i, now - 1800 * (i + 1), "raw")
        db.mark_enriched(con, iid, "Euro summary %d." % i, "diplomacy", "EUCOM",
                         3, json.dumps(["Ruritania"]), None)

    class _DeadDev(_Dev):
        def chat_json(self, system, user, max_tokens=800):
            return None, {"error": "TimeoutError: timed out"}

    dead_ctx = Ctx(tiiny=_DeadDev(), log=lambda m: None, db_path=path)
    first = job_synthesis(con, dead_ctx, force=True)
    second = job_synthesis(con, dead_ctx, force=True)
    assert first["did_work"] is False and second["did_work"] is False
    assert first["region"] != second["region"], \
        "a failing region was re-picked instead of rotating: %s" % first["region"]

    # 4. recluster assigns orphans (embeddings off -> title jaccard path)
    r = job_recluster(con, ctx)
    orphans = con.execute("SELECT COUNT(*) FROM items WHERE enriched_at IS NOT NULL"
                          " AND cluster_id IS NULL").fetchone()[0]
    assert r["assigned"] > 0 and orphans == 0, (r, orphans)

    # 5. leases always released, even when the body raises
    try:
        with lease(con, ctx, "TEST", seconds=60):
            # BOTH keys are held for chat work: img_hold_until is the one the
            # enricher and the cluster labeller actually poll, and advertising only
            # enrich_busy_until let a 250s generation run beside a second chat call
            assert _meta_float(con, "img_hold_until") > time.time()
            assert _meta_float(con, "enrich_busy_until") > time.time()
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert _meta_float(con, "img_hold_until") == 0.0
    assert _meta_float(con, "enrich_busy_until") == 0.0
    assert (db.get_meta(con, "now_doing") or "") == ""

    # 5b. release is compare-and-clear: a lease taken by somebody else (server.py's
    #     on-demand render writes the same keys from its own connection) must
    #     survive our finally, or the enricher fires a chat call into a live render
    with lease(con, ctx, "TEST2", seconds=60):
        _set(con, "img_hold_until", "%.3f" % (time.time() + 180))   # "server.py"
    assert _meta_float(con, "img_hold_until") > time.time(), \
        "a finished job zeroed somebody else's live image lease"
    _set(con, "img_hold_until", "0")

    # 6. image cap respected without a device
    _set(con, "autoimg_day", utc_day())
    _set(con, "autoimg_count", str(IMAGE_DAILY_CAP))
    r = job_image(con, ctx)
    assert r["did_work"] is False and "cap" in r["reason"], r
    _set(con, "autoimg_count", "0")

    # 6b. a device that will not render this item must not head-butt it forever:
    #     the failed id is remembered and the next pass moves on to another story
    first = job_image(con, ctx)
    assert first["did_work"] is False and first.get("item"), first
    assert _meta_float(con, _image_fail_key(first["item"]), 0.0) > 0, first
    second = job_image(con, ctx)
    assert second.get("item") != first["item"], \
        "a permanently-failing item was re-picked: %s" % first["item"]

    # 6c. model output that is not the shape the prompt asked for degrades to
    #     empty, never to three one-character "watch items" or a TypeError
    assert _strlist({"watch": "a, b, c"}, "watch", 3, 140) == []
    assert _strlist({"watch": {"a": 1}}, "watch", 3, 140) == []
    # slice first, then drop the blanks -- same order the callers always used
    assert _strlist({"watch": ["a", "", None, "b"]}, "watch", 3, 140) == ["a"]
    assert _strlist({"watch": ["a", "b", "c", "d"]}, "watch", 3, 140) == ["a", "b", "c"]
    assert _body({"brief": ["not", "prose"]}, "brief", 100) == ""
    assert _body({"brief": " x  y "}, "brief", 100) == "x y"

    # 6d. the digest's Watch list is emitted once, not inline AND as a section
    md = _brief_markdown("2026-01-01", "H", "Prose body.", [], ["alpha", "beta"])
    assert md.count("alpha") == 1, md

    # 7. scheduler picks one job per pass and stamps it
    reset_clocks()
    res = run_due(con, ctx)
    assert res["ran"] == "brief", res           # priority order
    assert _last_run(con, "brief") > 0
    res2 = run_due(con, ctx)
    assert res2["ran"] == "recluster", res2

    # 8. a crashing job still stamps (no hot loop)
    boom = Job("boom", 1.0, lambda c, x, force=False: (_ for _ in ()).throw(ValueError("x")))
    saved = globals()["JOBS"]
    globals()["JOBS"] = (boom,)
    try:
        res = run_due(con, ctx)
        assert res["ran"] == "boom" and "error" in res, res
        assert _last_run(con, "boom") > 0
    finally:
        globals()["JOBS"] = saved

    counts = db.doc_counts(con)
    assert counts["briefs"] == 1 and counts["syntheses"] == 1 and counts["dossiers"] == 1, counts
    st = status(con)
    assert len(st["jobs"]) == len(JOBS)

    con.close()
    print("jobs.py self-check OK  docs=%s" % counts)
    print("  db: %s" % path)
    return 0


def _cli(argv):
    args = list(argv)
    force = "--force" in args
    if "--selfcheck" in args:
        return _selfcheck()

    db_path = os.environ.get("DAYBREAK_DB") or os.path.join(BASE_DIR, "daybreak.db")
    con = db.connect(db_path)
    ctx = Ctx(db_path=db_path)
    try:
        if "--status" in args:
            print(json.dumps(status(con), indent=2, default=str))
            return 0

        day = None
        if "--day" in args:
            try:
                day = args[args.index("--day") + 1]
                day_bounds(day)          # validate
            except (IndexError, ValueError):
                print("--day needs YYYY-MM-DD")
                return 2

        if "--brief" in args:
            res = job_brief(con, ctx, force=force or bool(day), day=day)
        elif "--job" in args:
            try:
                name = args[args.index("--job") + 1]
            except IndexError:
                print("--job needs a name: %s" % ", ".join(BY_NAME))
                return 2
            job = BY_NAME.get(name)
            if job is None:
                print("unknown job %r; known: %s" % (name, ", ".join(BY_NAME)))
                return 2
            if not force and queue_pending(con) > 0:
                print("enrichment queue is not empty (%d pending); --force to override"
                      % queue_pending(con))
                return 1
            # --day was parsed and then silently dropped for --job, so
            # "--job audio --day 2026-08-19" quietly rendered whatever the job would
            # have picked anyway. Pass it to jobs that accept one; the rest are
            # day-agnostic and would reject the argument.
            kw = {"force": force}
            if day and job.name in ("audio", "brief"):
                kw["day"] = day
            res = job.fn(con, ctx, **kw) or {}
            _stamp(con, job.name)
        else:
            res = run_due(con, ctx)

        print(json.dumps(res, indent=2, default=str))
        return 0 if (res.get("did_work") or res.get("ran")) else 1
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(_cli(sys.argv[1:]))
