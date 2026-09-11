#!/usr/bin/env python3
"""DAYBREAK — Tiiny AI client + enrichment (stdlib only).

All AI work runs on the Tiiny device: Ornith-1.0-35B for chat, Qwen3-Embedding-0.6B
for vectors. Nothing here ever raises on a device problem: callers get None and the
loops keep running (this box runs unattended for a week).
"""

import json
import math
import os
import re
import struct
import threading
import time
import urllib.request

import device                       # one spelling of the device host

# db.py is a sibling module owned by another worker; enrich.py's device paths and the
# __main__ self-check must work even if it is missing/unfinished.
try:
    import db as _db
except Exception:  # pragma: no cover - import-time robustness only
    _db = None

CHAT_MODEL = "deepreinforce-ai/Ornith-1.0-35B"
EMBED_MODEL = "Qwen/Qwen3-Embedding-0.6B"

# The device does not batch; under a concurrent load generator a single completion can
# take 20-60s+, and the thinking-retry path generates up to 1600 tokens at ~26 tok/s.
TIMEOUT = float(os.environ.get("TIINY_TIMEOUT", "180"))

# Measured on the live unit 2026-08-21: with reasoning enabled Ornith burns ~1600 tokens
# of reasoning_content on a tagging prompt (~62s) and leaves `content` EMPTY at
# max_tokens=800. The server honours chat_template_kwargs.enable_thinking=false, which
# returns the same JSON in ~50 tokens / ~5-24s. That is the fast path; if it ever stops
# working the thinking path below still rescues the call. Set TIINY_NOTHINK=0 to disable.
NOTHINK = os.environ.get("TIINY_NOTHINK", "1") != "0"

CATEGORIES = (
    "conflict", "terrorism", "cyber", "diplomacy", "economy",
    "disaster", "health", "crime", "politics", "tech", "energy",
)
REGIONS = (
    "NORTHCOM", "SOUTHCOM", "EUCOM", "CENTCOM", "AFRICOM", "INDOPACOM", "GLOBAL",
)

DEFAULT_CATEGORY = "politics"
DEFAULT_REGION = "GLOBAL"

_TAG_RE = re.compile(r"<[^>]+>")
_WORD_RE = re.compile(r"[a-z0-9]+")
# Words that carry no topical signal for title-overlap clustering.
_STOP = frozenset("""
a an the of to in on at for from by with and or as is are was were be been being
after before over under new news says say said report reports update updates live
amid into its his her their this that these those it as up down out about more than
""".split())


# ---------------------------------------------------------------- small helpers

def _row_get(row, key, default=None):
    """Works for sqlite3.Row, dict, or anything with attributes."""
    try:
        val = row[key]
    except Exception:
        val = getattr(row, key, default)
    return default if val is None else val


def _clean(text, limit=1200):
    if not text:
        return ""
    text = _TAG_RE.sub(" ", str(text))
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit]


_CLOSERS = {"{": "}", "[": "]"}


def _scan_object(text, start):
    """-> (end_index_exclusive|None, open_stack, in_string) for the object at start."""
    stack = []
    in_str = False
    esc = False
    for i in range(start, len(text)):
        ch = text[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch in "{[":
            stack.append(ch)
        elif ch in "}]":
            if stack and _CLOSERS[stack[-1]] == ch:
                stack.pop()
                if not stack:
                    return i + 1, stack, in_str
            else:
                return None, stack, in_str  # malformed
    return None, stack, in_str


def _repair(frag, stack, in_str):
    """Best-effort close of a JSON object the model was cut off mid-way through."""
    out = frag.rstrip()
    if out.endswith("\\"):
        out = out[:-1]
    if in_str:
        out += '"'
    # Drop a dangling key with no value, a trailing comma, or a trailing colon.
    out = re.sub(r',\s*"[^"]*"\s*:?\s*$', "", out)
    out = re.sub(r',\s*$', "", out)
    out = re.sub(r':\s*$', ": null", out)
    for opener in reversed(stack):
        out += _CLOSERS[opener]
    return out


def _extract_json(text):
    """Return the last parseable {...} object in text, else None.

    Ornith wraps output in prose/fences and, when the token budget runs out inside
    reasoning_content, the only JSON present may be mid-thought and never closed.
    Scan right-to-left so the model's final answer wins; if nothing is complete,
    salvage the truncated tail rather than losing the item.
    """
    if not text:
        return None
    text = text.replace("```json", "```")
    starts = [i for i, ch in enumerate(text) if ch == "{"]

    complete = None
    salvaged = None
    for start in reversed(starts):
        end, stack, in_str = _scan_object(text, start)
        if end is not None:
            if complete is not None:
                continue
            try:
                obj = json.loads(text[start:end])
            except Exception:
                continue
            if isinstance(obj, dict):
                complete = obj
        elif stack:
            try:
                obj = json.loads(_repair(text[start:], stack, in_str))
            except Exception:
                continue
            # Keep scanning left: outer objects carry more keys than nested fragments.
            if isinstance(obj, dict) and (salvaged is None or len(obj) > len(salvaged)):
                salvaged = obj

    if salvaged is not None and (complete is None or len(salvaged) > len(complete)):
        # A truncated object at the tail is the model's latest answer; a small complete
        # one is usually a nested fragment or an earlier scratch object in the trace.
        return salvaged
    return complete


def _norm_tokens(title):
    return {w for w in _WORD_RE.findall((title or "").lower())
            if len(w) > 2 and w not in _STOP}


def jaccard(a_title, b_title):
    a, b = _norm_tokens(a_title), _norm_tokens(b_title)
    if not a or not b:
        return 0.0
    return len(a & b) / float(len(a | b))


def cosine(a, b):
    """Cosine similarity of two float32 blobs. 0.0 on any mismatch/empty."""
    if not a or not b or len(a) != len(b) or len(a) % 4:
        return 0.0
    n = len(a) // 4
    va = struct.unpack("<%df" % n, a)
    vb = struct.unpack("<%df" % n, b)
    dot = na = nb = 0.0
    for x, y in zip(va, vb):
        dot += x * y
        na += x * x
        nb += y * y
    if na <= 0.0 or nb <= 0.0:
        return 0.0
    val = dot / (math.sqrt(na) * math.sqrt(nb))
    if val != val:  # NaN
        return 0.0
    return max(-1.0, min(1.0, val))


# ---------------------------------------------------------------- device client

_TOKENS = {"n": 0, "out": 0, "ms": 0.0}
_TOKENS_LOCK = threading.Lock()


def _count_tokens(n_total, n_out=0):
    """Accumulate what the device actually processed.

    Both halves count. Prefill is not free: the device reports prompt_per_second around
    24 tok/s, the same order as generation, and our prompts are 2-4x the size of the
    replies - a single analyst brief feeds it ~2,800 tokens to get ~660 back. Counting
    completions alone described about a quarter of the work and made a busy device look
    idle. `out` is kept separately because generation speed is still the useful
    tokens-per-second figure.
    """
    if n_total or n_out:
        with _TOKENS_LOCK:
            _TOKENS["n"] += int(n_total or 0)
            _TOKENS["out"] += int(n_out or 0)


def count_device_ms(ms):
    """Milliseconds the NPU spent working, whatever it was working on.

    Tokens cannot describe this device. Image generation is diffusion and has no tokens
    at all; speech emits audio samples; embeddings emit a vector. Rendering three images
    is a minute of the NPU at full tilt and produces a token count of exactly zero,
    which is why the odometer looked wrong when it was in fact correct. This counter is
    the honest measure of how hard the thing is working.
    """
    try:
        ms = float(ms or 0)
    except (TypeError, ValueError):
        return
    if ms > 0:
        with _TOKENS_LOCK:
            _TOKENS["ms"] += ms


def drain_tokens():
    """Take and reset the tokens generated since the last drain.

    Accumulated in the process and flushed by whoever owns a database connection, so the
    client itself needs no knowledge of storage.
    """
    with _TOKENS_LOCK:
        n, o, ms = _TOKENS["n"], _TOKENS["out"], _TOKENS["ms"]
        _TOKENS["n"] = _TOKENS["out"] = 0
        _TOKENS["ms"] = 0.0
    return n, o, ms


class Tiiny:
    def __init__(self, host=None, key=None, timeout=None):
        # One spelling of the device for every module: onelane keys its lock on
        # this string, so two spellings coordinate nobody. See device.py.
        self.host = device.host() if host is None else \
            host.replace("http://", "").replace("https://", "").strip("/ ") \
                .split("/")[0].split(":")[0]
        self.base_url = "http://%s:%d" % (self.host, device.PORT)
        # No baked-in credential: the key comes from the environment (/etc/daybreak.env,
        # mode 0640) or nowhere. An empty key must fail loudly at call time so a
        # misconfigured deploy is visible instead of silently authenticating.
        self.key = key or os.environ.get("TIINY_KEY", "")
        self.timeout = float(timeout or TIMEOUT)

    # -- transport -------------------------------------------------------
    def _request(self, path, payload=None, timeout=None, method=None):
        """Every device call goes through here, and every one of them takes the lease.

        Except status reads. Those are metadata, not inference — they do not occupy the
        NPU and must not queue behind a 200-second story, or the board's thermal panel
        goes blind exactly when the device is busiest and most worth watching.
        """
        if "status" in path:
            return self._raw(path, payload, timeout, method)
        with device.lease("%s %s" % (device.UNIT, path.rsplit("/", 1)[-1])):
            return self._raw(path, payload, timeout, method)

    def _raw(self, path, payload=None, timeout=None, method=None):
        if not self.key:
            raise RuntimeError("TIINY_KEY unset — no device credential configured")
        url = self.base_url + path
        data = None
        headers = {
            "Authorization": "Bearer %s" % self.key,
            "Accept": "application/json",
            "User-Agent": "daybreak/1.0",
        }
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            url, data=data, headers=headers, method=method or ("POST" if data else "GET"))
        with urllib.request.urlopen(req, timeout=timeout or self.timeout) as resp:
            body = resp.read().decode("utf-8", "replace")
        return json.loads(body)

    # -- chat ------------------------------------------------------------
    def chat_json(self, system, user, max_tokens=800):
        """-> (dict|None, stats). stats={'gen_tps','ms','tokens_out'} (+ 'error' on failure).

        Two attempts. First with reasoning disabled (cheap, ~50 tokens). If that yields
        no JSON, retry with reasoning ON and a much larger budget -- and handle the
        Ornith quirk: reasoning lands in message.reasoning_content, counts against
        max_tokens, and leaves message.content EMPTY, so the JSON is scavenged from
        the last {...} block of the reasoning trace.
        """
        stats = {"gen_tps": 0.0, "ms": 0.0, "tokens_out": 0}
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
        # Retry budget must clear a full reasoning trace (~1600-2400 tokens observed)
        # or the trace truncates and only the salvage path in _extract_json saves it.
        attempts = [
            (int(max_tokens), NOTHINK),
            (max(int(max_tokens) * 3, 2400), False),
        ]
        last_err = "unknown"
        for budget, nothink in attempts:
            payload = {
                "model": CHAT_MODEL,
                "messages": messages,
                "max_tokens": budget,
                "temperature": 0.2,
            }
            if nothink:
                payload["chat_template_kwargs"] = {"enable_thinking": False}
            t0 = time.time()
            try:
                body = self._request("/v1/chat/completions", payload)
            except Exception as exc:
                stats["ms"] += (time.time() - t0) * 1000.0
                last_err = "%s: %s" % (type(exc).__name__, exc)
                continue
            stats["ms"] += (time.time() - t0) * 1000.0
            try:
                timings = body.get("timings") or {}
                usage = body.get("usage") or {}
                stats["gen_tps"] = float(timings.get("predicted_per_second") or 0.0)
                n_tok = int(usage.get("completion_tokens")
                            or timings.get("predicted_n") or 0)
                n_in = int(usage.get("prompt_tokens") or timings.get("prompt_n") or 0)
                stats["tokens_out"] += n_tok
                stats["tokens_in"] = stats.get("tokens_in", 0) + n_in
                # Counted HERE, at the one place every device generation passes through,
                # rather than at each caller. tokens_total was previously bumped only by
                # the enrichment path, so sitreps, cluster labels, briefs, syntheses,
                # dossiers, geocoding, Ask the Archive and deep dives were all invisible
                # to the odometer - measured at 14% under on docs alone, and more once
                # the paths that record nothing are included. Counting at the choke
                # point means a future caller cannot forget.
                _count_tokens(n_tok + n_in, n_tok)
                count_device_ms(stats.get("ms"))
                msg = ((body.get("choices") or [{}])[0].get("message") or {})
                obj = _extract_json(msg.get("content") or "")
                if obj is None:
                    obj = _extract_json(msg.get("reasoning_content") or "")
            except Exception as exc:
                last_err = "bad response shape: %s" % exc
                obj = None
            if obj is not None:
                return obj, stats
            last_err = "no JSON in response"
        stats["error"] = last_err
        return None, stats

    # -- embeddings ------------------------------------------------------
    def embed(self, text):
        """-> float32 bytes, or None if the embedding model isn't loaded/available."""
        text = _clean(text, 2000)
        if not text:
            return None
        try:
            # Short timeout: a 0.6B embed is sub-second, and this sits in the serial
            # enrich queue -- never let it stall the loop for the full chat timeout.
            body = self._request(
                "/v1/embeddings", {"model": EMBED_MODEL, "input": text},
                timeout=min(self.timeout, 60))
            vec = ((body.get("data") or [{}])[0]).get("embedding")
            if not vec:
                return None
            return struct.pack("<%df" % len(vec), *[float(x) for x in vec])
        except Exception:
            # 404 model_not_found until deploy/load-models.sh runs; clustering falls
            # back to title overlap and meta embeddings=off.
            return None

    # -- telemetry -------------------------------------------------------
    def device_stats(self):
        """-> merged NPU/host stats dict, or None if the device is unreachable."""
        try:
            npu = self._request("/api/v1/npu/status", timeout=min(self.timeout, 20))
        except Exception:
            return None
        out = {
            "npu_util": 0.0, "npu_mem_used_mb": 0.0, "npu_mem_total_mb": 0.0,
            "cpu_pct": 0.0, "mem_pct": 0.0, "models": [],
            "npu_total": 0, "npu_used": 0, "npu_available": 0,
        }
        try:
            devs = npu.get("devices") or []
            if devs:
                out["npu_util"] = max(float(d.get("util_percent") or 0.0) for d in devs)
                out["npu_mem_used_mb"] = sum(float(d.get("mem_used_mb") or 0.0) for d in devs)
                out["npu_mem_total_mb"] = sum(float(d.get("mem_total_mb") or 0.0) for d in devs)
            out["cpu_pct"] = float((npu.get("cpu") or {}).get("total_percent") or 0.0)
            out["mem_pct"] = float((npu.get("memory") or {}).get("usage_percent") or 0.0)
        except Exception:
            pass
        try:
            mods = self._request("/api/v1/models/npu/status", timeout=min(self.timeout, 20))
            out["npu_total"] = int(mods.get("npu_total") or 0)
            out["npu_used"] = int(mods.get("npu_used") or 0)
            out["npu_available"] = int(mods.get("npu_available") or 0)
            out["models"] = [
                {
                    "model_id": m.get("model_id"),
                    "npu_usage": m.get("npu_usage"),
                    "status": m.get("status"),
                }
                for m in (mods.get("models") or [])
            ]
        except Exception:
            pass  # NPU stats alone are still worth recording
        return out


# ---------------------------------------------------------------- enrichment

_SYSTEM = (
    "You are an OSINT tagging function for a military watch floor. You do not "
    "deliberate. You emit one JSON object immediately and stop."
)

# Schema-as-JSON with the rules inline reads better to Ornith than a prose spec, and it
# holds the enums even with reasoning disabled (validated across 4 live sources).
_PROMPT = """Tag this news item. Output ONE line of JSON, nothing else.

{{"summary":"1-2 factual sentences, max 45 words, no lead-in phrase","category":"one of: {cats}","region":"COCOM AOR of the primary country, one of: {regs}","severity":"integer 1-5: 5=mass casualty/major attack/war escalation/nuclear, 4=significant armed action/major disaster/coup, 3=notable political-security development, 2=routine geopolitics or economy, 1=low signal","countries":["up to 4 country names, [] if none"]}}

AOR map: NORTHCOM=US/Canada/Mexico/Caribbean. SOUTHCOM=Latin America. EUCOM=Europe/Russia/Ukraine. CENTCOM=Middle East/Iran/Iraq/Syria/Egypt/Afghanistan/Pakistan. AFRICOM=Africa minus Egypt. INDOPACOM=Asia-Pacific/India/China/Australia. GLOBAL=worldwide or none.

SOURCE: {source}
TITLE: {title}
BODY: {body}

JSON:"""


def enrich_item(t, row, stats_out=None):
    """-> dict with summary/category/region/severity/countries/embedding/stats, or None.

    `stats_out`: optional dict the caller owns; it is filled with the chat stats
    (including `error`) even when this returns None, so a failure reason survives
    instead of being thrown away with the return value.
    """
    def _fail(reason):
        if isinstance(stats_out, dict):
            stats_out.setdefault("error", reason)
        return None

    title = _clean(_row_get(row, "title", ""), 400)
    if not title:
        return _fail("empty title")
    body = _clean(_row_get(row, "raw_summary", ""), 900)
    source = _clean(_row_get(row, "source", "unknown"), 80)

    obj, stats = t.chat_json(
        _SYSTEM,
        _PROMPT.format(cats=" | ".join(CATEGORIES), regs=" | ".join(REGIONS),
                       source=source, title=title, body=body or "(none)"),
        max_tokens=800,
    )
    if isinstance(stats_out, dict):
        stats_out.update(stats)
    if not obj:
        return _fail("no usable JSON from chat")

    summary = _clean(obj.get("summary") or obj.get("Summary") or "", 600)
    if not summary:
        summary = body[:300] or title  # model gave enums but no prose; keep the row usable

    category = str(obj.get("category") or "").strip().lower()
    if category not in CATEGORIES:
        category = DEFAULT_CATEGORY

    region = str(obj.get("region") or "").strip().upper().replace(" ", "")
    if region not in REGIONS:
        region = DEFAULT_REGION

    try:
        severity = int(float(obj.get("severity")))
    except Exception:
        severity = 2
    severity = max(1, min(5, severity))

    countries = obj.get("countries")
    if isinstance(countries, str):
        countries = [c.strip() for c in countries.split(",")]
    if not isinstance(countries, list):
        countries = []
    clean_countries = []
    for c in countries:
        c = _clean(c, 60)
        if c and c.lower() not in ("none", "n/a", "null") and c not in clean_countries:
            clean_countries.append(c)
        if len(clean_countries) >= 4:
            break

    emb = t.embed("%s. %s" % (title, summary))

    return {
        "summary": summary,
        "category": category,
        "region": region,
        "severity": severity,
        "countries": clean_countries,
        "embedding": emb,
        "stats": stats,
    }


_LABEL_SYSTEM = (
    "You name developing news stories for a watch floor. You do not deliberate. You "
    "emit one JSON object immediately and stop."
)

# Words that must never be the last word of a cluster label after truncation.
_DANGLING = frozenset("""
a an the and or but of to in on at for from by with as is are was were be been
after before over under into during against about amid across near through
than then that this these those its his her their our your my no not
""".split())


def label_cluster(t, titles):
    """-> short headline (<=8 words) for a developing story, or None."""
    titles = [_clean(x, 200) for x in (titles or []) if _clean(x, 200)]
    if not titles:
        return None
    listed = "\n".join("- %s" % x for x in titles[:8])
    obj, _stats = t.chat_json(
        _LABEL_SYSTEM,
        "These headlines describe ONE developing story:\n%s\n\n"
        "Return STRICT JSON: {\"label\": string}\n"
        "label = a neutral headline naming the story, MAX 8 words, no trailing period,"
        " no quotes, Title Case. JSON:" % listed,
        max_tokens=800,
    )
    if not obj:
        return None
    label = _clean(obj.get("label") or obj.get("Label") or "", 120).strip(' "\'.')
    if not label:
        return None
    words = label.split()
    if len(words) > 8:
        words = words[:8]
        # Hard-cutting at 8 words leaves a dangling connective on the wall board —
        # observed live: "Pakistan Ex-PM Imran Khan Returns To Jail After". Drop
        # trailing joiners so the label reads as a finished headline.
        while len(words) > 3 and words[-1].strip(",;:").lower() in _DANGLING:
            words.pop()
        label = " ".join(words)
    return label.rstrip(" ,;:-–—").strip()


# ---------------------------------------------------------------- clustering

CLUSTER_WINDOW_S = 72 * 3600
JACCARD_THRESHOLD = 0.55  # per contract, title-overlap fallback
_MEMBERS_PER_CLUSTER = 8  # compare against each cluster's most recent members only

# 0.80 per contract, and MEASURED to be right — do not lower it without re-running
# the measurement. Integrator check 2026-08-21 against the live Qwen3 embedder:
# 89 real enriched articles off the running feed set = 3,916 real pairs, embedded
# exactly the way enrich_item does it. Every pair at or above 0.808 was a genuine
# same-story match (10 of them, spanning BBC/Guardian/Al Jazeera/France24). The
# next pair down, 0.774, was junk — "Israeli settler attacks in the West Bank"
# against "Labour stalling on Jackdaw and Rosebank" — and it sits BETWEEN two
# genuine matches at 0.724 (Ebola in DR Congo) and 0.720 (West Bank). The
# 0.72–0.78 band is therefore genuinely mixed; no threshold inside it separates
# signal from noise. 0.80 is the clean cut.
#
# An earlier pass lowered this to 0.72 off a 55-pair hand-picked sample that
# under-represented the unrelated tail; on the full corpus that put a visibly wrong
# story on the DEVELOPING panel. On a wall board a false join costs much more than
# a missed one — a missed join just leaves two items sitting on the wire, a false
# join invents a developing story and hangs an AI-written label on it.
CLUSTER_THRESHOLD = 0.80


def _item_title_severity(con, item_id):
    try:
        cur = con.execute(
            "SELECT title, severity FROM items WHERE id=?", (item_id,))
        row = cur.fetchone()
    except Exception:
        return "", 1
    if not row:
        return "", 1
    try:
        title = row["title"]
        sev = row["severity"]
    except Exception:
        title, sev = row[0], row[1]
    try:
        sev = max(1, min(5, int(sev)))
    except Exception:
        sev = 1
    return title or "", sev


def _recent_cluster_titles(con, since_ts):
    """-> {cluster_id: [titles newest-first]} for clusters touched since since_ts."""
    out = {}
    try:
        cur = con.execute(
            "SELECT i.cluster_id, i.title FROM items i JOIN clusters c ON c.id=i.cluster_id "
            "WHERE i.cluster_id IS NOT NULL AND c.updated_at >= ? "
            "ORDER BY i.id DESC LIMIT 600", (since_ts,))
        for row in cur.fetchall():
            try:
                cid, title = row["cluster_id"], row["title"]
            except Exception:
                cid, title = row[0], row[1]
            bucket = out.setdefault(cid, [])
            if len(bucket) < _MEMBERS_PER_CLUSTER:
                bucket.append(title or "")
    except Exception:
        return {}
    return out


def assign_cluster(con, item_id, emb_bytes, threshold=CLUSTER_THRESHOLD, members=None):
    """Join the nearest cluster touched in the last 72h, else start a new one.

    Embedding path: max cosine against each cluster's most recent members.
    Fallback (embeddings off): max title-Jaccard, threshold 0.55.
    Returns the cluster id, or None if the DB layer is unavailable.

    `members` is an optional pre-fetched [(item_id, cluster_id, embedding), ...]
    window, for callers assigning many items in one pass (jobs.job_recluster does
    up to RECLUSTER_BATCH=40). Without it each call re-runs clustered_embeddings,
    reloading every clustered item of the last 72h -- 4 KB of embedding each --
    from SQLite, forty times over, which is tens of seconds of pinned CPU on a Pi.
    Default None keeps the single-item behaviour exactly as it was.
    """
    if _db is None:
        return None
    now = time.time()
    since = now - CLUSTER_WINDOW_S
    title, severity = _item_title_severity(con, item_id)

    best_cid, best_score = None, 0.0
    try:
        if emb_bytes:
            if members is None:
                members = _db.clustered_embeddings(con, since) or []
            per_cluster = {}
            for iid, cid, blob in members:
                if cid is None or not blob or iid == item_id:
                    continue
                per_cluster.setdefault(cid, []).append((iid, blob))
            for cid, group in per_cluster.items():   # not `members`: that is the arg
                group.sort(key=lambda m: m[0], reverse=True)    # id asc == time asc
                for _iid, blob in group[:_MEMBERS_PER_CLUSTER]:
                    score = cosine(emb_bytes, blob)
                    if score > best_score:
                        best_cid, best_score = cid, score
            cutoff = threshold
        else:
            for cid, titles in _recent_cluster_titles(con, since).items():
                for other in titles:
                    score = jaccard(title, other)
                    if score > best_score:
                        best_cid, best_score = cid, score
            cutoff = JACCARD_THRESHOLD
    except Exception:
        best_cid, best_score, cutoff = None, 0.0, threshold

    try:
        if best_cid is not None and best_score >= cutoff:
            cid = _db.upsert_cluster(con, best_cid, None, severity, now)
        else:
            # label stays NULL until the pipeline calls label_cluster at >=3 members.
            cid = _db.upsert_cluster(con, None, None, severity, now)
        if cid is not None:
            _db.set_cluster(con, item_id, cid)
        return cid
    except Exception:
        return None


# ---------------------------------------------------------------- self-check

def _selfcheck():
    t = Tiiny()
    print("DAYBREAK enrich.py self-check -> %s" % t.base_url)

    print("\n[1] device_stats()")
    stats = t.device_stats()
    if stats is None:
        print("    DEVICE UNREACHABLE - skipping live checks (this is survivable).")
        return 0
    print("    " + json.dumps(stats))

    print("\n[2] enrich_item() on a synthetic item")
    fake = {
        "title": "Explosion at Kharkiv power substation kills three, cuts grid to 200,000",
        "raw_summary": ("Ukrainian officials said a large-scale missile strike hit "
                        "energy infrastructure in Kharkiv overnight, killing three "
                        "workers and leaving about 200,000 residents without power. "
                        "Emergency crews are working to restore the grid."),
        "source": "selfcheck",
    }
    t0 = time.time()
    out = enrich_item(t, fake)
    if out is None:
        print("    ENRICH FAILED (device returned no parseable JSON)")
    else:
        printable = dict(out)
        emb = printable.pop("embedding")
        printable["embedding"] = ("%d bytes / %d dims" % (len(emb), len(emb) // 4)
                                  if emb else None)
        print("    " + json.dumps(printable, indent=2))
    print("    wall: %.1fs" % (time.time() - t0))

    print("\n[3] embed()")
    emb = t.embed("test vector for daybreak")
    if emb is None:
        print("    EMBEDDINGS OFF - %s not loaded; clustering falls back to "
              "title-Jaccard (meta embeddings=off). Run deploy/load-models.sh."
              % EMBED_MODEL)
    else:
        print("    %d bytes = %d float32 dims" % (len(emb), len(emb) // 4))
        print("    self-cosine: %.4f" % cosine(emb, emb))

    print("\n[4] label_cluster()")
    label = label_cluster(t, [
        "Explosion at Kharkiv power substation kills three",
        "Kharkiv grid down for 200,000 after overnight strike",
        "Ukraine says missile barrage targeted energy infrastructure",
    ])
    print("    label: %r" % label)

    print("\n[5] offline math")
    a = struct.pack("<3f", 1.0, 0.0, 0.0)
    b = struct.pack("<3f", 0.0, 1.0, 0.0)
    print("    cosine(a,a)=%.3f cosine(a,b)=%.3f cosine(a,b'')=%.3f"
          % (cosine(a, a), cosine(a, b), cosine(a, b"")))
    print("    jaccard: %.3f / %.3f"
          % (jaccard("Kharkiv substation explosion kills three",
                     "Explosion at Kharkiv substation kills 3 workers"),
             jaccard("Kharkiv substation explosion", "Tokyo stock market rallies")))
    print("    json extract: %r"
          % _extract_json('thinking... {"a":1} then final {"severity":4,"ok":true}'))

    print("\n[6] assign_cluster() against a scratch DB")
    print("    " + _cluster_selfcheck())
    return 0


def _cluster_selfcheck():
    """Exercise both clustering paths against real db.py in a throwaway file."""
    if _db is None:
        return "db.py unavailable - skipped"
    import tempfile
    tmp = os.path.join(tempfile.mkdtemp(prefix="daybreak-selfcheck-"), "t.db")
    try:
        con = _db.connect(tmp)
        now = time.time()

        def add(url, title, emb):
            iid = _db.insert_item(con, url, "selfcheck", title, now, "")
            _db.mark_enriched(con, iid, "s", "conflict", "EUCOM", 4, "[]", emb)
            return iid, assign_cluster(con, iid, emb)

        v1 = struct.pack("<4f", 1.0, 0.0, 0.0, 0.0)
        v2 = struct.pack("<4f", 0.98, 0.2, 0.0, 0.0)   # ~0.98 cosine -> joins
        v3 = struct.pack("<4f", 0.0, 0.0, 1.0, 0.0)    # orthogonal -> new cluster
        _, c1 = add("http://x/1", "Kharkiv substation strike kills three", v1)
        _, c2 = add("http://x/2", "Three dead in Kharkiv substation attack", v2)
        _, c3 = add("http://x/3", "Tokyo stock market rallies on chip demand", v3)
        emb_ok = (c1 == c2 and c3 != c1)

        # embeddings off -> title-Jaccard fallback
        _, c4 = add("http://x/4", "Nairobi protest crackdown leaves dozens dead", None)
        _, c5 = add("http://x/5", "Dozens dead in Nairobi protest crackdown", None)
        _, c6 = add("http://x/6", "Chile copper output falls sharply", None)
        jac_ok = (c4 == c5 and c6 != c4)
        return ("embedding path %s (c1=%s c2=%s c3=%s) | jaccard path %s "
                "(c4=%s c5=%s c6=%s)"
                % ("OK" if emb_ok else "FAIL", c1, c2, c3,
                   "OK" if jac_ok else "FAIL", c4, c5, c6))
    except Exception as exc:
        return "FAILED: %s: %s" % (type(exc).__name__, exc)


if __name__ == "__main__":
    raise SystemExit(_selfcheck())
