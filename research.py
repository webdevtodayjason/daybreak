#!/usr/bin/env python3
"""Deep dive: what the outside world says about a SITREP card, and what it means.

The board is deliberately closed — twenty-seven feeds, everything analysed on the device,
no outbound calls. That is the right default and it is also a limit: the wire knows what
the wire carries, and nothing about what it missed.

A deep dive is the explicit exception. On request, and only on request, it searches the
open web for a card's subject, hands those results *plus* the board's own wire items to
Ornith, and asks for an assessment rather than a summary: what the outside reporting adds,
where it disagrees with our own, and what would change the picture.

Two things make it an analyst rather than a search box:

* It cites. Every external claim keeps its source and link, so a reader can check it.
* It remembers. Each dive is stored against a stable key for its subject, and the next
  dive on that subject is shown the previous one. That is what lets it say "this
  contradicts the assessment three days ago" instead of starting from nothing every time.

Search runs against TinyFish (free tier per their docs). The synthesis runs on the Tiiny.
"""

import contextlib
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import device

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

TF_URL = os.environ.get("TINYFISH_SEARCH_URL") or "https://agent.tinyfish.ai/v1/search"
TF_KEY_FILE = os.environ.get("TINYFISH_KEY_FILE") or "/etc/daybreak-tinyfish.key"
TF_RESULTS = int(os.environ.get("DAYBREAK_TF_RESULTS") or 6)
TF_TIMEOUT = float(os.environ.get("DAYBREAK_TF_TIMEOUT") or 25)

# A dive older than this is refreshed rather than served from cache.
DIVE_FRESH_S = float(os.environ.get("DAYBREAK_DIVE_FRESH") or 6 * 3600)
# The device's own gateway closes a request at roughly 220 seconds - measured: 580
# tokens generated, then HTTP 504 at 222.3s, with the client timeout still at 780. So
# the budget is not ours to choose. At ~25 tok/s generation and ~24 tok/s prefill, a
# 1,200 token prompt costs ~50s and leaves ~150s, which is about 3,700 tokens including
# whatever the model spends reasoning. Sized to land comfortably inside that.
STORY_TIMEOUT = float(os.environ.get("DAYBREAK_STORY_TIMEOUT") or 240)
STORY_MAX_TOKENS = int(os.environ.get("DAYBREAK_STORY_MAX_TOKENS") or 2000)
STORY_SRC_CHARS = int(os.environ.get("DAYBREAK_STORY_SRC_CHARS") or 6000)

_KEY = {"v": None, "read": False}


def _api_key():
    if not _KEY["read"]:
        _KEY["read"] = True
        val = os.environ.get("TINYFISH_API_KEY")
        if not val:
            try:
                with open(TF_KEY_FILE) as fh:
                    for line in fh:
                        line = line.strip()
                        if line.startswith("TINYFISH_API_KEY"):
                            val = line.split("=", 1)[1].strip().strip('"').strip("'")
                            break
                        if line and "=" not in line and not line.startswith("#"):
                            val = line            # bare key on a line by itself
                            break
            except OSError:
                val = None
        _KEY["v"] = val or None
    return _KEY["v"]


def available():
    return bool(_api_key())


STOP = {"the", "a", "an", "of", "in", "on", "at", "to", "for", "and", "as", "after",
        "with", "from", "over", "amid", "into", "by", "is", "are", "says", "said",
        "new", "more", "than", "that", "this", "its", "his", "her", "their", "up",
        "out", "off", "how", "why", "what", "who", "kills", "killed", "hits", "hit"}


def terms(title):
    """The significant words of a headline, as a set."""
    return {w for w in re.findall(r"[a-z0-9]+", str(title or "").lower())
            if len(w) > 3 and w not in STOP}


def subject_key(aor, title):
    """Exact cache key for one card. Identical wording reuses the stored dive."""
    ws = sorted(terms(title))
    raw = "%s|%s" % (str(aor or "").upper(), " ".join(ws))
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def find_prior(con, aor, title, exclude_key=None, window_days=21):
    """The most recent earlier dive on the same subject, matched by word overlap.

    Hashing the headline was the obvious approach and it does not work: "Iran closes
    Strait of Hormuz collapsing oil traffic" and "Iran closes the Strait of Hormuz as
    tankers divert" are plainly the same story and hash differently, because the words
    that differ land inside whatever window the hash takes. Continuity has to tolerate
    rewording, so it is measured by how many significant words two headlines share
    rather than by whether they are identical.
    """
    want = terms(title)
    if len(want) < 2:
        return None
    since = time.time() - window_days * 86400
    best, best_score = None, 0
    try:
        rows = con.execute(
            "SELECT id, key, aor, title, created_at, analysis FROM deepdive "
            "WHERE aor = ? AND created_at >= ? ORDER BY created_at DESC LIMIT 40",
            (str(aor or "").upper(), since)).fetchall()
    except Exception:
        return None
    for r in rows:
        if exclude_key and r["key"] == exclude_key:
            continue
        got = terms(r["title"])
        shared = want & got
        # Two shared significant words in the same theatre is a low bar in the abstract
        # and a high one in practice: "hormuz" plus "strait" is the same story, while
        # two unrelated EUCOM headlines rarely share two words this specific.
        score = len(shared)
        if score >= 2 and score > best_score:
            best, best_score = r, score
    return best


def search(query, n=None):
    """TinyFish web search. Returns [] on any failure — never raises at the caller."""
    key = _api_key()
    if not key:
        return []
    qs = urllib.parse.urlencode({"query": query[:300],
                                 "num_results": int(n or TF_RESULTS)})
    req = urllib.request.Request(TF_URL + "?" + qs,
                                 headers={"X-API-Key": key, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=TF_TIMEOUT) as resp:
            data = json.loads(resp.read() or b"{}")
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        return []
    out = []
    for r in (data.get("results") or [])[: int(n or TF_RESULTS)]:
        title = (r.get("title") or "").strip()
        url = (r.get("url") or "").strip()
        if not title or not url:
            continue
        out.append({"title": title[:200], "url": url[:400],
                    "site": (r.get("site_name") or "")[:80],
                    "date": (r.get("date") or "")[:40],
                    "snippet": " ".join((r.get("snippet") or "").split())[:400]})
    return out


ANALYST_SYSTEM = (
    "You are an intelligence analyst, not a summariser. You are given a standing "
    "assessment, the wire items it was written from, and open-source reporting found "
    "just now. Produce an analyst's read.\n\n"
    "Reply as labelled lines, exactly this shape and nothing else:\n\n"
    "ASSESSMENT: 2-3 sentences. What is actually going on, weighing the outside "
    "reporting against the wire. Lead with the judgement, not the recap.\n"
    "CONFIDENCE: HIGH, MODERATE or LOW, then a short clause saying why.\n"
    "ADDS: what the outside reporting establishes that the wire did not have. If it "
    "adds nothing, say so plainly.\n"
    "TENSION: where the sources disagree with each other or with the wire, naming who "
    "says what. If they agree, say they agree.\n"
    "WATCH: the specific indicator that would most change this assessment, and which "
    "way it would move it.\n"
    "CHANGED: only if a previous assessment is supplied — what has changed since it, or "
    "NOTHING if the picture is unchanged. Omit this line entirely if there is no "
    "previous assessment.\n\n"
    "Be concrete. Name actors, numbers and dates. Do not hedge everything; an analyst "
    "who never commits is useless. Do not invent facts not present in the material.")

FIELDS = ("assessment", "confidence", "adds", "tension", "watch", "changed")


def parse_analysis(raw):
    """Labelled lines out of a reply that may be buried in the model's reasoning."""
    out, cur = {}, None
    for line in str(raw or "").splitlines():
        line = line.strip()
        if not line:
            continue
        head, sep, rest = line.partition(":")
        key = head.strip().lower()
        if sep and key in FIELDS:
            cur = key
            out[cur] = " ".join(rest.split())
        elif cur and len(out.get(cur, "")) < 600:
            out[cur] = (out[cur] + " " + " ".join(line.split())).strip()
    for k in list(out):
        out[k] = out[k][:700]
    return out if out.get("assessment") else None


SCHEMA = """
CREATE TABLE IF NOT EXISTS deepdive(
  id         INTEGER PRIMARY KEY,
  key        TEXT NOT NULL,
  aor        TEXT,
  title      TEXT,
  created_at REAL NOT NULL,
  sources    TEXT,      -- JSON: the external results, with links, so claims stay checkable
  analysis   TEXT,      -- JSON: the parsed analyst fields
  prior_id   INTEGER,   -- the dive this one was written against, if any
  ms         INTEGER
);
CREATE INDEX IF NOT EXISTS idx_dive_key ON deepdive(key, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dive_aor ON deepdive(aor, created_at DESC);
"""


def ensure(con):
    con.executescript(SCHEMA)
    con.commit()


def latest(con, key, max_age=None):
    """A cached dive for this exact card, if it is still fresh."""
    try:
        r = con.execute("SELECT * FROM deepdive WHERE key=? ORDER BY created_at DESC LIMIT 1",
                        (key,)).fetchone()
    except Exception:
        return None
    if not r:
        return None
    if max_age is not None and (time.time() - float(r["created_at"])) > max_age:
        return None
    return r


def as_dict(row):
    if not row:
        return None
    try:
        analysis = json.loads(row["analysis"] or "{}")
    except (ValueError, TypeError):
        analysis = {}
    try:
        sources = json.loads(row["sources"] or "[]")
    except (ValueError, TypeError):
        sources = []
    return {"id": row["id"], "key": row["key"], "aor": row["aor"], "title": row["title"],
            "created_at": row["created_at"], "analysis": analysis, "sources": sources,
            "prior_id": row["prior_id"], "ms": row["ms"]}


def run_dive(con, dev, card, say=None):
    """Search, synthesise, store. Returns the stored dive as a dict, or None.

    Never raises: a deep dive is an enhancement, and a board panel must not be able to
    break because a search engine had a bad minute.
    """
    say = say or (lambda m: None)
    ensure(con)
    aor = str(card.get("aor") or "GLOBAL").upper()
    title = str(card.get("title") or card.get("situation") or "")[:220]
    key = subject_key(aor, title)

    cached = latest(con, key, DIVE_FRESH_S)
    if cached is not None:
        return as_dict(cached)

    t0 = time.time()
    query = "%s %s" % (title, aor if aor != "GLOBAL" else "")
    hits = search(query.strip())
    if not hits:
        say("[dive] no external results for %s" % title[:60])
        return None

    prior = find_prior(con, aor, title, exclude_key=key)
    prior_txt = ""
    if prior is not None:
        try:
            pa = json.loads(prior["analysis"] or "{}")
        except (ValueError, TypeError):
            pa = {}
        if pa.get("assessment"):
            age_h = (time.time() - float(prior["created_at"])) / 3600.0
            prior_txt = ("\n\nPREVIOUS ASSESSMENT (%.0f hours ago, on \"%s\"):\n%s"
                         % (age_h, str(prior["title"])[:120], pa["assessment"]))

    wire = ""
    for srcs in (card.get("sources") or [])[:6]:
        wire += "- [S%s] %s (%s)\n" % (srcs.get("severity"), srcs.get("title"),
                                       srcs.get("source"))
    outside = ""
    for i, h in enumerate(hits, 1):
        outside += "%d. %s — %s%s\n   %s\n" % (
            i, h["title"], h["site"], (" (%s)" % h["date"]) if h["date"] else "",
            h["snippet"])

    user = ("STANDING ASSESSMENT (%s):\n%s\n%s\n\nOUR WIRE ITEMS:\n%s\nOPEN-SOURCE "
            "REPORTING FOUND JUST NOW:\n%s%s"
            % (aor, title, card.get("situation") or "", wire or "(none)", outside,
               prior_txt))

    obj, _stats = dev.chat_json(
        ANALYST_SYSTEM + '\n\nReturn ONLY JSON: {"report": "<the labelled lines, newlines as \\n>"}',
        user, max_tokens=2600)
    parsed = parse_analysis((obj or {}).get("report"))
    if not parsed:
        say("[dive] model returned nothing usable for %s" % title[:60])
        return None

    ms = int((time.time() - t0) * 1000)
    cur = con.execute(
        "INSERT INTO deepdive(key,aor,title,created_at,sources,analysis,prior_id,ms) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (key, aor, title, time.time(), json.dumps(hits), json.dumps(parsed),
         prior["id"] if prior is not None else None, ms))
    con.commit()
    say("[dive] %s: %d sources, %s prior, %.1fs"
        % (title[:48], len(hits), "with" if prior is not None else "no", ms / 1000.0))
    return as_dict(con.execute("SELECT * FROM deepdive WHERE id=?",
                               (cur.lastrowid,)).fetchone())


# --------------------------------------------------------------------------- #
# full story: fetch the source, then write our own piece from it
# --------------------------------------------------------------------------- #

TF_FETCH_URL = os.environ.get("TINYFISH_FETCH_URL") or "https://agent.tinyfish.ai/v1/fetch"
STORY_SCHEMA = """
CREATE TABLE IF NOT EXISTS story(
  item_id    INTEGER PRIMARY KEY,
  created_at REAL NOT NULL,
  headline   TEXT,
  body       TEXT,        -- the written article
  words      INTEGER,
  src_url    TEXT,
  src_chars  INTEGER,     -- how much source text we actually got
  meta       TEXT,
  ms         INTEGER
);
"""


def story_ensure(con):
    con.executescript(STORY_SCHEMA)
    con.commit()


def fetch_article(url, timeout=None):
    """Pull the readable text of a page. Returns a dict or None; never raises.

    The wire gives a headline and two sentences of summary. The source gives the whole
    piece, which is both far better material to write from and a far larger prompt -
    which is the point: prefill is where most of the device work actually is.
    """
    key = _api_key()
    if not key or not url:
        return None
    body = json.dumps({"urls": [str(url)[:900]]}).encode()
    req = urllib.request.Request(
        TF_FETCH_URL, data=body,
        headers={"X-API-Key": key, "Content-Type": "application/json",
                 "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout or 45) as resp:
            data = json.loads(resp.read() or b"{}")
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        return None
    rows = data.get("results") or []
    if not rows:
        return None
    r = rows[0]
    text = " ".join(str(r.get("text") or "").split())
    if len(text) < 400:                  # a nav bar or a paywall stub, not an article
        return None
    return {"title": (r.get("title") or "")[:250], "text": text[:STORY_SRC_CHARS],
            "author": (r.get("author") or "")[:120],
            "published": (r.get("published_date") or "")[:40],
            "url": r.get("final_url") or r.get("url") or url}


# Written in sections, one call each. A single long-form generation cannot complete on
# this device: the gateway closes the request at ~220s and Ornith spends much of any
# budget reasoning before it writes, so a 2,000 token call still died at 504 twice over.
# Three short calls each land in 60-90s, never approach the limit, and between them
# produce more device work than the one call ever did - which is the point.
STORY_PARTS = (
    ("lede",
     "Write the OPENING: strictly the NEW development - what happened, when, and who did "
     "it. 2 short paragraphs, about 130 words. Background belongs in a later section: do "
     "not explain history, prior convictions, or how things got here."),
    ("detail",
     "Write the MIDDLE: the BACKGROUND the opening deliberately left out - who the parties "
     "are, how this came about, the prior history, the specific numbers and dates behind "
     "it. 2 to 3 short paragraphs, about 180 words. The opening already reported the news "
     "itself; do not report it again, build underneath it."),
    ("context",
     "Write the CLOSE: the wider pattern this sits in and what it bears on going forward. "
     "2 short paragraphs, about 130 words. No new facts and no recap - this section is "
     "significance only. Do not end with a paragraph about what the source does not cover."),
)

STORY_BASE = (
    "You are a desk writer on an OSINT wire. You are given the original reporting on a "
    "story and asked for one section of our own account of it.\n\n"
    "Rules that matter:\n"
    "- Use only facts present in the source. Do not invent numbers, quotes, names or "
    "dates. If the source does not say, do not say.\n"
    "- Attribute what the source attributes: who said it, who reported it.\n"
    "- Do not copy sentences from the source. Write it yourself.\n"
    "- Plain declarative prose. No subheadings, no bullets, no markdown.\n"
    "- Do not editorialise and do not tell the reader what to feel.\n"
    "- Answer immediately. Do not deliberate about structure before writing.\n\n"
    "Write ONLY the section asked for, as flowing paragraphs. No labels, no preamble.")

STORY_HEAD_SYSTEM = (
    "Write a headline for this article in our own words, under 14 words, plain and "
    "factual, no colon, no clickbait. Reply with the headline alone and nothing else.")


def parse_story(raw):
    txt = str(raw or "")
    head, body = "", ""
    m = re.search(r"HEADLINE\s*:\s*(.+)", txt)
    if m:
        head = " ".join(m.group(1).split())[:200]
    m2 = re.search(r"ARTICLE\s*:\s*(.+)", txt, re.S)
    if m2:
        body = m2.group(1).strip()
    if not body:
        return None
    # tidy: collapse runs of blank lines, drop any stray label the model repeated
    paras = [" ".join(p.split()) for p in re.split(r"\n\s*\n", body) if p.strip()]
    paras = [p for p in paras if not re.match(r"(?i)^(headline|article)\s*:", p)]
    body = "\n\n".join(paras)
    # Guard against the padding this model does when it is chasing a word count: it
    # starts strong and then writes paragraph after paragraph about what the source
    # does not say. Drop trailing paragraphs that are meta-commentary or that repeat an
    # earlier one, then hard-cap the length.
    META = re.compile(r"(?i)^(the (source|article|reporting|piece)\b|in (summary|conclusion)\b)")
    kept, seen = [], set()
    for para in paras:
        sig = " ".join(sorted(set(re.findall(r"[a-z]{5,}", para.lower()))))[:160]
        if sig in seen:
            continue                    # near-duplicate of something already said
        seen.add(sig)
        kept.append(para)
    # allow at most one meta paragraph, and never as the closer
    meta_seen = 0
    out = []
    for para in kept:
        if META.match(para):
            meta_seen += 1
            if meta_seen > 1:
                continue
        out.append(para)
    while out and META.match(out[-1]):
        out.pop()
    # hard cap around 900 words on a paragraph boundary
    final, total = [], 0
    for para in out:
        n = len(para.split())
        if total and total + n > 900:
            break
        final.append(para)
        total += n
    body = "\n\n".join(final)
    words = len(body.split())
    if words < 120:
        return None
    return {"headline": head, "body": body[:12000], "words": words}


@contextlib.contextmanager
def device_lease(con, label, seconds=420):
    """Hold the NPU while we write, the same way the job scheduler does.

    Without this the story competed with the pipeline for three minutes and lost: the
    enricher and the image job kept starting work on the same device handle, the call
    came back 150004, and the write produced nothing. img_hold_until is the key every
    consumer actually polls; enrich_busy_until and now_doing are read by the board.
    """
    # The onelane hold is what actually makes this atomic. A desk report is three
    # sectioned calls back to back -- the gateway hard-closes any single request at
    # ~220s, so it cannot be one call -- and between those sections the timestamps
    # could only ask other threads to wait. The on-demand image endpoint lives in the
    # OTHER process and is fired by a reader clicking an article, which is exactly the
    # collision that started this. Timestamps stay until commit 3 so rollback needs no
    # data migration.
    with device.lease("%s %s" % (device.UNIT, str(label)[:80]), wait=device.LEASE_WAIT):
        yield from _lease_flags(con, label, seconds)


def _lease_flags(con, label, seconds):
    until = "%.3f" % (time.time() + float(seconds))
    for k in ("enrich_busy_until", "img_hold_until"):
        try:
            con.execute("INSERT INTO meta(key,value) VALUES(?,?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (k, until))
        except Exception:
            pass
    try:
        con.execute("INSERT INTO meta(key,value) VALUES('now_doing',?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(label)[:120],))
        con.commit()
    except Exception:
        pass
    try:
        yield
    finally:
        for k in ("enrich_busy_until", "img_hold_until", "now_doing"):
            try:
                con.execute("UPDATE meta SET value=? WHERE key=? AND value=?",
                            ("0" if k != "now_doing" else "", k,
                             until if k != "now_doing" else str(label)[:120]))
            except Exception:
                pass
        try:
            con.commit()
        except Exception:
            pass


def _tidy(text):
    """Normalise whitespace WITHIN paragraphs but keep the breaks BETWEEN them.

    The first version collapsed the whole section with " ".join(text.split()), which
    destroyed every paragraph break the model had written and produced one continuous
    slab of prose on the page. Blank lines are the only structure this article has.
    """
    paras = [" ".join(p.split()) for p in re.split(r"\n\s*\n|\n(?=[A-Z\"'])", str(text or ""))]
    return "\n\n".join(p for p in paras if p)


def _one_part(dev, source_block, brief, written=None, max_tokens=900):
    """One section. Short enough that the gateway never sees a long request.

    `written` is what the article already says. Without it each section is composed
    blind and they all independently reach for the same headline facts - the first draft
    had the opening and the detail section covering the signatories, the conviction, the
    poll and Ramaphosa twice over.
    """
    sofar = ""
    if written:
        sofar = ("\n\nALREADY WRITTEN - do not repeat any of this, do not restate these "
                 "facts, continue from where it leaves off:\n" + written[-2200:])
    obj, stats = dev.chat_json(
        STORY_BASE + "\n\nSECTION TO WRITE:\n" + brief + sofar +
        '\n\nSeparate paragraphs with a blank line.'
        '\n\nReturn ONLY JSON: {"text": "<the section, newlines as \\n>"}',
        source_block, max_tokens=max_tokens)
    return _tidy((obj or {}).get("text")), (stats or {})


def run_story(con, dev, item, say=None, max_age=None):
    """Fetch the source and write our own article from it, section by section."""
    say = say or (lambda m: None)
    story_ensure(con)
    iid = int(item["id"])
    row = con.execute("SELECT * FROM story WHERE item_id=?", (iid,)).fetchone()
    if row is not None and (max_age is None or
                            (time.time() - float(row["created_at"])) <= max_age):
        return story_as_dict(row)

    t0 = time.time()
    page = fetch_article(item["url"])
    if not page:
        say("[story] could not read the source for #%d" % iid)
        return None

    source_block = ("ORIGINAL REPORTING\nSource: %s\nHeadline: %s\n%s%s\n\n%s"
                    % (item.get("source") or "",
                       page["title"] or item.get("title") or "",
                       ("By %s\n" % page["author"]) if page["author"] else "",
                       ("Published %s" % page["published"]) if page["published"] else "",
                       page["text"]))

    parts, tok = [], 0
    with device_lease(con, "WRITING DESK REPORT #%d — ORNITH-35B" % iid, seconds=900):
        for name, brief in STORY_PARTS:
            txt, stats = _one_part(dev, source_block, brief,
                                   written="\n\n".join(parts) if parts else None)
            tok += int(stats.get("tokens_out") or 0)
            if txt:
                parts.append(txt)
            else:
                say("[story] #%d: section %s came back empty" % (iid, name))
        head = ""
        if parts:
            hobj, hstats = dev.chat_json(
                STORY_HEAD_SYSTEM + '\n\nReturn ONLY JSON: {"text": "<headline>"}',
                parts[0][:900], max_tokens=500)
            tok += int((hstats or {}).get("tokens_out") or 0)
            head = " ".join(str((hobj or {}).get("text") or "").split())[:200]

    if len(parts) < 2:
        say("[story] #%d: only %d sections, discarding" % (iid, len(parts)))
        return None

    body = "\n\n".join(parts)
    parsed = parse_story("HEADLINE: %s\nARTICLE: %s" % (head or item.get("title") or "", body))
    if not parsed:
        say("[story] #%d: sections did not survive the parser" % iid)
        return None

    ms = int((time.time() - t0) * 1000)
    meta = {"src_title": page["title"], "author": page["author"],
            "published": page["published"], "tokens": tok, "sections": len(parts)}
    con.execute(
        "INSERT INTO story(item_id,created_at,headline,body,words,src_url,src_chars,meta,ms) "
        "VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(item_id) DO UPDATE SET "
        "created_at=excluded.created_at, headline=excluded.headline, body=excluded.body, "
        "words=excluded.words, src_chars=excluded.src_chars, meta=excluded.meta, ms=excluded.ms",
        (iid, time.time(), parsed["headline"], parsed["body"], parsed["words"],
         page["url"], len(page["text"]), json.dumps(meta), ms))
    con.commit()
    say("[story] #%d: %d words in %d sections, %d tokens, %.0fs"
        % (iid, parsed["words"], len(parts), tok, ms / 1000.0))
    return story_as_dict(con.execute("SELECT * FROM story WHERE item_id=?", (iid,)).fetchone())


def story_as_dict(row):
    if not row:
        return None
    try:
        meta = json.loads(row["meta"] or "{}")
    except (ValueError, TypeError):
        meta = {}
    return {"item_id": row["item_id"], "created_at": row["created_at"],
            "headline": row["headline"], "body": row["body"], "words": row["words"],
            "src_url": row["src_url"], "src_chars": row["src_chars"],
            "meta": meta, "ms": row["ms"]}
