"""DAYBREAK feed ingestion. Stdlib only.

Every URL in FEEDS was fetched and parsed at build time (2026-08-21). Two feeds named
in the contract were dead and are replaced -- see the NOTE comments below.

Nothing in here raises: a dead host, a 404, an HTML error page served as XML, or a
single malformed entry all end up as a string in the returned errors dict. The
pipeline calls this every 5 minutes for a week; it has to be boring.
"""

import email.utils
import gzip
import html
import io
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import db

USER_AGENT = "daybreak/1.0 (+https://daybreak.example.com)"
TIMEOUT = 15
MAX_ITEMS_PER_FEED = 100
MAX_SUMMARY_CHARS = 1200
FETCH_WORKERS = 6
RETRY_STATUS = (403, 408, 429, 500, 502, 503, 504)
RETRY_DELAY_S = 2.0
# Hard ceiling on what one feed host can push into this Pi's RAM. Six of these run
# concurrently every cycle; an RSS document is never anywhere near 4 MB, and an
# unbounded read()/gunzip of a broken origin is an OOM on a box hosting a 35B model.
MAX_BODY = 4 * 1024 * 1024
# Only http(s) article links are accepted from feeds (see parse_entry).
_SAFE_URL = re.compile(r"^https?://", re.I)

FEEDS = [
    # wave 2 (2026-08-21): weather, seismic, volcanic, disaster, cyber, defense,
    # aviation, energy — all verified live before shipping
    {"name": "NWS Severe Alerts", "url": "https://api.weather.gov/alerts/active.atom?severity=Severe,Extreme", "kind": "atom"},
    {"name": "USGS Quakes M2.5+", "url": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.atom", "kind": "atom"},
    {"name": "GDACS Disasters", "url": "https://www.gdacs.org/xml/rss.xml", "kind": "rss"},
    {"name": "Smithsonian Volcanoes", "url": "https://volcano.si.edu/news/WeeklyVolcanoRSS.xml", "kind": "rss"},
    {"name": "The Hacker News", "url": "https://feeds.feedburner.com/TheHackersNews", "kind": "rss"},
    {"name": "BleepingComputer", "url": "https://www.bleepingcomputer.com/feed/", "kind": "rss"},
    {"name": "Krebs on Security", "url": "https://krebsonsecurity.com/feed/", "kind": "rss"},
    {"name": "Breaking Defense", "url": "https://breakingdefense.com/feed/", "kind": "rss"},
    {"name": "The War Zone", "url": "https://www.twz.com/feed", "kind": "rss"},
    {"name": "NYT World", "url": "https://rss.nytimes.com/services/xml/rss/nyt/World.xml", "kind": "rss"},
    {"name": "Sky News World", "url": "https://feeds.skynews.com/feeds/rss/world.xml", "kind": "rss"},
    {"name": "AVweb Aviation", "url": "https://www.avweb.com/feed/", "kind": "rss"},
    {"name": "OilPrice Energy", "url": "https://oilprice.com/rss/main", "kind": "rss"},
    {"name": "BBC World", "url": "https://feeds.bbci.co.uk/news/world/rss.xml", "kind": "rss"},
    {"name": "Guardian World", "url": "https://www.theguardian.com/world/rss", "kind": "rss"},
    {"name": "Al Jazeera", "url": "https://www.aljazeera.com/xml/rss/all.xml", "kind": "rss"},
    {"name": "DW World", "url": "https://rss.dw.com/rdf/rss-en-world", "kind": "rdf"},
    {"name": "France 24", "url": "https://www.france24.com/en/rss", "kind": "rss"},
    # NOTE: the usual kyivindependent.com/feed/ is 404 as of 2026-08-21; this is the URL the
    # site itself advertises via <link rel="alternate">.
    {"name": "Kyiv Independent", "url": "https://kyivindependent.com/news-archive/rss/",
     "kind": "rss"},
    # Times of Israel 403s all UAs since 2026-08-21 (CDN bot-blocking); JPost covers the beat
    {"name": "Jerusalem Post", "url": "https://www.jpost.com/rss/rssfeedsfrontpage.aspx", "kind": "rss"},
    # NOTE: NHK World English RSS is gone (both /nhkworld/en/news/rss/all.xml and
    # /nhkworld/en/rss/news.xml return 404). Channel NewsAsia replaces it as the
    # INDOPACOM wire.
    {"name": "Channel NewsAsia",
     "url": "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml",
     "kind": "rss"},
    {"name": "CISA Advisories", "url": "https://www.cisa.gov/cybersecurity-advisories/all.xml",
     "kind": "rss"},
    {"name": "USGS M4.5+",
     "url": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.atom",
     "kind": "atom"},
    {"name": "ReliefWeb", "url": "https://reliefweb.int/updates/rss.xml", "kind": "rss"},
    {"name": "UN News", "url": "https://news.un.org/feed/subscribe/en/news/all/rss.xml",
     "kind": "rss"},
    {"name": "The Record", "url": "https://therecord.media/feed", "kind": "rss"},
    {"name": "Ukrinform", "url": "https://www.ukrinform.net/rss/block-lastnews", "kind": "rss"},
    # wave 3 (2026-08-22): the Americas + Australia, and a deliberate spread across the
    # US spectrum. Every general-news feed until now was an international desk edition
    # (BBC World, NYT World, Guardian World...), which is the right shape for a world
    # events board and the wrong shape for comparing how outlets tell the SAME story.
    # Comparison needs outlets that lean differently on purpose: the point is not to find
    # an unbiased source, it is to reconstruct the event from the spread. All verified
    # live before shipping; 7 candidates were dropped for 404s, timeouts or malformed XML.
    #
    # 2026-08-23: the Latin American and Canadian general feeds were swapped for their
    # world and politics sections. The whole-site feeds worked, but they are national
    # general news, so the device was spending real NPU grading Rio traffic accidents
    # and depression explainers. Correctly binned S1, and therefore invisible on the
    # board - but paid for at ~3s of inference each.
    {"name": "NPR News", "url": "https://feeds.npr.org/1001/rss.xml", "kind": "rss"},
    {"name": "PBS NewsHour", "url": "https://www.pbs.org/newshour/feeds/rss/headlines", "kind": "rss"},
    {"name": "CBS News World", "url": "https://www.cbsnews.com/latest/rss/world", "kind": "rss"},
    {"name": "ABC News Intl", "url": "https://feeds.abcnews.com/abcnews/internationalheadlines", "kind": "rss"},
    {"name": "NBC News World", "url": "http://feeds.nbcnews.com/nbcnews/public/world", "kind": "rss"},
    {"name": "Fox News World", "url": "https://moxie.foxnews.com/google-publisher/world.xml", "kind": "rss"},
    {"name": "Fox News Politics", "url": "https://moxie.foxnews.com/google-publisher/politics.xml", "kind": "rss"},
    {"name": "Washington Times", "url": "https://www.washingtontimes.com/rss/headlines/news/world/", "kind": "rss"},
    {"name": "The Hill", "url": "https://thehill.com/news/feed/", "kind": "rss"},
    {"name": "Politico", "url": "https://rss.politico.com/politics-news.xml", "kind": "rss"},
    {"name": "Common Dreams", "url": "https://www.commondreams.org/feeds/news.rss", "kind": "rss"},
    {"name": "WaPo World", "url": "https://feeds.washingtonpost.com/rss/world", "kind": "rss"},
    {"name": "WSJ World", "url": "https://feeds.a.dj.com/rss/RSSWorldNews.xml", "kind": "rss"},
    {"name": "CBC World", "url": "https://rss.cbc.ca/lineup/world.xml", "kind": "rss"},
    {"name": "CBC Politics", "url": "https://rss.cbc.ca/lineup/politics.xml", "kind": "rss"},
    {"name": "Global News World", "url": "https://globalnews.ca/world/feed/", "kind": "rss"},
    {"name": "Global News Politics", "url": "https://globalnews.ca/politics/feed/", "kind": "rss"},
    {"name": "National Post World", "url": "https://nationalpost.com/category/news/world/feed/", "kind": "rss"},
    {"name": "Mexico News Daily", "url": "https://mexiconewsdaily.com/feed/", "kind": "rss"},
    {"name": "La Jornada Mundo", "url": "https://www.jornada.com.mx/rss/mundo.xml", "kind": "rss"},
    {"name": "La Jornada Politica", "url": "https://www.jornada.com.mx/rss/politica.xml", "kind": "rss"},
    {"name": "Agencia Brasil Intl", "url": "https://agenciabrasil.ebc.com.br/rss/internacional/feed.xml", "kind": "rss"},
    {"name": "Agencia Brasil Pol", "url": "https://agenciabrasil.ebc.com.br/rss/politica/feed.xml", "kind": "rss"},
    {"name": "Rio Times", "url": "https://www.riotimesonline.com/feed/", "kind": "rss"},
    {"name": "Folha Mundo", "url": "https://feeds.folha.uol.com.br/mundo/rss091.xml", "kind": "rss"},
    {"name": "Folha Poder", "url": "https://feeds.folha.uol.com.br/poder/rss091.xml", "kind": "rss"},
    {"name": "G1 Mundo", "url": "https://g1.globo.com/rss/g1/mundo/", "kind": "rss"},
    {"name": "G1 Politica", "url": "https://g1.globo.com/rss/g1/politica/", "kind": "rss"},
    {"name": "ABC News AU", "url": "https://www.abc.net.au/news/feed/51120/rss.xml", "kind": "rss"},
    {"name": "Guardian Australia", "url": "https://www.theguardian.com/australia-news/rss", "kind": "rss"},
    {"name": "SMH", "url": "https://www.smh.com.au/rss/feed.xml", "kind": "rss"},
    {"name": "SBS News", "url": "https://www.sbs.com.au/news/topic/latest/feed", "kind": "rss"},
]

GNEWS_URL = ("https://gnews.io/api/v4/top-headlines"
             "?category=world&lang=en&max=25&apikey=%s")

_TAG_RE = re.compile(r"<[^>]{0,4000}>")
_SCRIPT_RE = re.compile(r"(?is)<(script|style)\b.*?</\1>")
_WS_RE = re.compile(r"\s+")
_ISO_HINT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]")

_DATE_TAGS = ("pubdate", "published", "updated", "date", "issued", "created")
_SUMMARY_TAGS = ("description", "summary", "encoded", "content", "subtitle")


def _local(tag):
    """'{ns}item' -> 'item'."""
    if not isinstance(tag, str):
        return ""
    return tag.rsplit("}", 1)[-1].lower()


def strip_html(text):
    """Feed descriptions arrive as escaped or raw HTML; flatten to one clean line."""
    if not text:
        return ""
    out = _SCRIPT_RE.sub(" ", text)
    out = _TAG_RE.sub(" ", out)
    out = html.unescape(out)
    out = _TAG_RE.sub(" ", out)  # entities can reveal a second layer of markup
    out = out.replace("\u00a0", " ")  # literal nbsp escaped: it must stay visible in source
    out = _WS_RE.sub(" ", out).strip()
    return out[:MAX_SUMMARY_CHARS]


def parse_date(text):
    """RFC-822 or ISO-8601 -> epoch seconds. None if unparseable or implausible."""
    if not text:
        return None
    text = text.strip()
    parsers = (_parse_iso, _parse_rfc822)
    if not _ISO_HINT_RE.match(text):
        parsers = (_parse_rfc822, _parse_iso)
    for fn in parsers:
        try:
            dt = fn(text)
        except Exception:
            dt = None
        if dt is None:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        ts = dt.timestamp()
        # Feeds with broken clocks would poison newest-first ordering.
        now = datetime.now(timezone.utc).timestamp()
        if 946684800 < ts < now + 172800:
            return ts
    return None


def _parse_rfc822(text):
    return email.utils.parsedate_to_datetime(text)


def _parse_iso(text):
    t = text.strip()
    if t.endswith("Z") or t.endswith("z"):
        t = t[:-1] + "+00:00"
    return datetime.fromisoformat(t)


def http_get(url, timeout=TIMEOUT, retries=1):
    """GET bytes. Transparently un-gzips; raises on final failure.

    One retry, because some publishers (timesofisrael.com) challenge the first request
    from a cold client and serve the very next one. Bounded at two requests per feed
    per cycle so we never hammer a rate limiter.
    """
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
        "Accept-Encoding": "gzip, identity",
    })
    attempt = 0
    while True:
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read(MAX_BODY + 1)
                encoding = (resp.headers.get("Content-Encoding") or "").lower()
            break
        except urllib.error.HTTPError as exc:
            if attempt >= retries or exc.code not in RETRY_STATUS:
                raise
        except Exception:
            if attempt >= retries:
                raise
        attempt += 1
        time.sleep(RETRY_DELAY_S)
    if len(raw) > MAX_BODY:
        raise ValueError("body over %d bytes" % MAX_BODY)
    if "gzip" in encoding or raw[:2] == b"\x1f\x8b":
        try:
            plain = gzip.GzipFile(fileobj=io.BytesIO(raw)).read(MAX_BODY + 1)
        except Exception:
            plain = None
        if plain is not None:
            if len(plain) > MAX_BODY:
                raise ValueError("body over %d bytes decompressed" % MAX_BODY)
            raw = plain
    return raw


def parse_feed(data):
    """Parse rss2 / atom / rdf bytes into [{url,title,summary,published}]. Raises on bad XML."""
    if not data:
        raise ValueError("empty body")
    if data[:3] == b"\xef\xbb\xbf":
        data = data[3:]
    data = data.lstrip()
    root = ET.fromstring(data)

    entries = []
    for el in root.iter():
        if _local(el.tag) in ("item", "entry"):
            entries.append(el)
            if len(entries) >= MAX_ITEMS_PER_FEED:
                break

    items = []
    for el in entries:
        try:
            item = _parse_entry(el)
        except Exception:
            continue
        if item:
            items.append(item)
    return items


def _parse_entry(el):
    title = ""
    link_alt = link_any = guid = ""
    summary = ""
    published = None

    for child in el:
        name = _local(child.tag)
        if name == "title" and not title:
            title = strip_html("".join(child.itertext()))
        elif name == "link":
            href = (child.get("href") or "").strip()
            if href:
                rel = (child.get("rel") or "alternate").lower()
                if rel == "alternate" and not link_alt:
                    link_alt = href
                elif not link_any:
                    link_any = href
            else:
                text = (child.text or "").strip()
                if text and not link_alt:
                    link_alt = text
        elif name in ("guid", "id") and not guid:
            guid = (child.text or "").strip()
        elif name in _SUMMARY_TAGS and not summary:
            summary = strip_html("".join(child.itertext()))
        elif name in _DATE_TAGS and published is None:
            published = parse_date("".join(child.itertext()))

    url = link_alt or link_any or (guid if guid.startswith("http") else "")
    url = url.strip()
    # Feeds are untrusted input and the URL ends up in an href: only http(s) may pass,
    # so a javascript:/data: link can never reach the board.
    if not url or not title or not _SAFE_URL.match(url):
        return None
    return {"url": url, "title": title, "summary": summary, "published": published}


def fetch_feed(con, feed):
    """Fetch+parse+insert one feed. Returns (new, items_seen, error_msg_or_None)."""
    try:
        data = http_get(feed["url"])
    except urllib.error.HTTPError as exc:
        return 0, 0, "HTTP %s" % exc.code, 0
    except Exception as exc:
        return 0, 0, "%s: %s" % (type(exc).__name__, str(exc)[:120]), 0
    return insert_parsed(con, feed["name"], data)



# --------------------------------------------------------------------------- #
# Ingest-time deduplication
#
# Measured 2026-08-21: 68% of the 200 newest items were near-duplicates and a
# single source (NWS) held 80% of the wire. Alert feeds re-publish the same
# warning under a fresh URL/GUID, so `url UNIQUE` never sees them, and each copy
# then costs a full Ornith inference and lands on the DEVELOPING panel as fake
# "coverage" ("Red Flag Warning Issued In Seattle Area", 26 identical members).
#
# The gate is a normalised title fingerprint per source: strip punctuation and
# EVERY number, which collapses the timestamps that are the only thing separating
# consecutive alerts from one office. "Red Flag Warning issued August 21 at
# 8:34PM PDT until August 22 at 9:00PM PDT by NWS Medford OR" and its 9:12PM
# reissue both reduce to the same signature, so the office's standing warning is
# represented once per window instead of 26 times.
#
# This is deliberately NOT the embedding clustering: clustering groups different
# outlets covering one event (wanted, on the DEVELOPING panel); this removes
# repeats of one source's own bulletin (not wanted anywhere).
# --------------------------------------------------------------------------- #

DEDUP_WINDOW_S = 12 * 3600      # how far back a fingerprint stays "already seen"
DEDUP_SCAN_LIMIT = 400          # recent rows examined per source; bounds the query
SOURCE_CYCLE_CAP = 25           # max NEW items one source may add in one fetch cycle

_FP_STOP = frozenset("""a an the of in on at to for and or with from by is are was were as
it its this that until issued by area county counties warning watch advisory statement
am pm edt cdt mdt pdt est cst mst pst utc""".split())


def title_fingerprint(title):
    """Normalised signature of a headline: lowercase, no punctuation, NO DIGITS.

    Dropping digits is the load-bearing part — it is what makes two reissues of
    the same weather bulletin collapse together while leaving ordinary news
    headlines (which rarely differ only by numbers) distinct.
    """
    t = re.sub(r"[^a-z\s]", " ", (title or "").lower())
    words = [w for w in t.split() if len(w) > 2 and w not in _FP_STOP]
    return " ".join(sorted(set(words)))


def _recent_fingerprints(con, source, now):
    """Fingerprints this source has already produced inside the dedup window."""
    try:
        rows = con.execute(
            "SELECT title FROM items WHERE source = ? AND fetched_at >= ? "
            "ORDER BY fetched_at DESC LIMIT ?",
            (source, now - DEDUP_WINDOW_S, DEDUP_SCAN_LIMIT)).fetchall()
    except Exception:
        return set()          # never let dedup break ingestion
    out = set()
    for r in rows:
        fp = title_fingerprint(r["title"] if hasattr(r, "keys") else r[0])
        if fp:
            out.add(fp)
    return out


def insert_parsed(con, source, data):
    try:
        items = parse_feed(data)
    except Exception as exc:
        return 0, 0, "parse: %s" % str(exc)[:120], 0
    now = time.time()
    seen = _recent_fingerprints(con, source, now)
    new = 0
    skipped = 0
    for item in items:
        if new >= SOURCE_CYCLE_CAP:
            skipped += 1          # one source may not own the wire in a single cycle
            continue
        fp = title_fingerprint(item["title"])
        if fp and fp in seen:
            skipped += 1
            continue
        try:
            if db.insert_item(con, item["url"], source, item["title"],
                              item["published"], item["summary"]):
                new += 1
                if fp:
                    seen.add(fp)   # also dedupes within this same batch
        except Exception:
            continue  # one bad row must not cost us the rest of the feed
    return new, len(items), None, skipped


def fetch_all(con):
    """Fetch every feed (bodies in parallel, DB writes serial) -> {new, checked, errors}."""
    bodies = {}

    def _grab(feed):
        try:
            return feed["name"], http_get(feed["url"]), None
        except urllib.error.HTTPError as exc:
            return feed["name"], None, "HTTP %s" % exc.code
        except Exception as exc:
            return feed["name"], None, "%s: %s" % (type(exc).__name__, str(exc)[:120])

    try:
        with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
            for name, data, err in pool.map(_grab, FEEDS):
                bodies[name] = (data, err)
    except Exception:  # thread pool unavailable (rare); fall back to serial fetching
        bodies = {}
        for feed in FEEDS:
            name, data, err = _grab(feed)
            bodies[name] = (data, err)

    total_new = 0
    total_skipped = 0
    errors = {}
    per_feed = {}
    for feed in FEEDS:
        name = feed["name"]
        data, err = bodies.get(name, (None, "not fetched"))
        if err:
            errors[name] = err
            per_feed[name] = {"items": 0, "new": 0, "error": err}
            continue
        new, seen, perr, skipped = insert_parsed(con, name, data)
        total_new += new
        total_skipped += skipped
        if perr:
            errors[name] = perr
        per_feed[name] = {"items": seen, "new": new, "error": perr,
                          "skipped": skipped}

    checked = len(FEEDS)
    if os.environ.get("GNEWS_API_KEY"):
        new, seen, gerr = fetch_gnews(con)
        checked += 1
        total_new += new
        if gerr:
            errors["GNews"] = gerr
        per_feed["GNews"] = {"items": seen, "new": new, "error": gerr}

    try:
        db.set_meta(con, "last_fetch_ts", time.time())
    except Exception:
        pass
    return {"new": total_new, "checked": checked, "errors": errors,
            "per_feed": per_feed, "skipped": total_skipped}


def fetch_gnews(con):
    """Optional GNews top-headlines pull. Returns (new, items_seen, error_or_None)."""
    key = os.environ.get("GNEWS_API_KEY")
    if not key:
        return 0, 0, "GNEWS_API_KEY unset"
    try:
        raw = http_get(GNEWS_URL % urllib.parse.quote(key))
        payload = json.loads(raw.decode("utf-8", "replace"))
        articles = payload.get("articles") or []
    except urllib.error.HTTPError as exc:
        return 0, 0, "HTTP %s" % exc.code
    except Exception as exc:
        # The key rides in the query string; never let it reach a log line.
        msg = str(exc).replace(key, "***")
        return 0, 0, "%s: %s" % (type(exc).__name__, msg[:120])
    new = 0
    for art in articles[:MAX_ITEMS_PER_FEED]:
        try:
            url = (art.get("url") or "").strip()
            title = strip_html(art.get("title") or "")
            if not url or not title:
                continue
            if db.insert_item(con, url, "GNews", title,
                              parse_date(art.get("publishedAt")),
                              strip_html(art.get("description") or "")):
                new += 1
        except Exception:
            continue
    return new, len(articles), None


if __name__ == "__main__":
    import sys

    con = db.connect(os.environ.get("DAYBREAK_DB", "daybreak.db"))
    started = time.time()
    result = fetch_all(con)
    elapsed = time.time() - started

    print("%-20s %-6s %6s %6s  %s" % ("FEED", "KIND", "ITEMS", "NEW", "ERROR"))
    print("-" * 78)
    kinds = {f["name"]: f["kind"] for f in FEEDS}
    alive = 0
    for name, stats in result["per_feed"].items():
        if stats["items"]:
            alive += 1
        print("%-20s %-6s %6d %6d  %s" % (name[:20], kinds.get(name, "json"),
                                          stats["items"], stats["new"], stats["error"] or ""))
    print("-" * 78)
    print("feeds_alive=%d/%d  new=%d  elapsed=%.1fs" %
          (alive, result["checked"], result["new"], elapsed))
    print("counts:", db.counts(con))
    if result["errors"]:
        print("errors:", result["errors"])
    ok = alive >= 5
    print("SELF-CHECK", "PASS" if ok else "FAIL", "(need >=5 feeds returning items)")
    sys.exit(0 if ok else 1)
