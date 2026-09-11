"""DAYBREAK storage layer. Stdlib only.

One SQLite file, WAL mode: the pipeline holds a writer connection and the server
holds reader connections to the same file at the same time. Writers wrap mutations
in `with con:` so a crash mid-batch never leaves a half-written row.

Prefer one connection per thread (connect() is cheap). Connections are created with
check_same_thread=False so a shared connection also works, but transactions on a
shared connection are global to that connection -- two threads writing through one
connection can commit each other's work.

RETENTION POLICY (rewritten 2026-08-21 -- the archive is the product)
--------------------------------------------------------------------
The AI analysis is the valuable output of this box, so it is the one thing that is
never thrown away:

  items + analysis   kept FOREVER by default. DAYBREAK_ITEM_RETENTION_DAYS sets a
                     window if an operator wants one; 0/unset = keep everything.
                     Measured cost: ~9.9 KB/article => ~4.3 GB/year, on a host with
                     4.4 TB free. Bounded by arithmetic, not by deletion.
  metrics (raw)      high-frequency telemetry, dropped after DAYBREAK_METRIC_
                     RETENTION_DAYS (default 8) -- but rollup_metrics() folds every
                     COMPLETED hour into metrics_hourly first, so the long-run trend
                     survives the raw rows. Hourly rows are ~192/day (~3 MB/year).
  images             bounded by disk cap, not by age: DAYBREAK_IMAGE_CAP_GB (default
                     20). Orphans go first, then the lowest-severity oldest images.
                     An S4/S5 item's image is never deleted.
  oplog              48h. It narrates; it is not the record.

Nothing here grows without a bound the operator can state in one sentence.
"""

import json
import os
import sqlite3
import time

SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")

# 0 = keep forever (the default). Overridden by DAYBREAK_ITEM_RETENTION_DAYS.
ITEM_RETENTION_S = 0.0
# A parked item is retried a couple of times before it is considered unenrichable:
# a device-side hiccup that slips past the liveness probe must not silently retire a row.
ENRICH_MAX_ATTEMPTS = 3
ENRICH_RETRY_S = 600.0
METRIC_RETENTION_S = 8 * 86400          # raw samples; the hourly rollup outlives them
METRICS_HOURLY_RETENTION_S = 0.0        # 0 = keep the downsampled series forever
OPLOG_RETENTION_S = 48 * 3600
# An empty cluster is only reaped once it is older than this, so the janitor can
# never delete a cluster another thread created microseconds ago but has not yet
# attached its first item to.
EMPTY_CLUSTER_GRACE_S = 3600.0
MAX_SERIES_POINTS = 120
# Cap rows pulled per metric key so a runaway poller can't make the stats endpoint slow.
_METRIC_SCAN_LIMIT = 5000

# Images: cap the directory. Anything at or above this severity is exempt from the
# normal eviction pass -- but NOT forever. Both image producers (jobs.job_image,
# which renders only severity >= 4, and server.py's on-demand endpoint, which has
# no severity filter at all) create exactly the protected class, so an
# exemption with no expiry made DAYBREAK_IMAGE_CAP_GB unreachable: once protected
# images alone exceeded the cap, every later prune freed 0 bytes and the directory
# grew until the disk filled. Protected renders older than this age are evictable
# as a last resort, oldest first, and only while still over cap.
IMAGE_CAP_GB_DEFAULT = 20.0
IMAGE_PROTECT_SEVERITY = 4
IMAGE_PROTECT_DAYS_DEFAULT = 30.0
_TEMP_IMAGE_AGE_S = 3600.0              # abandoned *.png.tmp from a crashed write
_ID_CHUNK = 800                         # ids per IN(...) lookup

# Cap state the janitor leaves behind so callers (jobs._disk_over_cap,
# server.py's on-demand render) can read it instead of re-running the sweep.
IMAGE_STATE_META = "img_cap_state"

_ROLLUP_META_KEY = "metrics_rollup_ts"
_CLOCK_SLACK_S = 26 * 3600      # watermark further ahead than this = clock went backwards


def _now():
    return time.time()


def _env_seconds(name, default_s):
    """Read a DAYBREAK_*_DAYS env var as seconds. Blank/garbage/<=0 -> default/forever.

    A malformed value must never take the janitor down, so it falls back silently
    to the compiled-in default.
    """
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return float(default_s)
    try:
        days = float(str(raw).strip())
    except (TypeError, ValueError):
        return float(default_s)
    return days * 86400.0 if days > 0 else 0.0


def item_retention_s():
    """Seconds of item history to keep. 0 => forever (the default)."""
    return _env_seconds("DAYBREAK_ITEM_RETENTION_DAYS", ITEM_RETENTION_S)


def metric_retention_s():
    """Seconds of RAW metric samples to keep (the hourly rollup keeps the trend)."""
    return _env_seconds("DAYBREAK_METRIC_RETENTION_DAYS", METRIC_RETENTION_S)


def metrics_hourly_retention_s():
    """Seconds of downsampled hourly metrics to keep. 0 => forever."""
    return _env_seconds("DAYBREAK_METRIC_HOURLY_RETENTION_DAYS",
                        METRICS_HOURLY_RETENTION_S)


def image_cap_bytes():
    """Byte cap for the generated-image directory. 0 => uncapped."""
    raw = os.environ.get("DAYBREAK_IMAGE_CAP_GB")
    gb = IMAGE_CAP_GB_DEFAULT
    if raw is not None and str(raw).strip():
        try:
            gb = float(str(raw).strip())
        except (TypeError, ValueError):
            gb = IMAGE_CAP_GB_DEFAULT
    return gb * 1e9 if gb > 0 else 0.0


def image_protect_age_s():
    """How long a high-severity render is exempt from eviction. 0 => forever
    (the old behaviour, which made the cap unenforceable -- opt in knowingly)."""
    return _env_seconds("DAYBREAK_IMAGE_PROTECT_DAYS",
                        IMAGE_PROTECT_DAYS_DEFAULT * 86400.0)


def image_cap_state(con, max_age_s=3600.0):
    """The janitor's last prune_images verdict: {"over_cap", "bytes", "ts"}.

    Callers that only need to know "is the image directory full?" read this rather
    than running a full scandir + severity lookup of their own. `over_cap` is None
    when there is no fresh reading, which callers should treat as "not known to be
    over" -- the hourly janitor will produce one."""
    out = {"over_cap": None, "bytes": 0, "ts": 0.0}
    raw = get_meta(con, IMAGE_STATE_META, "")
    if not raw:
        return out
    try:
        ts, over, nbytes = str(raw).split(",", 2)
        out["ts"] = float(ts)
        out["bytes"] = int(float(nbytes))
    except (TypeError, ValueError):
        return {"over_cap": None, "bytes": 0, "ts": 0.0}
    if max_age_s and (_now() - out["ts"]) > float(max_age_s):
        out["over_cap"] = None            # stale reading: do not act on it
    else:
        out["over_cap"] = (over == "1")
    return out


def _main_db_path(con):
    """Filesystem path behind this connection's `main` database, or None."""
    try:
        for row in con.execute("PRAGMA database_list").fetchall():
            if row[1] == "main":
                return row[2] or None
    except sqlite3.Error:
        pass
    return None


def default_images_dir(con):
    """`images/` next to the DB file -- the same path server.py/pipeline.py use."""
    path = _main_db_path(con)
    if not path:
        return None
    return os.path.join(os.path.dirname(os.path.abspath(path)), "images")


def connect(path="daybreak.db"):
    """Open (creating if needed) the daybreak DB with schema applied."""
    if not os.path.exists(SCHEMA_PATH):
        raise RuntimeError("schema.sql missing next to db.py: %s" % SCHEMA_PATH)
    con = sqlite3.connect(path, timeout=30.0, check_same_thread=False)
    con.row_factory = sqlite3.Row
    with open(SCHEMA_PATH, "r", encoding="utf-8") as fh:
        con.executescript(fh.read())
    # WAL is set by schema.sql; NORMAL sync is the right partner for it on flash storage.
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA busy_timeout=30000")
    _migrate(con)
    con.commit()
    return con


def _migrate(con):
    """Add columns/indexes that CREATE ... IF NOT EXISTS cannot add to an existing DB."""
    have = {r["name"] for r in con.execute("PRAGMA table_info(items)").fetchall()}
    for name, decl in (("enrich_attempts", "INTEGER DEFAULT 0"),
                       ("enrich_error_at", "REAL"),
                       # Precise incident location. The enricher already tags countries,
                       # but a country centroid puts a Kryvyi Rih strike in the middle of
                       # Ukraine; these carry the actual place.
                       ("place", "TEXT"),
                       ("lat", "REAL"),
                       ("lon", "REAL"),
                       ("geo_at", "REAL")):
        if name not in have:
            try:
                con.execute("ALTER TABLE items ADD COLUMN %s %s" % (name, decl))
            except sqlite3.OperationalError:
                pass  # another process won the race
    # idx_items_cluster was items(cluster_id); it is now items(cluster_id, severity)
    # so the per-cluster MAX(severity) rollup is index-only. IF NOT EXISTS won't
    # widen an index that already exists under that name, so swap it here.
    try:
        cols = con.execute("PRAGMA index_info(idx_items_cluster)").fetchall()
        if len(cols) == 1:
            con.execute("DROP INDEX idx_items_cluster")
            con.execute("CREATE INDEX idx_items_cluster ON items(cluster_id, severity)")
    except sqlite3.OperationalError:
        pass  # index missing or another process is rebuilding it


# --- index choices (items lives forever, so every hot query must be a seek) ----
#
# At 21-day retention the table topped out around 20k rows and a full scan was
# cheap enough to be invisible. Kept forever it passes 500k inside a year, and
# three queries run on a timer against it, so each one gets an index shaped like
# its ORDER BY, not just its WHERE:
#
# idx_items_recent  (COALESCE(published,fetched_at) DESC, id DESC)
#     WHERE enriched_at IS NOT NULL
#   recent_items sorts on the COALESCE expression, so a plain column index cannot
#   serve it -- SQLite would scan every row and sort. The expression index makes
#   the ORDER BY ... LIMIT a scan of the first N index entries. Partial on
#   `enriched_at IS NOT NULL` (that exact term is in the query, which is what lets
#   SQLite use a partial index) keeps unenriched rows out of it entirely.
# idx_items_region_recent / idx_items_category_recent
#   Same shape, keyed by the filter the AOR cards and category chips send. Without
#   them a filtered view of a quiet region walks the whole global ordering to find
#   100 matching rows.
# idx_items_pending  (fetched_at, id) WHERE enriched_at IS NULL
#   The one that matters most long-term: pending_items and counts()['pending'] run
#   every couple of seconds, and in a forever-archive ~100% of rows are enriched,
#   so an unpartitioned index would make every poll walk the entire history to find
#   nothing. This index only ever contains the live queue -- a handful of rows.
# idx_items_enriched_at  (enriched_at DESC) WHERE enriched_at IS NOT NULL
#   clustered_embeddings (enriched_at >= since) and counts()['enriched_24h'].
#   The partial clause is load-bearing in a second way: a full index on
#   enriched_at makes `enriched_at IS NULL` look like an equality seek, and the
#   planner picks it for pending_items and then sorts in a temp b-tree. Excluding
#   NULLs leaves idx_items_pending as the only candidate for the queue query.
# idx_items_cluster  (cluster_id, severity)
#   Widened from (cluster_id): _refresh_cluster's COUNT(*) and MAX(severity) per
#   cluster are now answered from the index without touching the table.
# idx_items_embedded  (id) WHERE embedding IS NOT NULL
#   archive_stats counts embedded rows; without it that count scans 500k rows that
#   each carry a 4 KB blob.
# idx_clusters_updated  (updated_at DESC)
#   top_clusters orders by updated_at; clusters accumulate alongside items now.
#
# Cost of all this: ~6 extra index writes per inserted item (one row per 300s
# fetch cycle) and a one-time build when an existing DB is first opened.
# EXPLAIN QUERY PLAN assertions in the self-check below pin the behaviour.


# --- items ---------------------------------------------------------------


def insert_item(con, url, source, title, published, raw_summary):
    """Insert a feed item. Returns new row id, or None if the URL is already known."""
    url = (url or "").strip()
    title = (title or "").strip()
    # The URL lands in an href on the board: only http(s) is ever stored, so a
    # javascript:/data: link from an untrusted feed cannot reach the frontend.
    if not url or not title or not url[:8].lower().startswith(("http://", "https://")):
        return None
    fetched_at = _now()
    if published is None:
        published = fetched_at
    with con:
        cur = con.execute(
            "INSERT OR IGNORE INTO items(url, source, title, published, fetched_at, raw_summary)"
            " VALUES(?,?,?,?,?,?)",
            (url, source, title, float(published), fetched_at, raw_summary),
        )
    return cur.lastrowid if cur.rowcount else None


_PENDING_WHERE = (
    "enriched_at IS NULL AND (enrich_error IS NULL OR"
    " (COALESCE(enrich_attempts,0) < ? AND COALESCE(enrich_error_at,0) <= ?))")


def pending_items(con, limit=20):
    """Items waiting on enrichment, oldest first.

    Never-tried rows plus rows parked fewer than ENRICH_MAX_ATTEMPTS times whose
    cooldown has expired -- so a transient device fault does not retire a row forever.
    """
    return con.execute(
        "SELECT * FROM items WHERE " + _PENDING_WHERE +
        " ORDER BY fetched_at ASC, id ASC LIMIT ?",
        (ENRICH_MAX_ATTEMPTS, _now() - ENRICH_RETRY_S, int(limit)),
    ).fetchall()


def mark_enriched(con, item_id, summary, category, region, severity, countries_json,
                  embedding_bytes):
    with con:
        con.execute(
            "UPDATE items SET summary=?, category=?, region=?, severity=?, countries=?,"
            " embedding=?, enriched_at=?, enrich_error=NULL, enrich_attempts=0,"
            " enrich_error_at=NULL WHERE id=?",
            (summary, category, region, severity, countries_json,
             sqlite3.Binary(embedding_bytes) if embedding_bytes else None,
             _now(), int(item_id)),
        )


def mark_error(con, item_id, err):
    """Park an item that could not be enriched.

    Counts the attempt and stamps the time: pending_items picks the row back up
    after ENRICH_RETRY_S until ENRICH_MAX_ATTEMPTS is reached, after which it
    stays parked for good.
    """
    with con:
        con.execute(
            "UPDATE items SET enrich_error=?, enrich_attempts=COALESCE(enrich_attempts,0)+1,"
            " enrich_error_at=? WHERE id=?",
            (str(err)[:500] or "unknown", _now(), int(item_id)),
        )


def recent_items(con, region=None, category=None, since=None, limit=100):
    """Enriched items, newest first (by publish time, falling back to fetch time)."""
    sql = ["SELECT id, url, source, title, published, fetched_at, raw_summary, summary,"
           " category, region, severity, countries, cluster_id, enriched_at"
           " FROM items WHERE enriched_at IS NOT NULL"]
    args = []
    if region:
        sql.append("AND region=?")
        args.append(region)
    if category:
        sql.append("AND category=?")
        args.append(category)
    if since is not None:
        sql.append("AND COALESCE(published, fetched_at) >= ?")
        args.append(float(since))
    sql.append("ORDER BY COALESCE(published, fetched_at) DESC, id DESC LIMIT ?")
    args.append(max(1, min(int(limit), 500)))
    return con.execute(" ".join(sql), args).fetchall()


# --- clusters ------------------------------------------------------------


def set_cluster(con, item_id, cluster_id):
    """Attach an item to a cluster and refresh that cluster's rollup."""
    with con:
        con.execute("UPDATE items SET cluster_id=? WHERE id=?", (cluster_id, int(item_id)))
        if cluster_id is not None:
            _refresh_cluster(con, cluster_id, _now())


def _refresh_cluster(con, cluster_id, ts):
    """Recompute item_count/top_severity from members. Caller holds the transaction."""
    con.execute(
        "UPDATE clusters SET"
        " item_count=(SELECT COUNT(*) FROM items WHERE items.cluster_id=clusters.id),"
        " top_severity=MAX(COALESCE(top_severity,1),"
        "   COALESCE((SELECT MAX(severity) FROM items WHERE items.cluster_id=clusters.id),1)),"
        " updated_at=? WHERE id=?",
        (ts, cluster_id),
    )


def clustered_embeddings(con, since_ts):
    """(item_id, cluster_id, embedding) for recently enriched clustered items, newest first."""
    rows = con.execute(
        "SELECT id, cluster_id, embedding FROM items"
        " WHERE cluster_id IS NOT NULL AND embedding IS NOT NULL AND enriched_at >= ?"
        " ORDER BY enriched_at DESC",
        (float(since_ts),),
    ).fetchall()
    return [(r["id"], r["cluster_id"], bytes(r["embedding"])) for r in rows]


def upsert_cluster(con, cluster_id, label, severity, ts):
    """Create or update a cluster; returns its id. label=None leaves an existing label alone."""
    sev = int(severity) if severity else 1
    ts = float(ts) if ts else _now()
    with con:
        if cluster_id is None:
            cur = con.execute(
                "INSERT INTO clusters(label, created_at, updated_at, top_severity, item_count)"
                " VALUES(?,?,?,?,0)", (label, ts, ts, sev))
            cluster_id = cur.lastrowid
        else:
            if label:
                con.execute("UPDATE clusters SET label=? WHERE id=?", (label, cluster_id))
            con.execute(
                "UPDATE clusters SET top_severity=MAX(COALESCE(top_severity,1),?) WHERE id=?",
                (sev, cluster_id))
        _refresh_cluster(con, cluster_id, ts)
    return cluster_id


def top_clusters(con, limit=12):
    return con.execute(
        "SELECT id, label, created_at, updated_at, top_severity, item_count FROM clusters"
        " WHERE item_count >= 2 ORDER BY updated_at DESC LIMIT ?",
        (max(1, int(limit)),),
    ).fetchall()


# --- metrics & meta ------------------------------------------------------


def record_metric(con, key, value, ts=None):
    with con:
        con.execute("INSERT INTO metrics(ts, key, value) VALUES(?,?,?)",
                    (float(ts if ts is not None else _now()), key, float(value)))


def latest_metrics(con, keys, window_s=3600):
    """{key: {"latest": float|None, "series": [[ts, value], ...]}} downsampled to <=120 points."""
    cutoff = _now() - float(window_s)
    out = {}
    for key in keys:
        rows = con.execute(
            "SELECT ts, value FROM metrics WHERE key=? AND ts>=? ORDER BY ts DESC LIMIT ?",
            (key, cutoff, _METRIC_SCAN_LIMIT),
        ).fetchall()
        if not rows:
            out[key] = {"latest": None, "series": []}
            continue
        latest = float(rows[0]["value"])
        points = [(float(r["ts"]), float(r["value"])) for r in reversed(rows)]
        out[key] = {"latest": latest, "series": _downsample(points, MAX_SERIES_POINTS)}
    return out


def _downsample(points, max_points):
    """Time-bucket average. Input ascending by ts."""
    if len(points) <= max_points:
        return [[ts, v] for ts, v in points]
    t0, t1 = points[0][0], points[-1][0]
    span = t1 - t0
    if span <= 0:
        step = max(1, len(points) // max_points)
        return [[ts, v] for ts, v in points[::step]][:max_points]
    width = span / max_points
    buckets = {}
    for ts, v in points:
        idx = min(max_points - 1, int((ts - t0) / width))
        acc = buckets.get(idx)
        if acc is None:
            buckets[idx] = [ts, v, 1]
        else:
            acc[1] += v
            acc[2] += 1
            acc[0] = ts
    return [[buckets[i][0], buckets[i][1] / buckets[i][2]] for i in sorted(buckets)]


def rollup_metrics(con, until_ts=None):
    """Fold completed hours of raw `metrics` into `metrics_hourly`. Idempotent.

    Raw telemetry is dropped after ~8 days; this is what keeps "NPU utilisation
    over the week" answerable afterwards. Runs on every prune(), NOT only on the
    rows about to be deleted -- otherwise the long series would lag the retention
    window and the first 8 days of history would show nothing.

    A watermark in meta.metrics_rollup_ts records the hour boundary already folded,
    so a bucket is never counted twice. The in-progress hour is always left alone;
    the merge is weighted (avg*n) so a bucket that keeps receiving samples stays
    exact. Samples written with a ts BEFORE the watermark (backdated) are skipped
    rather than double-counted.
    """
    end = float(until_ts) if until_ts is not None else _now()
    hour_end = int(end // 3600) * 3600
    try:
        start = float(get_meta(con, _ROLLUP_META_KEY, 0) or 0)
    except (TypeError, ValueError):
        start = 0.0
    if start > hour_end + _CLOCK_SLACK_S:
        # The clock moved backwards (a Pi with no RTC boots at the epoch, then NTP
        # corrects it). A watermark stranded in the future would block the rollup
        # forever, so resume from now instead of re-folding buckets we already have.
        #
        # "Now" is NOT enough on its own: if the bogus clock reads 1970 then
        # hour_end is 1970 too, and the watermark would be moved BACKWARDS past
        # every bucket already folded. When NTP corrects the clock the next pass
        # re-folds up to METRIC_RETENTION_S of raw samples into rows that already
        # contain them -- n doubles, and n is the weight _merge_buckets uses, so
        # the corruption propagates into every long-horizon series. Derive the
        # resume point from the DATA and never let the watermark regress.
        resume = float(hour_end)
        try:
            folded = con.execute("SELECT MAX(ts_hour) FROM metrics_hourly").fetchone()[0]
        except sqlite3.Error:
            folded = None
        if folded is not None:
            resume = max(resume, float(folded) + 3600.0)
        set_meta(con, _ROLLUP_META_KEY, "%.3f" % resume)
        return {"buckets": 0, "through": resume, "clock_reset": True}
    if hour_end <= start:
        return {"buckets": 0, "through": start}
    with con:
        # The SELECT must carry a WHERE clause or SQLite cannot tell this ON from
        # a join's ON (documented UPSERT parsing ambiguity).
        cur = con.execute(
            'INSERT INTO metrics_hourly(ts_hour, key, "avg", "min", "max", n)'
            ' SELECT CAST(ts/3600 AS INTEGER)*3600, key, SUM(value)/COUNT(*),'
            '        MIN(value), MAX(value), COUNT(*)'
            '   FROM metrics WHERE ts >= ? AND ts < ?'
            '  GROUP BY CAST(ts/3600 AS INTEGER)*3600, key'
            ' ON CONFLICT(ts_hour, key) DO UPDATE SET'
            '   "avg"=("avg"*n + excluded."avg"*excluded.n)/(n + excluded.n),'
            '   "min"=MIN("min", excluded."min"),'
            '   "max"=MAX("max", excluded."max"),'
            '   n=n + excluded.n',
            (start, float(hour_end)),
        )
        buckets = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        con.execute(
            "INSERT INTO meta(key, value) VALUES(?,?)"
            " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (_ROLLUP_META_KEY, "%.3f" % hour_end),
        )
    return {"buckets": buckets, "through": float(hour_end)}


def hourly_metrics(con, keys, window_s=7 * 86400, max_points=400):
    """Long-horizon series from the hourly rollup (survives raw-metric pruning).

    {key: {"latest": float|None, "series": [[ts_hour, avg, min, max, n], ...]}}
    Buckets are merged (weighted avg, true min/max) if the window holds more than
    max_points hours, so a year of history still answers in one small response.
    """
    cutoff = _now() - float(window_s)
    out = {}
    for key in keys:
        rows = con.execute(
            'SELECT ts_hour, "avg", "min", "max", n FROM metrics_hourly'
            ' WHERE key=? AND ts_hour>=? ORDER BY ts_hour ASC',
            (key, cutoff),
        ).fetchall()
        if not rows:
            out[key] = {"latest": None, "series": []}
            continue
        points = [[float(r[0]), float(r[1]), float(r[2]), float(r[3]), int(r[4])]
                  for r in rows]
        out[key] = {"latest": points[-1][1],
                    "series": _merge_buckets(points, max(1, int(max_points)))}
    return out


def _merge_buckets(points, max_points):
    """Merge [ts, avg, min, max, n] rows (ascending) down to <= max_points."""
    if len(points) <= max_points:
        return points
    group = (len(points) + max_points - 1) // max_points
    out = []
    for i in range(0, len(points), group):
        chunk = points[i:i + group]
        n = sum(c[4] for c in chunk) or 1
        avg = sum(c[1] * c[4] for c in chunk) / n
        out.append([chunk[0][0], avg, min(c[2] for c in chunk),
                    max(c[3] for c in chunk), sum(c[4] for c in chunk)])
    return out


def get_meta(con, key, default=None):
    row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row is not None else default


def set_meta(con, key, value):
    with con:
        con.execute(
            "INSERT INTO meta(key, value) VALUES(?,?)"
            " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )


# --- housekeeping --------------------------------------------------------


def add_event(con, kind, msg):
    """One line in the AI ops log. kind: FETCH|ENRICH|IMAGE|CLUSTER|ERROR|SYS."""
    try:
        with con:
            con.execute("INSERT INTO oplog(ts, kind, msg) VALUES(?,?,?)",
                        (_now(), str(kind)[:12], str(msg)[:300]))
    except sqlite3.Error:
        pass  # the log must never break the operation it narrates


def recent_events(con, limit=60):
    try:
        return con.execute(
            "SELECT ts, kind, msg FROM oplog ORDER BY ts DESC LIMIT ?",
            (max(1, min(200, int(limit))),)).fetchall()
    except sqlite3.Error:
        return []


# --- docs (long-form AI output from jobs.py) ------------------------------
#
# Kept forever alongside items: a dossier is ~3 KB and it is the thing the
# archive exists to produce. prune() deliberately has no DELETE for this table.


DOC_KINDS = ("dossier", "synthesis", "brief")


def put_doc(con, kind, subject, title, body, item_count=0, meta=None):
    """Store one long-form AI document. Returns the new row id, or None.

    Revisions are additive: writing the same (kind, subject) again keeps the older
    copy, so a re-run of a day's brief never destroys the one that shipped.
    """
    kind = str(kind or "").strip().lower()
    subject = str(subject or "").strip()
    body = (body or "").strip()
    if not kind or not subject or not body:
        return None
    if meta is not None and not isinstance(meta, str):
        try:
            meta = json.dumps(meta, default=str)
        except (TypeError, ValueError):
            meta = None
    try:
        with con:
            cur = con.execute(
                "INSERT INTO docs(kind, subject, title, body, created_at, item_count, meta)"
                " VALUES(?,?,?,?,?,?,?)",
                (kind, subject[:120], (title or "")[:200], body, _now(),
                 int(item_count or 0), meta))
        return cur.lastrowid
    except sqlite3.Error:
        return None


def latest_doc(con, kind, subject=None):
    """Newest doc of a kind (optionally for one subject), or None."""
    try:
        if subject:
            return con.execute(
                "SELECT * FROM docs WHERE kind=? AND subject=?"
                " ORDER BY created_at DESC LIMIT 1", (kind, subject)).fetchone()
        return con.execute("SELECT * FROM docs WHERE kind=?"
                           " ORDER BY created_at DESC LIMIT 1", (kind,)).fetchone()
    except sqlite3.Error:
        return None


def briefs_by_day(con, limit=20):
    """Briefs ordered by the DAY they cover, newest first.

    Deliberately not latest_doc(): that orders by created_at, and a backfill writes
    newest-day-first, so creation order is the reverse of day order. Anything asking
    "which is the most recent brief" means the day, not the write.
    """
    try:
        return con.execute(
            "SELECT * FROM docs WHERE kind='brief' ORDER BY subject DESC LIMIT ?",
            (max(1, min(200, int(limit or 20))),)).fetchall()
    except sqlite3.Error:
        return []


def add_tokens(con, n):
    """Add to the cumulative output-token odometer, atomically.

    A read-modify-write in Python would race: the pipeline and the web server are
    separate processes and both generate tokens now (Ask the Archive and deep dives run
    in the server). Doing the arithmetic in SQL makes the increment safe.
    """
    n = int(n or 0)
    if n <= 0:
        return
    try:
        con.execute(
            "INSERT INTO meta(key, value) VALUES('tokens_total', ?) "
            "ON CONFLICT(key) DO UPDATE SET "
            "value = CAST(COALESCE(meta.value,'0') AS INTEGER) + ?",
            (str(n), n))
        con.commit()
    except sqlite3.Error:
        pass


def add_device_ms(con, ms):
    """Cumulative NPU busy time. Atomic, same reasoning as add_tokens."""
    try:
        ms = int(float(ms or 0))
    except (TypeError, ValueError):
        return
    if ms <= 0:
        return
    try:
        con.execute(
            "INSERT INTO meta(key, value) VALUES('device_ms_total', ?) "
            "ON CONFLICT(key) DO UPDATE SET "
            "value = CAST(COALESCE(meta.value,'0') AS INTEGER) + ?", (str(ms), ms))
        con.commit()
    except sqlite3.Error:
        pass


def recent_docs(con, kind=None, limit=20, with_body=False):
    """Newest docs, optionally filtered by kind. Bodies omitted unless asked."""
    cols = "id, kind, subject, title, created_at, item_count" + (", body" if with_body else "")
    limit = max(1, min(200, int(limit or 20)))
    try:
        if kind:
            return con.execute("SELECT %s FROM docs WHERE kind=?"
                               " ORDER BY created_at DESC LIMIT ?" % cols,
                               (kind, limit)).fetchall()
        return con.execute("SELECT %s FROM docs ORDER BY created_at DESC LIMIT ?" % cols,
                           (limit,)).fetchall()
    except sqlite3.Error:
        return []


def doc_counts(con):
    """{dossiers, syntheses, briefs, docs_total, latest_doc_ts} for /api/stats."""
    out = {"dossiers": 0, "syntheses": 0, "briefs": 0, "docs_total": 0,
           "latest_doc_ts": None}
    plural = {"dossier": "dossiers", "synthesis": "syntheses", "brief": "briefs"}
    try:
        for row in con.execute("SELECT kind, COUNT(*) n FROM docs GROUP BY kind").fetchall():
            n = int(row["n"] or 0)
            out["docs_total"] += n
            key = plural.get((row["kind"] or "").lower())
            if key:
                out[key] = n
        ts = con.execute("SELECT MAX(created_at) FROM docs").fetchone()[0]
        out["latest_doc_ts"] = float(ts) if ts is not None else None
    except (sqlite3.Error, TypeError, ValueError):
        pass
    return out


def prune(con, images_dir=None):
    """Housekeeping. Items are KEPT FOREVER unless an operator sets a window.

    Order matters: roll raw metrics into the hourly series BEFORE deleting them,
    or the long-run trend dies with the samples.

    Returns {"items", "metrics", "clusters", "oplog", "metrics_rolled",
             "metrics_hourly", "images", "image_bytes"} -- the first three keys
    unchanged for existing callers (pipeline.janitor).
    """
    now = _now()
    keep_items = item_retention_s()
    rolled = rollup_metrics(con)          # its own transaction; must precede the delete
    with con:
        items = 0
        if keep_items > 0:
            items = con.execute("DELETE FROM items WHERE fetched_at < ?",
                                (now - keep_items,)).rowcount
        metrics = con.execute("DELETE FROM metrics WHERE ts < ?",
                              (now - metric_retention_s(),)).rowcount
        hourly_gone = 0
        keep_hourly = metrics_hourly_retention_s()
        if keep_hourly > 0:
            hourly_gone = con.execute("DELETE FROM metrics_hourly WHERE ts_hour < ?",
                                      (now - keep_hourly,)).rowcount
        oplog = con.execute("DELETE FROM oplog WHERE ts < ?",
                            (now - OPLOG_RETENTION_S,)).rowcount
        # Grace period: a cluster created seconds ago by another thread has no
        # members yet and must not be reaped out from under it.
        clusters = con.execute(
            "DELETE FROM clusters WHERE COALESCE(created_at, 0) < ? AND id NOT IN"
            " (SELECT DISTINCT cluster_id FROM items WHERE cluster_id IS NOT NULL)",
            (now - EMPTY_CLUSTER_GRACE_S,)).rowcount
        if items:
            # Only needed when members actually disappeared; with items kept
            # forever this full-table rollup never has to run.
            con.execute(
                "UPDATE clusters SET"
                " item_count=(SELECT COUNT(*) FROM items WHERE items.cluster_id=clusters.id),"
                " top_severity=COALESCE("
                "   (SELECT MAX(severity) FROM items WHERE items.cluster_id=clusters.id),"
                "   COALESCE(top_severity,1))")
    out = {"items": items, "metrics": metrics, "clusters": clusters, "oplog": oplog,
           "metrics_rolled": rolled.get("buckets", 0), "metrics_hourly": hourly_gone,
           "images": 0, "image_bytes": 0}
    try:
        img = prune_images(con, images_dir)
        out["images"] = img.get("deleted", 0)
        out["image_bytes"] = img.get("freed_bytes", 0)
    except Exception as exc:  # janitor must survive any filesystem weirdness
        out["images_error"] = "%s: %s" % (type(exc).__name__, exc)
    return out


def prune_images(con, images_dir=None, max_gb=None):
    """Keep the generated-image directory under a byte cap. Returns counts.

    Policy, in order:
      1. abandoned `*.png.tmp` older than an hour  -> always deleted
      2. `<id>.png` whose item row is gone         -> always deleted (orphan)
      3. still over cap: delete LOWEST severity first, oldest first inside a
         severity, until the directory fits.
      4. severity >= IMAGE_PROTECT_SEVERITY (S4/S5) is exempt from step 3. It is
         NOT exempt forever: both producers write only that class (job_image
         renders S4+ only, server.py's on-demand endpoint has no severity filter),
         so a permanent exemption meant the cap could be passed and never
         recovered -- every later prune freed 0 bytes and reported over_cap
         forever while the disk filled. Still over cap after step 3, protected
         renders older than image_protect_age_s() (DAYBREAK_IMAGE_PROTECT_DAYS,
         default 30) are evicted oldest-first until it fits.
      5. over cap on protected images that are ALL younger than that -> the
         directory stays over cap and says so (`over_cap: True`), which is a
         signal to raise the cap.

    The verdict is also written to meta.img_cap_state so callers can ask "is the
    directory full?" (db.image_cap_state) without repeating the whole sweep.

    images_dir=None -> `images/` beside the DB. max_gb=None -> DAYBREAK_IMAGE_CAP_GB
    (default 20). Files that are not `<int>.png` are left alone. Never raises for a
    missing directory or an unreadable file.
    """
    cap = float(max_gb) * 1e9 if max_gb is not None else image_cap_bytes()
    if images_dir is None:
        images_dir = default_images_dir(con)
    out = {"dir": images_dir, "cap_bytes": int(cap), "scanned": 0, "temp": 0,
           "orphans": 0, "protected": 0, "deleted": 0, "kept": 0,
           "bytes_before": 0, "bytes_after": 0, "freed_bytes": 0, "over_cap": False,
           "protected_evicted": 0}
    if not images_dir or not os.path.isdir(images_dir):
        return out

    now = _now()
    entries = []          # (item_id, path, size, mtime)
    freed = 0
    try:
        with os.scandir(images_dir) as it:
            for ent in it:
                try:
                    if not ent.is_file():
                        continue
                    name = ent.name
                    if name.endswith(".tmp"):
                        st = ent.stat()
                        if now - st.st_mtime > _TEMP_IMAGE_AGE_S:
                            os.remove(ent.path)
                            out["temp"] += 1
                            freed += st.st_size
                        continue
                    if not name.endswith(".png"):
                        continue
                    stem = name[:-4]
                    if not stem.isdigit():
                        continue      # not one of ours; leave it alone
                    st = ent.stat()
                    entries.append((int(stem), ent.path, st.st_size, st.st_mtime))
                except OSError:
                    continue
    except OSError:
        out["freed_bytes"] = freed
        return out

    out["scanned"] = len(entries)
    total = sum(e[2] for e in entries)
    out["bytes_before"] = total

    # severity for every image we hold, chunked so the IN(...) stays sane
    sev = {}
    ids = [e[0] for e in entries]
    try:
        for i in range(0, len(ids), _ID_CHUNK):
            chunk = ids[i:i + _ID_CHUNK]
            rows = con.execute(
                "SELECT id, severity FROM items WHERE id IN (%s)"
                % ",".join("?" * len(chunk)), chunk).fetchall()
            for r in rows:
                sev[int(r[0])] = r[1]
    except sqlite3.Error:
        # can't classify -> do nothing destructive beyond the tmp sweep
        out["freed_bytes"] = freed
        out["bytes_after"] = total
        return out

    def _drop(entry):
        nonlocal total, freed
        try:
            os.remove(entry[1])
        except OSError:
            return False
        total -= entry[2]
        freed += entry[2]
        out["deleted"] += 1
        return True

    survivors = []
    for e in entries:
        if e[0] not in sev:                      # item pruned/never existed
            out["orphans"] += 1
            _drop(e)
        else:
            survivors.append(e)

    if cap > 0 and total > cap:
        def rank(e):
            s = sev.get(e[0])
            return (int(s) if s is not None else 0, e[3], e[0])
        protected = [e for e in survivors
                     if (sev.get(e[0]) or 0) >= IMAGE_PROTECT_SEVERITY]
        candidates = [e for e in survivors
                      if (sev.get(e[0]) or 0) < IMAGE_PROTECT_SEVERITY]
        out["protected"] = len(protected)
        for e in sorted(candidates, key=rank):
            if total <= cap:
                break
            _drop(e)
        # Last resort: the cap is only a cap if it can be reached. Everything the
        # renderers produce is protected, so without this the directory would sit
        # over cap permanently and freeing 0 bytes on every pass.
        if total > cap:
            keep_age = image_protect_age_s()
            if keep_age > 0:
                stale = [e for e in protected if (now - e[3]) > keep_age]
                for e in sorted(stale, key=lambda x: (x[3], x[0])):  # oldest first
                    if total <= cap:
                        break
                    if _drop(e):
                        out["protected_evicted"] += 1
    else:
        out["protected"] = sum(1 for e in survivors
                               if (sev.get(e[0]) or 0) >= IMAGE_PROTECT_SEVERITY)

    out["bytes_after"] = total
    out["freed_bytes"] = freed
    out["kept"] = out["scanned"] - out["deleted"]
    out["over_cap"] = bool(cap > 0 and total > cap)
    try:
        set_meta(con, IMAGE_STATE_META,
                 "%.3f,%s,%d" % (now, "1" if out["over_cap"] else "0", int(total)))
    except sqlite3.Error:
        pass
    return out


_EMBEDDED_COUNT_SQL = ("SELECT COUNT(*) FROM items INDEXED BY idx_items_embedded"
                       " WHERE embedding IS NOT NULL")


def _count_embedded(con):
    """Rows carrying an embedding, as an index-only count. Falls back to the plain
    count on a DB old enough to be missing the index (it is created by schema.sql,
    so this only ever fires mid-migration)."""
    try:
        return con.execute(_EMBEDDED_COUNT_SQL).fetchone()[0]
    except sqlite3.Error:
        return con.execute(
            "SELECT COUNT(*) FROM items WHERE embedding IS NOT NULL").fetchone()[0]


def archive_stats(con):
    """How big the permanent archive is and what it costs to keep.

    {items_total, oldest_item_ts, span_days, db_bytes, embeddings_present,
     items_enriched, retention_days (0=forever), bytes_per_item, projected_gb_year}
    db_bytes counts the main file plus the WAL. projected_gb_year extrapolates the
    last 7 days of ingest at the measured bytes/item.
    """
    now = _now()
    out = {"items_total": 0, "oldest_item_ts": None, "span_days": 0.0,
           "db_bytes": 0, "embeddings_present": 0, "items_enriched": 0,
           "retention_days": round(item_retention_s() / 86400.0, 2),
           "bytes_per_item": 0, "projected_gb_year": 0.0}
    try:
        out["items_total"] = int(con.execute(
            "SELECT COUNT(*) FROM items").fetchone()[0] or 0)
        # MIN over the expression index (partial: enriched rows) is a seek; the same
        # MIN over ALL items has no index and costs ~57ms at 500k rows. Unenriched
        # rows are always the newest arrivals, so the enriched minimum IS the reach
        # of the archive -- fall back to fetched_at only when nothing is enriched.
        oldest = con.execute(
            "SELECT MIN(COALESCE(published, fetched_at)) FROM items"
            " WHERE enriched_at IS NOT NULL").fetchone()[0]
        if oldest is None:
            oldest = con.execute("SELECT MIN(fetched_at) FROM items").fetchone()[0]
        out["oldest_item_ts"] = float(oldest) if oldest is not None else None
        # Partial index idx_items_embedded => no 500k-row blob scan. INDEXED BY is
        # load-bearing, not decoration: the planner only prefers the partial index
        # while sqlite_stat1 is absent. One ANALYZE (a maintenance command nothing
        # here runs today, but any operator might) flips the plan to SCAN items and
        # this count starts reading the entire table of 4 KB blobs -- on a 10s
        # poll, forever. Pin the plan instead of hoping.
        out["embeddings_present"] = int(_count_embedded(con) or 0)
        out["items_enriched"] = int(con.execute(
            "SELECT COUNT(*) FROM items WHERE enriched_at IS NOT NULL").fetchone()[0] or 0)
        week = int(con.execute("SELECT COUNT(*) FROM items WHERE fetched_at > ?",
                               (now - 7 * 86400,)).fetchone()[0] or 0)
    except sqlite3.Error:
        return out
    if out["oldest_item_ts"]:
        out["span_days"] = round(max(0.0, now - out["oldest_item_ts"]) / 86400.0, 2)

    size = 0
    try:
        page = con.execute("PRAGMA page_count").fetchone()[0]
        psize = con.execute("PRAGMA page_size").fetchone()[0]
        size = int(page) * int(psize)
    except (sqlite3.Error, TypeError, IndexError):
        size = 0
    path = _main_db_path(con)
    if path:                       # page_count covers the main file; add the WAL
        try:
            size += os.path.getsize(path + "-wal")
        except OSError:
            pass
    out["db_bytes"] = size
    if out["items_total"]:
        out["bytes_per_item"] = int(size / out["items_total"])
        out["projected_gb_year"] = round(
            (week / 7.0) * 365.0 * out["bytes_per_item"] / 1e9, 2)
    return out


def counts(con):
    now = _now()
    day, hour = now - 86400, now - 3600
    q = lambda sql, args: con.execute(sql, args).fetchone()[0]  # noqa: E731
    return {
        "items_total": q("SELECT COUNT(*) FROM items", ()),
        "items_24h": q("SELECT COUNT(*) FROM items WHERE fetched_at > ?", (day,)),
        "enriched_24h": q("SELECT COUNT(*) FROM items WHERE enriched_at > ?", (day,)),
        "pending": q("SELECT COUNT(*) FROM items WHERE " + _PENDING_WHERE,
                     (ENRICH_MAX_ATTEMPTS, now - ENRICH_RETRY_S)),
        "errors_24h": q("SELECT COUNT(*) FROM items WHERE enrich_error IS NOT NULL"
                        " AND fetched_at > ?", (day,)),
        "sources_alive_1h": q("SELECT COUNT(DISTINCT source) FROM items WHERE fetched_at > ?",
                              (hour,)),
        # A one-hour window makes the board look thin: most feeds do not publish every
        # hour, so 9 of 28 sources having filed in the last hour is normal and reads as
        # "we only have 9 sources". The day window is the honest figure for the status
        # bar, alongside the total actually configured.
        "sources_alive_24h": q("SELECT COUNT(DISTINCT source) FROM items WHERE fetched_at > ?",
                               (day,)),
        "sources_total": q("SELECT COUNT(DISTINCT source) FROM items", ()),
    }


if __name__ == "__main__":
    import tempfile

    path = os.path.join(tempfile.mkdtemp(prefix="daybreak-"), "smoke.db")
    con = connect(path)
    now = time.time()

    iid = insert_item(con, "https://example.test/a", "SMOKE", "Test headline", now - 60, "desc")
    dup = insert_item(con, "https://example.test/a", "SMOKE", "Test headline", now - 60, "desc")
    assert iid and dup is None, (iid, dup)
    assert len(pending_items(con)) == 1

    mark_enriched(con, iid, "One line summary.", "conflict", "EUCOM", 4,
                  json.dumps(["Ukraine"]), b"\x00\x01\x02\x03")
    assert not pending_items(con)
    row = recent_items(con, region="EUCOM")[0]
    assert row["summary"] == "One line summary." and row["severity"] == 4

    cid = upsert_cluster(con, None, "Test cluster", 4, now)
    set_cluster(con, iid, cid)
    iid2 = insert_item(con, "https://example.test/b", "SMOKE", "Second headline", now, "d2")
    mark_enriched(con, iid2, "s2", "conflict", "EUCOM", 5, "[]", b"\x00\x01\x02\x04")
    set_cluster(con, iid2, cid)
    upsert_cluster(con, cid, None, 5, now)
    cl = top_clusters(con)[0]
    assert cl["item_count"] == 2 and cl["top_severity"] == 5, dict(cl)
    assert len(clustered_embeddings(con, now - 3600)) == 2

    bad = insert_item(con, "https://example.test/c", "SMOKE", "Bad", now, "")
    mark_error(con, bad, "device timeout")
    assert not pending_items(con)

    for i in range(400):
        record_metric(con, "npu_util", i % 100, now - 3600 + i * 9)
    m = latest_metrics(con, ["npu_util", "gen_tps"], 3600)
    assert m["gen_tps"] == {"latest": None, "series": []}
    assert len(m["npu_util"]["series"]) <= MAX_SERIES_POINTS and m["npu_util"]["latest"] is not None

    set_meta(con, "embeddings", "on")
    set_meta(con, "embeddings", "off")
    assert get_meta(con, "embeddings") == "off" and get_meta(con, "nope", "d") == "d"

    reader = connect(path)  # concurrent second connection, as server.py does
    assert reader.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 3

    # --- indexes: the hot queries must not degrade into table scans ------------
    def plan(sql, args):
        return " ".join(r["detail"] for r in
                        con.execute("EXPLAIN QUERY PLAN " + sql, args).fetchall())

    p = plan("SELECT id FROM items WHERE enriched_at IS NOT NULL"
             " ORDER BY COALESCE(published, fetched_at) DESC, id DESC LIMIT 100", ())
    assert "idx_items_recent" in p and "TEMP B-TREE" not in p, p
    p = plan("SELECT id FROM items WHERE enriched_at IS NOT NULL AND region=?"
             " ORDER BY COALESCE(published, fetched_at) DESC, id DESC LIMIT 100",
             ("EUCOM",))
    assert "idx_items_region_recent" in p, p
    p = plan("SELECT * FROM items WHERE " + _PENDING_WHERE +
             " ORDER BY fetched_at ASC, id ASC LIMIT 20",
             (ENRICH_MAX_ATTEMPTS, now))
    assert "idx_items_pending" in p, p
    p = plan("SELECT id, cluster_id, embedding FROM items WHERE cluster_id IS NOT NULL"
             " AND embedding IS NOT NULL AND enriched_at >= ? ORDER BY enriched_at DESC",
             (now - 3600,))
    assert "idx_items_enriched_at" in p, p
    p = plan(_EMBEDDED_COUNT_SQL, ())
    assert "idx_items_embedded" in p, p
    # ...and it must STAY that plan after ANALYZE. Without INDEXED BY the planner
    # flips to a full table scan (every row carrying a 4 KB blob) once sqlite_stat1
    # exists, and /api/stats runs this every 10s per open tab.
    con.execute("ANALYZE")
    p = plan(_EMBEDDED_COUNT_SQL, ())
    assert "idx_items_embedded" in p, "ANALYZE unpinned the embedded count: %s" % p
    con.execute("ANALYZE sqlite_schema")     # drop the stats again
    con.execute("DELETE FROM sqlite_stat1")
    con.commit()
    assert len(con.execute("PRAGMA index_info(idx_items_cluster)").fetchall()) == 2

    # --- retention: forever by default, windowed only when asked --------------
    old = insert_item(con, "https://example.test/old", "SMOKE", "Ancient", now - 400 * 86400,
                      "old")
    con.execute("UPDATE items SET fetched_at=? WHERE id=?", (now - 400 * 86400, old))
    con.commit()
    assert item_retention_s() == 0.0
    prune(con)
    assert con.execute("SELECT COUNT(*) FROM items WHERE id=?", (old,)).fetchone()[0] == 1, \
        "default retention must keep items forever"
    os.environ["DAYBREAK_ITEM_RETENTION_DAYS"] = "21"
    assert item_retention_s() == 21 * 86400
    res = prune(con)
    assert res["items"] == 1, res
    os.environ.pop("DAYBREAK_ITEM_RETENTION_DAYS")
    os.environ["DAYBREAK_ITEM_RETENTION_DAYS"] = "not-a-number"   # must not explode
    assert item_retention_s() == 0.0
    os.environ.pop("DAYBREAK_ITEM_RETENTION_DAYS")

    # --- hourly rollup: raw samples die, the trend survives -------------------
    h = int(now // 3600) * 3600
    # prune() above already advanced the watermark; rewind it as a fresh archive
    # would be (samples backdated behind the watermark are skipped by design).
    set_meta(con, _ROLLUP_META_KEY, "0")
    for i in range(120):                       # two full past hours of samples
        record_metric(con, "probe", 40 + (i % 20), h - 7200 + i * 60)
    record_metric(con, "probe", 99, now)       # in-progress hour: not rolled yet
    r1 = rollup_metrics(con)
    assert r1["buckets"] >= 2, r1
    hm = hourly_metrics(con, ["probe"], window_s=7 * 86400)["probe"]
    assert len(hm["series"]) == 2 and hm["series"][0][4] == 60, hm
    assert hm["series"][0][2] == 40.0 and hm["series"][0][3] == 59.0, hm
    assert rollup_metrics(con)["buckets"] == 0, "rollup must be idempotent"
    before = [list(x) for x in hm["series"]]
    con.execute("DELETE FROM metrics WHERE ts < ?", (h,))       # simulate raw prune
    con.commit()
    after = hourly_metrics(con, ["probe"], window_s=7 * 86400)["probe"]["series"]
    assert [list(x) for x in after] == before, "hourly series must outlive raw metrics"
    # a later sample landing in an already-folded bucket merges, never doubles
    record_metric(con, "probe", 100, h - 1800)
    set_meta(con, _ROLLUP_META_KEY, str(h - 3600))
    rollup_metrics(con)
    merged = hourly_metrics(con, ["probe"], window_s=7 * 86400)["probe"]["series"][-1]
    assert merged[4] == 61 and merged[3] == 100.0, merged
    # a watermark stranded in the future (clock jumped forward, then back) recovers
    counts_before = [list(x) for x in
                     hourly_metrics(con, ["probe"], window_s=7 * 86400)["probe"]["series"]]
    set_meta(con, _ROLLUP_META_KEY, str(now + 400 * 86400))
    assert rollup_metrics(con).get("clock_reset") is True
    assert float(get_meta(con, _ROLLUP_META_KEY)) <= now
    # ...and the recovery must not re-fold buckets it already has. A reset that
    # rewound the watermark to a bogus (epoch) hour_end made the next pass count
    # every surviving raw sample a second time: n doubled and avg skewed at the
    # retention boundary. Recovery is data-derived, so a full reset + rollup cycle
    # leaves the folded buckets byte-identical.
    set_meta(con, _ROLLUP_META_KEY, str(now + 400 * 86400))
    rollup_metrics(con)               # clock_reset branch
    rollup_metrics(con)               # the recovery pass that used to double-count
    counts_after = [list(x) for x in
                    hourly_metrics(con, ["probe"], window_s=7 * 86400)["probe"]["series"]]
    assert counts_after == counts_before, (counts_before, counts_after)

    # --- images: orphans go, cap enforced, S4/S5 never deleted ----------------
    images = os.path.join(os.path.dirname(path), "images")
    os.makedirs(images, exist_ok=True)
    blob = b"\x89PNG" + b"\x00" * 4096
    for iid in (iid2,):                                   # severity 5 -> protected
        with open(os.path.join(images, "%d.png" % iid), "wb") as fh:
            fh.write(blob)
    low = []
    for k in range(4):                                    # severity 1 -> candidates
        rid = insert_item(con, "https://example.test/low%d" % k, "SMOKE", "Low %d" % k,
                          now - k, "d")
        mark_enriched(con, rid, "s", "politics", "GLOBAL", 1, "[]", None)
        fp = os.path.join(images, "%d.png" % rid)
        with open(fp, "wb") as fh:
            fh.write(blob)
        os.utime(fp, (now - 1000 * (4 - k), now - 1000 * (4 - k)))
        low.append(rid)
    with open(os.path.join(images, "999999.png"), "wb") as fh:   # orphan
        fh.write(blob)
    with open(os.path.join(images, "12.png.tmp"), "wb") as fh:   # abandoned temp
        fh.write(blob)
    os.utime(os.path.join(images, "12.png.tmp"), (now - 7200, now - 7200))
    with open(os.path.join(images, "notes.txt"), "w") as fh:     # foreign file
        fh.write("leave me alone")
    res = prune_images(con, images, max_gb=(3 * len(blob)) / 1e9)
    assert res["orphans"] == 1 and res["temp"] == 1, res
    assert os.path.exists(os.path.join(images, "%d.png" % iid2)), "S5 image deleted!"
    assert os.path.exists(os.path.join(images, "notes.txt"))
    assert res["bytes_after"] <= res["cap_bytes"] and not res["over_cap"], res
    assert not os.path.exists(os.path.join(images, "%d.png" % low[0])), "oldest low first"
    assert os.path.exists(os.path.join(images, "%d.png" % low[3])), "newest low kept"
    assert prune_images(con, images + "-missing")["scanned"] == 0
    # the janitor leaves its verdict where cheap callers can read it
    st = image_cap_state(con)
    assert st["over_cap"] is False and st["bytes"] >= 0, st

    # the cap must be REACHABLE. Both producers write only the protected class, so
    # a permanent S4/S5 exemption meant a directory over cap stayed over cap: a
    # second prune freed 0 bytes forever while the disk filled.
    prot = []
    for k in range(4):
        pid = insert_item(con, "https://example.test/prot%d" % k, "SMOKE",
                          "Hot %d" % k, now - k, "d")
        mark_enriched(con, pid, "s", "conflict", "GLOBAL", 5, "[]", None)
        fp = os.path.join(images, "%d.png" % pid)
        with open(fp, "wb") as fh:
            fh.write(blob)
        os.utime(fp, (now - 90 * 86400 * (4 - k), now - 90 * 86400 * (4 - k)))
        prot.append(pid)
    tiny_cap = (2 * len(blob)) / 1e9
    first = prune_images(con, images, max_gb=tiny_cap)
    assert first["protected_evicted"] > 0, first
    assert not os.path.exists(os.path.join(images, "%d.png" % prot[0])), \
        "oldest protected render must go first"
    assert first["bytes_after"] <= first["cap_bytes"] and not first["over_cap"], first
    second = prune_images(con, images, max_gb=tiny_cap)
    assert second["freed_bytes"] == 0 and not second["over_cap"], second

    print("counts:", counts(con))
    print("archive:", archive_stats(con))
    print("prune:", prune(con))
    print("images:", prune_images(con, images))
    print("db.py self-check OK ->", path)
