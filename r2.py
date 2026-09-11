#!/usr/bin/env python3
"""DAYBREAK offsite sync — S3-compatible object storage (Cloudflare R2), stdlib only.

The Pi holds the only copy of the archive: a growing SQLite DB, a directory of
Z-Image renders, and the daily intel digests. This module pushes all three to an
S3-compatible bucket so a dead SD card is an inconvenience, not a total loss.

INERT BY DEFAULT. `from_env()` returns None when the R2_* variables are unset, and
nothing in here runs. A deployment with no credentials behaves exactly as it does
today — that is the point: this feature can ship dark and be switched on later by
adding four lines to /etc/daybreak.env.

Signing is AWS SigV4 done by hand with hmac/hashlib (no boto3, no pip). R2 speaks
path-style S3 against https://<accountid>.r2.cloudflarestorage.com with region
"auto"; the same code works against AWS S3, MinIO, Backblaze B2's S3 endpoint, etc.

NPU: none of this touches the Tiiny. There is no inference here, so this work does
NOT take the img_hold_until / enrich_busy_until lease — it is disk + network only
and can run while the device is mid-inference. It is still bounded per call so it
never competes with the pipeline for the Pi's uplink for long.

Every method returns a result instead of raising: False / None / an empty list, with
the reason left in `r2.last_error` for the caller's one-line log.

    python3 r2.py            # credential round-trip self-check (or "inert" + exit 0)
    python3 r2.py --sync     # one real sync pass against $DAYBREAK_DB
"""

import gzip
import hashlib
import hmac
import os
import re
import shutil
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ALGO = "AWS4-HMAC-SHA256"
SERVICE = "s3"
EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
UA = "daybreak-r2/1.0 (+https://daybreak.example.com)"

DEFAULT_TIMEOUT = 60.0
MAX_ATTEMPTS = 3            # 1 try + 2 retries, on transport errors and 5xx/429 only
RETRY_SLEEP = (1.5, 4.0)
CHUNK = 1 << 20

# Bounded per call so a week-long backlog drains steadily instead of in one 40-minute
# upload that starves the board's own traffic.
IMAGE_BATCH = 25
DIGEST_BATCH = 50
SNAPSHOT_KEEP = 14
MAX_LIST_KEYS = 5000        # hard ceiling on a single list_keys() walk
MAX_SNAPSHOT_MB = 512       # refuse to upload an implausibly large snapshot

# Snapshot staging: the copy is written next to the DB under this prefix and
# removed in a finally. A SIGKILL/power cut mid-VACUUM skips that finally, so the
# next run sweeps anything older than STAGING_MAX_AGE_S.
_STAGING_PREFIX = ".daybreak-snap-"
STAGING_MAX_AGE_S = 3600.0

# A failed snapshot must not re-run the whole VACUUM + gzip on the next R2 pass.
SNAPSHOT_BACKOFF_BASE_S = 900.0
SNAPSHOT_BACKOFF_MAX_S = 86400.0
# Floor on gz/raw for this schema: items carry 4 KB float32 embedding blobs that
# barely compress, so anything bigger than cap/RATIO cannot come out under cap.
SNAPSHOT_EST_RATIO = 0.5
_SNAP_FAIL = {"until": 0.0, "streak": 0}

_SNAPSHOT_RE = re.compile(r"^snapshots/\d{4}-\d{2}-\d{2}\.sqlite\.gz$")


def _env_float(name, default):
    try:
        return float(os.environ.get(name) or default)
    except (TypeError, ValueError):
        return float(default)


def _env_int(name, default):
    try:
        return int(float(os.environ.get(name) or default))
    except (TypeError, ValueError):
        return int(default)


# --------------------------------------------------------------------------- #
# SigV4 client
# --------------------------------------------------------------------------- #

class R2:
    """Minimal S3 client: put / head / get / list / delete. Never raises for an
    expected failure (404, auth error, timeout, dead network) — check the return
    value and read `last_error`."""

    def __init__(self, endpoint, access_key, secret_key, bucket, region="auto",
                 timeout=None):
        endpoint = (endpoint or "").strip().rstrip("/")
        if endpoint and "://" not in endpoint:
            endpoint = "https://" + endpoint
        parts = urllib.parse.urlsplit(endpoint)
        self.endpoint = "%s://%s" % (parts.scheme or "https", parts.netloc)
        self.host = parts.netloc
        self.access_key = (access_key or "").strip()
        self.secret_key = (secret_key or "").strip()
        self.bucket = (bucket or "").strip().strip("/")
        self.region = (region or "auto").strip() or "auto"
        self.timeout = float(timeout) if timeout else _env_float("R2_TIMEOUT",
                                                                 DEFAULT_TIMEOUT)
        self.last_error = None
        if not (self.host and self.access_key and self.secret_key and self.bucket):
            raise ValueError("R2 needs endpoint, access_key, secret_key and bucket")

    def __repr__(self):
        return "<R2 %s/%s region=%s>" % (self.host, self.bucket, self.region)

    # -- signing ----------------------------------------------------------

    def _sign_headers(self, method, canon_path, canon_query, sha_hex, extra=None,
                      amzdate=None):
        """Full signed header set for one attempt (fresh timestamp unless pinned).

        `amzdate` exists so the AWS SigV4 test vectors can be replayed against this
        signer; production always passes None."""
        amzdate = amzdate or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        datestamp = amzdate[:8]
        headers = {"host": self.host,
                   "x-amz-content-sha256": sha_hex,
                   "x-amz-date": amzdate}
        for k, v in (extra or {}).items():
            if v is None:
                continue
            headers[k.lower()] = str(v)
        names = sorted(headers)
        canon_headers = "".join(
            "%s:%s\n" % (n, " ".join(str(headers[n]).split())) for n in names)
        signed_headers = ";".join(names)

        canon_req = "\n".join([method, canon_path, canon_query, canon_headers,
                               signed_headers, sha_hex])
        scope = "%s/%s/%s/aws4_request" % (datestamp, self.region, SERVICE)
        to_sign = "\n".join([ALGO, amzdate, scope,
                             hashlib.sha256(canon_req.encode("utf-8")).hexdigest()])

        key = ("AWS4" + self.secret_key).encode("utf-8")
        for part in (datestamp, self.region, SERVICE, "aws4_request"):
            key = hmac.new(key, part.encode("utf-8"), hashlib.sha256).digest()
        signature = hmac.new(key, to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

        headers["authorization"] = "%s Credential=%s/%s, SignedHeaders=%s, Signature=%s" % (
            ALGO, self.access_key, scope, signed_headers, signature)
        headers["user-agent"] = UA
        return headers

    # -- transport --------------------------------------------------------

    def _object_path(self, key):
        key = (key or "").lstrip("/")
        # SigV4 for S3 encodes the path once; '/' stays a separator and the
        # unreserved set (A-Za-z0-9-._~) is never escaped, which is exactly what
        # urllib.parse.quote does with safe="/".
        return "/%s/%s" % (self.bucket, urllib.parse.quote(key, safe="/"))

    @staticmethod
    def _canon_query(query):
        if not query:
            return ""
        enc = [(urllib.parse.quote(str(k), safe="-_.~"),
                urllib.parse.quote(str(v), safe="-_.~")) for k, v in query.items()
               if v is not None]
        enc.sort()
        return "&".join("%s=%s" % (k, v) for k, v in enc)

    @staticmethod
    def _payload(data):
        """-> (sha256_hex, length, body_factory). Accepts bytes or a seekable file."""
        if data is None:
            return EMPTY_SHA256, 0, (lambda: None)
        if isinstance(data, (bytes, bytearray, memoryview)):
            raw = bytes(data)
            if not raw:
                return EMPTY_SHA256, 0, (lambda: None)
            return hashlib.sha256(raw).hexdigest(), len(raw), (lambda: raw)
        # file object: hash by streaming so a 200 MB snapshot never lands in RAM
        data.seek(0, os.SEEK_END)
        size = data.tell()
        data.seek(0)
        digest = hashlib.sha256()
        while True:
            chunk = data.read(CHUNK)
            if not chunk:
                break
            digest.update(chunk)
        data.seek(0)

        def factory():
            data.seek(0)
            return data

        return digest.hexdigest(), size, factory

    def _request(self, method, canon_path, query=None, data=None, content_type=None,
                 want_body=False, timeout=None):
        """-> {"status": int|None, "body": bytes, "error": str|None}. Never raises."""
        canon_query = self._canon_query(query)
        url = self.endpoint + canon_path + (("?" + canon_query) if canon_query else "")
        timeout = float(timeout) if timeout else self.timeout
        try:
            sha_hex, size, body_of = self._payload(data)
        except Exception as exc:                      # unreadable file, bad handle
            self.last_error = "payload: %s: %s" % (type(exc).__name__, exc)
            return {"status": None, "body": b"", "error": self.last_error}

        extra = {}
        if content_type and size:
            extra["content-type"] = content_type

        err = None
        for attempt in range(MAX_ATTEMPTS):
            try:
                headers = self._sign_headers(method, canon_path, canon_query, sha_hex,
                                             extra)
                body = body_of()
                req = urllib.request.Request(url, data=body, method=method)
                for k, v in headers.items():
                    req.add_header(k, v)
                if body is not None:
                    req.add_header("content-length", str(size))
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    payload = resp.read() if want_body else b""
                    self.last_error = None
                    return {"status": resp.status, "body": payload, "error": None}
            except urllib.error.HTTPError as exc:
                detail = ""
                try:
                    detail = (exc.read() or b"")[:300].decode("utf-8", "replace")
                except Exception:
                    pass
                err = "HTTP %s %s %s" % (exc.code, method, detail.replace("\n", " ").strip())
                if exc.code not in (429, 500, 502, 503, 504):
                    self.last_error = err
                    return {"status": exc.code, "body": b"", "error": err}
            except Exception as exc:                  # URLError, socket.timeout, OSError
                err = "%s: %s" % (type(exc).__name__, str(exc)[:200])
            if attempt < MAX_ATTEMPTS - 1:
                time.sleep(RETRY_SLEEP[min(attempt, len(RETRY_SLEEP) - 1)])
        self.last_error = err
        return {"status": None, "body": b"", "error": err}

    # -- public API -------------------------------------------------------

    def put_object(self, key, data, content_type="application/octet-stream"):
        """Upload bytes (or a seekable binary file object). -> True on success."""
        res = self._request("PUT", self._object_path(key), data=data,
                            content_type=content_type)
        st = res["status"]
        return st is not None and 200 <= st < 300

    def head_object(self, key):
        """-> True if the key exists. A 404 is not an error; last_error stays None."""
        res = self._request("HEAD", self._object_path(key))
        if res["status"] == 404:
            self.last_error = None
            return False
        return res["status"] == 200

    def get_object(self, key):
        """-> bytes, or None if missing/failed."""
        res = self._request("GET", self._object_path(key), want_body=True)
        if res["status"] == 200:
            return res["body"]
        if res["status"] == 404:
            self.last_error = None
        return None

    def delete_object(self, key):
        """-> True when the key is gone (404 counts: already gone is success)."""
        res = self._request("DELETE", self._object_path(key))
        if res["status"] == 404:
            self.last_error = None
            return True
        return res["status"] in (200, 202, 204)

    def list_keys(self, prefix="", max_keys=MAX_LIST_KEYS):
        """ListObjectsV2, following continuation tokens. -> sorted list of keys
        (empty on error — check last_error to tell empty-bucket from failure)."""
        out = []
        token = None
        pages = 0
        base = "/%s" % self.bucket
        while pages < 20 and len(out) < max_keys:
            pages += 1
            query = {"list-type": "2", "max-keys": "1000"}
            if prefix:
                query["prefix"] = prefix
            if token:
                query["continuation-token"] = token
            res = self._request("GET", base, query=query, want_body=True)
            if res["status"] != 200:
                return sorted(out)
            try:
                root = ET.fromstring(res["body"])
            except ET.ParseError as exc:
                self.last_error = "list parse: %s" % exc
                return sorted(out)
            token = None
            truncated = False
            for node in root:
                tag = node.tag.rsplit("}", 1)[-1]
                if tag == "Contents":
                    for child in node:
                        if child.tag.rsplit("}", 1)[-1] == "Key" and child.text:
                            out.append(child.text)
                elif tag == "NextContinuationToken" and node.text:
                    token = node.text
                elif tag == "IsTruncated":
                    truncated = (node.text or "").strip().lower() == "true"
            if not (truncated and token):
                break
        return sorted(out[:max_keys])


def missing_env():
    """Which required R2_* variables are unset. Empty list = configured."""
    return [n for n in ("R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY",
                        "R2_BUCKET") if not (os.environ.get(n) or "").strip()]


def from_env():
    """-> R2, or None when credentials are absent (the whole feature stays inert).

    A PARTIAL config also returns None; call missing_env() to log which ones are
    missing so a typo in /etc/daybreak.env is visible instead of silently dark."""
    if missing_env():
        return None
    try:
        return R2(os.environ["R2_ENDPOINT"], os.environ["R2_ACCESS_KEY_ID"],
                  os.environ["R2_SECRET_ACCESS_KEY"], os.environ["R2_BUCKET"],
                  os.environ.get("R2_REGION", "auto"))
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# synced ledger — one row per object we have already pushed
# --------------------------------------------------------------------------- #

def ensure_synced(con):
    """Create the ledger on first use. Nothing touches the DB while R2 is unset."""
    try:
        with con:
            con.execute("CREATE TABLE IF NOT EXISTS synced("
                        " kind TEXT NOT NULL, key TEXT NOT NULL, ts REAL NOT NULL,"
                        " PRIMARY KEY(kind, key))")
        return True
    except sqlite3.Error:
        return False


def _synced_map(con, kind):
    """{key: ts_of_last_upload}. A local file newer than its ts is re-uploaded, so a
    rewritten digest ships again while a write-once image never does."""
    try:
        return {r[0]: float(r[1] or 0)
                for r in con.execute("SELECT key, ts FROM synced WHERE kind=?",
                                     (kind,)).fetchall()}
    except (sqlite3.Error, TypeError, ValueError):
        return {}


def _mark_synced(con, kind, key):
    try:
        with con:
            con.execute("INSERT OR REPLACE INTO synced(kind, key, ts) VALUES(?,?,?)",
                        (kind, key, time.time()))
        return True
    except sqlite3.Error:
        return False


# --------------------------------------------------------------------------- #
# jobs
# --------------------------------------------------------------------------- #

def sync_images(con, images_dir, r2, limit=None, log=None):
    """Upload local renders not yet in R2 under images/<name>.

    Idempotent (the `synced` ledger is the memory), resumable (oldest first, so an
    interrupted run picks up where it stopped) and bounded (`limit` per call).
    -> {"uploaded", "skipped", "failed", "remaining"}"""
    out = {"uploaded": 0, "skipped": 0, "failed": 0, "remaining": 0}
    if r2 is None or not images_dir or not os.path.isdir(images_dir):
        return out
    limit = max(1, int(limit) if limit else _env_int("R2_IMAGE_BATCH", IMAGE_BATCH))
    if not ensure_synced(con):
        return out
    done = _synced_map(con, "image")

    todo = []
    try:
        with os.scandir(images_dir) as it:
            for entry in it:
                if not entry.name.endswith(".png"):
                    continue          # also skips *.png.tmp half-written renders
                try:
                    if not entry.is_file():
                        continue
                    mtime = entry.stat().st_mtime
                except OSError:
                    continue
                key = "images/" + entry.name
                if done.get(key, 0.0) >= mtime:
                    out["skipped"] += 1
                    continue
                todo.append((mtime, entry.name, entry.path, key))
    except OSError as exc:
        if log:
            log("[r2] images scan failed: %s" % exc)
        return out

    todo.sort()                        # oldest first: an interrupted run resumes here
    out["remaining"] = max(0, len(todo) - limit)
    for _mtime, name, path, key in todo[:limit]:
        try:
            with open(path, "rb") as fh:
                blob = fh.read()
        except OSError as exc:
            out["failed"] += 1
            if log:
                log("[r2] read %s failed: %s" % (name, exc))
            continue
        if not blob:
            out["skipped"] += 1
            continue
        if r2.put_object(key, blob, "image/png"):
            _mark_synced(con, "image", key)
            out["uploaded"] += 1
        else:
            out["failed"] += 1
            if log:
                log("[r2] put %s failed: %s" % (key, r2.last_error))
            if out["failed"] >= 3:     # bucket or link is unhappy; leave the rest
                out["remaining"] = len(todo) - out["uploaded"] - out["failed"]
                break
    if log and (out["uploaded"] or out["failed"]):
        log("[r2] images uploaded=%d failed=%d remaining=%d"
            % (out["uploaded"], out["failed"], out["remaining"]))
    return out


def digests_dir(db_path=None):
    """Where the vault loop's markdown digests land. Env DAYBREAK_DIGESTS_DIR wins."""
    explicit = (os.environ.get("DAYBREAK_DIGESTS_DIR") or "").strip()
    if explicit:
        return explicit
    base = db_path or os.environ.get("DAYBREAK_DB") or os.path.join(BASE_DIR,
                                                                    "daybreak.db")
    return os.path.join(os.path.dirname(os.path.abspath(base)), "digests")


def write_digest_copy(text, filename, db_path=None):
    """Drop a local copy of a digest so sync_digests can ship it offsite.

    Integration hook for pipeline._vault_file: one call, best-effort, never raises.
    -> the path written, or None."""
    if not text or not filename:
        return None
    try:
        target = digests_dir(db_path)
        os.makedirs(target, exist_ok=True)
        safe = os.path.basename(str(filename))
        if not safe.endswith(".md"):
            safe += ".md"
        tmp = os.path.join(target, safe + ".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(text)
        final = os.path.join(target, safe)
        os.replace(tmp, final)
        return final
    except Exception:
        return None


def sync_digests(r2, dir_path=None, con=None, limit=None, log=None):
    """Upload the daily intel digests under digests/<name>.

    With a `con` the `synced` ledger avoids re-listing; without one it falls back to
    a HEAD per file, which is fine at one digest a day. Bounded and idempotent.
    -> {"uploaded", "skipped", "failed", "remaining"}

    The batch limit caps UPLOADS, not files scanned. Slicing the filename list
    before the already-synced check silently stopped the backup dead once the
    directory held more than `limit` files: the window filled with the oldest
    names (all already in the ledger) and nothing new was ever looked at again.
    Same shape as sync_images: build the candidate list first, slice after."""
    out = {"uploaded": 0, "skipped": 0, "failed": 0, "remaining": 0}
    if r2 is None:
        return out
    dir_path = dir_path or digests_dir()
    if not os.path.isdir(dir_path):
        return out                    # nothing produced locally yet — not an error
    ledger = con is not None and ensure_synced(con)
    done = _synced_map(con, "digest") if ledger else {}
    # same shape as R2_IMAGE_BATCH: an explicit limit wins, else the env override
    limit = max(1, int(limit) if limit else _env_int("R2_DIGEST_BATCH", DIGEST_BATCH))

    try:
        # newest day first: a backlog drains from the end that matters, and today's
        # digest is never starved behind a year of archive.
        names = sorted((n for n in os.listdir(dir_path) if n.endswith(".md")),
                       reverse=True)
    except OSError as exc:
        if log:
            log("[r2] digests scan failed: %s" % exc)
        return out

    todo = []
    for name in names:
        key = "digests/" + name
        path = os.path.join(dir_path, name)
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            continue
        if ledger:
            # A rewritten digest (the bootstrap "partial") ships again; a finished
            # day's file is uploaded exactly once, ever.
            if done.get(key, 0.0) >= mtime:
                out["skipped"] += 1
                continue
        elif r2.head_object(key):
            out["skipped"] += 1
            continue
        todo.append((name, path, key))
        if not ledger and len(todo) >= limit:
            break                     # no ledger: stop HEADing once the batch is full

    out["remaining"] = max(0, len(todo) - limit)
    for name, path, key in todo[:limit]:
        try:
            with open(path, "rb") as fh:
                blob = fh.read()
        except OSError as exc:
            out["failed"] += 1
            if log:
                log("[r2] read %s failed: %s" % (name, exc))
            continue
        if not blob:
            out["skipped"] += 1
            continue
        if r2.put_object(key, blob, "text/markdown; charset=utf-8"):
            out["uploaded"] += 1
            if ledger:
                _mark_synced(con, "digest", key)
        else:
            out["failed"] += 1
            if log:
                log("[r2] put %s failed: %s" % (key, r2.last_error))
            if out["failed"] >= 3:     # each failure costs 3 tries; cap the pass
                break
    if log and (out["uploaded"] or out["failed"]):
        log("[r2] digests uploaded=%d failed=%d remaining=%d"
            % (out["uploaded"], out["failed"], out["remaining"]))
    return out


def _consistent_copy(db_path, dest):
    """Untorn copy of a live WAL database. VACUUM INTO first (compact, and it also
    drops the WAL), sqlite3's backup API as the fallback. -> True on success."""
    if not db_path or not os.path.exists(db_path):
        return False        # sqlite3.connect would CREATE it and "succeed" empty
    src = None
    try:
        # autocommit: VACUUM refuses to run inside an open transaction
        src = sqlite3.connect(db_path, timeout=60.0, isolation_level=None)
        src.execute("PRAGMA busy_timeout=60000")
        try:
            src.execute("VACUUM INTO ?", (dest,))
            return True
        except sqlite3.Error:
            if os.path.exists(dest):
                os.remove(dest)
        dst = sqlite3.connect(dest, timeout=60.0)
        try:
            src.backup(dst)           # page-by-page, retries busy pages itself
        finally:
            dst.close()
        return True
    except (sqlite3.Error, OSError):
        return False
    finally:
        if src is not None:
            try:
                src.close()
            except sqlite3.Error:
                pass


def _staging_dir(db_path):
    """Where the snapshot is staged. /tmp is tmpfs (RAM) on the Pi and the copy
    can be large, so stage next to the DB unless R2_TMP_DIR says otherwise."""
    return (os.environ.get("R2_TMP_DIR") or "").strip() or \
        os.path.dirname(os.path.abspath(db_path))


def _sweep_staging(work, log=None, max_age_s=STAGING_MAX_AGE_S):
    """Remove abandoned .daybreak-snap-* files. Nothing else ever reclaims them:
    the finally block below only runs on a clean exit, and a SIGKILL / power cut
    mid-VACUUM leaves a full-size copy of the database on the same volume. Same
    shape as db.prune_images' *.png.tmp sweep. -> files removed."""
    gone = 0
    cutoff = time.time() - float(max_age_s)
    try:
        names = os.listdir(work)
    except OSError:
        return 0
    for name in names:
        if not name.startswith(_STAGING_PREFIX):
            continue
        path = os.path.join(work, name)
        try:
            if os.path.getmtime(path) >= cutoff:
                continue              # a concurrent snapshot is using it
            os.remove(path)
            gone += 1
        except OSError:
            continue
    if gone and log:
        log("[r2] swept %d abandoned snapshot staging file(s)" % gone)
    return gone


def _snapshot_failed(res, log=None):
    """Remember a failed snapshot so the next pass does not redo the whole
    VACUUM + gzip 15 minutes later, forever.

    Without this, a snapshot the box will never manage to send (uplink down, or
    an archive that has grown past R2_MAX_SNAPSHOT_MB) costs a full multi-GB copy,
    compress and delete cycle every R2_INTERVAL: hundreds of GB/day of write
    amplification on the SD card, plus a long read transaction that pins the WAL
    of the live database against the pipeline's writers."""
    _SNAP_FAIL["streak"] += 1
    delay = min(SNAPSHOT_BACKOFF_MAX_S,
                SNAPSHOT_BACKOFF_BASE_S * (2 ** min(_SNAP_FAIL["streak"] - 1, 10)))
    _SNAP_FAIL["until"] = time.time() + delay
    res["backoff_s"] = delay
    if log:
        log("[r2] snapshot backing off %.0f min after %d failure(s)"
            % (delay / 60.0, _SNAP_FAIL["streak"]))
    return res


def _snapshot_ok():
    _SNAP_FAIL["streak"] = 0
    _SNAP_FAIL["until"] = 0.0


def _snapshot_giveup(r2, keep, res, log=None):
    """Record the failure, still retire old snapshots, return the result. Pruning
    is bucket housekeeping and has nothing to do with whether today's copy made
    it, so it must not be skipped on the failure paths."""
    _snapshot_failed(res, log)
    _prune_snapshots(r2, keep, res, log)
    return res


def snapshot_db(db_path, r2, keep=None, force=False, log=None):
    """Upload a gzipped consistent snapshot to snapshots/YYYY-MM-DD.sqlite.gz and
    keep the newest `keep` (env R2_SNAPSHOT_KEEP, default 14).

    One snapshot per UTC day: if today's key is already there it is skipped unless
    force=True. A failed snapshot backs off exponentially (900s -> 24h) instead of
    being retried on every R2 pass, and the size cap is checked BEFORE the copy is
    paid for, not after. -> {"key", "uploaded", "bytes", "deleted", "error"}"""
    res = {"key": None, "uploaded": False, "bytes": 0, "deleted": 0, "error": None}
    if r2 is None:
        return res
    if not db_path or not os.path.exists(db_path):
        res["error"] = "no db at %s" % db_path
        return res
    keep = int(keep) if keep else _env_int("R2_SNAPSHOT_KEEP", SNAPSHOT_KEEP)
    day = time.strftime("%Y-%m-%d", time.gmtime())
    key = "snapshots/%s.sqlite.gz" % day
    res["key"] = key
    cap = _env_int("R2_MAX_SNAPSHOT_MB", MAX_SNAPSHOT_MB) * 1000000

    work = _staging_dir(db_path)
    _sweep_staging(work, log)         # always, even on the skip/backoff paths

    if not force and time.time() < _SNAP_FAIL["until"]:
        res["error"] = "snapshot backing off (%d failure(s), %.0f min left)" % (
            _SNAP_FAIL["streak"], max(0.0, _SNAP_FAIL["until"] - time.time()) / 60.0)
        _prune_snapshots(r2, keep, res, log)
        return res

    if not force and r2.head_object(key):
        res["error"] = None
        _snapshot_ok()
        _prune_snapshots(r2, keep, res, log)
        return res

    # Cap check BEFORE the work. gzip barely dents a table full of float32
    # embedding blobs, so a raw file this far over the cap cannot come out under
    # it -- and finding that out after the VACUUM is what burned the SD card.
    try:
        raw_size = os.path.getsize(db_path)
    except OSError:
        raw_size = 0
    if cap > 0 and raw_size and raw_size * SNAPSHOT_EST_RATIO > cap:
        res["error"] = ("db %.1f MB cannot compress under cap %.0f MB -- "
                        "raise R2_MAX_SNAPSHOT_MB" % (raw_size / 1e6, cap / 1e6))
        if log:
            log("[r2] %s" % res["error"])
        return _snapshot_giveup(r2, keep, res, log)

    stamp = "%d-%d" % (int(time.time()), os.getpid())
    raw = os.path.join(work, "%s%s.sqlite" % (_STAGING_PREFIX, stamp))
    gz = raw + ".gz"
    try:
        if not _consistent_copy(db_path, raw):
            res["error"] = "snapshot copy failed"
            if log:
                log("[r2] %s" % res["error"])
            return _snapshot_giveup(r2, keep, res, log)
        try:
            with open(raw, "rb") as fin, gzip.open(gz, "wb", compresslevel=6) as fout:
                shutil.copyfileobj(fin, fout, CHUNK)
        except OSError as exc:
            res["error"] = "gzip failed: %s" % exc
            if log:
                log("[r2] %s" % res["error"])
            return _snapshot_giveup(r2, keep, res, log)
        size = os.path.getsize(gz)
        res["bytes"] = size
        if size > cap:
            res["error"] = "snapshot %.1f MB over cap %.0f MB" % (size / 1e6, cap / 1e6)
            if log:
                log("[r2] %s" % res["error"])
            return _snapshot_giveup(r2, keep, res, log)
        with open(gz, "rb") as fh:
            ok = r2.put_object(key, fh, "application/gzip")
        if not ok:
            res["error"] = r2.last_error or "put failed"
            if log:
                log("[r2] snapshot put failed: %s" % res["error"])
            return _snapshot_giveup(r2, keep, res, log)
        res["uploaded"] = True
        _snapshot_ok()
        if log:
            log("[r2] snapshot %s (%.1f MB)" % (key, size / 1e6))
    finally:
        for path in (raw, gz):
            try:
                if os.path.exists(path):
                    os.remove(path)
            except OSError:
                pass

    _prune_snapshots(r2, keep, res, log)
    return res


def _prune_snapshots(r2, keep, res, log=None):
    """Delete all but the newest `keep` snapshots. Names sort chronologically."""
    if keep <= 0:
        return
    keys = [k for k in r2.list_keys("snapshots/") if _SNAPSHOT_RE.match(k)]
    for key in keys[:-keep] if len(keys) > keep else []:
        if r2.delete_object(key):
            res["deleted"] += 1
            if log:
                log("[r2] retired %s" % key)
        elif log:
            log("[r2] delete %s failed: %s" % (key, r2.last_error))


def sync_all(con, db_path, images_dir, r2, log=None):
    """One full offsite pass: images, digests, daily snapshot. Bounded; safe to call
    on a timer. Every leg is independent — one failing does not skip the others."""
    out = {}
    if r2 is None:
        return out
    for name, fn in (("images", lambda: sync_images(con, images_dir, r2, log=log)),
                     ("digests", lambda: sync_digests(r2, con=con, log=log)),
                     ("snapshot", lambda: snapshot_db(db_path, r2, log=log))):
        try:
            out[name] = fn()
        except Exception as exc:      # belt and braces: this runs unattended
            out[name] = {"error": "%s: %s" % (type(exc).__name__, exc)}
            if log:
                log("[r2] %s leg crashed: %s: %s" % (name, type(exc).__name__, exc))
    return out


# --------------------------------------------------------------------------- #
# self-check
# --------------------------------------------------------------------------- #

def _selfcheck():
    r2 = from_env()
    if r2 is None:
        gaps = missing_env()
        if gaps and len(gaps) < 4:
            print("R2 partially configured — missing: %s" % ", ".join(gaps))
            print("R2 not configured (inert)")
        else:
            print("R2 not configured (inert)")
        return 0

    print("endpoint : %s" % r2.endpoint)
    print("bucket   : %s   region: %s" % (r2.bucket, r2.region))
    key = "daybreak-selfcheck/%d.txt" % int(time.time())
    body = ("daybreak r2 self-check %s\n"
            % time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())).encode()
    fail = 0

    print("1 put    : %s" % key)
    if not r2.put_object(key, body, "text/plain; charset=utf-8"):
        print("  FAILED : %s" % r2.last_error)
        return 1
    print("  ok     : %d bytes" % len(body))

    print("2 head   :")
    if r2.head_object(key):
        print("  ok     : present")
    else:
        fail += 1
        print("  FAILED : %s" % (r2.last_error or "absent"))

    print("3 get    :")
    got = r2.get_object(key)
    if got == body:
        print("  ok     : %d bytes match" % len(got))
    else:
        fail += 1
        print("  FAILED : %s" % (r2.last_error or "content mismatch"))

    print("4 list   : prefix daybreak-selfcheck/")
    keys = r2.list_keys("daybreak-selfcheck/")
    if key in keys:
        print("  ok     : %d key(s), ours present" % len(keys))
    else:
        fail += 1
        print("  FAILED : %d key(s), ours missing (%s)" % (len(keys), r2.last_error))

    print("5 delete :")
    if r2.delete_object(key):
        print("  ok     : deleted")
    else:
        fail += 1
        print("  FAILED : %s" % r2.last_error)

    print("6 verify :")
    if r2.head_object(key):
        fail += 1
        print("  FAILED : key still present after delete")
    else:
        print("  ok     : gone")

    snaps = r2.list_keys("snapshots/")
    print("snapshots: %d in bucket%s"
          % (len(snaps), (" (newest %s)" % snaps[-1]) if snaps else ""))
    print("r2.py self-check %s" % ("OK" if not fail else "FAILED (%d step(s))" % fail))
    return 0 if not fail else 1


def _sync_cli():
    r2 = from_env()
    if r2 is None:
        print("R2 not configured (inert)")
        return 0
    db_path = os.environ.get("DAYBREAK_DB") or os.path.join(BASE_DIR, "daybreak.db")
    images = os.path.join(os.path.dirname(os.path.abspath(db_path)), "images")
    con = sqlite3.connect(db_path, timeout=30.0)
    try:
        con.execute("PRAGMA busy_timeout=30000")
        out = sync_all(con, db_path, images, r2,
                       log=lambda m: print(m, flush=True))
    finally:
        con.close()
    for name in ("images", "digests", "snapshot"):
        print("%-9s %s" % (name, out.get(name)))
    return 0


if __name__ == "__main__":
    sys.exit(_sync_cli() if "--sync" in sys.argv[1:] else _selfcheck())
