# DAYBREAK offsite sync — Cloudflare R2

The Pi holds the only copy of the archive: `daybreak.db`, the `images/` renders and the
daily intel digests. `r2.py` pushes all three to an S3-compatible bucket using nothing but
the Python standard library (SigV4 signed by hand with `hmac`/`hashlib` — no pip, ever).

**It is inert until you configure it.** With the `R2_*` variables unset, `from_env()`
returns `None`, nothing opens a socket, nothing touches the DB, and the deployment behaves
exactly as it does today. You can ship the code first and turn sync on later.

What lands in the bucket:

```
snapshots/YYYY-MM-DD.sqlite.gz   one consistent gzipped DB copy per UTC day, newest 14 kept
images/<item_id>.png             every Z-Image render, uploaded once
digests/daybreak-intel-*.md      the daily intel digests
```

---

## 1. Create the bucket

Cloudflare dashboard → **R2 Object Storage** → **Create bucket**.

- **Name:** `daybreak` (anything works; it goes in `R2_BUCKET`)
- **Location:** Automatic, or pin a hint near the Pi
- **Storage class:** Standard

Leave public access **off**. Nothing in DAYBREAK reads from R2 — this is a one-way archive,
and the board never links to bucket objects. A public bucket would just be an unlisted copy
of the archive on the internet for no benefit.

After it exists, the bucket page shows the **S3 API** endpoint. It looks like:

```
https://<ACCOUNT_ID>.r2.cloudflarestorage.com
```

That is the value for `R2_ENDPOINT` — the account-level endpoint, **with** `https://`,
**without** the bucket name and **without** a trailing slash. `r2.py` speaks path-style S3,
so it builds `/<bucket>/<key>` itself. If you put the bucket in the endpoint you get 404s on
every object.

Jurisdiction-restricted buckets (EU / FedRAMP) use a different host —
`https://<ACCOUNT_ID>.eu.r2.cloudflarestorage.com`. Copy whatever the bucket page shows.

## 2. Create a scoped API token

R2 → **Manage R2 API Tokens** → **Create API token**.

- **Permissions:** *Object Read & Write* — not Admin. This code only needs
  PutObject / HeadObject / GetObject / DeleteObject / ListObjects.
- **Specify bucket:** *Apply to specific buckets* → pick `daybreak`. A leaked Pi token then
  reaches one bucket and nothing else in the account.
- **TTL:** forever is fine for an unattended rig; if you set an expiry, put a reminder
  somewhere, because an expired token shows up as `HTTP 403` in the pipeline log and
  nothing else breaks (which is exactly how you miss it for a month).

Create it. The result screen gives you three things — **the secret is shown once**:

| Screen says | Goes into |
|---|---|
| Access Key ID | `R2_ACCESS_KEY_ID` |
| Secret Access Key | `R2_SECRET_ACCESS_KEY` |
| Endpoint for S3 clients | `R2_ENDPOINT` |

Ignore the "Token value" at the top of that screen — that is the Cloudflare API token for
R2's own REST API, not the S3 credentials. `r2.py` wants the Access Key ID / Secret pair.

**Region is `auto`.** R2 has no regions in the AWS sense; the signing scope is the literal
string `auto`, which is what `r2.py` defaults to. Do not set `R2_REGION` unless something
tells you to.

## 3. Add the variables to `/etc/daybreak.env`

`install.sh` never overwrites this file, so edit it in place and append:

```sh
# --- offsite archive (Cloudflare R2). Remove/blank any line to go inert again.
R2_ENDPOINT=https://<ACCOUNT_ID>.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=<access key id>
R2_SECRET_ACCESS_KEY=<secret access key>
R2_BUCKET=daybreak
```

All four are required. If any one is missing the feature stays fully inert — that is
deliberate, but it also means a typo reads as "off", so run the verification in §4.

Optional knobs, all with working defaults:

| Variable | Default | What it does |
|---|---|---|
| `R2_REGION` | `auto` | SigV4 scope region. Leave alone for R2. |
| `R2_SNAPSHOT_KEEP` | `14` | Daily DB snapshots retained; older keys are deleted. |
| `R2_IMAGE_BATCH` | `25` | Max images uploaded per sync pass (keeps the uplink free). |
| `R2_TIMEOUT` | `60` | Seconds per HTTP request. Each request retries twice on 5xx/timeouts. |
| `R2_DIGEST_BATCH` | `50` | Max digests **uploaded** per pass. It caps uploads, not files scanned — the whole `digests/` directory is examined every pass, newest day first. |
| `R2_MAX_SNAPSHOT_MB` | `512` | Refuse to upload a snapshot larger than this. Checked against the DB file **before** the VACUUM+gzip is paid for, and a refused or failed snapshot then backs off 15 min → 24 h instead of redoing the copy every pass. |
| `R2_TMP_DIR` | dir of `DAYBREAK_DB` | Where the snapshot is staged. Must have room for one uncompressed DB copy plus its gzip. Do **not** point this at `/tmp` on the Pi — it is tmpfs, i.e. RAM. |
| `DAYBREAK_DIGESTS_DIR` | `<db dir>/digests` | Where local digest copies are read from. |

Permissions matter — the file holds a live credential:

```sh
sudo chown root:daybreak /etc/daybreak.env
sudo chmod 0640 /etc/daybreak.env
```

## 4. Verify

The self-check does a real put → head → get → list → delete → confirm-gone round-trip on a
throwaway key under `daybreak-selfcheck/`, printing each step. Run it as the service user so
you are testing the same environment systemd hands the daemons:

```sh
sudo -u daybreak env $(grep -E '^R2_' /etc/daybreak.env | xargs) \
  python3 /opt/daybreak/r2.py
```

Expected:

```
endpoint : https://<ACCOUNT_ID>.r2.cloudflarestorage.com
bucket   : daybreak   region: auto
1 put    : daybreak-selfcheck/1755820000.txt
  ok     : 47 bytes
2 head   :
  ok     : present
3 get    :
  ok     : 47 bytes match
4 list   : prefix daybreak-selfcheck/
  ok     : 1 key(s), ours present
5 delete :
  ok     : deleted
6 verify :
  ok     : gone
snapshots: 0 in bucket
r2.py self-check OK
```

With nothing configured it prints `R2 not configured (inert)` and exits 0. That is a pass,
not a failure — it is what the box should say before you do §3.

Then do one real sync pass:

```sh
sudo -u daybreak env $(grep -E '^(R2_|DAYBREAK_)' /etc/daybreak.env | xargs) \
  python3 /opt/daybreak/r2.py --sync
```

It uploads pending images, any digests, and today's DB snapshot, printing a line per leg.
Re-run it: the second pass should upload nothing (`uploaded=0`), which is the proof that the
`synced` ledger and the once-per-UTC-day snapshot rule are working.

### When it does not work

| Symptom | Cause |
|---|---|
| `R2 not configured (inert)` after §3 | A variable is blank or misspelled; the run also prints which ones are missing. Check you edited `/etc/daybreak.env` and not a copy. |
| `HTTP 403 SignatureDoesNotMatch` | Wrong secret, or the Pi's clock is off. SigV4 rejects a skew over 15 minutes — `timedatectl` and make sure NTP is on. |
| `HTTP 403 AccessDenied` | Token lacks Object Read **& Write**, or is scoped to a different bucket. |
| `HTTP 404 NoSuchBucket` | Bucket name in `R2_BUCKET` is wrong, or the bucket got appended to `R2_ENDPOINT`. |
| `URLError` / `timeout` | Pi has no route out, or the tunnel host blocks egress on 443. |
| `snapshot copy failed` | No disk room to stage the copy, or `R2_TMP_DIR` points somewhere unwritable. |

Nothing in this list stops DAYBREAK. Every failure logs one line and the next pass tries
again; a bucket that is down for a week costs you a week of offsite copies and nothing else.

## 5. Restoring

```sh
# newest snapshot for a given day
aws s3 cp s3://daybreak/snapshots/2026-08-21.sqlite.gz .   # or the R2 dashboard
gunzip 2026-08-21.sqlite.gz
sqlite3 2026-08-21.sqlite 'PRAGMA integrity_check; SELECT COUNT(*) FROM items;'
```

Snapshots are taken with `VACUUM INTO` (falling back to the sqlite3 backup API), so they are
untorn point-in-time copies even though the pipeline is mid-write, and they arrive
defragmented with no WAL sidecar. Drop one at `/opt/daybreak/daybreak.db` with the services
stopped and the board comes back with that day's archive.

Images restore by copying `images/` back next to the DB; item IDs are the filenames, so a
restored DB and a restored image set line up.

## 6. Wiring it into the pipeline (integrator)

`r2.py` ships standalone and is safe to deploy before anything calls it. Two steps make the
sync automatic:

**a. `deploy/install.sh`** — add `r2.py` to the copied file list so it lands in `/opt/daybreak`:

```sh
APP_FILES=(schema.sql db.py feeds.py enrich.py pipeline.py server.py r2.py static/index.html)
```

**b. `pipeline.py`** — one more supervised loop, alongside the others in `main()`:

```python
import r2 as r2mod

R2_INTERVAL_S = 900.0
_R2 = r2mod.from_env()
if _R2 is None and r2mod.missing_env() and len(r2mod.missing_env()) < 4:
    log("[r2] partially configured, staying inert — missing: %s"
        % ", ".join(r2mod.missing_env()))

def r2_body(con):
    if _R2 is None:
        return 3600.0          # inert: idle cheaply, never poll
    r2mod.sync_all(con, DB_PATH, IMAGES_DIR, _R2, log=log)
    return None

# in main(): _spawn("r2", r2_body, R2_INTERVAL_S)
```

To get the digests offsite, add one line to `pipeline._vault_file` right after the KB
finalize succeeds:

```python
r2mod.write_digest_copy(text, filename)      # local copy for offsite sync; never raises
```

Without that line everything else still syncs; the `digests/` prefix just stays empty,
because today the vault loop builds each digest in memory and hands it straight to the
device Knowledge Base without ever writing it to disk.

**This sync does not touch the Tiiny.** There is no inference in it, so it deliberately does
*not* take the `img_hold_until` / `enrich_busy_until` NPU lease — it is disk and network
only and is safe to run while the device is mid-inference. It is bounded per pass (25 images
by default, one snapshot per UTC day) so it never monopolises the Pi's uplink.

## 7. Cost

At DAYBREAK's rate — a few MB of DB snapshot a day, a dozen 512×512 renders, one markdown
digest — this sits inside R2's free allowance with room to spare, and R2 charges nothing for
egress, so pulling an archive back costs only the Class B operations. Check Cloudflare's R2
pricing page for the current free-tier numbers before you scale the retention window up.

The one thing that grows without a ceiling is `images/` — nothing prunes the local renders
on the Pi, and every one of them is uploaded once. If disk on the Pi ever gets tight, the
renders are the safe thing to delete locally once `sync_images` has shipped them: the
`synced` ledger keys off the filename, so deleting a local PNG does not cause a re-upload
and does not orphan anything in the bucket.
