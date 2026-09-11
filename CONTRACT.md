# DAYBREAK — Tiiny OSINT World-News Board · Build Contract

Every worker reads this FIRST. It pins the interfaces so parallel work meshes.
Deviate only if something here is factually impossible — then note it loudly in your report.

## What this is
A 24/7 OSINT world-news dashboard that gives Jason's Tiiny AI Pocket (an NPU edge device)
a REAL job: continuously ingest world news from free sources, have the **Tiiny device do all
AI work** (summarize / classify / geotag / score severity via Ornith-1.0-35B chat; embeddings
via Qwen3-Embedding-0.6B), cluster developing stories, and render a Forward-Observer-style
war board with live device stats and a rack camera feed. Runs unattended for a week on an
Orange Pi 6 Plus in a server rack, published at **daybreak.example.com** through a
Cloudflare tunnel. This is a showcase Tiiny (the vendor) will look at. Make it sharp.

## Hard rules
- **Python 3.11+ STDLIB ONLY** for all backend code. No pip installs, ever. (sqlite3,
  urllib, xml.etree, json, threading, http.server, hashlib, math, email.utils are all you need.)
- Frontend `static/index.html` is ONE self-contained file. System fonts only
  (ui-monospace stack). No CDNs, no build step.
- **The device API key NEVER reaches the browser.** All Tiiny calls happen server-side.
  Frontend talks only to our own `/api/*`.
- The Tiiny does NOT batch: one inference at a time, ~24 tok/s. Enrichment is a SERIAL queue.
- Every network call has a timeout and a try/except. A dead feed, a Tiiny timeout, or a
  malformed article must NEVER kill a loop. Log and continue. This runs a week unattended.
- No git operations. No files outside `the source tree/`.

## Tiiny device (live now, verified)
- Base: `http://TIINY_HOST:8800` where TIINY_HOST env (currently `192.168.1.50`).
- Auth: `Authorization: Bearer <TIINY-DEVICE-BEARER-KEY>` (env `TIINY_KEY`).
- Chat: `POST /v1/chat/completions` model `deepreinforce-ai/Ornith-1.0-35B`. Response includes
  `timings.predicted_per_second` (tok/s) and `usage`. **Ornith quirk:** reasoning goes to
  `message.reasoning_content` and COUNTS against max_tokens; with small budgets `content`
  comes back EMPTY. Use `max_tokens: 800`, `temperature: 0.2`. Parse JSON from `content`;
  if empty, scan `reasoning_content` for the last `{...}` block. One retry on parse failure.
- Embeddings: `POST /v1/embeddings` `{"model":"Qwen/Qwen3-Embedding-0.6B","input":"..."}` —
  endpoint exists; model must be downloaded+started first (deploy/load-models.sh does this via
  `POST /api/v1/models/{id}/download` then `/api/v1/models/{id}/start`; catalog id is
  `Qwen/Qwen3-Embedding-0.6B`; NPU budget: Ornith uses 50/100 units, 50 free — plenty).
- Device stats (HTTP, no SSH): `GET /api/v1/npu/status` →
  `{devices:[{util_percent, mem_used_mb, mem_total_mb, model,...}], cpu:{total_percent},
  memory:{usage_percent}, occupants:[...]}`. Also `GET /api/v1/models/npu/status` →
  `{npu_total, npu_used, npu_available, models:[{model_id, npu_usage, status}]}`.
- If embeddings are unavailable (model won't load), pipeline must still run: fall back to
  clustering by normalized-title token overlap (Jaccard ≥ 0.55) and set meta key
  `embeddings=off` so the UI can show it.

## File layout & ownership (one worker per file, no overlap)
```
daybreak/
  CONTRACT.md          (this file)
  schema.sql           W1
  db.py                W1
  feeds.py             W1
  enrich.py            W2
  pipeline.py          W3
  server.py            W3
  static/index.html    W4
  deploy/install.sh    W5
  deploy/daybreak.service, daybreak-pipeline.service, daybreak-camera.service   W5
  deploy/cloudflared-config.yml, deploy/TUNNEL.md                               W5
  deploy/load-models.sh                                                         W5
  README.md            W5
```
Expansion (2026-08-21), same rules apply:
```
  jobs.py              idle-time deep-work scheduler   (integrator)
  r2.py                offsite sync to Cloudflare R2   (W3-expansion)
  deploy/R2-SETUP.md   operator steps for the bucket   (W3-expansion)
```

## SQLite schema (schema.sql — exact)
```sql
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS items(
  id INTEGER PRIMARY KEY,
  url TEXT UNIQUE NOT NULL,
  source TEXT NOT NULL,
  title TEXT NOT NULL,
  published REAL,              -- epoch; feed pubDate parsed via email.utils, else fetched_at
  fetched_at REAL NOT NULL,
  raw_summary TEXT,            -- feed's own description, stripped of HTML
  summary TEXT,                -- Ornith 1-2 sentence summary (NULL until enriched)
  category TEXT,               -- conflict|terrorism|cyber|diplomacy|economy|disaster|health|crime|politics|tech|energy
  region TEXT,                 -- NORTHCOM|SOUTHCOM|EUCOM|CENTCOM|AFRICOM|INDOPACOM|GLOBAL
  severity INTEGER,            -- 1..5 (5 = flash/critical)
  countries TEXT,              -- JSON array of country names
  embedding BLOB,              -- float32 array bytes (struct.pack), NULL if unavailable
  cluster_id INTEGER,
  enriched_at REAL,
  enrich_error TEXT
);
CREATE INDEX IF NOT EXISTS idx_items_fetched ON items(fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_items_cluster ON items(cluster_id);
CREATE TABLE IF NOT EXISTS clusters(
  id INTEGER PRIMARY KEY,
  label TEXT,                  -- short headline for the developing story
  created_at REAL, updated_at REAL,
  top_severity INTEGER DEFAULT 1,
  item_count INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS metrics(
  ts REAL NOT NULL, key TEXT NOT NULL, value REAL NOT NULL
);  -- keys: npu_util, npu_mem_mb, cpu_pct, mem_pct, gen_tps, enrich_ms, queue_depth, tokens_out
CREATE INDEX IF NOT EXISTS idx_metrics ON metrics(key, ts DESC);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
-- meta keys: embeddings(on|off), last_fetch_ts, pipeline_started_ts, tokens_total, items_enriched_total
CREATE TABLE IF NOT EXISTS metrics_hourly(   -- downsampled long-term telemetry
  ts_hour INTEGER NOT NULL, key TEXT NOT NULL,
  "avg" REAL NOT NULL, "min" REAL NOT NULL, "max" REAL NOT NULL, n INTEGER NOT NULL,
  PRIMARY KEY(ts_hour, key)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS docs(      -- long-form AI output from jobs.py
  id INTEGER PRIMARY KEY,
  kind TEXT NOT NULL,          -- dossier | synthesis | brief
  subject TEXT NOT NULL,       -- entity name | COCOM region | YYYY-MM-DD
  title TEXT, body TEXT NOT NULL, created_at REAL NOT NULL,
  item_count INTEGER DEFAULT 0, meta TEXT);
```
`schema.sql` is the source of truth and also ships `items.enrich_attempts` / `enrich_error_at`
(retry budget), the `oplog` table behind `/api/log`, and the index set that keeps a
forever-growing `items` table fast (partial + expression indexes for `recent_items`,
`pending_items`, `clustered_embeddings`). Reasoning for each index is in db.py's
"index choices" block; `python3 db.py` asserts the query plans.

## db.py (W1) — public interface (exact signatures)
```python
connect(path="daybreak.db") -> sqlite3.Connection   # applies schema.sql, WAL, row_factory=Row
insert_item(con, url, source, title, published, raw_summary) -> int|None  # None if dup (INSERT OR IGNORE)
pending_items(con, limit=20) -> list[Row]           # enriched_at IS NULL AND enrich_error IS NULL, oldest first
mark_enriched(con, item_id, summary, category, region, severity, countries_json, embedding_bytes|None)
mark_error(con, item_id, err)
recent_items(con, region=None, category=None, since=None, limit=100) -> list[Row]  # enriched only, newest first
set_cluster(con, item_id, cluster_id)
clustered_embeddings(con, since_ts) -> list[(item_id, cluster_id, embedding_bytes)]
upsert_cluster(con, cluster_id|None, label, severity, ts) -> cluster_id  # updates count/top_severity/updated_at
top_clusters(con, limit=12) -> list[Row]            # by updated_at desc, item_count>=2
record_metric(con, key, value, ts=None)
latest_metrics(con, keys:list, window_s=3600) -> dict  # {key: {"latest": x, "series": [[ts,v],...]}} series downsampled ≤120 points
get_meta(con, key, default=None) / set_meta(con, key, value)
prune(con, images_dir=None)                         # see retention policy below
counts(con) -> dict  # {items_total, items_24h, enriched_24h, pending, errors_24h, sources_alive_1h(count of distinct source with fetched_at>now-3600)}
rollup_metrics(con, until_ts=None) -> dict          # folds completed hours into metrics_hourly (idempotent)
hourly_metrics(con, keys, window_s=7*86400, max_points=400) -> {key:{latest, series:[[ts,avg,min,max,n],...]}}
prune_images(con, images_dir=None, max_gb=None) -> dict   # bounded image cache, S4/S5 never deleted
archive_stats(con) -> dict  # {items_total, oldest_item_ts, span_days, db_bytes, embeddings_present, ...}
put_doc(con, kind, subject, title, body, item_count=0, meta=None) -> id|None  # kind: dossier|synthesis|brief
latest_doc(con, kind, subject=None) -> Row|None      # newest of a kind (optionally one subject)
recent_docs(con, kind=None, limit=20, with_body=False) -> list[Row]
doc_counts(con) -> dict  # {dossiers, syntheses, briefs, docs_total, latest_doc_ts}
```
`docs` rows are revisions, not upserts: writing the same (kind, subject) again keeps the
older copy. They are part of the permanent archive — `prune()` has no DELETE for them.
**Retention (revised 2026-08-21 — the archive is the product).** Items and their AI analysis are kept
**forever**; `DAYBREAK_ITEM_RETENTION_DAYS` (0/unset = forever) sets a window only if an operator wants
one. Measured cost 9.9 KB/article ≈ 4.3 GB/yr against 4.4 TB free. Raw `metrics` still expire
(`DAYBREAK_METRIC_RETENTION_DAYS`, default 8) but `prune()` folds every completed hour into
`metrics_hourly` first, so long-run trends survive. Generated images are bounded by **disk cap**
(`DAYBREAK_IMAGE_CAP_GB`, default 20): orphans first, then lowest-severity-oldest-first. An S4/S5
image is exempt from that pass but **not forever** — both producers (`jobs.job_image`, which renders
only S4+, and server.py's on-demand endpoint, which has no severity filter) write exactly the
protected class, so a permanent exemption made the cap unreachable: once protected images alone
passed it, every later prune freed 0 bytes and the directory grew until the disk filled. Still over
cap, protected renders older than `DAYBREAK_IMAGE_PROTECT_DAYS` (default 30, `0` = the old
forever behaviour) are evicted oldest-first. The verdict is published to `meta.img_cap_state` so
`jobs` and `server.py` can check it without repeating the sweep, and `job_image` writes an oplog
event when it stalls on it. `prune()` calls `prune_images()` itself using `images/` beside the DB, so
the existing hourly janitor needs no change.
Writers use `with con:` transactions. It must be safe for pipeline (writer) and server
(reader) to hold separate connections to the same file.

## feeds.py (W1)
- `FEEDS`: list of `{"name","url","kind"}` — world news + security/disaster sources, all free/keyless:
  BBC World, Guardian World, Al Jazeera, DW, France24, Kyiv Independent, Times of Israel,
  NHK World, CISA advisories, USGS M4.5+ quakes (atom), ReliefWeb updates, plus 2-3 more you
  verify. Mark each kind: rss|atom|rdf.
- `fetch_all(con) -> dict` — for each feed: GET with 15s timeout, UA
  `"daybreak/1.0 (+https://daybreak.example.com)"`, parse with xml.etree (handle rss2, atom,
  rdf namespaces), strip HTML tags/entities from descriptions (html.parser or regex), parse
  dates with email.utils.parsedate_to_datetime (fallbacks OK), insert_item each. Return
  `{"new": n, "checked": k, "errors": {name: msg}}`. A failing feed never raises.
- OPTIONAL: if env `GNEWS_API_KEY` is set, also pull GNews top-headlines (world) as a source.
- `if __name__ == "__main__":` self-check: fetch all, print per-feed counts (also validates
  the feed list is alive at build time — replace any dead feed you find).

## enrich.py (W2)
```python
class Tiiny:  # base_url, key from env TIINY_HOST/TIINY_KEY (host may include :port? no — host only, port fixed 8800)
    chat_json(self, system, user, max_tokens=800) -> (dict|None, stats)   # stats={"gen_tps","ms","tokens_out"}; Ornith quirk handling per above; 1 retry
    embed(self, text) -> bytes|None      # float32 struct.pack; None on any failure
    device_stats(self) -> dict|None      # merged npu/status + models/npu/status essentials:
        # {npu_util, npu_mem_used_mb, npu_mem_total_mb, cpu_pct, mem_pct, models:[{model_id,npu_usage,status}], npu_used, npu_available}
enrich_item(t: Tiiny, row) -> dict|None  # returns {"summary","category","region","severity","countries"(list),"embedding"(bytes|None),"stats"}
  # prompt: strict-JSON instruction with the exact allowed category/region enums, severity rubric:
  # 5=mass-casualty/major attack/war escalation/nuclear, 4=significant armed action/major disaster/coup,
  # 3=notable political-security development, 2=routine geopolitics/economy, 1=low signal.
  # Region = the COCOM AOR of the primary country involved (GLOBAL if none/worldwide).
  # Input: title + raw_summary + source. Validate enums after parse; clamp severity 1-5.
label_cluster(t: Tiiny, titles: list[str]) -> str|None   # ≤8-word headline for a developing story
cosine(a: bytes, b: bytes) -> float
assign_cluster(con, item_id, emb_bytes, threshold=0.80) -> cluster_id
  # nearest centroid among clusters touched in last 72h (compare vs each cluster's most-recent
  # member embeddings, max sim); join if ≥ threshold else new cluster. Title-Jaccard fallback when embeddings off.
```
`if __name__ == "__main__":` self-check hits the live device: one chat_json enrich of a fake
item + one embed + device_stats, printing results (skip gracefully if device unreachable).

## pipeline.py (W3) — daemon (systemd: daybreak-pipeline.service)
Threads (all daemon=True, all loops try/except-per-iteration with backoff):
1. fetcher: `feeds.fetch_all` every 300s; set_meta last_fetch_ts.
2. enricher: serial; take pending_items batch, for each → enrich_item → mark_enriched →
   assign_cluster → record gen_tps/enrich_ms/tokens_out metrics; accumulate meta tokens_total,
   items_enriched_total. When a cluster hits ≥3 items and has no AI label yet → label_cluster.
   Sleep 2s when queue empty.
3. device-poller: Tiiny.device_stats every 30s → record npu_util/npu_mem_mb/cpu_pct/mem_pct + queue_depth.
4. janitor: prune() hourly.
5. vault: daily digests + project brief into the device Knowledge Base; each digest also
   dropped locally via `r2.write_digest_copy` so the offsite loop can ship it.
6. idle (was "sitrep"): hourly SITREP, then `jobs.run_due(con, job_ctx())` — ONE job per pass.
7. r2: `r2.sync_all` every DAYBREAK_R2_INTERVAL (900s). **Spawned only when `r2.from_env()`
   is not None.** No R2_* env → no thread, no network, no `synced` ledger table. A PARTIAL
   R2 config logs a warning naming the missing vars (it is a typo, not a decision).
Env: TIINY_HOST, TIINY_KEY, DAYBREAK_DB (default ./daybreak.db). Log one line per event to stdout
(journald catches it). SIGTERM = clean exit.

## jobs.py — idle-time deep work (integrator)
```python
class Ctx(tiiny=None, log=None, stop=None, images_dir=None, db_path=None)  # host services
run_due(con, ctx, now=None) -> dict     # ONE job per pass; {"ran": name|None, ...}
status(con, now=None) -> dict           # what is due, last-run clocks, doc counts
job_recluster / job_synthesis / job_dossier / job_image / job_brief (con, ctx, force=False)
brief_day_due(con, now=None) -> "YYYY-MM-DD"|None   # newest unbriefed day, walking back 7
lease(con, ctx, label, seconds=CHAT_LEASE_S)   # context manager, always releases
```
Priority order (first DUE job runs): brief → recluster → synthesis → dossier → image.
Three invariants, enforced inside `run_due`/`lease` so callers cannot get them wrong:
1. **Nothing runs while `db.counts()['pending'] > 0`** — enrichment always wins.
2. **Every device call holds the lease — BOTH keys, chat work included.** `img_hold_until` is
   the key every consumer actually polls (`pipeline.enrich_body`'s between-item yield,
   `pipeline._label_clusters`, `jobs.wait_for_quiet`); `enrich_busy_until` is what server.py's
   on-demand image endpoint waits out. Advertising only `enrich_busy_until` for a 250s dossier
   left the enricher free to start a second chat call on the same device handle, so a chat lease
   now takes both. Release is **compare-and-clear**: these keys are shared across processes, and
   a blind `"0"` in a `finally` zeroes somebody else's live lease — which causes the 150004
   collision rather than preventing it. `jobs.wait_for_quiet` and server.py's release honour the
   same rule.
   Lease durations are DERIVED from the calls they guard — `CHAT_LEASE_S = 2×enrich.TIMEOUT+30`
   (chat_json makes two attempts) and `IMAGE_LEASE_S = image timeout + 30`. A lease shorter
   than its call is not a lease; the old inline 90s image lease against a 120s request is
   exactly how two jobs land on the NPU together (device error 150004).
3. **SIGTERM exits cleanly** — jobs check `ctx.stopped()` between units and never hold a lease
   across a stop. Every job stamps `meta.job_last_<name>` even when it fails, so the interval
   doubles as the backoff and a broken job cannot hot-loop.
Rotation is **attempt-keyed, not success-keyed**: `meta.job_try_<kind>_<subject>` is stamped
before the device call, so a region or entity whose generation keeps failing rotates out of the
way instead of winning the "oldest" race forever.
`python3 jobs.py --selfcheck` runs the whole thing device-free on a temp DB.
CLI (also the operator's brief script): `--status`, `--brief [--day YYYY-MM-DD] [--force]`,
`--job <name> [--force]`, or no args for one scheduler pass.

## server.py (W3) — daemon (systemd: daybreak.service), port 8811
stdlib ThreadingHTTPServer. Read-only DB connection(s). Routes:
- `GET /` → static/index.html ; `GET /healthz` → 200 json.
- `GET /api/items?region&category&since&limit` → `{"items":[{id,url,source,title,published,summary,category,region,severity,countries,cluster_id}]}` (limit≤200 default 100)
- `GET /api/clusters` → `{"clusters":[{id,label,item_count,top_severity,updated_at,titles:[3 newest member titles]}]}`
- `GET /api/stats` → `{"counts":{...db.counts...},"meta":{embeddings,last_fetch_ts,tokens_total,items_enriched_total,pipeline_started_ts},"device":{latest npu_util,npu_mem,cpu_pct,mem_pct + models list from most recent poll},"series":{npu_util:[[ts,v]..],gen_tps:[..],queue_depth:[..]}}`  (series from latest_metrics, 1h window)
  **Additive only.** The expansion adds two top-level keys — `archive` (db.archive_stats: how far
  the archive reaches, item totals, db bytes, projected GB/yr) and `docs` (db.doc_counts:
  dossiers/syntheses/briefs/docs_total/latest_doc_ts) — plus `meta.job_<name>_total`,
  `meta.images_total`, `meta.r2_last_sync_ts`, `meta.r2_last_ship_ts`, `meta.r2_objects_total`.
  Nothing that existed moved or changed type, so an older index.html keeps working.
  `?series=long[&days=N]` (opt-in, default payload unchanged) adds `series_long` from
  `db.hourly_metrics` — the hourly rollup that outlives the 8-day raw-metric retention.
  `meta.r2_last_sync_ts` means "a pass completed"; `meta.r2_last_ship_ts` means "something
  actually moved". Stamping only the latter made a healthy idle loop look like a dead one.
- `GET /api/docs?kind&limit` → `{"counts":{...},"docs":[{id,kind,subject,title,created_at,item_count}]}`
  (bodies omitted); `GET /api/docs?id=N` → `{"doc":{...,"body":"..."}}`. Unknown kind → 400.
- `GET /api/log` → also suppresses a STALE `now` string: `now_doing` is cleared in a `finally`,
  but a SIGKILL mid-inference strands it, so the board only shows it while a lease timestamp
  is still in the future.
- `GET /cam.mjpg` → streaming proxy to `http://127.0.0.1:8812/stream` (ustreamer): stream
  chunks through with correct content-type; if upstream down return 503 fast. Also
  `GET /cam.jpg` proxy to `:8812/snapshot`.
- JSON errors, no stack traces to client. CORS: same-origin only (no header needed).

## static/index.html (W4) — the showpiece
Aesthetic: Forward-Observer war board (reference: a terminal intel feed/index.html —
READ it for vibe; do not copy its GNews logic): near-black `#0a0c10`, panel `#11141b`,
red accent `#c8372d` with subtle glow, thin borders `#1d222e`, ui-monospace stack, uppercase
micro-labels, scanline/grid texture optional but tasteful. Single dark theme (it's a SOC board).
Layout (CSS grid, responsive ≥1280 wide, degrade gracefully on phone):
- Top bar: `▚ DAYBREAK` + `TIINY OSINT // LIVE`, UTC + CDT clocks (tick every s), status LEDs:
  PIPELINE (last_fetch_ts <10min), TIINY (device stats fresh), CAM (cam.jpg loads) — green/red dots.
- Left column (span 2 rows): LIVE WIRE — newest items, each: severity chip (S1..S5 color-coded
  1=#3f4a5a 2=#4a7 3=#d9a require good contrast — pick a 5-step scale ending in bright red),
  UTC time, source tag, title (links out, rel=noopener), one-line summary, region+category tags.
  Newest-first, poll /api/items every 30s, subtle flash animation on new arrivals.
- Center top: AOR GRID — six cards (NORTHCOM, SOUTHCOM, EUCOM, CENTCOM, AFRICOM, INDOPACOM):
  24h item count, max severity glow border, tiny sparkline of activity, click filters the wire.
- Center bottom: DEVELOPING — top clusters: label, item count, top severity, the 3 latest titles.
- Right column: TIINY UNIT panel — big NPU util % readout + bar, NPU mem used/total, CPU, host
  mem, loaded models w/ NPU units (Ornith 50u, Embed…), TOKENS BURNED total (big odometer),
  gen tok/s sparkline + current, items enriched total + /hr rate, queue depth. Then RACK CAM
  panel: `<img src=/cam.mjpg>` with IR badge + reload-on-error, fallback text "CAM OFFLINE".
- Bottom status bar: items 24h · sources alive · last fetch UTC · embeddings on/off · errors 24h.
- Filters: region (from AOR cards) + category chips + severity≥N selector; client-side state, re-query API.
Poll /api/stats every 10s, /api/items 30s, /api/clusters 60s. Sparklines = inline `<canvas>`,
no libraries. Keep it legible from across a room: big numbers, high contrast.

## deploy/ (W5)
- `install.sh` (idempotent, run as root on the Pi): creates /opt/daybreak, copies files,
  creates daybreak user, writes /etc/daybreak.env template (TIINY_HOST, TIINY_KEY, GNEWS_API_KEY
  optional), `apt-get install -y ustreamer` (fallback note if pkg missing: motion or ffmpeg mjpeg
  one-liner), installs+enables the three systemd units, prints status.
- `daybreak.service` / `daybreak-pipeline.service`: python3 /opt/daybreak/{server,pipeline}.py,
  EnvironmentFile=/etc/daybreak.env, Restart=always, RestartSec=5, WorkingDirectory=/opt/daybreak.
- `daybreak-camera.service`: ustreamer `-d /dev/video0 -r 1280x720 -f 15 --host 127.0.0.1 -p 8812`.
- `cloudflared-config.yml`: named-tunnel ingress `daybreak.example.com → http://localhost:8811`
  + 404 catchall. `TUNNEL.md`: exact operator steps (cloudflared install arm64, login, tunnel
  create daybreak, route dns, systemd service) — short and copy-pasteable.
- `load-models.sh`: curl the device: download + start `Qwen/Qwen3-Embedding-0.6B`, poll until
  running, verify `/v1/embeddings` returns a vector; idempotent; clear echo output.
- `README.md` (repo root): what this is, architecture diagram (ASCII), local dev run
  (two commands), Pi deploy (three commands), where the data lives.

## Env contract (all components)
TIINY_HOST (default 192.168.1.50) · TIINY_KEY (required) · DAYBREAK_DB (default ./daybreak.db,
/opt/daybreak/daybreak.db on Pi) · PORT (default 8811) · CAM_URL (default http://127.0.0.1:8812) ·
GNEWS_API_KEY (optional).
Retention (all optional): DAYBREAK_ITEM_RETENTION_DAYS (0/unset = keep forever) ·
DAYBREAK_METRIC_RETENTION_DAYS (default 8, raw samples only) ·
DAYBREAK_METRIC_HOURLY_RETENTION_DAYS (0/unset = forever) · DAYBREAK_IMAGE_CAP_GB (default 20) ·
DAYBREAK_IMAGE_PROTECT_DAYS (default 30, 0 = S4/S5 renders exempt forever).
Jobs (all optional): DAYBREAK_RECLUSTER_INTERVAL (1800) · DAYBREAK_SYNTHESIS_INTERVAL (3600) ·
DAYBREAK_DOSSIER_INTERVAL (5400) · DAYBREAK_IMAGE_INTERVAL (600) · DAYBREAK_BRIEF_CHECK (900) ·
DAYBREAK_BRIEF_AFTER_MIN (30) · DAYBREAK_IMAGE_DAILY_CAP (12) ·
DAYBREAK_IMAGE_RETRY_AFTER (21600, per-item image failure cooldown) · IMAGE_MODEL (Z-Image-Turbo).
Offsite (all four required together, or the feature stays inert): R2_ENDPOINT · R2_ACCESS_KEY_ID ·
R2_SECRET_ACCESS_KEY · R2_BUCKET · R2_REGION (auto) · DAYBREAK_R2_INTERVAL (900) ·
DAYBREAK_DIGESTS_DIR (default `digests/` beside the DB). See deploy/R2-SETUP.md.

## Definition of done (integrator verifies end-to-end, live)
1. `python3 feeds.py` fetches ≥5 feeds with new items.
2. `pipeline.py` enriches real items via the live Tiiny (summary/category/region/severity in DB).
3. Embedding model loaded on device; `/v1/embeddings` live; clusters form. (Or fallback documented.)
4. `server.py` serves `/`, all `/api/*` with real data; index.html renders it.
5. No secrets in frontend; all timeouts/try-except verified by reading, loops survive a dead feed + a Tiiny outage (simulate by wrong port → error logged, loop continues).
6. `python3 -m py_compile` clean on every .py; install.sh passes `bash -n`.
7. `python3 db.py`, `python3 jobs.py --selfcheck` and `python3 r2.py --selfcheck` all pass.
8. Each job type produces real output against the live device: an entity dossier, a regional
   synthesis, a recluster pass that re-homes orphaned items, an image render inside both caps,
   and a daily brief. Every one narrates itself into the AI OPS LOG.
9. R2 stays completely inert with no credentials (no thread, no requests, no ledger table) and
   ships images + digests + a daily snapshot when configured — idempotently.
