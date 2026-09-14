# Daybreak

**A live world-news wall that thinks entirely on your Tiiny. Nothing leaves your house.**

Daybreak pulls world news from 59 free feeds every five minutes and hands every single
article to your Tiiny Pocket. The Tiiny writes the summary, picks the category, works out
where on earth it happened, scores how serious it is, and builds the embedding that groups
related stories into developing threads.

No cloud model. No API key for an AI service. No subscription. No article text ever leaves
your network. The only things that touch the internet are the news feeds themselves, and
those are public RSS.

Then it renders the whole thing as a wall display you can leave running on a spare monitor.

## Why you might want this

You already own a device that can read. Daybreak gives it something to read all day.

It is genuinely useful as a news wall, but the reason it is worth building is that it shows
you what your Tiiny can actually do when you stop poking at it one prompt at a time and
give it a real job. A thousand articles a day, every one of them summarised, classified,
geotagged and embedded locally. It runs unattended for a week without being touched.

## Get it running in two minutes

There is nothing to install. No pip, no venv, no build step, no Node. It is Python 3.11
standard library only, so if you have Python you are done.

```bash
git clone https://github.com/webdevtodayjason/daybreak.git
cd daybreak

export TIINY_HOST=192.168.1.50                 # your Tiiny's IP
export TIINY_KEY='<Settings -> API key>'       # from the Tiiny itself

python3 pipeline.py &      # reads the news, thinks about it on the Tiiny
python3 server.py          # the board itself
```

Open http://localhost:8811 and watch it fill up. The database is created next to the
scripts on first run. Delete it and start over any time you like.

Or one command, if you would rather not run two:

```bash
python3 daybreak --serve                 # the wall on 8811, pipeline behind it
python3 daybreak --serve --port 7871     # somewhere else
```

Same two things, one process, and Ctrl-C stops both. The two commands above still work
and are what systemd runs on the Pi, because systemd wants one unit per thing it
restarts. `python3 daybreak --selfcheck` builds a database, serves the wall on a spare
loopback port, reads it back and exits, with no device and no network.

### From tiinyapp.farm

Daybreak is in the catalog, so on a machine that has the farm CLI:

```bash
farm install daybreak
farm start daybreak            # or farm start daybreak --port 7871
farm stop daybreak
```

The farm asks where your Tiiny is once, with `farm device`, and writes it to
`~/.tiinyapps/device.json`. Daybreak reads that when `TIINY_HOST` and `TIINY_KEY` are
not set, so there is nothing else to configure. With no device at all the wall still
comes up, keeps collecting articles, and says on the page that nothing is being
analysed yet. Name a device later and the queue drains in order from where it stopped.

If something looks wrong, every module tests itself:

```bash
python3 feeds.py     # fetch every feed once, print what each one gave you
python3 enrich.py    # one live enrichment on the device, so you know the key works
python3 db.py        # schema and query plans
```

## What your Tiiny is actually doing

Every article goes through the device five times:

| Step | What the Tiiny produces |
|---|---|
| Summary | Two sentences, written from the article body |
| Category | Conflict, cyber, disaster, politics, economy and so on |
| Location | The place it happened, geotagged to a real point on the map |
| Severity | 1 to 5, so the board can shout about the important ones |
| Embedding | A vector, used to cluster the same story across outlets |

When the news goes quiet, it does not sit idle. It writes daily briefs, follows up on
developing stories, and paints images for the most serious ones. That is the section
further down called *What the device does when the wire is quiet*.

## Things worth knowing up front

**Your Tiiny runs one inference at a time.** If anything else talks to the same device
while Daybreak is running, the two collide and you get error `150004`. Daybreak handles
this automatically if [OneLane](https://github.com/webdevtodayjason/onelane) is importable:
drop `onelane.py` next to the scripts and everything takes turns properly. Without it
Daybreak still runs fine on its own, it just cannot coordinate with other apps.

**It wants a always-on machine.** A Raspberry Pi, an Orange Pi, an old laptop, anything
that can stay awake. `deploy/` has the systemd units and an installer if you want it
running as a proper service rather than in a terminal.

**The board has no login.** It binds to loopback by default for that reason. If you expose
it on your LAN, anyone on your LAN can read it. There is a Cloudflare tunnel config in
`deploy/TUNNEL.md` if you want to reach it from outside without opening a port.

**Storage grows.** Articles are kept forever by default because the archive is the point.
Images are capped. Both are tunable in the environment table below.

---

*Everything below here is the reference material. You do not need any of it to run Daybreak.*

## Architecture

```
                  59 free OSINT feeds   (RSS / Atom / RDF, keyless)
   BBC · Guardian · Al Jazeera · DW · France24 · Kyiv Independent · Times of Israel
   Channel NewsAsia · Ukrinform · UN News · CISA advisories · USGS quakes
   ReliefWeb · The Record · NWS severe alerts · GDACS · and forty more (feeds.py)
                                       │
                                       │ 15 s timeout · a dead feed never stops the loop
                                       ▼
 ┌───────────────────── ORANGE PI 6 PLUS · arm64 · in rack ──────────────────────┐
 │                                                                               │
 │  daybreak-pipeline.service                     daybreak.service               │
 │  ┌─────────────────────────────┐               ┌─────────────────────────┐    │
 │  │ fetcher        every 300 s  │               │ server.py        :8811  │    │
 │  │ enricher       SERIAL queue │               │  GET /        the board │    │
 │  │ device-poller  every  30 s  │               │  GET /api/items         │    │
 │  │ janitor        every  60 m  │               │  GET /api/clusters      │    │
 │  └──────────────┬──────────────┘               │  GET /api/stats         │    │
 │                 │ writes                       │  GET /cam.mjpg ───┐     │    │
 │                 ▼                              └────────┬──────────┼─────┘    │
 │      ┌──────────────────────────┐   reads (WAL)         │          │          │
 │      │ sqlite   daybreak.db     │◀──────────────────────┘          │          │
 │      │ items · clusters         │                                  ▼          │
 │      │ metrics · meta           │              daybreak-camera.service        │
 │      └──────────────────────────┘              ustreamer /dev/video0 :8812    │
 │                                                (bound to 127.0.0.1 only)      │
 └──────────────┬────────────────────────────────────────────┬───────────────────┘
                │ LAN · Bearer TIINY_KEY                     │ localhost:8811
                ▼                                            ▼
  ┌─────────────────────────────────┐              ┌────────────────────┐
  │  TIINY AI POCKET   (NPU 100 u)  │              │    cloudflared     │
  │  Ornith-1.0-35B          50 u   │              └─────────┬──────────┘
  │  Qwen3-Embedding-0.6B     1 u   │                        │
  │  ~24 tok/s · does NOT batch     │                        ▼
  │  chat  → summary · category ·   │          https://daybreak.example.com
  │          region · severity      │
  │  embed → developing-story       │
  │          clustering             │
  └─────────────────────────────────┘
```

**The device API key never reaches the browser.** Every Tiiny call is server-side; the
frontend only ever talks to our own `/api/*` on port 8811.

---

## Layout

```
daybreak/
  daybreak             one command: the wall and the pipeline in one process
  version.py           the version, in one place
  LICENSE              MIT
  CONTRACT.md          build contract - the pinned interfaces
  schema.sql           sqlite schema (items, clusters, metrics, meta, docs)
  db.py                every DB access in the system
  feeds.py             feed list + fetch/parse/insert
  enrich.py            Tiiny client, enrichment prompt, clustering math
  jobs.py              idle-time deep work: dossiers · syntheses · cluster
                       sweeps · image backfill · the daily brief
  r2.py                offsite sync to Cloudflare R2 (inert without R2_* env)
  pipeline.py          daemon: fetcher · enricher · device-poller · janitor ·
                       vault · idle (jobs.py) · r2
  server.py            daemon: HTTP API + static + camera proxy  (:8811)
  static/index.html    the board - one self-contained file, no CDN, no build
  scripts/
    release.py              builds the tar.gz that tiinyapp.farm lists
  deploy/
    install.sh              idempotent Pi installer
    daybreak.service        systemd: API + UI
    daybreak-pipeline.service   systemd: ingest + enrichment
    daybreak-camera.service     systemd: ustreamer rack cam
    load-models.sh          load the embedder onto the NPU, verify it
    cloudflared-config.yml  named-tunnel ingress
    TUNNEL.md               operator steps for daybreak.example.com
    R2-SETUP.md             operator steps for the offsite bucket
  README.md            this file
```

### What the device does when the wire is quiet

The enrichment queue is the day job, and it drains. `jobs.py` is what the NPU does
instead - one job per pass, never while `pending > 0`, always holding the NPU lease:

| job | every | what it writes |
|---|---|---|
| `brief` | daily | the executive brief for the finished UTC day (`docs`, + a markdown copy for R2) |
| `recluster` | 30 min | re-homes enriched items that never landed in a cluster; labels clusters that grew |
| `synthesis` | 1 h | one COCOM AOR's last 24h read into a regional assessment |
| `dossier` | 90 min | the hottest entity on the wire, 14 days of coverage, written up |
| `image` | 10 min | one missing S4+ story render, inside the daily and disk caps |

Read them at `/api/docs`, counted at `/api/stats` → `docs`, narrated in the AI OPS LOG.

---

## Run it locally

One-time, in the shell you'll use:

```bash
export TIINY_HOST=192.168.1.50
export TIINY_KEY='<uuid from the Tiiny: Settings → API key>'
```

Then two commands, from the `daybreak/` directory:

```bash
python3 pipeline.py &      # ingest + NPU enrichment, logs to stdout
python3 server.py          # http://localhost:8811
```

`daybreak.db` is created next to the scripts on first run. There is nothing to install.

Each module also self-checks on its own:

```bash
python3 feeds.py     # fetch every feed once, print per-source counts
python3 enrich.py    # one live enrichment + one embedding + device stats
python3 db.py        # schema, retention, rollup + query-plan assertions
python3 jobs.py --selfcheck    # the whole scheduler, device-free, on a temp DB
python3 r2.py --selfcheck      # offsite round-trip (prints "inert" with no R2_* env)
```

The job scheduler is also the operator's brief script:

```bash
python3 jobs.py --status                        # what is due, last-run clocks, doc counts
python3 jobs.py --brief                         # file the brief for the last finished UTC day
python3 jobs.py --brief --day 2026-08-20 --force
python3 jobs.py --job synthesis --force         # run one job right now
```

---

## Deploy to the Pi

```bash
# 1 - copy the tree over
rsync -a --delete ~/code/tiiny/daybreak/ root@orangepi:/opt/daybreak-src/

# 2 - install: service user, /opt/daybreak, /etc/daybreak.env, ustreamer, 3 systemd units
ssh root@orangepi 'bash /opt/daybreak-src/deploy/install.sh'

# 3 - put the key in, start, and load the embedding model onto the NPU
ssh -t root@orangepi 'nano /etc/daybreak.env \
  && systemctl restart daybreak daybreak-pipeline \
  && bash /opt/daybreak/load-models.sh'
```

`install.sh` is idempotent - re-run it after every code change; it re-copies the files,
reinstalls the units, restarts the services, and leaves `/etc/daybreak.env` alone.

Publishing it on `daybreak.example.com` is a separate, one-time job: **`deploy/TUNNEL.md`**.

### Where things live on the Pi

| Path | What |
|---|---|
| `/opt/daybreak/` | code, owned root, group-writable by `daybreak` |
| `/opt/daybreak/daybreak.db` | **all the data** (+ `-wal`, `-shm` sidecars) |
| `/etc/daybreak.env` | `TIINY_KEY` and friends, `0640 root:daybreak` |
| `/etc/systemd/system/daybreak*.service` | the three units |
| `/etc/cloudflared/config.yml` | tunnel ingress |
| journald | every log line, per unit |

---

## Operating it

```bash
systemctl status daybreak daybreak-pipeline daybreak-camera
journalctl -fu daybreak-pipeline          # what the NPU is chewing on right now
journalctl -fu daybreak -n 100
curl -s localhost:8811/healthz            # {"ok":true,...}
curl -s localhost:8811/api/stats | python3 -m json.tool | head -40

systemctl restart daybreak daybreak-pipeline    # after a code change
bash /opt/daybreak/load-models.sh               # when the board reads EMBEDDINGS OFF
sqlite3 /opt/daybreak/daybreak.db 'select count(*) from items'   # if sqlite3 is installed
```

| Symptom | Look at |
|---|---|
| Wire is empty | `journalctl -u daybreak-pipeline` - feed errors, or `TIINY_KEY` unset |
| Items arrive but never get summaries | Tiiny unreachable or model stopped: `curl -H "Authorization: Bearer $TIINY_KEY" http://$TIINY_HOST:8800/api/v1/models/running` |
| `EMBEDDINGS OFF` in the bottom bar | embedder not loaded → `bash /opt/daybreak/load-models.sh` |
| `CAM OFFLINE` | `systemctl status daybreak-camera`, `curl -I 127.0.0.1:8812/snapshot` |
| Board reachable locally, 1033 publicly | cloudflared - see `deploy/TUNNEL.md` |

---

## Environment

| Variable | Default | Notes |
|---|---|---|
| `TIINY_HOST` | `192.168.1.50` | host only; the gateway port is probed (80 on firmware 1.0, else 8800) or pinned with `TIINY_PORT` |
| `TIINY_KEY` | - | **required**, server-side only, never sent to the browser |
| `TIINY_BASE` | unset | what the farm exports; a full base URL, read when `TIINY_HOST` is unset |
| `TIINYAPP_PORT` | unset | what the farm exports; the port, when `--port` is not given |
| `DAYBREAK_DB` | `./daybreak.db` | `/opt/daybreak/daybreak.db` on the Pi |
| `PORT` | `8811` | server.py listen port |
| `BIND` | `127.0.0.1` | loopback only - the tunnel is the intended path in. Set `0.0.0.0` to also expose the (unauthenticated) board on the LAN |
| `CAM_URL` | `http://127.0.0.1:8812` | ustreamer origin proxied by `/cam.mjpg` |
| `GNEWS_API_KEY` | unset | optional extra source; everything else is keyless |
| `DAYBREAK_IMAGE_DAILY_CAP` | `12` | idle image renders per UTC day |
| `DAYBREAK_IMAGE_CAP_GB` | `20` | disk ceiling on the image cache |
| `DAYBREAK_IMAGE_PROTECT_DAYS` | `30` | how long an S4/S5 render is exempt from eviction. Both renderers write only S4/S5, so a permanent exemption made the cap above unreachable. `0` = exempt forever (the old behaviour) |
| `DAYBREAK_IMAGE_RETRY_AFTER` | `21600` | seconds before retrying an item the device refused to render |
| `DAYBREAK_ITEM_RETENTION_DAYS` | `0` | 0 = keep every article forever (the archive is the product) |
| `DAYBREAK_{RECLUSTER,SYNTHESIS,DOSSIER,IMAGE}_INTERVAL` | see `jobs.py` | seconds between idle jobs |
| `R2_ENDPOINT` / `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` / `R2_BUCKET` | unset | all four or nothing - offsite sync is fully inert without them. See `deploy/R2-SETUP.md` |
| `DAYBREAK_R2_INTERVAL` | `900` | seconds between offsite passes |

---

## HTTP API

| Route | Returns |
|---|---|
| `GET /` | the board |
| `GET /healthz` | liveness json: `ok`, `version`, and `device` (is one configured) |
| `GET /api/items?region&category&since&limit` | enriched articles, newest first (limit ≤ 200) |
| `GET /api/clusters` | developing stories: label, count, top severity, 3 newest titles |
| `GET /api/stats` | counts, meta, live device telemetry, 1 h series for the sparklines, plus `archive` (span/size of the permanent archive) and `docs` (dossier/synthesis/brief counts) |
| `GET /api/docs?kind&limit` · `GET /api/docs?id=N` | the AI's long-form output: index, or one document with its body |
| `GET /api/log` | AI OPS LOG - every job narrates itself here |
| `GET /cam.mjpg` · `GET /cam.jpg` | rack camera, proxied from ustreamer |

---

## Constraints worth knowing before you change anything

- **Stdlib only.** No pip, ever. `sqlite3`, `urllib`, `xml.etree`, `json`, `threading`,
  `http.server`, `struct`, `email.utils` cover the whole system.
- **The Tiiny does not batch.** One inference at a time, ~24 tok/s. Enrichment is a
  strictly serial queue; parallelising it makes the device slower, not faster.
- **Ornith quirk.** Reasoning lands in `message.reasoning_content` and *counts against*
  `max_tokens`, so a small budget returns an empty `content`. We use `max_tokens: 800` and
  fall back to scanning `reasoning_content` for the trailing `{...}` block.
- **Embeddings are optional.** If `Qwen3-Embedding-0.6B` will not load, clustering falls
  back to normalized-title Jaccard overlap ≥ 0.55 and `meta.embeddings` is set to `off`,
  which the board surfaces in the bottom bar. The pipeline never stops.
- **Cluster join threshold is 0.80 cosine** (`enrich.CLUSTER_THRESHOLD`) and it is not an
  arbitrary number. Measured on 2026-08-21 against the live embedder over 89 real enriched
  articles (3,916 pairs): every pair at ≥ 0.808 was a genuine same-story match, the next
  one down at 0.774 was junk, and it sat between two genuine matches at 0.724 and 0.720.
  The 0.72–0.78 band is mixed, so lowering the threshold buys two real joins and one
  visibly wrong story on the DEVELOPING panel. Don't move it without re-running that
  measurement over a few hundred live items.
- **Every network call has a timeout and a try/except.** A dead feed, a Tiiny timeout or a
  malformed article gets logged and skipped. This thing runs a week with nobody watching.
