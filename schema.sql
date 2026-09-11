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
  enrich_error TEXT,
  enrich_attempts INTEGER DEFAULT 0,   -- failed enrichment attempts (retry budget)
  enrich_error_at REAL                 -- when the last failure parked the row
);
-- Items are a PERMANENT ARCHIVE (db.prune keeps them forever unless
-- DAYBREAK_ITEM_RETENTION_DAYS says otherwise), so every hot query has to stay
-- index-driven at 500k+ rows. See db.py "index choices" for the reasoning.
CREATE INDEX IF NOT EXISTS idx_items_fetched ON items(fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_items_cluster ON items(cluster_id, severity);
-- db.recent_items: WHERE enriched_at IS NOT NULL ORDER BY COALESCE(published,fetched_at) DESC
CREATE INDEX IF NOT EXISTS idx_items_recent
  ON items(COALESCE(published, fetched_at) DESC, id DESC)
  WHERE enriched_at IS NOT NULL;
-- ...same, with the region / category filters the board's AOR cards and chips send
CREATE INDEX IF NOT EXISTS idx_items_region_recent
  ON items(region, COALESCE(published, fetched_at) DESC)
  WHERE enriched_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_items_category_recent
  ON items(category, COALESCE(published, fetched_at) DESC)
  WHERE enriched_at IS NOT NULL;
-- db.pending_items / counts.pending: the unenriched set stays tiny forever
CREATE INDEX IF NOT EXISTS idx_items_pending
  ON items(fetched_at, id) WHERE enriched_at IS NULL;
-- db.clustered_embeddings + counts.enriched_24h. Partial so it cannot tempt the
-- planner away from idx_items_pending on the `enriched_at IS NULL` queue query.
CREATE INDEX IF NOT EXISTS idx_items_enriched_at
  ON items(enriched_at DESC) WHERE enriched_at IS NOT NULL;
-- db.archive_stats: count embedded rows without scanning 500k blob rows
CREATE INDEX IF NOT EXISTS idx_items_embedded ON items(id) WHERE embedding IS NOT NULL;
CREATE TABLE IF NOT EXISTS clusters(
  id INTEGER PRIMARY KEY,
  label TEXT,                  -- short headline for the developing story
  created_at REAL, updated_at REAL,
  top_severity INTEGER DEFAULT 1,
  item_count INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_clusters_updated ON clusters(updated_at DESC);
CREATE TABLE IF NOT EXISTS metrics(
  ts REAL NOT NULL, key TEXT NOT NULL, value REAL NOT NULL
);  -- keys: npu_util, npu_mem_mb, cpu_pct, mem_pct, gen_tps, enrich_ms, queue_depth, tokens_out
CREATE INDEX IF NOT EXISTS idx_metrics ON metrics(key, ts DESC);
-- Raw metrics are high-frequency telemetry and are dropped after ~8 days, but
-- db.rollup_metrics folds every completed hour into this table FIRST, so the
-- long-run trend ("NPU utilisation over the week/month/year") survives.
CREATE TABLE IF NOT EXISTS metrics_hourly(
  ts_hour INTEGER NOT NULL,    -- unix epoch truncated to the hour (bucket start)
  key     TEXT NOT NULL,
  "avg"   REAL NOT NULL,
  "min"   REAL NOT NULL,
  "max"   REAL NOT NULL,
  n       INTEGER NOT NULL,    -- raw samples folded into this bucket
  PRIMARY KEY(ts_hour, key)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_metrics_hourly_key ON metrics_hourly(key, ts_hour DESC);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
-- meta keys: embeddings(on|off), last_fetch_ts, pipeline_started_ts, tokens_total,
--   items_enriched_total, metrics_rollup_ts (watermark for the hourly rollup)
CREATE TABLE IF NOT EXISTS oplog(
  ts REAL NOT NULL, kind TEXT NOT NULL, msg TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_oplog ON oplog(ts DESC);
-- Long-form AI output produced by jobs.py while the enrichment queue is idle.
-- Part of the permanent archive: prune() never deletes these (one dossier is a
-- few KB, and the analysis is the product).
CREATE TABLE IF NOT EXISTS docs(
  id INTEGER PRIMARY KEY,
  kind TEXT NOT NULL,          -- dossier | synthesis | brief
  subject TEXT NOT NULL,       -- entity name | COCOM region | YYYY-MM-DD
  title TEXT,                  -- short headline written by the model
  body TEXT NOT NULL,
  created_at REAL NOT NULL,
  item_count INTEGER DEFAULT 0,-- wire items the model was shown
  meta TEXT                    -- JSON sidecar: {tokens_out, gen_tps, ms, ids:[...]}
);
-- /api/docs and doc_counts read newest-first per kind; the scheduler asks
-- "when did I last write about THIS subject" (kind+subject).
CREATE INDEX IF NOT EXISTS idx_docs_kind ON docs(kind, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_docs_subject ON docs(kind, subject, created_at DESC);
