library(arrow)
library(duckdb)
library(dplyr, warn.conflicts = FALSE)

args <- commandArgs(trailingOnly = TRUE)
TARGET_DAY <- if (length(args) >= 1) args[1] else "2025-04-01"
LOCAL_DIR <- paste0("/tmp/ais_parse/", TARGET_DAY)
SA_KEY <- Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS",
                     "/mnt/project/sondreskarsten-d7d14-8486be2d085b.json")
BUCKET <- Sys.getenv("GCS_BUCKET", "sondre_brreg_data")

dir.create(LOCAL_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(file.path(LOCAL_DIR, "out"), showWarnings = FALSE)

parts <- strsplit(TARGET_DAY, "-")[[1]]
yr <- parts[1]; mn <- parts[2]; dy <- parts[3]

cat(sprintf("[%s] parser day=%s\n", format(Sys.time(), "%H:%M:%S"), TARGET_DAY))

con <- dbConnect(duckdb(), dbdir = ":memory:")
dbExecute(con, "SET memory_limit='1500MB'")
dbExecute(con, "SET threads=2")

cat("  loading positions into DuckDB...\n")
dbExecute(con, sprintf("
  CREATE TABLE pos AS
  SELECT mmsi, msgtime, lon, lat,
    c4 AS cog, c5 AS sog, CAST(c6 AS INTEGER) AS msg_type,
    c7 AS calc_speed, CAST(c8 AS INTEGER) AS sec_prevpoint,
    CAST(c9 AS BIGINT) AS dist_prevpoint,
    CAST(c10 AS INTEGER) AS true_heading, CAST(c11 AS INTEGER) AS rot
  FROM read_parquet('%s/hour=*.parquet')
", LOCAL_DIR))

n_pos <- dbGetQuery(con, "SELECT count(*) AS n, count(DISTINCT mmsi) AS mmsis FROM pos")
cat(sprintf("  rows: %s  MMSIs: %d\n", format(n_pos$n, big.mark = ","), n_pos$mmsis))

has_statinfo <- file.exists(file.path(LOCAL_DIR, "statinfo.parquet"))
has_voyages <- file.exists(file.path(LOCAL_DIR, "voyages.parquet"))
has_fartoy <- file.exists(file.path(LOCAL_DIR, "fartoy.parquet"))

if (has_statinfo) {
  dbExecute(con, sprintf("CREATE TABLE nsr AS SELECT * FROM read_parquet('%s/statinfo.parquet')",
                         LOCAL_DIR))
}
if (has_fartoy) {
  dbExecute(con, sprintf("
    CREATE TABLE fartoy AS
    SELECT orgnr, radio_call_sign, name AS fr_name, length AS fr_length
    FROM read_parquet('%s/fartoy.parquet')
    WHERE radio_call_sign IS NOT NULL
  ", LOCAL_DIR))
}

cat("  building vessel_registry...\n")
if (has_statinfo && has_fartoy) {
  dbExecute(con, "
    CREATE TABLE vessel_reg AS
    SELECT n.mmsino, n.callsign, n.shipname, n.imono, n.shiptypegroupnor,
           n.shiptypenor, n.grosstonnage, n.length, n.breadth, n.yearofbuild,
           n.countrynameeng, f.orgnr, f.fr_name, f.fr_length
    FROM nsr n
    LEFT JOIN fartoy f ON n.callsign = f.radio_call_sign
  ")
  vr_stats <- dbGetQuery(con, "SELECT count(*) AS n, count(orgnr) AS with_orgnr FROM vessel_reg")
  cat(sprintf("  vessels: %d  with orgnr: %d\n", vr_stats$n, vr_stats$with_orgnr))
}

cat("  enriching + phase labelling...\n")
join_clause <- if (has_statinfo && has_fartoy) {
  "LEFT JOIN vessel_reg v ON p.mmsi = v.mmsino"
} else ""

select_extra <- if (has_statinfo && has_fartoy) {
  ", v.callsign, v.shipname, v.shiptypenor, v.shiptypegroupnor, v.orgnr, v.grosstonnage"
} else ", NULL AS callsign, NULL AS shipname, NULL AS shiptypenor, NULL AS shiptypegroupnor, NULL AS orgnr, NULL AS grosstonnage"

dbExecute(con, sprintf("
  CREATE TABLE enriched AS
  SELECT p.* %s,
    CASE
      WHEN (COALESCE(v.shiptypegroupnor = 'Fisk', false)
            OR COALESCE(v.shiptypenor LIKE '%%ishing%%', false))
           AND p.sog < 5 AND p.sog > 0.3 THEN 'fishing'
      WHEN p.sog <= 0.3 THEN 'node'
      WHEN p.sog <= 3.0 THEN 'manoeuvring'
      WHEN p.sog > 3.0  THEN 'cruising'
      ELSE 'unknown'
    END AS phase,
    COALESCE(v.shiptypegroupnor = 'Fisk', false)
      OR COALESCE(v.shiptypenor LIKE '%%ishing%%', false) AS is_fishing
  FROM pos p
  %s
", select_extra, join_clause))

cat("  assigning sail_ids...\n")
dbExecute(con, "
  CREATE TABLE with_lag AS
  SELECT *,
    LAG(phase, 1, 'node') OVER (PARTITION BY mmsi ORDER BY msgtime) AS prev_phase
  FROM enriched
")
dbExecute(con, "
  CREATE TABLE sailed AS
  SELECT *,
    CASE WHEN phase = 'node' THEN NULL
    ELSE mmsi || '_' || $1 || '_' ||
      SUM(CASE WHEN phase != 'node' AND prev_phase = 'node'
        THEN 1 ELSE 0 END
      ) OVER (PARTITION BY mmsi ORDER BY msgtime)
    END AS sail_id
  FROM with_lag
", params = list(TARGET_DAY))
dbExecute(con, "DROP TABLE with_lag")

out_dir <- file.path(LOCAL_DIR, "out")

cat("  writing positions_decoded...\n")
dbExecute(con, sprintf("
  COPY sailed TO '%s/positions_decoded.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)
", out_dir))
n_out <- dbGetQuery(con, "SELECT count(*) AS n FROM sailed")
cat(sprintf("    %s rows\n", format(n_out$n, big.mark = ",")))

if (has_statinfo && has_fartoy) {
  cat("  writing vessel_registry...\n")
  dbExecute(con, sprintf("
    COPY (SELECT *, '%s' AS observation_date FROM vessel_reg)
    TO '%s/vessel_registry.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)
  ", TARGET_DAY, out_dir))
}

cat("  writing voyages_maru...\n")
dbExecute(con, sprintf("
  COPY (
    SELECT mmsi, sail_id, count(*) AS n_positions,
      min(msgtime) AS start_time, max(msgtime) AS end_time,
      round(avg(sog), 2) AS mean_sog, round(max(sog), 2) AS max_sog,
      round(avg(cog), 1) AS mean_cog,
      first(lon) AS start_lon, first(lat) AS start_lat,
      last(lon) AS end_lon, last(lat) AS end_lat,
      round(100.0 * avg(CASE WHEN phase='fishing' THEN 1.0 ELSE 0.0 END), 1) AS pct_fishing,
      round(100.0 * avg(CASE WHEN phase='cruising' THEN 1.0 ELSE 0.0 END), 1) AS pct_cruising,
      first(shipname) AS shipname, first(shiptypenor) AS shiptypenor,
      first(orgnr) AS orgnr, first(is_fishing) AS is_fishing
    FROM sailed
    WHERE sail_id IS NOT NULL
    GROUP BY mmsi, sail_id
    HAVING count(*) >= 3
  ) TO '%s/voyages_maru.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)
", out_dir))

if (has_voyages) {
  cat("  writing voyages_kystverket...\n")
  kv_join <- if (has_statinfo && has_fartoy) {
    sprintf("
      COPY (
        SELECT kv.*, v.orgnr, v.shiptypenor AS nsr_shiptypenor, '%s' AS observation_date
        FROM read_parquet('%s/voyages.parquet') kv
        LEFT JOIN vessel_reg v ON kv.mmsi = v.mmsino
      ) TO '%s/voyages_kystverket.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)
    ", TARGET_DAY, LOCAL_DIR, out_dir)
  } else {
    sprintf("
      COPY (SELECT *, '%s' AS observation_date FROM read_parquet('%s/voyages.parquet'))
      TO '%s/voyages_kystverket.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)
    ", TARGET_DAY, LOCAL_DIR, out_dir)
  }
  dbExecute(con, kv_join)
}

dbDisconnect(con, shutdown = TRUE)

out_files <- list.files(out_dir, pattern = "\\.parquet$", full.names = TRUE)
for (f in out_files) {
  cat(sprintf("  local: %s (%s bytes)\n", basename(f), format(file.size(f), big.mark = ",")))
}
cat(sprintf("[%s] DONE day=%s (local only, upload handled by caller)\n",
    format(Sys.time(), "%H:%M:%S"), TARGET_DAY))
