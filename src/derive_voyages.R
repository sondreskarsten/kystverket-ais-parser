library(duckdb)
library(DBI)

args <- commandArgs(trailingOnly = TRUE)
TARGET_DAY <- if (length(args) >= 1) args[1] else "2025-04-01"
LOCAL_DIR <- paste0("/tmp/ais_parse/", TARGET_DAY)

dir.create(file.path(LOCAL_DIR, "out"), recursive = TRUE, showWarnings = FALSE)

cat(sprintf("[%s] derive_voyages day=%s\n", format(Sys.time(), "%H:%M:%S"), TARGET_DAY))
cat("  inputs: parsed positions + NSR statinfo (ship_type for phase classification)\n")

pos_file <- file.path(LOCAL_DIR, "out", "positions_decoded.parquet")
statinfo_file <- file.path(LOCAL_DIR, "statinfo.parquet")
voyages_file <- file.path(LOCAL_DIR, "voyages.parquet")
fartoy_file <- file.path(LOCAL_DIR, "fartoy.parquet")

if (!file.exists(pos_file)) {
  cat("  SKIP: positions_decoded.parquet not found (parse_positions must run first)\n")
  quit(status = 1)
}

con <- dbConnect(duckdb(), dbdir = ":memory:")
dbExecute(con, "SET memory_limit='1500MB'")
dbExecute(con, "SET threads=2")
dbExecute(con, "INSTALL h3 FROM community")
dbExecute(con, "LOAD h3")

has_statinfo <- file.exists(statinfo_file)
has_fartoy <- file.exists(fartoy_file)

if (has_statinfo) {
  dbExecute(con, sprintf("
    CREATE TABLE ship_type AS
    SELECT mmsino AS mmsi, shiptypegroupnor, shiptypenor, shipname
    FROM read_parquet('%s')
  ", statinfo_file))
}

type_join <- if (has_statinfo) "LEFT JOIN ship_type st ON p.mmsi = st.mmsi" else ""
fishing_expr <- if (has_statinfo) {
  "COALESCE(st.shiptypegroupnor = 'Fisk', false) OR COALESCE(st.shiptypenor LIKE '%ishing%', false)"
} else "false"
name_col <- if (has_statinfo) ", st.shipname, st.shiptypenor" else ", NULL AS shipname, NULL AS shiptypenor"

cat("  phase labelling...\n")
dbExecute(con, sprintf("
  CREATE TABLE phased AS
  SELECT p.*,
    CASE
      WHEN (%s) AND p.sog < 5 AND p.sog > 0.3 THEN 'fishing'
      WHEN p.sog <= 0.3 THEN 'node'
      WHEN p.sog <= 3.0 THEN 'manoeuvring'
      WHEN p.sog > 3.0  THEN 'cruising'
      ELSE 'unknown'
    END AS phase,
    (%s) AS is_fishing
    %s
  FROM read_parquet('%s') p
  %s
", fishing_expr, fishing_expr, name_col, pos_file, type_join))

cat("  sail_id assignment...\n")
dbExecute(con, "
  CREATE TABLE with_lag AS
  SELECT *, LAG(phase, 1, 'node') OVER (PARTITION BY mmsi ORDER BY msgtime) AS prev_phase
  FROM phased
")
dbExecute(con, "
  CREATE TABLE sailed AS
  SELECT *,
    CASE WHEN phase = 'node' THEN NULL
    ELSE mmsi || '_' || $1 || '_' ||
      SUM(CASE WHEN phase != 'node' AND prev_phase = 'node' THEN 1 ELSE 0 END)
        OVER (PARTITION BY mmsi ORDER BY msgtime)
    END AS sail_id
  FROM with_lag
", params = list(TARGET_DAY))
dbExecute(con, "DROP TABLE with_lag")
dbExecute(con, "DROP TABLE phased")

cat("  writing voyages_maru...\n")
dbExecute(con, sprintf("
  COPY (
    SELECT mmsi, sail_id, count(*) AS n_positions,
      min(msgtime) AS start_time, max(msgtime) AS end_time,
      round(avg(sog), 2) AS mean_sog, round(max(sog), 2) AS max_sog,
      round(avg(cog), 1) AS mean_cog,
      first(lon) AS start_lon, first(lat) AS start_lat,
      last(lon) AS end_lon, last(lat) AS end_lat,
      first(h3_r8) AS start_h3, last(h3_r8) AS end_h3,
      count(DISTINCT h3_r8) AS n_h3_cells,
      round(100.0 * avg(CASE WHEN phase='fishing' THEN 1.0 ELSE 0.0 END), 1) AS pct_fishing,
      round(100.0 * avg(CASE WHEN phase='cruising' THEN 1.0 ELSE 0.0 END), 1) AS pct_cruising,
      first(shipname) AS shipname, first(shiptypenor) AS shiptypenor,
      first(orgnr) AS orgnr, first(is_fishing) AS is_fishing
    FROM sailed
    WHERE sail_id IS NOT NULL
    GROUP BY mmsi, sail_id
    HAVING count(*) >= 3
  ) TO '%s/out/voyages_maru.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)
", LOCAL_DIR))

if (file.exists(voyages_file)) {
  cat("  writing voyages_kystverket...\n")
  orgnr_join_kv <- if (has_statinfo && has_fartoy) {
    sprintf("
      COPY (
        SELECT kv.*,
          f.orgnr,
          '%s' AS observation_date
        FROM read_parquet('%s') kv
        LEFT JOIN (
          SELECT n.mmsino, f.orgnr
          FROM read_parquet('%s') n
          JOIN (SELECT orgnr, radio_call_sign FROM read_parquet('%s') WHERE radio_call_sign IS NOT NULL) f
            ON n.callsign = f.radio_call_sign
        ) f ON kv.mmsi = f.mmsino
      ) TO '%s/out/voyages_kystverket.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)
    ", TARGET_DAY, voyages_file, statinfo_file, fartoy_file, LOCAL_DIR)
  } else {
    sprintf("
      COPY (SELECT *, '%s' AS observation_date FROM read_parquet('%s'))
      TO '%s/out/voyages_kystverket.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)
    ", TARGET_DAY, voyages_file, LOCAL_DIR)
  }
  dbExecute(con, orgnr_join_kv)
}

dbExecute(con, "DROP TABLE sailed")
dbDisconnect(con, shutdown = TRUE)

out_files <- list.files(file.path(LOCAL_DIR, "out"), pattern = "voyage", full.names = TRUE)
for (f in out_files) {
  cat(sprintf("  local: %s (%s bytes)\n", basename(f), format(file.size(f), big.mark = ",")))
}
cat(sprintf("[%s] DONE derive_voyages\n", format(Sys.time(), "%H:%M:%S")))
