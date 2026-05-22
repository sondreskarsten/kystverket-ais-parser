library(duckdb)
library(DBI)

args <- commandArgs(trailingOnly = TRUE)
TARGET_DAY <- if (length(args) >= 1) args[1] else "2025-04-01"
LOCAL_DIR <- paste0("/tmp/ais_parse/", TARGET_DAY)

dir.create(file.path(LOCAL_DIR, "out"), recursive = TRUE, showWarnings = FALSE)

parts <- strsplit(TARGET_DAY, "-")[[1]]
yr <- parts[1]; mn <- parts[2]; dy <- parts[3]

cat(sprintf("[%s] parse_positions day=%s\n", format(Sys.time(), "%H:%M:%S"), TARGET_DAY))

con <- dbConnect(duckdb(), dbdir = ":memory:")
dbExecute(con, "SET memory_limit='1500MB'")
dbExecute(con, "SET threads=2")
dbExecute(con, "INSTALL h3 FROM community")
dbExecute(con, "LOAD h3")

has_statinfo <- file.exists(file.path(LOCAL_DIR, "statinfo.parquet"))
has_fartoy <- file.exists(file.path(LOCAL_DIR, "fartoy.parquet"))

if (has_statinfo && has_fartoy) {
  dbExecute(con, sprintf("
    CREATE TABLE orgnr_bridge AS
    SELECT n.mmsino AS mmsi, f.orgnr
    FROM read_parquet('%s/statinfo.parquet') n
    JOIN (SELECT orgnr, radio_call_sign FROM read_parquet('%s/fartoy.parquet') WHERE radio_call_sign IS NOT NULL) f
      ON n.callsign = f.radio_call_sign
    WHERE f.orgnr IS NOT NULL
  ", LOCAL_DIR, LOCAL_DIR))
  n_bridge <- dbGetQuery(con, "SELECT count(*) AS n FROM orgnr_bridge")
  cat(sprintf("  orgnr bridge: %d mmsi→orgnr pairs\n", n_bridge$n))
}

orgnr_join <- if (has_statinfo && has_fartoy) {
  "LEFT JOIN orgnr_bridge b ON p.mmsi = b.mmsi"
} else ""
orgnr_col <- if (has_statinfo && has_fartoy) ", b.orgnr" else ", NULL AS orgnr"

dbExecute(con, sprintf("
  COPY (
    SELECT p.mmsi, p.msgtime, p.lon, p.lat,
      p.c4 AS cog, p.c5 AS sog, CAST(p.c6 AS INTEGER) AS msg_type,
      p.c7 AS calc_speed, CAST(p.c8 AS INTEGER) AS sec_prevpoint,
      CAST(p.c9 AS BIGINT) AS dist_prevpoint,
      CAST(p.c10 AS INTEGER) AS true_heading, CAST(p.c11 AS INTEGER) AS rot,
      h3_latlng_to_cell(p.lat, p.lon, 8)::VARCHAR AS h3_r8
      %s
    FROM read_parquet('%s/hour=*.parquet') p
    %s
  ) TO '%s/out/positions_decoded.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)
", orgnr_col, LOCAL_DIR, orgnr_join, LOCAL_DIR))

n <- dbGetQuery(con, sprintf("SELECT count(*) AS n FROM read_parquet('%s/out/positions_decoded.parquet')", LOCAL_DIR))
cat(sprintf("  positions_decoded: %s rows, 14 cols\n", format(n$n, big.mark = ",")))

dbDisconnect(con, shutdown = TRUE)
cat(sprintf("[%s] DONE parse_positions\n", format(Sys.time(), "%H:%M:%S")))
