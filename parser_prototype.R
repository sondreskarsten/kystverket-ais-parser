library(arrow)
library(duckdb)
library(dplyr, warn.conflicts = FALSE)

BUCKET <- "sondre_brreg_data"
SA_KEY <- "/mnt/project/sondreskarsten-d7d14-8486be2d085b.json"
TARGET_DAY <- "2025-04-01"

gcs <- GcsFileSystem$create(
  json_credentials = readLines(SA_KEY, warn = FALSE) |> paste(collapse = "")
)

read_gcs_pq <- function(path) {
  read_parquet(gcs$OpenInputFile(paste0(BUCKET, "/", path)))
}

parts <- strsplit(TARGET_DAY, "-")[[1]]
yr <- parts[1]; mn <- parts[2]; dy <- parts[3]

cat("=== Reading 24 position parquets for", TARGET_DAY, "===\n")
pos_prefix <- sprintf("ais/raw/positions/year=%s/month=%s/day=%s/", yr, mn, dy)
pos_files <- gcs$GetFileInfo(FileSelector$create(paste0(BUCKET, "/", pos_prefix), recursive = FALSE))
pos_files <- pos_files[grepl("\\.parquet$", sapply(pos_files, function(x) x$path))]
cat("  found:", length(pos_files), "files\n")

pos_list <- lapply(pos_files, function(fi) {
  read_parquet(gcs$OpenInputFile(fi$path))
})
pos <- bind_rows(pos_list)
cat("  total rows:", nrow(pos), "\n")

pos <- pos |>
  rename(
    cog = c4,
    sog = c5,
    msg_type = c6,
    calc_speed = c7,
    sec_prevpoint = c8,
    dist_prevpoint = c9,
    true_heading = c10,
    rot = c11
  )

cat("  distinct MMSIs:", n_distinct(pos$mmsi), "\n")
cat("  sog range:", min(pos$sog, na.rm = TRUE), "-", max(pos$sog, na.rm = TRUE), "\n")

cat("\n=== Reading NSR statinfo for", TARGET_DAY, "===\n")
statinfo_path <- sprintf("ais/raw/statinfo/year=%s/month=%s/day=%s.parquet", yr, mn, dy)
nsr <- tryCatch(read_gcs_pq(statinfo_path), error = function(e) {
  cat("  statinfo not available:", e$message, "\n")
  NULL
})
if (!is.null(nsr)) {
  cat("  NSR rows:", nrow(nsr), "\n")
  cat("  cols:", paste(names(nsr), collapse = ", "), "\n")
}

cat("\n=== Reading Fartøyregisteret latest state ===\n")
fartoy_files <- gcs$GetFileInfo(FileSelector$create(
  paste0(BUCKET, "/fiskeridir/parsed/v1/state/"), recursive = FALSE))
fartoy_paths <- sort(sapply(fartoy_files, function(x) x$path))
fartoy_paths <- fartoy_paths[grepl("\\.parquet$", fartoy_paths)]
latest_fartoy <- tail(fartoy_paths, 1)
cat("  using:", latest_fartoy, "\n")
fartoy <- read_parquet(gcs$OpenInputFile(latest_fartoy)) |>
  filter(!is.na(radio_call_sign)) |>
  select(orgnr, vessel_id, name, radio_call_sign, length, build_year, tonnage_gt)
cat("  rows with callsign:", nrow(fartoy), "\n")

cat("\n=== Building vessel registry (NSR + Fartøyregisteret) ===\n")
if (!is.null(nsr)) {
  vessel_reg <- nsr |>
    select(mmsino, callsign, shipname, imono, shiptypegroupnor,
           shiptypenor, grosstonnage, length, breadth, yearofbuild,
           countrynameeng) |>
    left_join(
      fartoy |> select(orgnr, radio_call_sign, fr_name = name, fr_length = length),
      by = c("callsign" = "radio_call_sign")
    )

  cat("  total vessels:", nrow(vessel_reg), "\n")
  cat("  with orgnr:", sum(!is.na(vessel_reg$orgnr)), "\n")
  cat("  ship type breakdown:\n")
  vessel_reg |>
    count(shiptypegroupnor, sort = TRUE) |>
    head(10) |>
    print()
}

cat("\n=== Enriching positions with vessel registry ===\n")
enriched <- pos |>
  left_join(
    vessel_reg |> select(mmsino, callsign, shipname, shiptypenor,
                         shiptypegroupnor, orgnr),
    by = c("mmsi" = "mmsino")
  )

cat("  enriched rows:", nrow(enriched), "\n")
cat("  with ship type:", sum(!is.na(enriched$shiptypenor)), "\n")
cat("  with orgnr:", sum(!is.na(enriched$orgnr)), "\n")

cat("\n=== Labelling MarU phases (SOG-only mode) ===\n")
enriched <- enriched |>
  mutate(
    is_fishing_vessel = shiptypegroupnor == "Fisk" | grepl("ishing", shiptypenor, ignore.case = TRUE),
    phase = case_when(
      is_fishing_vessel & sog < 5 & sog > 0.3 ~ "fishing",
      sog <= 0.3 ~ "node",
      sog <= 3.0 ~ "manoeuvring",
      sog > 3.0 ~ "cruising",
      TRUE ~ "unknown"
    )
  )

phase_summary <- enriched |>
  count(phase, sort = TRUE) |>
  mutate(pct = round(100 * n / sum(n), 1))
cat("  phase distribution:\n")
print(phase_summary)

fishing_vessels <- enriched |>
  filter(is_fishing_vessel) |>
  distinct(mmsi)
cat("\n  fishing vessels observed:", nrow(fishing_vessels), "\n")

fishing_phase <- enriched |>
  filter(is_fishing_vessel) |>
  count(phase, sort = TRUE) |>
  mutate(pct = round(100 * n / sum(n), 1))
cat("  fishing vessel phase distribution:\n")
print(fishing_phase)

with_orgnr <- enriched |>
  filter(!is.na(orgnr)) |>
  distinct(mmsi, orgnr)
cat("\n  positions with orgnr: MMSI-orgnr pairs:", nrow(with_orgnr), "\n")

cat("\n=== Sample enriched rows (fishing vessels with orgnr) ===\n")
enriched |>
  filter(!is.na(orgnr) & is_fishing_vessel) |>
  select(mmsi, msgtime, lon, lat, sog, cog, shipname, orgnr, phase) |>
  head(10) |>
  print()

cat("\nDONE.\n")
