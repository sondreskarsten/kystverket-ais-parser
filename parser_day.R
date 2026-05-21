library(arrow)
library(duckdb)
library(dplyr, warn.conflicts = FALSE)

TARGET_DAY <- "2025-04-01"
LOCAL_DIR <- paste0("/home/claude/ais_local/", TARGET_DAY)

con <- dbConnect(duckdb())

cat("=== Reading positions ===\n")
pos <- dbGetQuery(con, sprintf("
  SELECT mmsi, msgtime, lon, lat,
    c4 AS cog, c5 AS sog, c6 AS msg_type,
    c7 AS calc_speed, c8 AS sec_prevpoint, c9 AS dist_prevpoint,
    c10 AS true_heading, c11 AS rot
  FROM read_parquet('%s/hour=*.parquet')
", LOCAL_DIR)) |> as_tibble()

cat("  rows:", nrow(pos), "distinct MMSIs:", n_distinct(pos$mmsi), "\n")

cat("\n=== Reading NSR statinfo ===\n")
nsr <- read_parquet(file.path(LOCAL_DIR, "statinfo.parquet")) |> as_tibble()
cat("  vessels:", nrow(nsr), "\n")

cat("\n=== Reading Fartøyregisteret ===\n")
fartoy <- read_parquet(file.path(LOCAL_DIR, "fartoy.parquet")) |>
  filter(!is.na(radio_call_sign)) |>
  select(orgnr, radio_call_sign, fr_name = name, fr_length = length)
cat("  with callsign:", nrow(fartoy), "\n")

cat("\n=== Building vessel registry ===\n")
vessel_reg <- nsr |>
  select(mmsino, callsign, shipname, imono, shiptypegroupnor,
         shiptypenor, grosstonnage, length, breadth, yearofbuild,
         countrynameeng) |>
  left_join(fartoy, by = c("callsign" = "radio_call_sign"))

cat("  total:", nrow(vessel_reg), "with orgnr:", sum(!is.na(vessel_reg$orgnr)), "\n")
vessel_reg |> count(shiptypegroupnor, sort = TRUE) |> head(8) |> print()

cat("\n=== Enriching + MarU phases (SOG-only mode) ===\n")
enriched <- pos |>
  left_join(
    vessel_reg |> select(mmsino, callsign, shipname, shiptypenor,
                         shiptypegroupnor, orgnr, grosstonnage),
    by = c("mmsi" = "mmsino")
  ) |>
  mutate(
    is_fishing = coalesce(shiptypegroupnor == "Fisk", FALSE),
    phase = case_when(
      is_fishing & sog < 5 & sog > 0.3 ~ "fishing",
      sog <= 0.3 ~ "node",
      sog <= 3.0 ~ "manoeuvring",
      sog > 3.0  ~ "cruising",
      TRUE        ~ "unknown"
    )
  )

cat("  enriched rows:", nrow(enriched), "\n")
cat("  with ship_type:", sum(!is.na(enriched$shiptypenor)), "\n")
cat("  with orgnr:", sum(!is.na(enriched$orgnr)), "\n")

enriched |> count(phase, sort = TRUE) |>
  mutate(pct = round(100 * n / sum(n), 1)) |> print()

cat("\n=== Fishing vessel breakdown ===\n")
enriched |> filter(is_fishing) |> count(phase, sort = TRUE) |>
  mutate(pct = round(100 * n / sum(n), 1)) |> print()

fishing_mmsi <- enriched |> filter(is_fishing) |> distinct(mmsi)
cat("  fishing vessels observed:", nrow(fishing_mmsi), "\n")

cat("\n=== Sail-ID assignment (fishing vessels, 1-day window) ===\n")
fishing_pos <- enriched |>
  filter(is_fishing) |>
  arrange(mmsi, msgtime)

fishing_pos <- fishing_pos |>
  group_by(mmsi) |>
  mutate(
    is_stopped = phase == "node",
    was_stopped = lag(is_stopped, default = TRUE),
    sail_boundary = is_stopped != was_stopped & !is_stopped,
    sail_id_inc = cumsum(sail_boundary),
    sail_id = paste0(mmsi, "_", TARGET_DAY, "_", sail_id_inc)
  ) |>
  ungroup()

n_sails <- n_distinct(fishing_pos$sail_id[!fishing_pos$is_stopped])
cat("  unique sail_ids (non-stopped):", n_sails, "\n")

sail_summary <- fishing_pos |>
  filter(!is_stopped) |>
  group_by(mmsi, sail_id) |>
  summarise(
    n_pos = n(),
    start_time = min(msgtime),
    end_time = max(msgtime),
    mean_sog = round(mean(sog), 1),
    max_sog = round(max(sog), 1),
    pct_fishing = round(100 * mean(phase == "fishing"), 1),
    .groups = "drop"
  ) |>
  filter(n_pos >= 5)

cat("  sails with ≥5 positions:", nrow(sail_summary), "\n")
cat("\n  top sails by fishing %:\n")
sail_summary |>
  arrange(desc(pct_fishing), desc(n_pos)) |>
  head(10) |>
  print()

cat("\n=== Sample enriched positions (with orgnr) ===\n")
enriched |>
  filter(!is.na(orgnr)) |>
  select(mmsi, msgtime, sog, phase, shipname, orgnr) |>
  head(8) |>
  print()

dbDisconnect(con, shutdown = TRUE)
cat("\nDONE.\n")
