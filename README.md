# kystverket-ais-parser

Two-stage pipeline: (1) decode raw positions into 14-column parquet with H3 index + orgnr bridge, (2) derive voyage segments with activity classification.

## Overview

| | |
|---|---|
| **What** | AIS position decoding + MarU voyage segmentation |
| **Schedule** | 08:00 Oslo daily (2h after collector) |
| **Runtime** | ~3 min per day |
| **Input** | `ais/raw/positions/` (24 files, ~190 MB) + `ais/raw/statinfo/` + `fiskeridir/parsed/v1/state/` |
| **Output 1** | `ais/parsed/positions/year=Y/month=M/day=D.parquet` — ~10.5M rows, ~165 MB |
| **Output 2** | `ais/gold/voyages_maru/year=Y/month=M/day=D.parquet` — ~70K rows, ~3 MB |
| **Output 3** | `ais/gold/voyages_kystverket/{date}.parquet` — ~2.2K rows, ~90 KB |
| **Downstream** | → fleet_panel, capacity_utilization, portfolio monitor |

## Stage 1: Parsed positions (Layer 1)

### Schema (14 columns)

| Column | Type | Null% | Description |
|---|---|---|---|
| `mmsi` | int64 | 0 | MMSI radio identifier |
| `msgtime` | string | 0 | ISO timestamp |
| `lon` | float64 | 0 | WGS84 |
| `lat` | float64 | 0 | WGS84 |
| `cog` | float64 | <0.1 | Course over ground (degrees) |
| `sog` | float64 | <0.1 | Speed over ground (knots) |
| `msg_type` | int32 | 0 | AIS message type |
| `calc_speed` | float64 | 5 (-99) | Inter-point speed; -99 = first of session |
| `sec_prevpoint` | int32 | 5 (-99) | Seconds since prior position |
| `dist_prevpoint` | int64 | 5 (-99) | Meters since prior position |
| `true_heading` | int32 | 15 (511) | Gyrocompass; 511 = not available |
| `rot` | int32 | 1 | Rate of turn |
| `h3_r8` | string | 0 | H3 resolution-8 cell (~0.74 km²) |
| `orgnr` | string | 91 | Only fishing vessels resolved (mmsi→callsign→fartøy) |

No vessel attributes. No phase labels. No sail IDs. This is Layer 1.

**orgnr 91% null**: bridge only resolves Norwegian fishing vessels via fartøyregisteret (~350 of ~4,300 daily MMSIs). Cargo/tanker/passenger/foreign = NULL.

## Stage 2: Voyage segments (Layer 3 gold)

Consumes parsed positions + NSR statinfo (ship type for fishing classification).

### Phase classification (SOG-only)

| Phase | Rule | Share (all) | Share (fishing) |
|---|---|---|---|
| `fishing` | Fishing vessel + 0.3 < SOG < 5 kn | 10% | 49% |
| `node` | SOG ≤ 0.3 | 17% | 10% |
| `manoeuvring` | SOG ≤ 3 kn (non-fishing) | 13% | — |
| `cruising` | SOG > 3 kn | 60% | 41% |

### Voyage schema (21 columns)

Key fields: `mmsi, sail_id, n_positions, start_time, end_time, mean_sog, max_sog, start_h3, end_h3, n_h3_cells, pct_fishing, pct_cruising, shipname, orgnr, is_fishing`

**`n_h3_cells`**: distinct hex cells traversed. Trawler working one ground = 50-150 cells. Transit vessel = 500+. Activity fingerprint without geometry.

## Resolution chain

```
mmsi → NSR callsign → fartøyregisteret radio_call_sign → orgnr
```

Per day: ~4,300 MMSIs → ~2,500 NSR → ~350 with orgnr.

## Join keys

| Target | Key |
|---|---|
| NSR statinfo | `mmsi = mmsino` |
| Fartøyregisteret | via NSR `callsign = radio_call_sign` |
| BarentsWatch live | `mmsi` |

## Tech

R + DuckDB on r-base Docker image. DuckDB h3 community extension. Token refreshed per day iteration (1h expiry, parser takes 5h for full backfill).

## GCS layout

```
ais/
├── parsed/positions/year=Y/month=M/day=D.parquet    (14 cols, ~165 MB/day)
├── parsed/_checkpoint/parser.json
├── gold/voyages_maru/year=Y/month=M/day=D.parquet    (21 cols, ~3 MB/day)
├── gold/voyages_kystverket/{date}.parquet             (~90 KB/day)
├── gold/fleet_panel.parquet                           (34K rows, 31 cols)
└── gold/capacity_utilization.parquet                  (116 rows)
```

## Cloud Run

| Job | Resources | Schedule |
|---|---|---|
| `kystverket-ais-parser` | 4CPU/16Gi (r-base) | manual (backfill) |
| `kystverket-ais-parser-daily` | same | 08:00 Oslo |
