# kystverket-ais-parser

Two-stage pipeline: (1) decode raw AIS positions into a clean 14-column parquet with H3 spatial indexing and orgnr bridge, (2) derive voyage segments with activity classification from positions + NSR ship type.

## Stage 1: parse_positions.R → `ais/parsed/positions/`

### LUAS

**(mmsi, msgtime)**. One row = one AIS transmission from one vessel at one instant. ~10M rows per day.

### Schema (14 columns)

| Column | Type | Null rate | Description |
|---|---|---|---|
| `mmsi` | int64 | 0% | MMSI radio identifier |
| `msgtime` | string | 0% | ISO timestamp |
| `lon` | float64 | 0% | WGS84 longitude |
| `lat` | float64 | 0% | WGS84 latitude |
| `cog` | float64 | <0.1% | Course over ground (degrees) |
| `sog` | float64 | <0.1% | Speed over ground (knots) |
| `msg_type` | int32 | 0% | AIS message type |
| `calc_speed` | float64 | ~5% (-99) | Inter-point speed; -99 = first report of session |
| `sec_prevpoint` | int32 | ~5% (-99) | Seconds since prior position |
| `dist_prevpoint` | int64 | ~5% (-99) | Meters since prior position |
| `true_heading` | int32 | ~15% (511) | Gyrocompass heading; 511 = not available |
| `rot` | int32 | ~1% | Rate of turn |
| `h3_r8` | string | 0% | H3 resolution-8 hex cell ID (~0.74 km²) |
| `orgnr` | string | ~91% | Corporate owner (only fishing vessels resolved) |

**No vessel attributes, no phase labels, no sail IDs.** This is Layer 1 — source fields + spatial index + identity bridge. Derivations are in gold.

**orgnr null rate**: 91% because the bridge only resolves Norwegian fishing vessels via fartøyregisteret. Cargo, tanker, passenger, and foreign vessels return NULL. To get the ship name or type for any MMSI, join to `ais/raw/statinfo/` on mmsi = mmsino.

**H3 resolution 8**: ~460m edge length, ~0.74 km² per cell. Every position gets one. All spatial queries become integer equality/set membership tests. 213K distinct cells covered in one day across the Norwegian EEZ.

## Stage 2: derive_voyages.R → `ais/gold/voyages_maru/`

Consumes: parsed positions + NSR statinfo (ship_type for fishing/non-fishing classification).

### Phase classification (SOG-only, no nav_status)

| Phase | Rule | Typical share |
|---|---|---|
| `fishing` | NSR ship type = Fisk AND 0.3 < SOG < 5 kn | 10% overall, 49% for fishing vessels |
| `node` | SOG ≤ 0.3 kn | 17% |
| `manoeuvring` | SOG ≤ 3 kn (non-fishing, or fishing above threshold) | 13% |
| `cruising` | SOG > 3 kn | 60% |

### Sail-ID assignment

A new sail begins when a vessel transitions from `node` to any non-stopped phase. Each sail gets a unique ID: `{mmsi}_{date}_{sequence}`. ~70K segments per day fleet-wide, ~7K for fishing vessels.

### Voyage schema (21 columns)

```
mmsi, sail_id, n_positions, start_time, end_time, mean_sog, max_sog, mean_cog,
start_lon, start_lat, end_lon, end_lat, start_h3, end_h3, n_h3_cells,
pct_fishing, pct_cruising, shipname, shiptypenor, orgnr, is_fishing
```

**`n_h3_cells`** = count of distinct H3 cells traversed in this sail. Discriminates trawling (tight circles → 50-150 cells) from transit (straight line → 500+ cells). This is a spatial extent proxy without computing convex hulls.

## GCS layout

```
gs://sondre_brreg_data/ais/
├── parsed/positions/year=Y/month=M/day=D.parquet    (14 cols, Layer 1)
├── parsed/_checkpoint/parser.json
└── gold/
    ├── voyages_maru/year=Y/month=M/day=D.parquet     (21 cols, Layer 3)
    ├── voyages_kystverket/{date}.parquet              (Kystverket pre-computed + orgnr)
    ├── fleet_panel.parquet                            (cross-source gold)
    └── capacity_utilization.parquet                   (FDIR-format aggregates)
```

## Join keys

| Target | Key | Notes |
|---|---|---|
| NSR statinfo | `mmsi = mmsino` | Name, type, flag, dimensions |
| Fartøyregisteret | Via NSR `callsign = radio_call_sign` | Only way to get orgnr |
| Fangstdata | No direct join — both join to fartøyregisteret via callsign | |
| BarentsWatch live | `mmsi` | Same vessel, different temporal resolution |

## Tech

R + DuckDB on the r-base Docker image. DuckDB h3 community extension for H3 indexing. Two R scripts executed sequentially per day by a Python entrypoint. Token refreshed per day iteration to avoid GCS credential expiry on long runs.

## Cloud Run

| Job | Schedule | Runtime |
|---|---|---|
| `kystverket-ais-parser` | backfill (manual) | ~3 min/day, 91 days = ~5h |
| `kystverket-ais-parser-daily` | 08:00 Oslo | ~3 min |
