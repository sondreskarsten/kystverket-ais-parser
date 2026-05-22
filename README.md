# kystverket-ais-parser

Transforms raw AIS observations into enriched, queryable outputs. Two-stage pipeline: first decodes positions and resolves vessel identity (Layer 1), then derives voyage segments with activity classification (Layer 3 gold).

## What does it produce?

### Stage 1: Parsed positions (Layer 1)

Each AIS position row, decoded from the unnamed API array into named fields, indexed spatially with H3 hexagons, and keyed to a corporate identity (orgnr) where possible.

```
ais/parsed/positions/year=YYYY/month=MM/day=DD.parquet
```

14 columns: `mmsi, msgtime, lon, lat, cog, sog, msg_type, calc_speed, sec_prevpoint, dist_prevpoint, true_heading, rot, h3_r8, orgnr`

**The only cross-source join at this layer** is the orgnr bridge: `mmsi → NSR callsign → fartøyregisteret radio_call_sign → orgnr`. This join is admissible because it is the only way to attach a corporate identity to an AIS position. No vessel attributes, no activity labels, no derived metrics — those live in the gold layer.

**H3 resolution 8**: each position is tagged with its H3 hexagonal cell ID (~0.74 km² per cell). This converts all spatial queries from geometry computations to integer equality checks. "Which vessels were near Bergen port today?" becomes `WHERE h3_r8 IN ('612345...', '612346...')`.

### Stage 2: Voyage segments (Layer 3 gold)

Derived from parsed positions + NSR ship type. Classifies every position into an activity phase, groups consecutive non-stopped positions into voyages (sail segments), and summarizes each voyage.

```
ais/gold/voyages_maru/year=YYYY/month=MM/day=DD.parquet
```

21 columns per sail segment:

| Field | Description |
|---|---|
| `mmsi`, `sail_id` | Vessel + unique voyage identifier |
| `n_positions` | Position count in this segment |
| `start_time`, `end_time` | Voyage temporal bounds |
| `mean_sog`, `max_sog` | Speed statistics (knots) |
| `start_lon/lat`, `end_lon/lat` | Origin and destination coordinates |
| `start_h3`, `end_h3` | Origin and destination H3 cells |
| `n_h3_cells` | Spatial extent — number of distinct hex cells traversed |
| `pct_fishing`, `pct_cruising` | Activity phase distribution (percentage) |
| `shipname`, `shiptypenor`, `orgnr` | Vessel identity (from NSR + fartøyregisteret) |
| `is_fishing` | Boolean: NSR ship type = fishing vessel |

**Phase classification** (MarU-inspired, SOG-only mode):

| Phase | Rule | Interpretation |
|---|---|---|
| `fishing` | Fishing vessel + SOG 0.3–5 kn | Actively trawling or setting gear |
| `node` | SOG ≤ 0.3 kn | In port, at anchor, or stationary |
| `manoeuvring` | SOG ≤ 3 kn (non-fishing) | Harbour approach, docking, waiting |
| `cruising` | SOG > 3 kn | Transit between grounds or ports |

**Sail-ID assignment**: a new sail begins when a vessel transitions from `node` (stopped) to any non-stopped phase. This partitions each vessel's day into discrete trips. A trawler might have 3 sails: morning transit to grounds → fishing → return to port.

**`n_h3_cells` as activity fingerprint**: a trawler working one ground touches 50-150 cells; a cargo vessel transiting the coast touches 500+. This single integer separates operational patterns without geometry computation.

## Resolution chain (mmsi → orgnr)

```
AIS position (mmsi 258500000)
  → NSR statinfo (callsign "LGWH")
    → Fartøyregisteret (radio_call_sign "LGWH" → orgnr "979356749")
```

Empirical coverage (1 hour, Norwegian EEZ):
- 1,961 distinct MMSIs observed
- 1,089 (56%) resolve to NSR vessel registry
- 181 (9%) resolve all the way to a fartøyregisteret orgnr

The 9% hit rate is expected: fartøyregisteret contains only Norwegian fishing vessels (~4,665). The other 91% are foreign fishing vessels, cargo ships, tankers, and passenger vessels whose orgnr resolution requires the Sjøfartsdirektoratet ship register (not yet wired).

## GCS layout

```
gs://sondre_brreg_data/ais/
├── parsed/
│   ├── positions/year=YYYY/month=MM/day=DD.parquet   (Layer 1: 14 cols)
│   └── _checkpoint/parser.json
└── gold/
    ├── voyages_maru/year=YYYY/month=MM/day=DD.parquet (Layer 3: 21 cols)
    ├── voyages_kystverket/{date}.parquet               (passthrough + orgnr)
    ├── fleet_panel.parquet                             (cross-source materialization)
    └── capacity_utilization.parquet                    (FDIR-format benchmarks)
```

## Cloud Run

- **Backfill job**: `kystverket-ais-parser` (R + DuckDB on r-base image, 4CPU/16Gi)
- **Daily job**: `kystverket-ais-parser-daily` (60-day rolling window)
- **Schedule**: 08:00 Oslo daily (2h after collector)
- **Runtime**: ~3 min per day (download ~90s + R processing ~80s + upload ~20s)

## Upstream dependencies

← **kystverket-ais-collector**: positions, statinfo, voyages in `ais/raw/`
← **fiskeridir-parser**: `fiskeridir/parsed/v1/state/{date}.parquet` for the callsign→orgnr bridge
