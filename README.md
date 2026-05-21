# kystverket-ais-parser

Pipeline (R + DuckDB) that converts the immutable observations from `kystverket-ais-collector` into enriched silver layers consumable by downstream models and dashboards.

## Inputs (from kystverket-ais-collector)

```
gs://sondre_brreg_data/ais/raw/
├── positions/year=YYYY/month=MM/day=DD/hour=HH.parquet   12-col API array
├── statinfo/year=YYYY/month=MM/day=DD.parquet            NSR vessel registry
├── voyages/year=YYYY/month=MM/day=DD.parquet             Kystverket pre-computed voyages
```

Cross-pipeline inputs:

```
gs://sondre_brreg_data/fiskeridir/parsed/v1/state/{date}.parquet
                                                       4,662 fishing vessels
                                                       orgnr ↔ radio_call_sign bridge
```

## Outputs

```
gs://sondre_brreg_data/ais/silver/
├── positions_decoded/year=YYYY/month=MM/day=DD/hour=HH.parquet
│       Same as raw, but with c4..c11 decoded to AIS fields
│       (sog, cog, ship_type, true_heading, nav_status, rate_of_turn, etc.)
│
├── vessel_registry/{date}.parquet
│       Per-MMSI per-day registry snapshot, combining NSR + Fartøyregisteret +
│       Brønnøysund. Resolves mmsi → callsign → orgnr where possible. Slowly-
│       changing dimension (SCD2) keyed on (mmsi, valid_from).
│
├── voyages_maru/year=YYYY/month=MM/day=DD/voyage_segments.parquet
│       MarU voyage segmentation derived from positions_decoded.
│       Phase labels: a/n/c/m/dp-o/f/p (anchor/node/cruise/manoeuvre/DP/fishing/shore-power)
│       sail_id assignment per the "is_stopped >50%" rule.
│       H3 res-8 indexing per row for spatial joins.
│       Re-implementation of github.com/Kystverket/maru rules; not the same
│       outputs (we don't compute emissions).
│
└── voyages_kystverket/{date}.parquet
        Lightly-cleaned passthrough of voyages/ with orgnr enrichment.
        For comparison against voyages_maru.
```

## Positions column decoding (VERIFIED)

Decoded May 2026 via cross-vessel statistical analysis + exact numerical verification against known vessels (AKKARFJORD 257023700, ARNOYTIND 257127870, BARENTS SEA 257388000). Every check passes with exact numerical match.

| col | name | type | description | sentinel |
|---|---|---|---|---|
| `mmsi` | mmsi | int64 | MMSI | — |
| `msgtime` | msgtime | string | ISO datetime (Oslo local time, not UTC) | — |
| `lon` | lon | double | longitude WGS84 | — |
| `lat` | lat | double | latitude WGS84 | — |
| `c4` | cog | double | course over ground, degrees | 360.0 = NA |
| `c5` | sog | double | speed over ground, knots (from AIS message) | 102.3 = NA |
| `c6` | msg_type | int64 | AIS message type (1, 3 = Class A; 18 = Class B) | — |
| `c7` | calc_speed | double | speed from consecutive position deltas, knots | -99 = NA |
| `c8` | delta_seconds | int64 | seconds since previous position for this MMSI | -99 = NA |
| `c9` | delta_meters | int64 | distance from previous position, meters | -99 = NA |
| `c10` | true_heading | int64 | true heading, degrees | 511 = NA |
| `c11` | rot | int64 | rate of turn, decoded °/min | -731 = NA/max left, -99 = Class B NA |

**Key finding**: kystdatahuset pre-computes `calc_speed`, `delta_seconds`, `delta_meters` server-side. These map directly to MarU's `delta_previous_point_seconds` and `distance_previous_point_meters`. The voyage-segmentation preprocessing is done for us — the parser needs only the phase-labelling logic (SOG thresholds + H3 spatial gates), not the point-to-point delta computation.

**Missing from kystdatahuset positions**: `nav_status` (0=under way, 1=anchored, 5=moored, 7=fishing) and `ship_type` (30=fishing, 70-79=cargo, 80-89=tanker) are NOT in the 12-column position array. Both are required by MarU rules. `ship_type` must be joined from statinfo (NSR); `nav_status` is structurally unavailable from kystdatahuset — it exists in the hais.kystverket.no Parquet schema (`status` column) and in raw NMEA Type 1/2/3 messages but kystdatahuset strips it. This means the MarU "anchored" and "fishing by nav_status" rules cannot be applied to kystdatahuset-sourced data. The parser must fall back to SOG-only phase labelling, which MarU itself also supports as a degraded mode.

Verification method: `calc_speed = (delta_meters / delta_seconds) × (3600/1852)` matches `c7` to 1 decimal place for every row tested. `delta_seconds` matches `(msgtime[i] - msgtime[i-1]).total_seconds()` exactly for consecutive same-MMSI rows.

Class B rows (msg_type=18) have `calc_speed=-99`, `delta_seconds=-99`, `delta_meters=-99`, `rot=-99` — the backend does not compute deltas for Class B positions (lower reporting cadence makes inter-position deltas less meaningful).

## Resolution chain (mmsi → orgnr)

Empirical on 2025-04-01 hour-00:
- 1 961 distinct MMSIs in EEZ
- 1 089 (55.5%) match NSR vessel registry
- 181 (9.2%) match Fartøyregisteret → orgnr (fishing vessels only)

The drop from NSR → Fartøyregisteret is expected: Fartøyregisteret only contains fishing vessels (~4 662 in total). For non-fishing vessels (cargo, passenger, tanker), orgnr resolution requires Sjøfartsdirektoratet ship register or commercial sources — not yet wired.

## MarU voyage segmentation rules

Source: `Method description MarU Rev. 0`, github.com/Kystverket/maru

| Phase | SOG rule | Spatial rule |
|---|---|---|
| Node (port/anchor) | not "underway" (MA SOG ≤ 0.3 kn) | ≤1 H3 r8 hex from shore |
| Manoeuvring | ≤ 3 kn | else |
| Cruising | > 3 kn | else |
| Fishing | < 5 kn | ship_type=30 AND ≥6 hex offshore AND ≥1 hex from port |
| DP-1 | ≤ 0.5 kn | offshore vessel; ≥9 hex from shore; ≤2 hex from oil installation |
| Anchorage | not "underway" | inside NCA-designated anchorage polygon |
| Shore-power | Node ≥ 2 hours | ≤1 hex from registered shore-power installation |

Voyage segment minimum length: 5 minutes.

"Is stopped" segment: Node phase > 50% of segment duration.

sail_id: one ID per run of non-"Is stopped" segments between two "Is stopped" segments.

## Status

Not yet implemented. Scaffolded only.
