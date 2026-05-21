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

## Decoding plan for positions c4..c11

Provisional (to be verified against ITU-R M.1371 + cross-checks with
BarentsWatch `/v1/combined` and Kystverket NMEA TCP):

| col | name | type | source |
|---|---|---|---|
| `c4` | `sog` | float | speed over ground (kn), 102.3 = NA |
| `c5` | `unknown_1` | float | hypothesis: ?; probe |
| `c6` | `ship_type` | int | ITU ship-type code; 30 = fishing, 70-79 = cargo, 80-89 = tanker |
| `c7` | `unknown_2` | float | ? |
| `c8` | `cog` | int | course over ground (deg × 1?) |
| `c9` | `nav_status` | int | 0=under way, 1=anchored, 5=moored, 7=fishing, 15=undefined |
| `c10` | `true_heading` | int | 0..359, 511=NA |
| `c11` | `rot` | int | rate of turn signed |

Validation: pick a vessel with known characteristics (e.g. MMSI 258500000 = RICHARD WITH, Hurtigruten ferry), pull simultaneously from BarentsWatch combined and Kystverket positions, compare field values.

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
