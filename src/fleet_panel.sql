INSTALL httpfs; LOAD httpfs;

CREATE TABLE fartoy AS SELECT * FROM read_parquet('/tmp/gold/fartoy.parquet');

CREATE TABLE fangst AS
SELECT fartoy_id, radiokallesignal_seddel, fartoynavn, fangstar::INT AS year,
  siste_fangstdato, kvotetype, redskap_hovedgruppe, lengdegruppe,
  storste_lengde, bruttotonnasje_annen AS gt, besetning,
  art, art_hovedgruppe, rundvekt, fangstverdi,
  hovedomrade, kyst_hav_kode
FROM read_parquet('/tmp/gold/fangst_*.parquet')
WHERE fangstar::INT BETWEEN 2020 AND 2025;

CREATE TABLE nsr AS
SELECT DISTINCT mmsino, callsign FROM read_parquet('/tmp/gold/nsr.parquet')
WHERE callsign IS NOT NULL;

CREATE TABLE live AS
SELECT l.mmsi, l.name AS live_name, l.latitude AS live_lat, l.longitude AS live_lon,
  l.sog AS live_sog, l.nav_status AS live_nav_status, l.orgnr,
  l.captured_at AS live_captured_at
FROM read_parquet('/tmp/gold/latest.parquet') l;

CREATE TABLE finstat AS
SELECT LPAD(CAST(CAST(OffentligNr AS BIGINT) AS VARCHAR), 9, '0') AS orgnr,
  Regnskapsar AS year,
  TotaleInntekter / 1000.0 AS revenue_knok,
  SumDriftskostnader / 1000.0 AS costs_knok,
  Driftsresultat / 1000.0 AS ebitda_knok,
  Arsresultat / 1000.0 AS net_income_knok,
  SumEiendeler / 1000.0 AS total_assets_knok,
  SumEK / 1000.0 AS equity_knok,
  Lonnskostnad / 1000.0 AS wage_cost_knok
FROM read_parquet('/tmp/gold/finstat.parquet')
WHERE Regnskapsar BETWEEN 2020 AND 2024 AND RegnskapstypeKode = 'R';

CREATE TABLE fleet_panel AS
WITH catch_yearly AS (
  SELECT
    f.fartoy_id,
    f.radiokallesignal_seddel AS callsign,
    fr.orgnr,
    COALESCE(fr.name, f.fartoynavn) AS vessel_name,
    fr.length,
    fr.build_year,
    f.lengdegruppe AS length_group,
    f.year,
    f.redskap_hovedgruppe AS gear_type,
    round(sum(f.rundvekt) / 1000, 1) AS catch_tonnes,
    round(sum(CASE WHEN f.fangstverdi > 0 THEN f.fangstverdi ELSE 0 END) / 1000, 0) AS catch_value_knok,
    count(DISTINCT f.siste_fangstdato) AS landing_days,
    count(DISTINCT f.art) AS n_species,
    max(f.besetning) AS max_crew,
    max(f.gt) AS gt
  FROM fangst f
  LEFT JOIN fartoy fr ON f.radiokallesignal_seddel = fr.radio_call_sign
  GROUP BY 1,2,3,4,5,6,7,8,9
)
SELECT
  cy.*,
  fs.revenue_knok, fs.costs_knok, fs.ebitda_knok, fs.net_income_knok,
  fs.total_assets_knok, fs.equity_knok, fs.wage_cost_knok,
  live.live_lat, live.live_lon, live.live_sog, live.live_nav_status, live.live_captured_at
FROM catch_yearly cy
LEFT JOIN finstat fs ON cy.orgnr = fs.orgnr AND cy.year = fs.year
LEFT JOIN nsr ON cy.callsign = nsr.callsign
LEFT JOIN live ON nsr.mmsino = live.mmsi;

COPY fleet_panel TO '/tmp/gold/out/fleet_panel.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY);

COPY (
  SELECT
    year, length_group, gear_type,
    count(DISTINCT fartoy_id) AS n_vessels,
    round(avg(catch_tonnes), 1) AS avg_catch_tonnes,
    round(avg(catch_value_knok), 0) AS avg_catch_value_knok,
    round(avg(landing_days), 0) AS avg_landing_days,
    round(avg(revenue_knok), 0) AS avg_revenue_knok,
    round(avg(ebitda_knok), 0) AS avg_ebitda_knok,
    round(avg(net_income_knok), 0) AS avg_net_income_knok,
    round(avg(CASE WHEN gt > 0 THEN catch_tonnes / gt END), 2) AS catch_per_gt,
    round(avg(n_species), 1) AS avg_species_count,
    round(avg(max_crew), 1) AS avg_crew
  FROM fleet_panel
  WHERE orgnr IS NOT NULL
  GROUP BY year, length_group, gear_type
  ORDER BY year, length_group, gear_type
) TO '/tmp/gold/out/capacity_utilization.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY);
