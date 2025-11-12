
CREATE DATABASE IF NOT EXISTS aoi;


CREATE TABLE IF NOT EXISTS aoi.aoi_inspections
(
    ts_ms              UInt64,
    ts                 DateTime DEFAULT toDateTime(intDiv(ts_ms, 1000)),

    event_id           String,
    product_code       String,
    station_id         String,
    board_serial       Nullable(String),

    model_family       String,
    model_version      String,

    latency_ms         UInt32,
    aql_mini_decision  String,
    aql_final_decision String,
    fail_reason        Nullable(String),

    defect_count       UInt16,
    defects_json       String,
    image_overlay_url  String,
    image_raw_url      String,

    ingested_at        DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (ts, product_code, station_id, event_id)
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS aoi.yield_5m
(
    t_5m         DateTime,
    product_code String,
    station_id   String,
    pass_cnt     UInt64,
    total_cnt    UInt64
)
ENGINE = SummingMergeTree
ORDER BY (t_5m, product_code, station_id)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS aoi.mv_yield_5m
TO aoi.yield_5m
AS
SELECT
    toStartOfFiveMinute(ts) AS t_5m,
    product_code,
    station_id,
    sum(aql_final_decision = 'PASS') AS pass_cnt,
    count() AS total_cnt
FROM aoi.aoi_inspections
GROUP BY t_5m, product_code, station_id;


CREATE TABLE IF NOT EXISTS aoi.latency_avg_5m
(
    t_5m           DateTime,
    product_code   String,
    station_id     String,
    sum_latency_ms UInt64,
    total_cnt      UInt64
)
ENGINE = SummingMergeTree
ORDER BY (t_5m, product_code, station_id)
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS aoi.mv_latency_avg_5m
TO aoi.latency_avg_5m
AS
SELECT
    toStartOfFiveMinute(ts) AS t_5m,
    product_code,
    station_id,
    sum(CAST(latency_ms AS UInt64)) AS sum_latency_ms,
    count() AS total_cnt
FROM aoi.aoi_inspections
GROUP BY t_5m, product_code, station_id;

DROP VIEW IF EXISTS aoi.v_latency_avg_5m;
CREATE VIEW aoi.v_latency_avg_5m AS
SELECT
    t_5m,
    product_code,
    station_id,
    sum_latency_ms / NULLIF(toFloat64(total_cnt), 0) AS avg_latency_ms
FROM aoi.latency_avg_5m;
