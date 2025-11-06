/* ===========================
   0) DATABASE
   =========================== */
CREATE DATABASE IF NOT EXISTS aoi;

/* ===========================
   1) FACT TABLE: aoi.aoi_inspections
   - Dùng cú pháp MergeTree cũ: ENGINE = MergeTree(date_column, (primary_key...), index_granularity)
   - KHÔNG dùng PARTITION BY / TTL / LowCardinality
   - Thêm cột ts (DateTime) DEFAULT từ ts_ms để dùng làm cột "date" bắt buộc cho MergeTree cũ
   =========================== */
CREATE TABLE IF NOT EXISTS aoi.aoi_inspections
(
    -- thời gian epoch ms
    ts_ms              UInt64,
    -- cột DateTime (bắt buộc cho MergeTree cũ)
    ts                 DateTime DEFAULT toDateTime(intDiv(ts_ms, 1000)),

    -- định danh
    event_id           String,
    product_code       String,
    station_id         String,
    board_serial       Nullable(String),

    -- model info
    model_family       String,
    model_version      String,

    -- hiệu năng/quyết định
    latency_ms         UInt32,
    aql_mini_decision  String,
    aql_final_decision String,
    fail_reason        Nullable(String),

    -- lỗi & ảnh
    defect_count       UInt16,
    defects_json       String,
    image_overlay_url  String,
    image_raw_url      String,

    -- thời điểm ghi
    ingested_at        DateTime DEFAULT now()
)
ENGINE = MergeTree(ts, (ts, product_code, station_id, event_id), 8192);

/* ===========================
   2) FPY 5 PHÚT (SummingMergeTree cũ)
   - Tính pass_cnt/total_cnt theo 5m
   =========================== */
CREATE TABLE IF NOT EXISTS aoi.yield_5m
(
    t_5m         DateTime,
    product_code String,
    station_id   String,
    pass_cnt     UInt64,
    total_cnt    UInt64
)
ENGINE = SummingMergeTree(t_5m, (t_5m, product_code, station_id), 8192);

-- MV đổ dữ liệu vào yield_5m
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

/* ===========================
   3) LATENCY AVG 5 PHÚT (đơn giản, không quantiles)
   =========================== */
CREATE TABLE IF NOT EXISTS aoi.latency_avg_5m
(
    t_5m           DateTime,
    product_code   String,
    station_id     String,
    sum_latency_ms UInt64,
    total_cnt      UInt64
)
ENGINE = SummingMergeTree(t_5m, (t_5m, product_code, station_id), 8192);

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

-- VIEW tiện truy vấn avg latency
DROP VIEW IF EXISTS aoi.v_latency_avg_5m;
CREATE VIEW aoi.v_latency_avg_5m AS
SELECT
    t_5m,
    product_code,
    station_id,
    sum_latency_ms / NULLIF(toFloat64(total_cnt), 0) AS avg_latency_ms
FROM aoi.latency_avg_5m;

/* ===========================
   4) PHÂN QUYỀN đơn giản (tuỳ chọn)
   =========================== */
CREATE ROLE IF NOT EXISTS aoi_ro;
GRANT SELECT ON aoi.* TO aoi_ro;

CREATE USER IF NOT EXISTS grafana IDENTIFIED WITH plaintext_password BY 'grafana_password_here';
GRANT aoi_ro TO grafana;
