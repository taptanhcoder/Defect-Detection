# src/ops_api/main.py
import os
import json
from datetime import timedelta

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from minio import Minio

CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://127.0.0.1:8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9002")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "aoi")


_minio_secure = MINIO_ENDPOINT.startswith("https://")
_minio_host = (
    MINIO_ENDPOINT.replace("http://", "").replace("https://", "").rstrip("/")
)
minio_client = Minio(
    _minio_host,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=_minio_secure,
)

app = FastAPI(title="AOI Ops API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)



def ch_select(sql: str):

    url = f"{CLICKHOUSE_URL}/"
    params = {"database": "aoi"}
    auth = (CLICKHOUSE_USER, CLICKHOUSE_PASSWORD) if CLICKHOUSE_PASSWORD else None

    r = requests.post(url, params=params, data=sql + " FORMAT JSONEachRow", auth=auth)
    if r.status_code == 401:
        raise HTTPException(status_code=500, detail="ClickHouse unauthorized")
    r.raise_for_status()

    rows = []
    for line in r.text.strip().splitlines():
        rows.append(json.loads(line))
    return rows


def ch_exists_table(table_name: str) -> bool:
    sql = f"EXISTS TABLE aoi.{table_name}"
    rows = ch_select(sql)
    if not rows:
        return False

    return bool(list(rows[0].values())[0])



class Inspection(BaseModel):
    ts_ms: int
    event_id: str
    product_code: str
    station_id: str
    board_serial: str | None = None
    model_family: str | None = None
    model_version: str | None = None
    latency_ms: int | None = None
    aql_mini_decision: str | None = None
    aql_final_decision: str | None = None
    fail_reason: str | None = None
    defect_count: int | None = None
    image_overlay_url: str | None = None
    image_raw_url: str | None = None
    defects_json: str | None = None



@app.get("/healthz")
def healthz():

    try:
        ch_ok = ch_exists_table("aoi_inspections")
    except Exception:
        ch_ok = False

    try:
        found = minio_client.bucket_exists(MINIO_BUCKET)
        minio_ok = bool(found)
    except Exception:
        minio_ok = False

    return {
        "status": "ok" if (ch_ok and minio_ok) else "degraded",
        "clickhouse": "ok" if ch_ok else "down",
        "minio": "ok" if minio_ok else "down",
    }


@app.get("/filters")
def get_filters():
    if not ch_exists_table("aoi_inspections"):
        return {"products": [], "stations": []}
    sql = """
    SELECT DISTINCT product_code FROM aoi.aoi_inspections
    """
    products = [r["product_code"] for r in ch_select(sql)]

    sql = """
    SELECT DISTINCT station_id FROM aoi.aoi_inspections
    """
    stations = [r["station_id"] for r in ch_select(sql)]
    return {"products": products, "stations": stations}


@app.get("/inspections/recent")
def inspections_recent(
    limit: int = Query(20, ge=1, le=500),
    product: str | None = None,
    station: str | None = None,
):
    if not ch_exists_table("aoi_inspections"):
        return {"items": [], "page": 1, "limit": limit}

    conds = []
    if product:
        conds.append(f"product_code = '{product}'")
    if station:
        conds.append(f"station_id = '{station}'")

    where_clause = ""
    if conds:
        where_clause = "WHERE " + " AND ".join(conds)

    sql = f"""
    SELECT
        ts_ms,
        event_id,
        product_code,
        station_id,
        board_serial,
        model_family,
        model_version,
        latency_ms,
        aql_mini_decision,
        aql_final_decision,
        fail_reason,
        defect_count,
        image_overlay_url,
        image_raw_url,
        defects_json
    FROM aoi.aoi_inspections
    {where_clause}
    ORDER BY ts_ms DESC
    LIMIT {limit}
    """
    rows = ch_select(sql)
    return {"items": rows, "page": 1, "limit": limit}


@app.get("/inspections/search")
def inspections_search(
    product: str | None = None,
    station: str | None = None,
    decision: str | None = None,
    page: int = 1,
    limit: int = 30,
    from_: str | None = Query(None, alias="from"),
    to: str | None = None,
):
    if not ch_exists_table("aoi_inspections"):
        return {"items": [], "page": page, "limit": limit}

    conds = []
    if product:
        conds.append(f"product_code = '{product}'")
    if station:
        conds.append(f"station_id = '{station}'")
    if decision:
        conds.append(f"aql_final_decision = '{decision.upper()}'")


    where_clause = ""
    if conds:
        where_clause = "WHERE " + " AND ".join(conds)

    offset = (page - 1) * limit

    sql = f"""
    SELECT
        ts_ms,
        event_id,
        product_code,
        station_id,
        board_serial,
        model_family,
        model_version,
        latency_ms,
        aql_mini_decision,
        aql_final_decision,
        fail_reason,
        defect_count,
        image_overlay_url,
        image_raw_url,
        defects_json
    FROM aoi.aoi_inspections
    {where_clause}
    ORDER BY ts_ms DESC
    LIMIT {limit} OFFSET {offset}
    """
    rows = ch_select(sql)
    return {"items": rows, "page": page, "limit": limit}


@app.get("/inspections/{event_id}")
def inspection_detail(event_id: str):
    if not ch_exists_table("aoi_inspections"):
        raise HTTPException(status_code=404, detail="not found")

    sql = f"""
    SELECT
        ts_ms,
        event_id,
        product_code,
        station_id,
        board_serial,
        model_family,
        model_version,
        latency_ms,
        aql_mini_decision,
        aql_final_decision,
        fail_reason,
        defect_count,
        image_overlay_url,
        image_raw_url,
        defects_json
    FROM aoi.aoi_inspections
    WHERE event_id = '{event_id}'
    ORDER BY ts_ms DESC
    LIMIT 1
    """
    rows = ch_select(sql)
    if not rows:
        raise HTTPException(status_code=404, detail="not found")

    item = rows[0]

    defects = []
    dj = item.get("defects_json")
    if dj:
        try:
            defects = json.loads(dj)
        except Exception:
            defects = []

    return {
        "item": item,
        "defects": defects,
        "variants": [],  
    }


@app.get("/media/presign")
def media_presign(key: str):

    try:
        url = minio_client.get_presigned_url(
            "GET",
            bucket_name=MINIO_BUCKET,
            object_name=key,
            expires=timedelta(hours=1),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"minio presign failed: {e}")

    return {"url": url, "overlay_url": url}
