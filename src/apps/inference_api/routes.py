from __future__ import annotations
import time, uuid, logging
from typing import Optional, List, Dict
from pathlib import Path

import numpy as np
import cv2
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse

from .schemas import InferRequestMeta, InferResponse, HealthzResponse, DefectItem
from . import deps
from aoi import (
    register_to_template, tile_960, merge_tiles, draw_overlay,
    quick_decision, build_inference_payload
)

router = APIRouter()
log = logging.getLogger("aoi.inference_api")


@router.get("/healthz", response_model=HealthzResponse)
async def healthz():
    ok_minio = "ok" if deps.minio_enabled() else ("disabled" if deps.get_minio() is None else "unknown")
    kafka_state = "mock" if deps.is_mock_producer() else ("ok" if deps.get_producer().healthy() else "down")
    return HealthzResponse(status="ok", minio=ok_minio, kafka=kafka_state, details={})


def _save_overlay_local(product_code: str, event_id: str, ts_ms: int, overlay_bgr) -> str:
    d = time.gmtime(ts_ms / 1000.0)
    rel = Path("data/processed/overlays") / product_code / f"{d.tm_year:04d}" / f"{d.tm_mon:02d}" / f"{d.tm_mday:02d}"
    rel.mkdir(parents=True, exist_ok=True)
    out = rel / f"{event_id}_overlay.jpg"
    ok = cv2.imwrite(str(out), overlay_bgr, [int(cv2.IMWRITE_JPEG_QUALITY), 90])
    if not ok:
        raise RuntimeError("Failed to write overlay image")
    return f"file://{out.resolve()}"


@router.post("/v1/infer", response_model=InferResponse)
async def infer(
    image: UploadFile = File(..., description="Ảnh PCB (jpg/png)"),
    product_code: str = Form(...),
    station_id: str = Form(...),
    board_serial: Optional[str] = Form(None),
):
    # 1) Parse metadata
    meta = InferRequestMeta(product_code=product_code, station_id=station_id, board_serial=board_serial)

    # 2) Nạp ảnh
    try:
        raw_bytes = await image.read()
        arr = np.frombuffer(raw_bytes, dtype=np.uint8)
        img_bgr = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if img_bgr is None:
            raise ValueError("cv2.imdecode returned None")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid image: {e}")

    cfg = deps.get_config()
    flags = deps.get_flags()

    # 3) Runner
    runner = deps.get_runner(meta.station_id)
    if runner is None:
        raise HTTPException(status_code=400, detail=f"station_id '{meta.station_id}' is not configured")

    t0 = time.perf_counter()

    # 4) Registration (optional)
    img_infer = img_bgr
    if flags.get("enable_registration"):
        tpl_path = (cfg.get("app", {}) or {}).get("template_image")
        if tpl_path:
            try:
                tpl = cv2.imread(tpl_path)
                if tpl is not None:
                    img_infer, _H = register_to_template(img_bgr, tpl)
            except Exception as e:
                log.warning("registration failed: %s", e)

    # 5) Tiling + predict
    tiles = tile_960(img_infer, tile_size=runner.imgsz, overlap=64)
    tile_preds: List[Dict] = []
    for t in tiles:
        dets_tile = runner.predict_tile(t["tile"])
        tile_preds.append({"xy0": t["xy0"], "dets": dets_tile})

    # 6) Merge
    defects = merge_tiles(tile_preds, iou_thres=0.5, per_class_nms=True)

    # 7) AQL mini
    decision = quick_decision(defects, measures=None, rules=None)

    # 8) Overlay & upload/save
    overlay_bgr = draw_overlay(img_infer, defects)
    event_id = str(uuid.uuid4())
    ts_ms = int(time.time() * 1000)

    raw_url = ""
    overlay_url = ""

    if deps.minio_enabled():
        minio = deps.get_minio()
        try:
            overlay_key = minio.make_overlay_key(meta.product_code, event_id, ts_ms)
            overlay_url = minio.put_image(overlay_key, overlay_bgr, return_presigned=True)
        except Exception as e:
            log.error("MinIO upload failed: %s", e)
    else:
        try:
            overlay_url = _save_overlay_local(meta.product_code, event_id, ts_ms, overlay_bgr)
        except Exception as e:
            log.error("Local overlay save failed: %s", e)

    latency_ms = int((time.perf_counter() - t0) * 1000)

    # 9) Payload để ghi DB/Kafka
    model_cfg = deps.get_station_model_cfg(meta.station_id)
    payload = build_inference_payload(
        product_code=meta.product_code,
        station_id=meta.station_id,
        model_family=model_cfg["family"],
        model_version=deps.get_model_version(model_cfg),
        latency_ms=latency_ms,
        defects=defects,
        raw_url=raw_url or overlay_url,
        overlay_url=overlay_url,
        board_serial=meta.board_serial,
        event_id=event_id,
        ts_ms=ts_ms,
        aql_mini_decision=decision,
    )

    try:
        deps.get_producer().publish(payload)
    except Exception as e:
        log.error("Publish failed: %s", e)

    # 10) Response cho client
    preview = [DefectItem(**d) for d in (defects[:3] if defects else [])]
    resp = InferResponse(
        ts_ms=ts_ms,                  
        event_id=event_id,
        aql_mini_decision=decision,
        overlay_url=overlay_url,
        defect_count=len(defects),
        latency_ms=latency_ms,
        product_code=meta.product_code,
        station_id=meta.station_id,
        model_family=model_cfg["family"],
        model_version=deps.get_model_version(model_cfg),
        defects_preview=preview or None,
    )
    return JSONResponse(status_code=200, content=resp.model_dump())
