from __future__ import annotations
from typing import List, Dict, Optional
import time
import uuid


def _now_ms() -> int:
    return int(time.time() * 1000)


def build_inference_payload(
    *,
    product_code: str,
    station_id: str,
    model_family: str,
    model_version: str,
    latency_ms: int,
    defects: List[Dict],
    raw_url: str,
    overlay_url: str,
    board_serial: Optional[str] = None,
    event_id: Optional[str] = None,
    ts_ms: Optional[int] = None,
    measures: Optional[Dict] = None,
    tiles: Optional[List[Dict]] = None,
    meta: Optional[Dict] = None,
    aql_mini_decision: Optional[str] = None,
) -> Dict:

    if event_id is None:
        event_id = str(uuid.uuid4())
    if ts_ms is None:
        ts_ms = _now_ms()

    if aql_mini_decision is None:
        aql_mini_decision = "FAIL" if len(defects) > 0 else "PASS"

    norm_defects: List[Dict] = []
    for d in defects or []:
        bbox = d.get("bbox", {})
        norm_defects.append({
            "cls": str(d.get("cls", "")),
            "score": float(d.get("score", 0.0)),
            "bbox": {
                "x": int(bbox.get("x", 0)),
                "y": int(bbox.get("y", 0)),
                "w": int(bbox.get("w", 0)),
                "h": int(bbox.get("h", 0)),
            },

            "mask_url": d.get("mask_url", None)
        })

    payload = {
        "event_id": event_id,
        "ts_ms": int(ts_ms),
        "product_code": str(product_code),
        "station_id": str(station_id),
        "board_serial": board_serial if board_serial is None else str(board_serial),

        "model_family": str(model_family),
        "model_version": str(model_version),
        "latency_ms": int(latency_ms),

        "aql_mini_decision": str(aql_mini_decision),

        "measures": measures if measures is not None else {
            "trace_width_um": None,
            "clearance_um": None,
            "pad_offset_um": None
        },

        "defects": norm_defects,

        "image_urls": {
            "raw_url": str(raw_url),
            "overlay_url": str(overlay_url),
            "tiles": tiles or []
        },

        "meta": meta or {"capture_id": None, "notes": None},
    }
    return payload
