from __future__ import annotations
from typing import Dict, Any, Optional, Tuple
import json
import logging
import time
from .rules import apply_aql 
from .spec_loader import SpecRepository
from src.pipelines.clickhouse_writer import ClickHouseWriter 

log = logging.getLogger("aoi.stream_processor.handlers")


def _validate_payload(p: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    required = [
        "event_id", "ts_ms", "product_code", "station_id",
        "model_family", "model_version", "latency_ms",
        "aql_mini_decision", "defects", "image_urls"
    ]
    for k in required:
        if k not in p:
            return False, f"missing field: {k}"
    if not isinstance(p["defects"], list):
        return False, "defects must be a list"
    iu = p.get("image_urls", {})
    if "overlay_url" not in iu:
        return False, "image_urls.overlay_url missing"
    if "raw_url" not in iu:

        pass
    return True, None


def handle_inference_result(
    payload: Dict[str, Any],
    ck_writer: ClickHouseWriter,
    spec_repo: SpecRepository,
    qc_event_producer=None,  
) -> None:
    """Xử lý 1 message inference_results."""
    ok, err = _validate_payload(payload)
    if not ok:
        log.error("invalid payload: %s", err)
        return

    product_code = str(payload["product_code"])
    station_id = str(payload["station_id"])
    event_id = str(payload["event_id"])
    ts_ms = int(payload["ts_ms"])
    defects = payload.get("defects") or []
    measures = payload.get("measures") or None
    spec = spec_repo.load_spec(product_code)
    final_decision, reason, severity = apply_aql(defects, measures, spec)
    iu = payload.get("image_urls", {}) or {}
    overlay_url = str(iu.get("overlay_url", ""))
    raw_url = str(iu.get("raw_url") or overlay_url)

    record = {
        "ts_ms": ts_ms,
        "event_id": event_id,
        "product_code": product_code,
        "station_id": station_id,
        "board_serial": payload.get("board_serial"),
        "model_family": str(payload.get("model_family", "")),
        "model_version": str(payload.get("model_version", "")),
        "latency_ms": int(payload.get("latency_ms", 0)),
        "aql_mini_decision": str(payload.get("aql_mini_decision", "")),
        "aql_final_decision": final_decision,
        "fail_reason": None if final_decision == "PASS" else reason,
        "defect_count": int(len(defects)),
        "defects_json": defects,  
        "image_overlay_url": overlay_url,
        "image_raw_url": raw_url,
    }


    ck_writer.insert_inspection(record)

    if qc_event_producer and (final_decision == "FAIL"):
        sev = severity or "MAJOR"
        qc_event = {
            "event_id": event_id,
            "ts_ms": ts_ms,
            "product_code": product_code,
            "station_id": station_id,
            "severity": sev,
            "reason": reason,
            "overlay_url": overlay_url,
            "defect_count": len(defects),
        }
        try:
            qc_event_producer.publish(qc_event)
        except Exception as e:
            log.warning("qc_event publish failed: %s", e)


    log.info(
        "ingested event_id=%s product=%s station=%s defects=%d final=%s severity=%s",
        event_id, product_code, station_id, len(defects), final_decision, severity or "-"
    )
