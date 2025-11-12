from __future__ import annotations
from typing import Dict, Optional, Any
from pathlib import Path
import json
import logging
import os

from .config_loader import load_inference_config
from aoi.models import YoloV8DetONNX
from aoi.io import MinIOClient
from .producer import EventProducer

log = logging.getLogger("aoi.inference_api.deps")


_CFG: Dict[str, Any] | None = None
_FLAGS: Dict[str, Any] | None = None
_RUNNERS: Dict[str, YoloV8DetONNX] = {}
_MINIO: Optional[MinIOClient] = None
_MINIO_ENABLED: bool = True
_PRODUCER: Optional[EventProducer] = None
_IS_MOCK: bool = False
_PROJECT_ROOT: Path | None = None


def init(config_path: str | Path, project_root: str | Path = ".") -> None:
    global _CFG, _FLAGS, _RUNNERS, _MINIO, _MINIO_ENABLED, _PRODUCER, _IS_MOCK, _PROJECT_ROOT
    _PROJECT_ROOT = Path(project_root).resolve()
    _CFG = load_inference_config(config_path, _PROJECT_ROOT)
    _FLAGS = _CFG.get("features", {}) or {}


    mcfg = _CFG.get("minio", {}) or {}
    env_disable_minio = os.getenv("AOI_DISABLE_MINIO", "0").strip() in ("1", "true", "yes")
    cfg_disable_minio = not bool(mcfg.get("enabled", True))
    _MINIO_ENABLED = not (env_disable_minio or cfg_disable_minio)

    if _MINIO_ENABLED:
        _MINIO = MinIOClient(
            endpoint=str(mcfg.get("endpoint")),
            access_key=str(mcfg.get("access_key")),
            secret_key=str(mcfg.get("secret_key")),
            secure=bool(mcfg.get("secure", False)),
            default_bucket=str(mcfg.get("bucket", "aoi")),
        )
        log.info("MinIO enabled @ %s bucket=%s", mcfg.get("endpoint"), mcfg.get("bucket", "aoi"))
    else:
        _MINIO = None
        log.warning("MinIO is DISABLED (AOI_DISABLE_MINIO=%s, config.enabled=%s)",
                    env_disable_minio, mcfg.get("enabled", True))

    kcfg = _CFG.get("kafka", {}) or {}
    topic = str(kcfg.get("topic_results", "aoi.inference_results"))
    _PRODUCER = EventProducer(
        brokers=str(kcfg.get("brokers", "localhost:9092")),
        schema_registry_url=str(kcfg.get("schema_registry", "http://localhost:8081")),
        topic=topic,
        mock=None,
        jsonl_path=_PROJECT_ROOT / "data" / "processed" / "inference_results.jsonl",
    )
    _IS_MOCK = os.getenv("AOI_PRODUCER_MODE", "").lower().strip() == "mock"


    _RUNNERS.clear()
    stations = (_CFG.get("models", {}) or {}).get("stations", {}) or {}
    for sid, meta in stations.items():
        runner = YoloV8DetONNX(
            onnx_path=meta["onnx"],
            labels_path=meta["labels"],
            imgsz=int(meta.get("imgsz", 960)),
        )
        _RUNNERS[sid] = runner
        log.info("Loaded runner for station %s (imgsz=%s)", sid, runner.imgsz)

    log.info("deps.init done. stations=%s mock_producer=%s minio_enabled=%s",
             list(_RUNNERS.keys()), _IS_MOCK, _MINIO_ENABLED)


def shutdown() -> None:
    pass



def get_config() -> Dict[str, Any]:
    assert _CFG is not None
    return _CFG


def get_flags() -> Dict[str, Any]:
    return _FLAGS or {}


def get_runner(station_id: str) -> Optional[YoloV8DetONNX]:
    return _RUNNERS.get(station_id)


def get_station_model_cfg(station_id: str) -> Dict[str, Any]:
    models = (_CFG.get("models", {}) or {}).get("stations", {}) or {}
    return models.get(station_id, {})


def get_minio() -> Optional[MinIOClient]:
    return _MINIO


def minio_enabled() -> bool:
    return bool(_MINIO_ENABLED and _MINIO is not None)


def get_producer() -> EventProducer:
    assert _PRODUCER is not None
    return _PRODUCER


def is_mock_producer() -> bool:
    return _IS_MOCK


def get_model_version(model_cfg: Dict[str, Any]) -> str:
    onnx_path = Path(model_cfg["onnx"])
    mc_path = onnx_path.parent / "model_card.json"
    if mc_path.exists():
        try:
            data = json.loads(mc_path.read_text(encoding="utf-8"))
            ver = data.get("model_version")
            if ver:
                return str(ver)
        except Exception:
            pass
    return onnx_path.parent.name
