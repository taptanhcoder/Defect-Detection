from __future__ import annotations
from typing import Any, Dict, Tuple
from pathlib import Path
import os
import yaml


def _env_or(default: str, env_key: str) -> str:
    v = os.getenv(env_key)
    return v if v not in (None, "") else default


def _resolve_path(p: str | None, project_root: Path) -> str | None:
    if not p:
        return p
    pp = Path(p)
    if pp.is_absolute():
        return str(pp)
    return str((project_root / pp).resolve())


def load_inference_config(path: str | Path, project_root: str | Path) -> Dict[str, Any]:
    cfg_path = Path(path).resolve()
    proj = Path(project_root).resolve()

    if not cfg_path.exists():
        raise FileNotFoundError(f"inference.yaml not found: {cfg_path}")

    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}

    # Kafka
    raw.setdefault("kafka", {})
    raw["kafka"]["brokers"] = _env_or(raw["kafka"].get("brokers", "kafka:9092"), "KAFKA_BROKERS")
    raw["kafka"]["schema_registry"] = _env_or(raw["kafka"].get("schema_registry", "http://schema-registry:8081"),
                                              "SCHEMA_REGISTRY_URL")
    # MinIO
    raw.setdefault("minio", {})
    raw["minio"]["endpoint"] = _env_or(raw["minio"].get("endpoint", "minio:9000"), "MINIO_ENDPOINT")
    raw["minio"]["access_key"] = _env_or(raw["minio"].get("access_key", ""), "MINIO_ACCESS_KEY")
    raw["minio"]["secret_key"] = _env_or(raw["minio"].get("secret_key", ""), "MINIO_SECRET_KEY")
    raw["minio"]["bucket"] = raw["minio"].get("bucket", "aoi")
    raw["minio"]["secure"] = bool(raw["minio"].get("secure", False))

    # App & features
    raw.setdefault("app", {})
    raw.setdefault("features", {})

    # ---- resolve template & models ----
    template_image = raw["app"].get("template_image")
    raw["app"]["template_image"] = _resolve_path(template_image, proj)

    models = raw.get("models", {})
    stations = (models or {}).get("stations", {}) or {}
    if not stations:
        raise ValueError("No stations configured under models.stations in inference.yaml")

    normalized_stations: Dict[str, Dict[str, Any]] = {}
    for sid, meta in stations.items():
        onnx = _resolve_path(meta.get("onnx"), proj)
        labels = _resolve_path(meta.get("labels"), proj)
        imgsz = int(meta.get("imgsz", 960))
        family = meta.get("family", "yolov8-det")

        if not onnx or not Path(onnx).exists():
            raise FileNotFoundError(f"ONNX not found for station '{sid}': {onnx}")
        if not labels or not Path(labels).exists():
            raise FileNotFoundError(f"labels.txt not found for station '{sid}': {labels}")

        normalized_stations[sid] = {
            "family": family,
            "onnx": onnx,
            "labels": labels,
            "imgsz": imgsz,
        }

    raw["models"]["stations"] = normalized_stations
    return raw
