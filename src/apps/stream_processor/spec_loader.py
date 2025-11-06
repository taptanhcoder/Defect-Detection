from __future__ import annotations
from typing import Dict, Any, Optional
from pathlib import Path
import json
import time
import logging
import yaml

from aoi.io import MinIOClient  

log = logging.getLogger("aoi.stream_processor.spec_loader")


def load_streaming_config(path: str | Path) -> Dict[str, Any]:
    p = Path(path).resolve()
    if not p.exists():
        raise FileNotFoundError(f"streaming.yaml not found: {p}")
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


class SpecRepository:
    def __init__(
        self,
        mode: str = "local",               
        local_dir: str | Path = "configs/specs",
        minio_client: Optional[MinIOClient] = None,
        minio_bucket: Optional[str] = None,
        minio_prefix: str = "specs",
        ttl_seconds: int = 600             
    ):
        self.mode = mode
        self.local_dir = Path(local_dir)
        self.minio = minio_client
        self.minio_bucket = minio_bucket
        self.minio_prefix = minio_prefix.strip("/")
        self.ttl = int(ttl_seconds)

        self._cache: Dict[str, tuple[float, Dict[str, Any]]] = {}

    def _is_fresh(self, ts: float) -> bool:
        return (time.time() - ts) < self.ttl

    def _local_path(self, product_code: str) -> Path:
        return self.local_dir / f"{product_code}.json"

    def _minio_key(self, product_code: str) -> str:
        return f"{self.minio_prefix}/{product_code}.json"

    def load_spec(self, product_code: str) -> Dict[str, Any]:

        if product_code in self._cache:
            ts, data = self._cache[product_code]
            if self._is_fresh(ts):
                return data

        spec = None
        if self.mode == "local":
            p = self._local_path(product_code)
            if p.exists():
                try:
                    spec = json.loads(p.read_text(encoding="utf-8"))
                except Exception as e:
                    log.warning("Spec parse failed for %s: %s", p, e)
        elif self.mode == "minio":
            if self.minio is None or not self.minio_bucket:
                log.error("MinIO mode requires minio_client and bucket")
            else:
                key = self._minio_key(product_code)
                try:

                    log.warning("Spec mode=minio yêu cầu MinIOClient.get_object (chưa implement). Fallback local.")
                except Exception as e:
                    log.warning("MinIO get spec failed: %s", e)

        if spec is None:

            spec = {
                "banned_classes": [],
                "max_defects": 999999,
                "max_by_class": {},
                "thresholds": {},
                "severity_by_class": {}
            }

        self._cache[product_code] = (time.time(), spec)
        return spec

    @classmethod
    def from_yaml(cls, path: str | Path, minio_client: Optional[MinIOClient] = None):
        cfg = load_streaming_config(path)
        sc = cfg.get("specs", {}) or {}
        mode = sc.get("source", "local")
        local_dir = sc.get("local_dir", "configs/specs")
        bucket = None
        if mode == "minio":
            mcfg = cfg.get("minio", {}) or {}
            bucket = mcfg.get("bucket", "aoi")
        return cls(
            mode=mode,
            local_dir=local_dir,
            minio_client=minio_client,
            minio_bucket=bucket,
            minio_prefix=sc.get("prefix", "specs"),
            ttl_seconds=int(sc.get("ttl_seconds", 600)),
        )
