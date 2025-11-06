
from __future__ import annotations
from typing import Dict, Any, List
from pathlib import Path
import logging, time, json, threading
from urllib.parse import urlparse
import yaml, requests

log = logging.getLogger("aoi.clickhouse_writer")

def load_streaming_config(path: str | Path) -> Dict[str, Any]:
    p = Path(path).resolve()
    if not p.exists():
        raise FileNotFoundError(f"streaming.yaml not found: {p}")
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}

class ClickHouseWriter:

    REQUIRED_FIELDS = [
        "ts_ms", "event_id", "product_code", "station_id",
        "model_family", "model_version", "latency_ms",
        "aql_mini_decision", "aql_final_decision",
        "defect_count", "defects_json",
        "image_overlay_url", "image_raw_url",
    ]

    _COLS = [
        "ts_ms", "event_id", "product_code", "station_id", "board_serial",
        "model_family", "model_version", "latency_ms",
        "aql_mini_decision", "aql_final_decision", "fail_reason",
        "defect_count", "defects_json",
        "image_overlay_url", "image_raw_url",
    ]

    def __init__(
        self,
        http_url: str,
        database: str = "aoi",
        user: str = "default",
        password: str = "",
        table: str = "aoi_inspections",
        bulk_max_rows: int = 0,
        bulk_max_seconds: float = 0.0,
        timeout: float = 10.0,
    ):
        self.http_url = http_url.rstrip("/")
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.bulk_max_rows = int(bulk_max_rows)
        self.bulk_max_seconds = float(bulk_max_seconds)
        self.timeout = timeout

        # kiểm tra endpoint sớm (không bắt buộc)
        u = urlparse(self.http_url)
        if not u.scheme.startswith("http"):
            raise ValueError(f"Invalid clickhouse http_url: {self.http_url}")

        self._buffer: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._last_flush = time.monotonic()

    # ---------- public ----------
    def insert_inspection(self, record: Dict[str, Any]) -> None:
        rec = self._validate_and_cast(record)
        if self.bulk_max_rows > 0 or self.bulk_max_seconds > 0:
            self._buffered(rec)
        else:
            self._insert_many_http([rec])

    def flush(self) -> None:
        with self._lock:
            if not self._buffer:
                return
            rows = self._buffer
            self._buffer = []
        self._insert_many_http(rows)

    # ---------- internals ----------
    def _buffered(self, rec: Dict[str, Any]) -> None:
        rows = None
        with self._lock:
            self._buffer.append(rec)
            now = time.monotonic()
            if self.bulk_max_rows and len(self._buffer) >= self.bulk_max_rows:
                rows = self._buffer; self._buffer = []; self._last_flush = now
            elif self.bulk_max_seconds and (now - self._last_flush) >= self.bulk_max_seconds:
                rows = self._buffer; self._buffer = []; self._last_flush = now
        if rows:
            self._insert_many_http(rows)

    def _insert_many_http(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        # Chuẩn bị NDJSON theo JSONEachRow
        ndjson = "\n".join(json.dumps({k: r.get(k) for k in self._COLS}, ensure_ascii=False) for r in rows) + "\n"
        sql = f"INSERT INTO {self.database}.{self.table} ({', '.join(self._COLS)}) FORMAT JSONEachRow"

        params = {"query": sql, "database": self.database}
        auth = (self.user, self.password) if (self.user or self.password) else None
        resp = requests.post(self.http_url, params=params, data=ndjson.encode("utf-8"),
                             auth=auth, timeout=self.timeout)
        if resp.status_code != 200:
            # Không nuốt lỗi: báo đầy đủ để debug
            raise RuntimeError(f"ClickHouse insert failed {resp.status_code}: {resp.text}")

    def _validate_and_cast(self, r: Dict[str, Any]) -> Dict[str, Any]:
        for f in self.REQUIRED_FIELDS:
            if f not in r:
                raise ValueError(f"Missing required field: {f}")

        out = dict(r)
        out["ts_ms"] = int(out["ts_ms"])
        out["event_id"] = str(out["event_id"])
        out["product_code"] = str(out["product_code"])
        out["station_id"] = str(out["station_id"])
        out["board_serial"] = (None if out.get("board_serial") in (None, "", "null") else str(out.get("board_serial")))
        out["model_family"] = str(out["model_family"])
        out["model_version"] = str(out["model_version"])
        out["latency_ms"] = int(out["latency_ms"])
        out["aql_mini_decision"] = str(out["aql_mini_decision"])
        out["aql_final_decision"] = str(out["aql_final_decision"])
        out["fail_reason"] = (None if out.get("fail_reason") in (None, "", "null") else str(out.get("fail_reason")))
        out["defect_count"] = int(out["defect_count"])

        dj = out.get("defects_json")
        if isinstance(dj, (list, dict)):
            out["defects_json"] = json.dumps(dj, ensure_ascii=False)
        elif isinstance(dj, str):
            pass
        else:
            out["defects_json"] = json.dumps(dj, ensure_ascii=False)

        out["image_overlay_url"] = str(out["image_overlay_url"])
        out["image_raw_url"] = str(out["image_raw_url"])
        return out

    @classmethod
    def from_yaml(cls, path: str | Path, bulk_max_rows: int = 0, bulk_max_seconds: float = 0.0):
        cfg = load_streaming_config(path)
        ch = cfg.get("clickhouse", {}) or {}
        return cls(
            http_url=str(ch.get("http_url", "http://localhost:8123")),
            database=str(ch.get("database", "aoi")),
            user=str(ch.get("user", "default")),
            password=str(ch.get("password", "")),
            table=str(ch.get("table", "aoi_inspections")),
            bulk_max_rows=bulk_max_rows,
            bulk_max_seconds=bulk_max_seconds,
        )
