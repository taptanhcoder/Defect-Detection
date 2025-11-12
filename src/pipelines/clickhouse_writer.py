# src/pipelines/clickhouse_writer.py
from __future__ import annotations
from typing import Dict, Any, List, Optional
from pathlib import Path
import logging, time, json, threading, os
from urllib.parse import urlparse
import requests

try:
    import yaml
except Exception:
    yaml = None 

log = logging.getLogger("aoi.clickhouse_writer")
DEFAULT_HTTP_URL = "http://localhost:8123"


def _safe_read_yaml(path: str | Path) -> Dict[str, Any]:
    p = Path(path).resolve()
    if not p.exists():
        raise FileNotFoundError(f"streaming.yaml not found: {p}")
    if yaml is None:
        # Không có pyyaml -> trả rỗng để dùng ENV/mặc định
        log.warning("PyYAML not installed; falling back to ENV/defaults for ClickHouse config.")
        return {}
    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        return {}
    return data


def _build_http_url(cfg_clickhouse: Dict[str, Any]) -> str:

    env_url = os.getenv("CLICKHOUSE_URL") or os.getenv("CLICKHOUSE_HTTP_URL")
    if env_url:
        return env_url.rstrip("/")


    http_url = (cfg_clickhouse.get("http_url") or "").strip()
    if http_url:
        return str(http_url).rstrip("/")

    host = os.getenv("CLICKHOUSE_HOST", str(cfg_clickhouse.get("host", "localhost")))
    port = int(os.getenv("CLICKHOUSE_PORT", str(cfg_clickhouse.get("port", 8123))))
    secure = os.getenv("CLICKHOUSE_SECURE", str(cfg_clickhouse.get("secure", "0"))).lower() in ("1", "true", "yes")
    scheme = "https" if secure else "http"
    return f"{scheme}://{host}:{port}".rstrip("/")


class ClickHouseWriter:


    REQUIRED_FIELDS = [
        "ts_ms", "event_id", "product_code", "station_id",
        "model_family", "model_version", "latency_ms",
        "aql_mini_decision",  
        "defect_count",      
        "image_overlay_url", 
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
        timeout: float = 15.0,
    ):
        self.http_url = http_url.rstrip("/") if http_url else DEFAULT_HTTP_URL
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.bulk_max_rows = int(bulk_max_rows or 0)
        self.bulk_max_seconds = float(bulk_max_seconds or 0.0)
        self.timeout = float(timeout or 15.0)

        u = urlparse(self.http_url)
        if not u.scheme or not u.scheme.startswith("http"):
            raise ValueError(f"Invalid clickhouse http_url: {self.http_url}")

        self._buf: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._last_flush = time.monotonic()


    def add(self, row: Dict[str, Any]) -> None:

        self._add_or_buffer(row)

    def add_row(self, row: Dict[str, Any]) -> None:

        self._add_or_buffer(row)

    def append(self, row: Dict[str, Any]) -> None:
 
        self._add_or_buffer(row)


    def insert_inspection(self, record: Dict[str, Any]) -> None:
        self._add_or_buffer(record)

    def flush(self) -> int:

        with self._lock:
            if not self._buf:
                return 0
            rows = self._buf
            self._buf = []
            self._last_flush = time.monotonic()

        self._insert_many_http(rows)
        return len(rows)

    def healthy(self) -> bool:

        try:
            params = {"query": "SELECT 1", "database": self.database}
            auth = (self.user, self.password) if (self.user or self.password) else None
            r = requests.get(self.http_url, params=params, auth=auth, timeout=min(self.timeout, 5.0))
            return r.status_code == 200 and r.text.strip() == "1"
        except Exception as e:
            log.warning("ClickHouse healthy() failed: %s", e)
            return False


    def _add_or_buffer(self, row: Dict[str, Any]) -> None:
        rec = self._validate_and_cast(row)

        if self.bulk_max_rows > 0 or self.bulk_max_seconds > 0:
            to_flush: Optional[List[Dict[str, Any]]] = None
            with self._lock:
                self._buf.append(rec)
                now = time.monotonic()

                if self.bulk_max_rows and len(self._buf) >= self.bulk_max_rows:
                    to_flush = self._buf
                    self._buf = []
                    self._last_flush = now

                elif self.bulk_max_seconds and (now - self._last_flush) >= self.bulk_max_seconds:
                    to_flush = self._buf
                    self._buf = []
                    self._last_flush = now
            if to_flush:
                self._insert_many_http(to_flush)
        else:

            self._insert_many_http([rec])

    def _insert_many_http(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return


        ndjson_lines = []
        for r in rows:
            item = {k: r.get(k) for k in self._COLS}
            ndjson_lines.append(json.dumps(item, ensure_ascii=False))
        ndjson = "\n".join(ndjson_lines) + "\n"

        sql = f"INSERT INTO {self.database}.{self.table} ({', '.join(self._COLS)}) FORMAT JSONEachRow"
        params = {"query": sql, "database": self.database}
        auth = (self.user, self.password) if (self.user or self.password) else None

        resp = requests.post(
            self.http_url, params=params, data=ndjson.encode("utf-8"),
            auth=auth, timeout=self.timeout
        )
        if resp.status_code != 200:
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

        # Optional
        bs = out.get("board_serial")
        out["board_serial"] = None if bs in (None, "", "null") else str(bs)

        out["model_family"] = str(out["model_family"])
        out["model_version"] = str(out["model_version"])
        out["latency_ms"] = int(out["latency_ms"])

        # Quyết định
        out["aql_mini_decision"] = str(out["aql_mini_decision"])
        if "aql_final_decision" not in out or out["aql_final_decision"] in (None, "", "null"):
 
            out["aql_final_decision"] = out["aql_mini_decision"]
        else:
            out["aql_final_decision"] = str(out["aql_final_decision"])

        fr = out.get("fail_reason")
        out["fail_reason"] = None if fr in (None, "", "null") else str(fr)

        out["defect_count"] = int(out["defect_count"])

        dj = out.get("defects_json")
        if dj is None:
            out["defects_json"] = "[]"
        elif isinstance(dj, (list, dict)):
            out["defects_json"] = json.dumps(dj, ensure_ascii=False)
        elif isinstance(dj, str):

            out["defects_json"] = dj
        else:

            out["defects_json"] = json.dumps(dj, ensure_ascii=False)

        out["image_overlay_url"] = str(out["image_overlay_url"])
        iru = out.get("image_raw_url")
        out["image_raw_url"] = str(iru) if iru not in (None, "", "null") else out["image_overlay_url"]

        return out


    @classmethod
    def from_yaml(
        cls,
        path: str | Path,
        project_root: Optional[str] = None,
        bulk_max_rows: int = 0,
        bulk_max_seconds: float = 0.0,
    ) -> "ClickHouseWriter":

        cfg = _safe_read_yaml(path)
        ch = (cfg.get("clickhouse") or {}) if isinstance(cfg, dict) else {}

        http_url = _build_http_url(ch)

        user = os.getenv("CLICKHOUSE_USER", str(ch.get("user", "default")))
        password = os.getenv("CLICKHOUSE_PASSWORD", str(ch.get("password", "")))
        database = os.getenv("CLICKHOUSE_DATABASE", str(ch.get("database", "aoi")))
        table = os.getenv("CLICKHOUSE_TABLE", str(ch.get("table", "aoi_inspections")))

        env_bulk_rows = os.getenv("CLICKHOUSE_BULK_ROWS")
        env_bulk_secs = os.getenv("CLICKHOUSE_BULK_SECONDS")
        if env_bulk_rows is not None:
            bulk_max_rows = int(env_bulk_rows)
        elif bulk_max_rows == 0:
            bulk_max_rows = int((ch.get("bulk", {}) or {}).get("max_rows", 0))

        if env_bulk_secs is not None:
            bulk_max_seconds = float(env_bulk_secs)
        elif bulk_max_seconds == 0.0:
            bulk_max_seconds = float((ch.get("bulk", {}) or {}).get("max_seconds", 0.0))

        return cls(
            http_url=http_url,
            database=database,
            user=user,
            password=password,
            table=table,
            bulk_max_rows=bulk_max_rows,
            bulk_max_seconds=bulk_max_seconds,
        )
