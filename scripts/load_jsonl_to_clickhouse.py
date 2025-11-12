
import os
import sys
import json
import argparse
from pathlib import Path
from typing import Dict, Any, Iterable, Optional

from pipelines.clickhouse_writer import ClickHouseWriter



def read_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for ln, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception as e:
                print(f"[WARN] invalid json at line {ln}: {e}", file=sys.stderr)



def _as_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default

def _ensure_str(v: Any, default: str = "") -> str:
    if v is None:
        return default
    return str(v)

def _json_dumps_safe(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return "[]"

def normalize_record(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:

    ts_ms = raw.get("ts_ms")
    event_id = raw.get("event_id")


    product_code = raw.get("product_code")
    station_id = raw.get("station_id")
    latency_ms = raw.get("latency_ms")
    aql_mini_decision = raw.get("aql_mini_decision")
    model_family = raw.get("model_family")
    model_version = raw.get("model_version")
    overlay_url_api = raw.get("overlay_url")
    defects_preview = raw.get("defects_preview")

    image_urls = raw.get("image_urls")
    defects_full = raw.get("defects")
    board_serial = raw.get("board_serial")


    required = {
        "ts_ms": ts_ms,
        "event_id": event_id,
        "product_code": product_code,
        "station_id": station_id,
        "latency_ms": latency_ms,
        "aql_mini_decision": aql_mini_decision,
        "model_family": model_family,
        "model_version": model_version,
    }
    missing = [k for k, v in required.items() if v is None]
    if missing:
        print(f"[WARN] invalid payload: missing field(s): {', '.join(missing)}", file=sys.stderr)
        return None

    # --- Decide image URLs ---
    image_overlay_url = ""
    image_raw_url = ""
    if isinstance(image_urls, dict):
        image_overlay_url = _ensure_str(image_urls.get("overlay_url"), "")
        image_raw_url = _ensure_str(image_urls.get("raw_url") or image_overlay_url, "")
    else:
        image_overlay_url = _ensure_str(overlay_url_api, "")
        image_raw_url = image_overlay_url  # fallback

    # --- Defects / defect_count / defects_json ---
    defects_obj = []
    if isinstance(defects_full, list):
        defects_obj = defects_full
        defect_count = len(defects_full)
    elif isinstance(defects_preview, list):
        defects_obj = defects_preview
        defect_count = len(defects_preview)
    else:
        defects_obj = []
        defect_count = _as_int(raw.get("defect_count"), 0)

    defects_json = _json_dumps_safe(defects_obj)

    # --- aql_final_decision: mirror mini unless provided ---
    aql_final_decision = _ensure_str(raw.get("aql_final_decision") or aql_mini_decision, aql_mini_decision)

    # --- Optional fields ---
    fail_reason = raw.get("fail_reason")
    board_serial = board_serial if (board_serial is None or isinstance(board_serial, str)) else str(board_serial)

    # --- Build row matching table columns ---
    row = {
        "ts_ms": _as_int(ts_ms),
        # "ts" column is DEFAULTed by ClickHouse
        "event_id": _ensure_str(event_id),
        "product_code": _ensure_str(product_code),
        "station_id": _ensure_str(station_id),
        "board_serial": board_serial if board_serial is None else _ensure_str(board_serial),
        "model_family": _ensure_str(model_family),
        "model_version": _ensure_str(model_version),
        "latency_ms": _as_int(latency_ms),
        "aql_mini_decision": _ensure_str(aql_mini_decision),
        "aql_final_decision": _ensure_str(aql_final_decision),
        "fail_reason": None if fail_reason is None else _ensure_str(fail_reason),
        "defect_count": _as_int(defect_count),
        "defects_json": defects_json,
        "image_overlay_url": _ensure_str(image_overlay_url),
        "image_raw_url": _ensure_str(image_raw_url),
        # "ingested_at" is DEFAULT now()
    }
    return row

# ------------------------------ Writer glue --------------------------- #

def writer_add_row(writer: Any, row: Dict[str, Any]) -> bool:
    """
    Add one row into the writer using any of the common method names.
    Returns True if added, False if the writer has no supported method.
    """
    if hasattr(writer, "add_row"):
        writer.add_row(row)
        return True
    if hasattr(writer, "append"):
        writer.append(row)
        return True
    if hasattr(writer, "add"):
        writer.add(row)
        return True
    return False

# ------------------------------ CLI main ------------------------------ #

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Load AOI JSONL into ClickHouse (aoi.aoi_inspections)")
    ap.add_argument("--stream-cfg", required=True, help="Path to streaming.yaml (ClickHouse writer config)")
    ap.add_argument("--jsonl", required=True, help="Path to JSONL file to load")
    ap.add_argument("--project-root", default=".", help="(unused; kept for compatibility/logging)")
    # NEW: explicit auth override
    ap.add_argument("--user", default=None, help="ClickHouse user (override ENV/YAML)")
    ap.add_argument("--password", default=None, help="ClickHouse password (override ENV/YAML)")
    return ap.parse_args()

def _apply_credentials(ck: Any, user: str, password: str) -> None:
    """
    Try common attribute names so we don't depend on the writer's exact field names.
    """
    for attr_user in ("user", "username", "login"):
        if hasattr(ck, attr_user):
            setattr(ck, attr_user, user)
    for attr_pwd in ("password", "passwd", "secret"):
        if hasattr(ck, attr_pwd):
            setattr(ck, attr_pwd, password)
    # also try a setter if available
    if hasattr(ck, "set_auth") and callable(getattr(ck, "set_auth")):
        try:
            ck.set_auth(user=user, password=password)  # type: ignore
        except Exception:
            pass

def main() -> int:
    args = parse_args()
    stream_cfg_path = str(Path(args.stream_cfg).resolve())
    jsonl_path = Path(args.jsonl).resolve()

    if not jsonl_path.exists():
        print(f"[ERROR] file not found: {jsonl_path}", file=sys.stderr)
        return 2

    # Build writer strictly with one-argument signature
    try:
        ck = ClickHouseWriter.from_yaml(stream_cfg_path)
    except TypeError as e:
        print(f"[ERROR] from_yaml signature mismatch: {e}", file=sys.stderr)
        return 3

    # Resolve credentials (CLI > ENV > empty)
    user = args.user or os.environ.get("CLICKHOUSE_USER", "default")
    password = args.password or os.environ.get("CLICKHOUSE_PASSWORD", "")

    _apply_credentials(ck, user, password)

    print(f"[INFO] target ClickHouse writer ready (user='{user}', cfg='{stream_cfg_path}')", file=sys.stderr)

    inserted = 0
    skipped = 0

    for raw in read_jsonl(jsonl_path):
        row = normalize_record(raw)
        if row is None:
            skipped += 1
            continue

        if not writer_add_row(ck, row):
            print("[ERROR] ClickHouseWriter has no add_row/append/add", file=sys.stderr)
            skipped += 1
            continue

        inserted += 1

    # Flush once at the end
    try:
        ck.flush()
    except Exception as e:
        print(f"[ERROR] flush failed: {e}", file=sys.stderr)
        return 4

    print(f"Done. Inserted={inserted}, Skipped={skipped}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
