#!/usr/bin/env python3
from __future__ import annotations
"""
Nạp file data/processed/inference_results.jsonl (do Inference API mock) vào ClickHouse,
áp dụng AQL chuẩn qua handlers giống hệt stream_processor.

Cách dùng:
  python scripts/load_jsonl_to_clickhouse.py \
    --stream-cfg configs/streaming.yaml \
    --jsonl data/processed/inference_results.jsonl \
    [--project-root .]

Tip nếu không sửa file: cũng có thể chạy với PYTHONPATH=.
"""

import argparse
import json
import os
import sys
from pathlib import Path

def _ensure_project_root_on_path(project_root: Path):
    project_root = project_root.resolve()
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream-cfg", default="configs/streaming.yaml",
                    help="Đường dẫn configs/streaming.yaml để lấy ClickHouse URL & spec loader")
    ap.add_argument("--jsonl", default="data/processed/inference_results.jsonl",
                    help="File JSONL do Inference API (mock producer) tạo ra")
    ap.add_argument("--project-root", default=".", help="Project root chứa thư mục src/")
    args = ap.parse_args()

    project_root = Path(args.project_root)
    _ensure_project_root_on_path(project_root)

    # Import sau khi đã thêm project_root vào sys.path
    try:
        from src.pipelines.clickhouse_writer import ClickHouseWriter, load_streaming_config as load_stream_cfg
        from src.apps.stream_processor.spec_loader import SpecRepository
        from src.apps.stream_processor.handlers import handle_inference_result
    except Exception as e:
        print(f"[ERR] Import project modules failed: {e}", file=sys.stderr)
        print("=> Kiểm tra lại --project-root hoặc chạy: PYTHONPATH=$(pwd) python scripts/load_jsonl_to_clickhouse.py ...", file=sys.stderr)
        return 2

    stream_cfg_path = Path(args.stream_cfg)
    jsonl_path = Path(args.jsonl)

    if not jsonl_path.exists():
        print(f"[ERR] JSONL not found: {jsonl_path}", file=sys.stderr)
        return 2

    # Đọc cấu hình streaming (ClickHouse/minio/specs)
    cfg = load_stream_cfg(stream_cfg_path)

    # ClickHouse writer (bật bulk cho nhanh)
    ck = ClickHouseWriter.from_yaml(stream_cfg_path, bulk_max_rows=500, bulk_max_seconds=2.0)

    # Spec repo (đọc theo cấu hình: local/minio). Ở demo: local configs/specs
    spec_repo = SpecRepository.from_yaml(stream_cfg_path)

    cnt_ok = 0
    cnt_err = 0

    with jsonl_path.open("r", encoding="utf-8") as f:
        for ln_no, ln in enumerate(f, start=1):
            line = ln.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception as e:
                cnt_err += 1
                print(f"[WARN] L{ln_no}: JSON decode failed: {e}", file=sys.stderr)
                continue

            try:
                handle_inference_result(
                    payload=payload,
                    ck_writer=ck,
                    spec_repo=spec_repo,
                    qc_event_producer=None,  # offline: không cần alert
                )
                cnt_ok += 1
            except Exception as e:
                cnt_err += 1
                print(f"[WARN] L{ln_no}: handle_inference_result failed: {e}", file=sys.stderr)


    ck.flush()

    print(f"Done. Inserted={cnt_ok}, Skipped={cnt_err}")
    return 0 if cnt_err == 0 else 1

if __name__ == "__main__":
    raise SystemExit(main())
