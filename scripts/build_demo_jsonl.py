#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import argparse
import uuid
import time
import json
import os
from pathlib import Path
from datetime import datetime
import random

def list_images(root: Path):
    exts = {".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff"}
    for p in sorted(root.rglob("*")):
        if p.suffix.lower() in exts and p.is_file():
            yield p

def to_file_url(p: Path) -> str:
    # normalize absolute path -> file://
    return "file://" + str(p.resolve())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--images", required=True, help="Folder contains input images")
    ap.add_argument("--out", required=True, help="Output JSONL path")
    ap.add_argument("--product", default="PCB_A")
    ap.add_argument("--station", default="ST01")
    ap.add_argument("--model-family", default="yolov8-det")
    ap.add_argument("--model-version", default="v20251104_0740392")
    ap.add_argument("--base-latency-ms", type=int, default=250, help="Mean latency for demo")
    ap.add_argument("--latency-jitter", type=int, default=120, help="+/- jitter for demo")
    ap.add_argument("--start-ts-ms", type=int, default=None,
                    help="Override start epoch ms; default: now()*1000")
    args = ap.parse_args()

    img_dir = Path(args.images)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # time seed
    now_ms = int(time.time() * 1000) if args.start_ts_ms is None else int(args.start_ts_ms)

    written = 0
    with out_path.open("w", encoding="utf-8") as fout:
        for i, img_path in enumerate(list_images(img_dir), start=1):
            event_id = str(uuid.uuid4())

            # demo timestamp: +500ms mỗi ảnh để có chuỗi thời gian đẹp
            ts_ms = now_ms + i * 500

            # latency demo (xấp xỉ base ± jitter)
            jitter = random.randint(-args.latency_jitter, args.latency_jitter)
            latency_ms = max(1, args.base_latency_ms + jitter)

            # defects demo: nếu tên file chứa “ok” coi như không lỗi, còn lại để list rỗng
            # (bạn có thể cải theo quy ước tên ảnh của dataset)
            has_defect = any(k in img_path.stem.lower() for k in ["defect_", "missing", "short", "bridge"])
            defects = [] if not has_defect else [{
                "type": "demo_defect",
                "bbox": [100, 100, 200, 200],
                "score": 0.85
            }]

            aql_mini = "FAIL" if has_defect else "PASS"
            aql_final = aql_mini  # demo: coi như quyết định cuối trùng mini

            # Image URLs
            raw_url = to_file_url(img_path)

            # Overlay path (chỉ là đường dẫn "đẹp" để ClickHouse/Grafana có string—không cần tồn tại)
            dt = datetime.utcfromtimestamp(ts_ms / 1000.0)
            overlay_rel = Path("data/processed/overlays") / args.product / f"{dt.year:04d}" / f"{dt.month:02d}" / f"{dt.day:02d}" / f"{event_id}_overlay.jpg"
            overlay_url = "file://" + str((Path.cwd() / overlay_rel).resolve())
            tiles = []  # demo không tạo tile

            record = {
                # thời gian & id
                "ts_ms": int(ts_ms),
                "event_id": event_id,

                # định danh
                "product_code": args.product,
                "station_id": args.station,
                "board_serial": f"DEMO_{i:04d}",

                # model info
                "model_family": args.model_family,
                "model_version": args.model_version,

                # hiệu năng & quyết định
                "latency_ms": int(latency_ms),
                "aql_mini_decision": aql_mini,
                "aql_final_decision": aql_final,
                "fail_reason": None if aql_final == "PASS" else "demo_fail",

                # defects (đủ cả hai dạng để thỏa loader + ClickHouse)
                "defects": defects,
                "defects_json": json.dumps(defects, ensure_ascii=False),
                "defect_count": len(defects),

                # images (vừa dạng object, vừa dạng phẳng cho ClickHouse)
                "image_urls": {
                    "raw_url": raw_url,
                    "overlay_url": overlay_url,
                    "tiles": tiles
                },
                "image_raw_url": raw_url,
                "image_overlay_url": overlay_url,

                # optional measures khớp schema demo trước đó
                "measures": {
                    "trace_width_um": None,
                    "clearance_um": None,
                    "pad_offset_um": None
                },

                # metadata thêm
                "meta": {
                    "capture_id": None,
                    "notes": None
                }
            }

            fout.write(json.dumps(record, ensure_ascii=False) + "\n")
            written += 1

    print(f"[OK] Wrote {written} records -> {out_path}")

if __name__ == "__main__":
    main()
