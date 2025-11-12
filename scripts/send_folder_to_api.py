#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, time, json
from pathlib import Path
import requests

def list_images(root: Path):
    exts = {".jpg",".jpeg",".png",".bmp",".tif",".tiff"}
    for p in sorted(root.rglob("*")):
        if p.suffix.lower() in exts and p.is_file():
            yield p

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--images", required=True, help="Folder ảnh input")
    ap.add_argument("--meta", required=False, default="configs/demo_metadata.yaml", help="YAML metadata đơn giản (product_code, station_id, ...)")
    ap.add_argument("--api", default="http://127.0.0.1:8000/v1/infer")
    ap.add_argument("--out-jsonl", default="data/processed/inference_results.jsonl")
    ap.add_argument("--sleep-ms", type=int, default=200, help="nghỉ giữa các request để giả lập realtime")
    args = ap.parse_args()


    meta = {}
    try:
        import yaml
        meta = yaml.safe_load(Path(args.meta).read_text(encoding="utf-8"))
    except Exception:
        meta = {
            "product_code": "PCB_A",
            "station_id": "ST01",
        }

    product = meta.get("product_code") or meta.get("product") or "PCB_A"
    station  = meta.get("station_id") or "ST01"

    out_path = Path(args.out_jsonl)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fout = out_path.open("a", encoding="utf-8")

    sent = 0
    for i, img in enumerate(list_images(Path(args.images)), start=1):
        files = {"image": (img.name, img.read_bytes(), "application/octet-stream")}
        data = {
            "product_code": product,
            "station_id": station,
            "board_serial": f"API_{i:04d}",
        }
        try:
            r = requests.post(args.api, data=data, files=files, timeout=60)
            r.raise_for_status()
            resp = r.json()
            line = resp.get("payload", resp)
            fout.write(json.dumps(line, ensure_ascii=False) + "\n")
            sent += 1
            print(f"[OK] {img.name} -> event_id={line.get('event_id')} decision={line.get('aql_mini_decision')}")
        except Exception as e:
            print(f"[ERR] {img.name}: {e}")
        time.sleep(args.sleep_ms/1000.0)

    fout.close()
    print(f"Done. Appended {sent} records -> {out_path}")

if __name__ == "__main__":
    main()
