import os, sys, json, time, glob
import requests

API = os.environ.get("AOI_API", "http://127.0.0.1:8000")
PRODUCT = os.environ.get("AOI_PRODUCT", "PCB_A")
STATION = os.environ.get("AOI_STATION", "ST01")
IMG_DIR = os.environ.get("AOI_IMG_DIR", "data/samples")

def guess_board_serial(path):

    base = os.path.basename(path)
    return os.path.splitext(base)[0]

def infer_one(img_path):
    url = f"{API}/v1/infer"
    with open(img_path, "rb") as f:
        files = {"image": (os.path.basename(img_path), f, "image/jpeg")}
        data = {
            "product_code": PRODUCT,
            "station_id": STATION,
            "board_serial": guess_board_serial(img_path),
        }
        r = requests.post(url, files=files, data=data, timeout=60)
    r.raise_for_status()
    return r.json()

def main():
    exts = ("*.jpg","*.jpeg","*.png","*.bmp","*.tif","*.tiff")
    paths = []
    for e in exts:
        paths.extend(glob.glob(os.path.join(IMG_DIR, e)))
    if not paths:
        print(f"[WARN] No images found in {IMG_DIR}")
        return
    print(f"[INFO] Found {len(paths)} images")
    for i,p in enumerate(sorted(paths)):
        try:
            resp = infer_one(p)
            eid = resp.get("event_id")
            print(f"[{i+1}/{len(paths)}] OK {os.path.basename(p)} -> event_id={eid}, decision={resp.get('aql_mini_decision')}, overlay={resp.get('overlay_url')}")
            time.sleep(0.1)
        except Exception as ex:
            print(f"[ERR] {p}: {ex}")
    print("[DONE] Feed complete. If producer=mock, results are in data/processed/inference_results.jsonl")

if __name__ == "__main__":
    main()
