from __future__ import annotations
import argparse, json, os, re, shutil, sys, time, hashlib
from pathlib import Path
from typing import List, Optional

try:
    import yaml
except Exception:
    yaml = None

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def read_names(names_yaml: Optional[Path], names_csv: Optional[str]) -> List[str]:
    if names_yaml:
        if yaml is None:
            raise RuntimeError("PyYAML not installed; either install pyyaml or use --names.")
        data = yaml.safe_load(Path(names_yaml).read_text(encoding="utf-8"))
        names = data.get("names")
        if not isinstance(names, list) or not names:
            raise ValueError(f"names not found/empty in {names_yaml}")
        return [str(n) for n in names]
    if names_csv:
        return [s.strip() for s in names_csv.split(",") if s.strip()]
    raise ValueError("Provide either --names-yaml or --names.")

def guess_version_from_run(run_dir: Path) -> str:
    m = re.search(r"(\d{8}[_-]\d{6,})", run_dir.name)
    if m:
        return f"v{m.group(1).replace('-', '_')}"
    return "v" + time.strftime("%Y%m%d_%H%M%S")

def load_json_safe(p: Path) -> dict:
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def make_latest_symlink(latest_dir: Path, target_dir: Path):
    if latest_dir.exists() or latest_dir.is_symlink():
        try:
            if latest_dir.is_symlink():
                latest_dir.unlink()
            else:
                shutil.rmtree(latest_dir)
        except Exception:
            pass
    try:
        latest_dir.symlink_to(target_dir.name, target_is_directory=True)
        return "symlink"
    except Exception:
        shutil.copytree(target_dir, latest_dir)
        return "copy"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, type=Path, help="artifacts/m1/runs/<RUN_NAME>")
    ap.add_argument("--dest-root", required=True, type=Path, help="models/yolov8-det")
    ap.add_argument("--version", default=None, help="override version folder name, e.g., v20251104_0740392")
    ap.add_argument("--family", default="yolov8-det", choices=["yolov8-det","yolov8-seg"])
    ap.add_argument("--imgsz", type=int, default=960)
    ap.add_argument("--names-yaml", type=Path, default=None, help="YAML with 'names' list (e.g., configs/data/m1_data.yaml)")
    ap.add_argument("--names", type=str, default=None, help="Comma separated names if not using YAML")
    args = ap.parse_args()

    run_dir: Path = args.run_dir.resolve()
    weights_dir = run_dir / "weights"
    if not weights_dir.exists():
        sys.exit(f"[ERR] weights dir not found: {weights_dir}")

    onnx_src = None

    candidates = list(weights_dir.glob("*.onnx"))
    if candidates:

        onnx_src = max(candidates, key=lambda p: p.stat().st_mtime)
    if onnx_src is None:
        sys.exit("[ERR] No ONNX found in weights/. Export ONNX first.")

    pt_src = None
    for name in ("best.pt", "last.pt"):
        p = weights_dir / name
        if p.exists():
            pt_src = p
            break

    metrics = load_json_safe(run_dir / "metrics.json")
    train_args = load_json_safe(run_dir / "train_args.json")
    res_csv = run_dir / "results.csv"

    names = read_names(args.names_yaml, args.names)
    nc = len(names)

    version = args.version or guess_version_from_run(run_dir)
    dest_version_dir = (args.dest_root / version).resolve()
    dest_version_dir.mkdir(parents=True, exist_ok=True)

    onnx_dst = dest_version_dir / "model.onnx"
    shutil.copy2(onnx_src, onnx_dst)
    onnx_sha = sha256_file(onnx_dst)

    if pt_src:
        shutil.copy2(pt_src, dest_version_dir / "best.pt")

    (dest_version_dir / "labels.txt").write_text("\n".join(names) + "\n", encoding="utf-8")

    if res_csv.exists():
        shutil.copy2(res_csv, dest_version_dir / "results.csv")

    model_card = {
        "model_family": args.family,
        "model_version": version,
        "imgsz": int(args.imgsz),
        "nc": int(nc),
        "names": names,
        "artifact_run": run_dir.name,
        "files": {
            "model.onnx": {"sha256": onnx_sha, "bytes": onnx_dst.stat().st_size},
            "best.pt": {"present": bool(pt_src)}
        },
        "metrics": metrics or None,
        "train_args": train_args or None,
    }
    (dest_version_dir / "model_card.json").write_text(json.dumps(model_card, indent=2, ensure_ascii=False), encoding="utf-8")

    latest_dir = args.dest_root / "latest"
    mode = make_latest_symlink(latest_dir, dest_version_dir)

    print("=== PROMOTE OK ===")
    print("Family     :", args.family)
    print("Run dir    :", run_dir)
    print("Promoted to:", dest_version_dir)
    print("Latest mode:", mode)
    print("ONNX sha256:", onnx_sha)
    print("Labels     :", names)
    if metrics:
        keys = ["metrics/mAP50(B)","metrics/mAP50-95(B)","metrics/precision(B)","metrics/recall(B)"]
        summary = {k: metrics.get(k) for k in metrics.keys() if any(s in k for s in ["mAP","precision","recall"])}
        print("Metrics    :", json.dumps(summary, ensure_ascii=False))
    print("Next steps : Update configs/inference.yaml (if not using latest/), then run API.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
