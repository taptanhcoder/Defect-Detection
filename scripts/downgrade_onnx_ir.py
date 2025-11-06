from __future__ import annotations
import argparse, onnx, sys

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True, help="path to input model.onnx")
    ap.add_argument("--out", dest="out", required=True, help="path to output model_ir11.onnx")
    ap.add_argument("--ir", dest="ir", type=int, default=11, help="target IR version (default 11)")
    args = ap.parse_args()

    m = onnx.load(args.inp)
    print(f"[before] ir_version={m.ir_version}")
    m.ir_version = args.ir  
    onnx.checker.check_model(m)  
    onnx.save(m, args.out)
    print(f"[after ] ir_version={onnx.load(args.out).ir_version}")
    print(f"Saved: {args.out}")

if __name__ == "__main__":
    sys.exit(main())
