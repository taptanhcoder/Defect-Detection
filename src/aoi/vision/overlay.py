from __future__ import annotations
from typing import List, Dict, Tuple
import cv2
import numpy as np


def draw_overlay(
    img_bgr: np.ndarray,
    defects: List[Dict],
    text_scale: float = 0.5,
    thickness: int = 2,
) -> np.ndarray:
 
    out = img_bgr.copy()
    H, W = out.shape[:2]

    for d in defects:
        b = d["bbox"]
        x, y, w, h = int(b["x"]), int(b["y"]), int(b["w"]), int(b["h"])
        # clamp vào ảnh
        x1 = max(0, min(W - 1, x))
        y1 = max(0, min(H - 1, y))
        x2 = max(0, min(W - 1, x + w))
        y2 = max(0, min(H - 1, y + h))

        label = f'{d["cls"]} {d["score"]:.2f}'
        color = (0, 255, 0)  
        cv2.rectangle(out, (x1, y1), (x2, y2), color, thickness)

        (tw, th), baseline = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, text_scale, 1)
        ty1 = max(0, y1 - th - 4)
        cv2.rectangle(out, (x1, ty1), (x1 + tw + 6, ty1 + th + 4), color, -1)
        cv2.putText(out, label, (x1 + 3, ty1 + th + 1),
                    cv2.FONT_HERSHEY_SIMPLEX, text_scale, (0, 0, 0), 1, cv2.LINE_AA)

    return out
