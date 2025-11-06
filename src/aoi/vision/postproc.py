from __future__ import annotations
from typing import List, Dict
import numpy as np


def merge_tiles(
    tile_preds: List[Dict],
    iou_thres: float = 0.5,
    per_class_nms: bool = True,
) -> List[Dict]:

    global_boxes = []  
    classes: List[str] = []

    for tile in tile_preds:
        x0, y0 = tile["xy0"]
        dets = tile.get("dets", []) or []
        for d in dets:
            cls = d["cls"]; score = float(d["score"])
            bx = d["bbox"]; x, y, w, h = int(bx["x"]), int(bx["y"]), int(bx["w"]), int(bx["h"])
            x1, y1, x2, y2 = x0 + x, y0 + y, x0 + x + w, y0 + y + h

            try:
                cls_idx = classes.index(cls)
            except ValueError:
                classes.append(cls)
                cls_idx = len(classes) - 1

            global_boxes.append([x1, y1, x2, y2, score, cls_idx])

    if not global_boxes:
        return []

    arr = np.array(global_boxes, dtype=np.float32)
    if per_class_nms:
        keep = _nms_per_class(arr, iou_thres)
    else:
        keep = _nms_global(arr, iou_thres)

    defects: List[Dict] = []
    for i in keep:
        x1, y1, x2, y2, score, c = arr[i]
        defects.append({
            "cls": classes[int(c)],
            "score": float(score),
            "bbox": {
                "x": int(x1),
                "y": int(y1),
                "w": int(max(0.0, x2 - x1)),
                "h": int(max(0.0, y2 - y1)),
            }
        })

    defects.sort(key=lambda d: d["score"], reverse=True)
    return defects


def _iou_xyxy(a: np.ndarray, b: np.ndarray) -> float:
    x1 = max(a[0], b[0]); y1 = max(a[1], b[1])
    x2 = min(a[2], b[2]); y2 = min(a[3], b[3])
    inter_w = max(0.0, x2 - x1); inter_h = max(0.0, y2 - y1)
    inter = inter_w * inter_h
    area_a = max(0.0, a[2] - a[0]) * max(0.0, a[3] - a[1])
    area_b = max(0.0, b[2] - b[0]) * max(0.0, b[3] - b[1])
    denom = area_a + area_b - inter + 1e-6
    return float(inter / denom)


def _nms_indices(boxes: np.ndarray, scores: np.ndarray, iou_thres: float) -> List[int]:
    if boxes.size == 0:
        return []
    order = scores.argsort()[::-1]
    keep: List[int] = []
    while order.size > 0:
        i = int(order[0])
        keep.append(i)
        if order.size == 1:
            break
        rest = order[1:]
        ious = np.array([_iou_xyxy(boxes[i], boxes[j]) for j in rest], dtype=np.float32)
        remain = rest[ious <= iou_thres]
        order = remain
    return keep


def _nms_per_class(arr: np.ndarray, iou_thres: float) -> List[int]:

    keep_global: List[int] = []
    cls_ids = np.unique(arr[:, 5]).astype(int)
    for c in cls_ids:
        mask = arr[:, 5] == c
        sub = arr[mask]
        idxs = np.where(mask)[0]
        if sub.shape[0] == 0:
            continue
        boxes = sub[:, :4]
        scores = sub[:, 4]
        keep_local = _nms_indices(boxes, scores, iou_thres)
        keep_global.extend([int(idxs[j]) for j in keep_local])
    keep_global.sort(key=lambda i: float(arr[i, 4]), reverse=True)
    return keep_global


def _nms_global(arr: np.ndarray, iou_thres: float) -> List[int]:
    boxes = arr[:, :4]; scores = arr[:, 4]
    keep = _nms_indices(boxes, scores, iou_thres)
    keep.sort(key=lambda i: float(scores[i]), reverse=True)
    return keep
