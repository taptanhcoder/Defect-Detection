from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional

import numpy as np
import cv2
import onnxruntime as ort


@dataclass
class DetBox:
    cls: str
    score: float
    x: int
    y: int
    w: int
    h: int

    def as_dict(self) -> Dict:
        return {"cls": self.cls, "score": float(self.score),
                "bbox": {"x": int(self.x), "y": int(self.y), "w": int(self.w), "h": int(self.h)}}


class YoloV8DetONNX:
 
    def __init__(
        self,
        onnx_path: str,
        labels_path: str,
        providers: Tuple[str, ...] = ("CUDAExecutionProvider", "CPUExecutionProvider"),
        imgsz: int = 960,
    ):
        self.imgsz = int(imgsz)
        sess_opts = ort.SessionOptions()
        sess_opts.log_severity_level = 3

        try:
            self.session = ort.InferenceSession(onnx_path, sess_options=sess_opts, providers=list(providers))
        except Exception:
            self.session = ort.InferenceSession(onnx_path, sess_options=sess_opts, providers=["CPUExecutionProvider"])

        self.input_name = self.session.get_inputs()[0].name
        self.output_names = [o.name for o in self.session.get_outputs()]

        with open(labels_path, "r", encoding="utf-8") as f:
            self.labels = [ln.strip() for ln in f.readlines() if ln.strip()]
        if not self.labels:
            raise ValueError(f"labels.txt rỗng: {labels_path}")
        self.nc = len(self.labels)

    def predict_tile(
        self,
        img_bgr_tile: np.ndarray,
        conf_thres: float = 0.25,
        iou_thres: float = 0.45,
        per_class_nms: bool = True,
    ) -> List[Dict]:
 
        inp = self._preprocess_bgr(img_bgr_tile) 
        outputs = self.session.run(self.output_names, {self.input_name: inp})

        raw = outputs[0]
        dets = self._decode_detections(raw, conf_thres=conf_thres)

  
        if per_class_nms:
            keep_indices = self._nms_per_class(dets, iou_thres)
        else:
            keep_indices = self._nms_global(dets, iou_thres)

        kept = [dets[i].as_dict() for i in keep_indices]
        return kept


    def _preprocess_bgr(self, img_bgr: np.ndarray) -> np.ndarray:
        img = cv2.resize(img_bgr, (self.imgsz, self.imgsz), interpolation=cv2.INTER_LINEAR)
        img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB).astype(np.float32) / 255.0
        img = img.transpose(2, 0, 1)  
        return img[None, ...]  

    def _decode_detections(self, raw: np.ndarray, conf_thres: float) -> List[DetBox]:

        arr = raw
        if arr.ndim == 3:
            arr = arr[0] 
        elif arr.ndim == 4:

            arr = arr[0, 0]

        if arr.shape[0] < arr.shape[1]:

            arr = arr.T

        N, C = arr.shape
        if C < 4 + 1: 
            return []

        has_obj = (C == (4 + 1 + self.nc)) or (C > (4 + self.nc) and C <= (4 + 1 + self.nc + 4))
        x = arr[:, 0]; y = arr[:, 1]; w = arr[:, 2]; h = arr[:, 3]

        if has_obj:
            obj = arr[:, 4]
            cls_logits = arr[:, 5: 5 + self.nc]
            cls_id = np.argmax(cls_logits, axis=1)
            cls_prob = np.max(cls_logits, axis=1)
            scores = (obj * cls_prob).astype(np.float32)
        else:
  
            cls_logits = arr[:, 4: 4 + self.nc]
            cls_id = np.argmax(cls_logits, axis=1)
            cls_prob = np.max(cls_logits, axis=1)
            scores = cls_prob.astype(np.float32)

        keep_mask = scores >= float(conf_thres)
        if not np.any(keep_mask):
            return []

        x, y, w, h, cls_id, scores = x[keep_mask], y[keep_mask], w[keep_mask], h[keep_mask], cls_id[keep_mask], scores[keep_mask]

        x1 = x - w / 2.0
        y1 = y - h / 2.0
        x2 = x + w / 2.0
        y2 = y + h / 2.0


        x1 = np.clip(x1, 0, self.imgsz - 1)
        y1 = np.clip(y1, 0, self.imgsz - 1)
        x2 = np.clip(x2, 0, self.imgsz - 1)
        y2 = np.clip(y2, 0, self.imgsz - 1)

        dets: List[DetBox] = []
        for i in range(x1.shape[0]):
            xi1, yi1, xi2, yi2 = float(x1[i]), float(y1[i]), float(x2[i]), float(y2[i])
            w_box = max(0.0, xi2 - xi1)
            h_box = max(0.0, yi2 - yi1)
            if w_box <= 0 or h_box <= 0:
                continue
            cls_name = self.labels[int(cls_id[i])]
            dets.append(
                DetBox(
                    cls=cls_name,
                    score=float(scores[i]),
                    x=int(round(xi1)),
                    y=int(round(yi1)),
                    w=int(round(w_box)),
                    h=int(round(h_box)),
                )
            )
        return dets


    @staticmethod
    def _iou_xyxy(a: np.ndarray, b: np.ndarray) -> float:

        x1 = max(a[0], b[0])
        y1 = max(a[1], b[1])
        x2 = min(a[2], b[2])
        y2 = min(a[3], b[3])
        inter_w = max(0.0, x2 - x1)
        inter_h = max(0.0, y2 - y1)
        inter = inter_w * inter_h
        area_a = max(0.0, (a[2] - a[0])) * max(0.0, (a[3] - a[1]))
        area_b = max(0.0, (b[2] - b[0])) * max(0.0, (b[3] - b[1]))
        denom = area_a + area_b - inter + 1e-6
        return float(inter / denom)

    def _nms_indices(self, boxes: np.ndarray, scores: np.ndarray, iou_thres: float) -> List[int]:

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
            ious = np.array([self._iou_xyxy(boxes[i], boxes[j]) for j in rest], dtype=np.float32)
            remain = rest[ious <= iou_thres]
            order = remain
        return keep

    def _nms_per_class(self, dets: List[DetBox], iou_thres: float) -> List[int]:

        if not dets:
            return []

        by_cls: Dict[str, List[int]] = {}
        for i, d in enumerate(dets):
            by_cls.setdefault(d.cls, []).append(i)

        keep_global: List[int] = []
        for cls_name, idxs in by_cls.items():
            if not idxs:
                continue
            boxes = np.array([[dets[i].x, dets[i].y, dets[i].x + dets[i].w, dets[i].y + dets[i].h] for i in idxs],
                             dtype=np.float32)
            scores = np.array([dets[i].score for i in idxs], dtype=np.float32)
            keep_local = self._nms_indices(boxes, scores, iou_thres)
            keep_global.extend([idxs[j] for j in keep_local])
        keep_global.sort(key=lambda i: dets[i].score, reverse=True)
        return keep_global

    def _nms_global(self, dets: List[DetBox], iou_thres: float) -> List[int]:

        if not dets:
            return []
        boxes = np.array([[d.x, d.y, d.x + d.w, d.y + d.h] for d in dets], dtype=np.float32)
        scores = np.array([d.score for d in dets], dtype=np.float32)
        keep = self._nms_indices(boxes, scores, iou_thres)
        keep.sort(key=lambda i: dets[i].score, reverse=True)
        return keep
