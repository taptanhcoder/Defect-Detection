from __future__ import annotations
from typing import Tuple
import numpy as np
import cv2


def register_to_template(
    img_bgr: np.ndarray,
    template_bgr: np.ndarray,
    method: str = "orb",
    max_features: int = 2000,
    good_match_percent: float = 0.15,
) -> Tuple[np.ndarray, np.ndarray]:
 
    th, tw = template_bgr.shape[:2]
    img_bgr_resz = cv2.resize(img_bgr, (tw, th), interpolation=cv2.INTER_LINEAR)

    if method.lower() == "akaze":
        detector = cv2.AKAZE_create()
        norm_type = cv2.NORM_HAMMING
    else:  
        detector = cv2.ORB_create(nfeatures=max_features, fastThreshold=5)
        norm_type = cv2.NORM_HAMMING

    img_gray = cv2.cvtColor(img_bgr_resz, cv2.COLOR_BGR2GRAY)
    tpl_gray = cv2.cvtColor(template_bgr, cv2.COLOR_BGR2GRAY)

    k1, d1 = detector.detectAndCompute(img_gray, None)
    k2, d2 = detector.detectAndCompute(tpl_gray, None)

    if d1 is None or d2 is None or len(k1) < 10 or len(k2) < 10:
        return img_bgr_resz, np.eye(3, dtype=np.float64)

    matcher = cv2.BFMatcher(normType=norm_type, crossCheck=False)
    matches = matcher.knnMatch(d1, d2, k=2)

    good = []
    for m, n in matches:
        if m.distance < 0.75 * n.distance:
            good.append(m)

    if len(good) < 10:
        return img_bgr_resz, np.eye(3, dtype=np.float64)

    good = sorted(good, key=lambda x: x.distance)
    good = good[: max(10, int(len(good) * good_match_percent))]

    pts1 = np.float32([k1[m.queryIdx].pt for m in good]).reshape(-1, 1, 2)
    pts2 = np.float32([k2[m.trainIdx].pt for m in good]).reshape(-1, 1, 2)

    H, mask = cv2.findHomography(pts1, pts2, method=cv2.RANSAC, ransacReprojThreshold=3.0)
    if H is None:
        return img_bgr_resz, np.eye(3, dtype=np.float64)

    aligned = cv2.warpPerspective(img_bgr_resz, H, (tw, th), flags=cv2.INTER_LINEAR)
    return aligned, H
