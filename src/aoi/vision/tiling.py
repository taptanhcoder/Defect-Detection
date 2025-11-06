
from __future__ import annotations
from typing import List, Dict, Tuple
import numpy as np


def tile_960(img_bgr: np.ndarray, tile_size: int = 960, overlap: int = 64) -> List[Dict]:

    H, W = img_bgr.shape[:2]
    s = int(tile_size)
    ov = int(overlap)
    tiles: List[Dict] = []

    if H <= s and W <= s:
        canvas = np.zeros((s, s, 3), dtype=img_bgr.dtype)
        canvas[:H, :W] = img_bgr
        tiles.append({"tile": canvas, "xy0": (0, 0)})
        return tiles

    y_step = max(1, s - ov)
    x_step = max(1, s - ov)

    for y0 in range(0, max(1, H - s + 1), y_step):
        for x0 in range(0, max(1, W - s + 1), x_step):
            y1 = min(y0 + s, H)
            x1 = min(x0 + s, W)
            patch = img_bgr[y0:y1, x0:x1]

            if patch.shape[0] != s or patch.shape[1] != s:
                canvas = np.zeros((s, s, 3), dtype=img_bgr.dtype)
                canvas[: patch.shape[0], : patch.shape[1]] = patch
                patch = canvas

            tiles.append({"tile": patch, "xy0": (x0, y0)})

    if (W - s) % x_step != 0:
        x0 = max(0, W - s)
        for y0 in range(0, max(1, H - s + 1), y_step):
            y1 = min(y0 + s, H)
            patch = img_bgr[y0:y1, x0:W]
            if patch.shape[0] != s or patch.shape[1] != s:
                canvas = np.zeros((s, s, 3), dtype=img_bgr.dtype)
                canvas[: patch.shape[0], : patch.shape[1]] = patch
                patch = canvas
            tiles.append({"tile": patch, "xy0": (x0, y0)})

    if (H - s) % y_step != 0:
        y0 = max(0, H - s)
        for x0 in range(0, max(1, W - s + 1), x_step):
            x1 = min(x0 + s, W)
            patch = img_bgr[y0:H, x0:x1]
            if patch.shape[0] != s or patch.shape[1] != s:
                canvas = np.zeros((s, s, 3), dtype=img_bgr.dtype)
                canvas[: patch.shape[0], : patch.shape[1]] = patch
                patch = canvas
            tiles.append({"tile": patch, "xy0": (x0, y0)})


    if (W - s) % x_step != 0 and (H - s) % y_step != 0:
        x0 = max(0, W - s)
        y0 = max(0, H - s)
        patch = img_bgr[y0:H, x0:W]
        if patch.shape[0] != s or patch.shape[1] != s:
            canvas = np.zeros((s, s, 3), dtype=img_bgr.dtype)
            canvas[: patch.shape[0], : patch.shape[1]] = patch
            patch = canvas
        tiles.append({"tile": patch, "xy0": (x0, y0)})

    return tiles
