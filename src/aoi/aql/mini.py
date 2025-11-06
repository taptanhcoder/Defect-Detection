from __future__ import annotations
from typing import Dict, List, Optional


DEFAULT_RULES: Dict = {
    "max_defects": 0,
    "min_score": 0.0,
    "banned_classes": [], 
    "max_by_class": {},
    "measure_thresholds": {}
}


def quick_decision(
    defects: List[Dict],
    measures: Optional[Dict] = None,
    rules: Optional[Dict] = None
) -> str:

    R = {**DEFAULT_RULES, **(rules or {})}

    eff_defects = []
    min_score = R.get("min_score")
    for d in defects or []:
        s = float(d.get("score", 0.0))
        if (min_score is None) or (s >= float(min_score)):
            eff_defects.append(d)

    banned = set(R.get("banned_classes") or [])
    if any((d.get("cls") in banned) for d in eff_defects):
        return "FAIL"

    max_defects = int(R.get("max_defects", 0))
    if len(eff_defects) > max_defects:
        return "FAIL"

    max_by_class: Dict[str, int] = R.get("max_by_class") or {}
    if max_by_class:
        counts: Dict[str, int] = {}
        for d in eff_defects:
            c = str(d.get("cls"))
            counts[c] = counts.get(c, 0) + 1
        for c, lim in max_by_class.items():
            if counts.get(c, 0) > int(lim):
                return "FAIL"

    meas_th: Dict[str, float] = R.get("measure_thresholds") or {}
    if measures and meas_th:

        if "clearance_um_min" in meas_th:
            m = measures.get("clearance_um")
            if m is not None and float(m) < float(meas_th["clearance_um_min"]):
                return "FAIL"

        if "trace_width_um_min" in meas_th:
            m = measures.get("trace_width_um")
            if m is not None and float(m) < float(meas_th["trace_width_um_min"]):
                return "FAIL"
 
        if "pad_offset_um_max" in meas_th:
            m = measures.get("pad_offset_um")
            if m is not None and float(m) > float(meas_th["pad_offset_um_max"]):
                return "FAIL"

    return "PASS"
