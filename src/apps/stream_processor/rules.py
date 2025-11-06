from __future__ import annotations
from typing import Dict, List, Tuple, Optional


_SEV_ORDER = {"CRITICAL": 3, "MAJOR": 2, "MINOR": 1, "INFO": 0}
_SEV_KEYS = set(_SEV_ORDER.keys())


def _max_severity(a: Optional[str], b: Optional[str]) -> Optional[str]:
    if not a:
        return b
    if not b:
        return a
    return a if _SEV_ORDER.get(a, -1) >= _SEV_ORDER.get(b, -1) else b


def apply_aql(
    defects: List[Dict],
    measures: Optional[Dict],
    spec: Dict
) -> Tuple[str, str, Optional[str]]:

    defects = defects or []
    measures = measures or {}

    banned = set(spec.get("banned_classes") or [])
    max_defects = int(spec.get("max_defects", 999999))
    max_by_class: Dict[str, int] = spec.get("max_by_class") or {}
    thr: Dict[str, float] = spec.get("thresholds") or {}
    sev_by_class: Dict[str, str] = {
        k: (v if v in _SEV_KEYS else "MINOR") for k, v in (spec.get("severity_by_class") or {}).items()
    }

    reasons: List[str] = []
    severity: Optional[str] = None

    present = sorted({d.get("cls") for d in defects})
    banned_present = [c for c in present if c in banned]
    if banned_present:
        reasons.append("banned:" + ",".join(banned_present))
        for c in banned_present:
            severity = _max_severity(severity, sev_by_class.get(c, "MAJOR"))


    if len(defects) > max_defects:
        reasons.append(f"too_many_defects(total={len(defects)}>{max_defects})")
        severity = _max_severity(severity, "MAJOR")


    if max_by_class:
        counts: Dict[str, int] = {}
        for d in defects:
            c = str(d.get("cls"))
            counts[c] = counts.get(c, 0) + 1
        exceeded = [f"{c}:{counts[c]}>{lim}" for c, lim in max_by_class.items() if counts.get(c, 0) > int(lim)]
        if exceeded:
            reasons.append("exceed_by_class(" + ",".join(exceeded) + ")")
            for item in exceeded:
                c = item.split(":")[0]
                severity = _max_severity(severity, sev_by_class.get(c, "MINOR"))


    thr_msgs: List[str] = []
    if "clearance_um_min" in thr:
        v = measures.get("clearance_um")
        if v is not None and float(v) < float(thr["clearance_um_min"]):
            thr_msgs.append(f"clearance<{thr['clearance_um_min']}")
            severity = _max_severity(severity, "MAJOR")
    if "trace_width_um_min" in thr:
        v = measures.get("trace_width_um")
        if v is not None and float(v) < float(thr["trace_width_um_min"]):
            thr_msgs.append(f"trace_width<{thr['trace_width_um_min']}")
            severity = _max_severity(severity, "MAJOR")
    if "pad_offset_um_max" in thr:
        v = measures.get("pad_offset_um")
        if v is not None and float(v) > float(thr["pad_offset_um_max"]):
            thr_msgs.append(f"pad_offset>{thr['pad_offset_um_max']}")
            severity = _max_severity(severity, "MINOR")

    if thr_msgs:
        reasons.append("thresholds:" + ",".join(thr_msgs))

    if reasons:
        decision = "FAIL"
        reason = "; ".join(reasons)
    else:
        decision = "PASS"
        reason = "ok"

    return decision, reason, severity
