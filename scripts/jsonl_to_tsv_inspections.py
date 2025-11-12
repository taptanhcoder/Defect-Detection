import sys, json


for line in sys.stdin:
    line=line.strip()
    if not line:
        continue
    try:
        obj = json.loads(line)
        ts_ms = obj.get("ts_ms")
        product_code = obj.get("product_code")
        station_id = obj.get("station_id")
        aql = obj.get("aql_mini_decision")
        latency_ms = obj.get("latency_ms")
        dc = obj.get("defect_count")
        if dc is None:
            dets = obj.get("defects") or []
            dc = len(dets)


        if ts_ms is None or product_code is None or station_id is None or aql is None or latency_ms is None:
            continue


        print(f"{int(ts_ms)}\t{product_code}\t{station_id}\t{aql}\t{int(dc)}\t{int(latency_ms)}")
    except Exception:
        continue
