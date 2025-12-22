# src/ui/app.py
import os
import json
import urllib.parse
from datetime import timedelta
from collections import Counter

from dotenv import load_dotenv
from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, jsonify
)
import httpx

load_dotenv()

# --------- Config từ env ---------
OPS_API_BASE = os.getenv("OPS_API_BASE", "http://127.0.0.1:8100").rstrip("/")
GRAFANA_BASE = os.getenv("GRAFANA_BASE", "http://localhost:3000").rstrip("/")
GRAFANA_DASH_UID = os.getenv("GRAFANA_DASH_UID", "aoi_overview")

# Nếu set sẵn 2 URL embed trong .env thì UI sẽ dùng trực tiếp
GRAFANA_FPY_SRC = os.getenv("GRAFANA_FPY_SRC")   # FPY (5m)
GRAFANA_LAT_SRC = os.getenv("GRAFANA_LAT_SRC")   # Latency avg (5m)

APP_PORT = int(os.getenv("APP_PORT", "8090"))
FLASK_DEBUG = os.getenv("FLASK_DEBUG", "0") == "1"
FLASK_SECRET = os.getenv("FLASK_SECRET", "change-me-please")

app = Flask(__name__, static_folder="static", template_folder="templates")
app.secret_key = FLASK_SECRET
app.permanent_session_lifetime = timedelta(days=1)

# ---------- Mapping ký hiệu lỗi ----------
DEFECT_CODE_MAP = {
    "SH": "Short (Ngắn mạch / chập mạch)",
    "SP": "Spur (Gờ đồng / gai đồng)",
    "SC": "Spurious Copper (Đồng thừa / mảng đồng dư)",
    "OP": "Open (Mạch hở / đứt mạch)",
    "MB": "Mouse Bite (Chuột gặm / mẻ cạnh đường mạch)",
    "HB": "Hole Breakout (Lỗ khoan bị breakout khỏi pad)",
    "CS": "Conductor Scratch (Xước đường dẫn)",
    "CFO": "Conductor Foreign Object (Vật thể lạ dẫn điện trên đường mạch)",
    "BMFO": "Base Material Foreign Object (Vật thể lạ trên nền base)",
}


def expand_defect_label(code_or_label: str | None) -> str:
    """
    'SC' -> 'SC – Spurious Copper (Đồng thừa / mảng đồng dư)'
    Chuỗi đã đầy đủ rồi thì trả nguyên.
    """
    if not code_or_label:
        return ""
    txt = str(code_or_label).strip()
    # Nếu đã có “–” hoặc “ - ” thì coi như đã có mô tả
    if "–" in txt or " - " in txt:
        return txt
    code = txt.upper()
    desc = DEFECT_CODE_MAP.get(code)
    if desc:
        return f"{code} – {desc}"
    return txt


def grafana_panel_url(panel_id: str, product: str | None, station: str | None) -> str:
    """
    Xây URL embed panel Grafana (Scenes) theo pattern embed của Grafana.
    panel_id: 'panel-101', 'panel-102', ...
    """
    params: dict[str, str] = {
        "orgId": "1",
        "refresh": "10s",
        "from": "now-24h",
        "to": "now",
        "timezone": "utc",
        "__feature.dashboardSceneSolo": "true",
    }
    if product:
        params["var-product_code"] = product
    if station:
        params["var-station_id"] = station

    qs = urllib.parse.urlencode(params)
    return f"{GRAFANA_BASE}/d-solo/{GRAFANA_DASH_UID}/aoi-overview?panelId={panel_id}&{qs}"


def api_get(path: str, params: dict | None = None, timeout: float = 10.0):
    url = f"{OPS_API_BASE}{path}"
    with httpx.Client(timeout=timeout) as cx:
        r = cx.get(url, params=params)
        r.raise_for_status()
        return r.json()


def api_post(path: str, data: dict | None = None, files: dict | None = None, timeout: float = 60.0):
    url = f"{OPS_API_BASE}{path}"
    with httpx.Client(timeout=timeout) as cx:
        r = cx.post(url, data=data, files=files)
        r.raise_for_status()
        return r.json()


# ================== ROUTES ==================

@app.get("/")
def overview():
    try:
        filters = api_get("/filters", timeout=5.0)
    except Exception as e:
        filters = {"products": [], "stations": []}
        flash(f"Lỗi tải filters từ ops_api: {e}", "danger")

    product = request.args.get("product") or ""
    station = request.args.get("station") or ""

    # ƯU TIÊN: nếu đã cấu hình sẵn link embed trong .env thì dùng luôn
    # (thường là copy nguyên src="..." từ Grafana)
    if GRAFANA_FPY_SRC and GRAFANA_LAT_SRC:
        fpy_src = GRAFANA_FPY_SRC
        lat_src = GRAFANA_LAT_SRC
    else:
        # Nếu không có, build động theo panelId Scenes
        fpy_src = grafana_panel_url(panel_id="panel-101", product=product or None, station=station or None)
        lat_src = grafana_panel_url(panel_id="panel-102", product=product or None, station=station or None)

    return render_template(
        "index.html",
        filters=filters,
        product=product,
        station=station,
        fpy_src=fpy_src,
        lat_src=lat_src,
        active_page="overview",
        title="AOI • Overview"
    )


@app.get("/live")
def live():
    try:
        filters = api_get("/filters", timeout=5.0)
    except Exception:
        filters = {"products": [], "stations": []}
    product = request.args.get("product") or ""
    station = request.args.get("station") or ""
    return render_template(
        "live.html",
        filters=filters,
        product=product,
        station=station,
        active_page="live",
        title="AOI • Live Feed"
    )


@app.get("/inspections")
def inspections():
    product = request.args.get("product") or ""
    station = request.args.get("station") or ""
    decision = (request.args.get("decision") or "").upper()

    try:
        filters = api_get("/filters", timeout=5.0)
    except Exception:
        filters = {"products": [], "stations": []}

    items = []
    try:
        data = api_get("/inspections/recent", params={
            "limit": 100,
            "product": product or None,
            "station": station or None
        })
        items = data.get("items", [])
        if decision in ("PASS", "FAIL"):
            items = [x for x in items if (x.get("aql_final_decision", "").upper() == decision)]
    except Exception as e:
        flash(f"Lỗi tải dữ liệu inspections: {e}", "danger")

    return render_template(
        "inspections.html",
        filters=filters,
        product=product,
        station=station,
        decision=decision,
        items=items,
        active_page="inspections",
        title="AOI • Inspections"
    )


@app.get("/inspections/<event_id>")
def inspection_detail(event_id: str):
    try:
        d = api_get(f"/inspections/{event_id}", timeout=10.0)
    except Exception as e:
        flash(f"Lỗi tải chi tiết {event_id}: {e}", "danger")
        return redirect(url_for("inspections"))

    # backend trả {"item": {...}, "defects": [...], "variants": [...]}
    if isinstance(d, dict) and "item" in d:
        item = d["item"]
        defects = d.get("defects")
        variants = d.get("variants", [])
    else:
        item = d
        defects = None
        variants = []

    # Bóc defects nếu chưa có
    if defects is None:
        dj = item.get("defects_json")
        if dj:
            try:
                defects = json.loads(dj)
            except Exception:
                defects = []
        else:
            defects = []

    # Trang trí label đầy đủ cho từng defect
    decorated_defects = []
    for raw in defects:
        if isinstance(raw, dict):
            dct = dict(raw)
        else:
            dct = {"cls": str(raw)}
        cls_val = dct.get("cls") or dct.get("label")
        dct["ui_label"] = expand_defect_label(cls_val)
        decorated_defects.append(dct)
    defects = decorated_defects

    # ----- TÍNH FAIL REASON HIỂN THỊ -----
    decision = (item.get("aql_final_decision") or "").upper()
    backend_reason = item.get("fail_reason")

    if decision != "FAIL":
        fail_reason_ui = backend_reason or "PASS – Không có lỗi vượt ngưỡng AQL."
    else:
        if backend_reason:
            fail_reason_ui = backend_reason
        elif defects:
            labels = [
                (d.get("ui_label") or d.get("cls") or d.get("label") or "").strip()
                for d in defects
            ]
            labels = [x for x in labels if x]
            if labels:
                counts = Counter(labels)
                top_label, top_n = counts.most_common(1)[0]
                if len(counts) == 1:
                    fail_reason_ui = f"FAIL – Có {top_n} lỗi {top_label}."
                else:
                    fail_reason_ui = (
                        f"FAIL – Có nhiều loại lỗi, lỗi chính: {top_label} (số lượng {top_n})."
                    )
            else:
                fail_reason_ui = "FAIL – Có lỗi nhưng không xác định được loại lỗi."
        else:
            fail_reason_ui = "FAIL – Bảng được đánh FAIL nhưng không có danh sách lỗi chi tiết."

    overlay_url = item.get("image_overlay_url") or item.get("overlay_url")
    raw_url = item.get("image_raw_url") or item.get("raw_url")

    return render_template(
        "detail.html",
        item=item,
        defects=defects,
        variants=variants,
        overlay_url=overlay_url,
        raw_url=raw_url,
        fail_reason_ui=fail_reason_ui,
        active_page="inspections",
        title=f"AOI • {event_id}"
    )


@app.route("/test-console", methods=["GET", "POST"])
def test_console():
    if request.method == "GET":
        return render_template("test_console.html", result=None, active_page="test", title="AOI • Test Console")

    try:
        product_code = request.form.get("product_code") or ""
        station_id = request.form.get("station_id") or ""
        board_serial = request.form.get("board_serial") or ""
        image = request.files.get("image")

        if not (image and product_code and station_id):
            flash("Thiếu ảnh hoặc product_code/station_id.", "warning")
            return render_template("test_console.html", result=None, active_page="test", title="AOI • Test Console")

        file_tuple = (image.filename, image.stream.read(), image.mimetype or "application/octet-stream")
        res = api_post(
            "/infer-test",
            data={"product_code": product_code, "station_id": station_id, "board_serial": board_serial},
            files={"image": file_tuple},
            timeout=120.0
        )
        return render_template("test_console.html", result=res, active_page="test", title="AOI • Test Console")
    except Exception as e:
        flash(f"Gửi infer-test lỗi: {e}", "danger")
        return render_template("test_console.html", result=None, active_page="test", title="AOI • Test Console")


@app.get("/health")
def health():
    try:
        h = api_get("/healthz", timeout=5.0)
    except Exception as e:
        h = {"status": "fail", "error": str(e)}
        flash(f"Lỗi healthz: {e}", "danger")
    return render_template("health.html", h=h, active_page="health", title="AOI • Health")


@app.get("/gallery")
def gallery():
    try:
        filters = api_get("/filters", timeout=5.0)
    except Exception:
        filters = {"products": [], "stations": []}
    product = request.args.get("product") or ""
    station = request.args.get("station") or ""
    return render_template(
        "defects.html",
        filters=filters,
        product=product,
        station=station,
        active_page="gallery",
        title="AOI • Defect Gallery"
    )


# --------- API proxy cho UI ----------
@app.get("/api/recent-proxy")
def recent_proxy():
    product = request.args.get("product") or ""
    station = request.args.get("station") or ""
    limit = int(request.args.get("limit") or 20)
    try:
        data = api_get(
            "/inspections/recent",
            params={"limit": limit, "product": product or None, "station": station or None}
        )
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@app.get("/api/defects")
def api_defects():
    product = request.args.get("product") or None
    station = request.args.get("station") or None
    decision = (request.args.get("decision") or "FAIL").upper()
    time_from = request.args.get("from") or None
    time_to = request.args.get("to") or None
    page = int(request.args.get("page") or 1)
    limit = int(request.args.get("limit") or 30)

    try:
        params = {
            "product": product,
            "station": station,
            "decision": decision,
            "from": time_from,
            "to": time_to,
            "page": page,
            "limit": limit,
        }
        data = api_get("/inspections/search", params=params, timeout=15.0)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@app.get("/api/presign")
def api_presign():
    key = request.args.get("key")
    if not key:
        return jsonify({"error": "missing key"}), 400
    try:
        data = api_get("/media/presign", params={"key": key}, timeout=10.0)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": f"presign failed: {e}"}), 502


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=APP_PORT, debug=FLASK_DEBUG)
