# src/ui/app.py
import os
import json
import urllib.parse
from datetime import timedelta
from dotenv import load_dotenv

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, jsonify
)
import httpx

load_dotenv()
OPS_API_BASE = os.getenv("OPS_API_BASE", "http://127.0.0.1:8100").rstrip("/")
GRAFANA_BASE = os.getenv("GRAFANA_BASE", "http://localhost:3000").rstrip("/")
GRAFANA_DASH_UID = os.getenv("GRAFANA_DASH_UID", "aoi_overview")
APP_PORT = int(os.getenv("APP_PORT", "8090"))
FLASK_DEBUG = os.getenv("FLASK_DEBUG", "0") == "1"
FLASK_SECRET = os.getenv("FLASK_SECRET", "change-me-please")

app = Flask(__name__, static_folder="static", template_folder="templates")
app.secret_key = FLASK_SECRET
app.permanent_session_lifetime = timedelta(days=1)


def grafana_panel_url(panel_id: int, product: str | None, station: str | None) -> str:
    params = {
        "orgId": "1",
        "refresh": "10s",
        "kiosk": "tv",
        "from": "now-24h",
        "to": "now",
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


@app.get("/")
def overview():
    try:
        filters = api_get("/filters", timeout=5.0)
    except Exception as e:
        filters = {"products": [], "stations": []}
        flash(f"Lỗi tải filters từ ops_api: {e}", "danger")

    product = request.args.get("product") or ""
    station = request.args.get("station") or ""

    fpy_src = grafana_panel_url(panel_id=1, product=product or None, station=station or None)
    lat_src = grafana_panel_url(panel_id=2, product=product or None, station=station or None)

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

    # backend mới trả {"item": {...}, "defects": [...], "variants": [...]}
    if isinstance(d, dict) and "item" in d:
        item = d["item"]
        defects = d.get("defects")
        variants = d.get("variants", [])
    else:
        item = d
        defects = None
        variants = []

    if defects is None:
        # thử bóc defects_json
        dj = item.get("defects_json")
        if dj:
            try:
                defects = json.loads(dj)
            except Exception:
                defects = []
        else:
            defects = []

    overlay_url = item.get("image_overlay_url") or item.get("overlay_url")
    raw_url = item.get("image_raw_url") or item.get("raw_url")

    return render_template(
        "detail.html",
        item=item,
        defects=defects,
        variants=variants,
        overlay_url=overlay_url,
        raw_url=raw_url,
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
        res = api_post("/infer-test",
                       data={"product_code": product_code, "station_id": station_id, "board_serial": board_serial},
                       files={"image": file_tuple},
                       timeout=120.0)
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
        data = api_get("/inspections/recent",
                       params={"limit": limit, "product": product or None, "station": station or None})
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
            "limit": limit
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
