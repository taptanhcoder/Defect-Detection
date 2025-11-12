

from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.queue import Kafka
from diagrams.onprem.monitoring import Grafana
from diagrams.programming.language import Python
from diagrams.onprem.compute import Server

# ---- Compatibility shims (phòng khi bản diagrams thiếu vài icon) ----
# Camera icon
try:
    from diagrams.generic.device import Camera as CameraNode  # type: ignore
except Exception:
    try:
        from diagrams.onprem.client import Client as CameraNode  # fallback
    except Exception:
        CameraNode = Server  # fallback cuối: dùng Server và đặt nhãn "Camera"

# ClickHouse icon (nhiều bản diagrams chưa có)
try:
    from diagrams.onprem.database import Clickhouse as ClickHouseNode  # type: ignore
except Exception:
    try:
        from diagrams.onprem.database import Postgresql as ClickHouseNode  # fallback dùng Postgres icon
    except Exception:
        ClickHouseNode = Server  # fallback cuối: Server

# MinIO icon
try:
    from diagrams.onprem.storage import Minio as MinioNode  # type: ignore
except Exception:
    try:
        from diagrams.generic.storage import Storage as MinioNode  # generic storage icon nếu có
    except Exception:
        MinioNode = Server  # fallback cuối: Server

graph_attr = {
    "fontsize": "18",
    "splines": "spline",
    "rankdir": "LR",
    "pad": "0.5",
}

node_attr = {
    "fontcolor": "black",
    "fontsize": "14",
    "penwidth": "2",
}

with Diagram(
    name="AOI PCB – High-Level Visualization",
    filename="aoi_pipeline",
    show=False,
    outformat="png",
    graph_attr=graph_attr,
    node_attr=node_attr,
):
    # Nguồn ảnh (Camera)
    camera = CameraNode("Ảnh PCB\n(Camera)")

    # Khối Inference API
    with Cluster("Inference API", graph_attr={"labeljust": "l"}):
        inf_api = Server("Inference API\n(FastAPI / ONNXRuntime)")

        # Nhánh 1: Kafka -> Processor -> ClickHouse
        kafka = Kafka("Kafka")
        processor = Python("Processor\n(Streaming Service)")
        clickhouse = ClickHouseNode("ClickHouse")

        # Nhánh 2: MinIO (data storage)
        minio = MinioNode("MinIO\n(Data Storage)")

    # Grafana bên ngoài, được “khỏ” (feed) từ cả hai nhánh
    grafana = Grafana("Grafana\n(Real-time Dashboards)")

    # ----- Luồng kết nối -----
    # Camera -> Inference API
    camera >> Edge(label="POST /v1/infer\n(JPEG/PNG)") >> inf_api

    # Inference API tách 2 nhánh
    # Nhánh 1
    inf_api >> Edge(label="publish InferResponse") >> kafka
    kafka >> Edge(label="consume / process") >> processor
    processor >> Edge(label="INSERT fact/agg") >> clickhouse
    clickhouse >> Edge(label="SQL / Views") >> grafana

    # Nhánh 2
    inf_api >> Edge(label="upload overlay\n(presigned URL)") >> minio
    minio >> Edge(label="overlay links") >> grafana
