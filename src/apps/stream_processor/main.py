from __future__ import annotations
import os
import sys
import signal
import time
import logging
from pathlib import Path

from src.pipelines.kafka_consumer import KafkaJSONConsumer, load_streaming_config as load_stream_cfg
from src.pipelines.clickhouse_writer import ClickHouseWriter 
from src.apps.stream_processor.spec_loader import SpecRepository 
from src.apps.stream_processor.handlers import handle_inference_result 
from src.apps.stream_processor.producer import QCEventProducer 


logging.basicConfig(
    level=os.getenv("AOI_LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("aoi.stream_processor")


_STOP = False
def _sig_handler(signum, frame):
    global _STOP
    log.info("Received signal %s → stopping...", signum)
    _STOP = True


def main():
    cfg_path = os.getenv("AOI_STREAMING_CONFIG", "configs/streaming.yaml")
    cfg = load_stream_cfg(cfg_path)

    # ---- Kafka consumer
    consumer = KafkaJSONConsumer.from_yaml(cfg_path)
    consumer.subscribe()

    # ---- ClickHouse writer
    ck_bulk_rows = int(os.getenv("AOI_CK_BULK_ROWS", "500"))
    ck_bulk_secs = float(os.getenv("AOI_CK_BULK_SECS", "2.0"))
    ck = ClickHouseWriter.from_yaml(cfg_path, bulk_max_rows=ck_bulk_rows, bulk_max_seconds=ck_bulk_secs)

    # ---- Spec repository
    spec_repo = SpecRepository.from_yaml(cfg_path)

    # ---- Optional QC events producer
    kc = cfg.get("kafka", {}) or {}
    alerts_enabled = bool(cfg.get("processor", {}).get("emit_alerts", True))
    qc_producer = None
    if alerts_enabled:
        qc_topic = str(kc.get("topic_qc_events", "aoi.qc_events"))
        qc_producer = QCEventProducer(
            brokers=str(kc.get("brokers", "kafka:9092")),
            schema_registry_url=str(kc.get("schema_registry", "http://schema-registry:8081")),
            topic=qc_topic,
            jsonl_path=Path("data/processed/qc_events.jsonl"),
        )

    # ---- Signals
    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    log.info("Stream Processor started. bulk_rows=%s bulk_secs=%s alerts=%s",
             ck_bulk_rows, ck_bulk_secs, alerts_enabled)

    idle_backoff = 0.2
    last_flush_ts = time.monotonic()
    flush_interval = max(ck_bulk_secs, 2.0) if (ck_bulk_rows > 0 or ck_bulk_secs > 0) else 0.0

    try:
        while not _STOP:
            msg = next(consumer.iter_messages(), None)
            if not msg or not msg.get("payload"):
                time.sleep(idle_backoff)

                if flush_interval and (time.monotonic() - last_flush_ts) >= flush_interval:
                    ck.flush()
                    last_flush_ts = time.monotonic()
                continue

            payload = msg["payload"]
            try:
                handle_inference_result(
                    payload=payload,
                    ck_writer=ck,
                    spec_repo=spec_repo,
                    qc_event_producer=qc_producer,
                )
                consumer.commit(msg["raw"])
            except Exception as e:
                log.exception("handle message failed: %s", e)

            if flush_interval and (time.monotonic() - last_flush_ts) >= flush_interval:
                ck.flush()
                last_flush_ts = time.monotonic()

    finally:

        try:
            ck.flush()
        except Exception:
            pass
        try:
            consumer.close()
        except Exception:
            pass
        log.info("Stream Processor stopped.")

if __name__ == "__main__":
    sys.exit(main())
