from __future__ import annotations
from typing import Optional, Dict, Any
from pathlib import Path
import json
import os
import threading


class MockQCEventProducer:
    def __init__(self, out_path: str | Path = "data/processed/qc_events.jsonl"):
        self.path = Path(out_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def publish(self, event: Dict[str, Any]) -> str:
        with self._lock:
            with self.path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(event, ensure_ascii=False) + "\n")
        return str(event.get("event_id", ""))

    def healthy(self) -> bool:
        try:
            self.path.touch(exist_ok=True)
            return True
        except Exception:
            return False


class KafkaAvroQCEventProducer:
    _value_schema_str = json.dumps({
        "type": "record",
        "name": "AoiQCEventV1",
        "namespace": "aoi",
        "fields": [
            {"name": "event_id", "type": "string"},
            {"name": "ts_ms", "type": "long"},
            {"name": "product_code", "type": "string"},
            {"name": "station_id", "type": "string"},
            {"name": "severity", "type": "string"},
            {"name": "reason", "type": "string"},
            {"name": "overlay_url", "type": "string"},
            {"name": "defect_count", "type": "int"}
        ]
    })

    def __init__(self, brokers: str, schema_registry_url: str, topic: str):
        try:
            from confluent_kafka.schema_registry import SchemaRegistryClient 
            from confluent_kafka.serialization import StringSerializer
            from confluent_kafka.schema_registry.avro import AvroSerializer 
            from confluent_kafka import Producer
        except Exception as e:
            raise RuntimeError(
                "KafkaAvroQCEventProducer requires confluent_kafka. "
                "Install: pip install 'confluent-kafka[avro]'"
            ) from e

        self._Producer = Producer
        self._StringSerializer = StringSerializer
        self._AvroSerializer = AvroSerializer
        self._SchemaRegistryClient = SchemaRegistryClient

        self.topic = topic
        self._sr = SchemaRegistryClient({"url": schema_registry_url})
        self._value_serializer = AvroSerializer(self._sr, self._value_schema_str)
        self._key_serializer = StringSerializer("utf_8")
        self._producer = Producer({"bootstrap.servers": brokers})

    def publish(self, event: Dict[str, Any]) -> str:
        key = str(event.get("event_id", ""))
        def cb(err, msg):
            # có thể log delivery ở đây nếu muốn
            pass
        self._producer.produce(
            topic=self.topic,
            key=self._key_serializer(key, None),
            value=self._value_serializer(event, None),
            on_delivery=cb
        )
        self._producer.poll(0)
        return key

    def healthy(self) -> bool:
        try:
            self._producer.poll(0)
            return True
        except Exception:
            return False


# ----------------- Facade chọn chế độ -----------------
class QCEventProducer:
    def __init__(
        self,
        brokers: str,
        schema_registry_url: str,
        topic: str,
        mock: Optional[bool] = None,
        jsonl_path: str | Path = "data/processed/qc_events.jsonl",
    ):
        mode_env = os.getenv("AOI_QC_EVENTS_MODE", "").lower().strip()
        if mock is None:
            mock = (mode_env == "mock")

        if mock:
            self._impl = MockQCEventProducer(jsonl_path)
        else:
            try:
                self._impl = KafkaAvroQCEventProducer(brokers, schema_registry_url, topic)
            except Exception:
                # fallback mock nếu thiếu thư viện hoặc registry
                self._impl = MockQCEventProducer(jsonl_path)

    def publish(self, event: Dict[str, Any]) -> str:
        return self._impl.publish(event)

    def healthy(self) -> bool:
        return bool(self._impl.healthy())
