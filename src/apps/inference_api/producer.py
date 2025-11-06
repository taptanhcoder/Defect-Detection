from __future__ import annotations
from typing import Optional, Dict, Any
from pathlib import Path
import json
import time
import os
import threading

class MockJsonlProducer:
    def __init__(self, out_path: str | Path = "data/processed/inference_results.jsonl"):
        self.path = Path(out_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def publish(self, payload: Dict[str, Any]) -> str:

        with self._lock:
            with self.path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        return str(payload.get("event_id", ""))

    def healthy(self) -> bool:
        try:
            self.path.touch(exist_ok=True)
            return True
        except Exception:
            return False


class KafkaAvroProducer:

    _value_schema_str = json.dumps({
        "type": "record",
        "name": "AoiInferenceResultV1",
        "namespace": "aoi",
        "fields": [
            {"name": "event_id", "type": "string"},
            {"name": "ts_ms", "type": "long"},
            {"name": "product_code", "type": "string"},
            {"name": "station_id", "type": "string"},
            {"name": "board_serial", "type": ["null", "string"], "default": None},

            {"name": "model_family", "type": "string"},
            {"name": "model_version", "type": "string"},
            {"name": "latency_ms", "type": "int"},

            {"name": "aql_mini_decision", "type": "string"},

            {"name": "measures", "type": ["null", {
                "type": "record", "name": "Measures",
                "fields": [
                    {"name": "trace_width_um", "type": ["null","double"], "default": None},
                    {"name": "clearance_um", "type": ["null","double"], "default": None},
                    {"name": "pad_offset_um", "type": ["null","double"], "default": None}
                ]
            }], "default": None},

            {"name": "defects", "type": {
                "type": "array", "items": {
                    "name": "DefectItem", "type": "record",
                    "fields": [
                        {"name": "cls", "type": "string"},
                        {"name": "score", "type": "double"},
                        {"name": "bbox", "type": {
                            "type": "record", "name": "BBox", "fields": [
                                {"name": "x", "type": "int"},
                                {"name": "y", "type": "int"},
                                {"name": "w", "type": "int"},
                                {"name": "h", "type": "int"}
                            ]
                        }},
                        {"name": "mask_url", "type": ["null","string"], "default": None}
                    ]
                }
            }},

            {"name": "image_urls", "type": {
                "type": "record", "name": "ImageUrls",
                "fields": [
                    {"name": "raw_url", "type": "string"},
                    {"name": "overlay_url", "type": "string"},
                    {"name": "tiles", "type": {"type":"array", "items":"string"}, "default": []}
                ]
            }},

            {"name": "meta", "type": {
                "type": "record", "name": "Meta",
                "fields": [
                    {"name": "capture_id", "type": ["null","string"], "default": None},
                    {"name": "notes", "type": ["null","string"], "default": None}
                ]
            }}
        ]
    })

    def __init__(self, brokers: str, schema_registry_url: str, topic: str):
        try:
            from confluent_kafka.schema_registry import SchemaRegistryClient # type: ignore
            from confluent_kafka.serialization import StringSerializer # type: ignore
            from confluent_kafka.schema_registry.avro import AvroSerializer # type: ignore
            from confluent_kafka import Producer # type: ignore
        except Exception as e:
            raise RuntimeError(
                "KafkaAvroProducer requires confluent_kafka. Install: "
                "pip install 'confluent-kafka[avro]'" ) from e

        self._Producer = Producer
        self._StringSerializer = StringSerializer
        self._AvroSerializer = AvroSerializer
        self._SchemaRegistryClient = SchemaRegistryClient

        self.topic = topic
        self._sr = SchemaRegistryClient({"url": schema_registry_url})
        self._value_serializer = AvroSerializer(self._sr, self._value_schema_str)
        self._key_serializer = StringSerializer("utf_8")
        self._producer = Producer({"bootstrap.servers": brokers})

    def publish(self, payload: Dict[str, Any]) -> str:
        key = str(payload.get("event_id", ""))

        def cb(err, msg):

            if err:

                pass
        self._producer.produce(
            topic=self.topic,
            key=self._key_serializer(key, None),
            value=self._value_serializer(payload, None),
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



class EventProducer:

    def __init__(self,
                 brokers: str,
                 schema_registry_url: str,
                 topic: str,
                 mock: Optional[bool] = None,
                 jsonl_path: str | Path = "data/processed/inference_results.jsonl"):
        mode_env = os.getenv("AOI_PRODUCER_MODE", "").lower().strip()
        if mock is None:
            mock = (mode_env == "mock")

        self._impl = None  
        if mock:
            self._impl = MockJsonlProducer(jsonl_path)
        else:
            try:
                self._impl = KafkaAvroProducer(brokers, schema_registry_url, topic)
            except Exception:
                self._impl = MockJsonlProducer(jsonl_path)

    def publish(self, payload: Dict[str, Any]) -> str:
        assert self._impl is not None
        return self._impl.publish(payload)

    def healthy(self) -> bool:
        assert self._impl is not None
        return bool(self._impl.healthy())
