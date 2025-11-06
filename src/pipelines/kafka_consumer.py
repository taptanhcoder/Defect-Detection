from __future__ import annotations
from typing import Iterator, Dict, Any, Optional, List
from pathlib import Path
import json
import time
import logging
import yaml
import os

try:
    from confluent_kafka import Consumer, KafkaException, KafkaError 
except Exception as e:
    raise RuntimeError(
        "pipelines.kafka_consumer: cần confluent-kafka. Cài: pip install confluent-kafka"
    ) from e

log = logging.getLogger("aoi.kafka_consumer")


def load_streaming_config(path: str | Path) -> Dict[str, Any]:
    p = Path(path).resolve()
    if not p.exists():
        raise FileNotFoundError(f"streaming.yaml not found: {p}")
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


class KafkaJSONConsumer:
    def __init__(
        self,
        brokers: str,
        group_id: str,
        topics: List[str],
        auto_offset_reset: str = "earliest",
        session_timeout_ms: int = 10000,
        max_poll_interval_ms: int = 300000,
        enable_auto_commit: bool = False,
        extra: Optional[Dict[str, Any]] = None,
    ):
        cfg = {
            "bootstrap.servers": brokers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": enable_auto_commit,
            "session.timeout.ms": session_timeout_ms,
            "max.poll.interval.ms": max_poll_interval_ms,
        }
        if extra:
            cfg.update(extra)

        self._consumer = Consumer(cfg)
        self._topics = topics

    def subscribe(self):
        self._consumer.subscribe(self._topics)
        log.info("Kafka subscribed topics=%s", self._topics)

    def close(self):
        try:
            self._consumer.close()
        except Exception:
            pass

    def iter_messages(self, poll_timeout: float = 1.0) -> Iterator[Dict[str, Any]]:
        while True:
            msg = self._consumer.poll(poll_timeout)
            if msg is None:
                yield {"payload": None, "raw": None}
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.warning("Kafka poll error: %s", msg.error())
                time.sleep(0.5)
                continue

            key = msg.key().decode("utf-8") if msg.key() else None
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                log.error("JSON decode failed (skip message). key=%s err=%s", key, e)
                try:
                    self._consumer.commit(message=msg, asynchronous=False)
                except Exception:
                    pass
                continue

            yield {
                "payload": payload,
                "raw": msg,
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": key,
            }

    def commit(self, raw_msg) -> None:
        try:
            self._consumer.commit(message=raw_msg, asynchronous=False)
        except Exception as e:
            log.error("Commit failed: %s", e)

    @classmethod
    def from_yaml(cls, config_path: str | Path):
        cfg = load_streaming_config(config_path)
        kc = cfg.get("kafka", {}) or {}
        topics = []
        if "topic_inference_results" in kc:
            topics.append(str(kc["topic_inference_results"]))
        elif "topics" in kc:
            topics = [str(t) for t in kc["topics"]]
        if not topics:
            raise ValueError("No topics configured in streaming.yaml (kafka.topic_inference_results or kafka.topics)")

        return cls(
            brokers=str(kc.get("brokers", "kafka:9092")),
            group_id=str(kc.get("group_id", "aoi.processor.v1")),
            topics=topics,
            auto_offset_reset=str(kc.get("auto_offset_reset", "earliest")),
        )
