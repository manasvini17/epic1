import json
from typing import Any, Dict
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from app.settings import settings

def _dumps(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")

def _loads(b: bytes) -> Dict[str, Any]:
    return json.loads(b.decode("utf-8"))

async def make_producer() -> AIOKafkaProducer:
    p = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        client_id=settings.KAFKA_CLIENT_ID,
        value_serializer=_dumps,
    )
    await p.start()
    return p

async def make_consumer(topic: str, group_id: str) -> AIOKafkaConsumer:
    c = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        group_id=group_id,
        client_id=settings.KAFKA_CLIENT_ID,
        value_deserializer=_loads,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
    await c.start()
    return c
