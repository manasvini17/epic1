import asyncio
from app.infra.kafka import make_consumer
from app.settings import settings
from app.infra.db import Postgres
from app.services.soi_projector import SoIProjector

async def main():
    consumer = await make_consumer(settings.TOPIC_EVENTS, group_id="epic1-soi")
    db = Postgres()
    projector = SoIProjector(db)
    try:
        async for msg in consumer:
            ev = msg.value
            projector.project(ev.get("event_type",""), ev.get("payload") or {})
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
