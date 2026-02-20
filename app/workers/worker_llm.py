import asyncio
from app.infra.kafka import make_consumer, make_producer
from app.settings import settings
from app.infra.db import Postgres
from app.infra.storage import make_storage, parse_storage_uri
from app.services.artifacts import ArtifactService
from app.services.llm_orchestrator import LLMOrchestrator
from app.services.audit import AuditService
from app.contracts.events import DomainEvent

async def main():
    consumer = await make_consumer(settings.TOPIC_EVENTS, group_id="epic1-llm")
    producer = await make_producer()
    db = Postgres()
    storage = make_storage()
    artifacts = ArtifactService(db, storage)
    llm = LLMOrchestrator(db, artifacts)
    audit = AuditService(db)

    try:
        async for msg in consumer:
            ev = msg.value
            if ev.get("event_type") != "EPIC1.LLM.DERIVATION_REQUESTED":
                continue

            correlation_id = ev["correlation_id"]
            actor = ev["actor"]
            version_id = ev["payload"]["version_id"]
            stable_text_artifact_id = ev["payload"]["stable_text_artifact_id"]

            a = db.fetchone("SELECT * FROM derived_artifacts WHERE artifact_id=%s", (stable_text_artifact_id,))
            if not a:
                audit.write(entity_type="version", entity_id=version_id, action="EPIC1.INGESTION.FAILED",
                            actor=actor, correlation_id=correlation_id, details={"reason": "stable_text artifact missing"})
                continue

            scheme, key_or_path = parse_storage_uri(a["storage_uri"])
            stable_text = storage.get_bytes(key_or_path).decode("utf-8", errors="replace") if scheme == "s3" else open(key_or_path, "r", encoding="utf-8").read()

            res = await llm.summarize_for_indexing(version_id=version_id, stable_text=stable_text)
            audit.write(entity_type="version", entity_id=version_id, action="EPIC1.LLM.DERIVATION_COMPLETED",
                        actor=actor, correlation_id=correlation_id, details=res)

            out = DomainEvent(
                event_type="EPIC1.LLM.DERIVATION_COMPLETED",
                correlation_id=correlation_id,
                actor=actor,
                entity_type="version",
                entity_id=version_id,
                payload={"version_id": version_id, **res}
            )
            await producer.send_and_wait(settings.TOPIC_EVENTS, out.model_dump())
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())
