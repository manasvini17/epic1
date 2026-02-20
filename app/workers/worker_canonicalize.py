import asyncio
from uuid import uuid4
from app.infra.kafka import make_consumer, make_producer
from app.settings import settings
from app.infra.db import Postgres
from app.infra.storage import make_storage, parse_storage_uri
from app.services.canonical_pipeline import CanonicalTextPipeline
from app.services.artifacts import ArtifactService
from app.services.audit import AuditService
from app.services.registry import RegistryService
from app.services.chunking import SimpleDeterministicChunker
from app.contracts.events import DomainEvent

async def main():
    consumer = await make_consumer(settings.TOPIC_EVENTS, group_id="epic1-canonicalize")
    producer = await make_producer()
    db = Postgres()
    storage = make_storage()
    pipeline = CanonicalTextPipeline()
    artifacts = ArtifactService(db, storage)
    audit = AuditService(db)
    registry = RegistryService(db)
    chunker = SimpleDeterministicChunker()

    try:
        async for msg in consumer:
            ev = msg.value
            if ev.get("event_type") != "EPIC1.REGISTRY.VERSION_CREATED":
                continue

            correlation_id = ev["correlation_id"]
            actor = ev["actor"]
            version_id = ev["payload"]["version_id"]
            file_id = ev["payload"]["file_id"]

            f = db.fetchone("SELECT * FROM evidence_files WHERE file_id=%s", (file_id,))
            if not f:
                registry.set_status_pending_to_failed(version_id)
                audit.write(entity_type="version", entity_id=version_id, action="EPIC1.INGESTION.FAILED",
                            actor=actor, correlation_id=correlation_id, details={"reason": "evidence_files not found"})
                continue

            scheme, key_or_path = parse_storage_uri(f["storage_uri"])
            try:
                pdf_bytes = storage.get_bytes(key_or_path) if scheme == "s3" else open(key_or_path, "rb").read()
            except Exception as e:
                registry.set_status_pending_to_failed(version_id)
                audit.write(entity_type="version", entity_id=version_id, action="EPIC1.INGESTION.FAILED",
                            actor=actor, correlation_id=correlation_id, details={"reason": "evidence read failed", "error": str(e)})
                continue

            try:
                stable_text, page_map, layout_map = pipeline.extract(pdf_bytes)
            except Exception as e:
                registry.set_status_pending_to_failed(version_id)
                audit.write(entity_type="version", entity_id=version_id, action="EPIC1.INGESTION.FAILED",
                            actor=actor, correlation_id=correlation_id, details={"reason": "canonicalization failed", "error": str(e)})
                continue

            canonical_ids = artifacts.store_canonical(
                version_id=version_id,
                stable_text=stable_text,
                page_map=page_map,
                layout_map=layout_map,
                extractor_version=settings.EXTRACTOR_VERSION,
                layout_version=settings.LAYOUT_VERSION,
            )

            # Deterministic chunking policy comes from config (no hard-coded values)
            chunks, manifest = chunker.chunk(
                stable_text=stable_text,
                page_map=page_map,
                max_chars=settings.CHUNK_MAX_CHARS,
                overlap_chars=settings.CHUNK_OVERLAP_CHARS,
            )
            chunk_set_obj = {
                "version_id": version_id,
                "chunk_schema_version": settings.CHUNK_SCHEMA_VERSION,
                "chunker_version": settings.CHUNKER_VERSION,
                "manifest": manifest,
                "chunks": chunks,
            }
            chunk_set_artifact_id = artifacts.store_chunk_set(
                version_id=version_id,
                chunk_set_obj=chunk_set_obj,
                generator_version=f"{settings.CHUNKER_VERSION}|{settings.CHUNK_SCHEMA_VERSION}",
            )

            # Future-proof Retrieval Manifest (agents/RAG consume this, never raw bytes)
            retrieval_manifest = {
                "version_id": version_id,
                "raw_sha256": f["sha256"],
                "canonical_artifacts": {
                    "stable_text_id": canonical_ids["stable_text_id"],
                    "page_map_id": canonical_ids["page_map_id"],
                    "layout_map_id": canonical_ids["layout_map_id"],
                },
                "chunk_sets": [
                    {
                        "chunk_set_id": chunk_set_artifact_id,
                        "chunker_version": settings.CHUNKER_VERSION,
                        "chunk_schema_version": settings.CHUNK_SCHEMA_VERSION,
                    }
                ],
                "embedding_sets": [],
                "policies": {"citation_required": True, "max_context_tokens": 8192},
                "provenance": {
                    "extractor_version": settings.EXTRACTOR_VERSION,
                    "layout_version": settings.LAYOUT_VERSION,
                    "chunker_version": settings.CHUNKER_VERSION,
                },
            }
            retrieval_manifest_id = artifacts.store_json_artifact(
                version_id=version_id,
                kind="retrieval_manifest",
                obj=retrieval_manifest,
                generator_name="manifest",
                generator_version="retrieval_manifest@1.0.0",
                key=f"indexes/{version_id}/retrieval_manifest.json",
            )

            for ch in chunks:
                cid = str(uuid4())
                db.execute(
                    """INSERT INTO chunks(chunk_id, version_id, chunk_set_artifact_id, chunk_schema_version,
                                            start_char, end_char, page_start, page_end, bbox_refs, text_sha256)
                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (cid, version_id, chunk_set_artifact_id, settings.CHUNK_SCHEMA_VERSION,
                     int(ch["start_char"]), int(ch["end_char"]), int(ch["page_start"]), int(ch["page_end"]),
                     db.json(ch.get("bbox_refs")), ch["text_sha256"])
                )

            registry.set_status_pending_to_active(version_id)

            # Keep a compact map on the version row for UI and downstream services.
            # Requires migration 003_artifacts_map.sql.
            try:
                registry.set_artifacts_json(
                    version_id,
                    {
                        **canonical_ids,
                        "chunk_set_id": chunk_set_artifact_id,
                        "retrieval_manifest_id": retrieval_manifest_id,
                    },
                )
            except Exception:
                # Don't fail ingestion if the optional column isn't applied yet.
                pass

            llm_ev = DomainEvent(
                event_type="EPIC1.LLM.DERIVATION_REQUESTED",
                correlation_id=correlation_id,
                actor=actor,
                entity_type="version",
                entity_id=version_id,
                payload={"version_id": version_id, "stable_text_artifact_id": canonical_ids["stable_text_id"]},
            )
            await producer.send_and_wait(settings.TOPIC_EVENTS, llm_ev.model_dump())

            done = DomainEvent(
                event_type="EPIC1.INGESTION.COMPLETED",
                correlation_id=correlation_id,
                actor=actor,
                entity_type="version",
                entity_id=version_id,
                payload={"version_id": version_id},
            )
            await producer.send_and_wait(settings.TOPIC_EVENTS, done.model_dump())
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())
