from __future__ import annotations

from fastapi import APIRouter, UploadFile, File, Form, Request, HTTPException
from typing import Optional

from app.infra.db import Postgres
from app.infra.auth import require_auth, require_role, ROLE_OPERATOR, ROLE_AUDITOR
from app.infra.storage import make_storage, parse_storage_uri
from app.services.ingestion import IngestionService
from app.contracts.models import UploadResponse
from app.settings import settings
from app.infra.kafka import make_producer
from app.contracts.events import DomainEvent

router = APIRouter(prefix="/api/v1/epic1", tags=["EPIC-1"])


@router.post("/regulations/upload", response_model=UploadResponse)
async def upload_regulation(
    request: Request,
    file: UploadFile = File(...),
    jurisdiction: str = Form(...),
    title: str = Form(...),
    regulation_family: str = Form(...),
    instrument_type: str = Form(...),
    # primary_axis can be provided by the Operator (truth) OR derived by deterministic rules.
    # If an LLM suggests, that suggestion is stored separately and never treated as truth.
    primary_axis: Optional[str] = Form(None),
    tenant_id: str = Form(...),
    effective_year: int = Form(...),
    effective_date: Optional[str] = Form(None),
    version_label: Optional[str] = Form(None),
    parent_version_id: Optional[str] = Form(None),
    force_new_version: Optional[bool] = Form(False),
):
    claims = require_auth(request)
    require_role(claims, [ROLE_OPERATOR])
    actor = claims.get("sub", "unknown")

    if file.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="File must be application/pdf")

    pdf_bytes = await file.read()

    db = Postgres()
    storage = make_storage()
    svc = IngestionService(db, storage)

    meta = {
        "jurisdiction": jurisdiction,
        "title": title,
        "regulation_family": regulation_family,
        "instrument_type": instrument_type,
        "primary_axis": primary_axis,
        "tenant_id": tenant_id,
        "effective_year": int(effective_year),
        "effective_date": effective_date,
        "version_label": version_label,
        "parent_version_id": parent_version_id,
    }

    res = svc.ingest_request(
        pdf_bytes=pdf_bytes,
        meta=meta,
        actor=actor,
        force_new_version=bool(force_new_version),
    )

    # Emit an event only when a new version is created.
    if res["http_status"] == 201:
        producer = await make_producer()
        try:
            ev = DomainEvent(
                event_type="EPIC1.REGISTRY.VERSION_CREATED",
                correlation_id=res["correlation_id"],
                actor=actor,
                entity_type="version",
                entity_id=res["version_id"],
                payload={
                    "document_id": res["document_id"],
                    "version_id": res["version_id"],
                    "file_id": res["file_id"],
                    "raw_sha256": res["sha256"],
                },
            )
            await producer.send_and_wait(settings.TOPIC_EVENTS, ev.model_dump())
        finally:
            await producer.stop()

    return UploadResponse(
        document_id=res["document_id"],
        version_id=res["version_id"],
        file_id=res["file_id"],
        fingerprint_sha256=res["sha256"],
        ingestion_status=res["ingestion_status"],
        artifacts=res.get(
            "artifacts",
            {"stable_text_id": None, "page_map_id": None, "layout_map_id": None, "chunk_set_id": None},
        ),
        correlation_id=res.get("correlation_id"),
        # New: make primary_axis provenance explicit.
        primary_axis_source=res.get("primary_axis_source"),
        primary_axis_suggestion=res.get("primary_axis_suggestion"),
    )


@router.get("/documents/{document_id}")
async def get_document(request: Request, document_id: str):
    claims = require_auth(request)
    require_role(claims, [ROLE_OPERATOR, ROLE_AUDITOR])
    db = Postgres()
    doc = db.fetchone("SELECT * FROM documents WHERE document_id=%s", (document_id,))
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    return doc


@router.get("/versions/{version_id}")
async def get_version(request: Request, version_id: str):
    claims = require_auth(request)
    require_role(claims, [ROLE_OPERATOR, ROLE_AUDITOR])
    db = Postgres()
    v = db.fetchone("SELECT * FROM document_versions WHERE version_id=%s", (version_id,))
    if not v:
        raise HTTPException(status_code=404, detail="Version not found")
    # Return full version row, including artifacts_json (if migration applied).
    return v


@router.get("/files/{file_id}")
async def get_file(request: Request, file_id: str):
    claims = require_auth(request)
    require_role(claims, [ROLE_OPERATOR, ROLE_AUDITOR])
    db = Postgres()
    f = db.fetchone("SELECT * FROM evidence_files WHERE file_id=%s", (file_id,))
    if not f:
        raise HTTPException(status_code=404, detail="File not found")
    storage = make_storage()
    scheme, key_or_path = parse_storage_uri(f["storage_uri"])
    if scheme == "s3":
        url = storage.get_signed_url(key_or_path, settings.SIGNED_URL_EXPIRES_SEC)
        return {"file_id": file_id, "signed_url": url, "mime_type": f["mime_type"], "sha256": f["sha256"]}
    return {"file_id": file_id, "storage_uri": f["storage_uri"], "mime_type": f["mime_type"], "sha256": f["sha256"]}


@router.get("/artifacts/{artifact_id}")
async def get_artifact(request: Request, artifact_id: str):
    claims = require_auth(request)
    require_role(claims, [ROLE_OPERATOR, ROLE_AUDITOR])
    db = Postgres()
    a = db.fetchone("SELECT * FROM derived_artifacts WHERE artifact_id=%s", (artifact_id,))
    if not a:
        raise HTTPException(status_code=404, detail="Artifact not found")
    return a
