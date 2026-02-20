from __future__ import annotations

import uuid
from typing import Any, Dict, Optional

from fastapi import HTTPException

from app.infra.db import Postgres
from app.infra.storage import StorageAdapter
from app.refdata.rules import enforce_upload_rules, derive_primary_axis_deterministic
from app.services.audit import AuditService
from app.services.evidence_store import EvidenceStore
from app.services.fingerprint import FingerprintService
from app.services.registry import RegistryService
from app.settings import settings


class IngestionService:
    """EPIC-1 Ingestion Orchestrator (System-of-Event -> System-of-Insight).

    This service is intentionally *transactional* and light-weight:
      - validates request
      - computes sha256
      - applies dedupe policy
      - creates registry rows (document + version)
      - writes evidence immutably (or reuses existing file_id when policy allows)
      - writes audit events

    Canonicalization / chunking can be executed synchronously or via events.
    In this codebase, canonical pipeline is handled by downstream consumers.

    IMPORTANT PRIMARY_AXIS RULES:
      - If operator provides primary_axis: store it as truth with source="UPLOAD".
      - If not provided: derive deterministically and store source="DETERMINISTIC_RULE".
      - If an LLM suggests: store it ONLY as a suggestion (never overwrites truth).
    """

    def __init__(self, db: Postgres, storage: StorageAdapter) -> None:
        self.db = db
        self.storage = storage
        self.audit = AuditService(db)
        self.fp = FingerprintService()
        self.registry = RegistryService(db)
        self.evidence = EvidenceStore(db, storage)

    def _rules(self) -> Dict[str, Any]:
        row = self.db.fetchone(
            "SELECT rule_json FROM ref_rules WHERE rule_key=%s AND is_active=true",
            ("EPIC1_UPLOAD_RULES",),
        )
        return row["rule_json"] if row else {"required_fields": [], "max_pdf_mb": settings.MAX_PDF_MB}

    def _dedupe_match_existing(self, *, sha256: str, meta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Return existing IDs when sha256 + metadata match."""
        f = self.db.fetchone("SELECT * FROM evidence_files WHERE sha256=%s LIMIT 1", (sha256,))
        if not f:
            return None
        rows = self.db.fetchall(
            """SELECT v.*, d.title, d.jurisdiction, d.regulation_family, d.instrument_type
                 FROM document_versions v JOIN documents d ON d.document_id=v.document_id
                 WHERE v.raw_sha256=%s AND v.file_id=%s
                 ORDER BY v.uploaded_at DESC""",
            (sha256, f["file_id"]),
        )
        for r in rows:
            if (
                r["jurisdiction"] == meta["jurisdiction"]
                and r["regulation_family"] == meta["regulation_family"]
                and r["title"] == meta["title"]
                and r["instrument_type"] == meta["instrument_type"]
            ):
                return {
                    "document_id": r["document_id"],
                    "version_id": r["version_id"],
                    "file_id": f["file_id"],
                    "sha256": sha256,
                }
        return None

    def ingest_request(self, *, pdf_bytes: bytes, meta: Dict[str, Any], actor: str, force_new_version: bool) -> Dict[str, Any]:
        correlation_id = str(uuid.uuid4())

        # Validate request using configurable rules (refdata).
        enforce_upload_rules(self._rules(), meta)
        max_mb = int(self._rules().get("max_pdf_mb", settings.MAX_PDF_MB))
        if len(pdf_bytes) > max_mb * 1024 * 1024:
            raise HTTPException(status_code=400, detail=f"PDF too large; max={max_mb}MB")

        self.audit.write(
            entity_type="system",
            entity_id="epic1",
            action="EPIC1.REQUEST.RECEIVED",
            actor=actor,
            correlation_id=correlation_id,
            details={"meta": meta, "force_new_version": force_new_version},
        )

        sha = self.fp.sha256_bytes(pdf_bytes)
        self.audit.write(
            entity_type="system",
            entity_id="epic1",
            action="EPIC1.FINGERPRINT.COMPUTED",
            actor=actor,
            correlation_id=correlation_id,
            details={"raw_sha256": sha},
        )

        self.audit.write(
            entity_type="system",
            entity_id="epic1",
            action="EPIC1.DEDUP.CHECKED",
            actor=actor,
            correlation_id=correlation_id,
            details={"raw_sha256": sha},
        )

        existing = self._dedupe_match_existing(sha256=sha, meta=meta)
        if existing and not force_new_version:
            self.audit.write(
                entity_type="version",
                entity_id=existing["version_id"],
                action="EPIC1.DEDUP.SHORTCIRCUIT_RETURNED",
                actor=actor,
                correlation_id=correlation_id,
                details={"raw_sha256": sha},
            )
            # Surface current primary_axis provenance for the UI (read from documents).
            doc = self.db.fetchone("SELECT primary_axis_source FROM documents WHERE document_id=%s", (existing["document_id"],))
            return {
                "http_status": 200,
                "ingestion_status": "DEDUP_RETURN_EXISTING",
                "correlation_id": correlation_id,
                "primary_axis_source": (doc or {}).get("primary_axis_source"),
                **existing,
            }

        # Decide primary_axis truth value and source.
        # If user provided, it is treated as truth (UPLOAD). Otherwise derive deterministically.
        if meta.get("primary_axis") and str(meta["primary_axis"]).strip():
            primary_axis_value = str(meta["primary_axis"]).strip()
            primary_axis_source = "UPLOAD"
        else:
            primary_axis_value, primary_axis_source = derive_primary_axis_deterministic(
                jurisdiction=meta.get("jurisdiction"),
                title=meta.get("title"),
                regulation_family=meta.get("regulation_family"),
                instrument_type=meta.get("instrument_type"),
            )
            meta["primary_axis"] = primary_axis_value

        doc = self.registry.find_document_by_metadata(
            title=meta["title"],
            jurisdiction=meta["jurisdiction"],
            regulation_family=meta["regulation_family"],
            instrument_type=meta["instrument_type"],
        )
        if doc:
            document_id = doc["document_id"]
            created_new_doc = False
            # Guardrail: do not silently change an existing document's primary_axis.
            if meta.get("primary_axis") and doc.get("primary_axis") and meta["primary_axis"] != doc["primary_axis"]:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "primary_axis mismatch for existing document. "
                        f"Stored={doc['primary_axis']} Provided/Derived={meta['primary_axis']}"
                    ),
                )
            primary_axis_source_out = doc.get("primary_axis_source")
        else:
            document_id = self.registry.create_document(
                title=meta["title"],
                jurisdiction=meta["jurisdiction"],
                regulation_family=meta["regulation_family"],
                instrument_type=meta["instrument_type"],
                primary_axis=primary_axis_value,
                primary_axis_source=primary_axis_source,
            )
            created_new_doc = True
            primary_axis_source_out = primary_axis_source

        # Create version first (file_id is attached after evidence write)
        version_id = self.registry.create_version(
            document_id=document_id,
            tenant_id=meta["tenant_id"],
            effective_year=int(meta["effective_year"]),
            uploaded_by=actor,
            raw_sha256=sha,
            version_label=meta.get("version_label"),
            effective_date=meta.get("effective_date"),
            parent_version_id=meta.get("parent_version_id"),
            file_id=None,
            status="PENDING",
        )

        # Evidence handling: either reuse existing file_id (force_new_version) or create new immutable evidence
        evidence_row = self.evidence.find_by_sha(sha)
        if evidence_row and force_new_version:
            file_id = evidence_row["file_id"]
        else:
            file_id, _key, _uri = self.evidence.create_evidence(
                sha256=sha,
                pdf_bytes=pdf_bytes,
                document_id=document_id,
                version_id=version_id,
            )

        # Attach file_id to version + set uploaded_at when evidence is committed
        self.registry.set_version_file_id(version_id, file_id)

        if meta.get("parent_version_id"):
            self.registry.mark_parent_superseded(meta["parent_version_id"])
            self.audit.write(
                entity_type="version",
                entity_id=meta["parent_version_id"],
                action="EPIC1.PARENT_VERSION_SUPERSEDED",
                actor=actor,
                correlation_id=correlation_id,
                details={"child_version_id": version_id},
            )

        self.audit.write(
            entity_type="version",
            entity_id=version_id,
            action="EPIC1.REGISTRY.VERSION_CREATED",
            actor=actor,
            correlation_id=correlation_id,
            details={"document_id": document_id, "file_id": file_id, "raw_sha256": sha},
        )

        # Optional: store an LLM suggestion (derived-only). Never overwrites truth.
        primary_axis_suggestion = None
        if settings.ENABLE_LLM_PRIMARY_AXIS_SUGGESTION:
            suggested, confidence, details = self._suggest_primary_axis(meta)
            self.registry.upsert_primary_axis_suggestion(
                version_id=version_id,
                suggested_axis=suggested,
                model_name=settings.LLM_MODEL_NAME,
                model_version=settings.LLM_MODEL_VERSION,
                confidence=confidence,
                details_json=details,
            )
            self.audit.write(
                entity_type="version",
                entity_id=version_id,
                action="EPIC1.LLM.PRIMARY_AXIS_SUGGESTED",
                actor=actor,
                correlation_id=correlation_id,
                details={
                    "suggested_axis": suggested,
                    "confidence": confidence,
                    "model_name": settings.LLM_MODEL_NAME,
                    "model_version": settings.LLM_MODEL_VERSION,
                },
            )
            primary_axis_suggestion = {
                "value": suggested,
                "model_name": settings.LLM_MODEL_NAME,
                "model_version": settings.LLM_MODEL_VERSION,
                "confidence": confidence,
            }

        return {
            "http_status": 201,
            "ingestion_status": "CREATED_NEW_VERSION_REUSED_FILE"
            if (evidence_row and force_new_version)
            else ("CREATED_NEW_DOCUMENT_AND_VERSION" if created_new_doc else "CREATED_NEW_VERSION"),
            "correlation_id": correlation_id,
            "document_id": document_id,
            "version_id": version_id,
            "file_id": file_id,
            "sha256": sha,
            "primary_axis_source": primary_axis_source_out,
            "primary_axis_suggestion": primary_axis_suggestion,
        }

    def _suggest_primary_axis(self, meta: Dict[str, Any]):
        """Return a derived-only suggestion.

        This default implementation is intentionally conservative (rule-based stub).
        You can swap this with a real LLM call later while preserving the same
        output contract and audit logging.

        Returns: (suggested_axis, confidence, details_json)
        """
        # Use the deterministic rule as baseline, but mark it as a suggestion.
        suggested, _src = derive_primary_axis_deterministic(
            jurisdiction=meta.get("jurisdiction"),
            title=meta.get("title"),
            regulation_family=meta.get("regulation_family"),
            instrument_type=meta.get("instrument_type"),
        )
        return suggested, 0.55, {"method": "stub_rule_suggestion"}
