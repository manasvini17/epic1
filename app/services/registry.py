from __future__ import annotations

import json
from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import HTTPException

from app.infra.db import Postgres

ALLOWED_STATUSES = {"PENDING", "ACTIVE", "SUPERSEDED", "FAILED"}


class RegistryService:
    """System-of-Insight registry for EPIC-1.

    Owns the durable metadata entities:
      - documents (logical regulation)
      - document_versions (version chain + snapshot identity)
      - optional suggestion tables (derived-only suggestions)

    NOTE: This service does not handle object storage or PDF extraction.
    """

    def __init__(self, db: Postgres) -> None:
        self.db = db

    def find_document_by_metadata(
        self, *, title: str, jurisdiction: str, regulation_family: str, instrument_type: str
    ) -> Optional[Dict[str, Any]]:
        return self.db.fetchone(
            """SELECT * FROM documents
                 WHERE title=%s AND jurisdiction=%s AND regulation_family=%s AND instrument_type=%s
                 LIMIT 1""",
            (title, jurisdiction, regulation_family, instrument_type),
        )

    def create_document(
        self,
        *,
        title: str,
        jurisdiction: str,
        regulation_family: str,
        instrument_type: str,
        primary_axis: str,
        primary_axis_source: str,
    ) -> str:
        """Create a new documents row.

        primary_axis_source:
          - "UPLOAD" when provided by operator
          - "DETERMINISTIC_RULE" when derived by deterministic rules

        IMPORTANT: LLM suggestion MUST NOT set this field.
        """
        document_id = str(uuid4())
        self.db.execute(
            """INSERT INTO documents(
                    document_id, title, jurisdiction, regulation_family, instrument_type,
                    primary_axis, primary_axis_source
                 )
                 VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (document_id, title, jurisdiction, regulation_family, instrument_type, primary_axis, primary_axis_source),
        )
        return document_id

    def create_version(
        self,
        *,
        document_id: str,
        tenant_id: str,
        effective_year: int,
        uploaded_by: str,
        raw_sha256: str,
        version_label,
        effective_date,
        parent_version_id,
        file_id: Optional[str] = None,
        status: str = "PENDING",
    ) -> str:
        if status not in ALLOWED_STATUSES:
            raise HTTPException(status_code=400, detail=f"Invalid status: {status}")
        if parent_version_id:
            parent = self.db.fetchone(
                "SELECT version_id, document_id FROM document_versions WHERE version_id=%s", (parent_version_id,)
            )
            if not parent:
                raise HTTPException(status_code=400, detail="parent_version_id not found")
            if parent["document_id"] != document_id:
                raise HTTPException(status_code=400, detail="parent_version_id belongs to a different document_id")
        version_id = str(uuid4())
        self.db.execute(
            """INSERT INTO document_versions(
                   version_id, document_id, version_label, effective_date, status,
                   parent_version_id, tenant_id, effective_year, uploaded_by, raw_sha256, file_id)
                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                version_id,
                document_id,
                version_label,
                effective_date,
                status,
                parent_version_id,
                tenant_id,
                int(effective_year),
                uploaded_by,
                raw_sha256,
                file_id,
            ),
        )
        return version_id

    def set_version_file_id(self, version_id: str, file_id: str) -> None:
        self.db.execute(
            """UPDATE document_versions SET file_id=%s, uploaded_at=now(), updated_at=now()
                 WHERE version_id=%s""",
            (file_id, version_id),
        )

    def set_artifacts_json(self, version_id: str, artifacts_json: Dict[str, Any]) -> None:
        # Requires migration 003_artifacts_map.sql
        self.db.execute(
            """UPDATE document_versions SET artifacts_json=%s::jsonb, updated_at=now()
                 WHERE version_id=%s""",
            (json.dumps(artifacts_json), version_id),
        )

    def mark_parent_superseded(self, parent_version_id: str) -> None:
        self.db.execute(
            """UPDATE document_versions SET status='SUPERSEDED', updated_at=now()
                 WHERE version_id=%s AND status='ACTIVE'""",
            (parent_version_id,),
        )

    def set_status_pending_to_active(self, version_id: str) -> None:
        self.db.execute(
            """UPDATE document_versions SET status='ACTIVE', updated_at=now()
                 WHERE version_id=%s AND status='PENDING'""",
            (version_id,),
        )

    def set_status_pending_to_failed(self, version_id: str) -> None:
        self.db.execute(
            """UPDATE document_versions SET status='FAILED', updated_at=now()
                 WHERE version_id=%s AND status='PENDING'""",
            (version_id,),
        )

    # --- Derived-only suggestions (LLM or other analytics). Never treated as truth. ---

    def upsert_primary_axis_suggestion(
        self,
        *,
        version_id: str,
        suggested_axis: str,
        model_name: str,
        model_version: str,
        confidence: float,
        details_json: Dict[str, Any],
    ) -> None:
        self.db.execute(
            """INSERT INTO primary_axis_suggestions(
                   suggestion_id, version_id, suggested_axis, model_name, model_version, confidence, details_json
                 ) VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb)
                 ON CONFLICT (version_id) DO UPDATE SET
                   suggested_axis=EXCLUDED.suggested_axis,
                   model_name=EXCLUDED.model_name,
                   model_version=EXCLUDED.model_version,
                   confidence=EXCLUDED.confidence,
                   details_json=EXCLUDED.details_json,
                   updated_at=now()""",
            (
                str(uuid4()),
                version_id,
                suggested_axis,
                model_name,
                model_version,
                float(confidence),
                json.dumps(details_json),
            ),
        )

    def get_primary_axis_suggestion(self, *, version_id: str) -> Optional[Dict[str, Any]]:
        return self.db.fetchone(
            """SELECT version_id, suggested_axis, model_name, model_version, confidence, created_at, updated_at
                 FROM primary_axis_suggestions WHERE version_id=%s""",
            (version_id,),
        )
