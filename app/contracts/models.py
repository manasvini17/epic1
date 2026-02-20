from __future__ import annotations

from datetime import date, datetime
from typing import Dict, Literal, Optional

from pydantic import BaseModel, Field

Status = Literal["PENDING", "ACTIVE", "SUPERSEDED", "FAILED"]


class PrimaryAxisSuggestion(BaseModel):
    """Derived-only suggestion.

    Guardrail:
      - This MUST NOT be used as source-of-truth.
      - primary_axis in documents remains the truth.

    Stored for UX assistance + auditability.
    """

    value: str
    model_name: str
    model_version: str
    confidence: float


class UploadResponse(BaseModel):
    document_id: str
    version_id: str
    file_id: str
    fingerprint_sha256: str
    ingestion_status: str
    artifacts: Dict[str, Optional[str]] = Field(default_factory=dict)
    correlation_id: Optional[str] = None

    # New: primary_axis provenance (truth value is on the document)
    primary_axis_source: Optional[str] = None  # UPLOAD | DETERMINISTIC_RULE
    primary_axis_suggestion: Optional[PrimaryAxisSuggestion] = None


class DocumentDTO(BaseModel):
    document_id: str
    title: str
    jurisdiction: str
    regulation_family: str
    instrument_type: str
    primary_axis: str
    primary_axis_source: str
    created_at: datetime


class VersionDTO(BaseModel):
    version_id: str
    document_id: str
    status: Status
    version_label: Optional[str]
    effective_date: Optional[date]
    parent_version_id: Optional[str]
    uploaded_by: str
    uploaded_at: datetime
    raw_sha256: str
    tenant_id: str
    effective_year: int
    file_id: Optional[str]
    artifacts_json: Optional[Dict[str, str]] = None
