"""Lazy generation of per-character artifacts.

These artifacts are **not** generated in the default upload pipeline because
they can be large (storage + CPU). They are generated on demand for highlight-
level traceability workflows.

Guardrails:
 - Does NOT mutate evidence.
 - Writes derived_artifacts(kind=char_map|char_boxes) immutably.
 - Logs audit events.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional

import fitz  # PyMuPDF

from app.services.artifacts import ArtifactService
from app.services.audit import AuditLog
from app.settings import settings


class CharArtifactsService:
    def __init__(self, *, db, storage) -> None:
        self.db = db
        self.storage = storage
        self.artifacts = ArtifactService(db)
        self.audit = AuditLog(db)

    def _get_version_row(self, version_id: str) -> Dict[str, Any]:
        v = self.db.fetchone("SELECT * FROM document_versions WHERE version_id=%s", (version_id,))
        if not v:
            raise ValueError("version not found")
        return v

    def _get_pdf_bytes(self, file_id: str) -> bytes:
        f = self.db.fetchone("SELECT * FROM evidence_files WHERE file_id=%s", (file_id,))
        if not f:
            raise ValueError("evidence file not found")
        # Resolve uri -> key/path, then read.
        uri = f["storage_uri"]
        from app.infra.storage import parse_storage_uri

        scheme, loc = parse_storage_uri(uri)
        if scheme == "s3":
            return self.storage.get_bytes(loc)
        # local
        with open(loc, "rb") as fp:
            return fp.read()

    def _already(self, version_id: str, kind: str) -> Optional[str]:
        row = self.db.fetchone(
            """SELECT artifact_id FROM derived_artifacts
                 WHERE version_id=%s AND kind=%s
                 ORDER BY created_at DESC LIMIT 1""",
            (version_id, kind),
        )
        return row["artifact_id"] if row else None

    def ensure_char_map(self, *, version_id: str, actor: str) -> Dict[str, Any]:
        existing = self._already(version_id, "char_map")
        if existing:
            return {"version_id": version_id, "char_map_id": existing, "status": "EXISTS"}

        v = self._get_version_row(version_id)
        if not v.get("file_id"):
            return {"version_id": version_id, "status": "NOT_READY"}

        pdf_bytes = self._get_pdf_bytes(v["file_id"])
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        if doc.page_count > settings.CHAR_ARTIFACT_MAX_PAGES:
            return {"version_id": version_id, "status": "REJECTED", "reason": "too_many_pages"}

        # Minimal char_map: per-page text in reading order + sha256.
        pages = []
        for i in range(doc.page_count):
            text = doc.load_page(i).get_text("text")
            pages.append({"page": i + 1, "text": text})
        payload = {
            "version_id": version_id,
            "raw_sha256": v["raw_sha256"],
            "pages": pages,
        }
        artifact_id = self.artifacts.store_json_artifact(
            version_id=version_id,
            kind="char_map",
            obj=payload,
            generator_name="pymupdf",
            generator_version=f"{settings.EXTRACTOR_VERSION}|char_map@1.0.0",
            key=f"canonical/{version_id}/char_map.json",
        )

        self.audit.write(
            entity_type="artifact",
            entity_id=artifact_id,
            action="EPIC1.CANONICALIZE.CHAR_MAP_GENERATED",
            actor=actor,
            correlation_id="-",
            details={"version_id": version_id, "kind": "char_map"},
        )
        return {"version_id": version_id, "char_map_id": artifact_id, "status": "CREATED"}

    def ensure_char_boxes(self, *, version_id: str, actor: str) -> Dict[str, Any]:
        existing = self._already(version_id, "char_boxes")
        if existing:
            return {"version_id": version_id, "char_boxes_id": existing, "status": "EXISTS"}

        v = self._get_version_row(version_id)
        if not v.get("file_id"):
            return {"version_id": version_id, "status": "NOT_READY"}

        pdf_bytes = self._get_pdf_bytes(v["file_id"])
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        if doc.page_count > settings.CHAR_ARTIFACT_MAX_PAGES:
            return {"version_id": version_id, "status": "REJECTED", "reason": "too_many_pages"}

        # char_boxes: per-page list of character bounding boxes.
        pages = []
        for i in range(doc.page_count):
            page = doc.load_page(i)
            raw = page.get_text("rawdict")
            char_items = []
            for block in raw.get("blocks", []):
                for line in block.get("lines", []):
                    for span in line.get("spans", []):
                        for ch in span.get("chars", []):
                            char_items.append({
                                "c": ch.get("c"),
                                "bbox": ch.get("bbox"),
                            })
            pages.append({"page": i + 1, "chars": char_items})

        payload = {"version_id": version_id, "raw_sha256": v["raw_sha256"], "pages": pages}
        artifact_id = self.artifacts.store_json_artifact(
            version_id=version_id,
            kind="char_boxes",
            obj=payload,
            generator_name="pymupdf",
            generator_version=f"{settings.LAYOUT_VERSION}|char_boxes@1.0.0",
            key=f"canonical/{version_id}/char_boxes.json",
        )
        self.audit.write(
            entity_type="artifact",
            entity_id=artifact_id,
            action="EPIC1.CANONICALIZE.CHAR_BOXES_GENERATED",
            actor=actor,
            correlation_id="-",
            details={"version_id": version_id, "kind": "char_boxes"},
        )
        return {"version_id": version_id, "char_boxes_id": artifact_id, "status": "CREATED"}
