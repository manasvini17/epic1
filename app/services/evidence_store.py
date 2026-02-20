from __future__ import annotations
from uuid import uuid4
from typing import Tuple
from app.infra.db import Postgres
from app.infra.storage import StorageAdapter

class EvidenceStore:
    def __init__(self, db: Postgres, storage: StorageAdapter) -> None:
        self.db = db
        self.storage = storage

    def find_by_sha(self, sha256: str):
        return self.db.fetchone(
            "SELECT * FROM evidence_files WHERE sha256=%s ORDER BY created_at DESC LIMIT 1",
            (sha256,),
        )

    def get(self, file_id: str):
        return self.db.fetchone("SELECT * FROM evidence_files WHERE file_id=%s", (file_id,))

    def create_evidence(self, *, sha256: str, pdf_bytes: bytes, document_id: str, version_id: str) -> Tuple[str, str, str]:
        file_id = str(uuid4())
        key = f"evidence/{document_id}/{version_id}/{file_id}.pdf"
        storage_uri = self.storage.put_bytes_write_once(key, pdf_bytes, "application/pdf")
        self.db.execute(
            """INSERT INTO evidence_files(file_id, version_id, sha256, mime_type, size_bytes, storage_uri)
                 VALUES (%s,%s,%s,%s,%s,%s)""",
            (file_id, version_id, sha256, "application/pdf", len(pdf_bytes), storage_uri),
        )
        return file_id, key, storage_uri
