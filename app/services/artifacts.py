from __future__ import annotations
import json, hashlib
from uuid import uuid4
from typing import Any, Dict
from app.infra.db import Postgres
from app.infra.storage import StorageAdapter

def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def _json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")

class ArtifactService:
    def __init__(self, db: Postgres, storage: StorageAdapter) -> None:
        self.db = db
        self.storage = storage

    def register(self, *, version_id: str, kind: str, content_bytes: bytes, key: str, generator_name: str, generator_version: str) -> str:
        artifact_id = str(uuid4())
        sha = _sha256_bytes(content_bytes)
        uri = self.storage.put_bytes_write_once(key, content_bytes, "application/json" if key.endswith(".json") else "text/plain")
        self.db.execute(
            """INSERT INTO derived_artifacts(artifact_id, version_id, kind, sha256, storage_uri, generator_name, generator_version)
                 VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (artifact_id, version_id, kind, sha, uri, generator_name, generator_version),
        )
        return artifact_id

    def store_canonical(self, *, version_id: str, stable_text: str, page_map: Any, layout_map: Any,
                        extractor_version: str, layout_version: str) -> Dict[str, str]:
        ids: Dict[str, str] = {}
        ids["stable_text_id"] = self.register(
            version_id=version_id, kind="stable_text",
            content_bytes=stable_text.encode("utf-8"),
            key=f"canonical/{version_id}/stable_text.txt",
            generator_name="canonical_text_pipeline",
            generator_version=extractor_version,
        )
        ids["page_map_id"] = self.register(
            version_id=version_id, kind="page_map",
            content_bytes=_json_bytes(page_map),
            key=f"canonical/{version_id}/page_map.json",
            generator_name="canonical_text_pipeline",
            generator_version=extractor_version,
        )
        ids["layout_map_id"] = self.register(
            version_id=version_id, kind="layout_map",
            content_bytes=_json_bytes(layout_map),
            key=f"canonical/{version_id}/layout_map.json",
            generator_name="canonical_layout_pipeline",
            generator_version=layout_version,
        )
        return ids

    def store_chunk_set(self, *, version_id: str, chunk_set_obj: Any, generator_version: str) -> str:
        return self.register(
            version_id=version_id, kind="chunk_set",
            content_bytes=_json_bytes(chunk_set_obj),
            key=f"indexes/{version_id}/chunk_sets/chunk_set.json",
            generator_name="chunker",
            generator_version=generator_version,
        )

    def store_json_artifact(self, *, version_id: str, kind: str, obj: Any, generator_name: str, generator_version: str, key: str) -> str:
        """Store an immutable JSON artifact under a deterministic or versioned key."""
        return self.register(
            version_id=version_id,
            kind=kind,
            content_bytes=_json_bytes(obj),
            key=key,
            generator_name=generator_name,
            generator_version=generator_version,
        )

    def get(self, artifact_id: str):
        return self.db.fetchone("SELECT * FROM derived_artifacts WHERE artifact_id=%s", (artifact_id,))

    def signed_url(self, artifact_id: str, *, expires_seconds: int = 3600) -> str:
        row = self.get(artifact_id)
        if not row:
            raise ValueError("artifact not found")
        return self.storage.signed_url_from_uri(row["storage_uri"], expires_seconds=expires_seconds)
