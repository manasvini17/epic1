from __future__ import annotations
from typing import Dict, Any
from app.infra.db import Postgres

class SoIProjector:
    def __init__(self, db: Postgres) -> None:
        self.db = db

    def project(self, event_type: str, payload: Dict[str, Any]) -> None:
        if event_type in ("EPIC1.REGISTRY.VERSION_CREATED","EPIC1.INGESTION.COMPLETED","EPIC1.INGESTION.FAILED"):
            vid = payload.get("version_id")
            if not vid:
                return
            v = self.db.fetchone("SELECT * FROM document_versions WHERE version_id=%s", (vid,))
            if not v:
                return
            self.db.execute(
                """INSERT INTO soi_versions(version_id, document_id, status, version_label, effective_date, uploaded_by, uploaded_at, raw_sha256, artifact_count, updated_at)
                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
                     ON CONFLICT (version_id) DO UPDATE
                       SET status=EXCLUDED.status, artifact_count=EXCLUDED.artifact_count, updated_at=now()""",
                (v["version_id"], v["document_id"], v["status"], v.get("version_label"), v.get("effective_date"),
                 v["uploaded_by"], v["uploaded_at"], v["raw_sha256"], 0)
            )
            d = self.db.fetchone("SELECT * FROM documents WHERE document_id=%s", (v["document_id"],))
            if d:
                self.db.execute(
                    """INSERT INTO soi_documents(document_id, title, jurisdiction, regulation_family, instrument_type, primary_axis, latest_version_id, latest_status, updated_at)
                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,now())
                         ON CONFLICT (document_id) DO UPDATE
                           SET latest_version_id=EXCLUDED.latest_version_id, latest_status=EXCLUDED.latest_status, updated_at=now()""",
                    (d["document_id"], d["title"], d["jurisdiction"], d["regulation_family"], d["instrument_type"], d["primary_axis"],
                     v["version_id"], v["status"])
                )

        if event_type in ("EPIC1.INGESTION.COMPLETED",):
            vid = payload.get("version_id")
            if not vid:
                return
            row = self.db.fetchone("SELECT count(*) AS c FROM derived_artifacts WHERE version_id=%s", (vid,))
            c = int(row["c"]) if row else 0
            self.db.execute("UPDATE soi_versions SET artifact_count=%s, updated_at=now() WHERE version_id=%s", (c, vid))
