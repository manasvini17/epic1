from __future__ import annotations
import hashlib, json
from typing import Any, Dict, Optional
from uuid import uuid4
from app.infra.db import Postgres

def _stable_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

class AuditService:
    def __init__(self, db: Postgres) -> None:
        self.db = db

    def last_hash_for_entity(self, entity_type: str, entity_id: str) -> Optional[str]:
        row = self.db.fetchone(
            """SELECT event_hash FROM audit_log
                 WHERE entity_type=%s AND entity_id=%s AND event_hash IS NOT NULL
                 ORDER BY at DESC LIMIT 1""",
            (entity_type, entity_id),
        )
        return row["event_hash"] if row else None

    def write(self, *, entity_type: str, entity_id: str, action: str, actor: str, correlation_id: str,
              details: Dict[str, Any], enable_hash_chain: bool = True) -> str:
        event_id = str(uuid4())
        prev = self.last_hash_for_entity(entity_type, entity_id) if enable_hash_chain else None
        payload = {
            "event_id": event_id,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "action": action,
            "actor": actor,
            "correlation_id": correlation_id,
            "details": details,
            "prev_event_hash": prev,
        }
        event_hash = _sha256(_stable_json(payload)) if enable_hash_chain else None
        self.db.execute(
            """INSERT INTO audit_log(event_id, entity_type, entity_id, action, actor, correlation_id, details_json, prev_event_hash, event_hash)
                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (event_id, entity_type, entity_id, action, actor, correlation_id, self.db.json(details), prev, event_hash),
        )
        return event_id
