from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, Literal
from datetime import datetime
from uuid import uuid4

EventType = Literal[
    "EPIC1.REGISTRY.VERSION_CREATED",
    "EPIC1.LLM.DERIVATION_REQUESTED",
    "EPIC1.LLM.DERIVATION_COMPLETED",
    "EPIC1.INGESTION.COMPLETED",
    "EPIC1.INGESTION.FAILED"
]

EntityType = Literal["document","version","file","artifact","system"]

class DomainEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: EventType
    at: datetime = Field(default_factory=datetime.utcnow)
    correlation_id: str
    actor: str
    entity_type: EntityType
    entity_id: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    prev_event_hash: Optional[str] = None
    event_hash: Optional[str] = None
