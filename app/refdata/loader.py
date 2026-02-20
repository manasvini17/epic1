from __future__ import annotations
from app.infra.db import Postgres
from app.settings import settings

DEFAULT_RULES = {
  "required_fields": ["title","jurisdiction","regulation_family","instrument_type","primary_axis","tenant_id","effective_year"],
  "max_pdf_mb": settings.MAX_PDF_MB
}

def ensure_default_rules(db: Postgres) -> None:
    db.execute(
        """
        INSERT INTO ref_rules(rule_key, rule_desc, rule_json, is_active)
        VALUES (%s,%s,%s,true)
        ON CONFLICT (rule_key) DO UPDATE
          SET rule_json=EXCLUDED.rule_json, is_active=true
        """,
        ("EPIC1_UPLOAD_RULES", "Upload validation rules for EPIC-1", db.json(DEFAULT_RULES)),
    )
