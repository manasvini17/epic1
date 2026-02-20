from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from fastapi import HTTPException


def enforce_upload_rules(ref_rules: Dict[str, Any], request_payload: Dict[str, Any]) -> None:
    """Validate required upload fields using refdata.

    This keeps the API contract explicit (no hidden assumptions):
    - Required fields are configured in refdata tables / json, not hardcoded.
    """
    required = (ref_rules.get("required_fields") or [])
    missing = [f for f in required if request_payload.get(f) in (None, "", [])]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")


def derive_primary_axis_deterministic(
    *,
    jurisdiction: Optional[str],
    title: Optional[str],
    regulation_family: Optional[str],
    instrument_type: Optional[str],
) -> Tuple[str, str]:
    """Derive a deterministic default for primary_axis when the user does not provide it.

    Business rule (HLD):
      1) If tied to a governing legal area (jurisdiction) -> primary_axis = "jurisdiction"
      2) If primarily about a product class -> "product_scope"
      3) If cross-industry disclosure framework -> "theme"

    IMPORTANT:
      - This function must be deterministic.
      - It must NOT use any LLM.
      - It must be safe to run without additional reference tables.

    Returns:
      (primary_axis_value, primary_axis_source)
      source is always "DETERMINISTIC_RULE" for this function.

    Note:
      If you later add controlled vocab tables, you can replace the keyword
      heuristics below with table-driven lookups while keeping the same signature.
    """

    # Rule 1: jurisdiction provided => axis=jurisdiction
    if jurisdiction and jurisdiction.strip():
        return "jurisdiction", "DETERMINISTIC_RULE"

    # Rule 2/3 require a signal. Without refdata tables, we use minimal keywords.
    hay = " ".join([(title or ""), (regulation_family or ""), (instrument_type or "")]).lower()

    # Product scope signals (extend later via refdata tables)
    product_keywords = [
        "battery",
        "batteries",
        "aluminium",
        "cement clinker",
        "steel",
        "fertilizer",
        "hydrogen",
    ]
    if any(k in hay for k in product_keywords):
        return "product_scope", "DETERMINISTIC_RULE"

    # Theme signals (cross-industry disclosure / frameworks)
    theme_keywords = [
        "disclosure",
        "reporting",
        "framework",
        "standard",
        "taxonomy",
        "csrd",
        "esrs",
    ]
    if any(k in hay for k in theme_keywords):
        return "theme", "DETERMINISTIC_RULE"

    # Safe fallback
    return "theme", "DETERMINISTIC_RULE"
