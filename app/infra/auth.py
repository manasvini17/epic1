from __future__ import annotations
import jwt
from fastapi import HTTPException, Request
from app.settings import settings

ROLE_OPERATOR = "operator"
ROLE_DATA_STEWARD = "data_steward"
ROLE_AUDITOR = "auditor"

def _roles_from_scope(scope: str) -> list[str]:
    return [s.strip() for s in (scope or "").split() if s.strip()]

def require_auth(request: Request) -> dict:
    if settings.AUTH_MODE == "none":
        return {"sub": "dev-user", "roles": [ROLE_OPERATOR]}
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = auth[len("Bearer "):].strip()
    try:
        claims = jwt.decode(
            token,
            settings.JWT_HS256_SECRET,
            algorithms=["HS256"],
            audience=settings.JWT_AUD,
            issuer=settings.JWT_ISS,
        )
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")
    roles = claims.get("roles")
    if not roles:
        claims["roles"] = _roles_from_scope(claims.get("scope", ""))
    return claims

def require_role(claims: dict, allowed_roles: list[str]) -> None:
    roles = claims.get("roles") or []
    if not any(r in roles for r in allowed_roles):
        raise HTTPException(status_code=403, detail="Forbidden: insufficient role")
