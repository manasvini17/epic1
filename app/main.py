from fastapi import FastAPI

from app.api.routes_epic1 import router as epic1_router
from app.infra.db import Postgres
from app.infra.logging import configure_logging
from app.infra.middleware import correlation_id_middleware
from app.refdata.loader import ensure_default_rules

configure_logging()

app = FastAPI(title="EPIC-1 Truth Capture (SoE + SoI)")

# Observability: correlation id per request (also returned in header)
app.middleware("http")(correlation_id_middleware)

@app.on_event("startup")
def startup():
    db = Postgres()
    ensure_default_rules(db)

app.include_router(epic1_router)
