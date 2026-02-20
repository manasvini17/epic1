from __future__ import annotations
from typing import Dict, Any
from uuid import uuid4
import hashlib
from app.infra.db import Postgres
from app.services.artifacts import ArtifactService

def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

class LLMClient:
    async def run(self, *, purpose: str, input_text: str) -> str:
        # Stub: replace with real LLM. Output is always derived, never truth.
        return f"[LLM:{purpose}] " + input_text[:800]

class LLMOrchestrator:
    def __init__(self, db: Postgres, artifacts: ArtifactService) -> None:
        self.db = db
        self.artifacts = artifacts
        self.llm = LLMClient()

    async def summarize_for_indexing(self, *, version_id: str, stable_text: str) -> Dict[str, Any]:
        prompt_template = "Summarize regulation for indexing; do not invent facts."
        prompt_hash = sha256_str(prompt_template)
        self.db.execute(
            """INSERT INTO prompts(prompt_hash, prompt_template, prompt_version)
                 VALUES (%s,%s,%s) ON CONFLICT (prompt_hash) DO NOTHING""",
            (prompt_hash, prompt_template, "v1"),
        )

        run_id = str(uuid4())
        input_fingerprint = sha256_str(f"{version_id}:{prompt_hash}:{sha256_str(stable_text)}")
        self.db.execute(
            """INSERT INTO llm_runs(run_id, version_id, purpose, model_name, model_version, prompt_hash, tools_used, input_fingerprint, status)
                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (run_id, version_id, "summarize_for_indexing", "stub-llm", "0", prompt_hash, self.db.json({"tools": []}), input_fingerprint, "RUNNING"),
        )

        output = await self.llm.run(purpose="summarize_for_indexing", input_text=stable_text)
        artifact_id = self.artifacts.register(
            version_id=version_id, kind="llm_output",
            content_bytes=output.encode("utf-8"),
            key=f"llm_outputs/{version_id}/{run_id}.txt",
            generator_name="llm_orchestrator",
            generator_version="stub-0",
        )
        self.db.execute("UPDATE llm_runs SET output_artifact_id=%s, status='COMPLETED' WHERE run_id=%s", (artifact_id, run_id))
        return {"run_id": run_id, "output_artifact_id": artifact_id}
