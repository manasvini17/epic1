import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Settings:
    SERVICE_NAME: str = os.getenv("SERVICE_NAME", "epic1")
    ENV: str = os.getenv("ENV", "dev")
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/epic1")

    KAFKA_BOOTSTRAP: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    KAFKA_CLIENT_ID: str = os.getenv("KAFKA_CLIENT_ID", "epic1-service")
    TOPIC_EVENTS: str = os.getenv("TOPIC_EVENTS", "epic1.events")

    STORAGE_MODE: str = os.getenv("STORAGE_MODE", "s3")  # s3|local
    STORAGE_ROOT: str = os.getenv("STORAGE_ROOT", "./storage")
    S3_ENDPOINT_URL: str = os.getenv("S3_ENDPOINT_URL", "")
    S3_ACCESS_KEY_ID: str = os.getenv("S3_ACCESS_KEY_ID", "")
    S3_SECRET_ACCESS_KEY: str = os.getenv("S3_SECRET_ACCESS_KEY", "")
    S3_BUCKET: str = os.getenv("S3_BUCKET", "epic1")
    S3_REGION: str = os.getenv("S3_REGION", "us-east-1")
    SIGNED_URL_EXPIRES_SEC: int = int(os.getenv("SIGNED_URL_EXPIRES_SEC", "900"))

    EXTRACTOR_VERSION: str = os.getenv("EXTRACTOR_VERSION", "pymupdf-text@1.0.0")
    LAYOUT_VERSION: str = os.getenv("LAYOUT_VERSION", "pymupdf-layout@1.0.0")
    CHUNKER_VERSION: str = os.getenv("CHUNKER_VERSION", "simple-chunker@1.0.0")
    CHUNK_SCHEMA_VERSION: str = os.getenv("CHUNK_SCHEMA_VERSION", "chunk_set@1.0.0")

    # LLM suggestion features (derived-only; never treated as source-of-truth)
    ENABLE_LLM_PRIMARY_AXIS_SUGGESTION: bool = os.getenv("ENABLE_LLM_PRIMARY_AXIS_SUGGESTION", "false").lower() in ("1", "true", "yes")
    LLM_MODEL_NAME: str = os.getenv("LLM_MODEL_NAME", "stub-llm")
    LLM_MODEL_VERSION: str = os.getenv("LLM_MODEL_VERSION", "0")


    AUTH_MODE: str = os.getenv("AUTH_MODE", "jwt_hs256")  # jwt_hs256|none
    JWT_HS256_SECRET: str = os.getenv("JWT_HS256_SECRET", "dev-secret")
    JWT_AUD: str = os.getenv("JWT_AUD", "epic1")
    JWT_ISS: str = os.getenv("JWT_ISS", "local")

    MAX_PDF_MB: int = int(os.getenv("MAX_PDF_MB", "50"))

settings = Settings()
