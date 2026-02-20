from __future__ import annotations
import os
from urllib.parse import urlparse
from abc import ABC, abstractmethod
from urllib.parse import urlparse
import boto3
from botocore.client import Config
from app.settings import settings


def parse_storage_uri(uri: str) -> tuple[str, str]:
    """Return (scheme, location).

    - s3://bucket/key -> ("s3", "key")
    - file:///abs/path -> ("file", "/abs/path")
    - file://C:/path (windows) also supported.
    """
    p = urlparse(uri)
    scheme = p.scheme or "file"
    if scheme == "s3":
        # path includes leading '/'
        key = p.path.lstrip("/")
        return "s3", key
    if scheme == "file":
        # file:// + netloc for windows drive sometimes
        loc = (p.netloc + p.path) if p.netloc else p.path
        return "file", loc
    return scheme, p.path.lstrip("/")

class StorageAdapter(ABC):
    @abstractmethod
    def put_bytes_write_once(self, key: str, data: bytes, content_type: str) -> str: ...
    @abstractmethod
    def get_signed_url(self, key: str, expires_sec: int) -> str: ...
    @abstractmethod
    def exists(self, key: str) -> bool: ...
    @abstractmethod
    def get_bytes(self, key: str) -> bytes: ...

    def signed_url_from_uri(self, storage_uri: str, *, expires_seconds: int = 3600) -> str:
        """Generate a read URL from a stored URI.

        For S3 this returns a presigned URL.
        For local mode this returns a file:// path.
        """
        scheme, loc = parse_storage_uri(storage_uri)
        if scheme == "s3":
            return self.get_signed_url(loc, expires_seconds)
        # local
        return f"file://{loc}"

class LocalStorage(StorageAdapter):
    def __init__(self, root: str) -> None:
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)

    def _full(self, key: str) -> str:
        p = os.path.join(self.root, key)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        return p

    def put_bytes_write_once(self, key: str, data: bytes, content_type: str) -> str:
        path = self._full(key)
        if not os.path.exists(path):
            with open(path, "wb") as f:
                f.write(data)
        return f"file://{path}"

    def exists(self, key: str) -> bool:
        return os.path.exists(self._full(key))

    def get_bytes(self, key: str) -> bytes:
        with open(self._full(key), "rb") as f:
            return f.read()

    def get_signed_url(self, key: str, expires_sec: int) -> str:
        return self.put_bytes_write_once(key, b"", "application/octet-stream")

class S3Storage(StorageAdapter):
    def __init__(self) -> None:
        self.bucket = settings.S3_BUCKET
        self.s3 = boto3.client(
            "s3",
            endpoint_url=settings.S3_ENDPOINT_URL or None,
            aws_access_key_id=settings.S3_ACCESS_KEY_ID,
            aws_secret_access_key=settings.S3_SECRET_ACCESS_KEY,
            region_name=settings.S3_REGION,
            config=Config(signature_version="s3v4"),
        )
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except Exception:
            self.s3.create_bucket(Bucket=self.bucket)

    def exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False

    def put_bytes_write_once(self, key: str, data: bytes, content_type: str) -> str:
        if self.exists(key):
            return f"s3://{self.bucket}/{key}"
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)
        return f"s3://{self.bucket}/{key}"

    def get_signed_url(self, key: str, expires_sec: int) -> str:
        return self.s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=expires_sec,
        )

    def get_bytes(self, key: str) -> bytes:
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()

def parse_storage_uri(storage_uri: str) -> tuple[str, str]:
    if storage_uri.startswith("s3://"):
        u = urlparse(storage_uri)
        return ("s3", u.path.lstrip("/"))
    if storage_uri.startswith("file://"):
        return ("file", storage_uri[len("file://"):])
    raise ValueError(f"Unsupported storage_uri: {storage_uri}")

def make_storage() -> StorageAdapter:
    if settings.STORAGE_MODE == "local":
        return LocalStorage(settings.STORAGE_ROOT)
    return S3Storage()
