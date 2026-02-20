from __future__ import annotations
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from typing import Any, Dict, Optional, Tuple, List
from app.settings import settings

class Postgres:
    def __init__(self) -> None:
        self._conn = psycopg2.connect(settings.DATABASE_URL)
        self._conn.autocommit = True

    @contextmanager
    def cursor(self):
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield cur
        finally:
            cur.close()

    def execute(self, sql: str, params: Tuple[Any, ...] = ()) -> None:
        with self.cursor() as cur:
            cur.execute(sql, params)

    def fetchone(self, sql: str, params: Tuple[Any, ...] = ()) -> Optional[Dict[str, Any]]:
        with self.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None

    def fetchall(self, sql: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
        with self.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]

    @staticmethod
    def json(v: Any):
        return psycopg2.extras.Json(v)
