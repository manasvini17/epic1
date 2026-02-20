from __future__ import annotations
import hashlib
from typing import Any, Dict, List, Tuple

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _page_for_offset(page_map: List[Dict[str, Any]], pos: int) -> int:
    for p in page_map:
        if p["start_char"] <= pos <= p["end_char"]:
            return int(p["page"])
    return int(page_map[-1]["page"]) if page_map else 1

class SimpleDeterministicChunker:
    def __init__(self, max_chars: int = 1500) -> None:
        self.max_chars = max_chars

    def chunk(
        self,
        *,
        stable_text: str,
        page_map: List[Dict[str, Any]],
        max_chars: int | None = None,
        overlap_chars: int = 0,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        chunks: List[Dict[str, Any]] = []
        i = 0
        n = len(stable_text)

        max_len = int(max_chars or self.max_chars)
        overlap = max(0, int(overlap_chars))
        if max_len > 1:
            overlap = min(overlap, max_len - 1)
        else:
            overlap = 0

        def emit(start: int, end: int):
            text = stable_text[start:end]
            if not text.strip():
                return
            chunks.append({
                "start_char": start,
                "end_char": end,
                "page_start": _page_for_offset(page_map, start),
                "page_end": _page_for_offset(page_map, max(start, end-1)),
                "text_sha256": sha256_text(text),
            })

        while i < n:
            j = stable_text.find("\n\n", i)
            if j == -1:
                j = n
            para_end = j
            start = i
            while start < para_end:
                end = min(start + max_len, para_end)
                emit(start, end)
                # overlap creates better recall for RAG; keep deterministic
                start = end if overlap == 0 else max(start + 1, end - overlap)
            i = para_end + 2

        manifest = {
            "policy": {"max_chars": max_len, "overlap_chars": overlap, "split": "paragraph_then_hard"},
            "count": len(chunks),
        }
        return chunks, manifest
