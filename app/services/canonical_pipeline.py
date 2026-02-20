from __future__ import annotations
import fitz
from typing import Dict, Any, List, Tuple

class CanonicalTextPipeline:
    def extract(self, pdf_bytes: bytes) -> Tuple[str, List[Dict[str, Any]], Dict[str, Any]]:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages_text: List[str] = []
        page_map: List[Dict[str, Any]] = []
        layout_lines: List[Dict[str, Any]] = []
        offset = 0
        for page_no in range(doc.page_count):
            page = doc.load_page(page_no)
            text = page.get_text("text") or ""
            pages_text.append(text)
            rect = page.rect
            page_len = len(text)
            page_map.append({
                "page": page_no + 1,
                "start_char": offset,
                "end_char": offset + page_len,
                "bbox": {"width": float(rect.width), "height": float(rect.height), "unit": "pt"},
            })
            d = page.get_text("dict")
            for block in d.get("blocks", []):
                if block.get("type") != 0:
                    continue
                for line in block.get("lines", []):
                    spans = [{"text": sp.get("text") or "", "bbox": sp.get("bbox")} for sp in line.get("spans", [])]
                    layout_lines.append({"page": page_no + 1, "bbox": line.get("bbox"), "spans": spans})
            offset += page_len
        stable_text = "".join(pages_text)
        layout_map = {"lines": layout_lines}
        return stable_text, page_map, layout_map
