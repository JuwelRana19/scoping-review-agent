from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _sha1_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _chunk_text(text: str, *, max_chars: int, overlap_chars: int) -> List[Tuple[int, int, str]]:
    """
    Returns list of (start_idx, end_idx, chunk_text).
    Character-based chunking as a simple MVP strategy.
    """
    text = text or ""
    if not text.strip():
        return []
    if max_chars <= 0:
        return [(0, len(text), text)]

    chunks: List[Tuple[int, int, str]] = []
    step = max(1, max_chars - overlap_chars)
    for start in range(0, len(text), step):
        end = min(len(text), start + max_chars)
        chunk = text[start:end]
        chunks.append((start, end, chunk))
        if end >= len(text):
            break
    return chunks


def _sanitize_windows_filename(value: str) -> str:
    """
    Windows filenames cannot contain: < > : " / \\ | ? *
    Colons in particular may be interpreted as alternate data streams (ADS),
    so we replace them to avoid cache write/read issues.
    """
    unsafe_chars = set('<>:"/\\|?*')
    return "".join("_" if ch in unsafe_chars else ch for ch in value)


def extract_pdf_pages(pdf_path: str | Path) -> List[Dict[str, Any]]:
    """
    Extract text per page with page numbers.
    """
    pdf_path = Path(pdf_path)
    # Lazy import to avoid hard dependency if user skips PDF extraction.
    from pypdf import PdfReader  # type: ignore

    reader = PdfReader(str(pdf_path))
    pages: List[Dict[str, Any]] = []
    for i, page in enumerate(reader.pages):
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        pages.append({"page_number": i + 1, "text": text})
    return pages


def make_page_aware_chunks(
    pages: List[Dict[str, Any]],
    *,
    max_chars: int,
    overlap_chars: int,
) -> List[Dict[str, Any]]:
    """
    Create overlapping chunks while preserving page boundaries where possible.
    """
    chunks: List[Dict[str, Any]] = []
    for page in pages:
        page_num = int(page.get("page_number", 0) or 0)
        text = page.get("text") or ""
        segs = _chunk_text(text, max_chars=max_chars, overlap_chars=overlap_chars)
        for j, (start, end, chunk_text) in enumerate(segs):
            if not chunk_text.strip():
                continue
            chunks.append(
                {
                    "chunk_id": f"p{page_num}_c{j}",
                    "page_start": page_num,
                    "page_end": page_num,
                    "text": chunk_text.strip(),
                }
            )
    return chunks


def parse_pdf_to_pages_and_chunks(
    *,
    paper_id: str,
    pdf_path: str | Path,
    cache_dir: str | Path,
    max_chars: int,
    overlap_chars: int,
) -> Dict[str, Any]:
    """
    Cache extracted pages/chunks keyed by (file sha1) so incremental reruns are fast.
    """
    pdf_path = Path(pdf_path)
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    file_hash = _sha1_file(pdf_path)
    key = f"{paper_id}__{file_hash}"

    safe_paper_id = _sanitize_windows_filename(paper_id)
    pages_cache = cache_dir / f"{safe_paper_id}__pages.json"
    chunks_cache = cache_dir / f"{safe_paper_id}__chunks.json"
    meta_cache = cache_dir / f"{safe_paper_id}__meta.json"

    if pages_cache.exists() and chunks_cache.exists() and meta_cache.exists():
        try:
            meta = json.loads(meta_cache.read_text(encoding="utf-8"))
            if meta.get("file_hash") == file_hash:
                return {
                    "pages": json.loads(pages_cache.read_text(encoding="utf-8")),
                    "chunks": json.loads(chunks_cache.read_text(encoding="utf-8")),
                    "file_hash": file_hash,
                    "cache_key": key,
                    "pdf_path": str(pdf_path),
                }
        except Exception:
            pass

    pages = extract_pdf_pages(pdf_path)
    chunks = make_page_aware_chunks(pages, max_chars=max_chars, overlap_chars=overlap_chars)

    pages_cache.write_text(json.dumps(pages, ensure_ascii=False), encoding="utf-8")
    chunks_cache.write_text(json.dumps(chunks, ensure_ascii=False), encoding="utf-8")
    meta_cache.write_text(json.dumps({"file_hash": file_hash, "cache_key": key}), encoding="utf-8")

    return {
        "pages": pages,
        "chunks": chunks,
        "file_hash": file_hash,
        "cache_key": key,
        "pdf_path": str(pdf_path),
    }

