from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict, List, Tuple

from scoping_review_agent.src.utils.io import ensure_dir, write_jsonl


def _sha1_text(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()


def ingest_local_pdf_folder(
    *,
    local_pdf_folder: str | Path,
    output_dir: str | Path,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Build candidate and acquisition records from manually uploaded PDFs.
    """
    folder = Path(local_pdf_folder)
    out_dir = Path(output_dir)
    ensure_dir(out_dir)

    if not folder.exists():
        return [], []

    candidates: List[Dict[str, Any]] = []
    acquisitions: List[Dict[str, Any]] = []

    for p in sorted(folder.rglob("*.pdf")):
        title = p.stem.replace("_", " ").strip()
        paper_id = "localpdf:" + _sha1_text(str(p.resolve()))[:16]
        candidate = {
            "paper_id": paper_id,
            "pmid": "",
            "doi": "",
            "pmcid": "",
            "title": title or p.name,
            "authors": [],
            "year": None,
            "journal": "",
            "journalAbbr": "",
            "url": "",
            "doi_url": "",
            "pmcid_url": "",
            "abstract": "",
            "source_type": "local_pdf",
            "local_pdf_path": str(p.resolve()),
        }
        acquisition = dict(candidate)
        acquisition["pdf_status"] = "local_uploaded"
        acquisition["pdf_path"] = str(p.resolve())
        candidates.append(candidate)
        acquisitions.append(acquisition)

    write_jsonl(out_dir / "local_candidates.jsonl", candidates)
    write_jsonl(out_dir / "local_pdf_acquisitions.jsonl", acquisitions)
    return candidates, acquisitions

