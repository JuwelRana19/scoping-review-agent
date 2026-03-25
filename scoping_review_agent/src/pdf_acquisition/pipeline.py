from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List

from scoping_review_agent.src.pdf_acquisition.pdf import acquire_pdf_for_paper
from scoping_review_agent.src.utils.io import ensure_dir, read_jsonl, write_jsonl


def acquire_pdfs(
    candidates_jsonl_path: str | Path,
    pdfs_root_dir: str | Path,
    screening_csv_path: str | Path | None,
    config: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Acquire PDFs for papers.

    If `screening_csv_path` is provided, acquire only for include/uncertain decisions.
    Otherwise, acquire for all.
    """
    candidates = read_jsonl(candidates_jsonl_path)
    pdfs_root_dir = Path(pdfs_root_dir)
    ensure_dir(pdfs_root_dir)

    included_ids: set[str] | None = None
    if screening_csv_path:
        import pandas as pd

        df = pd.read_csv(screening_csv_path)
        included_ids = set(df.loc[df["decision"].isin(["include", "uncertain"]), "paper_id"].astype(str).tolist())

    zotero_folder = (config.get("pdf_acquisition") or {}).get("zotero_storage_folder") or (config.get("pdf_acquisition") or {}).get("zotero_folder")
    try_download_when_missing = bool((config.get("pdf_acquisition") or {}).get("try_download_when_missing", True))
    allowed_domains = (config.get("pdf_acquisition") or {}).get("allowed_domains") or []

    out_rows: List[Dict[str, Any]] = []
    for p in candidates:
        if included_ids is not None and str(p.get("paper_id")) not in included_ids:
            continue

        res = acquire_pdf_for_paper(
            p,
            zotero_folder=zotero_folder,
            pdf_cache_dir=pdfs_root_dir,
            try_download_when_missing=try_download_when_missing,
            allowed_domains=allowed_domains,
        )
        row = dict(p)
        row.update(res)
        out_rows.append(row)

    write_jsonl(pdfs_root_dir / "pdf_acquisitions.jsonl", out_rows)
    return out_rows

