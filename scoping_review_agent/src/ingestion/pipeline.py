from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from scoping_review_agent.src.ingestion.mylibrary import fetch_mybibliography_pmids
from scoping_review_agent.src.ingestion.eutils import esummary_pubmed, fetch_abstracts_efetch
from scoping_review_agent.src.ingestion.normalize import normalize_record, paper_id_from_pmid_doi
from scoping_review_agent.src.utils.io import ensure_dir, write_jsonl


def ingest_from_mybibliography(
    public_url: str,
    output_dir: str | Path,
    max_pages: int = 10,
    batch_size: int = 50,
) -> List[Dict[str, Any]]:
    output_dir = Path(output_dir)
    ensure_dir(output_dir)

    pmids = fetch_mybibliography_pmids(public_url=public_url, max_pages=max_pages)
    # Enrich via E-utilities in batches to avoid URL limits.
    candidates: List[Dict[str, Any]] = []

    for i in range(0, len(pmids), batch_size):
        batch = pmids[i : i + batch_size]
        esums = esummary_pubmed(batch)
        abstracts = fetch_abstracts_efetch(batch)
        for pmid in batch:
            doc = esums.get(str(pmid), {})
            # DOI/PMCID
            doi, pmcid = (None, None)
            try:
                from scoping_review_agent.src.ingestion.eutils import extract_doi_pmcid_from_esummary

                doi, pmcid = extract_doi_pmcid_from_esummary(doc)
            except Exception:
                doi, pmcid = (None, None)
            abstract = abstracts.get(str(pmid))
            rec = normalize_record(
                pmid=str(pmid),
                esum_doc=doc,
                doi=doi,
                pmcid=pmcid,
                abstract=abstract,
            )
            candidates.append(rec)

    # Deduplicate by paper_id.
    seen: set[str] = set()
    deduped: List[Dict[str, Any]] = []
    for c in candidates:
        pid = c.get("paper_id") or ""
        if not pid or pid in seen:
            continue
        seen.add(pid)
        deduped.append(c)

    write_jsonl(output_dir / "candidates.jsonl", deduped)
    return deduped

