from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, Optional


def normalize_doi(doi: str | None) -> Optional[str]:
    if not doi:
        return None
    doi = doi.strip()
    doi = re.sub(r"^https?://doi\.org/", "", doi, flags=re.IGNORECASE)
    doi = doi.lower()
    return doi or None


def paper_id_from_pmid_doi(pmid: str | None, doi: str | None) -> str:
    """
    Stable dedupe key.
    """
    if doi:
        return "doi:" + normalize_doi(doi)
    if pmid:
        return "pmid:" + str(pmid)
    # fallback: hash title not ideal but better than nothing
    return "unknown:" + hashlib.sha1((pmid or "") + (doi or "")).hexdigest()


def normalize_record(
    pmid: str,
    esum_doc: Dict[str, Any],
    doi: Optional[str],
    pmcid: Optional[str],
    abstract: Optional[str],
) -> Dict[str, Any]:
    title = esum_doc.get("title") or ""
    journal = ""
    journal_abbr = ""
    authors = []
    year = None

    # Common fields in NCBI esummary JSON:
    # - full journal name or source
    # - pubdate / sortdate
    # - authors list with name pieces
    for key in ["fulljournalname", "source", "journal"]:
        if key in esum_doc and esum_doc.get(key):
            journal = esum_doc.get(key)
            break
    for key in ["volume", "issue", "pages"]:
        pass

    # Attempt year from pubdate-like fields
    for key in ["pubdate", "epubdate", "sortpubdate"]:
        val = esum_doc.get(key)
        if isinstance(val, str) and val:
            m = re.search(r"(19|20)\d{2}", val)
            if m:
                year = int(m.group(0))
                break

    # Author names
    # NCBI varies; attempt a few known shapes.
    if "authors" in esum_doc and isinstance(esum_doc["authors"], list):
        for a in esum_doc["authors"]:
            if not isinstance(a, dict):
                continue
            name = a.get("name") or ""
            if name:
                authors.append(name)

    # If authors not present, fall back to "authors" string fields.
    if not authors and isinstance(esum_doc.get("authors"), str):
        authors = [esum_doc["authors"]]

    doi_norm = normalize_doi(doi)
    paper_id = paper_id_from_pmid_doi(pmid=pmid, doi=doi_norm)

    doi_url = f"https://doi.org/{doi_norm}" if doi_norm else ""
    pm_url = f"https://www.ncbi.nlm.nih.gov/pubmed/{pmid}/"
    pmcid_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmcid}/" if pmcid else ""

    return {
        "paper_id": paper_id,
        "pmid": str(pmid),
        "doi": doi_norm or "",
        "pmcid": pmcid or "",
        "title": title,
        "authors": authors,
        "year": year,
        "journal": journal,
        "journalAbbr": journal_abbr,
        "url": pm_url,
        "doi_url": doi_url,
        "pmcid_url": pmcid_url,
        "abstract": abstract or "",
    }

