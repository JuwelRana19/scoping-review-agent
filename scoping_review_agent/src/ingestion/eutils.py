from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

import requests


def _normalize_doi(doi: str) -> str:
    doi = doi.strip()
    doi = re.sub(r"^https?://doi\.org/", "", doi, flags=re.IGNORECASE)
    return doi.lower()


def esummary_pubmed(pmids: List[str], timeout_s: int = 60) -> Dict[str, Dict[str, Any]]:
    """
    Returns a mapping: pmid -> esummary document object.
    """
    if not pmids:
        return {}
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "json",
    }
    r = requests.get(base, params=params, timeout=timeout_s)
    r.raise_for_status()
    data = r.json()
    result = data.get("result", {})
    out: Dict[str, Dict[str, Any]] = {}
    for pmid in pmids:
        doc = result.get(pmid)
        if isinstance(doc, dict):
            out[pmid] = doc
    return out


def extract_doi_pmcid_from_esummary(doc: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    doi = None
    pmcid = None

    # articleids can vary in shape; handle common patterns.
    articleids = doc.get("articleids", []) or []
    if isinstance(articleids, dict):
        # sometimes it's already a dict
        articleids = [articleids]
    if isinstance(articleids, list):
        for item in articleids:
            if not isinstance(item, dict):
                continue
            idtype = item.get("idtype") or item.get("type")
            value = item.get("value") or item.get("text")
            if not value:
                continue
            if str(idtype).lower() == "doi":
                doi = _normalize_doi(str(value))
            if str(idtype).lower() in {"pmc", "pmcid"}:
                pmcid_val = str(value)
                pmcid = pmcid_val.lower().replace("pmc", "")
    else:
        pass

    # Some records may store DOI under `elocationid`.
    if not doi:
        elocation = doc.get("elocationid")
        if isinstance(elocation, str) and elocation.lower().startswith("doi:"):
            doi = _normalize_doi(elocation.split(":", 1)[1])

    return doi, pmcid


def fetch_abstracts_efetch(pmids: List[str], timeout_s: int = 60) -> Dict[str, Optional[str]]:
    if not pmids:
        return {}
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "xml",
    }
    r = requests.get(base, params=params, timeout=timeout_s)
    r.raise_for_status()
    xml = r.text

    # Minimal regex extraction to avoid XML dependency.
    # This may miss very complex abstracts but is good enough for MVP.
    abstract_by_pmid: Dict[str, Optional[str]] = {}
    for pmid in pmids:
        m = re.search(rf"<PMID>{re.escape(pmid)}</PMID>.*?<AbstractText[^>]*>(.*?)</AbstractText>", xml, re.DOTALL)
        if m:
            abstract_by_pmid[pmid] = re.sub(r"<.*?>", "", m.group(1)).strip()
        else:
            abstract_by_pmid[pmid] = None
    return abstract_by_pmid

