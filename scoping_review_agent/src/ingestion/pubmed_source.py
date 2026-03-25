from __future__ import annotations

import json
import os
import re
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from scoping_review_agent.src.ingestion.eutils import esummary_pubmed, fetch_abstracts_efetch, extract_doi_pmcid_from_esummary
from scoping_review_agent.src.ingestion.normalize import normalize_record
from scoping_review_agent.src.utils.io import ensure_dir, write_jsonl
from scoping_review_agent.src.screening.llm_client import extract_first_json_object, llm_text_call


def _extract_term_from_pubmed_search_url(url: str) -> Optional[str]:
    """
    For PubMed URLs like:
      https://pubmed.ncbi.nlm.nih.gov/?term=...
    or
      .../ ?term=...&...
    """
    try:
        u = urllib.parse.urlparse(url)
        q = urllib.parse.parse_qs(u.query)
        term = q.get("term", [None])[0]
        return term
    except Exception:
        return None


def _normalize_esearch_term(term: str) -> str:
    return (term or "").strip()


def pmids_from_esearch_term(term: str, *, retmax: int = 2000, timeout_s: int = 60) -> List[str]:
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    params = {
        "db": "pubmed",
        "term": term,
        "retmax": int(retmax),
        "retmode": "json",
    }
    r = requests.get(base, params=params, timeout=timeout_s)
    r.raise_for_status()
    data = r.json()
    ids = data.get("esearchresult", {}).get("idlist", []) or []
    return [str(x) for x in ids]


def pmids_from_dois(dois: List[str], *, retmax_per_doi: int = 5) -> List[str]:
    pmids: List[str] = []
    seen: set[str] = set()
    for doi in dois:
        if not doi:
            continue
        doi = str(doi).strip()
        term = f"\"{doi}\"[DOI]"
        ids = pmids_from_esearch_term(term, retmax=retmax_per_doi)
        for pid in ids:
            if pid not in seen:
                seen.add(pid)
                pmids.append(pid)
    return pmids


def heuristic_pubmed_term_from_keywords(objective_text: str, keywords: List[str]) -> str:
    # Use only keyword hints. Objective text is sentence-like, so avoid injecting full sentence.
    kws = [k.strip() for k in (keywords or []) if k and str(k).strip()]
    if not kws:
        # fallback to a few objective nouns
        base = objective_text or ""
        kws = re.findall(r"[A-Za-z0-9][A-Za-z0-9\\-]{2,}", base)
        kws = kws[:6]
    clauses = [f"({k}[Title/Abstract])" for k in kws[:10]]
    return " OR ".join(clauses) if clauses else "cancer"


def llm_generate_pubmed_term(
    *,
    provider: str,
    model: str,
    temperature: float,
    objective_text: str,
    keywords: List[str],
    search_strategy: str | None,
    api_key_env: str,
) -> str:
    prompt = {
        "objective_text": objective_text,
        "keywords": keywords,
        "search_strategy_hint": search_strategy,
        "task": (
            "Generate a PubMed ESearch term (entrez query syntax) to retrieve candidate articles "
            "for a scoping review. Prefer concise keyword/MeSH combinations. "
            "Return a single string suitable for `esearch.fcgi` parameter `term`."
        ),
    }
    user_prompt = (
        "JSON input:\n" + json.dumps(prompt, ensure_ascii=False, indent=2) +
        "\n\nReturn strict JSON: {\"pubmed_term\":\"...\"}."
    )
    content = llm_text_call(
        provider=provider,
        model=model,
        temperature=temperature,
        system_instructions="You generate PubMed search terms for systematic/scoping review candidate retrieval.",
        user_prompt=user_prompt,
        api_key_env=api_key_env,
    )
    parsed = extract_first_json_object(content) or {}
    term = parsed.get("pubmed_term") or ""
    return str(term).strip()


def determine_pubmed_term(
    objective: Dict[str, Any],
    *,
    llm_cfg: Dict[str, Any],
) -> str:
    # Prefer explicitly provided PubMed terms/search strategy.
    for key in ["pubmed_term", "search_strategy"]:
        t = objective.get(key)
        if t:
            return _normalize_esearch_term(str(t))

    pubmed_url = objective.get("pubmed_search_url")
    if pubmed_url:
        extracted = _extract_term_from_pubmed_search_url(str(pubmed_url))
        if extracted:
            return _normalize_esearch_term(extracted)

    keywords = objective.get("keywords") or []
    keywords = keywords if isinstance(keywords, list) else [str(keywords)]
    if llm_cfg:
        provider = llm_cfg.get("provider", "openai")
        provider_l = str(provider).lower()
        api_key_env = llm_cfg.get("api_key_env", "OPENAI_API_KEY")
        api_key = os.environ.get(api_key_env, "") if api_key_env else ""
        llm_available = (provider_l == "ollama") or bool(api_key)
        if llm_available and (objective.get("objective_text") or "").strip():
            model = llm_cfg.get("model", "gpt-4.1-mini")
            temperature = float(llm_cfg.get("temperature", 0.2))
            search_strategy = objective.get("search_strategy") or None
            term = llm_generate_pubmed_term(
                provider=provider,
                model=model,
                temperature=temperature,
                objective_text=objective.get("objective_text") or "",
                keywords=keywords,
                search_strategy=search_strategy,
                api_key_env=api_key_env,
            )
            if term:
                return term

    return heuristic_pubmed_term_from_keywords(objective.get("objective_text") or "", keywords)


def ingest_candidates_from_pmids(
    pmids: List[str],
    *,
    output_dir: str | Path,
    max_eutils_batch: int = 50,
) -> List[Dict[str, Any]]:
    output_dir = Path(output_dir)
    ensure_dir(output_dir)

    candidates: List[Dict[str, Any]] = []

    for i in range(0, len(pmids), max_eutils_batch):
        batch = pmids[i : i + max_eutils_batch]
        esums = esummary_pubmed(batch)
        abstracts = fetch_abstracts_efetch(batch)
        for pmid in batch:
            doc = esums.get(str(pmid), {})
            doi, pmcid = (None, None)
            try:
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

    # Deduplicate
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


def ingest_candidates_for_objective(
    objective: Dict[str, Any],
    *,
    output_dir: str | Path,
    llm_cfg: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Supports:
    - objective.candidate_pmids
    - objective.candidate_dois
    - objective.pubmed_term / objective.search_strategy
    - objective.pubmed_search_url containing term=
    - otherwise generates a term from objective_text via LLM/heuristics
    """
    pmids: List[str] = []

    candidate_pmids = objective.get("candidate_pmids") or []
    if isinstance(candidate_pmids, list) and candidate_pmids:
        pmids = [str(x) for x in candidate_pmids if x]

    candidate_dois = objective.get("candidate_dois") or []
    if (not pmids) and isinstance(candidate_dois, list) and candidate_dois:
        pmids = pmids_from_dois([str(x) for x in candidate_dois if x])

    if not pmids:
        term = determine_pubmed_term(objective, llm_cfg=llm_cfg)
        pmids = pmids_from_esearch_term(term, retmax=int(objective.get("retmax", 2000) or 2000))

    return ingest_candidates_from_pmids(pmids, output_dir=output_dir)

