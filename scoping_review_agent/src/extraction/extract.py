from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from scoping_review_agent.src.extraction.retrieval import retrieve_top_chunks
from scoping_review_agent.src.screening.llm_client import llm_text_call, extract_first_json_object


DEFAULT_REQUIRED_FIELDS = [
    "study_design",
    "research_objective",
    "key_findings",
    "strengths",
    "limitations",
    "future_research_agenda",
    "causal_or_epidemiologic_methods",
]


def _stringify_field_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        parts: List[str] = []
        for item in value:
            if isinstance(item, str):
                s = item.strip()
                if s:
                    parts.append(s)
            elif isinstance(item, dict):
                s = str(item.get("text") or item.get("quote") or "").strip()
                if s:
                    parts.append(s)
        return " ".join(parts).strip()
    if isinstance(value, dict):
        return str(value.get("text") or "").strip()
    return str(value).strip()


def _quotes_from_field_value(value: Any) -> List[Dict[str, str]]:
    quotes: List[Dict[str, str]] = []
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                quote = str(item.get("text") or item.get("quote") or "").strip()
                locator = str(item.get("citation_locator") or item.get("locator") or "").strip()
                if quote:
                    quotes.append(
                        {
                            "quote": quote,
                            "citation_locator": locator,
                        }
                    )
    elif isinstance(value, dict):
        quote = str(value.get("text") or value.get("quote") or "").strip()
        locator = str(value.get("citation_locator") or value.get("locator") or "").strip()
        if quote:
            quotes.append(
                {
                    "quote": quote,
                    "citation_locator": locator,
                }
            )
    return quotes


def load_codebook(codebook_path: str | Path) -> Dict[str, Any]:
    p = Path(codebook_path)
    return yaml.safe_load(p.read_text(encoding="utf-8"))


def build_extraction_user_prompt(
    *,
    objective_text: str,
    paper: Dict[str, Any],
    chunks: List[Dict[str, Any]],
    chunks_by_field: Dict[str, List[Dict[str, Any]]],
    required_fields: List[str],
    retry_hint: str | None = None,
) -> str:
    title = paper.get("title") or ""
    journal = paper.get("journal") or ""
    year = paper.get("year") or ""

    header = (
        f"Research objective (user):\n{objective_text}\n\n"
        f"Candidate paper metadata:\n"
        f"- Title: {title}\n"
        f"- Journal: {journal}\n"
        f"- Year: {year}\n"
        f"- PMID: {paper.get('pmid','')}\n"
        f"- DOI: {paper.get('doi','')}\n\n"
        "Evidence excerpts (already selected for relevance by the agent).\n"
        "Each excerpt includes a chunk_id and page location.\n"
    )

    evidence_blocks: List[str] = []
    for field in required_fields:
        cs = chunks_by_field.get(field, [])[:6]
        block = [f"[{field}]"]
        for c in cs:
            loc = f"chunk_id={c.get('chunk_id')} page={c.get('page_start')}-{c.get('page_end')}"
            txt = c.get("text") or ""
            # Keep prompt bounded
            txt = txt[:1200]
            block.append(f"({loc}) {txt}")
        evidence_blocks.append("\n".join(block))

    instructions = (
        "\n\nTask:\n"
        "Extract the following fields as concise text, ONLY using the evidence excerpts.\n"
        "Do NOT invent details.\n\n"
        "Output strict JSON with keys:\n"
        f"- study_design\n"
        f"- research_objective\n"
        f"- causal_or_epidemiologic_methods\n"
        f"- key_findings\n"
        f"- strengths\n"
        f"- limitations\n"
        f"- future_research_agenda\n"
        "- evidence_quotes: object where each field maps to an array of {quote, citation_locator}\n"
        "- confidence_score: number 0-1\n\n"
        "citation_locator must be derived from the provided chunk/page locations.\n"
    )
    if retry_hint:
        instructions += "\nRetry note:\n" + retry_hint.strip() + "\n"
    return header + "\n\n" + "\n\n".join(evidence_blocks) + instructions


def llm_extract_call(
    *,
    provider: str,
    model: str,
    temperature: float,
    system_instructions: str,
    user_prompt: str,
    api_key_env: str,
) -> Dict[str, Any]:
    content = llm_text_call(
        provider=provider,
        model=model,
        temperature=temperature,
        system_instructions=system_instructions,
        user_prompt=user_prompt,
        api_key_env=api_key_env,
    )
    parsed = extract_first_json_object(content) or {}
    return parsed


def naive_extract_if_no_llm(paper: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "study_design": "Not extracted (LLM/API key not configured)",
        "research_objective": "",
        "causal_or_epidemiologic_methods": "Not extracted (LLM/API key not configured)",
        "key_findings": "",
        "strengths": "",
        "limitations": "",
        "future_research_agenda": "",
        "evidence_quotes": {},
        "confidence_score": 0.1,
    }


def extract_paper_fields(
    *,
    paper: Dict[str, Any],
    objective_text: str,
    chunks: List[Dict[str, Any]],
    codebook: Dict[str, Any],
    llm_cfg: Dict[str, Any],
    chunks_top_k: int,
    retry_hint: str | None = None,
) -> Dict[str, Any]:
    required_fields = (codebook.get("extraction_schema", {}) or {}).get("required_fields", DEFAULT_REQUIRED_FIELDS)
    required_fields = required_fields if isinstance(required_fields, list) else DEFAULT_REQUIRED_FIELDS

    chunks_by_field: Dict[str, List[Dict[str, Any]]] = {}
    for field in required_fields:
        chunks_by_field[field] = retrieve_top_chunks(chunks, field_name=field, top_k=chunks_top_k)

    api_key_env = llm_cfg.get("api_key_env", "OPENAI_API_KEY")
    api_key = os.environ.get(api_key_env, "")
    provider = llm_cfg.get("provider", "openai")
    provider_l = str(provider).lower()
    llm_available = (provider_l == "ollama") or bool(api_key)
    if (not llm_available) or (not objective_text) or ("PASTE YOUR RESEARCH OBJECTIVE" in objective_text):
        return naive_extract_if_no_llm(paper)

    model = llm_cfg.get("model", "gpt-4.1-mini")
    temperature = float(llm_cfg.get("temperature", 0.2))

    user_prompt = build_extraction_user_prompt(
        objective_text=objective_text,
        paper=paper,
        chunks=chunks,
        chunks_by_field=chunks_by_field,
        required_fields=required_fields,
        retry_hint=retry_hint,
    )

    system_instructions = (
        "You are extracting data for a scoping review. "
        "Use evidence excerpts only. "
        "Return strict JSON matching the requested keys. "
        "If evidence is missing, leave the field as empty string and set low confidence."
    )

    parsed = llm_extract_call(
        provider=provider,
        model=model,
        temperature=temperature,
        system_instructions=system_instructions,
        user_prompt=user_prompt,
        api_key_env=api_key_env,
    )
    # Ensure required keys exist.
    evidence_quotes = parsed.get("evidence_quotes")
    if not isinstance(evidence_quotes, dict):
        evidence_quotes = {}

    for k in ["study_design", "research_objective", "causal_or_epidemiologic_methods", "key_findings", "strengths", "limitations", "future_research_agenda"]:
        if k not in parsed:
            parsed[k] = ""
        else:
            raw_value = parsed.get(k)
            parsed[k] = _stringify_field_value(raw_value)
            if k not in evidence_quotes:
                inferred_quotes = _quotes_from_field_value(raw_value)
                if inferred_quotes:
                    evidence_quotes[k] = inferred_quotes
    parsed["evidence_quotes"] = evidence_quotes
    if "confidence_score" not in parsed:
        parsed["confidence_score"] = 0.2
    return parsed

