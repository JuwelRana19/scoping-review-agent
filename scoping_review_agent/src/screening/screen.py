from __future__ import annotations

import re
from typing import Any, Dict, List


def _normalize_blob(p: Dict[str, Any]) -> str:
    t = (p.get("title") or "") + " " + (p.get("abstract") or "") + " " + (p.get("journal") or "")
    return t.lower()


def heuristic_screen(p: Dict[str, Any], objective_text: str, include_keywords: List[str], exclude_keywords: List[str]) -> Dict[str, Any]:
    """
    Deterministic fallback. Uses keywords for include/exclude; objective_text is used only for sanity.
    """
    blob = _normalize_blob(p)
    reasons: List[str] = []

    if exclude_keywords:
        for kw in exclude_keywords:
            if not kw:
                continue
            if kw.lower() in blob:
                return {
                    "decision": "exclude",
                    "reasons": [f"Matched exclusion keyword: {kw}"],
                    "evidence_snippet": "",
                    "objective_alignment_tags": [],
                }

    if include_keywords:
        for kw in include_keywords:
            if not kw:
                continue
            if kw.lower() in blob:
                return {
                    "decision": "include",
                    "reasons": [f"Matched inclusion keyword: {kw}"],
                    "evidence_snippet": "",
                    "objective_alignment_tags": [kw],
                }

    # If the user hasn't provided an objective, keep uncertain.
    if not objective_text or "PASTE YOUR RESEARCH OBJECTIVE" in objective_text:
        return {
            "decision": "uncertain",
            "reasons": ["Research objective not configured; using heuristic fallback."],
            "evidence_snippet": "",
            "objective_alignment_tags": [],
        }

    return {
        "decision": "uncertain",
        "reasons": ["No explicit include/exclude keywords matched; needs objective-based judgement."],
        "evidence_snippet": "",
        "objective_alignment_tags": [],
    }


def build_screening_user_prompt(
    p: Dict[str, Any],
    objective_text: str,
    *,
    keyword_hints: List[str] | None = None,
    search_strategy: str | None = None,
) -> str:
    title = p.get("title") or ""
    abstract = p.get("abstract") or ""
    journal = p.get("journal") or ""
    year = p.get("year") or ""

    evidence_parts = [
        f"Title: {title}",
        f"Journal: {journal}",
        f"Year: {year}",
        f"Abstract: {abstract}" if abstract else "Abstract: (not available)",
    ]

    keyword_section = ""
    if keyword_hints:
        kh = [k for k in keyword_hints if k and str(k).strip()]
        if kh:
            keyword_section = "Keyword hints for relevance (use these to interpret the objective):\n" + ", ".join([str(x) for x in kh]) + "\n\n"

    strategy_section = ""
    if search_strategy and str(search_strategy).strip():
        strategy_section = "Search strategy / candidate retrieval terms (may include synonyms):\n" + str(search_strategy).strip() + "\n\n"

    return (
        "Your research objective:\n"
        f"{objective_text}\n\n"
        + keyword_section
        + strategy_section
        + "Candidate study evidence (for screening):\n"
        + "\n".join(evidence_parts)
        + "\n\n"
        + "Decide if this study should be INCLUDED in the scoping review relative to the objective."
        + " Output strict JSON with keys: decision, reasons, evidence_snippet, objective_alignment_tags."
    )

