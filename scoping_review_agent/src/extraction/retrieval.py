from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple


def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()


def score_chunk_for_field(chunk_text: str, field_name: str) -> int:
    """
    Very simple lexical scoring MVP. This is deterministic and cheap.
    You can replace with embeddings later.
    """
    t = normalize_text(chunk_text)
    # Field-name-driven keywords (lightweight heuristic).
    field_kw = {
        "study_design": ["design", "cohort", "cross-sectional", "case-control", "random", "trial", "protocol", "simulation", "scoping review", "meta-analysis"],
        "research_objective": ["objective", "aim", "purpose", "we aimed", "to assess", "to examine"],
        "causal_or_epidemiologic_methods": ["causal", "mediation", "g-formula", "g formula", "dag", "doubly robust", "marginal", "structural", "wqs", "bkmr", "bkmr", "confounding", "adjust", "identification", "emulation"],
        "key_findings": ["result", "results", "we found", "associated", "association", "hazard", "risk", "effect", "odds", "prevalence", "incidence"],
        "strengths": ["strength", "robust", "robustness", "validated", "we controlled", "sensitivity", "large sample"],
        "limitations": ["limitation", "limitations", "bias", "uncertainty", "we acknowledge", "residual", "generalizability"],
        "future_research_agenda": ["future", "further", "recommend", "research is needed", "agenda"],
    }
    kws = field_kw.get(field_name, [field_name])
    score = 0
    for kw in kws:
        if kw.lower() in t:
            score += 1
    # Extra: boost matches on the field name itself.
    if normalize_text(field_name) in t:
        score += 1
    return score


def retrieve_top_chunks(
    chunks: List[Dict[str, Any]],
    *,
    field_name: str,
    top_k: int,
) -> List[Dict[str, Any]]:
    scored: List[Tuple[int, Dict[str, Any]]] = []
    for c in chunks:
        s = score_chunk_for_field(c.get("text", "") or "", field_name=field_name)
        if s > 0:
            scored.append((s, c))
    scored.sort(key=lambda x: x[0], reverse=True)
    top = [c for _, c in scored[:top_k]]
    # if nothing scored, just return first chunks as fallback
    if not top:
        return chunks[:top_k]
    return top

