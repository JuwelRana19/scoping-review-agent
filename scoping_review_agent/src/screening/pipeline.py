from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from scoping_review_agent.src.screening.llm_client import llm_screening_call
from scoping_review_agent.src.screening.screen import build_screening_user_prompt, heuristic_screen
from scoping_review_agent.src.utils.io import read_jsonl, ensure_dir


def screen_candidates(
    candidates_jsonl_path: str | Path,
    config: Dict[str, Any],
    output_dir: str | Path,
) -> List[Dict[str, Any]]:
    output_dir = Path(output_dir)
    ensure_dir(output_dir)

    candidates = read_jsonl(candidates_jsonl_path)
    objective_text = (config.get("screening", {}) or {}).get("objective_text") or ""
    include_keywords = (config.get("screening", {}) or {}).get("include_keywords") or []
    exclude_keywords = (config.get("screening", {}) or {}).get("exclude_keywords") or []
    keyword_hints = include_keywords
    search_strategy = (config.get("screening", {}) or {}).get("search_strategy") or (config.get("screening", {}) or {}).get("pubmed_term") or ""

    llm_cfg = config.get("llm", {}) or {}
    api_key_env = llm_cfg.get("api_key_env", "OPENAI_API_KEY")
    api_key = os.environ.get(api_key_env, "")
    provider = llm_cfg.get("provider", "openai")
    provider_l = str(provider).lower()
    llm_available = (provider_l == "ollama") or bool(api_key)
    model = llm_cfg.get("model", "gpt-4.1-mini")
    temperature = llm_cfg.get("temperature", 0.2)

    screening_records: List[Dict[str, Any]] = []
    for p in candidates:
        # Heuristic pass first (fast & deterministic).
        rec = heuristic_screen(p, objective_text=objective_text, include_keywords=include_keywords, exclude_keywords=exclude_keywords)

        # If heuristic is uncertain and we have an API key + objective configured, call LLM.
        if rec.get("decision") == "uncertain" and llm_available and objective_text and "PASTE YOUR RESEARCH OBJECTIVE" not in objective_text:
            system_instructions = (
                "You are a systematic-review assistant. Be strict and evidence-grounded. "
                "Only decide include/exclude based on relevance to the objective. "
                "If uncertain, set decision=uncertain and provide why."
            )
            user_prompt = build_screening_user_prompt(
                p,
                objective_text=objective_text,
                keyword_hints=keyword_hints,
                search_strategy=search_strategy,
            )
            llm_out = llm_screening_call(
                provider=provider,
                model=model,
                temperature=float(temperature),
                system_instructions=system_instructions,
                user_prompt=user_prompt,
                api_key_env=api_key_env,
            )
            # Merge heuristic reasons with LLM reasons.
            rec = {
                "paper_id": p.get("paper_id", ""),
                "pmid": p.get("pmid", ""),
                "decision": llm_out.get("decision") or rec.get("decision", "uncertain"),
                "reasons": llm_out.get("reasons") or rec.get("reasons") or [],
                "evidence_snippet": llm_out.get("evidence_snippet") or rec.get("evidence_snippet") or "",
                "objective_alignment_tags": llm_out.get("objective_alignment_tags") or [],
            }

        screening_records.append(rec)

    # Write outputs
    df = pd.DataFrame(screening_records)
    df.to_csv(output_dir / "screening.csv", index=False)
    # Keep a JSONL for traceability.
    with (output_dir / "screening.jsonl").open("w", encoding="utf-8") as f:
        for r in screening_records:
            f.write(pd.Series(r).to_json() + "\n")

    return screening_records

