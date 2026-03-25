from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from scoping_review_agent.src.extraction.extract import extract_paper_fields, load_codebook
from scoping_review_agent.src.pdf_parsing.parse import parse_pdf_to_pages_and_chunks
from scoping_review_agent.src.quality.validate import quality_check_extraction
from scoping_review_agent.src.utils.io import ensure_dir, read_jsonl, read_json, write_json, write_jsonl


def extract_from_pdfs(
    *,
    candidates_jsonl_path: str | Path,
    pdf_acquisitions_jsonl_path: str | Path,
    screening_csv_path: str | Path | None,
    config: Dict[str, Any],
    codebook_path: str | Path,
    pdf_cache_dir: str | Path,
    output_dir: str | Path,
) -> List[Dict[str, Any]]:
    """
    Extract evidence-grounded structured fields from cached PDF text chunks.
    """
    output_dir = Path(output_dir)
    ensure_dir(output_dir)
    pdf_cache_dir = Path(pdf_cache_dir)
    ensure_dir(pdf_cache_dir)

    candidates = {str(c.get("paper_id")): c for c in read_jsonl(candidates_jsonl_path)}
    acquisitions = read_jsonl(pdf_acquisitions_jsonl_path)

    include_ids: set[str] | None = None
    if screening_csv_path:
        df = pd.read_csv(screening_csv_path)
        include_ids = set(
            df.loc[df["decision"].isin(["include", "uncertain"]), "paper_id"]
            .astype(str)
            .tolist()
        )

    objective_text = (config.get("screening") or {}).get("objective_text") or ""
    extraction_cfg = config.get("extraction") or {}
    chunk_cfg = extraction_cfg.get("chunk") or {}
    max_chars = int(chunk_cfg.get("max_chars", 6000))
    overlap_chars = int(chunk_cfg.get("overlap_chars", 800))
    top_k_chunks = int(chunk_cfg.get("top_k_chunks", 8))

    codebook = load_codebook(codebook_path)
    llm_cfg = config.get("llm") or {}
    required_fields = (codebook.get("extraction_schema") or {}).get("required_fields") or []
    if not isinstance(required_fields, list) or not required_fields:
        required_fields = [
            "study_design",
            "research_objective",
            "key_findings",
            "strengths",
            "limitations",
            "future_research_agenda",
            "causal_or_epidemiologic_methods",
        ]

    extraction_cache_dir = output_dir / "extraction_cache"
    ensure_dir(extraction_cache_dir)
    quality_require_quotes = bool((config.get("extraction") or {}).get("require_evidence_quotes", True))

    out_rows: List[Dict[str, Any]] = []
    for acq in acquisitions:
        paper_id = str(acq.get("paper_id") or "")
        if not paper_id:
            continue
        if include_ids is not None and paper_id not in include_ids:
            continue
        pdf_path = str(acq.get("pdf_path") or "")
        pdf_status = acq.get("pdf_status") or ""
        if not pdf_path or pdf_status == "missing":
            continue

        paper = candidates.get(paper_id) or dict(acq)

        parsed = parse_pdf_to_pages_and_chunks(
            paper_id=paper_id,
            pdf_path=pdf_path,
            cache_dir=pdf_cache_dir,
            max_chars=max_chars,
            overlap_chars=overlap_chars,
        )
        file_hash = str(parsed.get("file_hash") or "")

        cache_key_path = extraction_cache_dir / f"{paper_id}__{file_hash}.json"
        if cache_key_path.exists():
            try:
                cached_row = read_json(cache_key_path)
                if isinstance(cached_row, dict) and cached_row:
                    out_rows.append(cached_row)
                    continue
            except Exception:
                pass

        extracted = extract_paper_fields(
            paper=paper,
            objective_text=objective_text,
            chunks=parsed.get("chunks") or [],
            codebook=codebook,
            llm_cfg=llm_cfg,
            chunks_top_k=top_k_chunks,
        )
        extracted_row = dict(paper)
        extracted_row.update(extracted)
        extracted_row["pdf_status"] = pdf_status
        extracted_row["pdf_file_hash"] = file_hash

        quality = quality_check_extraction(
            extracted_row,
            required_fields=required_fields,
            require_evidence_quotes=quality_require_quotes,
            confidence_min=float((config.get("extraction") or {}).get("confidence_min", 0.3)),
        )
        extracted_row.update(quality)
        # Convenience: a human-review boolean based on quality.
        extracted_row["human_review_required"] = bool(quality.get("needs_human_review"))

        retry_enabled = bool((config.get("extraction") or {}).get("retry_on_quality_issues", False))
        max_retries = int((config.get("extraction") or {}).get("max_retries", 1))
        api_key_env = llm_cfg.get("api_key_env", "OPENAI_API_KEY")
        api_key = os.environ.get(api_key_env, "")
        if retry_enabled and extracted_row["human_review_required"] and api_key and max_retries >= 1:
            retry_hint = "Quality issues detected: " + ", ".join(quality.get("quality_flags") or [])
            extracted_retry = extract_paper_fields(
                paper=paper,
                objective_text=objective_text,
                chunks=parsed.get("chunks") or [],
                codebook=codebook,
                llm_cfg=llm_cfg,
                chunks_top_k=top_k_chunks,
                retry_hint=retry_hint,
            )
            extracted_row.update(extracted_retry)
            quality_retry = quality_check_extraction(
                extracted_row,
                required_fields=required_fields,
                require_evidence_quotes=quality_require_quotes,
                confidence_min=float((config.get("extraction") or {}).get("confidence_min", 0.3)),
            )
            extracted_row.update(quality_retry)
            extracted_row["human_review_required"] = bool(quality_retry.get("needs_human_review"))

        write_json(cache_key_path, extracted_row)
        out_rows.append(extracted_row)

    write_jsonl(output_dir / "extractions.jsonl", out_rows)
    # A separate file makes it easier to audit extraction quality over time.
    write_jsonl(output_dir / "extraction_quality.jsonl", out_rows)
    return out_rows

