from __future__ import annotations

import argparse
import hashlib
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from scoping_review_agent.src.config import AgentConfig
from scoping_review_agent.src.ingestion.pubmed_source import ingest_candidates_for_objective
from scoping_review_agent.src.ingestion.local_pdf_source import ingest_local_pdf_folder
from scoping_review_agent.src.outputs.pipeline import export_outputs
from scoping_review_agent.src.pdf_acquisition.pipeline import acquire_pdfs
from scoping_review_agent.src.extraction.pipeline import extract_from_pdfs
from scoping_review_agent.src.screening.pipeline import screen_candidates
from scoping_review_agent.src.state import load_state, save_state
from scoping_review_agent.src.objectives import load_objectives
from scoping_review_agent.src.utils.io import write_jsonl


def resolve_output_dir(template: str | Path) -> Path:
    """
    Supports simple token substitution for convenience in config.
    """
    s = str(template)
    now = datetime.now()
    s = s.replace("YYYYMMDD", now.strftime("%Y%m%d"))
    s = s.replace("HHMMSS", now.strftime("%H%M%S"))
    s = s.replace("HHMM", now.strftime("%H%M"))
    return Path(s)


def sha1_text(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Path to config.yaml")
    ap.add_argument("--objectives_file", required=False, help="Path to objectives.json/yaml for multi-objective runs")
    args = ap.parse_args()

    cfg = AgentConfig.load(args.config)

    out_template = (cfg.outputs or {}).get("output_dir", "outputs/scoping_review_run_YYYYMMDD_HHMMSS")
    out_root = resolve_output_dir(out_template)

    state_file = (cfg.outputs or {}).get("state_file", "state.json")
    state_path = out_root / state_file

    out_root.mkdir(parents=True, exist_ok=True)

    state = load_state(state_path)

    # Load codebook path relative to this package.
    codebook_path = Path(__file__).parent / "codebook.yaml"

    # Load objectives list.
    if args.objectives_file:
        objectives = load_objectives(args.objectives_file)
    else:
        objective_text = (cfg.screening or {}).get("objective_text") or ""
        objective_text = objective_text.strip()
        if not objective_text:
            raise ValueError("No objectives_file provided and config.screening.objective_text is empty.")
        default_objective_id = sha1_text(objective_text)[:12]
        objectives = [
            {
                "objective_id": default_objective_id,
                "objective_text": objective_text,
                "keywords": (cfg.screening or {}).get("include_keywords") or [],
            }
        ]

    state.setdefault("objectives", {})
    # update codebook hash
    state["codebook_hash"] = sha1_text(codebook_path.read_text(encoding="utf-8"))

    for obj in objectives:
        objective_id = str(obj.get("objective_id") or "").strip()
        objective_text = str(obj.get("objective_text") or "").strip()
        if not objective_id or not objective_text:
            continue

        objective_out_root = out_root / objective_id
        objective_out_root.mkdir(parents=True, exist_ok=True)

        ingestion_dir = objective_out_root / "ingestion"
        screening_dir = objective_out_root / "screening"
        pdf_dir = objective_out_root / "pdfs"
        extraction_dir = objective_out_root / "extraction"
        exports_dir = objective_out_root / "exports"

        source_mode = str(obj.get("source_mode") or "pubmed_only").strip().lower()
        if source_mode not in {"pubmed_only", "local_pdf_only", "pubmed_plus_local_pdf"}:
            source_mode = "pubmed_only"

        # 1) Ingest candidates (objective-specific)
        pubmed_candidates: list[dict[str, Any]] = []
        local_candidates: list[dict[str, Any]] = []
        local_acquisitions: list[dict[str, Any]] = []

        if source_mode in {"pubmed_only", "pubmed_plus_local_pdf"}:
            pubmed_candidates = ingest_candidates_for_objective(
                obj,
                output_dir=ingestion_dir / "pubmed",
                llm_cfg=cfg.llm,
            )

        if source_mode in {"local_pdf_only", "pubmed_plus_local_pdf"}:
            local_pdf_folder = (obj.get("local_pdf_folder") or "").strip()
            if local_pdf_folder:
                local_candidates, local_acquisitions = ingest_local_pdf_folder(
                    local_pdf_folder=local_pdf_folder,
                    output_dir=ingestion_dir / "local_pdf",
                )

        # Merge candidates and dedupe by paper_id
        candidates: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for c in pubmed_candidates + local_candidates:
            pid = str(c.get("paper_id") or "").strip()
            if not pid or pid in seen_ids:
                continue
            seen_ids.add(pid)
            candidates.append(c)
        write_jsonl(ingestion_dir / "candidates.jsonl", candidates)
        candidates_jsonl = ingestion_dir / "candidates.jsonl"

        # 2) Screen (objective-specific)
        screening_cfg = dict(cfg.screening or {})
        screening_cfg["objective_text"] = objective_text
        # Keyword hints:
        screening_cfg["include_keywords"] = obj.get("screening_include_keywords") or obj.get("keywords") or screening_cfg.get("include_keywords") or []
        screening_cfg["exclude_keywords"] = obj.get("screening_exclude_keywords") or screening_cfg.get("exclude_keywords") or []
        screening_cfg["search_strategy"] = obj.get("search_strategy") or obj.get("pubmed_term") or obj.get("pubmed_search_url") or ""

        screening_records = screen_candidates(
            candidates_jsonl_path=candidates_jsonl,
            config={
                "screening": screening_cfg,
                "llm": cfg.llm,
                "extraction": cfg.extraction,
            },
            output_dir=screening_dir,
        )
        screening_csv = screening_dir / "screening.csv"

        # 3) Acquire PDFs (include/uncertain only)
        # - PubMed candidates: locate/download by DOI/PMID
        # - Local PDF candidates: already acquired
        pdf_acquisitions: list[dict[str, Any]] = []
        if pubmed_candidates:
            write_jsonl(ingestion_dir / "pubmed_candidates.jsonl", pubmed_candidates)
            pubmed_pdf_acq = acquire_pdfs(
                candidates_jsonl_path=ingestion_dir / "pubmed_candidates.jsonl",
                pdfs_root_dir=pdf_dir,
                screening_csv_path=screening_csv,
                config={
                    "pdf_acquisition": cfg.pdf_acquisition,
                },
            )
            pdf_acquisitions.extend(pubmed_pdf_acq or [])
        if local_acquisitions:
            pdf_acquisitions.extend(local_acquisitions)
        write_jsonl(pdf_dir / "pdf_acquisitions.jsonl", pdf_acquisitions)
        pdf_acquisitions_jsonl = pdf_dir / "pdf_acquisitions.jsonl"

        # 4) Extract (objective-specific)
        extractions = extract_from_pdfs(
            candidates_jsonl_path=candidates_jsonl,
            pdf_acquisitions_jsonl_path=pdf_acquisitions_jsonl,
            screening_csv_path=screening_csv,
            config={
                "screening": screening_cfg,
                "llm": cfg.llm,
                "extraction": cfg.extraction,
            },
            codebook_path=codebook_path,
            pdf_cache_dir=pdf_dir / "pdf_text_cache",
            output_dir=extraction_dir,
        )

        # 5) Outputs
        export_outputs(
            extractions_jsonl_path=extraction_dir / "extractions.jsonl",
            output_dir=exports_dir,
        )

        # Update state per objective.
        objective_hash = sha1_text(objective_text)
        screening_map: Dict[str, Dict[str, Any]] = {}
        for r in screening_records:
            pid = str(r.get("paper_id") or "")
            if pid:
                screening_map[pid] = r

        state["objectives"][objective_id] = {
            "objective_hash": objective_hash,
            "n_candidates": len(candidates) if candidates is not None else None,
            "n_extractions": len(extractions) if extractions is not None else None,
            "papers": {},
        }

        for ex in extractions:
            pid = str(ex.get("paper_id") or "")
            if not pid:
                continue
            state["objectives"][objective_id]["papers"][pid] = {
                "decision": screening_map.get(pid, {}).get("decision", ""),
                "pdf_status": ex.get("pdf_status", ""),
                "pdf_file_hash": ex.get("pdf_file_hash", ""),
                "confidence_score": ex.get("confidence_score", None),
                "human_review_required": bool(ex.get("human_review_required", False)),
            }

    save_state(state_path, state)
    print(f"Pipeline complete. Outputs in: {out_root}")


if __name__ == "__main__":
    main()

