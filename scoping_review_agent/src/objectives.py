from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


def sha1_text(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()


def _load_yaml_or_json(path: str | Path) -> Any:
    p = Path(path)
    txt = p.read_text(encoding="utf-8")
    if p.suffix.lower() in {".json"}:
        return json.loads(txt)
    return yaml.safe_load(txt)


def load_objectives(objectives_path: str | Path) -> List[Dict[str, Any]]:
    """
    Objectives file format:
    - either a list of objective objects, OR a dict with key `objectives`.

    Each objective object supports:
    - objective_id (optional; if missing it is derived from objective_text)
    - objective_text (required)
    - source_mode (optional): pubmed_only | local_pdf_only | pubmed_plus_local_pdf
    - local_pdf_folder (optional): folder path for manually uploaded PDFs
    - keywords (optional; list[str]) keyword hints for screening + query generation
    - search_strategy (optional; string) exact PubMed query syntax (preferred)
    - pubmed_term (optional; string) esearch term string
    - pubmed_search_url (optional; string) a PubMed URL containing term=...
    - candidate_pmids (optional; list[str]) manual set of PMIDs to screen
    - candidate_dois (optional; list[str]) manual set of DOIs to screen (mapped to PMIDs)
    - screening_include_keywords/exclude_keywords (optional; overrides `keywords` for heuristic screening)
    """
    raw = _load_yaml_or_json(objectives_path)
    if isinstance(raw, dict) and "objectives" in raw:
        raw = raw["objectives"]
    if not isinstance(raw, list):
        raise ValueError("objectives file must contain a list or a dict with key 'objectives'")

    out: List[Dict[str, Any]] = []
    for obj in raw:
        if not isinstance(obj, dict):
            continue
        objective_text = (obj.get("objective_text") or "").strip()
        if not objective_text:
            raise ValueError("Each objective must have non-empty objective_text")
        objective_id = (obj.get("objective_id") or "").strip() or sha1_text(objective_text)[:12]
        out.append({**obj, "objective_id": objective_id, "objective_text": objective_text})
    return out

