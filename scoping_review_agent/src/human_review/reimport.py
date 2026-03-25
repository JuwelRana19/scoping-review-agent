from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from docx import Document

from scoping_review_agent.src.utils.io import ensure_dir, read_jsonl


DECISION_RE = re.compile(r"^Decision \(include/exclude/uncertain\):\s*(.*)$", flags=re.IGNORECASE)
NOTES_RE = re.compile(r"^Notes / corrections:\s*(.*)$", flags=re.IGNORECASE)
PAPER_ID_RE = re.compile(r"^Paper ID:\s*(.*)$", flags=re.IGNORECASE)


def reimport_from_word(
    *,
    word_docx_path: str | Path,
    extractions_jsonl_path: str | Path,
    output_dir: str | Path,
) -> Dict[str, str]:
    """
    Parse the edited Word document and extract human decisions/notes.

    Assumes the Word doc structure produced by `src/outputs/word_export.py`.
    """
    word_docx_path = Path(word_docx_path)
    output_dir = Path(output_dir)
    ensure_dir(output_dir)

    rows = read_jsonl(extractions_jsonl_path)
    by_pid: Dict[str, Dict[str, Any]] = {str(r.get("paper_id") or ""): r for r in rows if str(r.get("paper_id") or "")}

    doc = Document(str(word_docx_path))
    paragraphs = [p.text.strip() for p in doc.paragraphs if p.text and p.text.strip()]

    out_updates: List[Dict[str, Any]] = []
    current_pid: Optional[str] = None
    pending_decision: Optional[str] = None
    pending_notes: Optional[str] = None

    def flush() -> None:
        nonlocal pending_decision, pending_notes, current_pid
        if current_pid:
            out_updates.append(
                {
                    "paper_id": current_pid,
                    "human_decision": (pending_decision or "").strip(),
                    "human_notes": (pending_notes or "").strip(),
                }
            )
        pending_decision = None
        pending_notes = None

    for t in paragraphs:
        m_pid = PAPER_ID_RE.match(t)
        if m_pid:
            # new section: flush previous
            if current_pid and (pending_decision is not None or pending_notes is not None):
                flush()
            current_pid = (m_pid.group(1) or "").strip()
            continue

        m_dec = DECISION_RE.match(t)
        if m_dec:
            pending_decision = m_dec.group(1) or ""
            continue

        m_notes = NOTES_RE.match(t)
        if m_notes:
            pending_notes = m_notes.group(1) or ""
            continue

    # flush last
    if current_pid and (pending_decision is not None or pending_notes is not None):
        flush()

    updates_df = pd.DataFrame(out_updates)
    updates_df.to_csv(output_dir / "human_updates.csv", index=False)

    # Merge into extractions
    base_df = pd.DataFrame(rows)
    merged = base_df.merge(updates_df, on="paper_id", how="left")
    merged.to_csv(output_dir / "extractions_with_human.csv", index=False)

    return {
        "human_updates_csv": str(output_dir / "human_updates.csv"),
        "merged_csv": str(output_dir / "extractions_with_human.csv"),
    }

