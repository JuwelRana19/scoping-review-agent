from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from scoping_review_agent.src.outputs.csv_tables import export_extractions_csv, export_summary_tables
from scoping_review_agent.src.utils.io import ensure_dir


def export_outputs(
    *,
    extractions_jsonl_path: str | Path,
    output_dir: str | Path,
) -> Dict[str, Any]:
    output_dir = Path(output_dir)
    ensure_dir(output_dir)

    df = export_extractions_csv(extractions_jsonl_path=extractions_jsonl_path, output_dir=output_dir)
    export_summary_tables(df, output_dir=output_dir)
    doc_path = None
    try:
        from scoping_review_agent.src.outputs.word_export import export_word_document

        doc_path = export_word_document(extractions_jsonl_path=extractions_jsonl_path, output_dir=output_dir)
    except ModuleNotFoundError as exc:
        if exc.name != "docx":
            raise

    return {
        "extractions_csv": str(output_dir / "extractions.csv"),
        "word_document": str(doc_path) if doc_path else None,
    }

