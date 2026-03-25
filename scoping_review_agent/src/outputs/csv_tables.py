from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from scoping_review_agent.src.utils.io import ensure_dir, read_jsonl, write_jsonl


def export_extractions_csv(
    *,
    extractions_jsonl_path: str | Path,
    output_dir: str | Path,
) -> pd.DataFrame:
    output_dir = Path(output_dir)
    ensure_dir(output_dir)

    rows = read_jsonl(extractions_jsonl_path)
    df = pd.DataFrame(rows)
    df.to_csv(output_dir / "extractions.csv", index=False)
    df.to_json(output_dir / "extractions.json", orient="records", force_ascii=False, indent=2)
    return df


def export_summary_tables(
    df: pd.DataFrame,
    *,
    output_dir: str | Path,
) -> None:
    output_dir = Path(output_dir)
    ensure_dir(output_dir / "tables")

    # Table 1: counts by extracted study design (top-level heuristic).
    if "study_design" in df.columns:
        t1 = df[["paper_id", "study_design"]].copy()
        t1["study_design_clean"] = t1["study_design"].fillna("").astype(str).str.strip()
        t1 = t1[t1["study_design_clean"] != ""]
        counts = t1.groupby("study_design_clean").size().reset_index(name="n_studies").sort_values("n_studies", ascending=False)
        counts.to_csv(output_dir / "tables" / "study_design_counts.csv", index=False)

    # Table 2: causal/epi methods presence.
    if "causal_or_epidemiologic_methods" in df.columns:
        t2 = df[["paper_id", "causal_or_epidemiologic_methods"]].copy()
        t2["has_causal_methods"] = t2["causal_or_epidemiologic_methods"].fillna("").astype(str).str.strip().ne("")
        by = t2.groupby("has_causal_methods").size().reset_index(name="n_studies")
        by.to_csv(output_dir / "tables" / "causal_methods_presence.csv", index=False)

    # Table 3: quality flags count.
    if "quality_flags" in df.columns:
        n_review = df.get("human_review_required", pd.Series([False] * len(df))).sum() if len(df) else 0
        pd.DataFrame([{"human_review_required_count": int(n_review)}]).to_csv(
            output_dir / "tables" / "human_review_required_summary.csv", index=False
        )

