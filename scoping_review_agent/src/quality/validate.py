from __future__ import annotations

from typing import Any, Dict, List, Tuple


def is_empty(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        return not v.strip()
    if isinstance(v, (list, dict)):
        return len(v) == 0
    return False


def quality_check_extraction(
    row: Dict[str, Any],
    *,
    required_fields: List[str],
    require_evidence_quotes: bool = True,
    confidence_min: float = 0.3,
) -> Dict[str, Any]:
    """
    Produces flags for missing/low-confidence extraction.
    """
    flags: List[str] = []
    evidence_quotes = row.get("evidence_quotes") or {}

    for field in required_fields:
        if field not in row:
            flags.append(f"Missing field key: {field}")
            continue
        if is_empty(row.get(field)):
            flags.append(f"Empty extracted value: {field}")

        if require_evidence_quotes:
            q = evidence_quotes.get(field, [])
            if not isinstance(q, list) or len(q) == 0:
                flags.append(f"No evidence quotes found for: {field}")

    try:
        conf = float(row.get("confidence_score") or 0.0)
    except Exception:
        conf = 0.0
    if conf < confidence_min:
        flags.append(f"Low confidence_score: {conf:.2f}")

    needs_human_review = len(flags) > 0
    return {
        "quality_flags": flags,
        "needs_human_review": needs_human_review,
        "confidence_score": conf,
    }

