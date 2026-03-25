from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


@dataclass(frozen=True)
class AgentConfig:
    ingestion: Dict[str, Any]
    pdf_acquisition: Dict[str, Any]
    screening: Dict[str, Any]
    extraction: Dict[str, Any]
    llm: Dict[str, Any]
    outputs: Dict[str, Any]

    @staticmethod
    def load(path: str | Path) -> "AgentConfig":
        p = Path(path)
        raw = yaml.safe_load(p.read_text(encoding="utf-8"))
        return AgentConfig(
            ingestion=raw.get("ingestion", {}),
            pdf_acquisition=raw.get("pdf_acquisition", {}),
            screening=raw.get("screening", {}),
            extraction=raw.get("extraction", {}),
            llm=raw.get("llm", {}),
            outputs=raw.get("outputs", {}),
        )


def ensure_required(cfg: AgentConfig, required_keys: List[str]) -> None:
    """
    required_keys: list of paths like "ingestion.pubmed_mybibliography_public_url"
    """
    for key in required_keys:
        cur: Any = cfg
        parts = key.split(".")
        for part in parts:
            if isinstance(cur, AgentConfig):
                cur = getattr(cur, parts[0])  # should not happen
            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                cur = None
        if cur is None or cur == "":
            raise ValueError(f"Missing required config value: {key}")

