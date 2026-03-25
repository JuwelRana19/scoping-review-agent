from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict


def _sha1_bytes(b: bytes) -> str:
    return hashlib.sha1(b).hexdigest()


def file_sha1(path: str | Path) -> str:
    p = Path(path)
    return _sha1_bytes(p.read_bytes())


def load_state(state_path: str | Path) -> Dict[str, Any]:
    p = Path(state_path)
    if not p.exists():
        return {
            "version": 1,
            "objective_hash": "",
            "codebook_hash": "",
            "papers": {},
        }
    return json.loads(p.read_text(encoding="utf-8"))


def save_state(state_path: str | Path, state: Dict[str, Any]) -> None:
    p = Path(state_path)
    p.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")

