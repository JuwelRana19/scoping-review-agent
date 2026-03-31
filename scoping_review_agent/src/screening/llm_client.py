from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, Optional

import requests


def extract_first_json_object(text: str) -> Optional[Dict[str, Any]]:
    """
    Best-effort extraction of a single JSON object from an LLM response.
    """
    if not text:
        return None
    # Try fenced JSON first.
    m = re.search(r"```json\\s*(\\{.*?\\})\\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    # Fallback: first {...}
    m = re.search(r"(\\{.*\\})", text, flags=re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None


def llm_screening_call(
    *,
    provider: str,
    model: str,
    temperature: float,
    system_instructions: str,
    user_prompt: str,
    api_key_env: str = "OPENAI_API_KEY",
) -> Dict[str, Any]:
    content = llm_text_call(
        provider=provider,
        model=model,
        temperature=temperature,
        system_instructions=system_instructions,
        user_prompt=user_prompt,
        api_key_env=api_key_env,
    )
    parsed = extract_first_json_object(content) or {}
    return parsed


def llm_text_call(
    *,
    provider: str,
    model: str,
    temperature: float,
    system_instructions: str,
    user_prompt: str,
    api_key_env: str,
) -> str:
    provider = (provider or "openai").lower().strip()
    api_key = os.environ.get(api_key_env, "")

    if provider == "openai":
        if not api_key:
            raise ValueError(f"Missing API key in env var: {api_key_env}")
        url = "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": f"Bearer {api_key}"}
        payload = {
            "model": model,
            "temperature": temperature,
            "messages": [
                {"role": "system", "content": system_instructions},
                {"role": "user", "content": user_prompt},
            ],
        }
        r = requests.post(url, headers=headers, json=payload, timeout=180)
        r.raise_for_status()
        data = r.json()
        return ((data.get("choices") or [{}])[0].get("message") or {}).get("content", "")

    if provider == "gemini":
        if not api_key:
            raise ValueError(f"Missing API key in env var: {api_key_env}")
        # model example: gemini-1.5-pro or gemini-1.5-flash
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
        payload = {
            "generationConfig": {"temperature": temperature},
            "contents": [
                {"role": "user", "parts": [{"text": f"System instructions:\n{system_instructions}\n\nUser request:\n{user_prompt}"}]}
            ],
        }
        r = requests.post(url, json=payload, timeout=180)
        r.raise_for_status()
        data = r.json()
        cands = data.get("candidates", []) or []
        if not cands:
            return ""
        parts = (((cands[0] or {}).get("content") or {}).get("parts") or [])
        return "\n".join([(p.get("text") or "") for p in parts if isinstance(p, dict)])

    if provider in {"anthropic", "claude"}:
        if not api_key:
            raise ValueError(f"Missing API key in env var: {api_key_env}")
        # model example: claude-3-5-sonnet-latest
        url = "https://api.anthropic.com/v1/messages"
        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        payload = {
            "model": model,
            "max_tokens": 2048,
            "temperature": temperature,
            "system": system_instructions,
            "messages": [{"role": "user", "content": user_prompt}],
        }
        r = requests.post(url, headers=headers, json=payload, timeout=180)
        r.raise_for_status()
        data = r.json()
        content = data.get("content", []) or []
        texts = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                texts.append(item.get("text") or "")
        return "\n".join(texts)

    if provider == "ollama":
        # model example: llama3.1:8b-instruct-q4_K_M
        # OLLAMA_HOST can override default local endpoint.
        base = os.environ.get("OLLAMA_HOST", "http://127.0.0.1:11434").rstrip("/")
        url = f"{base}/api/chat"
        # Local models can be slow on first run / long PDFs. Allow override via env var.
        # Defaults to 30 minutes.
        try:
            timeout_sec = int(os.environ.get("OLLAMA_TIMEOUT_SEC", "1800"))
        except Exception:
            timeout_sec = 1800
        payload = {
            "model": model,
            "stream": False,
            "options": {"temperature": temperature},
            "messages": [
                {"role": "system", "content": system_instructions},
                {"role": "user", "content": user_prompt},
            ],
        }
        r = requests.post(url, json=payload, timeout=timeout_sec)
        r.raise_for_status()
        data = r.json()
        msg = data.get("message") or {}
        return msg.get("content", "") or ""

    raise ValueError(f"Unsupported llm.provider: {provider}")

