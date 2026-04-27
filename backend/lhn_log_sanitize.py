"""Redação de segredos em mensagens de log (API keys, secrets)."""

from __future__ import annotations

import os
import re
from typing import Optional


def _strip_if_present(text: str, secret: Optional[str]) -> str:
    if not secret or len(secret) < 6:
        return text
    return text.replace(secret, "***REDACTED***")


def sanitize_log_message(
    text: str,
    api_key: Optional[str] = None,
    api_secret: Optional[str] = None,
    lhn_api_key: Optional[str] = None,
) -> str:
    """Remove credenciais conhecidas e padrões sensíveis antes de imprimir ou guardar em histórico."""
    t = str(text)
    for s in (api_key, api_secret, lhn_api_key):
        t = _strip_if_present(t, s)
    env_lhn = (os.environ.get("LHN_API_KEY") or "").strip()
    if env_lhn:
        t = _strip_if_present(t, env_lhn)
    for ev in (
        "BYBIT_API_KEY",
        "LHN_BYBIT_API_KEY",
        "BYBIT_API_SECRET",
        "LHN_BYBIT_API_SECRET",
    ):
        v = (os.environ.get(ev) or "").strip()
        if v:
            t = _strip_if_present(t, v)
    # Padrões comuns em payloads de erro (sem números genéricos — evita apagar preços)
    t = re.sub(
        r"(account[_\-]?id|uid|member[_\-]?id)([\"':\s]+)([0-9]{8,})",
        r"\1\2***",
        t,
        flags=re.I,
    )
    return t


def sanitize_exception_message(exc: BaseException) -> str:
    return sanitize_log_message(str(exc))
