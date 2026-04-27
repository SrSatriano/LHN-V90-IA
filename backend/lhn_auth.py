"""Autenticação em nível de aplicação (LHN_API_KEY) para HTTP e WebSocket."""

import os
from typing import Mapping, Optional, Tuple

from fastapi import Header, HTTPException, Query, status


def expected_api_key() -> str:
    return (os.environ.get("LHN_API_KEY") or "").strip()


async def verify_api_key(
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
    authorization: Optional[str] = Header(None),
    token: Optional[str] = Query(None),
) -> None:
    """Exige X-API-Key, Bearer ou ?token= (sendBeacon) quando LHN_API_KEY está definida.
    Se LHN_API_KEY estiver vazia, não requer autenticação (modo teste local)."""
    exp = expected_api_key()
    if not exp:
        return
    got = (x_api_key or "").strip()
    if not got and authorization:
        auth_l = authorization.strip()
        if auth_l.lower().startswith("bearer "):
            got = auth_l[7:].strip()
    if not got and token is not None:
        got = (token or "").strip()
    if got != exp:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Chave de API inválida ou ausente.",
        )


def _parse_ws_protocol_list(raw: Optional[str]) -> list[str]:
    if not raw:
        return []
    return [x.strip() for x in str(raw).split(",") if x.strip()]


def websocket_subprotocol_token_ok(
    headers: Mapping[str, str]
) -> Tuple[bool, Optional[str]]:
    """
    Autenticação WebSocket via Sec-WebSocket-Protocol: o cliente deve incluir o mesmo
    valor que LHN_API_KEY como um dos subprotocolos negociados (ex.: ['lhn.auth.v1', key]).
    """
    exp = expected_api_key()
    if not exp:
        # Modo teste: aceita handshake sem negociar token no subprotocolo
        return True, None
    raw = headers.get("sec-websocket-protocol") or headers.get("Sec-WebSocket-Protocol")
    requested = _parse_ws_protocol_list(raw)
    if exp in requested:
        return True, exp
    return False, None
