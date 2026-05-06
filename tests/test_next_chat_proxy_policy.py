"""Garante que /api/chat não seja tratado por uma rota Next isolada do FastAPI."""

from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]


def test_chat_must_not_be_shadowed_by_next_app_route():
    """
    Um Route Handler em app/api/chat/route.ts tem precedência sobre rewrites e
    contornava verify_api_key + enriquecimento de contexto em server.py, falando
    direto com o sidecar Nexus (sem autenticação HTTP).
    """
    rogue = _REPO / "frontend" / "app" / "api" / "chat" / "route.ts"
    assert not rogue.is_file(), (
        f"Remova {rogue.relative_to(_REPO)}: use o rewrite para FastAPI :9002."
    )


def test_next_config_rewrites_api_to_trading_backend():
    cfg = (_REPO / "frontend" / "next.config.ts").read_text(encoding="utf-8")
    assert "127.0.0.1:9002" in cfg
    assert 'source: "/api/:path*"' in cfg or "source: '/api/:path*'" in cfg
