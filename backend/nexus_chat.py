"""
Motor conversacional NEXUS (Qwen2.5 Instruct) — pode rodar:
- embutido: importado por server.py (carregamento lazy na 1ª mensagem);
- sidecar: `python nexus_chat.py` (porta NEXUS_SIDECAR_PORT, default 9001).
"""

from __future__ import annotations

import asyncio
import os
import warnings
from typing import Any

warnings.filterwarnings("ignore")

LLM_READY = False
LIGHT_MODE = False
chatbot = None
_load_attempted = False


def _eh_falha_memoria(exc: BaseException) -> bool:
    if isinstance(exc, MemoryError):
        return True
    msg = str(exc).lower()
    if "out of memory" in msg or "outofmemoryerror" in msg:
        return True
    if "cuda" in msg and ("memory" in msg or "alloc" in msg):
        return True
    try:
        import torch

        oom_cls = getattr(torch.cuda, "OutOfMemoryError", None)
        if oom_cls is not None and isinstance(exc, oom_cls):
            return True
    except Exception:
        pass
    return False


def _resposta_modo_leve(mensagem: str, contexto: dict[str, Any]) -> str:
    """Respostas rápidas quando o modelo Qwen não está em RAM/VRAM."""
    t = (mensagem or "").strip().lower()
    ops = contexto.get("operacoes", []) or []
    nlp = float(contexto.get("nlp_score", 5.0) or 5.0)
    if "status" in t or "como você" in t or "como voce" in t or "como estamos" in t:
        return (
            f"NEXUS [modo leve]: sistemas nominalmente estáveis. NLP {nlp:.1f}/10. "
            f"Posições abertas: {len(ops)}."
        )
    if "mercado" in t:
        return (
            f"NEXUS [modo leve]: referência de sentimento {nlp:.1f}/10. "
            "Modelo neural local indisponível (memória) — respostas limitadas."
        )
    if any(x in t for x in ("compre", "venda", "long", "short")):
        return (
            "NEXUS [modo leve]: decisões de execução ficam com o motor Sniper no backend. "
            "Aqui apenas contexto tático resumido."
        )
    return (
        f"NEXUS [modo leve]: LLM local não carregado. "
        f"Contexto: {len(ops)} posição(ões), NLP {nlp:.1f}/10. "
        "Pergunte status ou mercado para respostas rápidas."
    )


def _formatar_operacoes_prompt(operacoes: Any) -> str:
    """Converte operações do frontend em texto objetivo para o system prompt."""
    if not isinstance(operacoes, list) or not operacoes:
        return "Nenhuma operação informada pelo usuário."

    linhas: list[str] = []
    for i, op in enumerate(operacoes[:30], start=1):
        if not isinstance(op, dict):
            continue
        ativo = str(op.get("ativo") or op.get("symbol") or "?").strip()
        lado = str(op.get("tipo") or op.get("side") or op.get("direcao") or "?").strip()
        resultado = str(op.get("resultado") or "").strip()
        origem = str(op.get("origem") or "").strip()
        hora = str(op.get("hora") or "").strip()
        lucro_raw = op.get("lucro", op.get("pnl", op.get("pnl_usd", 0)))
        try:
            lucro = float(lucro_raw or 0)
        except (TypeError, ValueError):
            lucro = 0.0
        partes = [
            f"{i}. Ativo={ativo}",
            f"Lado={lado}",
            f"PnL={lucro:+.2f} USDT",
        ]
        if resultado:
            partes.append(f"Resultado={resultado}")
        if hora:
            partes.append(f"Hora={hora}")
        if origem:
            partes.append(f"Origem={origem}")
        linhas.append(" | ".join(partes))

    return "\n".join(linhas) if linhas else "Nenhuma operação válida no contexto."


def _ensure_pipeline() -> None:
    """Carrega Qwen na primeira chamada (não bloqueia import do server)."""
    global chatbot, LLM_READY, LIGHT_MODE, _load_attempted
    if _load_attempted:
        return
    _load_attempted = True
    if os.environ.get("LHN_NEXUS_DISABLE", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        LIGHT_MODE = True
        LLM_READY = False
        print("[AVISO] Nexus LLM desativado (LHN_NEXUS_DISABLE).")
        return

    print(
        "[+] Nexus: carregando motor Qwen (lazy). Primeira mensagem pode demorar "
        "(download/cache HF)."
    )
    try:
        import torch
        from transformers import pipeline

        model = (
            os.environ.get("LHN_NEXUS_MODEL", "Qwen/Qwen2.5-1.5B-Instruct") or ""
        ).strip()
        chatbot = pipeline(
            "text-generation",
            model=model,
            device_map="auto",
            torch_dtype=torch.float16,
        )
        _normalize_generation_config()
        LLM_READY = True
        LIGHT_MODE = False
        print(f"[OK] Nexus LLM online ({model}).")
    except Exception as e:
        chatbot = None
        LLM_READY = False
        LIGHT_MODE = True
        mem = _eh_falha_memoria(e)
        tag = "memória/GPU" if mem else "dependência ou hardware"
        print(
            f"[AVISO] Nexus em modo leve — LLM indisponível ({tag}): "
            f"{type(e).__name__}: {e}"
        )


def _max_new_tokens() -> int:
    raw = os.environ.get("LHN_NEXUS_MAX_NEW_TOKENS", "4096").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 4096
    return max(64, min(n, 10_000))


def _normalize_generation_config() -> None:
    """
    Remove `max_length` legado (20) do generation_config para não conflitar
    com `max_new_tokens` e evitar spam de warning do Transformers.
    """
    global chatbot
    try:
        model = getattr(chatbot, "model", None)
        if model is None:
            return
        gen_cfg = getattr(model, "generation_config", None)
        if gen_cfg is None:
            return
        if getattr(gen_cfg, "max_length", None) is not None:
            gen_cfg.max_length = None
    except Exception:
        # Melhor não falhar o boot por causa de metadado de geração.
        pass


def generate_nexus_chat_sync(mensagem: str, contexto: dict[str, Any] | None) -> str:
    """
    Gera resposta do NEXUS (Qwen Instruct) ou modo leve.
    `contexto` pode incluir: operacoes, nlp_score, contexto_extra (texto do backend).
    """
    contexto = dict(contexto or {})
    _ensure_pipeline()

    if LIGHT_MODE or not LLM_READY or chatbot is None:
        return _resposta_modo_leve(mensagem, contexto)

    ops = contexto.get("operacoes", [])
    ops_texto = _formatar_operacoes_prompt(ops)
    nlp = float(contexto.get("nlp_score", 5.0) or 5.0)
    extra = (contexto.get("contexto_extra") or "").strip()

    contexto_str = f"Sentimento do Mercado (NLP): {nlp}/10.0. "
    if ops:
        partes = []
        for o in ops:
            if isinstance(o, dict):
                partes.append(f"{o.get('tipo', '?')} em {o.get('symbol', '?')}")
        contexto_str += (
            f"Posições abertas ({len(ops)}): {', '.join(partes)}. "
            if partes
            else f"Posições abertas: {len(ops)}. "
        )
    else:
        contexto_str += "Nenhuma posição aberta. "
    if extra:
        contexto_str += extra

    messages = [
        {
            "role": "system",
            "content": (
                "Você é o NEXUS, a IA tática da plataforma LHN Sovereign V90. "
                "Responda sempre em português. Seja direto, frio, militar e analítico. "
                "Baseie-se APENAS nos dados fornecidos. Não invente operações, preços, PnL ou resultados. "
                "Se faltar dado, diga explicitamente que está ausente. "
                "Aqui estão as últimas operações do usuário:\n"
                + ops_texto
                + "\n\nAgora responda à pergunta do usuário usando somente os dados acima e o contexto de sistema. "
                "Contexto adicional: " + contexto_str
            ),
        },
        {"role": "user", "content": mensagem},
    ]

    try:
        resultado = chatbot(
            messages,
            max_new_tokens=_max_new_tokens(),
            temperature=0.4,
            top_k=40,
            top_p=0.90,
            repetition_penalty=1.05,
        )
        resposta_limpa = resultado[0]["generated_text"][-1]["content"].strip()
        return resposta_limpa
    except Exception as e:
        if _eh_falha_memoria(e):
            return _resposta_modo_leve(mensagem, contexto)
        return f"[Erro no Córtex LLM]: {str(e)}"


# --- Sidecar HTTP opcional (mesmo contrato que o painel esperava na porta 8001/9001) ---

from pydantic import BaseModel, Field  # noqa: E402


class ChatRequest(BaseModel):
    mensagem: str
    contexto: dict = Field(default_factory=dict)


def create_sidecar_app():
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware

    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.post("/api/chat")
    async def chat_endpoint(req: ChatRequest):
        # Geração bloqueante — não trava o event loop do Uvicorn
        r = await asyncio.to_thread(
            generate_nexus_chat_sync, req.mensagem, req.contexto
        )
        return {"resposta": r}

    return app


if __name__ == "__main__":
    import uvicorn

    if os.environ.get("LHN_NEXUS_EAGER", "").strip().lower() in ("1", "true", "yes"):
        print("[+] NEXUS: pré-carregamento do modelo (LHN_NEXUS_EAGER)...")
        _ensure_pipeline()

    port = int(os.environ.get("NEXUS_SIDECAR_PORT", "9001"))
    uvicorn.run(create_sidecar_app(), host="0.0.0.0", port=port)
