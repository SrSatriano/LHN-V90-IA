"""
Gerenciamento de Risco Dinâmico em Camadas (Tiered Risk Management).

A faixa (tier) e os percentuais aplicam-se ao saldo de referência da conta
(equity / wallet USDT — `saldo_atual` no motor), coerente com as travas por
tamanho de conta (US$ 100 / 1.000 / 50.000).
"""

from __future__ import annotations

# Regra global inflexível: máximo de operações simultâneas (independente do saldo).
MAX_OPERACOES_SIMULTANEAS = 4


def obter_limites_risco(saldo: float) -> dict:
    """
    Retorna limites percentuais conforme o saldo de referência (USD).

    Tier 1 — saldo < US$ 100:
        teto por operação: 10% | exposição máxima total (soma das margens): 40%
    Tier 2 — US$ 100 ≤ saldo ≤ US$ 1.000:
        5% | 30%
    Tier 3 — US$ 1.000,01 < saldo ≤ US$ 50.000:
        iguais ao Tier 2 (5% | 30%)
    Tier 4 — saldo > US$ 50.000:
        3% | 20%
    """
    s = max(0.0, float(saldo or 0.0))
    if s < 100.0:
        tier = 1
        pct_op = 0.10
        pct_tot = 0.40
    elif s <= 1000.0:
        tier = 2
        pct_op = 0.05
        pct_tot = 0.30
    elif s <= 50000.0:
        tier = 3
        pct_op = 0.05
        pct_tot = 0.30
    else:
        tier = 4
        pct_op = 0.03
        pct_tot = 0.20
    return {
        "tier": tier,
        "pct_max_por_operacao": pct_op,
        "pct_exposicao_total": pct_tot,
        "saldo_referencia": s,
    }
