"""
Gatilho unificado de abertura (Simulação + Real): regras da exchange + caixa.

- Margem isolada (Isolated) é aplicada na execução real (engine); aqui só dimensionamos
  e validamos coerência com min_qty / min_notional e saldo disponível.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from typing import Any, Dict, Optional, Tuple


@dataclass
class OrderOpenValidationResult:
    ok: bool
    margem_usada: float
    qty: float
    notional: float
    step_size: float
    tick_size: float
    min_qty: float
    min_notional: float
    motivo: str


def _parse_filter_float(
    filters: list, filter_type: str, key: str, default: float
) -> float:
    for f in filters or []:
        if f.get("filterType") == filter_type and f.get(key) is not None:
            try:
                return float(f[key])
            except (TypeError, ValueError):
                pass
    return default


def extract_linear_symbol_filters(
    symbol: str, exchange_info: Optional[Dict[str, Any]]
) -> Tuple[float, float, float, float]:
    """
    Lê LOT_SIZE / PRICE_FILTER / MIN_QTY / MIN_NOTIONAL do cache (formato Binance-compat).
    Fallback: step 0.001, tick 0.01, min_qty 0.001, min_notional 5 USDT (Bybit linear comum).
    """
    step_size = 0.001
    tick_size = 0.01
    min_qty = 0.001
    min_notional = 5.0
    if not exchange_info or not isinstance(exchange_info, dict):
        return step_size, tick_size, min_qty, min_notional
    for s in exchange_info.get("symbols") or []:
        if s.get("symbol") != symbol:
            continue
        filters = s.get("filters") or []
        step_size = _parse_filter_float(filters, "LOT_SIZE", "stepSize", step_size)
        tick_size = _parse_filter_float(filters, "PRICE_FILTER", "tickSize", tick_size)
        min_qty = _parse_filter_float(filters, "MIN_QTY", "minQty", min_qty)
        min_notional = _parse_filter_float(
            filters, "MIN_NOTIONAL", "minNotional", min_notional
        )
        # Se MIN_QTY não veio explícito, LOT_SIZE.stepSize serve como floor de incremento
        if min_qty <= 0:
            min_qty = step_size if step_size > 0 else 0.001
        break
    return step_size, tick_size, min_qty, min_notional


def round_qty_to_step(qty_bruta: float, step_size: float) -> Tuple[float, int]:
    """Quantidade ao step da exchange (floor), espelha engine_mixin.executar_ordem_real."""
    if step_size <= 0:
        return max(0.0, float(qty_bruta)), 8
    dec_qty = Decimal(str(qty_bruta))
    dec_step = Decimal(str(step_size))
    qty_calc = float((dec_qty // dec_step) * dec_step)
    precision = 0
    if step_size < 1:
        precision = len(f"{step_size:.8f}".rstrip("0").split(".")[1])
    if precision == 0:
        qty_out = float(int(qty_calc))
    else:
        qty_out = round(qty_calc, precision)
    return max(0.0, float(qty_out)), precision


def validate_linear_open_order(
    *,
    symbol: str,
    margem: float,
    leverage: int,
    mark_price: float,
    exchange_info: Optional[Dict[str, Any]],
    available_balance_usdt: float,
    max_order_usd: float = 500_000.0,
    margin_buffer_pct: float = 0.002,
) -> OrderOpenValidationResult:
    """
    Dupla verificação obrigatória antes de autorizar envio:

    1) Regra da exchange: min_qty e min_notional (valor do contrato em USDT) após arredondar qty.
    2) Regra de caixa: margem inicial isolada (nocional / alavancagem) <= saldo disponível.

    Opcionalmente aumenta a margem para cumprir mínimos, se o caixa permitir.
    """
    sym = str(symbol or "").strip().upper()
    if not sym:
        return OrderOpenValidationResult(
            False,
            0.0,
            0.0,
            0.0,
            0.001,
            0.01,
            0.001,
            5.0,
            "símbolo inválido",
        )
    lev = max(1, int(leverage))
    price = float(mark_price)
    if price <= 0:
        return OrderOpenValidationResult(
            False,
            0.0,
            0.0,
            0.0,
            0.001,
            0.01,
            0.001,
            5.0,
            "preço de marcação inválido",
        )

    step_size, tick_size, min_qty, min_notional = extract_linear_symbol_filters(
        sym, exchange_info
    )

    m = float(margem)
    
    # [CORREÇÃO 1: PISO RÍGIDO (Hard Floor) PARA KELLY DINÂMICO]
    # Força a margem a ser, no mínimo, US$ 5.00. Contorna o min_notional 
    # da exchange para bancas baixas, sem desperdiçar tiros da IA.
    m = max(5.00, m)

    if m <= 0:
        return OrderOpenValidationResult(
            False,
            0.0,
            0.0,
            0.0,
            step_size,
            tick_size,
            min_qty,
            min_notional,
            "margem calculada <= 0",
        )

    buf = max(0.0, float(margin_buffer_pct))

    def _margin_for_constraints() -> float:
        """Margem mínima teórica para respeitar min_qty e min_notional (linear USDT)."""
        need_q = (min_qty * price) / float(lev)
        need_n = float(min_notional) / float(lev)
        return max(need_q, need_n) * (1.0 + buf)

    # Ajuste fino: até 5 passos para subir margem até caber nos mínimos
    for _ in range(6):
        if m > available_balance_usdt + 1e-9:
            return OrderOpenValidationResult(
                False,
                m,
                0.0,
                0.0,
                step_size,
                tick_size,
                min_qty,
                min_notional,
                f"margem US${m:.4f} > saldo disponível US${available_balance_usdt:.4f}",
            )

        qty_bruta = (m * float(lev)) / price
        qf, _prec = round_qty_to_step(qty_bruta, step_size)
        notional = float(qf) * price

        if float(qf) <= 0:
            need = _margin_for_constraints()
            if need > available_balance_usdt:
                return OrderOpenValidationResult(
                    False,
                    m,
                    0.0,
                    0.0,
                    step_size,
                    tick_size,
                    min_qty,
                    min_notional,
                    "quantidade zero após step; insuficiente para mínimos da exchange com o caixa atual",
                )
            m = min(need, available_balance_usdt)
            continue

        if notional > max_order_usd:
            return OrderOpenValidationResult(
                False,
                m,
                float(qf),
                notional,
                step_size,
                tick_size,
                min_qty,
                min_notional,
                f"notional US${notional:,.2f} excede teto max_order_usd (US${max_order_usd:,.2f})",
            )

        if float(qf) + 1e-12 < float(min_qty) or notional + 1e-9 < float(min_notional):
            need = _margin_for_constraints()
            if need > available_balance_usdt + 1e-9:
                return OrderOpenValidationResult(
                    False,
                    m,
                    float(qf),
                    notional,
                    step_size,
                    tick_size,
                    min_qty,
                    min_notional,
                    f"abaixo dos mínimos da exchange (min_qty≥{min_qty}, min_notional≥{min_notional} USDT); "
                    f"caixa US${available_balance_usdt:.4f} não permite elevar a margem",
                )
            m = min(need, available_balance_usdt)
            continue

        return OrderOpenValidationResult(
            True,
            m,
            float(qf),
            notional,
            step_size,
            tick_size,
            min_qty,
            min_notional,
            "ok",
        )

    return OrderOpenValidationResult(
        False,
        m,
        0.0,
        0.0,
        step_size,
        tick_size,
        min_qty,
        min_notional,
        "falha ao convergir margem para requisitos da exchange",
    )
