"""
Canais de suporte/resistência (agrupamento de pivots) — alinhado ao pipeline LHN (ohlc).
Requer scipy (argrelextrema).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.signal import argrelextrema


def _normalize_ohlc_df(df: pd.DataFrame) -> pd.DataFrame | None:
    if df is None or len(df) < 10:
        return None
    d = df.copy()
    colmap = {"h": "high", "l": "low", "c": "close", "o": "open"}
    for a, b in colmap.items():
        if a in d.columns and b not in d.columns:
            d = d.rename(columns={a: b})
    for need in ("high", "low", "close"):
        if need not in d.columns:
            return None
    return d


def calcular_canais_sr(
    df: pd.DataFrame,
    prd: int = 10,
    channel_w_pct: float = 5.0,
    loopback: int = 290,
    min_strength: int = 1,
    max_sr: int = 6,
):
    """
    Agrupa pivots em canais de S/R; detecta rompimento vs último fecho.
    `df` pode usar colunas h/l/c (Bybit) ou high/low/close.
    """
    df_n = _normalize_ohlc_df(df)
    if df_n is None:
        return {"canais": [], "resistencia_rompida": False, "suporte_rompido": False}

    df_calc = df_n.iloc[-loopback:].copy()
    if len(df_calc) < prd * 2:
        return {"canais": [], "resistencia_rompida": False, "suporte_rompido": False}

    highs = df_calc["high"].values
    lows = df_calc["low"].values
    closes = df_calc["close"].values

    ph_idx = argrelextrema(highs, np.greater_equal, order=prd)[0]
    pl_idx = argrelextrema(lows, np.less_equal, order=prd)[0]
    pivots = np.concatenate([highs[ph_idx], lows[pl_idx]])

    if len(pivots) == 0:
        return {"canais": [], "resistencia_rompida": False, "suporte_rompido": False}

    cwidth = (np.max(highs) - np.min(lows)) * (channel_w_pct / 100.0)
    canais_brutos = []

    for p in pivots:
        lo, hi = p, p
        for cpp in pivots:
            wdth = (hi - cpp) if (cpp <= hi) else (cpp - lo)
            if wdth <= cwidth:
                lo, hi = min(lo, cpp), max(hi, cpp)
        if not any(c["hi"] == hi and c["lo"] == lo for c in canais_brutos):
            canais_brutos.append({"hi": hi, "lo": lo, "strength": 0})

    for c in canais_brutos:
        h, l = c["hi"], c["lo"]
        c["strength"] += len([p for p in pivots if l <= p <= h]) * 20
        c["strength"] += np.sum(
            ((highs <= h) & (highs >= l)) | ((lows <= h) & (lows >= l))
        )

    canais_finais = []
    for c in sorted(canais_brutos, key=lambda x: x["strength"], reverse=True):
        if c["strength"] < (min_strength * 20):
            continue
        if not any(
            (cf["lo"] <= c["hi"] <= cf["hi"]) or (cf["lo"] <= c["lo"] <= cf["hi"])
            for cf in canais_finais
        ):
            canais_finais.append(c)
        if len(canais_finais) >= max_sr:
            break

    fechamento_atual, fechamento_anterior = closes[-1], closes[-2]
    not_in_a_channel = not any(
        c["lo"] <= fechamento_atual <= c["hi"] for c in canais_finais
    )
    res_romp, sup_romp = False, False

    if not_in_a_channel:
        for c in canais_finais:
            if fechamento_anterior <= c["hi"] and fechamento_atual > c["hi"]:
                res_romp = True
            if fechamento_anterior >= c["lo"] and fechamento_atual < c["lo"]:
                sup_romp = True

    return {
        "canais": canais_finais,
        "resistencia_rompida": res_romp,
        "suporte_rompido": sup_romp,
    }
