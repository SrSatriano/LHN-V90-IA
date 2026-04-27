"""
lhn_indicators — tradução quantitativa (Pine-style → Pandas/NumPy vetorizado).

Convenção de entrada: pd.DataFrame com colunas em minúsculas
    open, high, low, close, volume

Saídas: pd.Series ou pd.DataFrame numéricos (categorias / distâncias / flags).
Sem dependência de Keras nem de ai_mixin.
"""

from __future__ import annotations

from typing import Tuple

import numpy as np
import pandas as pd

REQUIRED_OHLCV: Tuple[str, ...] = ("open", "high", "low", "close", "volume")


def assert_ohlcv(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_OHLCV if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame em falta colunas OHLCV: {missing}")


def _rsi_wilder(close: pd.Series, length: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1.0 / float(length), min_periods=length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / float(length), min_periods=length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    return (100.0 - (100.0 / (1.0 + rs))).fillna(50.0)


def _atr_wilder(df: pd.DataFrame, length: int) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat(
        [
            (h - l).abs(),
            (h - prev_c).abs(),
            (l - prev_c).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1.0 / float(length), min_periods=length, adjust=False).mean()


def _mfi_wilder(df: pd.DataFrame, length: int) -> pd.Series:
    """Money Flow Index (Wilder-style), vetorizado."""
    assert_ohlcv(df)
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    raw_mf = tp * pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
    tp_diff = tp.diff()
    pos_flow = raw_mf.where(tp_diff > 0.0, 0.0)
    neg_flow = raw_mf.where(tp_diff < 0.0, 0.0)
    pos_sum = pos_flow.rolling(length, min_periods=length).sum()
    neg_sum = neg_flow.rolling(length, min_periods=length).sum()
    mfr = pos_sum / neg_sum.replace(0.0, np.nan)
    return (100.0 - (100.0 / (1.0 + mfr))).clip(0.0, 100.0).fillna(50.0)


def mfi_wilder(df: pd.DataFrame, length: int = 14) -> pd.Series:
    """MFI 0–100 (Wilder-style); API pública para `mfi_raw` no vetor neural."""
    return _mfi_wilder(df, int(length))


def tabajara_categorical(
    df: pd.DataFrame,
    fast: int = 8,
    slow: int = 20,
    neutral_band_atr: float = 0.08,
    atr_len: int = 14,
) -> pd.Series:
    """
    Tabajara 5.1 (ribbon MA8 / MA20) → série categórica:
        1  — tendência de alta (verde)
        -1 — tendência de baixa (vermelho)
        0  — zona neutra / transição (amarelo)

    Neutro quando |MA8−MA20| < neutral_band_atr * ATR (faixa anti-chiado).
    """
    assert_ohlcv(df)
    c = pd.to_numeric(df["close"], errors="coerce")
    ma_fast = c.rolling(fast, min_periods=fast).mean()
    ma_slow = c.rolling(slow, min_periods=slow).mean()
    atr = _atr_wilder(df, atr_len)
    sep = (ma_fast - ma_slow).abs()
    neutral = sep < (atr * float(neutral_band_atr))
    trend = np.sign(ma_fast - ma_slow)
    out = np.where(neutral, 0.0, trend)
    return pd.Series(out, index=df.index, dtype="float64").replace(
        {np.nan: 0.0}
    ).astype("int64")


def alphatrend_features(
    df: pd.DataFrame,
    atr_len: int = 14,
    rsi_len: int = 14,
    mfi_len: int = 14,
    mult: float = 1.0,
    hl2_col: bool = True,
) -> pd.DataFrame:
    """
    AlphaTrend (proxy institucional): combina RSI, MFI e ATR num gatilho vetorizado.

    Retorna DataFrame com:
        alphatrend_line — linha de referência (HL2 ± mult*ATR conforme regime RSI/MFI)
        distance_pct    — (close − line) / close * 100
        cross_signal      — 1 compra (cruzamento de baixo para cima), −1 venda, 0 caso contrário

    Nota: versão linearizada (sem estado recursivo tipo Pine completo); adequada a
    features de ranking e distância, com cruzamentos explícitos close vs linha.
    """
    assert_ohlcv(df)
    c = pd.to_numeric(df["close"], errors="coerce")
    h = pd.to_numeric(df["high"], errors="coerce")
    l = pd.to_numeric(df["low"], errors="coerce")
    hl2 = (h + l) / 2.0 if hl2_col else c
    atr = _atr_wilder(df, atr_len)
    rsi = _rsi_wilder(c, rsi_len)
    mfi = _mfi_wilder(df, mfi_len)
    bull_core = (rsi >= 50.0) & (mfi >= 50.0)
    bear_core = (rsi < 50.0) & (mfi < 50.0)
    line_bull = hl2 - float(mult) * atr
    line_bear = hl2 + float(mult) * atr
    line_mixed = hl2
    line = np.where(bull_core, line_bull, np.where(bear_core, line_bear, line_mixed))
    line_s = pd.Series(line, index=df.index, dtype="float64")
    dist_pct = ((c - line_s) / c.replace(0.0, np.nan)) * 100.0
    dist_pct = dist_pct.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    prev_diff = (c.shift(1) - line_s.shift(1))
    curr_diff = c - line_s
    buy_x = (prev_diff <= 0.0) & (curr_diff > 0.0)
    sell_x = (prev_diff >= 0.0) & (curr_diff < 0.0)
    cross = np.where(buy_x, 1, np.where(sell_x, -1, 0))
    return pd.DataFrame(
        {
            "alphatrend_line": line_s,
            "distance_pct": dist_pct,
            "cross_signal": cross.astype("int64"),
        },
        index=df.index,
    )


def cm_macd_histogram_4color(
    df: pd.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> pd.Series:
    """
    MACD + Signal; estado do histograma em 4 categorias (momentum + sinal):

        2  — histograma > 0 e a subir (verde forte / expansão positiva)
        1  — histograma > 0 e a descer (verde fraco / contração positiva)
        -1 — histograma < 0 e a subir (vermelho fraco / contração negativa)
        -2 — histograma < 0 e a cair (vermelho forte / expansão negativa)
    """
    assert_ohlcv(df)
    c = pd.to_numeric(df["close"], errors="coerce")
    ema_f = c.ewm(span=fast, adjust=False, min_periods=fast).mean()
    ema_s = c.ewm(span=slow, adjust=False, min_periods=slow).mean()
    macd = ema_f - ema_s
    sig = macd.ewm(span=signal, adjust=False, min_periods=signal).mean()
    hist = macd - sig
    dh = hist.diff()
    pos = hist > 0.0
    neg = hist < 0.0
    state = np.where(
        pos & (dh > 0.0),
        2,
        np.where(
            pos & (dh <= 0.0),
            1,
            np.where(
                neg & (dh < 0.0),
                -2,
                np.where(neg & (dh >= 0.0), -1, 0),
            ),
        ),
    )
    return pd.Series(state, index=df.index, dtype="int64")


def detect_pin_bar(
    df: pd.DataFrame,
    shadow_body_ratio: float = 2.0,
    min_body_pct_range: float = 1e-6,
) -> pd.Series:
    """
    Pin bar por proporção sombra / corpo (sem loops por linha):

        1  — pin bullish (sombra inferior dominante vs corpo e vs sombra superior)
        -1 — pin bearish (sombra superior dominante)
        0  — não é pin segundo o critério

    Corpo mínimo relativo ao range total para evitar divisão instável em doji.
    """
    assert_ohlcv(df)
    o = pd.to_numeric(df["open"], errors="coerce")
    h = pd.to_numeric(df["high"], errors="coerce")
    l = pd.to_numeric(df["low"], errors="coerce")
    c = pd.to_numeric(df["close"], errors="coerce")
    body = (c - o).abs()
    rng = (h - l).abs().replace(0.0, np.nan)
    upper = h - pd.concat([o, c], axis=1).max(axis=1)
    lower = pd.concat([o, c], axis=1).min(axis=1) - l
    body_ok = body >= (rng * float(min_body_pct_range))
    bull = (lower >= float(shadow_body_ratio) * body) & (lower > upper) & body_ok
    bear = (upper >= float(shadow_body_ratio) * body) & (upper > lower) & body_ok
    out = np.where(bull, 1, np.where(bear, -1, 0))
    return pd.Series(out, index=df.index, dtype="int64")


def sr_channel_position(
    df: pd.DataFrame,
    period: int = 20,
) -> pd.Series:
    """
    Posição normalizada dentro de canais tipo Donchian (proxy S/R vetorizado):

        0.0 — junto ao suporte (mínimo rolling)
        1.0 — junto à resistência (máximo rolling)
        NaN nas primeiras barras sem janela completa.

    Útil como feature contínua complementar a pin bars (sem redesenhar pivôs manuais).
    """
    assert_ohlcv(df)
    h = pd.to_numeric(df["high"], errors="coerce")
    l = pd.to_numeric(df["low"], errors="coerce")
    c = pd.to_numeric(df["close"], errors="coerce")
    up = h.rolling(period, min_periods=period).max()
    lo = l.rolling(period, min_periods=period).min()
    width = (up - lo).replace(0.0, np.nan)
    pos = (c - lo) / width
    return pos.clip(0.0, 1.0)


def pinbar_and_sr_bundle(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Pacote único: pin bar categórico + posição no canal (S/R Donchian), ambos vetorizados.

    kwargs repassados a detect_pin_bar (ex.: shadow_body_ratio).
    """
    pin = detect_pin_bar(df, **{k: v for k, v in kwargs.items() if k in ("shadow_body_ratio", "min_body_pct_range")})
    period = int(kwargs.get("sr_period", 20))
    sr = sr_channel_position(df, period=period)
    return pd.DataFrame({"pin_bar": pin, "sr_channel_pos": sr}, index=df.index)
