"""
Exporta `replay_buffer` (SQLite) → `neural_training_lake.parquet` para o ciclo
`forjar_memoria_recente` (Experience Replay 1H nas redes RESERVA).

Cada linha: `features` = lista (10, dim) alinhada ao LSTM; `target` ∈ [0,1] para SNIPER.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# Mesma lógica que AIMixin._neural_feature_dim()
def neural_feature_dim_from_cfg(cfg: Optional[Dict[str, Any]]) -> int:
    c = cfg
    if c is None:
        try:
            from config import DEFAULT_CFG

            c = DEFAULT_CFG
        except Exception:
            c = {}
    if c.get("use_mtf_neural", False) and c.get("use_65d_layout", False):
        return 65
    if c.get("use_mtf_neural", False):
        return 58
    if c.get("use_institutional_microstructure", False):
        return 32
    return 26


def _parse_state(state_raw: Any) -> Optional[List]:
    if state_raw is None:
        return None
    if isinstance(state_raw, bytes):
        state_raw = state_raw.decode("utf-8", errors="ignore")
    try:
        state_arr = json.loads(state_raw)
    except (json.JSONDecodeError, TypeError):
        return None
    return state_arr if isinstance(state_arr, list) else None


def _target_for_row(agent_id: str, action: Any, modo_lat: str) -> Optional[float]:
    """Alvo [0,1] compatível com `forjar_memoria_recente` (sniper BCE + lateral via y>0.5)."""
    try:
        act_i = int(action) if action is not None else 0
    except (TypeError, ValueError):
        return None
    if agent_id in ("SNIPER_V90 FINAL", "ARENA_SNIPER_RESERVA"):
        if act_i in (0, 1):
            return float(act_i)
        return 1.0 if act_i >= 1 else 0.0
    if agent_id == "ARENA_LATERAL_RESERVA":
        if modo_lat == "tanh":
            return 1.0 if act_i >= 1 else 0.0
        return float(act_i)
    return None


def export_replay_buffer_to_neural_lake_parquet(
    db_path: str,
    out_path: str,
    cfg: Optional[Dict[str, Any]] = None,
    max_rows: int = 50_000,
) -> int:
    """
    Lê experiências válidas do SQLite e grava Parquet com colunas `features`, `target`.

    Retorna o número de linhas exportadas (0 se nada a gravar).
    """
    try:
        import pandas as pd
    except ImportError as e:
        logger.error("pandas/pyarrow necessários: %s", e)
        return 0

    if not db_path or not os.path.isfile(db_path):
        logger.warning("SQLite inexistente: %s", db_path)
        return 0

    dim_exp = neural_feature_dim_from_cfg(cfg)
    # Espelho AIMixin._lateral_neural_mode()
    modo_lat = (
        "tanh" if cfg and cfg.get("use_reinforcement_lateral_training") else "sigmoid"
    )

    agents = (
        "SNIPER_V90 FINAL",
        "ARENA_SNIPER_RESERVA",
        "ARENA_LATERAL_RESERVA",
    )

    sql = f"""
        SELECT agent_id, state, action, reward
        FROM replay_buffer
        WHERE state IS NOT NULL
          AND action IS NOT NULL
          AND reward IS NOT NULL
          AND agent_id IN ({",".join("?" * len(agents))})
        ORDER BY id DESC
        LIMIT ?
    """

    try:
        with sqlite3.connect(db_path, timeout=15) as conn:
            rows = conn.execute(sql, (*agents, int(max_rows))).fetchall()
    except sqlite3.Error as e:
        logger.exception("Erro ao ler replay_buffer: %s", e)
        return 0

    rows.reverse()

    feats_out: List[List[List[float]]] = []
    targets_out: List[float] = []

    for agent_id, state_raw, action, _reward in rows:
        state_arr = _parse_state(state_raw)
        if not state_arr:
            continue
        if (
            len(state_arr) != 10
            or not isinstance(state_arr[0], list)
            or len(state_arr[0]) != dim_exp
        ):
            continue
        tgt = _target_for_row(str(agent_id), action, modo_lat)
        if tgt is None:
            continue
        try:
            clean: List[List[float]] = []
            for t in range(10):
                row_t = state_arr[t]
                clean.append([float(x) for x in row_t])
        except (TypeError, ValueError):
            continue
        feats_out.append(clean)
        targets_out.append(float(tgt))

    n = len(feats_out)
    if n == 0:
        return 0

    os.makedirs(os.path.dirname(os.path.abspath(out_path)) or ".", exist_ok=True)

    df = pd.DataFrame({"features": feats_out, "target": targets_out})
    try:
        df.to_parquet(
            out_path,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )
    except Exception:
        logger.exception("Falha ao gravar %s", out_path)
        return 0

    logger.info(
        "neural_training_lake.parquet: %s linhas (dim=%s) → %s",
        n,
        dim_exp,
        out_path,
    )
    return n


def try_build_lake_from_replay(
    db_path: Optional[str],
    workspace_raiz: Optional[str],
    cfg: Optional[Dict[str, Any]],
    max_rows: int = 50_000,
) -> Tuple[int, Optional[str]]:
    """
    Caminho de saída padrão: ``<workspace>/lhn_datalake/neural_training_lake.parquet``.

    Retorna (n_linhas, caminho) ou (0, None) se não gerou ficheiro.
    """
    if not workspace_raiz or not db_path:
        return 0, None
    out = os.path.join(workspace_raiz, "lhn_datalake", "neural_training_lake.parquet")
    n = export_replay_buffer_to_neural_lake_parquet(
        db_path, out, cfg=cfg, max_rows=max_rows
    )
    if n <= 0:
        return 0, None
    return n, out
