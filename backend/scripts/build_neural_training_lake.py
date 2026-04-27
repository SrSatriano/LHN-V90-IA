#!/usr/bin/env python3
"""
Gera ou atualiza ``lhn_datalake/neural_training_lake.parquet`` a partir de
``Dados/LHN_DEEP_MEMORY.sqlite`` (tabela ``replay_buffer``).

Uso (na pasta ``backend``):
  python scripts/build_neural_training_lake.py
  python scripts/build_neural_training_lake.py --workspace "D:/caminho/Workspace_LHN"

Variável opcional: ``LHN_WORKSPACE`` (sobrescreve o default).
"""
from __future__ import annotations

import argparse
import json
import os
import sys

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

from neural_lake_export import (  # noqa: E402
    export_replay_buffer_to_neural_lake_parquet, neural_feature_dim_from_cfg)


def _default_workspace() -> str:
    env = os.environ.get("LHN_WORKSPACE", "").strip()
    if env:
        return os.path.abspath(env)
    return os.path.abspath(os.path.join(BACKEND_DIR, "..", "Workspace_LHN"))


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Exporta replay_buffer SQLite → neural_training_lake.parquet"
    )
    ap.add_argument(
        "--workspace",
        default=_default_workspace(),
        help="Raiz do workspace (contém Dados/ e lhn_datalake/)",
    )
    ap.add_argument(
        "--max-rows",
        type=int,
        default=50_000,
        help="Máximo de linhas lidas do replay (mais recentes)",
    )
    args = ap.parse_args()
    ws = os.path.abspath(args.workspace)
    db_path = os.path.join(ws, "Dados", "LHN_DEEP_MEMORY.sqlite")
    out_path = os.path.join(ws, "lhn_datalake", "neural_training_lake.parquet")
    cfg_path = os.path.join(ws, "Dados", "LHN_CONFIG_MASTER.json")

    cfg = None
    if os.path.isfile(cfg_path):
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except OSError as e:
            print(f"Aviso: não foi possível ler {cfg_path}: {e}", file=sys.stderr)

    dim = neural_feature_dim_from_cfg(cfg)
    print(f"workspace={ws}")
    print(f"dimensão de features (cfg)={dim}")
    print(f"sqlite={db_path}")
    print(f"saída={out_path}")

    if not os.path.isfile(db_path):
        print(
            "Erro: SQLite não encontrado. Confirme o caminho do workspace.",
            file=sys.stderr,
        )
        return 1

    n = export_replay_buffer_to_neural_lake_parquet(
        db_path, out_path, cfg=cfg, max_rows=args.max_rows
    )
    if n <= 0:
        print(
            "Nenhuma linha exportada (sem experiências com reward + formato 10×dim, "
            "ou agentes SNIPER/LATERAL_RESERVA)."
        )
        return 2

    print(f"OK: {n} amostras gravadas em {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
