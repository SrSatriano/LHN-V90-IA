import unicodedata


# --- NLP: normalização para matching em títulos RSS (PT-BR com/sem acento + EN) ---
def fold_lexicon_text(s: str) -> str:
    """Minúsculas + remoção de diacríticos — comparação estável com substring."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s).lower())
    return "".join(c for c in s if not unicodedata.combining(c))


# --- PALETA DE CORES TRADINGVIEW (TV DARK PREMIUM) ---
TV_BG_MAIN = "#131722"  # Fundo master escuro
TV_BG_PANEL = "#1E222D"  # Fundo dos painéis e grids
TV_BORDER = "#2A2E39"  # Bordas sutis
TV_TEXT = "#D1D4DC"  # Texto claro
TV_TEXT_MUTED = "#8B98A5"  # Texto secundário (Cinza Azulado)
TV_GREEN = "#089981"  # Verde TV exato
TV_RED = "#F23645"  # Vermelho TV exato
TV_BLUE = "#2962FF"  # Azul TV Institucional
TV_YELLOW = "#F5B041"  # Amarelo suave
TV_PURPLE = "#B39DDB"  # Roxo elegante para destaques

# --- PALETA LIGHT THEME ---
LIGHT_BG_MAIN = "#FFFFFF"
LIGHT_BG_PANEL = "#F0F3FA"
LIGHT_BORDER = "#D1D4DC"
LIGHT_TEXT = "#131722"
LIGHT_TEXT_MUTED = "#787B86"

# SISTEMA DE IDIOMAS V1 REVISÃO FINAL
UI_LANGUAGES = {
    "Português-Brasil": {
        "title": "🦅 LHN SOVEREIGN V1 FINAL BINANCE - INSTITUTIONAL",
        "matriz": "Matriz de Indicadores",
        "ia_gestao": "IA & Gestão Financeira",
        "pref_ui": "Preferências UI",
        "buscar": "▶ Buscar Oportunidades",
        "buscar_wait": "⏳ Buscando...",
        "cancelar": "⏹ Cancelar Busca",
        "history": "Histórico",
        "logs": "Logs do Sistema",
        "errors": "Erros Críticos",
    },
    "Inglês": {
        "title": "🦅 LHN SOVEREIGN V1 FINAL BINANCE - INSTITUTIONAL",
        "matriz": "Indicator Matrix",
        "ia_gestao": "AI & Financial Mgmt",
        "pref_ui": "UI Preferences",
        "buscar": "▶ Search Opportunities",
        "buscar_wait": "⏳ Searching...",
        "cancelar": "⏹ Cancel Search",
        "history": "History",
        "logs": "System Logs",
        "errors": "Critical Errors",
    },
    "Espanhol": {
        "title": "🦅 LHN SOVEREIGN V1 FINAL BINANCE - INSTITUTIONAL",
        "matriz": "Matriz de Indicadores",
        "ia_gestao": "IA y Gestión Financiera",
        "pref_ui": "Preferencias de UI",
        "buscar": "▶ Buscar Oportunidades",
        "buscar_wait": "⏳ Buscando...",
        "cancelar": "⏹ Cancelar Búsqueda",
        "history": "Historial",
        "logs": "Registros del Sistema",
        "errors": "Errores Críticos",
    },
    "Francês": {
        "title": "🦅 LHN SOVEREIGN V1 FINAL BINANCE - INSTITUTIONAL",
        "matriz": "Matrice d'Indicateurs",
        "ia_gestao": "IA & Gestion Financière",
        "pref_ui": "Préférences UI",
        "buscar": "▶ Chercher Opportunités",
        "buscar_wait": "⏳ Recherche...",
        "cancelar": "⏹ Annuler Recherche",
        "history": "Historique",
        "logs": "Journaux Système",
        "errors": "Erreurs Critiques",
    },
}

# [M9] Dicionários NLP Expandidos — Cobertura Institucional Completa (PT-BR + EN)
DICIONARIO_PANICO = [
    # PT-BR
    "queda",
    "tensão",
    "guerra",
    "hacker",
    "roubo",
    "proibição",
    "processo",
    "derretimento",
    "golpe",
    "investigação",
    "colapso",
    "fraude",
    "terror",
    "pânico",
    "sangria",
    "falhão",
    "crise",
    "recessão",
    "inflação",
    "desvalorização",
    "fuga",
    "venda massiva",
    "capitulação",
    "medo",
    "insolvência",
    "inadimplência",
    "sanção",
    # EN financeiro/crypto
    "crash",
    "dump",
    "liquidation",
    "ban",
    "hack",
    "exploit",
    "vulnerability",
    "sec",
    "regulation",
    "bankruptcy",
    "insolvency",
    "sell-off",
    "selloff",
    "bear market",
    "recession",
    "collapse",
    "delisting",
    "rug",
    "scam",
    "embargo",
    "fine",
    "lawsuit",
    "investigation",
    "depression",
    "default",
    "debt",
    "contagion",
    "implosion",
    "ponzi",
    "fraud",
    "exit scam",
    "wipeout",
    "margin call",
    "flash crash",
    "depegging",
    "depeg",
    # RSS crypto frequente (PT/EN)
    "recua",
    "desce",
    "cai",
    "perdas",
    "preocupação",
    "alerta",
    "volatilidade",
    "suspensão",
    "rejeição",
    "delay",
    "deny",
    "denies",
    "raid",
    "tumble",
    "slump",
    "plunge",
    "selloff",
]
DICIONARIO_EUFORIA = [
    # PT-BR
    "alta",
    "lua",
    "recorde",
    "adoção",
    "parceria",
    "aprovado",
    "disparó",
    "disparo",
    "touro",
    "otimismo",
    "superou",
    "surpreende",
    "crescimento",
    "lucro",
    "expansão",
    "novo recorde",
    "adoção em massa",
    # EN crypto/bull
    "etf",
    "halving",
    "rally",
    "pump",
    "bull",
    "moon",
    "ath",
    "breakout",
    "bullish",
    "institutional",
    "adoption",
    "approval",
    "upgrade",
    "listing",
    "integration",
    "milestone",
    "breakthrough",
    "launch",
    "mainnet",
    "airdrop",
    "staking",
    "defi",
    "partnership",
    "accumulation",
    "all-time high",
    "record",
    "surge",
    "soar",
    "etf approval",
    "spot etf",
    "inflows",
    "buy",
    "accumulate",
    "institutional buy",
    "whale buy",
    "positive",
    "optimism",
    "outperform",
    "upgrade",
    "catalyst",
    # RSS PT frequente
    "sobe",
    "dispara",
    "valorização",
    "valoriza",
    "rompe",
    "superação",
    "alívio",
    "alivio",
    "recuperação",
    "recuperacao",
    "alta histórica",
    "alta historica",
    "green",
    "gains",
    "rebound",
]

# Léxico já normalizado (dedup para não contar a mesma substring duas vezes)
PANICO_LEX_FOLD = list(
    dict.fromkeys(w for w in (fold_lexicon_text(x) for x in DICIONARIO_PANICO) if w)
)
EUFORIA_LEX_FOLD = list(
    dict.fromkeys(w for w in (fold_lexicon_text(x) for x in DICIONARIO_EUFORIA) if w)
)


def _default_use_mc_dropout():
    """Monte Carlo dropout: desligado se CPU estiver alta (menor tempo de ciclo neural). psutil opcional."""
    try:
        import psutil

        return float(psutil.cpu_percent(interval=0.12)) < 72.0
    except Exception:
        return False


# Limiar unificado ADX: tendência (Sniper) vs lateral — Maestro, regime, treino neural, gates
REGIME_ADX_MIN = 25.0

DEFAULT_CFG = {
    # Parâmetros mais permissivos para destravar entradas no V90 FINAL (podem ser reajustados via UI)
    "margem_entrada": 10.0,
    "alavancagem": 20,
    # Antes: 80.0 — extremamente seletivo, dificultando sinais em conta demo
    "winrate_minimo": 50.0,
    # Antes: 3 — exige muita confluência; 2 aproxima mais do comportamento do V87
    "confluencia_min": 2,
    "tipo_execucao": "TAKER",
    "use_rsi": True,
    "rsi_p": 14,
    "rsi_ob": 70,
    "rsi_os": 30,
    "use_ema": True,
    "ema_count": 2,
    "emas": [9, 21, 50, 100, 200],
    "use_macd": True,
    "macd_f": 12,
    "macd_s": 26,
    "macd_sig": 9,
    "use_bb": True,
    "bb_p": 20,
    "bb_std": 2.0,
    "use_adx": True,
    "adx_p": 14,
    "adx_thresh": REGIME_ADX_MIN,
    "use_stoch": False,
    "stoch_k": 14,
    "stoch_d": 3,
    "use_vol": False,
    "vol_sma": 20,
    "use_mtf": True,
    "smart_exit": True,
    "escudo_btc": True,
    "use_breakeven": True,
    "use_atr_target": True,
    # NOVAS MELHORIAS INSTITUCIONAIS V1 FINAL:
    "use_trailing_stop": True,
    "trailing_dist_pct": 0.005,  # 0.5% de distância para estrangular o preço
    "use_async_engine": True,
    "use_ws": True,
    # Kline 15m via WebSocket público (reduz REST get_kline no radar)
    "use_kline_ws": True,
    "kline_ws_interval": "15",
    # Túnel 4: máximo de símbolos kline WS (0 = sem limite). Default 20 = top da lista (elite+volume).
    "kline_ws_max_symbols": 20,
    "use_kelly": True,
    "ai_certainty_threshold": 85.0,
    "use_backtest": True,
    "use_l2_depth": True,
    "saldo_simulacao_inicial": 50.0,  # Capital inicial após salvar config em modo simulação
    "use_binance_vision": True,  # Data lake em massa via Bybit linear REST (chave legada no JSON)
    "use_ws_orders": False,  # Envio de Ordem via WebSocket API (Latência Zero)
    # MTF multi-timeframe (58D): mutuamente exclusivo com use_institutional_microstructure na pipeline atual.
    "use_mtf_neural": False,
    # V90.65D: 26 base + 7 (lhn_indicators) + 16 (1h) + 16 (4h) — só ativo com use_mtf_neural True.
    "use_65d_layout": True,
    # Se True (e use_65d_layout+use_mtf_neural), apaga todo o replay_buffer uma vez (evita misturar 58D com 65D).
    "replay_buffer_purge_on_layout_upgrade": False,
    # UI CUSTOMIZATION V1 FINAL
    "escudo_dark": True,
    "idioma": "Português-Brasil",
    "tema_visual": "Dark",
    "fuso_horario": "America/Sao_Paulo",
    # [FIX 21] Parâmetros que antes eram hardcoded dentro do código
    "calibracao_temperatura": 0.35,  # Temperatura do sigmoid de calibração da IA (menor = mais extremo)
    # Após o sigmoid: p' = 50 + (p-50)*stretch. Ex.: 1.08 aumenta certeza longe de 50% (mais agressivo).
    "certeza_stretch": 1.0,
    "trailing_tp_pct": 0.10,  # Percentual de extensão do TP no modo trailing (0.10 = 10%)
    "use_dca_grid": False,  # Liga/desliga o sistema de DCA em grid
    "hard_sl_pct": 0.015,
    "rr_ratio": 3.0,
    "use_trailing_tp": True,  # Liga/desliga o Trailing Take-Profit
    "max_ops_pct_saldo": 0.10,  # Legado; limites efetivos em services.risk_limits.obter_limites_risco
    # --- MELHORIAS IA V2 (M1–M12) ---
    "use_transformer": False,  # [M3] Usar Transformer em vez de LSTM (requer retreino)
    "use_label_smoothing": True,  # [M4] Suavização de rótulos durante treinamento
    "label_smoothing_factor": 0.05,  # [M4] Fator de suavização (0.05 = 5%)
    "use_early_stopping": True,  # [M11] Early Stopping + ReduceLR durante treino
    "use_walkforward": True,  # [M5] Validação Walk-Forward (Time-Series Split)
    # [M8] Padrão via CPU no import; sob carga alta fica False (ciclos mais rápidos)
    "use_mc_dropout": _default_use_mc_dropout(),
    "mc_dropout_samples": 5,  # [M8] Alinhado ao teto de 5 amostras no loop neural
    # Limite de incerteza afrouxado para permitir mais cenários de entrada
    "mc_uncertainty_max": 0.35,
    # Filtros institucionais relaxados por padrão para aproximar o V87
    "use_regime_filter": False,  # [M12] Filtro de regime de mercado pré-entrada
    "use_funding_filter": False,  # [M2] Filtro por Funding Rate da Binance
    "use_oi_filter": False,  # [M2] Filtro por variação de Open Interest
    "use_reward_shaping": True,  # [M6] Reward contínuo em vez de binário no RL
    # --- Aprendizado por reforço (replay): mais peso em perdas, reforço em ganhos (não garante lucro) ---
    "replay_loss_weight_multiplier": 15.0,
    "replay_win_weight_boost": 1.35,
    "replay_parquet_extreme_weight": 12.0,
    # Micro-retreino das RESERVAS logo após fechar trade (debounce evita sobrecarga)
    "incremental_replay_on_trade_close": True,
    "incremental_replay_debounce_sec": 90.0,
    "incremental_replay_min_samples": 8,
    "incremental_replay_epochs": 4,
    "incremental_replay_batch_size": 256,
    # Amostras SQLite pós-trade (replay micro-fit) — até dezenas de milhares
    "incremental_replay_sql_limit": 12000,
    # --- Treino massivo Keras (datalake / CPU) — até ~10GB RAM alvo ---
    "neural_train_min_samples": 16,
    "neural_fit_epochs": 50,
    "neural_fit_batch_size_cap": 512,
    "neural_fit_batch_size_floor": 64,
    "neural_early_stop_patience": 8,
    "neural_mtf_merge_cap": 80000,
    "neural_label_stride": 1,
    "replay_buffer_fit_epochs": 8,
    "replay_buffer_sql_limit": 50000,
    # Punição das reservas: linhas negativas do replay_buffer (override do módulo PUNISH_SAMPLES_LIMIT)
    "punish_samples_limit": 50000,
    "shadow_lake_tail_rows": 0,
    "continuous_learn_min_samples": 12,
    "continuous_learn_epochs": 8,
    "continuous_learn_batch_size": 256,
    # Pós-boot: retreino datalake em background (segundos de espera; 0 = imediato)
    "silent_background_treinar_ia": True,
    "silent_retrain_delay_sec": 90.0,
    # Thresholds dinâmicos para filtros institucionais (Triple Confluence + Regime)
    "adx_regime_minimo": REGIME_ADX_MIN,
    "nlp_sentimento_minimo": -3.0,
    "l2_imbalance_corte": 0.0,
    # [V90 FINAL.5] Escalada de risco
    "max_operacoes_simultaneas": 4,  # Motor usa MAX_OPERACOES_SIMULTANEAS em services.risk_limits (fixo 4)
    # Teto rígido de valor nocional (margem × alavancagem em USD) — independente de Kelly
    "max_order_usd": 500_000.0,
    # Margem isolada (Bybit linear): nunca usar Cross no motor de ordens
    "force_isolated_margin": True,
    # Buffer extra ao elevar margem para cumprir min_qty / min_notional da exchange
    "order_margin_buffer_pct": 0.002,
    # Considera feed WS “morto” após N segundos sem tick (kill switch de novas ordens)
    "ws_feed_stale_sec": 30.0,
    "kelly_max_multiplier": 3.0,
    # Intervalo entre reavaliações do Comitê (regime LATERAL vs TENDÊNCIA / Sniper vs Arbitragem).
    # Antes: 300s (5 min). 60s = 1 min — reage mais rápido a mudanças de ADX macro.
    "comite_macro_interval_sec": 60,
    # --- Maestro V91: filtros em regime macro lateral (Smart Money) ---
    "use_lateral_smart_money_filters": True,
    "lateral_oi_min_delta_pct": 0.0,
    "lateral_funding_bias_eps": 0.0001,
    "lateral_funding_bias_certeza_pts": 3.0,
    # --- DNA neural opcional: Lateral com Hinge + Tanh (requer retreino) ---
    "use_reinforcement_lateral_training": False,
    # +6 dimensões (CVD/VPIN multi-janela/OI×funding) — retreino obrigatório após ativar
    "use_institutional_microstructure": True,
    # --- Gestão de lucro (covarde / rápida) — usados pelo radar de posição ---
    "use_scale_out": True,
    "scale_out_pct": 0.50,
    "alvo_lucro_base": 0.008,
    "breakeven_trigger": 0.003,
    "trailing_tp_activation": 0.005,
    "trailing_tp_callback": 0.0015,
    "trailing_fee_buffer_pct": 0.0012,
    "trailing_min_profit_lock_pct": 0.0025,
}

# --- Janelas de histórico (treino: processar_matriz_ativo / Data Lake klines) ---
# Modo implacável: teto alto — o Parquet no disco + max_rows ditam o que entra no RAM.
NEURAL_BARS_15M_MTF = 100000
NEURAL_BARS_15M_BASE = 100000
NEURAL_BARS_1H_YEAR = 20000  # ~2.3a em 1h
NEURAL_BARS_4H_YEAR = 6000  # ~2.7a em 4h

# Punição neural: máximo de perdas recentes por agente (SQL LIMIT); ajuste via cfg["punish_samples_limit"]
PUNISH_SAMPLES_LIMIT = 50000

# Núcleo fixo do sistema — ordem 0..9 no Top 100 e base de treino/IA (não remove por volume)
ATIVOS_ELITE_PERMANENTE = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "DOTUSDT",
]

# Cada par de ATIVOS_ELITE_PERMANENTE deve existir aqui para labels/UI.
NOMES_ATIVOS_MASTER = {
    "BTCUSDT": "BTC",
    "ETHUSDT": "ETH",
    "SOLUSDT": "SOL",
    "XRPUSDT": "XRP",
    "DOGEUSDT": "DOGE",
    "ZECUSDT": "ZEC",
    "BNBUSDT": "BNB",
    "1000PEPEUSDT": "PEPE",
    "SUIUSDT": "SUI",
    "FLOWUSDT": "FLOW",
    "ADAUSDT": "ADA",
    "AVAXUSDT": "AVAX",
    "PAXGUSDT": "PAXG",
    "TAOUSDT": "TAO",
    "NEARUSDT": "NEAR",
    "LINKUSDT": "LINK",
    "BCHUSDT": "BCH",
    "DOGSUSDT": "DOGS",
    "FILUSDT": "FIL",
    "DOTUSDT": "DOT",
    "ENAUSDT": "ENA",
    "DENTUSDT": "DENT",
    "AAVEUSDT": "AAVE",
    "WIFUSDT": "WIF",
    "LTCUSDT": "LTC",
    "UNIUSDT": "UNI",
    "WLDUSDT": "WLD",
    "1000SHIBUSDT": "SHIB",
    "VIRTUALUSDT": "VIRTUAL",
    "SXTUSDT": "SXT",
    "TRUMPUSDT": "TRUMP",
    "SEIUSDT": "SEI",
    "TRXUSDT": "TRX",
    "ARBUSDT": "ARB",
    "XLMUSDT": "XLM",
    "1000BONKUSDT": "BONK",
    "CRVUSDT": "CRV",
    "ZROUSDT": "ZRO",
    "APTUSDT": "APT",
    "DEXEUSDT": "DEXE",
    "OPUSDT": "OP",
    "HBARUSDT": "HBAR",
    "ETCUSDT": "ETC",
    "GALAUSDT": "GALA",
    "PHAUSDT": "PHA",
    "MANTRAUSDT": "MANTRA",
    "LITUSDT": "LIT",
    "XMRUSDT": "XMR",
    "DASHUSDT": "DASH",
    "INJUSDT": "INJ",
    "CHZUSDT": "CHZ",
    "GRASSUSDT": "GRASS",
    "ONDOUSDT": "ONDO",
    "TONUSDT": "TON",
    "FETUSDT": "FET",
    "ICPUSDT": "ICP",
    "ETHFIUSDT": "ETHFI",
    "AXSUSDT": "AXS",
}

_missing_elite = set(ATIVOS_ELITE_PERMANENTE) - set(NOMES_ATIVOS_MASTER.keys())
if _missing_elite:
    raise RuntimeError(
        f"NOMES_ATIVOS_MASTER incompleto para ATIVOS_ELITE_PERMANENTE: {_missing_elite}"
    )
