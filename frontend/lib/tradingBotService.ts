import { api } from "./api";

// --- Alinhado a server.py (Pydantic) ---

export interface CommandRequest {
  command: string;
}

export interface SaldoRequest {
  saldo: number;
}

/** Espelho de ConfigModel no back-end FastAPI */
export interface ConfigModel {
  api_key: string;
  api_secret: string;
  modo_real: boolean;
  tema_visual: string;
  idioma: string;
  margem_entrada: number;
  alavancagem: number;
  winrate_minimo: number;
  rsi_p: number;
  rsi_ob: number;
  rsi_os: number;
  ema_count: number;
  adx_p: number;
  adx_thresh: number;
  confluencia_min: number;
  use_kelly: boolean;
  use_trailing_stop: boolean;
  escudo_btc: boolean;
  adx_regime_minimo: number;
  nlp_sentimento_minimo: number;
  l2_imbalance_corte: number;
  use_rsi: boolean;
  use_ema: boolean;
  use_macd: boolean;
  use_bb: boolean;
  use_adx: boolean;
  use_stoch: boolean;
  use_vol: boolean;
  use_mtf: boolean;
  macd_f: number;
  macd_s: number;
  macd_sig: number;
  bb_p: number;
  bb_std: number;
  stoch_k: number;
  stoch_d: number;
  vol_sma: number;
  tipo_execucao: string;
  use_async_engine: boolean;
  use_ws: boolean;
  use_l2_depth: boolean;
  use_backtest: boolean;
  /** Legado UI; o backend usa use_binance_vision */
  use_bybit_vision?: boolean;
  use_binance_vision: boolean;
  use_ws_orders: boolean;
  use_mtf_neural: boolean;
  use_institutional_microstructure: boolean;
  use_breakeven: boolean;
  smart_exit: boolean;
  use_trailing_tp: boolean;
  escudo_dark: boolean;
  fuso_horario: string;
  telegram_token: string;
  telegram_chat_id: string;
  /** Capital inicial (modo simulação); obrigatório no POST para o Pydantic não cair no default 50 */
  saldo_simulacao_inicial: number;
  /** Filtros institucionais / motor (espelho DEFAULT_CFG) */
  use_funding_filter: boolean;
  use_oi_filter: boolean;
  use_regime_filter: boolean;
  hard_sl_pct: number;
  engine_param_1: number;
  engine_param_2: number;
  use_lateral_smart_money_filters: boolean;
  lateral_oi_min_delta_pct: number;
  lateral_funding_bias_eps: number;
  lateral_funding_bias_certeza_pts: number;
  use_reinforcement_lateral_training: boolean;
  use_scale_out: boolean;
  scale_out_pct: number;
  alvo_lucro_base: number;
  breakeven_trigger: number;
  trailing_tp_activation: number;
  trailing_tp_callback: number;
  trailing_fee_buffer_pct: number;
  trailing_min_profit_lock_pct: number;
  /** Governança: motor pode abrir novas operações */
  trading_enabled?: boolean;
  /** Máx. fração do saldo em risco ao SL (alinhado a engine executar_ordem_real) */
  risk_threshold?: number;
  /** Layout 65D (MTF + pacote lhn_indicators + sensores no vetor) */
  use_65d_layout?: boolean;
  /** Apagar replay_buffer uma vez ao subir de 58D→65D (evita tensores mistos) */
  replay_buffer_purge_on_layout_upgrade?: boolean;
  /** Gatilho spoof: fração mínima de liquidez que “evapora” vs tick anterior (0.05–0.95) */
  spoof_evaporation_pct?: number;
  /** |Δmark| % máximo tratado como “preço plano” no spoof catcher */
  spoof_price_flat_eps_pct?: number;
  /** Pairs: |Z| de entrada (divergência) */
  arb_zscore_entry?: number;
  /** Pairs: |Z| de saída (convergência à média) */
  arb_zscore_exit?: number;
  /** Intervalo do loop de arbitragem (s) */
  arb_loop_interval_sec?: number;
  /** Cofre neural: quota alvo em GB (zelador WORM) */
  cofre_quota_gb?: number;
  /** Lucro mínimo USD para promoção WORM no cofre */
  cofre_worm_profit_usd?: number;
}

/** Campos aceitos pelo POST /api/config (espelho server.py ConfigModel). */
const CONFIG_POST_KEYS = [
  "api_key",
  "api_secret",
  "modo_real",
  "tema_visual",
  "idioma",
  "margem_entrada",
  "alavancagem",
  "winrate_minimo",
  "rsi_p",
  "rsi_ob",
  "rsi_os",
  "ema_count",
  "adx_p",
  "adx_thresh",
  "confluencia_min",
  "use_kelly",
  "use_trailing_stop",
  "escudo_btc",
  "adx_regime_minimo",
  "nlp_sentimento_minimo",
  "l2_imbalance_corte",
  "use_rsi",
  "use_ema",
  "use_macd",
  "use_bb",
  "use_adx",
  "use_stoch",
  "use_vol",
  "use_mtf",
  "macd_f",
  "macd_s",
  "macd_sig",
  "bb_p",
  "bb_std",
  "stoch_k",
  "stoch_d",
  "vol_sma",
  "tipo_execucao",
  "use_async_engine",
  "use_ws",
  "use_l2_depth",
  "use_backtest",
  "use_binance_vision",
  "use_ws_orders",
  "use_mtf_neural",
  "use_institutional_microstructure",
  "use_breakeven",
  "smart_exit",
  "use_trailing_tp",
  "escudo_dark",
  "fuso_horario",
  "telegram_token",
  "telegram_chat_id",
  "saldo_simulacao_inicial",
  "use_funding_filter",
  "use_oi_filter",
  "use_regime_filter",
  "hard_sl_pct",
  "engine_param_1",
  "engine_param_2",
  "use_lateral_smart_money_filters",
  "lateral_oi_min_delta_pct",
  "lateral_funding_bias_eps",
  "lateral_funding_bias_certeza_pts",
  "use_reinforcement_lateral_training",
  "use_scale_out",
  "scale_out_pct",
  "alvo_lucro_base",
  "breakeven_trigger",
  "trailing_tp_activation",
  "trailing_tp_callback",
  "trailing_fee_buffer_pct",
  "trailing_min_profit_lock_pct",
  "trading_enabled",
  "risk_threshold",
  "use_65d_layout",
  "replay_buffer_purge_on_layout_upgrade",
  "spoof_evaporation_pct",
  "spoof_price_flat_eps_pct",
  "arb_zscore_entry",
  "arb_zscore_exit",
  "arb_loop_interval_sec",
  "cofre_quota_gb",
  "cofre_worm_profit_usd",
] as const;

export function pickConfigForPost(
  raw: Record<string, unknown>
): Partial<ConfigModel> {
  const out: Record<string, unknown> = {};
  for (const k of CONFIG_POST_KEYS) {
    if (k in raw && raw[k] !== undefined) {
      out[k] = raw[k];
    }
  }
  return out as Partial<ConfigModel>;
}

/** ChatMessage: campo oficial é `message`; `mensagem` espelha para nexus_chat / legado */
export interface ChatMessagePayload {
  message: string;
  mensagem: string;
  contexto?: unknown;
}

export interface SafeShutdownRequest {
  reason: string;
  confirmed_exit: boolean;
  audit: Record<string, unknown>;
}

// --- Respostas ---

export interface CommandResponse {
  status: "success" | "error";
  command: string;
  detail?: string;
}

export interface SaldoResponse {
  status: string;
}

export interface HistoryCandle {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
}

export interface ChatApiResponse {
  status: string;
  reply: string;
}

export interface SafeShutdownResponse {
  status: "saved" | "noop" | "error";
  critical_risk?: boolean;
  risks?: string[];
  error?: string;
  reason?: string;
}

export interface ClosePositionResponse {
  status: string;
}

export interface ConfigUpdateResponse {
  status: string;
  applied?: string[];
  detail?: string;
  trace?: string;
}

// --- Funções existentes (lógica inalterada: GET/POST /api/config) ---

export async function getGeneralConfig(): Promise<ConfigModel> {
  const { data } = await api.get<ConfigModel>("/api/config");
  return data;
}

export async function updateConfig(
  cfg: ConfigModel | Partial<ConfigModel>
): Promise<ConfigUpdateResponse> {
  const { data } = await api.post<ConfigUpdateResponse>("/api/config", cfg);
  return data;
}

// --- Rotas adicionais (server.py) ---

export async function sendCommand(command: string): Promise<CommandResponse> {
  const body: CommandRequest = { command };
  const { data } = await api.post<CommandResponse>("/api/command", body);
  return data;
}

export async function updateSaldo(saldo: number): Promise<SaldoResponse> {
  const body: SaldoRequest = { saldo };
  const { data } = await api.post<SaldoResponse>("/api/saldo", body);
  return data;
}

export async function getHistory(
  symbol: string,
  interval: string
): Promise<HistoryCandle[]> {
  const { data } = await api.get<HistoryCandle[]>(
    `/api/history/${encodeURIComponent(symbol)}`,
    { params: { interval } }
  );
  return data;
}

export async function sendChatMessage(
  message: string,
  context?: unknown
): Promise<ChatApiResponse> {
  const trimmed = message.trim();
  const body: ChatMessagePayload = {
    message: trimmed,
    mensagem: trimmed,
    ...(context !== undefined ? { contexto: context } : {}),
  };
  const { data } = await api.post<ChatApiResponse>("/api/chat", body);
  return data;
}

export async function safeShutdown(
  reason: string,
  options?: Partial<
    Pick<SafeShutdownRequest, "confirmed_exit" | "audit">
  >
): Promise<SafeShutdownResponse> {
  const body: SafeShutdownRequest = {
    reason,
    confirmed_exit: options?.confirmed_exit ?? true,
    audit: options?.audit ?? {},
  };
  const { data } = await api.post<SafeShutdownResponse>(
    "/api/safe-shutdown",
    body
  );
  return data;
}

export async function closePosition(symbol: string): Promise<ClosePositionResponse> {
  const { data } = await api.post<ClosePositionResponse>("/api/close", {
    symbol,
  });
  return data;
}

/** Namespace opcional para importação única */
export const TradingBotService = {
  getGeneralConfig,
  pickConfigForPost,
  updateConfig,
  sendCommand,
  updateSaldo,
  getHistory,
  sendChatMessage,
  safeShutdown,
  closePosition,
};

export default TradingBotService;
