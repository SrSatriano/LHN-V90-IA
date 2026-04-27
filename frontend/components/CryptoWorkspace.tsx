"use client";

import React, { useEffect, useState, useRef, useMemo } from "react";
import { useGlobalWebSocket } from "@/app/WebSocketContext";
import { fetchWithAuth, withApiKeyQuery } from "@/lib/lhnAuth";
import { updateConfig, type ConfigModel } from "../lib/tradingBotService";

import { SignalsHistory } from "./SignalsHistory";

import { TransmissionConfig } from "./TransmissionConfig";
import { MemoizedTradingChart, type MemoizedTradingChartHandle } from "./MemoizedTradingChart";
import { Activity, Square, Play, Shield, Cpu, Key, Radio, Settings, Terminal, BarChart2, TrendingUp, TrendingDown, Clock, Search, Globe, Database, X, LineChart, AlertTriangle, Layers, Pencil, Check, ChevronDown, ShieldAlert, Eye, EyeOff, Send } from "lucide-react";

// Top 100 moedas da Bybit Futures — Radar analisa TODAS para encontrar entradas, IA treina com Top 10
const WATCHLIST_SYMBOLS = [
  // Tier 1 — Blue Chips
  "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
  "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LTCUSDT", "LINKUSDT",
  // Tier 2 — Large Cap
  "DOTUSDT", "MATICUSDT", "UNIUSDT", "ATOMUSDT", "ETCUSDT",
  "BCHUSDT", "NEARUSDT", "APTUSDT", "OPUSDT", "ARBUSDT",
  "INJUSDT", "SUIUSDT", "SEIUSDT", "TIAUSDT", "FETUSDT",
  "XLMUSDT", "AAVEUSDT", "MKRUSDT", "COMPUSDT", "CRVUSDT",
  // Tier 3 — Mid Cap DeFi & Layer1
  "RUNEUSDT", "WLDUSDT", "ORDIUSDT", "STXUSDT",
  "MINAUSDT", "QNTUSDT", "HBARUSDT", "VETUSDT", "ALGOUSDT",
  "ICPUSDT", "FILUSDT", "THETAUSDT", "EGLDUSDT", "XTZUSDT",
  "EOSUSDT", "SANDUSDT", "MANAUSDT", "AXSUSDT", "RNDRUSDT",
  "GMTUSDT", "APEUSDT", "GALAUSDT", "ENSUSDT", "LRCUSDT",
  // Tier 4 — Momentum / Trending
  "PYTHUSDT", "DYMUSDT", "JTOUSDT",
  "WUSDT", "STRAUSDT", "BOMEUSDT",
  "PENGUUSDT", "MOVEUSDT", "XAIUSDT", "ZETAUSDT", "NOTUSDT",
  "TONUSDT", "1000PEPEUSDT", "1000SHIBUSDT", "1000BONKUSDT", "1000FLOKIUSDT",
  // Tier 5 — Layer 2 & Infrastructure
  "STRKUSDT", "WIFUSDT", "POPCATUSDT", "EIGENUSDT",
  "SCRUSDT", "REZUSDT", "LISTAUSDT", "IORAUSDT", "BANANAUSDT",
  "LPTUSDT", "GRTUSDT", "IOTXUSDT", "OMUSDT", "ZENUSDT",
  "SKLUSDT", "STORJUSDT", "ROSEUSDT", "1INCHUSDT", "ZRXUSDT",
  // Tier 6 — Completar Top 100
  "XMRUSDT", "ZECUSDT", "DASHUSDT", "RVNUSDT", "WAVESUSDT",
  "ANKRUSDT", "BATUSDT", "COTIUSDT", "CELOUSDT", "OCEANUSDT",
  "SFPUSDT", "TRXUSDT", "SONICUSDT", "FTTUSDT"
];

const CRYPTO_LOGOS: Record<string, string> = {};
WATCHLIST_SYMBOLS.forEach(sym => {
  const baseAsset = sym.replace("USDT", "").toLowerCase();
  CRYPTO_LOGOS[sym] = `https://assets.coincap.io/assets/icons/${baseAsset}@2x.png`;
});
// Substituições especiais
CRYPTO_LOGOS["IOTAUSDT"] = "https://assets.coincap.io/assets/icons/miota@2x.png";
CRYPTO_LOGOS["1000SHIBUSDT"] = "https://assets.coincap.io/assets/icons/shib@2x.png";
CRYPTO_LOGOS["1000PEPEUSDT"] = "https://assets.coincap.io/assets/icons/pepe@2x.png";
CRYPTO_LOGOS["1000BONKUSDT"] = "https://assets.coincap.io/assets/icons/bonk@2x.png";
CRYPTO_LOGOS["1000FLOKIUSDT"] = "https://assets.coincap.io/assets/icons/floki@2x.png";
const TIMEFRAMES = ["5m", "15m", "1h", "4h", "1d"];
const BOTTOM_TABS = ["Histórico de Operações", "Log de Informações", "Alertas e Erros", "Notícias do Mercado", "Desempenho"];

const MOCK_NEWS = [
  { time: "há 2 horas", title: "Bitcoin rompe barreira histórica impulsionado por adoção institucional", provider: "Reuters" },
  { time: "há 5 horas", title: "Ethereum layer 2 atinge novo recorde de TVL", provider: "CoinDesk" },
  { time: "há 14 horas", title: "Solana anuncia nova atualização na mainnet para reduzir taxas", provider: "Bloomberg" },
  { time: "ontem", title: "Juros do FED inalterados: Mercado crypto reage positivamente", provider: "Reuters" },
  { time: "há 2 dias", title: "CEO de grande corretora aponta para fim do inverno cripto", provider: "Forbes" },
];

const NetworkSyncWidget = ({
  lastNeuralScanTs,
  motorRunning,
}: {
  lastNeuralScanTs?: number | null;
  motorRunning?: boolean;
}) => {
  const [time, setTime] = useState(new Date());
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
    const timer = setInterval(() => setTime(new Date()), 1000);
    return () => clearInterval(timer);
  }, []);

  const formatTime = (date: Date) => {
    return date.toLocaleTimeString("pt-BR", {
      hour12: false,
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  };

  const getLatencyText = () => {
    if (lastNeuralScanTs == null || Number.isNaN(Number(lastNeuralScanTs))) {
      if (motorRunning === false) {
        return "motor parado";
      }
      // Evita repetir "varredura" (label já diz) e texto longo que quebrava em 2 linhas feias.
      return "aguardando leitura neural";
    }
    const ts = Number(lastNeuralScanTs);
    const nowSecs = Date.now() / 1000;
    const diff = Math.max(0, nowSecs - ts);

    if (diff < 60) return `${Math.floor(diff)}s atrás`;
    const m = Math.floor(diff / 60);
    const s = Math.floor(diff % 60);
    return `${m}m ${s}s atrás`;
  };

  const scanTitle =
    "Atualiza após o côrtex processar pelo menos um ativo do radar (não é o tick de preço).";

  const scanStatus = getLatencyText();

  if (!mounted) {
    return (
      <div
        className="bg-[#1E222D] rounded-lg px-3 h-[52px] border border-[#2A2E39] flex flex-col justify-center min-w-[200px] max-w-[260px] shadow-sm shrink-0 opacity-0"
        aria-hidden
      >
        <span className="text-[10px] text-gray-500 uppercase font-bold tracking-widest leading-none mb-0.5">
          Hora local
        </span>
        <div className="flex items-center gap-1.5 min-h-[18px] text-[10px] text-[#8B98A5] font-mono">
          <Clock className="w-3.5 h-3.5 text-emerald-500 shrink-0 opacity-80" />
          <span className="text-[#D1D4DC] font-mono font-bold text-sm tabular-nums">--:--:--</span>
          <span className="text-gray-600">·</span>
          <span className="truncate">…</span>
        </div>
      </div>
    );
  }

  return (
    <div
      className="bg-[#1E222D] rounded-lg px-3 h-[52px] border border-[#2A2E39] flex flex-col justify-center min-w-[200px] max-w-[260px] shadow-sm shrink-0 leading-tight"
      title={scanTitle}
    >
      <span className="text-[10px] text-gray-500 uppercase font-bold tracking-widest leading-none mb-0.5">
        Hora local
      </span>
      <div className="flex items-center gap-1.5 min-w-0">
        <Clock className="w-3.5 h-3.5 text-emerald-500 shrink-0 opacity-90" aria-hidden />
        <time
          dateTime={time.toISOString()}
          className="text-[#D1D4DC] font-mono font-bold text-sm tabular-nums tracking-tight leading-none shrink-0"
        >
          {formatTime(time)}
        </time>
        <span className="text-gray-600 text-[10px] shrink-0">·</span>
        <span
          className="text-[#8B98A5] text-[10px] font-mono truncate min-w-0"
          title={scanStatus}
        >
          <span className="text-gray-500 uppercase tracking-wide">Var.</span> {scanStatus}
        </span>
      </div>
    </div>
  );
};

export function CryptoWorkspace({
  tabContext,
  onOpenGovernance,
}: {
  tabContext?: string;
  /** Nexus HFT V90: abre MasterConfigForm (governança única) no shell pai. */
  onOpenGovernance?: () => void;
}) {
  const pricesRef = useRef<Record<string, number>>({});
  const chartHandleRef = useRef<MemoizedTradingChartHandle | null>(null);
  /** Nexus HFT V90: throttle 250ms no merge do stream HFT → setBotData (menos pressão na Main Thread). */
  const botDataWsMergeRef = useRef<((prev: any) => any) | null>(null);
  const botDataWsTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Estados UI Adicionais
  const [activeTab, setActiveTab] = useState<
    "trading" | "terminal" | "ia" | "history" | "positions" | "horizonte" | "sinais" | "transmissao"
  >("trading");
  const [isBalanceSyncing, setIsBalanceSyncing] = useState(false);
  const [snapshotLoading, setSnapshotLoading] = useState(false);
  const [snapshotToast, setSnapshotToast] = useState<{type: 'success'|'error', msg: string} | null>(null);

  const triggerSnapshot = async () => {
    if (snapshotLoading) return;
    setSnapshotLoading(true);
    setSnapshotToast(null);
    try {
      const res = await fetchWithAuth("/api/snapshot", { method: "POST" });
      let data: any = {};
      try {
        data = await res.json();
      } catch (err) {
        data = { error: "Backend indisponível" };
      }
      if (res.ok && data.status === "ok") {
        setSnapshotToast({ type: "success", msg: data.message || "Estado salvo com sucesso!" });
      } else {
        setSnapshotToast({ type: "error", msg: data.detail || "Erro ao salvar snapshot." });
      }
    } catch (e) {
      setSnapshotToast({ type: "error", msg: "Falha de conexão com o backend." });
    } finally {
      setSnapshotLoading(false);
      setTimeout(() => setSnapshotToast(null), 4000);
    }
  };
  useEffect(() => {
    if (tabContext) {
      setActiveTab(tabContext as any);
    }
  }, [tabContext]);
  const [isSidebarOpen, setIsSidebarOpen] = useState(true);
  const [activeBottomTab, setActiveBottomTab] = useState("Log de Informações");
  const [selectedSymbol, setSelectedSymbol] = useState("BTCUSDT");
  const [timeframe, setTimeframe] = useState("15m");
  const [watchlistData] = useState<any[]>(() =>
    WATCHLIST_SYMBOLS.map((symbol, idx) => ({
      symbol,
      price: 0,
      changePercent: 0,
      volume: WATCHLIST_SYMBOLS.length - idx,
    }))
  );
  const [enabledIndicators, setEnabledIndicators] = useState({
    sma20: false,
    ema9: false,
    ema21: false,
    bb: false
  });
  const [showIndicatorsMenu, setShowIndicatorsMenu] = useState(false);
  const [editingSaldoSim, setEditingSaldoSim] = useState(false);
  const editingSaldoSimRef = useRef(false);
  const saldoSimInputRef = useRef<HTMLInputElement | null>(null);

  const [saldoSimInputValue, setSaldoSimInputValue] = useState("");
  const [riskLevel, setRiskLevel] = useState<number>(1.0); // 0.1% to 5.0%

  // Chatbot State
  const [chatMessages, setChatMessages] = useState<{role: string, text: string}[]>([{
    role: "ai", text: "Eu sou o IA Sniper Nexus V90. Fui forjada com 48 Dimensões Institucionais. Como posso te ajudar hoje?"
  }]);
  const [coreChatInput, setCoreChatInput] = useState("");
  const chatEndRef = useRef<HTMLDivElement>(null);
  const logContainerRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);
  const [livePrices, setLivePrices] = useState<Record<string, number>>({});

  // --- SESSÃO: estado isolado, persistido no localStorage (não toca o backend) ---
  type SessionTrade = { id: string; lucro: number; resultado: string; certeza?: number };
  const SESSION_KEY = "lhn_session_stats_v90";
  const HISTORY_RESET_KEY = "lhn_history_reset_epoch_v90";
  const loadSession = (): SessionTrade[] => {
    try { return JSON.parse(localStorage.getItem(SESSION_KEY) || "[]"); } catch { return []; }
  };
  const loadHistoryResetEpoch = (): number => {
    try {
      const n = Number(localStorage.getItem(HISTORY_RESET_KEY) || 0);
      return Number.isFinite(n) ? n : 0;
    } catch {
      return 0;
    }
  };
  const [sessionTrades, setSessionTrades] = useState<SessionTrade[]>(loadSession);
  const sessionTradeIdsRef = useRef<Set<string>>(new Set(loadSession().map((t) => String(t?.id ?? ""))));
  const recentTradeIdsRef = useRef<Set<string>>(new Set());
  const [historyResetEpoch, setHistoryResetEpoch] = useState<number>(loadHistoryResetEpoch);
  const historyResetEpochRef = useRef<number>(historyResetEpoch);
  useEffect(() => {
    historyResetEpochRef.current = historyResetEpoch;
  }, [historyResetEpoch]);
  const tradeEpochMs = (t: any): number => {
    for (const k of ["ts", "close_ts", "timestamp"]) {
      const raw = t?.[k];
      if (raw == null) continue;
      if (typeof raw === "number") return raw > 1e12 ? raw : raw * 1000;
      const n = Number(raw);
      if (Number.isFinite(n)) return n > 1e12 ? n : n * 1000;
      const parsed = Date.parse(String(raw).replace(" ", "T") + (String(raw).includes("Z") ? "" : "Z"));
      if (Number.isFinite(parsed)) return parsed;
    }
    const hora = String(t?.hora ?? "").trim();
    if (hora) {
      const parsed = Date.parse(hora.replace(" ", "T") + (hora.includes("Z") ? "" : "Z"));
      if (Number.isFinite(parsed)) return parsed;
    }
    return 0;
  };
  const filterHistoricoAfterReset = (historico: any[]): any[] => {
    const cutoff = historyResetEpochRef.current;
    if (!cutoff || !Array.isArray(historico)) return Array.isArray(historico) ? historico : [];
    return historico.filter((t) => {
      const ts = tradeEpochMs(t);
      return ts > 0 && ts >= cutoff;
    });
  };
  const makeTradeId = (t: any): string => String(
    t?.ts ??
    `${t?.hora ?? ""}|${t?.ativo ?? ""}|${t?.tipo ?? ""}|${t?.resultado ?? ""}|${Number(t?.lucro ?? 0).toFixed(8)}`
  );
  const appendSessionTradesFromHistorico = (historico: any[]) => {
    historico = filterHistoricoAfterReset(historico);
    if (!Array.isArray(historico) || historico.length === 0) return;
    setSessionTrades((prev) => {
      const incoming: SessionTrade[] = [];
      const seen = sessionTradeIdsRef.current;
      for (const t of historico) {
        const id = makeTradeId(t);
        if (!id || seen.has(id)) continue;
        seen.add(id);
        incoming.push({
          id,
          lucro: Number(t?.lucro ?? 0),
          resultado: String(t?.resultado ?? ""),
          certeza: Number(t?.certeza ?? 0),
        });
      }
      if (incoming.length === 0) return prev;
      const updated = [...prev, ...incoming];
      try {
        localStorage.setItem(SESSION_KEY, JSON.stringify(updated));
      } catch {
        /* quota */
      }
      return updated;
    });
  };

  const sessionAvgCertainty = sessionTrades.length > 0
    ? sessionTrades.reduce((s, t) => s + (t.certeza ?? 0), 0) / sessionTrades.length
    : 0;

  const [sortConfig, setSortConfig] = useState<{key: string, direction: 'asc'|'desc'|'default'}>({key: 'volume', direction: 'desc'});
  
  const sortedWatchlist = useMemo(() => {
    if (sortConfig.direction === 'default') return watchlistData;
    return [...watchlistData].sort((a, b) => {
      let aVal, bVal;
      switch (sortConfig.key) {
        case 'symbol':
          aVal = a.symbol;
          bVal = b.symbol;
          break;
        case 'price':
          aVal = livePrices[a.symbol] ?? a.price ?? 0;
          bVal = livePrices[b.symbol] ?? b.price ?? 0;
          break;
        case 'changePercent':
          aVal = (a.changePercent == null || isNaN(a.changePercent) || a.changePercent === "") ? 0 : Number(a.changePercent);
          bVal = (b.changePercent == null || isNaN(b.changePercent) || b.changePercent === "") ? 0 : Number(b.changePercent);
          break;
        case 'volume':
        default:
          aVal = Number(a.volume) || 0;
          bVal = Number(b.volume) || 0;
          break;
      }
      if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
      if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
      return 0;
    });
  }, [watchlistData, sortConfig, livePrices]);

  const handleSort = (key: string) => {
    setSortConfig(prev => {
      if (prev.key !== key) return { key, direction: 'desc' };
      if (prev.direction === 'desc') return { key, direction: 'asc' };
      return { key: 'volume', direction: 'desc' };
    });
  };

  const [iaSubTab, setIaSubTab] = useState<"telemetry" | "chat">("telemetry");
  const [nexusChat, setNexusChat] = useState<{role: "user" | "nexus", text: string}[]>([
    { role: "nexus", text: "Saudações, Comandante. Sistemas online. Qual a sua diretriz?" }
  ]);
  const [chatInput, setChatInput] = useState("");
  const [isTyping, setIsTyping] = useState(false);

  const {
    isConnected: connected,
    latestMessage: streamPayload,
    clearLatestMessage,
  } = useGlobalWebSocket();
  const [botData, setBotData] = useState<any>({
    sentiment_nlp: 0.0,
    is_running: false,
    config: { theme: "Dark" },
    logs: [],
    historico: [],
    ia_confidence_map: {}, // Nexus HFT V90: placeholder até 1º payload WS com mapa por ativo.
  });

  const flushThrottledBotData = () => {
    botDataWsTimerRef.current = null;
    const fn = botDataWsMergeRef.current;
    botDataWsMergeRef.current = null;
    if (fn) setBotData(fn);
  };

  const queueThrottledSetBotData = (updater: (prev: any) => any) => {
    const prior = botDataWsMergeRef.current;
    botDataWsMergeRef.current = (prev: any) => updater(prior ? prior(prev) : prev);
    if (botDataWsTimerRef.current != null) return;
    botDataWsTimerRef.current = setTimeout(flushThrottledBotData, 250);
  };

  useEffect(() => {
    return () => {
      if (botDataWsTimerRef.current != null) {
        clearTimeout(botDataWsTimerRef.current);
        botDataWsTimerRef.current = null;
      }
    };
  }, []);

  const latestBotDataRef = useRef<any>(botData);
  const latestConfigRef = useRef<any>(null);
  const latestConnectedRef = useRef<boolean>(false);
  const hasPersistedShutdownRef = useRef(false);
  const [motorHandshake, setMotorHandshake] = useState<"ok" | "offline" | "checking">("checking");
  const [uiClockTick, setUiClockTick] = useState(0);

  useEffect(() => {
    const id = window.setInterval(() => setUiClockTick((n) => n + 1), 1000);
    return () => window.clearInterval(id);
  }, []);

  // --- FUNÇÃO DE PROJEÇÃO DE META FINANCEIRA (V90) ---
  const calculateDaysToTarget = (currentBalance: number, targetBalance: number, dailyRatePercent: number) => {
    if (currentBalance >= targetBalance) return 0;
    if (dailyRatePercent <= 0) return Infinity; // Se estiver no prejuízo, não há como prever
    
    const r = dailyRatePercent / 100;
    const days = Math.log(targetBalance / currentBalance) / Math.log(1 + r);
    return Math.ceil(days);
  };



  // Settings API Config State
  const [apiConfig, setApiConfig] = useState<any>({
    api_key: "YOUR_API_KEY_HERE",
    api_secret: "YOUR_API_SECRET_HERE",
    telegram_token: "YOUR_TELEGRAM_BOT_TOKEN_HERE",
    telegram_chat_id: "YOUR_TELEGRAM_CHAT_ID_HERE",
    modo_real: false,
    tema_visual: "Dark",
    idioma: "pt-BR",
    margem_entrada: 3.0,
    alavancagem: 20,
    winrate_minimo: 80.0,
    // Indicadores Técnicos
    use_rsi: true, rsi_p: 14, rsi_ob: 70, rsi_os: 30,
    use_ema: true, ema_count: 2,
    use_macd: true, macd_f: 12, macd_s: 26, macd_sig: 9,
    use_bb: true, bb_p: 20, bb_std: 2.0,
    use_adx: true, adx_p: 14, adx_thresh: 25,
    use_stoch: false, stoch_k: 14, stoch_d: 3,
    use_vol: false, vol_sma: 20,
    use_mtf: true, confluencia_min: 3,
    // IA & Gestão
    tipo_execucao: "TAKER",
    use_news_filter: true, use_ia_sniper: true, use_ia_lateral: true,
    use_airbag: true, use_triplo_consenso: true,
    use_async_engine: true, use_ws: true, use_l2_depth: true,
    use_kelly: true, use_backtest: true, use_bybit_vision: true,
    use_ws_orders: false, use_mtf_neural: true,
    use_trailing_stop: true, escudo_btc: true,
    use_breakeven: true, smart_exit: true,
    use_trailing_tp: true,
    trailing_fee_buffer_pct: 0.0012,
    trailing_min_profit_lock_pct: 0.0025,
    adx_regime_minimo: 20,
    nlp_sentimento_minimo: -3.0,
    l2_imbalance_corte: 0.0,
    use_funding_filter: false,
    use_oi_filter: false,
    use_regime_filter: false,
    hard_sl_pct: 0.015,
    engine_param_1: 0.5,
    engine_param_2: 100,
    // Preferências UI
    escudo_dark: false,
    fuso_horario: "America/Sao_Paulo",
    saldo_simulacao_inicial: 50.0,
  });

  const margemOrdemExibicao = useMemo(() => {
    const pickFirstPositive = (...raw: Array<number | undefined | null>) => {
      for (const x of raw) {
        const n = Number(x);
        if (Number.isFinite(n) && n > 0) return n;
      }
      return 10;
    };
    return pickFirstPositive(
      apiConfig?.margem_entrada,
      botData?.config?.margem_entrada,
      (botData as { margem_entrada?: number })?.margem_entrada
    );
  }, [apiConfig?.margem_entrada, botData?.config, botData]);

  const alavancagemExibicao = useMemo(() => {
    const pickFirstPositive = (...raw: Array<number | undefined | null>) => {
      for (const x of raw) {
        const n = Number(x);
        if (Number.isFinite(n) && n > 0) return Math.round(n);
      }
      return 20;
    };
    return pickFirstPositive(
      apiConfig?.alavancagem,
      botData?.config?.alavancagem,
      botData?.dynamic_leverage,
      (botData as { alavancagem?: number })?.alavancagem
    );
  }, [apiConfig?.alavancagem, botData?.config, botData?.dynamic_leverage, botData]);

  /** Saldo USDT livre para novas margens (espelha backend WS). */
  const saldoLivreExibicao = useMemo(() => {
    const saldo = Number(botData?.saldo ?? 0);
    const aloc = Number(botData?.margem_alocada_total ?? 0);
    if (!Number.isFinite(saldo)) return 0;
    if (!Number.isFinite(aloc)) return Math.max(0, saldo);
    return Math.max(0, saldo - aloc);
  }, [botData?.saldo, botData?.margem_alocada_total]);

  const valorNotionalExibicao = useMemo(() => {
    return margemOrdemExibicao * alavancagemExibicao;
  }, [margemOrdemExibicao, alavancagemExibicao]);

  const [isCommandLoading, setIsCommandLoading] = useState(false);
  const setConfig = setApiConfig;

  // Meta financeira V90
  const [targetBalance, setTargetBalance] = useState(100000); // Começa em 100k como você pediu
  const [isEditingTarget, setIsEditingTarget] = useState(false);
  const [tempTarget, setTempTarget] = useState("100000");
  const [dailyGrowthRate, setDailyGrowthRate] = useState<number>(0.15); // Taxa simulada inicial

  useEffect(() => {
    latestBotDataRef.current = botData;
  }, [botData]);

  useEffect(() => {
    latestConfigRef.current = apiConfig;
  }, [apiConfig]);

  useEffect(() => {
    editingSaldoSimRef.current = editingSaldoSim;
  }, [editingSaldoSim]);

  useEffect(() => {
    latestConnectedRef.current = connected;
  }, [connected]);

  const buildChatOperationsContext = () => {
    const historico = Array.isArray(botData?.historico) ? botData.historico : [];
    const abertas = Array.isArray(botData?.operacoes) ? botData.operacoes : [];

    const recentesFechadas = historico.slice(0, 20).map((t: any) => ({
      origem: "historico",
      ativo: String(t?.ativo ?? ""),
      tipo: String(t?.tipo ?? ""),
      lucro: Number(t?.lucro ?? 0),
      resultado: String(t?.resultado ?? ""),
      hora: String(t?.hora ?? ""),
    }));

    const abertasNormalizadas = abertas.slice(0, 20).map((op: any) => ({
      origem: "aberta",
      ativo: String(op?.symbol ?? op?.ativo ?? ""),
      tipo: String(op?.tipo ?? op?.direcao ?? ""),
      lucro: Number(op?.pnl_flutuante_usd ?? op?.lucro ?? 0),
      resultado: "EM_ANDAMENTO",
      hora: "",
      preco_entrada: Number(op?.preco ?? 0),
      preco_atual: Number(
        livePrices[String(op?.symbol ?? op?.ativo ?? "")] ??
          pricesRef.current[String(op?.symbol ?? op?.ativo ?? "")] ??
          0
      ),
    }));

    return [...recentesFechadas, ...abertasNormalizadas];
  };

  useEffect(() => {
    let alive = true;
    const ping = async () => {
      try {
        const r = await fetchWithAuth("/api/config", { cache: "no-store" });
        if (!alive) return;
        setMotorHandshake(r.ok ? "ok" : "offline");
      } catch (e) {
        if (alive) {
          setMotorHandshake("offline");
          console.error("Motor handshake (config) falhou:", e);
        }
      }
    };
    ping();
    const id = setInterval(ping, 60000);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, []);

  // Calcula dinamicamente sempre que a tela atualizar
  const estimatedDays = calculateDaysToTarget(botData?.saldo || 1000, targetBalance, dailyGrowthRate);
  const estimatedYears = estimatedDays !== Infinity ? (estimatedDays / 365).toFixed(1) : "∞";

  const fetchConfig = async () => {
    try {
      const res = await fetchWithAuth("/api/config");
      if (res.ok) {
        let data: any;
        try {
          data = await res.json();
        } catch (err) {
          console.warn("Falha ao parsear config JSON", err);
          return;
        }
        // Mescla o que veio do backend com os padrões locais
        setApiConfig((prev: any) => ({
          ...prev,
          ...data,
        }));
      }
    } catch (e) {
      console.error("Config fetch error", e);
    }
  };

  const atualizarSaldoSimulado = async () => {
    const raw = String(saldoSimInputRef.current?.value ?? saldoSimInputValue ?? "")
      .trim()
      .replace(/\s/g, "")
      .replace(",", ".");
    const val = Number(raw);
    if (!Number.isFinite(val) || val < 0) return;
    try {
      const res = await fetchWithAuth("/api/saldo", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ saldo: val }),
      });
      if (res.ok) {
        setApiConfig((prev: any) => ({ ...prev, saldo_simulacao_inicial: val }));
        setBotData((prev: any) => ({ ...prev, saldo: val }));
        editingSaldoSimRef.current = false;
        setEditingSaldoSim(false);
      }
    } catch (e) {
      console.error("Erro ao atualizar saldo", e);
    }
  };

  // Estado referencial para o símbolo selecionado evitar recriação de connection WebSocket
  const selectedSymbolRef = useRef(selectedSymbol);
  useEffect(() => {
    selectedSymbolRef.current = selectedSymbol;
  }, [selectedSymbol]);

  // Stream HFT: única conexão WebSocket (WebSocketProvider + useGlobalWebSocket).
  useEffect(() => {
    if (streamPayload == null) return;
    try {
      const data = streamPayload;
      const prices = data?.prices;
      if (data.prices && typeof data.prices === "object") {
        pricesRef.current = data.prices;
        setLivePrices((prev) => ({ ...prev, ...data.prices }));
      }
      const { prices: _p, equity_total: _omitEquity, ...rest } = data;
      void _omitEquity;
      const saldo = Number(data?.saldo);
      const pnlLiquido = Number(data?.pnl_liquido);
      const totalProfitUsd = Number(data?.total_profit_usd);
      const totalLossUsd = Number(data?.total_loss_usd);
      const winRate = Number(data?.win_rate);
      const nlpScore = Number(data?.nlp_score ?? data?.sentiment_nlp);
      const wsHistoricoRaw = Array.isArray(data?.operacoes_finalizadas)
        ? data.operacoes_finalizadas
        : Array.isArray(data?.historico)
          ? data.historico
          : null;
      const wsHistorico = Array.isArray(wsHistoricoRaw)
        ? filterHistoricoAfterReset(wsHistoricoRaw)
        : null;
      const wsStats = data?.estatisticas && typeof data.estatisticas === "object"
        ? data.estatisticas
        : null;
      const resetAtivo = historyResetEpochRef.current > 0;
      const cfg = latestConfigRef.current;
      const freezeSaldoWs =
        editingSaldoSimRef.current === true &&
        cfg &&
        !cfg.modo_real &&
        data.saldo !== undefined;
      // Nexus HFT V90: merge do stream HFT com throttle (ver queueThrottledSetBotData).
      queueThrottledSetBotData((prev: any) => ({
        ...prev,
        ...rest,
        saldo: freezeSaldoWs
          ? Number.isFinite(Number(prev.saldo))
            ? Number(prev.saldo)
            : 0
          : Number.isFinite(saldo)
            ? saldo
            : 0,
        pnl_liquido: Number.isFinite(pnlLiquido) ? pnlLiquido : 0,
        sentiment_nlp: Number.isFinite(nlpScore)
          ? nlpScore
          : Number.isFinite(Number(prev?.sentiment_nlp))
            ? Number(prev.sentiment_nlp)
            : 0,
        historico: Array.isArray(wsHistorico)
          ? wsHistorico
          : Array.isArray(prev.historico)
            ? prev.historico
            : [],
        total_trades:
          resetAtivo
            ? (Array.isArray(wsHistorico) ? wsHistorico.length : 0)
            : wsStats && Number.isFinite(Number(wsStats.total_trades))
            ? Number(wsStats.total_trades)
            : Number.isFinite(Number(rest?.total_trades))
              ? Number(rest.total_trades)
              : Number.isFinite(Number(prev?.total_trades))
                ? Number(prev.total_trades)
                : 0,
        total_wins:
          resetAtivo
            ? (Array.isArray(wsHistorico) ? wsHistorico.filter((t: any) => Number(t?.lucro ?? t?.pnl ?? t?.profit ?? 0) > 0).length : 0)
            : wsStats && Number.isFinite(Number(wsStats.wins))
            ? Number(wsStats.wins)
            : Number.isFinite(Number(rest?.total_wins))
              ? Number(rest.total_wins)
              : Number.isFinite(Number(prev?.total_wins))
                ? Number(prev.total_wins)
                : 0,
        total_losses:
          resetAtivo
            ? (Array.isArray(wsHistorico) ? wsHistorico.filter((t: any) => Number(t?.lucro ?? t?.pnl ?? t?.profit ?? 0) <= 0).length : 0)
            : wsStats && Number.isFinite(Number(wsStats.losses))
            ? Number(wsStats.losses)
            : Number.isFinite(Number(rest?.total_losses))
              ? Number(rest.total_losses)
              : Number.isFinite(Number(prev?.total_losses))
                ? Number(prev.total_losses)
                : 0,
        total_profit_usd:
          resetAtivo
            ? (Array.isArray(wsHistorico) ? wsHistorico.filter((t: any) => Number(t?.lucro ?? t?.pnl ?? t?.profit ?? 0) > 0).reduce((sum: number, t: any) => sum + Number(t?.lucro ?? t?.pnl ?? t?.profit ?? 0), 0) : 0)
            : wsStats && Number.isFinite(Number(wsStats.lucro_bruto_usd))
            ? Number(wsStats.lucro_bruto_usd)
            : Number.isFinite(totalProfitUsd)
              ? totalProfitUsd
              : 0,
        total_loss_usd:
          resetAtivo
            ? (Array.isArray(wsHistorico) ? wsHistorico.filter((t: any) => Number(t?.lucro ?? t?.pnl ?? t?.profit ?? 0) <= 0).reduce((sum: number, t: any) => sum + Math.abs(Number(t?.lucro ?? t?.pnl ?? t?.profit ?? 0)), 0) : 0)
            : wsStats && Number.isFinite(Number(wsStats.perda_bruta_usd))
            ? Number(wsStats.perda_bruta_usd)
            : Number.isFinite(totalLossUsd)
              ? totalLossUsd
              : 0,
        win_rate:
          resetAtivo
            ? (Array.isArray(wsHistorico) && wsHistorico.length > 0
              ? (wsHistorico.filter((t: any) => Number(t?.lucro ?? t?.pnl ?? t?.profit ?? 0) > 0).length / wsHistorico.length) * 100
              : 0)
            : wsStats && Number.isFinite(Number(wsStats.winrate))
            ? Number(wsStats.winrate)
            : Number.isFinite(winRate)
              ? winRate
              : 0,
      }));

      // Governança no MasterConfigForm (aba settings desmonta este workspace).
      if (data.config && typeof data.config === "object") {
        setApiConfig((prev: any) => ({
          ...prev,
          margem_entrada:
            data.config.margem_entrada != null && data.config.margem_entrada !== ""
              ? Number(data.config.margem_entrada)
              : prev.margem_entrada,
          alavancagem:
            data.config.alavancagem != null && data.config.alavancagem !== ""
              ? Number(data.config.alavancagem)
              : prev.alavancagem,
          winrate_minimo:
            data.config.winrate_minimo != null && data.config.winrate_minimo !== ""
              ? Number(data.config.winrate_minimo)
              : prev.winrate_minimo,
        }));
      }

      if (Array.isArray(wsHistorico)) {
        appendSessionTradesFromHistorico(wsHistorico);
      }

      if (data.saldo !== undefined) {
        const wsSaldo = Number(data.saldo);
        const wsPnl = Number(data.pnl_liquido ?? 0);
        const saldoInicialEstimado =
          (Number.isFinite(wsSaldo) ? wsSaldo : 0) - (Number.isFinite(wsPnl) ? wsPnl : 0);
        let currentSessionRate = 0;
        if (saldoInicialEstimado > 0) {
          currentSessionRate = ((Number.isFinite(wsPnl) ? wsPnl : 0) / saldoInicialEstimado) * 100;
        }
        const safeRate = Math.max(
          0.15,
          Math.min(currentSessionRate > 0 ? currentSessionRate : 0.15, 2.0)
        );
        setDailyGrowthRate(safeRate);
      }

      const sym = selectedSymbolRef.current;
      const livePrice = pricesRef.current[sym];
      if (livePrice !== undefined) {
        const price = Number(livePrice);
        if (Number.isFinite(price)) {
          chartHandleRef.current?.applyLiveTick(sym, sym, price);
        }
      }
    } catch (e) {
      console.error("Erro ao processar mensagem WebSocket:", e);
    }
  }, [streamPayload]);

  // UI otimista: watchlist nasce localmente; preços/trades hidratam exclusivamente via WebSocket.
  useEffect(() => {
    fetchConfig();
  }, []);

  const sendChatMessage = async () => {
    if (!coreChatInput.trim()) return;
    
    const userText = coreChatInput;
    setChatMessages(prev => [...prev, {role: "user", text: userText}]);
    setCoreChatInput("");

    try {
      const res = await fetchWithAuth("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        timeoutMs: 600_000,
        body: JSON.stringify({
          mensagem: userText,
          contexto: {
            operacoes: buildChatOperationsContext(),
            estatisticas: botData?.estatisticas || {},
            nlp_score: 5.0,
          },
        }),
      });
      const data = await res.json();
      const reply = data.resposta ?? data.reply;
      if (reply) {
        setChatMessages(prev => [...prev, { role: "ai", text: String(reply) }]);
      }
    } catch (e) {
      console.error("Chat Núcleo AI:", e);
      setChatMessages(prev => [...prev, {role: "ai", text: "Erro de conexão com o Núcleo AI."}]);
    }
  };

  useEffect(() => {
    if (chatEndRef.current) {
        chatEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [nexusChat?.length, iaSubTab]);

  // 🛡️ FIX NEXUS: Dependência estática (.length) para evitar Crash do React
  useEffect(() => {
    if (autoScroll && activeBottomTab === "Log de Informações" && logContainerRef.current) {
      logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight;
    }
  }, [botData.log_history?.length, autoScroll, activeBottomTab]);

  // Nexus HFT V90: gráfico isolado em MemoizedTradingChart (sem createChart inline aqui).

  useEffect(() => {
    const buildShutdownAudit = () => {
      const snapshot = latestBotDataRef.current || {};
      const cfgSnapshot = latestConfigRef.current || {};
      const operacoes = Array.isArray(snapshot.operacoes) ? snapshot.operacoes : [];
      const detalhes: string[] = [];

      for (const op of operacoes) {
        const symbol = String(op?.symbol || "ATIVO_DESCONHECIDO");
        const sl = Number(op?.sl);
        if (!Number.isFinite(sl) || sl <= 0) {
          detalhes.push(`[${symbol}] Stop Loss ausente ou invalido.`);
        }
        if (cfgSnapshot.use_trailing_stop) {
          const tp = Number(op?.tp);
          if (!Number.isFinite(tp) || tp <= 0) {
            detalhes.push(`[${symbol}] Gatilho de trailing inconsistente (TP invalido).`);
          }
        }
      }

      if (cfgSnapshot.use_trailing_stop && operacoes.length > 0 && !latestConnectedRef.current) {
        detalhes.push("WebSocket de rastreio inativo para trailing stop.");
      }

      const critical = detalhes.length > 0;
      return {
        critical,
        details: detalhes,
        payload: {
          websocket_online: latestConnectedRef.current,
          active_orders: operacoes.length,
          details: detalhes,
        },
      };
    };

    const persistSafeShutdown = () => {
      if (hasPersistedShutdownRef.current) return;
      hasPersistedShutdownRef.current = true;

      const audit = buildShutdownAudit();
      const payload = JSON.stringify({
        reason: "browser_unload",
        confirmed_exit: true,
        audit: audit.payload,
        ts: Date.now(),
      });

      try {
        if (navigator.sendBeacon) {
          const blob = new Blob([payload], { type: "application/json" });
          navigator.sendBeacon(withApiKeyQuery("/api/safe-shutdown"), blob);
          return;
        }
      } catch (e) {
        console.error("safe-shutdown sendBeacon:", e);
      }

      try {
        fetchWithAuth("/api/safe-shutdown", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: payload,
          keepalive: true,
        });
      } catch (e) {
        console.error("safe-shutdown fetch:", e);
      }
    };

    const handleBeforeUnload = (event: BeforeUnloadEvent) => {
      const audit = buildShutdownAudit();
      if (audit.critical) {
        try {
          window.alert(
            "ALERTA CRITICO DE SAIDA:\n" +
            "Foram detectadas ordens desprotegidas ou trailing instavel.\n" +
            audit.details.join("\n")
          );
        } catch (e) {}
      }
      event.preventDefault();
      event.returnValue = "";
    };

    window.addEventListener("beforeunload", handleBeforeUnload);
    window.addEventListener("pagehide", persistSafeShutdown);
    window.addEventListener("unload", persistSafeShutdown);

    return () => {
      window.removeEventListener("beforeunload", handleBeforeUnload);
      window.removeEventListener("pagehide", persistSafeShutdown);
      window.removeEventListener("unload", persistSafeShutdown);
    };
  }, []);

  const sendCommand = async (cmd: string) => {
    if (cmd === "START_ENGINE" || cmd === "STOP_ENGINE") {
      setIsCommandLoading(true);
    }
    try {
      await fetchWithAuth("/api/command", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ command: cmd })
      });
    } catch (e) {
      console.error("Erro ao enviar comando:", e);
    } finally {
      if (cmd === "START_ENGINE" || cmd === "STOP_ENGINE") {
        setTimeout(() => setIsCommandLoading(false), 600);
      }
    }
  };

  /** Zera desempenho no motor (histórico + contadores) e o cache local da aba Sessão. */
  const zerarSessao = async () => {
    try {
      const res = await fetchWithAuth("/api/command", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ command: "ZERAR_STATS" }),
      });
      if (!res.ok) {
        console.error("ZERAR_STATS falhou:", res.status);
        return;
      }
    } catch (e) {
      console.error("Erro ao zerar desempenho:", e);
      return;
    }
    if (botDataWsTimerRef.current) clearTimeout(botDataWsTimerRef.current);
    botDataWsTimerRef.current = null;
    botDataWsMergeRef.current = null;
    const resetEpoch = Date.now();
    historyResetEpochRef.current = resetEpoch;
    setHistoryResetEpoch(resetEpoch);
    setSessionTrades([]);
    sessionTradeIdsRef.current.clear();
    recentTradeIdsRef.current.clear();
    try {
      localStorage.removeItem(SESSION_KEY);
      localStorage.setItem(HISTORY_RESET_KEY, String(resetEpoch));
    } catch {
      /* quota */
    }
    setBotData((prev: any) => ({
      ...prev,
      historico: [],
      total_trades: 0,
      total_wins: 0,
      total_losses: 0,
      total_profit_usd: 0,
      total_loss_usd: 0,
      win_rate: 0,
      pnl_liquido: 0,
      log_history: [],
      logs: [],
    }));
    if (clearLatestMessage) clearLatestMessage();
  };

  /** Nexus HFT V90: TP/SL no gráfico memoizado (overlay estável por símbolo). */
  const chartOverlay = useMemo(() => {
    const operacoes = Array.isArray(botData.operacoes) ? botData.operacoes : [];
    const opSelecionada = operacoes.find(
      (op: any) => String(op?.symbol ?? op?.ativo ?? "").toUpperCase() === selectedSymbol.toUpperCase()
    );
    if (!opSelecionada) return null;
    const tp = typeof opSelecionada.tp === "number" ? opSelecionada.tp : undefined;
    const sl = typeof opSelecionada.sl === "number" ? opSelecionada.sl : undefined;
    if (tp == null && sl == null) return null;
    return { tp, sl };
  }, [botData.operacoes, selectedSymbol]);

  /** Nexus HFT V90: card Confiança Preditiva — prob. do símbolo do gráfico (mapa), fallback média / "Analisando...". */
  const predictiveConfidenceUi = useMemo(() => {
    const sym = String(selectedSymbol || "").trim().toUpperCase();
    const map = botData?.ia_confidence_map as Record<string, unknown> | undefined;
    const meanGlobal = botData?.ia_confidence;
    if (sym && map && typeof map === "object") {
      const raw = map[sym] ?? map[String(selectedSymbol || "").trim()];
      if (raw !== undefined && raw !== null && Number.isFinite(Number(raw))) {
        const pct = Math.max(0, Math.min(100, Number(raw)));
        return { kind: "symbol" as const, pct, text: `${pct.toFixed(2)}%` };
      }
    }
    if (
      meanGlobal != null &&
      meanGlobal !== "" &&
      Number.isFinite(Number(meanGlobal))
    ) {
      const pct = Math.max(0, Math.min(100, Number(meanGlobal)));
      return { kind: "mean" as const, pct, text: `${pct.toFixed(2)}%` };
    }
    return { kind: "pending" as const, pct: null as number | null, text: "Analisando..." };
  }, [botData?.ia_confidence_map, botData?.ia_confidence, selectedSymbol]);

  const menuItems = [
    { icon: Activity,  label: "Painel Central" },
    { icon: BarChart2, label: "Posições Abertas" },
    { icon: Cpu,       label: "IA Nexus" },
    { icon: Terminal,  label: "Log de Informações" },
    { icon: Radio,     label: "Histórico de Sinais" },
    { icon: Send,      label: "Transmissão Telegram" },
    { icon: Settings,  label: "Configurações" },
  ];

  const logs = botData.log_history ?? [];
  const pickHistoricoFromWs = (sp: any, bd: any): any[] => {
    if (sp == null) return filterHistoricoAfterReset(Array.isArray(bd?.historico) ? bd.historico : []);
    if (Array.isArray(sp.operacoes_finalizadas)) return filterHistoricoAfterReset(sp.operacoes_finalizadas);
    if (Array.isArray(sp.historico)) return filterHistoricoAfterReset(sp.historico);
    return filterHistoricoAfterReset(Array.isArray(bd?.historico) ? bd.historico : []);
  };
  const historicoRealtime = pickHistoricoFromWs(streamPayload, botData);
  const wsStatsDireto =
    historyResetEpoch <= 0 && streamPayload?.estatisticas && typeof streamPayload.estatisticas === "object"
      ? streamPayload.estatisticas
      : null;
  const consFin = wsStatsDireto?.consolidado_finalizadas as
    | {
        total_trades?: number;
        wins?: number;
        losses?: number;
        winrate?: number;
        pnl_liquido?: number;
      }
    | undefined;
  const useConsFin =
    consFin != null &&
    Number.isFinite(Number(consFin.total_trades)) &&
    Number(consFin.total_trades) > 0 &&
    Number.isFinite(Number(consFin.pnl_liquido));
  const sessionPnl = useConsFin
    ? Number(consFin!.pnl_liquido)
    : sessionTrades.reduce((sum, t) => sum + (t.lucro ?? 0), 0);
  const sessionWins = useConsFin
    ? Number(consFin!.wins ?? 0)
    : sessionTrades.filter((t) => t.resultado?.includes("WIN") || t.resultado?.includes("GANHO")).length;
  const sessionLosses = useConsFin
    ? Number(consFin!.losses ?? 0)
    : sessionTrades.filter((t) => t.resultado?.includes("LOSS") || t.resultado?.includes("PERDA")).length;
  const sessionTradesCount = useConsFin
    ? Number(consFin!.total_trades ?? 0)
    : sessionTrades.length;
  const sessionWinrate = useConsFin
    ? Number(consFin!.winrate ?? 0)
    : sessionTrades.length > 0
      ? (sessionWins / sessionTrades.length) * 100
      : 0;
  const cfTotal = consFin != null ? Number(consFin.total_trades) : NaN;
  const statsTotal = Number(wsStatsDireto?.total_trades ?? botData.total_trades ?? 0);
  const preferConsolidadoCounts =
    consFin != null &&
    Number.isFinite(cfTotal) &&
    cfTotal > 0 &&
    (statsTotal === 0 || cfTotal > statsTotal);
  const totalTradesRealtime = Number(
    preferConsolidadoCounts ? cfTotal : wsStatsDireto?.total_trades ?? botData.total_trades ?? 0
  );
  const totalWinsRealtime = Number(
    preferConsolidadoCounts
      ? consFin!.wins ?? 0
      : wsStatsDireto?.wins ?? botData.total_wins ?? 0
  );
  const totalLossesRealtime = Number(
    preferConsolidadoCounts
      ? consFin!.losses ?? 0
      : wsStatsDireto?.losses ?? botData.total_losses ?? 0
  );
  const totalProfitRealtime = Number(wsStatsDireto?.lucro_bruto_usd ?? botData.total_profit_usd ?? 0);
  const totalLossRealtime = Number(wsStatsDireto?.perda_bruta_usd ?? botData.total_loss_usd ?? 0);
  const nlpScoreRealtime = React.useMemo(() => {
    const v =
      streamPayload?.nlp_score ??
      streamPayload?.sentiment_nlp ??
      botData.sentiment_nlp ??
      botData.sentimento_nlp ??
      botData.nlp_sentiment;
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  }, [
    streamPayload?.nlp_score,
    streamPayload?.sentiment_nlp,
    streamPayload?.news,
    botData.sentiment_nlp,
    botData.sentimento_nlp,
    botData.nlp_sentiment,
  ]);

  const totalNoticiasNlp = React.useMemo(() => {
    const n = streamPayload?.news;
    return Array.isArray(n) ? n.length : 0;
  }, [streamPayload?.news]);

  const neuralStats = React.useMemo(() => {
    const logs = botData.log_history ?? [];
    let nlpScore = 5.0;
    let memoryCount = 0;
    const neuralEvents: string[] = [];

    for (let i = logs.length - 1; i >= 0; i--) {
      const log = String(logs[i] ?? "");

      if (log.includes("Sentimento NLP Atualizado:") && nlpScore === 5.0) {
        const match = log.match(/Atualizado:\s*([0-9.]+)/);
        if (match) nlpScore = parseFloat(match[1]);
      }

      if (log.includes("Córtex Expandido com") && memoryCount === 0) {
        const match = log.match(/com\s*(\d+)\s*memórias/);
        if (match) memoryCount = parseInt(match[1]);
      }

      if (
        (log.includes("Ilusões Institucionais") || log.includes("Fluxo Tóxico")) &&
        5 > neuralEvents.length
      ) {
        neuralEvents.push(log);
      }
    }

    return { nlpScore, memoryCount, neuralEvents };
  }, [botData.log_history?.length]);

  return (
    <div className="flex min-h-0 min-w-0 w-full flex-1 flex-col overflow-hidden bg-[#0b0e11] font-sans text-[13px] antialiased text-gray-300">
            

      {/* 1. TOP BAR BYBIT STYLE (BENTO BOX GRID) */}
      <header className="relative z-50 flex h-[76px] min-h-[76px] shrink-0 items-center justify-between gap-2 border-b border-[#2A2E39] bg-[#0b0e11] px-3 shadow-sm sm:gap-3 sm:px-4">
        <div className="flex min-w-0 flex-1 flex-nowrap items-center gap-2 overflow-x-auto sm:gap-2.5 [scrollbar-width:thin]">
          
          {/* Bento Card: Asset & Price — altura fixa alinhada ao restante da faixa */}
          <div className="bg-[#1E222D] rounded-lg px-2.5 h-[52px] border border-[#2A2E39] flex items-center gap-2 sm:gap-3 shadow-sm shrink-0 min-w-0 max-w-[min(100%,420px)]">
            <div className="flex items-center gap-2 shrink-0">
               {CRYPTO_LOGOS[selectedSymbol] && (
                 <img
                   src={CRYPTO_LOGOS[selectedSymbol]}
                   className="h-6 w-6"
                   alt={selectedSymbol}
                   draggable={false}
                   suppressHydrationWarning={true}
                 />
               )}
               <h3 className="font-bold text-lg sm:text-xl font-mono tracking-wider text-white truncate">
                 {selectedSymbol.replace("USDT", "")}<span className="text-gray-500 text-sm">/USDT</span>
               </h3>
               <span className="text-[10px] font-bold bg-[#2b3139]/80 text-[#0ecb81] px-1.5 py-0.5 rounded border border-[#0ecb81]/20 ml-1">Perpétuo</span>
            </div>
            <div className="flex shrink-0 flex-col justify-center">
              <span className="text-xl font-bold font-mono tabular-nums text-white">
                {(livePrices[selectedSymbol] ?? pricesRef.current[selectedSymbol]) != null
                  ? Number(livePrices[selectedSymbol] ?? pricesRef.current[selectedSymbol]).toFixed(4)
                  : "0.0000"}
              </span>
              <span className="text-[10px] uppercase font-bold text-gray-500 underline decoration-dashed">Preço de Mercado</span>
            </div>
          </div>
          
          {/* Bento Card: Fundos (Dynamically syncs on Env Toggle) */}
          <div className={`bg-[#1E222D] rounded-lg px-2.5 h-[52px] border border-[#2A2E39] flex flex-col justify-center min-w-[118px] shadow-sm relative overflow-hidden transition-all duration-300 shrink-0 ${isBalanceSyncing ? 'ring-1 ring-[#00d2ff] opacity-90' : ''}`}>
            {isBalanceSyncing && <div className="absolute inset-0 bg-gradient-to-r from-transparent via-[#00d2ff]/10 to-transparent animate-[shimmer_1.5s_infinite] pointer-events-none" />}
            <span className="text-[10px] text-gray-500 uppercase font-bold tracking-widest leading-none mb-1">
              Fundos {apiConfig?.modo_real ? '(Conta Real)' : '(Simulação)'}
            </span>
            <span className={`text-sm font-mono font-bold transition-all duration-300 ${isBalanceSyncing ? 'text-transparent animate-pulse' : 'text-white'}`}>
              {isBalanceSyncing ? (
                <span className="text-[#00d2ff] flex items-center gap-2"><div className="w-3 h-3 border-2 border-[#00d2ff] border-t-transparent rounded-full animate-spin"></div>Sincronizando...</span>
              ) : (
                `US$ ${Number(botData.saldo ?? 0).toFixed(2)}`
              )}
            </span>
          </div>

          {/* Bento Card: Confiança Preditiva — Nexus HFT V90: valor por selectedSymbol (ia_confidence_map). */}
          <div className="bg-[#1E222D] rounded-lg px-2.5 h-[52px] border border-[#2A2E39] flex flex-col justify-center shadow-sm shrink-0 min-w-[108px]">
            <span className="text-[10px] text-gray-500 uppercase font-bold tracking-widest leading-none mb-1">Confiança Preditiva</span>
            <span
              className={`text-sm font-mono font-bold ${
                predictiveConfidenceUi.pct == null
                  ? "text-gray-400"
                  : predictiveConfidenceUi.pct > 80
                    ? "text-[#0ecb81]"
                    : predictiveConfidenceUi.pct < 40
                      ? "text-[#f6465d]"
                      : "text-[#f0b90b]"
              }`}
            >
              {predictiveConfidenceUi.text}
            </span>
          </div>
        </div>

        {/* NLP + relógio + ações — mesma linha que os cartões (print). */}
        <div className="flex shrink-0 flex-nowrap items-center gap-2 sm:gap-2.5">
          <div
            className="bg-[#1E222D] rounded-lg px-2.5 h-[52px] w-[min(220px,28vw)] border border-[#2A2E39] flex flex-col justify-center shadow-sm overflow-hidden"
            title={`Filtro NLP mín. (config): ≥${Number(apiConfig?.nlp_sentimento_minimo ?? -3).toFixed(1)}`}
          >
            <span className="text-[10px] text-gray-500 uppercase font-bold tracking-widest leading-none mb-0.5 truncate">
              Sentimento Global
            </span>
            <div className="flex items-center gap-1 min-w-0">
              <span
                className={`text-sm font-bold font-mono tabular-nums leading-none shrink-0 ${nlpScoreRealtime > 0 ? "text-green-500" : nlpScoreRealtime < 0 ? "text-red-500" : "text-gray-400"}`}
              >
                {nlpScoreRealtime > 0 ? "+" : ""}
                {nlpScoreRealtime.toFixed(2)}
              </span>
              <Globe className="w-3 h-3 opacity-50 shrink-0" aria-hidden />
              <span className="text-[9px] text-gray-500 leading-none truncate min-w-0">
                {totalNoticiasNlp} Notícias (1h)
              </span>
            </div>
          </div>

          <NetworkSyncWidget
            lastNeuralScanTs={botData.last_neural_scan_ts}
            motorRunning={botData.is_running}
          />
          
          <button 
             onClick={() => sendCommand(botData.is_running ? "STOP_ENGINE" : "START_ENGINE")}
             disabled={isCommandLoading}
             className={`h-[52px] min-w-[140px] flex items-center justify-center gap-2 px-4 rounded-lg border font-bold uppercase tracking-widest text-[11px] transition-all relative overflow-hidden
               ${botData.is_running 
                 ? 'bg-[#f6465d]/10 border-[#f6465d]/50 text-[#f6465d] hover:bg-[#f6465d]/20 shadow-[0_0_10px_rgba(246,70,93,0.2)]' 
                 : 'bg-[#0ecb81]/10 border-[#0ecb81]/50 text-[#0ecb81] hover:bg-[#0ecb81]/20 shadow-[0_0_10px_rgba(14,203,129,0.2)]'}
               ${isCommandLoading ? 'opacity-50 cursor-not-allowed scale-95' : 'hover:scale-105 active:scale-95'}`}
          >
            {isCommandLoading ? (
               <div className="h-3 w-3 border-2 border-current border-t-transparent rounded-full animate-spin" />
            ) : (
               <div className={`h-2 w-2 rounded-full ${botData.is_running ? 'bg-[#f6465d] animate-pulse' : 'bg-[#0ecb81]'}`} />
            )}
            {isCommandLoading ? 'AGUARDE...' : (botData.is_running ? 'DESLIGAR IA' : 'LIGAR IA')}
          </button>

          <button
            type="button"
            onClick={() => onOpenGovernance?.()}
            title="Governança (MasterConfigForm)"
            className="h-[52px] w-[52px] flex items-center justify-center text-gray-400 hover:text-white transition-colors bg-[#2b3139]/30 hover:bg-[#2b3139] rounded-lg border border-transparent hover:border-[#4b5563] shrink-0"
          >
            <Settings className="h-4 w-4" />
          </button>
        </div>
      </header>

      {/* Nexus HFT V90: palco — flex-col; abas secundárias e painel em fluxo (flex-1 / h-full). */}
      <div className="relative flex flex-col flex-1 w-full h-full min-h-0 overflow-hidden">
        <div
          className={
            (activeTab as string) === "sinais"
              ? "flex-1 w-full h-full flex flex-col min-h-0 min-w-0 overflow-hidden animate-in fade-in duration-300"
              : "hidden"
          }
        >
          <SignalsHistory />
        </div>
        <div
          className={
            (activeTab as string) === "transmissao"
              ? "flex-1 w-full h-full flex flex-col min-h-0 min-w-0 overflow-hidden animate-in fade-in duration-300"
              : "hidden"
          }
        >
          <TransmissionConfig />
        </div>
        <div
          className={
            activeTab === "horizonte"
              ? "flex-1 w-full h-full flex flex-col min-h-0 min-w-0 overflow-y-auto p-4 md:p-6 custom-scrollbar space-y-6 animate-in fade-in duration-300"
              : "hidden"
          }
        >
            {/* Header da Aba */}
            <div className="bg-[#161a1e] p-6 rounded-xl border-t-2 border-t-emerald-500/50 flex flex-col md:flex-row justify-between items-start md:items-center gap-4 shadow-lg border border-[#2b3139]">
              <div>
                <h2 className="text-2xl font-bold text-gray-100 flex items-center gap-3">
                  <Globe className="h-7 w-7 text-emerald-400" />
                  Horizonte Sovereign
                </h2>
                <p className="text-sm text-gray-400 mt-1">Projeção HFT de Longo Prazo e Gestão de Patrimônio Cripto.</p>
              </div>
              
              <div className="bg-[#0b0e11] border border-[#2b3139] px-4 py-3 rounded-lg flex items-center gap-4">
                <div>
                  <div className="text-xs text-gray-500 uppercase tracking-widest">Taxa Diária (Projeção)</div>
                  <div className="text-xl font-mono font-bold text-emerald-400 flex items-center">
                    +{dailyGrowthRate.toFixed(2)}%
                    <TrendingUp className="h-4 w-4 ml-2 opacity-70" />
                  </div>
                </div>
              </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              <div className="lg:col-span-2 bg-[#161a1e] border border-[#2b3139] p-6 md:p-8 rounded-xl shadow-lg relative">
                <div className="flex justify-between items-start mb-8">
                  <div>
                    <h3 className="text-lg font-medium text-gray-300 mb-2">Meta Financeira Master</h3>
                    <div className="flex items-center gap-3">
                      {isEditingTarget ? (
                        <div className="flex items-center gap-2 bg-[#0b0e11] border border-emerald-500/50 rounded-lg px-3 py-2">
                          <span className="text-xl text-emerald-400">$</span>
                          <input 
                            type="number" 
                            value={tempTarget}
                            onChange={(e) => setTempTarget(e.target.value)}
                            className="bg-transparent text-2xl md:text-3xl font-bold text-white outline-none w-40"
                            autoFocus
                            onBlur={() => {
                              setIsEditingTarget(false);
                              setTargetBalance(Number(tempTarget) || 1000000);
                            }}
                            onKeyDown={(e) => {
                              if (e.key === 'Enter') {
                                setIsEditingTarget(false);
                                setTargetBalance(Number(tempTarget) || 1000000);
                              }
                            }}
                          />
                        </div>
                      ) : (
                        <div 
                          className="flex items-center gap-3 cursor-pointer group"
                          onClick={() => !isEditingTarget && setIsEditingTarget(true)}
                        >
                          <span className="text-4xl md:text-5xl font-bold text-transparent bg-clip-text bg-gradient-to-r from-emerald-400 to-cyan-400 transition-all">
                            ${targetBalance.toLocaleString('en-US')}
                          </span>
                          <div className="p-2 bg-[#0b0e11] border border-[#2b3139] rounded-full group-hover:border-emerald-500/50 transition-colors">
                            <Pencil className="h-4 w-4 text-gray-400 group-hover:text-emerald-400" />
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                  
                  <div className="text-right">
                    <div className="text-sm text-gray-500 mb-1">Saldo Protegido Atual</div>
                    <div className="text-2xl font-mono text-gray-200">
                      ${Number(botData.saldo ?? 0).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})}
                    </div>
                  </div>
                </div>

                <div className="mb-8">
                  <div className="flex justify-between text-xs font-mono text-gray-400 mb-2">
                    <span>Progresso do Alvo</span>
                    <span>{Number(botData.saldo ?? 0) && targetBalance ? ((Number(botData.saldo ?? 0) / targetBalance) * 100).toFixed(4) : "0.0000"}%</span>
                  </div>
                  <div className="w-full h-4 bg-[#0b0e11] rounded-full border border-[#2b3139] overflow-hidden">
                    <div 
                      className="h-full bg-gradient-to-r from-cyan-500 to-emerald-500 relative transition-all duration-1000 ease-out"
                      style={{ 
                        width: `${Math.min(100, (Number(botData.saldo ?? 0) && targetBalance ? (Number(botData.saldo ?? 0) / targetBalance) * 100 : 0))}%`,
                        boxShadow: '0 0 10px rgba(16, 185, 129, 0.4)'
                      }}
                    />
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div className="bg-[#0b0e11] border border-[#2b3139] p-4 rounded-lg">
                    <div className="text-xs text-gray-500 uppercase tracking-widest mb-1 flex items-center gap-2">
                      <Clock className="h-3 w-3" /> Dias Estimados
                    </div>
                    <div className="text-3xl font-bold text-gray-200">
                      {estimatedDays !== null && estimatedDays !== Infinity ? `~${estimatedDays.toLocaleString('en-US')}` : '---'}
                    </div>
                  </div>
                  <div className="bg-[#0b0e11] border border-[#2b3139] p-4 rounded-lg">
                    <div className="text-xs text-gray-500 uppercase tracking-widest mb-1 flex items-center gap-2">
                      <Layers className="h-3 w-3" /> Anos Estimados
                    </div>
                    <div className="text-3xl font-bold text-gray-200">
                      {estimatedYears} <span className="text-sm font-normal text-gray-500">anos</span>
                    </div>
                  </div>
                </div>
              </div>

              <div className="space-y-6">
                <div className="bg-[#161a1e] border border-[#2b3139] p-6 rounded-xl shadow-md">
                  <h3 className="text-sm font-bold text-gray-300 mb-4 flex items-center gap-2 border-b border-[#2b3139] pb-2">
                    <Shield className="h-4 w-4 text-[#0ecb81]" />
                    Muralha HFT (Sessão Atual)
                  </h3>
                  <div className="space-y-4">
                    <div>
                      <div className="text-xs text-gray-500 uppercase">Lucro Líquido (PnL)</div>
                      <div className={`text-xl font-mono font-bold ${Number(botData.pnl_liquido ?? 0) >= 0 ? 'text-[#0ecb81]' : 'text-[#f6465d]'}`}>
                        ${Number(botData.pnl_liquido ?? 0).toFixed(2)}
                      </div>
                    </div>
                    <div>
                      <div className="text-xs text-gray-500 uppercase">Taxa de Acerto (Win Rate)</div>
                      <div className="text-xl font-mono font-bold text-gray-200">
                        {botData.total_trades > 0 ? ((botData.total_wins / botData.total_trades) * 100).toFixed(1) : '0.0'}%
                      </div>
                    </div>
                    <div>
                      <div className="text-xs text-gray-500 uppercase">Agressividade (Kelly)</div>
                      <div className="text-lg font-mono text-gray-300">
                        {botData.kelly_multiplier ? `${botData.kelly_multiplier}x` : '1.0x'}
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
        <div
          className={
            activeTab === "terminal"
              ? "flex-1 w-full h-full flex flex-col min-h-0 min-w-0 overflow-hidden bg-[#0b0e11] p-4 md:p-8 custom-scrollbar animate-in fade-in duration-300"
              : "hidden"
          }
        >
            <div className="w-full bg-[#131722] border border-[#2A2E39] rounded-2xl shadow-xl flex flex-col h-full flex-1 overflow-hidden">
               <div className="px-6 py-4 border-b border-[#2A2E39] bg-[#1a1e28]">
                 <h2 className="text-xl font-bold flex items-center gap-2 text-white">Log de Informações</h2>
               </div>
               <div className="flex-1 overflow-y-auto p-4 font-mono text-xs text-gray-400 custom-scrollbar whitespace-pre-wrap h-full" ref={logContainerRef} onScroll={(e) => setAutoScroll(e.currentTarget.scrollHeight - e.currentTarget.scrollTop <= e.currentTarget.clientHeight + 50)}>
                    {logs.map((log: string, idx: number) => (
                      <div key={idx} className={`mb-1 ${log.includes("Erro") || log.includes("❌") ? 'text-[#f6465d]' : log.includes("Sucesso") || log.includes("✅") ? 'text-[#0ecb81]' : ''}`}>
                        {log}
                      </div>
                    ))}
                    {!logs.length && <div className="text-gray-600 animate-pulse">Conectando ao logstream da rede LHN V90 HFT...</div>}
                 </div>
            </div>
         </div>
        <div
          className={
            activeTab === "ia"
              ? "flex-1 w-full h-full flex flex-col min-h-0 min-w-0 overflow-hidden bg-[#0b0e11] p-4 md:p-8 custom-scrollbar animate-in fade-in duration-300"
              : "hidden"
          }
        >
            <div className="w-full bg-[#131722] border border-[#2A2E39] rounded-2xl shadow-xl flex flex-col h-full flex-1 overflow-hidden">
               <div className="px-6 py-4 border-b border-[#2A2E39] bg-[#1a1e28]">
                 <h2 className="text-xl font-bold flex items-center gap-2 text-white">Central IA Nexus</h2>
               </div>
               <div className="flex flex-col h-full bg-[#0b0e11]">
                   <div className="flex-1 overflow-y-auto custom-scrollbar p-5 space-y-4">
                      {nexusChat.map((msg, idx) => (
                        <div key={idx} className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}>
                          <div className={`max-w-[75%] p-3.5 rounded-xl text-[13px] leading-relaxed whitespace-pre-wrap font-mono ${msg.role === "user" ? "bg-[#334155]/60 border border-[#475569] text-white rounded-br-none" : "bg-[#161a1e] border border-[#2b3139] text-gray-200 rounded-bl-none shadow-[0_0_15px_rgba(14,203,129,0.05)] border-l-[#0ecb81]"}`}>
                            {msg.text}
                          </div>
                        </div>
                      ))}
                      {isTyping && <div className="text-[#0ecb81] font-mono text-xs animate-pulse pl-2">Sintetizando resposta no Córtex LLM...</div>}
                   </div>
                   <form onSubmit={async (e) => {
                     e.preventDefault();
                     if (!chatInput.trim() || isTyping) return;
                     const userMsg = chatInput;
                     setChatInput("");
                     setNexusChat(prev => [...prev, { role: "user", text: userMsg }]);
                     setIsTyping(true);
                     try {
                       const res = await fetchWithAuth("/api/chat", {
                         method: "POST",
                         headers: { "Content-Type": "application/json" },
                        timeoutMs: 600_000,
                         body: JSON.stringify({
                           message: userMsg,
                           mensagem: userMsg,
                          contexto: { operacoes: buildChatOperationsContext() },
                         }),
                       });
                       const data = await res.json().catch(() => ({}));
                       const replyText =
                         data.resposta ?? data.reply ?? (typeof data.detail === "string" ? data.detail : null);
                       if (!res.ok) {
                         const errMsg =
                           replyText ||
                           (Array.isArray(data.detail)
                             ? data.detail.map((d: { msg?: string }) => d.msg).filter(Boolean).join(" ")
                             : "") ||
                           `Erro HTTP ${res.status}`;
                         setNexusChat((prev) => [...prev, { role: "nexus", text: String(errMsg) }]);
                       } else {
                         setNexusChat((prev) => [
                           ...prev,
                           { role: "nexus", text: String(replyText ?? "(sem resposta)") },
                         ]);
                       }
                     } catch (err) {
                      setNexusChat(prev => [...prev, { role: "nexus", text: "Erro: falha de conexão/timeout ao consultar o Nexus." }]);
                     } finally { setIsTyping(false); }
                   }} className="p-4 bg-[#161a1e] border-t border-[#2b3139] flex gap-3 shrink-0">
                     <Terminal className="h-5 w-5 mt-2.5 text-[#0ecb81] opacity-70" />
                     <input type="text" value={chatInput} onChange={e => setChatInput(e.target.value)} className="flex-1 bg-[#0b0e11] border border-[#2b3139] rounded px-4 py-2.5 text-[13px] text-white focus:outline-none focus:border-[#0ecb81] transition-colors" placeholder="Envie comandos em linguagem natural..." />
                     <button type="submit" disabled={isTyping} className="bg-[#2b3139] hover:bg-[#3b434f] text-gray-300 hover:text-white px-6 py-2.5 rounded font-bold text-xs uppercase tracking-widest disabled:opacity-50 transition-colors">Processar Interação</button>
                   </form>
                 </div>
            </div>
         </div>
        {/* Painel Central: grid 3 colunas — w-full h-full força preenchimento vertical no palco. */}
        <div
          className={
            ["terminal", "ia", "horizonte", "sinais", "transmissao"].includes(String(activeTab))
              ? "hidden"
              : "w-full h-full grid grid-cols-[280px_minmax(0,1fr)_300px] lg:grid-cols-[300px_minmax(0,1fr)_320px] min-h-0 overflow-hidden"
          }
        >
        
        {/* Coluna 1 — Watchlist HFT (largura da grelha; não flex-shrink) */}
        <div className="flex min-h-0 min-w-0 flex-col overflow-hidden border-r border-[#2b3139] bg-[#131722]">
           <div className="flex h-10 shrink-0 items-center justify-between border-b border-[#2b3139] px-4">
              <span className="font-bold text-[13px] text-gray-300">Watchlist HFT</span>
              <Activity className="w-4 h-4 text-[#0ecb81]" />
           </div>
           
           <div className="min-h-0 flex-1 overflow-x-hidden overflow-y-auto custom-scrollbar">
             <table className="w-full min-w-[260px] table-fixed text-left text-[11px] whitespace-nowrap">
                <thead className="sticky top-0 bg-[#161a1e]/90 backdrop-blur z-10 shadow-sm border-b border-[#2b3139]">
                  <tr className="text-gray-500 font-mono">
                    <th className="py-2.5 px-3 w-[45%] font-normal cursor-pointer hover:text-white select-none transition-colors" onClick={() => handleSort('symbol')}>
                      Par {sortConfig.key === 'symbol' && sortConfig.direction !== 'default' ? (sortConfig.direction === 'asc' ? '↑' : '↓') : ''}
                    </th>
                    <th className="py-2.5 px-3 text-right w-[30%] font-normal cursor-pointer hover:text-white select-none transition-colors" onClick={() => handleSort('price')}>
                      Preço {sortConfig.key === 'price' && sortConfig.direction !== 'default' ? (sortConfig.direction === 'asc' ? '↑' : '↓') : ''}
                    </th>
                    <th className="py-2.5 px-3 text-right w-[25%] opacity-70 font-normal cursor-pointer hover:text-white select-none transition-colors" onClick={() => handleSort('changePercent')}>
                      24h {sortConfig.key === 'changePercent' && sortConfig.direction !== 'default' ? (sortConfig.direction === 'asc' ? '↑' : '↓') : ''}
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {sortedWatchlist.map((d: any) => {
                     const isSel = selectedSymbol === d.symbol;
                     const livePrc = livePrices[d.symbol] ?? d.price;
                     const rawChange = d.changePercent;
                     const isInvalidChange = rawChange == null || isNaN(rawChange) || rawChange === "";
                     const dChange = isInvalidChange ? 0 : Number(rawChange);
                     const reg = dChange > 1.5 ? "Trend UP" : dChange < -1.5 ? "Trend DOWN" : "Lateral";
                     const regColor = reg === "Trend UP" ? "text-emerald-500" : reg === "Trend DOWN" ? "text-rose-500" : "text-gray-500";
                     const pctColor = isInvalidChange ? "text-gray-500" : dChange > 0 ? "text-[#0ecb81]" : dChange < 0 ? "text-[#f6465d]" : "text-gray-500";
                     
                     return (
                        <tr key={d.symbol} onClick={() => setSelectedSymbol(d.symbol)} className={`border-b border-[#2b3139]/30 cursor-pointer h-12 transition-colors ${isSel ? 'bg-[#2b3139] border-l-2 border-l-[#0ecb81]' : 'hover:bg-[#1e2329] border-l-2 border-l-transparent'}`}>
                           <td className="py-1 px-3">
                             <div className="flex items-center gap-2">
                               {CRYPTO_LOGOS[d.symbol] ? (
                                 <img
                                   src={CRYPTO_LOGOS[d.symbol]}
                                   className="w-4 h-4 rounded-full"
                                   alt="logo"
                                   draggable={false}
                                   suppressHydrationWarning={true}
                                 />
                               ) : <div className="w-4 h-4 rounded-full bg-gray-600"></div>}
                               <div className="flex flex-col">
                                 <div className={`font-bold tracking-wide ${isSel ? 'text-white' : 'text-gray-300'}`}>{d.symbol.replace("USDT","")}</div>
                                 <div className={`text-[9px] font-mono leading-none mt-0.5 ${regColor}`}>{reg}</div>
                               </div>
                             </div>
                           </td>
                           <td className={`py-1 px-3 text-right font-mono font-bold tracking-tight ${livePrices[d.symbol] != null && livePrc !== d.price ? 'text-[#f0b90b]' : isSel ? 'text-white' : 'text-gray-300'}`}>
                             {livePrc > 0 ? livePrc.toFixed(livePrc < 10 ? 4 : 2) : "0.00"}
                           </td>
                           <td className={`py-1 px-3 text-right font-mono tracking-tight font-medium ${pctColor}`}>
                             {isInvalidChange ? "-" : `${dChange > 0 ? '+' : ''}${dChange.toFixed(1)}%`}
                           </td>
                        </tr>
                     )
                  })}
                  {sortedWatchlist.length === 0 && (
                     <tr><td colSpan={3} className="text-center py-6 text-gray-600 animate-pulse font-mono tracking-widest text-[10px]">Sincronizando Bybit Websocket...</td></tr>
                  )}
                </tbody>
             </table>
           </div>
        </div>
        
        {/* Coluna 2 — Gráfico + Posições / Histórico / Sessão */}
        <div className="flex min-h-0 min-w-0 flex-col overflow-hidden border-r border-[#2b3139] bg-[#0b0e11]">
          
          {/* Chart Area */}
          <div className="flex-[6.5] flex flex-col relative min-h-0 bg-[#0b0e11]">
             {/* Chart Controls Strip */}
             <div className="h-10 flex items-center px-4 border-b border-[#2b3139] shrink-0 justify-between">
                <div className="flex items-center gap-4 text-gray-400 font-medium font-sans text-[13px]">
                  <span className="text-gray-500 font-bold uppercase text-[10px] tracking-widest">Tempo</span>
                  {['1m', '5m', '15m', '1h', '4h', '1d'].map(tf => (
                    <button key={tf} onClick={() => setTimeframe(tf)} className={`hover:text-white transition-colors ${timeframe === tf ? 'text-[#0ecb81] font-bold border-b border-[#0ecb81] pb-0.5 -mb-0.5' : ''}`}>{tf}</button>
                  ))}
                  
                  <div className="w-px h-4 bg-[#2b3139] mx-2"></div>
                  
                  <div className="relative">
                    <button onClick={() => setShowIndicatorsMenu(!showIndicatorsMenu)} className="text-gray-400 hover:text-white flex items-center gap-1.5 font-medium transition-colors">
                      <Activity className="h-3.5 w-3.5" /> Indicadores <ChevronDown className="h-3 w-3 opacity-70" />
                    </button>
                    {showIndicatorsMenu && (
                      <div className="absolute top-full mt-2 left-0 w-56 bg-[#161a1e] border border-[#2b3139] rounded-md shadow-2xl z-50 overflow-hidden">
                        <button onClick={() => { setEnabledIndicators({...enabledIndicators, sma20: !enabledIndicators.sma20}); setShowIndicatorsMenu(false); }} className="w-full text-left px-4 py-3 text-xs font-mono text-gray-300 hover:bg-[#2b3139] hover:text-white transition-colors border-b border-[#2b3139]/50 flex items-center gap-2">
                          <div className={`w-3 h-3 border rounded-sm flex items-center justify-center ${enabledIndicators.sma20 ? 'bg-[#0ecb81] border-[#0ecb81]' : 'border-gray-500'}`}>{enabledIndicators.sma20 && <Check className="w-2.5 h-2.5 text-black" />}</div>
                          Média Móvel Simples (SMA)
                        </button>
                        <button onClick={() => { setEnabledIndicators({...enabledIndicators, ema9: !enabledIndicators.ema9}); setShowIndicatorsMenu(false); }} className="w-full text-left px-4 py-3 text-xs font-mono text-gray-300 hover:bg-[#2b3139] hover:text-white transition-colors border-b border-[#2b3139]/50 flex items-center gap-2">
                          <div className={`w-3 h-3 border rounded-sm flex items-center justify-center ${enabledIndicators.ema9 ? 'bg-[#0ecb81] border-[#0ecb81]' : 'border-gray-500'}`}>{enabledIndicators.ema9 && <Check className="w-2.5 h-2.5 text-black" />}</div>
                          Média Móvel Exp (EMA9)
                        </button>
                        <button onClick={() => { setEnabledIndicators({...enabledIndicators, bb: !enabledIndicators.bb}); setShowIndicatorsMenu(false); }} className="w-full text-left px-4 py-3 text-xs font-mono text-gray-300 hover:bg-[#2b3139] hover:text-white transition-colors flex items-center gap-2">
                          <div className={`w-3 h-3 border rounded-sm flex items-center justify-center ${enabledIndicators.bb ? 'bg-[#0ecb81] border-[#0ecb81]' : 'border-gray-500'}`}>{enabledIndicators.bb && <Check className="w-2.5 h-2.5 text-black" />}</div>
                          Bandas de Bollinger
                        </button>
                      </div>
                    )}
                  </div>
                </div>

                <div className="flex items-center gap-2">
                   <div className="text-[10px] text-gray-500 font-mono flex items-center gap-1.5 border border-[#2b3139] px-2 py-0.5 rounded bg-[#161a1e]">
                     Motor HFT FastAPI {motorHandshake === "ok" ? <span className="text-[#0ecb81]">Online</span> : <span className="text-[#f6465d]">Offline</span>}
                   </div>
                </div>
             </div>
             
             {/* Nexus HFT V90: córtex visual isolado + ticks via ref (sem setState no candle). */}
             <MemoizedTradingChart
               ref={chartHandleRef}
               symbol={selectedSymbol}
               timeframe={timeframe}
               enabledIndicators={enabledIndicators}
               overlay={chartOverlay}
               watchlistReady={watchlistData.length > 0}
             />
          </div>

          <div className="h-2 border-b border-[#2b3139] bg-[#0b0e11] cursor-ns-resize shrink-0 flex items-center justify-center group">
             <div className="w-10 h-0.5 bg-gray-600 rounded-full group-hover:bg-[#0ecb81] transition-colors"></div>
          </div>

          {/* Bottom Panel (Tabs & Content) */}
          <div className="flex min-h-0 flex-[3.5] flex-col overflow-hidden bg-[#161a1e]">
            <div className="h-10 border-b border-[#2b3139] flex items-center px-4 gap-6 shrink-0 font-medium">
              <button onClick={() => setActiveTab("positions")} className={`h-full border-b-[3px] font-bold text-[13px] tracking-wide transition-colors ${activeTab === "positions" ? "border-[#0ecb81] text-white" : "border-transparent text-gray-400 hover:text-gray-200"}`}>
                Posições Abertas ({botData.operacoes ? botData.operacoes.length : 0})
              </button>
              <button onClick={() => setActiveTab("history")} className={`h-full border-b-[3px] font-bold text-[13px] tracking-wide transition-colors ${activeTab === "history" ? "border-[#0ecb81] text-white" : "border-transparent text-gray-400 hover:text-gray-200"}`}>
                Histórico de Operações
              </button>
              <button onClick={() => setActiveTab("session" as any)} className={`h-full border-b-[3px] font-bold text-[13px] tracking-wide transition-colors ${(activeTab as string) === "session" ? "border-[#f0b90b] text-[#f0b90b]" : "border-transparent text-gray-400 hover:text-gray-200"}`}>
                ⚡ Desempenho da Sessão
              </button>
              
              {/* Spacer */}
              <div className="flex-1"></div>
              
              {/* Zerar Sessão (visível apenas na aba de sessão) */}
              {(activeTab as string) === "session" && (
                <button onClick={zerarSessao} className="mr-2 flex items-center gap-1.5 bg-[#2b3139] hover:bg-[#f6465d]/20 border border-[#f6465d]/40 hover:border-[#f6465d] text-[#f6465d] px-3 py-1 text-[11px] rounded font-bold tracking-widest transition-colors">
                  🗑️ Zerar Sessão
                </button>
              )}
              {/* Checkboxes PnL */}
              {(activeTab as string) !== "session" && (
              <div className="flex items-center gap-4 text-[11px] text-gray-400 font-mono">
                 <label className="flex items-center gap-1.5 cursor-pointer hover:text-white"><input type="checkbox" defaultChecked className="accent-[#0ecb81]" /> Ocultar Outros Pares</label>
                 <label className="flex items-center gap-1.5 cursor-pointer hover:text-white"><input type="checkbox" defaultChecked className="accent-[#0ecb81]" /> Notificações Ticker</label>
              </div>
              )}
            </div>
            
            <div className="flex-1 overflow-y-auto custom-scrollbar p-0 bg-[#0b0e11]">
               {activeTab === "positions" ? (
                  <table className="w-full text-left border-collapse text-xs whitespace-nowrap">
                    <thead className="text-gray-500 font-normal sticky top-0 bg-[#161a1e] shadow-sm z-10">
                      <tr>
                        <th className="font-normal py-2.5 px-4">Contrato</th>
                        <th className="font-normal py-2.5 px-4 text-center">Tipo</th>
                        <th className="font-normal py-2.5 px-4 text-right">Qtd/Margem</th>
                        <th className="font-normal py-2.5 px-4 text-right">Preço de Entrada</th>
                        <th className="font-normal py-2.5 px-4 text-right">Preço da Marcação</th>
                        <th className="font-normal py-2.5 px-4 text-center">Certeza IA</th>
                        <th className="font-normal py-2.5 px-4 text-right">PNL Não Realizado (%)</th>
                        <th className="font-normal py-2.5 px-4 text-right">TP/SL</th>
                        <th className="font-normal py-2.5 px-4 text-center">Ação Pivot</th>
                      </tr>
                    </thead>
                    <tbody className="font-mono">
                      {botData.operacoes && Array.isArray(botData.operacoes) && botData.operacoes.length > 0 ? (
                        botData.operacoes.map((op: any, idx: number) => {
                          const symbol = (op.symbol ?? op.ativo ?? "---").toUpperCase();
                          const entryPrice = Number(op.preco_entrada ?? op.entry_price ?? op.preco ?? 0);
                          const margin = Number(op.margem ?? op.margem_gasta ?? op.margin ?? 0);
                          const leverage = Number(op.alavancagem ?? op.alav ?? 20);
                          const tipo = String(op.tipo ?? op.direcao ?? "---").toUpperCase();
                          const isLong = ["LONG", "COMPRA", "BUY"].includes(tipo);
  
                          const currentPrice = Number(livePrices[symbol] ?? entryPrice);
  
                          let pnlPercent = 0;
                          let pnlUsd = 0;
                          if (entryPrice > 0) {
                            const diff = isLong ? (currentPrice - entryPrice) : (entryPrice - currentPrice);
                            pnlPercent = (diff / entryPrice) * leverage * 100;
                            pnlUsd = (margin * pnlPercent) / 100;
                          }
                          const isProfit = pnlUsd >= 0;
                          
                          const certainty = Number(op.certeza ?? op.ia_prob ?? op.confidence ?? 0);
                          let certaintyColor = "text-gray-500";
                          if (certainty >= 80) certaintyColor = "text-[#0ecb81]";
                          else if (certainty >= 50) certaintyColor = "text-[#F5B041]";
                          else if (certainty > 0) certaintyColor = "text-[#f6465d]";
                          
                          return (
                            <tr key={String(symbol || idx)} className="hover:bg-[#1e2329] transition-colors border-b border-[#2b3139]/30">
                              <td className="py-2.5 px-4">
                                <span className="font-bold text-gray-200 block text-[13px]">{symbol} <span className="text-gray-500 font-normal border border-gray-700 rounded px-1 ml-1 text-[10px]">Perp</span></span>
                                <span className="text-gray-600 text-[10px] mt-0.5 block">{op.hora ?? "--:--"} | {leverage}x Cross</span>
                              </td>
                              <td className="py-2.5 px-4 text-center">
                                <span className={`font-bold tracking-widest ${isLong ? 'text-[#0ecb81]' : 'text-[#f6465d]'}`}>
                                  {isLong ? 'LONG' : 'SHORT'}
                                </span>
                              </td>
                              <td className="py-2.5 px-4 text-right">
                                <span className="text-gray-200 block">${margin.toFixed(2)}</span>
                                <span className="text-gray-600 text-[10px] uppercase font-sans">Margem Inicial</span>
                              </td>
                              <td className="py-2.5 px-4 text-right text-gray-300">
                                {entryPrice > 0 ? entryPrice.toFixed(4) : "0.0000"}
                              </td>
                              <td className={`py-2.5 px-4 text-right font-bold ${livePrices[symbol] != null ? 'text-[#f0b90b]' : 'text-gray-500'}`}>
                                {currentPrice > 0 ? currentPrice.toFixed(4) : "---"}
                              </td>
                              <td className="py-2.5 px-4 text-center">
                                <span className={`font-bold block text-[13px] tracking-widest ${certaintyColor}`}>
                                  {certainty > 0 ? `${certainty.toFixed(1)}%` : "---"}
                                </span>
                              </td>
                              <td className="py-2.5 px-4 text-right">
                                <span className={`font-bold block text-[14px] ${isProfit ? 'text-[#0ecb81]' : 'text-[#f6465d]'}`}>
                                  {isProfit ? '+' : ''}{pnlUsd.toFixed(2)} USDT
                                </span>
                                <span className={`text-[11px] font-sans ${isProfit ? 'text-[#0ecb81]' : 'text-[#f6465d]'}`}>
                                  {isProfit ? '+' : ''}{pnlPercent.toFixed(2)}%
                                </span>
                              </td>
                              <td className="py-2.5 px-4 text-right">
                                <span className="text-gray-400 block">${op.tp ? op.tp.toFixed(4) : "---"}</span>
                                <span className="text-gray-500 text-[10px] block">${op.sl ? op.sl.toFixed(4) : "---"}</span>
                              </td>
                              <td className="py-2.5 px-4 text-center">
                                <button className="bg-[#1e2329] hover:bg-[#2b3139] border border-gray-700 text-gray-300 px-3 py-1 text-[11px] rounded transition-colors" onClick={() => sendCommand("CLOSE_" + symbol)}>
                                  Mercado
                                </button>
                              </td>
                            </tr>
                          );
                        })
                      ) : (
                        <tr>
                          <td colSpan={9} className="py-12 text-center text-gray-500 font-sans">
                            <Layers className="h-8 w-8 mx-auto opacity-20 mb-2" />
                            A interface Bybit não possui posições abertas no momento.
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
               ) : activeTab === "history" ? (
                 <div className="flex flex-col h-full min-h-0 bg-[#0b0e11]">
                   <div className="grid grid-cols-4 gap-3 p-4 border-b border-[#1e293b]/50 shrink-0">
                     <div className="bg-[#161a1e] px-4 py-3 rounded-xl border border-[#2b3139] flex flex-col items-center">
                       <span className="text-gray-500 text-[10px] uppercase tracking-widest font-bold mb-1">Total Trades</span>
                      <span className="text-2xl font-mono font-bold text-white">{totalTradesRealtime || 0}</span>
                     </div>
                     <div className="bg-[#161a1e] px-4 py-3 rounded-xl border border-[#2b3139] flex flex-col items-center">
                       <span className="text-gray-500 text-[10px] uppercase tracking-widest font-bold mb-1">Vitórias / Derrotas</span>
                       <div className="flex items-center gap-2">
                        <span className="text-xl font-mono font-bold text-[#0ecb81]">{totalWinsRealtime || 0}</span>
                         <span className="text-gray-600">/</span>
                        <span className="text-xl font-mono font-bold text-[#f6465d]">{totalLossesRealtime || 0}</span>
                       </div>
                     </div>
                     <div className="bg-[#161a1e] px-4 py-3 rounded-xl border border-[#2b3139] flex flex-col items-center">
                       <span className="text-gray-500 text-[10px] uppercase tracking-widest font-bold mb-1">Lucro Bruto</span>
                      <span className="text-xl font-mono font-bold text-[#0ecb81]">+ US$ {Number(totalProfitRealtime ?? 0).toFixed(2)}</span>
                     </div>
                     <div className="bg-[#161a1e] px-4 py-3 rounded-xl border border-[#2b3139] flex flex-col items-center">
                       <span className="text-gray-500 text-[10px] uppercase tracking-widest font-bold mb-1">Perda Bruta</span>
                      <span className="text-xl font-mono font-bold text-[#f6465d]">- US$ {Number(totalLossRealtime ?? 0).toFixed(2)}</span>
                     </div>
                   </div>
                   <div className="flex-1 overflow-y-auto custom-scrollbar">
                     <table className="w-full text-left border-collapse text-xs whitespace-nowrap">
                       <thead className="sticky top-0 bg-[#161a1e] shadow-sm z-10">
                         <tr className="text-gray-500 font-normal border-b border-[#2b3139]/50">
                           <th className="font-normal py-2.5 px-4">Hora</th>
                           <th className="font-normal py-2.5 px-4">Ativo</th>
                           <th className="font-normal py-2.5 px-4 text-center">Tipo</th>
                           <th className="font-normal py-2.5 px-4 text-right">Margem ($)</th>
                           <th className="font-normal py-2.5 px-4 text-center">Certeza</th>
                           <th className="font-normal py-2.5 px-4">Resultado de Saída</th>
                           <th className="font-normal py-2.5 px-4 text-right">Lucro/Perda</th>
                         </tr>
                       </thead>
                       <tbody className="font-mono">
                        {historicoRealtime && historicoRealtime.length > 0 ? (
                          historicoRealtime.map((trade: any, idx: number) => (
                            <tr key={String(trade?.ts ?? `${trade?.hora ?? ''}-${trade?.ativo ?? ''}-${trade?.resultado ?? ''}-${idx}`)} className="border-b border-[#2b3139]/30 hover:bg-[#1e2329] transition-colors group">
                               <td className="py-3 px-4 text-gray-500">{trade.hora}</td>
                               <td className="py-3 px-4 font-bold text-gray-200">{trade.ativo}</td>
                               <td className="py-3 px-4 text-center">
                                 <span className={`text-[10px] border px-1.5 py-0.5 rounded ${trade.tipo === 'LONG' ? 'border-[#0ecb81] text-[#0ecb81]' : 'border-[#f6465d] text-[#f6465d]'}`}>
                                   {trade.tipo}
                                 </span>
                               </td>
                               <td className="py-3 px-4 text-right text-gray-400">
                                 ${trade.margem_gasta ? trade.margem_gasta.toFixed(2) : "0.00"}
                                 <span className="opacity-50 text-[9px] ml-1">({trade.alavancagem ? trade.alavancagem : 1}x)</span>
                               </td>
                               <td className="py-3 px-4 text-center text-gray-500">
                                 {trade.certeza ? `${trade.certeza.toFixed(1)}%` : '--'}
                               </td>
                               <td className="py-3 px-4 text-gray-400">{trade.resultado}</td>
                              <td className={`py-3 px-4 font-bold text-right ${Number(trade.pnl ?? trade.profit ?? trade.lucro ?? 0) > 0 ? 'text-[#0ecb81]' : Number(trade.pnl ?? trade.profit ?? trade.lucro ?? 0) < 0 ? 'text-[#f6465d]' : 'text-gray-400'}`}>
                                {Number(trade.pnl ?? trade.profit ?? trade.lucro ?? 0) > 0 ? '+' : ''}{Number(trade.pnl ?? trade.profit ?? trade.lucro ?? 0).toFixed(2)} USDT
                                <span className="block text-[10px] opacity-70">
                                  {Number(trade.pnl_pct ?? 0) > 0 ? '+' : ''}{Number(trade.pnl_pct ?? 0).toFixed(2)}%
                                </span>
                               </td>
                             </tr>
                           ))
                         ) : (
                           <tr>
                             <td colSpan={7} className="py-12 text-center text-gray-500 font-sans">
                               Nenhum registro de histórico disponível ainda.
                             </td>
                           </tr>
                         )}
                       </tbody>
                     </table>
                   </div>
                 </div>
               ) : (activeTab as string) === "session" ? (
                  <div className="w-full h-full flex flex-col gap-0 bg-[#0b0e11] p-5">
                    <div className="grid grid-cols-3 gap-4 mb-5">
                      <div className="bg-[#161a1e] border border-[#2b3139] rounded-2xl p-5 flex flex-col gap-2 shadow-lg hover:border-[#0ecb81]/40 transition-colors">
                        <span className="text-gray-500 text-[10px] uppercase tracking-[0.18em] font-bold">PNL Total da Sessão</span>
                        <span className={`text-3xl font-mono font-black tracking-tight ${sessionPnl >= 0 ? 'text-[#0ecb81]' : 'text-[#f6465d]'}`}>
                          {sessionPnl >= 0 ? '+' : ''}{sessionPnl.toFixed(2)}
                          <span className="text-[14px] font-normal ml-1 text-gray-500">USDT</span>
                        </span>
                        <span className="text-[11px] text-gray-600">{sessionTradesCount} operações nesta sessão</span>
                        <div className="w-full h-0.5 bg-gradient-to-r from-transparent via-[#0ecb81]/20 to-transparent mt-1 rounded-full"></div>
                      </div>
                      <div className="bg-[#161a1e] border border-[#2b3139] rounded-2xl p-5 flex flex-col gap-2 shadow-lg hover:border-[#f0b90b]/40 transition-colors">
                        <span className="text-gray-500 text-[10px] uppercase tracking-[0.18em] font-bold">Taxa de Acerto</span>
                        <span className={`text-3xl font-mono font-black tracking-tight ${sessionWinrate >= 50 ? 'text-[#0ecb81]' : sessionTradesCount > 0 ? 'text-[#f6465d]' : 'text-gray-600'}`}>
                          {sessionTradesCount > 0 ? sessionWinrate.toFixed(1) : '--'}<span className="text-[14px] font-normal ml-1 text-gray-500">%</span>
                        </span>
                        <div className="flex items-center gap-3">
                          <span className="flex items-center gap-1 text-[12px] font-bold text-[#0ecb81]"><span className="w-2 h-2 rounded-full bg-[#0ecb81] inline-block"></span>{sessionWins} Wins</span>
                          <span className="text-gray-700">/</span>
                          <span className="flex items-center gap-1 text-[12px] font-bold text-[#f6465d]"><span className="w-2 h-2 rounded-full bg-[#f6465d] inline-block"></span>{sessionLosses} Losses</span>
                        </div>
                        <div className="w-full h-1.5 bg-[#2b3139] rounded-full overflow-hidden">
                          <div className="h-full bg-gradient-to-r from-[#0ecb81] to-[#089b5f] rounded-full transition-all duration-700" style={{ width: `${sessionTradesCount > 0 ? Math.min(100, sessionWinrate) : 0}%` }}></div>
                        </div>
                      </div>
                      <div className="bg-[#161a1e] border border-[#2b3139] rounded-2xl p-5 flex flex-col gap-2 shadow-lg hover:border-[#b39ddb]/40 transition-colors">
                        <span className="text-gray-500 text-[10px] uppercase tracking-[0.18em] font-bold">Certeza Média da IA</span>
                        <span className={`text-3xl font-mono font-black tracking-tight ${sessionAvgCertainty >= 80 ? 'text-[#0ecb81]' : sessionAvgCertainty >= 50 ? 'text-[#f0b90b]' : sessionTrades.length > 0 ? 'text-[#f6465d]' : 'text-gray-600'}`}>
                          {sessionTrades.length > 0 ? sessionAvgCertainty.toFixed(1) : '--'}<span className="text-[14px] font-normal ml-1 text-gray-500">%</span>
                        </span>
                        <span className="text-[11px] text-gray-600">Média das entradas desta sessão</span>
                        <div className="w-full h-1.5 bg-[#2b3139] rounded-full overflow-hidden">
                          <div className="h-full bg-gradient-to-r from-[#b39ddb] to-[#7c4dff] rounded-full transition-all duration-700" style={{ width: `${sessionTrades.length > 0 ? Math.min(100, sessionAvgCertainty) : 0}%` }}></div>
                        </div>
                      </div>
                    </div>
                    {!useConsFin && sessionTrades.length === 0 && (
                      <div className="flex flex-col items-center justify-center flex-1 text-gray-600 gap-3 py-8">
                        <Activity className="w-10 h-10 opacity-20" />
                        <span className="text-sm font-sans">Nenhuma operação registada nesta sessão.</span>
                        <span className="text-xs text-gray-700 font-mono">Os dados acumulam automaticamente via WebSocket.</span>
                      </div>
                    )}
                  </div>
               ) : null}
            </div>
          </div>
        </div>

        {/* Coluna 3 — Execução / margem (largura da grelha) */}
        <div className="flex min-h-0 min-w-0 flex-col overflow-hidden border-l border-[#2b3139] bg-[#131722]">
           {/* Margin Mode / Leverage Strip — h-10 alinhado às barras Watchlist / Gráfico */}
           <div className="flex h-10 shrink-0 items-center justify-between border-b border-[#2b3139]/50 bg-[#2b3139]/20 px-4">
              <div className="flex items-center gap-2">
                <div className="bg-[#2b3139] rounded px-3 py-1 text-xs font-semibold text-gray-400 select-none border border-[#3b434f]">Margem Isolada</div>
                <div className="bg-[#2b3139] rounded px-3 py-1 text-xs font-semibold text-[#00d2ff] select-none border border-[#3b434f]">{alavancagemExibicao}x</div>
              </div>
           </div>

           <div className="p-4 flex-1 flex flex-col overflow-y-auto custom-scrollbar">
             <div className="flex justify-center mb-3">
                <span className="bg-[#0ecb81]/10 text-[#0ecb81] px-3 py-1.5 rounded-md text-[10px] uppercase font-bold tracking-widest border border-[#0ecb81]/30 shadow-[0_0_10px_rgba(14,203,129,0.1)]">
                  Execução a Mercado
                </span>
             </div>

             {/* Inputs */}
             <div className="flex flex-col gap-2.5">
                 <div className="flex justify-between items-center bg-[#1e2329] border border-[#2b3139] rounded-lg px-3 py-2 shadow-inner">
                  <span className="text-gray-400 text-xs font-medium shrink-0">Preço Atual</span>
                  <div className="flex items-center gap-1">
                    <span className="text-white font-mono font-bold text-base tracking-wide">
                      {(livePrices[selectedSymbol] ?? pricesRef.current[selectedSymbol]) != null
                        ? Number(livePrices[selectedSymbol] ?? pricesRef.current[selectedSymbol]).toFixed(4)
                        : "0.0000"}
                    </span>
                    <span className="text-gray-500 text-[10px] font-bold">USDT</span>
                  </div>
                </div>

                 <div className="flex justify-between items-center bg-[#1e2329] border border-[#2b3139] rounded-lg px-3 py-2 shadow-inner">
                  <span className="text-gray-400 text-xs font-medium shrink-0">Margem / Ordem</span>
                  <div className="flex items-center gap-1">
                    <span className="text-white font-mono font-bold text-base tracking-wide">{margemOrdemExibicao.toFixed(2)}</span>
                    <span className="text-gray-500 text-[10px] font-bold">USDT</span>
                  </div>
                </div>

                {/* Context Info */}
                 <div className="bg-[#161a1e] border border-[#2b3139] p-3 rounded-lg text-xs mt-1.5 mb-1.5 flex flex-col gap-2 shadow-sm">
                   <div className="flex justify-between items-center">
                     <span className="text-gray-500 font-medium tracking-wide">Valor com Alav.</span>
                     <span className="text-gray-200 font-mono font-bold">{valorNotionalExibicao.toFixed(2)} <span className="text-gray-500 text-[10px]">USDT</span></span>
                   </div>
                   <div className="flex justify-between items-center">
                     <span className="text-gray-500 font-medium tracking-wide" title="Saldo disponível após margens alocadas (WS)">Custo (Livre)</span>
                     <span className="text-gray-200 font-mono font-bold">{saldoLivreExibicao.toFixed(2)} <span className="text-gray-500 text-[10px]">USDT</span></span>
                   </div>
                   <div className="flex justify-between items-center pt-2 border-t border-[#2b3139]">
                     <span className="text-gray-500 font-medium tracking-wide">Ordens HFT</span>
                     <span className={`font-bold font-mono px-2 py-0.5 rounded text-[10px] uppercase tracking-wider ${botData.is_running ? 'bg-[#0ecb81]/10 text-[#0ecb81] border border-[#0ecb81]/20' : 'bg-[#f6465d]/10 text-[#f6465d] border border-[#f6465d]/20'}`}>
                       {botData.is_running ? 'Ativas' : 'Suspensas'}
                     </span>
                   </div>
                </div>

                 <div className="mt-auto pt-2 flex flex-col gap-3">
                  <div className="rounded-lg border border-[#2b3139] bg-[#161a1e] px-3 py-3 text-[11px] text-gray-400 leading-relaxed">
                    Ordens LONG/SHORT são abertas pelo <span className="text-gray-200 font-semibold">motor IA</span>. Use <span className="text-[#0ecb81] font-semibold">Ligar IA</span> no topo; ajuste margem em Configurações.
                  </div>

                  {/* ENVIRONMENT SELECTOR (Área 1) */}
                  <div className="border-t border-[#2b3139] pt-4 mt-2">
                    <div className="bg-[#161a1e] border border-[#2b3139] p-3 -mx-1 rounded-xl flex items-center justify-between shadow-sm cursor-pointer hover:bg-[#1a1f24] hover:border-[#3b434f] transition-all"
                         onClick={async () => {
                           if (!apiConfig) return;
                           setIsBalanceSyncing(true);
                           const newVal = !apiConfig.modo_real;
                           setApiConfig({...apiConfig, modo_real: newVal});
                           await updateConfig({ modo_real: newVal } as ConfigModel);
                           setTimeout(() => setIsBalanceSyncing(false), 2000); // UI visual timeout
                         }}
                    >
                      <div className="flex items-center gap-3">
                        <div className={`p-2 rounded-lg transition-colors ${apiConfig?.modo_real ? 'bg-[#f6465d]/10 text-[#f6465d]' : 'bg-[#00d2ff]/10 text-[#00d2ff]'}`}>
                          {apiConfig?.modo_real ? <ShieldAlert className="w-5 h-5" /> : <Activity className="w-5 h-5" />}
                        </div>
                        <div className="flex flex-col">
                          <span className="text-[10px] text-gray-500 font-bold uppercase tracking-widest leading-none mb-1">Ambiente de Operação</span>
                          <span className={`text-[13px] font-bold tracking-wide transition-colors ${apiConfig?.modo_real ? 'text-[#f6465d]' : 'text-[#00d2ff]'}`}>
                            {apiConfig?.modo_real ? 'CONTA REAL' : 'SIMULAÇÃO'}
                          </span>
                        </div>
                      </div>
                      
                      {/* Toggle Switch UI */}
                      <div className={`w-12 h-6 rounded-full p-1 transition-colors duration-300 ${apiConfig?.modo_real ? 'bg-[#f6465d]' : 'bg-[#1E222D] border border-[#2b3139] shadow-inner'}`}>
                        <div className={`bg-white w-4 h-4 rounded-full shadow-md transition-transform duration-300 ${apiConfig?.modo_real ? 'translate-x-6' : 'translate-x-0'}`} />
                      </div>
                    </div>
                  </div>

                     {/* SNAPSHOT NEURAL BUTTON */}
                     <button
                       onClick={triggerSnapshot}
                       disabled={snapshotLoading}
                       className="w-full mt-3 flex items-center justify-center gap-2.5 bg-[#161a1e] hover:bg-[#1a1f24] border border-[#2b3139] hover:border-[#4a90e2]/60 text-gray-300 hover:text-[#4a90e2] py-3 rounded-xl transition-all duration-200 font-bold text-[12px] uppercase tracking-widest shadow-sm active:scale-[0.98] disabled:opacity-60 disabled:cursor-not-allowed group"
                     >
                       {snapshotLoading ? (
                         <>
                           <div className="w-4 h-4 border-2 border-[#4a90e2]/30 border-t-[#4a90e2] rounded-full animate-spin" />
                           <span className="text-[#4a90e2]">Salvando Estado...</span>
                         </>
                       ) : (
                         <>
                           <Database className="w-4 h-4 group-hover:text-[#4a90e2] transition-colors" />
                           <span>Snapshot Neural</span>
                         </>
                       )}
                     </button>
                     {snapshotToast && (
                       <div className={`mt-2 px-3 py-2.5 rounded-lg text-[11px] font-mono font-bold flex items-center gap-2 transition-all ${snapshotToast.type === "success" ? "bg-[#0ecb81]/10 border border-[#0ecb81]/30 text-[#0ecb81]" : "bg-[#f6465d]/10 border border-[#f6465d]/30 text-[#f6465d]"}`}>
                         <span>{snapshotToast.type === "success" ? "" : ""}</span>
                         <span className="leading-snug">{snapshotToast.msg}</span>
                       </div>
                     )}

                </div>
             </div>
          </div>
        </div>
      </div>
    </div>
  );
}
