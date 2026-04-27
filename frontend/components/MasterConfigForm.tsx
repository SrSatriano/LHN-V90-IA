"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import {
  AlertCircle,
  Brain,
  CheckCircle2,
  ChevronDown,
  Database,
  Eye,
  EyeOff,
  KeyRound,
  Loader2,
  Radar,
  Scale,
  Shield,
} from "lucide-react";
import {
  getGeneralConfig,
  pickConfigForPost,
  updateConfig,
  type ConfigModel,
} from "@/lib/tradingBotService";
import { fetchWithAuth } from "@/lib/lhnAuth";

type ConfigTab = "motor" | "cortex" | "sensors" | "arb" | "memory";

const NUM_INPUT =
  "mt-1 w-full max-w-md rounded-md border border-gray-700 bg-[#0f172a] px-3 py-2.5 text-sm text-white shadow-inner transition-colors placeholder:text-gray-600 focus:border-cyan-500 focus:outline-none focus:ring-2 focus:ring-cyan-500/25 disabled:cursor-not-allowed disabled:opacity-50";

function ToggleSwitch({
  checked,
  onChange,
  disabled,
  id,
}: {
  checked: boolean;
  onChange: (v: boolean) => void;
  disabled?: boolean;
  id?: string;
}) {
  return (
    <button
      id={id}
      type="button"
      role="switch"
      aria-checked={checked}
      disabled={disabled}
      onClick={() => onChange(!checked)}
      className={`relative inline-flex h-8 w-14 shrink-0 cursor-pointer items-center rounded-full border border-white/5 transition-colors duration-200 ease-out focus:outline-none focus-visible:ring-2 focus-visible:ring-cyan-400/80 disabled:cursor-not-allowed disabled:opacity-50 ${
        checked
          ? "bg-gradient-to-r from-cyan-600 to-emerald-600 shadow-[0_0_20px_rgba(6,182,212,0.25)]"
          : "bg-gray-800"
      }`}
    >
      <span
        className={`pointer-events-none inline-block h-6 w-6 transform rounded-full bg-white shadow-md ring-0 transition duration-200 ease-out ${
          checked ? "translate-x-7" : "translate-x-1"
        }`}
      />
    </button>
  );
}

function FieldBlock({
  label,
  hintKey,
  description,
  children,
}: {
  label: string;
  hintKey?: string;
  description?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-4 shadow-sm backdrop-blur-sm">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div className="min-w-0 flex-1 space-y-1">
          <div className="text-sm font-medium text-white">{label}</div>
          {hintKey ? (
            <code className="text-xs text-gray-500">{hintKey}</code>
          ) : null}
          {description ? (
            <p className="text-xs leading-relaxed text-gray-400">{description}</p>
          ) : null}
        </div>
        <div className="flex shrink-0 items-center justify-end sm:pl-4">{children}</div>
      </div>
    </div>
  );
}

/** Nexus HFT V90: acordeão para densidade de config sem sub-tabs. */
function AccordionSection({
  title,
  defaultOpen,
  children,
}: {
  title: string;
  defaultOpen?: boolean;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(Boolean(defaultOpen));
  return (
    <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/30 shadow-sm">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center justify-between gap-2 px-4 py-3 text-left text-sm font-semibold text-white transition hover:bg-gray-800/40"
      >
        <span>{title}</span>
        <ChevronDown
          className={`h-4 w-4 shrink-0 text-cyan-400/90 transition-transform ${open ? "rotate-180" : ""}`}
        />
      </button>
      {open ? (
        <div className="space-y-3 border-t border-gray-800/80 px-4 py-4">{children}</div>
      ) : null}
    </div>
  );
}

const TABS: {
  id: ConfigTab;
  label: string;
  short: string;
  icon: React.ElementType;
}[] = [
  { id: "motor", label: "🛡️ Risco & Motor", short: "Motor", icon: Shield },
  { id: "cortex", label: "🧠 Córtex Neural (65D)", short: "65D", icon: Brain },
  { id: "sensors", label: "🐋 Sensores & Spoofing", short: "HFT", icon: Radar },
  { id: "arb", label: "⚖️ Arbitragem (Pairs)", short: "Pairs", icon: Scale },
  { id: "memory", label: "💾 Deep Memory", short: "Cofre", icon: Database },
];

export function MasterConfigForm() {
  const [activeConfigTab, setActiveConfigTab] = useState<ConfigTab>("motor");
  const [config, setConfig] = useState<Partial<ConfigModel>>({});
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [saveMessage, setSaveMessage] = useState<string | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);
  // Nexus HFT V90: credenciais isoladas (modal), fora das abas de toggles numéricos.
  const [credentialsOpen, setCredentialsOpen] = useState(false);
  const [showApiSecretFields, setShowApiSecretFields] = useState(false);
  const [showTgTok, setShowTgTok] = useState(false);
  const [showTgChat, setShowTgChat] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setLoadError(null);
    try {
      const data = await getGeneralConfig();
      setConfig(data as ConfigModel);
    } catch (e) {
      setLoadError(
        e instanceof Error ? e.message : "Falha ao carregar /api/config."
      );
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const setBool = useCallback((key: keyof ConfigModel, v: boolean) => {
    setConfig((c) => ({ ...c, [key]: v }));
  }, []);

  const setNum = useCallback((key: keyof ConfigModel, raw: string) => {
    const t = raw.trim().replace(",", ".");
    if (t === "" || t === "-" || t === "." || t === "-.") {
      setConfig((c) => ({ ...c, [key]: undefined }));
      return;
    }
    const n = Number(t);
    setConfig((c) => ({ ...c, [key]: Number.isFinite(n) ? n : c[key] }));
  }, []);

  const setStr = useCallback((key: keyof ConfigModel, v: string) => {
    setConfig((c) => ({ ...c, [key]: v as never }));
  }, []);

  const validateForSave = useCallback((): string | null => {
    const rt = config.risk_threshold;
    if (rt !== undefined && (!Number.isFinite(rt) || rt <= 0 || rt > 1)) {
      return "risk_threshold inválido: use (0, 1], ex.: 0.02 = 2%.";
    }
    const se = config.spoof_evaporation_pct;
    if (
      se !== undefined &&
      (!Number.isFinite(se) || se < 0.05 || se > 0.95)
    ) {
      return "spoof_evaporation_pct deve estar entre 0.05 e 0.95.";
    }
    const eps = config.spoof_price_flat_eps_pct;
    if (eps !== undefined && (!Number.isFinite(eps) || eps < 0 || eps > 5)) {
      return "spoof_price_flat_eps_pct inválido (sugestão 0–1).";
    }
    const ze = config.arb_zscore_entry;
    if (ze !== undefined && (!Number.isFinite(ze) || ze < 0.5 || ze > 6)) {
      return "arb_zscore_entry fora do intervalo seguro (0.5–6).";
    }
    const zx = config.arb_zscore_exit;
    if (zx !== undefined && (!Number.isFinite(zx) || zx < 0.05 || zx > 2)) {
      return "arb_zscore_exit fora do intervalo (0.05–2).";
    }
    const loop = config.arb_loop_interval_sec;
    if (loop !== undefined && (!Number.isFinite(loop) || loop < 1 || loop > 120)) {
      return "arb_loop_interval_sec deve estar entre 1 e 120 s.";
    }
    const gb = config.cofre_quota_gb;
    if (gb !== undefined && (!Number.isFinite(gb) || gb < 1 || gb > 500)) {
      return "cofre_quota_gb inválido (1–500 GB).";
    }
    const worm = config.cofre_worm_profit_usd;
    if (worm !== undefined && (!Number.isFinite(worm) || worm < 0)) {
      return "cofre_worm_profit_usd deve ser ≥ 0.";
    }
    return null;
  }, [config]);

  const handleSave = async (): Promise<boolean> => {
    setSaving(true);
    setSaveMessage(null);
    setSaveError(null);
    try {
      const err = validateForSave();
      if (err) {
        setSaveError(err);
        return false;
      }
      const fresh = await getGeneralConfig();
      const merged: Record<string, unknown> = {
        ...(fresh as unknown as Record<string, unknown>),
      };
      for (const [k, v] of Object.entries(config)) {
        if (v !== undefined) {
          merged[k] = v;
        }
      }
      if (merged.api_secret === "***MASCARADO***") {
        delete merged.api_secret;
      }
      const payload = pickConfigForPost(merged);
      const res = await updateConfig(payload as ConfigModel);
      if (res.status !== "success") {
        throw new Error(res.detail || res.trace || "Resposta inesperada do servidor.");
      }
      setSaveMessage("Configuração aplicada e persistida.");
      await load();
      return true;
    } catch (e) {
      setSaveError(
        e instanceof Error ? e.message : "Erro ao gravar configuração."
      );
      return false;
    } finally {
      setSaving(false);
    }
  };

  const tabPanel = useMemo(() => {
    if (loading || loadError) return null;

    switch (activeConfigTab) {
      case "motor":
        return (
          <div className="w-full max-w-3xl space-y-3">
            <AccordionSection title="Governança base" defaultOpen>
              <FieldBlock
                label="Trading ativo"
                hintKey="trading_enabled"
                description="Quando desligado, o motor não abre novas operações."
              >
                <ToggleSwitch
                  checked={Boolean(config.trading_enabled ?? true)}
                  disabled={saving}
                  onChange={(v) => setBool("trading_enabled", v)}
                />
              </FieldBlock>
              <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-4">
                <label htmlFor="risk_threshold" className="text-sm font-medium text-white">
                  Limite de risco (fração do saldo vs. perda ao SL)
                </label>
                <code className="mt-0.5 block text-xs text-gray-500">risk_threshold</code>
                <input
                  id="risk_threshold"
                  type="text"
                  inputMode="decimal"
                  className={NUM_INPUT}
                  disabled={saving}
                  value={
                    config.risk_threshold !== undefined &&
                    Number.isFinite(config.risk_threshold)
                      ? String(config.risk_threshold)
                      : ""
                  }
                  onChange={(e) => setNum("risk_threshold", e.target.value)}
                  placeholder="0.02"
                />
              </div>
            </AccordionSection>
            <AccordionSection title="Execução & risco">
              <FieldBlock label="Conta real" hintKey="modo_real" description="Exige chaves válidas (credenciais no cofre isolado).">
                <ToggleSwitch
                  checked={Boolean(config.modo_real)}
                  disabled={saving}
                  onChange={(v) => setBool("modo_real", v)}
                />
              </FieldBlock>
              <div className="grid gap-3 sm:grid-cols-2">
                <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3">
                  <label className="text-xs text-gray-400">margem_entrada</label>
                  <input type="text" inputMode="decimal" className={NUM_INPUT} disabled={saving} value={config.margem_entrada !== undefined && Number.isFinite(config.margem_entrada) ? String(config.margem_entrada) : ""} onChange={(e) => setNum("margem_entrada", e.target.value)} />
                </div>
                <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3">
                  <label className="text-xs text-gray-400">alavancagem</label>
                  <input type="text" inputMode="numeric" className={NUM_INPUT} disabled={saving} value={config.alavancagem !== undefined && Number.isFinite(config.alavancagem) ? String(config.alavancagem) : ""} onChange={(e) => setNum("alavancagem", e.target.value)} />
                </div>
                <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3">
                  <label className="text-xs text-gray-400">winrate_minimo (%)</label>
                  <input type="text" inputMode="decimal" className={NUM_INPUT} disabled={saving} value={config.winrate_minimo !== undefined && Number.isFinite(config.winrate_minimo) ? String(config.winrate_minimo) : ""} onChange={(e) => setNum("winrate_minimo", e.target.value)} />
                </div>
                <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3">
                  <label className="text-xs text-gray-400">saldo_simulacao_inicial</label>
                  <input type="text" inputMode="decimal" className={NUM_INPUT} disabled={saving} value={config.saldo_simulacao_inicial !== undefined && Number.isFinite(Number(config.saldo_simulacao_inicial)) ? String(config.saldo_simulacao_inicial) : ""} onChange={(e) => setNum("saldo_simulacao_inicial", e.target.value)} />
                </div>
                <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3 sm:col-span-2">
                  <label className="text-xs text-gray-400">tipo_execucao</label>
                  <div className="mt-2 flex gap-4">
                    <label className="flex cursor-pointer items-center gap-2 text-sm text-gray-300">
                      <input type="radio" name="tipo_exec_m" checked={(config.tipo_execucao || "TAKER") === "TAKER"} onChange={() => setStr("tipo_execucao", "TAKER")} className="accent-cyan-500" disabled={saving} />
                      TAKER
                    </label>
                    <label className="flex cursor-pointer items-center gap-2 text-sm text-gray-300">
                      <input type="radio" name="tipo_exec_m" checked={config.tipo_execucao === "MAKER"} onChange={() => setStr("tipo_execucao", "MAKER")} className="accent-cyan-500" disabled={saving} />
                      MAKER
                    </label>
                  </div>
                </div>
                <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3 sm:col-span-2">
                  <label className="text-xs text-gray-400">hard_sl_pct</label>
                  <input type="text" inputMode="decimal" className={NUM_INPUT} disabled={saving} value={config.hard_sl_pct !== undefined && Number.isFinite(config.hard_sl_pct) ? String(config.hard_sl_pct) : ""} onChange={(e) => setNum("hard_sl_pct", e.target.value)} />
                </div>
              </div>
            </AccordionSection>
            <AccordionSection title="Filtros de mercado">
              <div className="space-y-2">
                <FieldBlock label="Filtro funding" hintKey="use_funding_filter"><ToggleSwitch checked={Boolean(config.use_funding_filter)} disabled={saving} onChange={(v) => setBool("use_funding_filter", v)} /></FieldBlock>
                <FieldBlock label="Filtro open interest" hintKey="use_oi_filter"><ToggleSwitch checked={Boolean(config.use_oi_filter)} disabled={saving} onChange={(v) => setBool("use_oi_filter", v)} /></FieldBlock>
                <FieldBlock label="Filtro regime" hintKey="use_regime_filter"><ToggleSwitch checked={Boolean(config.use_regime_filter)} disabled={saving} onChange={(v) => setBool("use_regime_filter", v)} /></FieldBlock>
              </div>
              <div className="mt-3 grid gap-3 sm:grid-cols-2">
                <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3">
                  <label className="text-xs text-gray-400">engine_param_1</label>
                  <input type="text" className={NUM_INPUT} disabled={saving} value={config.engine_param_1 !== undefined && Number.isFinite(config.engine_param_1) ? String(config.engine_param_1) : ""} onChange={(e) => setNum("engine_param_1", e.target.value)} />
                </div>
                <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3">
                  <label className="text-xs text-gray-400">engine_param_2</label>
                  <input type="text" className={NUM_INPUT} disabled={saving} value={config.engine_param_2 !== undefined && Number.isFinite(config.engine_param_2) ? String(config.engine_param_2) : ""} onChange={(e) => setNum("engine_param_2", e.target.value)} />
                </div>
              </div>
            </AccordionSection>
            <AccordionSection title="Trailing & escala">
              <div className="space-y-2">
                <FieldBlock label="Scale-out" hintKey="use_scale_out"><ToggleSwitch checked={Boolean(config.use_scale_out)} disabled={saving} onChange={(v) => setBool("use_scale_out", v)} /></FieldBlock>
                <FieldBlock label="Break-even" hintKey="use_breakeven"><ToggleSwitch checked={Boolean(config.use_breakeven)} disabled={saving} onChange={(v) => setBool("use_breakeven", v)} /></FieldBlock>
                <FieldBlock label="Smart exit" hintKey="smart_exit"><ToggleSwitch checked={Boolean(config.smart_exit)} disabled={saving} onChange={(v) => setBool("smart_exit", v)} /></FieldBlock>
                <FieldBlock label="Trailing stop" hintKey="use_trailing_stop"><ToggleSwitch checked={Boolean(config.use_trailing_stop)} disabled={saving} onChange={(v) => setBool("use_trailing_stop", v)} /></FieldBlock>
                <FieldBlock label="Trailing TP" hintKey="use_trailing_tp"><ToggleSwitch checked={Boolean(config.use_trailing_tp)} disabled={saving} onChange={(v) => setBool("use_trailing_tp", v)} /></FieldBlock>
                <FieldBlock label="Escudo BTC" hintKey="escudo_btc"><ToggleSwitch checked={Boolean(config.escudo_btc)} disabled={saving} onChange={(v) => setBool("escudo_btc", v)} /></FieldBlock>
              </div>
              <div className="mt-3 grid gap-3 sm:grid-cols-2">
                {(["scale_out_pct", "alvo_lucro_base", "breakeven_trigger", "trailing_tp_activation", "trailing_tp_callback", "trailing_fee_buffer_pct", "trailing_min_profit_lock_pct"] as const).map((k) => (
                  <div key={k} className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3">
                    <label className="text-xs text-gray-400">{k}</label>
                    <input type="text" inputMode="decimal" className={NUM_INPUT} disabled={saving} value={config[k] !== undefined && Number.isFinite(Number(config[k])) ? String(config[k]) : ""} onChange={(e) => setNum(k, e.target.value)} />
                  </div>
                ))}
              </div>
            </AccordionSection>
            <AccordionSection title="Motor HFT / túnel">
              <FieldBlock label="Motor assíncrono" hintKey="use_async_engine"><ToggleSwitch checked={Boolean(config.use_async_engine)} disabled={saving} onChange={(v) => setBool("use_async_engine", v)} /></FieldBlock>
              <FieldBlock label="WebSocket mercado" hintKey="use_ws"><ToggleSwitch checked={Boolean(config.use_ws)} disabled={saving} onChange={(v) => setBool("use_ws", v)} /></FieldBlock>
              <FieldBlock label="Order book L2" hintKey="use_l2_depth"><ToggleSwitch checked={Boolean(config.use_l2_depth)} disabled={saving} onChange={(v) => setBool("use_l2_depth", v)} /></FieldBlock>
              <FieldBlock label="Backtest nativo" hintKey="use_backtest"><ToggleSwitch checked={Boolean(config.use_backtest)} disabled={saving} onChange={(v) => setBool("use_backtest", v)} /></FieldBlock>
              <FieldBlock label="Binance Vision / data lake" hintKey="use_binance_vision"><ToggleSwitch checked={Boolean(config.use_binance_vision)} disabled={saving} onChange={(v) => setBool("use_binance_vision", v)} /></FieldBlock>
              <FieldBlock label="WS ordens" hintKey="use_ws_orders"><ToggleSwitch checked={Boolean(config.use_ws_orders)} disabled={saving} onChange={(v) => setBool("use_ws_orders", v)} /></FieldBlock>
              <FieldBlock label="Microestrutura institucional" hintKey="use_institutional_microstructure"><ToggleSwitch checked={Boolean(config.use_institutional_microstructure)} disabled={saving} onChange={(v) => setBool("use_institutional_microstructure", v)} /></FieldBlock>
            </AccordionSection>
            <AccordionSection title="Lateral / smart money">
              <FieldBlock label="Filtros lateral smart money" hintKey="use_lateral_smart_money_filters"><ToggleSwitch checked={Boolean(config.use_lateral_smart_money_filters)} disabled={saving} onChange={(v) => setBool("use_lateral_smart_money_filters", v)} /></FieldBlock>
              <FieldBlock label="RL lateral (treino)" hintKey="use_reinforcement_lateral_training"><ToggleSwitch checked={Boolean(config.use_reinforcement_lateral_training)} disabled={saving} onChange={(v) => setBool("use_reinforcement_lateral_training", v)} /></FieldBlock>
              <div className="grid gap-3 sm:grid-cols-2">
                <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3">
                  <label className="text-xs text-gray-400">lateral_oi_min_delta_pct</label>
                  <input type="text" className={NUM_INPUT} disabled={saving} value={config.lateral_oi_min_delta_pct !== undefined && Number.isFinite(config.lateral_oi_min_delta_pct) ? String(config.lateral_oi_min_delta_pct) : ""} onChange={(e) => setNum("lateral_oi_min_delta_pct", e.target.value)} />
                </div>
                <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3">
                  <label className="text-xs text-gray-400">lateral_funding_bias_eps</label>
                  <input type="text" className={NUM_INPUT} disabled={saving} value={config.lateral_funding_bias_eps !== undefined && Number.isFinite(config.lateral_funding_bias_eps) ? String(config.lateral_funding_bias_eps) : ""} onChange={(e) => setNum("lateral_funding_bias_eps", e.target.value)} />
                </div>
                <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3 sm:col-span-2">
                  <label className="text-xs text-gray-400">lateral_funding_bias_certeza_pts</label>
                  <input type="text" className={NUM_INPUT} disabled={saving} value={config.lateral_funding_bias_certeza_pts !== undefined && Number.isFinite(config.lateral_funding_bias_certeza_pts) ? String(config.lateral_funding_bias_certeza_pts) : ""} onChange={(e) => setNum("lateral_funding_bias_certeza_pts", e.target.value)} />
                </div>
              </div>
            </AccordionSection>
            <AccordionSection title="Interface (tema)">
              <div className="grid gap-3 sm:grid-cols-2">
                <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3 sm:col-span-2">
                  <label className="text-xs text-gray-400">tema_visual</label>
                  <select className={NUM_INPUT} disabled={saving} value={config.tema_visual || "Dark"} onChange={(e) => setStr("tema_visual", e.target.value)}>
                    <option value="Dark">Dark</option>
                    <option value="Light">Light</option>
                    <option value="TradingView">TradingView</option>
                  </select>
                </div>
                <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3 sm:col-span-2">
                  <label className="text-xs text-gray-400">idioma</label>
                  <select className={NUM_INPUT} disabled={saving} value={config.idioma || "pt-BR"} onChange={(e) => setStr("idioma", e.target.value)}>
                    <option value="pt-BR">pt-BR</option>
                    <option value="Português-Brasil">Português-Brasil</option>
                    <option value="Inglês">Inglês</option>
                  </select>
                </div>
                <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3 sm:col-span-2">
                  <label className="text-xs text-gray-400">fuso_horario</label>
                  <select className={NUM_INPUT} disabled={saving} value={config.fuso_horario || "America/Sao_Paulo"} onChange={(e) => setStr("fuso_horario", e.target.value)}>
                    <option value="America/Sao_Paulo">America/Sao_Paulo</option>
                    <option value="America/New_York">America/New_York</option>
                    <option value="UTC">UTC</option>
                  </select>
                </div>
              </div>
              <FieldBlock label="Escudo dark (UI)" hintKey="escudo_dark"><ToggleSwitch checked={Boolean(config.escudo_dark)} disabled={saving} onChange={(v) => setBool("escudo_dark", v)} /></FieldBlock>
            </AccordionSection>
          </div>
        );
      case "cortex":
        return (
          <div className="w-full max-w-3xl space-y-3">
            <AccordionSection title="Córtex 65D / replay" defaultOpen>
              <FieldBlock label="Rede neural MTF" hintKey="use_mtf_neural" description="15m+1h+4h para LSTM.">
                <ToggleSwitch checked={Boolean(config.use_mtf_neural)} disabled={saving} onChange={(v) => setBool("use_mtf_neural", v)} />
              </FieldBlock>
              <FieldBlock label="Layout 65D" hintKey="use_65d_layout"><ToggleSwitch checked={Boolean(config.use_65d_layout)} disabled={saving} onChange={(v) => setBool("use_65d_layout", v)} /></FieldBlock>
              <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-4">
                <label className="text-sm font-medium text-white">
                  Limiar de Certeza da IA (%)
                </label>
                <code className="mt-0.5 block text-xs text-gray-500">ai_certainty_threshold</code>
                <p className="mt-1 text-xs text-gray-400">
                  A IA só enviará sinais de ordem se a probabilidade (confiança) for estritamente superior a este valor (0 a 100).
                </p>
                <input
                  type="text"
                  inputMode="decimal"
                  className={NUM_INPUT}
                  disabled={saving}
                  value={
                    config.ai_certainty_threshold !== undefined &&
                    Number.isFinite(config.ai_certainty_threshold)
                      ? String(config.ai_certainty_threshold)
                      : "85.0"
                  }
                  onChange={(e) => setNum("ai_certainty_threshold", e.target.value)}
                  placeholder="85.0"
                />
              </div>
              <FieldBlock label="Purgar replay ao mudar layout" hintKey="replay_buffer_purge_on_layout_upgrade"><ToggleSwitch checked={Boolean(config.replay_buffer_purge_on_layout_upgrade)} disabled={saving} onChange={(v) => setBool("replay_buffer_purge_on_layout_upgrade", v)} /></FieldBlock>
            </AccordionSection>
            <AccordionSection title="Indicadores técnicos">
              <div className="space-y-2">
                <FieldBlock label="RSI" hintKey="use_rsi"><ToggleSwitch checked={Boolean(config.use_rsi)} disabled={saving} onChange={(v) => setBool("use_rsi", v)} /></FieldBlock>
                <div className="flex flex-wrap gap-2">
                  <input placeholder="rsi_p" type="text" className={`${NUM_INPUT} max-w-[6rem]`} disabled={saving} value={config.rsi_p !== undefined ? String(config.rsi_p) : ""} onChange={(e) => setNum("rsi_p", e.target.value)} />
                  <input placeholder="rsi_ob" type="text" className={`${NUM_INPUT} max-w-[6rem]`} disabled={saving} value={config.rsi_ob !== undefined ? String(config.rsi_ob) : ""} onChange={(e) => setNum("rsi_ob", e.target.value)} />
                  <input placeholder="rsi_os" type="text" className={`${NUM_INPUT} max-w-[6rem]`} disabled={saving} value={config.rsi_os !== undefined ? String(config.rsi_os) : ""} onChange={(e) => setNum("rsi_os", e.target.value)} />
                </div>
                <FieldBlock label="EMA" hintKey="use_ema"><ToggleSwitch checked={Boolean(config.use_ema)} disabled={saving} onChange={(v) => setBool("use_ema", v)} /></FieldBlock>
                <input placeholder="ema_count" type="text" className={NUM_INPUT} disabled={saving} value={config.ema_count !== undefined ? String(config.ema_count) : ""} onChange={(e) => setNum("ema_count", e.target.value)} />
                <FieldBlock label="MACD" hintKey="use_macd"><ToggleSwitch checked={Boolean(config.use_macd)} disabled={saving} onChange={(v) => setBool("use_macd", v)} /></FieldBlock>
                <div className="flex flex-wrap gap-2">
                  <input placeholder="macd_f" type="text" className={`${NUM_INPUT} max-w-[6rem]`} disabled={saving} value={config.macd_f !== undefined ? String(config.macd_f) : ""} onChange={(e) => setNum("macd_f", e.target.value)} />
                  <input placeholder="macd_s" type="text" className={`${NUM_INPUT} max-w-[6rem]`} disabled={saving} value={config.macd_s !== undefined ? String(config.macd_s) : ""} onChange={(e) => setNum("macd_s", e.target.value)} />
                  <input placeholder="macd_sig" type="text" className={`${NUM_INPUT} max-w-[6rem]`} disabled={saving} value={config.macd_sig !== undefined ? String(config.macd_sig) : ""} onChange={(e) => setNum("macd_sig", e.target.value)} />
                </div>
                <FieldBlock label="Bollinger" hintKey="use_bb"><ToggleSwitch checked={Boolean(config.use_bb)} disabled={saving} onChange={(v) => setBool("use_bb", v)} /></FieldBlock>
                <div className="flex flex-wrap gap-2">
                  <input placeholder="bb_p" type="text" className={`${NUM_INPUT} max-w-[6rem]`} disabled={saving} value={config.bb_p !== undefined ? String(config.bb_p) : ""} onChange={(e) => setNum("bb_p", e.target.value)} />
                  <input placeholder="bb_std" type="text" className={`${NUM_INPUT} max-w-[6rem]`} disabled={saving} value={config.bb_std !== undefined ? String(config.bb_std) : ""} onChange={(e) => setNum("bb_std", e.target.value)} />
                </div>
                <FieldBlock label="ADX" hintKey="use_adx"><ToggleSwitch checked={Boolean(config.use_adx)} disabled={saving} onChange={(v) => setBool("use_adx", v)} /></FieldBlock>
                <div className="flex flex-wrap gap-2">
                  <input placeholder="adx_p" type="text" className={`${NUM_INPUT} max-w-[6rem]`} disabled={saving} value={config.adx_p !== undefined ? String(config.adx_p) : ""} onChange={(e) => setNum("adx_p", e.target.value)} />
                  <input placeholder="adx_thresh" type="text" className={`${NUM_INPUT} max-w-[6rem]`} disabled={saving} value={config.adx_thresh !== undefined ? String(config.adx_thresh) : ""} onChange={(e) => setNum("adx_thresh", e.target.value)} />
                </div>
                <FieldBlock label="Stochastic" hintKey="use_stoch"><ToggleSwitch checked={Boolean(config.use_stoch)} disabled={saving} onChange={(v) => setBool("use_stoch", v)} /></FieldBlock>
                <div className="flex flex-wrap gap-2">
                  <input placeholder="stoch_k" type="text" className={`${NUM_INPUT} max-w-[6rem]`} disabled={saving} value={config.stoch_k !== undefined ? String(config.stoch_k) : ""} onChange={(e) => setNum("stoch_k", e.target.value)} />
                  <input placeholder="stoch_d" type="text" className={`${NUM_INPUT} max-w-[6rem]`} disabled={saving} value={config.stoch_d !== undefined ? String(config.stoch_d) : ""} onChange={(e) => setNum("stoch_d", e.target.value)} />
                </div>
                <FieldBlock label="Volume SMA" hintKey="use_vol"><ToggleSwitch checked={Boolean(config.use_vol)} disabled={saving} onChange={(v) => setBool("use_vol", v)} /></FieldBlock>
                <input placeholder="vol_sma" type="text" className={NUM_INPUT} disabled={saving} value={config.vol_sma !== undefined ? String(config.vol_sma) : ""} onChange={(e) => setNum("vol_sma", e.target.value)} />
                <FieldBlock label="MTF análise" hintKey="use_mtf"><ToggleSwitch checked={Boolean(config.use_mtf)} disabled={saving} onChange={(v) => setBool("use_mtf", v)} /></FieldBlock>
                <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3">
                  <label className="text-xs text-gray-400">confluencia_min</label>
                  <input type="text" className={NUM_INPUT} disabled={saving} value={config.confluencia_min !== undefined ? String(config.confluencia_min) : ""} onChange={(e) => setNum("confluencia_min", e.target.value)} />
                </div>
              </div>
            </AccordionSection>
            <AccordionSection title="Regime / Kelly / NLP">
              <FieldBlock label="Kelly" hintKey="use_kelly"><ToggleSwitch checked={Boolean(config.use_kelly)} disabled={saving} onChange={(v) => setBool("use_kelly", v)} /></FieldBlock>
              <div className="grid gap-3 sm:grid-cols-2">
                <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3">
                  <label className="text-xs text-gray-400">adx_regime_minimo</label>
                  <input type="text" className={NUM_INPUT} disabled={saving} value={config.adx_regime_minimo !== undefined ? String(config.adx_regime_minimo) : ""} onChange={(e) => setNum("adx_regime_minimo", e.target.value)} />
                </div>
                <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3">
                  <label className="text-xs text-gray-400">nlp_sentimento_minimo</label>
                  <input type="text" className={NUM_INPUT} disabled={saving} value={config.nlp_sentimento_minimo !== undefined ? String(config.nlp_sentimento_minimo) : ""} onChange={(e) => setNum("nlp_sentimento_minimo", e.target.value)} />
                </div>
                <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-3 sm:col-span-2">
                  <label className="text-xs text-gray-400">l2_imbalance_corte</label>
                  <input type="text" className={NUM_INPUT} disabled={saving} value={config.l2_imbalance_corte !== undefined ? String(config.l2_imbalance_corte) : ""} onChange={(e) => setNum("l2_imbalance_corte", e.target.value)} />
                </div>
              </div>
            </AccordionSection>
            <div className="mt-8 border-t border-red-900/30 pt-6">
              <h3 className="text-sm font-semibold text-red-500 mb-2 uppercase tracking-wider">🧨 Ações Críticas</h3>
              <p className="text-xs text-gray-400 mb-4">
                Isso apagará os arquivos .keras atuais na pasta &apos;modelos&apos; e iniciará a Forja Neural do zero.
              </p>
              <button
                type="button"
                disabled={saving}
                onClick={async () => {
                  if (
                    window.confirm(
                      "ATENÇÃO: Deseja realmente destruir os modelos atuais e iniciar um novo Bootcamp Neural?"
                    )
                  ) {
                    try {
                      await fetchWithAuth("/api/command", {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ command: "FORCAR_TREINO" }),
                      });
                      alert("🚀 Ordem de forja enviada! Verifique o terminal do motor.");
                    } catch {
                      alert("❌ Erro ao enviar comando.");
                    }
                  }
                }}
                className="w-full py-3 border border-red-600/50 text-red-500 hover:bg-red-600 hover:text-white transition-all rounded font-bold text-xs uppercase disabled:cursor-not-allowed disabled:opacity-50"
              >
                Forçar Retreino da IA (Zerar Modelos)
              </button>
            </div>
          </div>
        );
      case "sensors":
        return (
          <div className="w-full max-w-3xl space-y-4">
            <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-4">
              <label className="text-sm font-medium text-white">
                Evaporação mínima (spoof)
              </label>
              <code className="mt-0.5 block text-xs text-gray-500">
                spoof_evaporation_pct
              </code>
              <p className="mt-1 text-xs text-gray-400">
                Fração do depth que deve sumir num tick vs t−1 para gatilho (0.4 = 40%).
              </p>
              <input
                type="text"
                inputMode="decimal"
                className={NUM_INPUT}
                disabled={saving}
                value={
                  config.spoof_evaporation_pct !== undefined &&
                  Number.isFinite(config.spoof_evaporation_pct)
                    ? String(config.spoof_evaporation_pct)
                    : ""
                }
                onChange={(e) => setNum("spoof_evaporation_pct", e.target.value)}
                placeholder="0.4"
              />
            </div>
            <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-4">
              <label className="text-sm font-medium text-white">
                Epsilon plano de preço (%)
              </label>
              <code className="mt-0.5 block text-xs text-gray-500">
                spoof_price_flat_eps_pct
              </code>
              <p className="mt-1 text-xs text-gray-400">
                |Δmark| entre ticks tratado como “sem rally/queda” no radar de spoofing.
              </p>
              <input
                type="text"
                inputMode="decimal"
                className={NUM_INPUT}
                disabled={saving}
                value={
                  config.spoof_price_flat_eps_pct !== undefined &&
                  Number.isFinite(config.spoof_price_flat_eps_pct)
                    ? String(config.spoof_price_flat_eps_pct)
                    : ""
                }
                onChange={(e) =>
                  setNum("spoof_price_flat_eps_pct", e.target.value)
                }
                placeholder="0.06"
              />
            </div>
          </div>
        );
      case "arb":
        return (
          <div className="w-full max-w-3xl space-y-4">
            <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-4">
              <label className="text-sm font-medium text-white">
                Z-Score de entrada
              </label>
              <code className="mt-0.5 block text-xs text-gray-500">
                arb_zscore_entry
              </code>
              <p className="mt-1 text-xs text-gray-400">
                |Z| alvo para abrir pernas LONG/SHORT market-neutral (divergência).
              </p>
              <input
                type="text"
                inputMode="decimal"
                className={NUM_INPUT}
                disabled={saving}
                value={
                  config.arb_zscore_entry !== undefined &&
                  Number.isFinite(config.arb_zscore_entry)
                    ? String(config.arb_zscore_entry)
                    : ""
                }
                onChange={(e) => setNum("arb_zscore_entry", e.target.value)}
                placeholder="2.0"
              />
            </div>
            <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-4">
              <label className="text-sm font-medium text-white">
                Z-Score de saída (convergência)
              </label>
              <code className="mt-0.5 block text-xs text-gray-500">
                arb_zscore_exit
              </code>
              <p className="mt-1 text-xs text-gray-400">
                Fecha o par quando |Z| recua para esta banda perto de zero (mean reversion).
              </p>
              <input
                type="text"
                inputMode="decimal"
                className={NUM_INPUT}
                disabled={saving}
                value={
                  config.arb_zscore_exit !== undefined &&
                  Number.isFinite(config.arb_zscore_exit)
                    ? String(config.arb_zscore_exit)
                    : ""
                }
                onChange={(e) => setNum("arb_zscore_exit", e.target.value)}
                placeholder="0.5"
              />
            </div>
            <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-4">
              <label className="text-sm font-medium text-white">
                Intervalo do loop (s)
              </label>
              <code className="mt-0.5 block text-xs text-gray-500">
                arb_loop_interval_sec
              </code>
              <p className="mt-1 text-xs text-gray-400">
                Cadência de atualização do Z-score e gatilhos de pairs (1–120 s).
              </p>
              <input
                type="text"
                inputMode="decimal"
                className={NUM_INPUT}
                disabled={saving}
                value={
                  config.arb_loop_interval_sec !== undefined &&
                  Number.isFinite(config.arb_loop_interval_sec)
                    ? String(config.arb_loop_interval_sec)
                    : ""
                }
                onChange={(e) => setNum("arb_loop_interval_sec", e.target.value)}
                placeholder="5"
              />
            </div>
          </div>
        );
      case "memory":
        return (
          <div className="w-full max-w-3xl space-y-4">
            <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-4">
              <label className="text-sm font-medium text-white">
                Quota do cofre (GB)
              </label>
              <code className="mt-0.5 block text-xs text-gray-500">cofre_quota_gb</code>
              <p className="mt-1 text-xs text-gray-400">
                Teto lógico de uso combinado (SQLite + Parquet) para o zelador WORM.
              </p>
              <input
                type="text"
                inputMode="decimal"
                className={NUM_INPUT}
                disabled={saving}
                value={
                  config.cofre_quota_gb !== undefined &&
                  Number.isFinite(config.cofre_quota_gb)
                    ? String(config.cofre_quota_gb)
                    : ""
                }
                onChange={(e) => setNum("cofre_quota_gb", e.target.value)}
                placeholder="45"
              />
            </div>
            <div className="rounded-lg border border-gray-800/80 bg-[#0f172a]/40 p-4">
              <label className="text-sm font-medium text-white">
                Lucro mínimo WORM (USD)
              </label>
              <code className="mt-0.5 block text-xs text-gray-500">
                cofre_worm_profit_usd
              </code>
              <p className="mt-1 text-xs text-gray-400">
                Referência de PnL para promoção/arquivamento no pipeline de cofre neural.
              </p>
              <input
                type="text"
                inputMode="decimal"
                className={NUM_INPUT}
                disabled={saving}
                value={
                  config.cofre_worm_profit_usd !== undefined &&
                  Number.isFinite(config.cofre_worm_profit_usd)
                    ? String(config.cofre_worm_profit_usd)
                    : ""
                }
                onChange={(e) => setNum("cofre_worm_profit_usd", e.target.value)}
                placeholder="15"
              />
            </div>
          </div>
        );
      default:
        return null;
    }
  }, [
    activeConfigTab,
    config,
    loading,
    loadError,
    saving,
    setBool,
    setNum,
    setStr,
  ]);

  return (
    <div className="flex h-full min-h-0 flex-col bg-[#0b0f19] text-gray-200">
      <header className="shrink-0 border-b border-gray-800/80 bg-[#0b0f19]/95 px-4 py-4 backdrop-blur sm:px-6">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div className="min-w-0">
            <h1 className="text-lg font-semibold tracking-tight text-white sm:text-xl">
              Governança do motor
            </h1>
            <p className="mt-1 max-w-3xl text-sm text-gray-500">
              Central institucional — persistência via{" "}
              <code className="rounded bg-gray-800/80 px-1.5 py-0.5 text-cyan-400/90">
                GET/POST /api/config
              </code>{" "}
              (single source of truth).
            </p>
          </div>
          {/* Nexus HFT V90: cofre de credenciais isolado */}
          <button
            type="button"
            onClick={() => setCredentialsOpen(true)}
            className="inline-flex shrink-0 items-center gap-2 rounded-lg border border-amber-600/50 bg-amber-950/40 px-4 py-2.5 text-sm font-semibold text-amber-100 transition hover:bg-amber-900/50"
          >
            <KeyRound className="h-4 w-4" />
            Credenciais &amp; API
          </button>
        </div>
      </header>

      {loadError ? (
        <div className="shrink-0 border-b border-red-900/50 bg-red-950/30 px-4 py-3 sm:px-6">
          <div className="flex items-start gap-2 text-sm text-red-200">
            <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
            <span>{loadError}</span>
          </div>
        </div>
      ) : null}

      <div className="flex min-h-0 flex-1 flex-col md:flex-row">
        <nav
          className="shrink-0 border-b border-gray-800/80 md:w-56 md:border-b-0 md:border-r md:border-gray-800/80 md:py-4"
          aria-label="Secções de configuração"
        >
          <div className="flex gap-1 overflow-x-auto px-2 py-2 md:flex-col md:gap-0 md:px-2 md:py-0">
            {TABS.map(({ id, label, short, icon: Icon }) => {
              const active = activeConfigTab === id;
              return (
                <button
                  key={id}
                  type="button"
                  onClick={() => setActiveConfigTab(id)}
                  className={`flex min-w-[9.5rem] items-center gap-2 rounded-lg px-3 py-2.5 text-left text-sm transition-colors md:min-w-0 ${
                    active
                      ? "bg-cyan-950/50 text-cyan-100 ring-1 ring-cyan-500/40"
                      : "text-gray-400 hover:bg-gray-800/50 hover:text-white"
                  }`}
                >
                  <Icon
                    className={`h-4 w-4 shrink-0 ${active ? "text-cyan-400" : "text-gray-500"}`}
                    strokeWidth={1.75}
                  />
                  <span className="hidden font-medium sm:inline">{label}</span>
                  <span className="font-medium sm:hidden">{short}</span>
                </button>
              );
            })}
          </div>
        </nav>

        <main className="min-h-0 flex-1 overflow-y-auto custom-scrollbar p-4 sm:p-6">
          {loading ? (
            <div className="flex items-center gap-2 text-sm text-gray-400">
              <Loader2 className="h-4 w-4 animate-spin text-cyan-500" />
              A carregar configuração…
            </div>
          ) : (
            tabPanel
          )}
        </main>
      </div>

      <footer className="shrink-0 border-t border-gray-800/90 bg-[#0b0f19]/95 px-4 py-3 backdrop-blur sm:px-6">
        <div className="mx-auto flex max-w-5xl flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div className="min-h-[1.25rem] text-sm">
            {saveMessage ? (
              <span className="inline-flex items-center gap-1.5 text-emerald-300/95">
                <CheckCircle2 className="h-4 w-4 shrink-0" />
                {saveMessage}
              </span>
            ) : null}
            {saveError ? (
              <span className="inline-flex items-center gap-1.5 text-red-300">
                <AlertCircle className="h-4 w-4 shrink-0" />
                {saveError}
              </span>
            ) : null}
          </div>
          <button
            type="button"
            disabled={saving || loading || Boolean(loadError)}
            onClick={() => void handleSave().then(() => undefined)}
            className="inline-flex items-center justify-center gap-2 rounded-lg bg-gradient-to-r from-cyan-600 to-emerald-600 px-5 py-2.5 text-sm font-semibold text-white shadow-lg shadow-cyan-900/20 transition hover:from-cyan-500 hover:to-emerald-500 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {saving ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin" />
                A gravar…
              </>
            ) : (
              "Salvar governança"
            )}
          </button>
        </div>
      </footer>

      {/* Nexus HFT V90: modal estrito — só segredos (não misturar com toggles do motor). */}
      {credentialsOpen ? (
        <div
          className="fixed inset-0 z-[100] flex items-center justify-center bg-black/75 p-4"
          role="dialog"
          aria-modal="true"
          aria-labelledby="cred-title"
        >
          <div className="max-h-[90vh] w-full max-w-lg overflow-y-auto rounded-xl border border-gray-700 bg-[#0f172a] p-6 shadow-2xl">
            <div className="mb-4 flex items-center justify-between gap-2">
              <h2 id="cred-title" className="text-lg font-semibold text-white">
                Credenciais
              </h2>
              <button
                type="button"
                onClick={() => setCredentialsOpen(false)}
                className="rounded-lg px-3 py-1.5 text-sm text-gray-400 hover:bg-gray-800 hover:text-white"
              >
                Fechar
              </button>
            </div>
            <p className="mb-4 text-xs text-gray-500">
              Chaves Bybit e Telegram. O secret mascarado não é reenviado — deixe em branco para não alterar.
            </p>
            <div className="space-y-4">
              <div>
                <label className="mb-1 block text-xs text-gray-400">api_key</label>
                <input
                  type={showApiSecretFields ? "text" : "password"}
                  className={NUM_INPUT}
                  autoComplete="off"
                  value={config.api_key ?? ""}
                  onChange={(e) => setStr("api_key", e.target.value)}
                  disabled={saving}
                />
              </div>
              <div>
                <label className="mb-1 block text-xs text-gray-400">api_secret</label>
                <div className="relative">
                  <input
                    type={showApiSecretFields ? "text" : "password"}
                    className={NUM_INPUT}
                    autoComplete="off"
                    value={config.api_secret === "***MASCARADO***" ? "" : config.api_secret ?? ""}
                    placeholder="(inalterado se vazio)"
                    onChange={(e) => setStr("api_secret", e.target.value)}
                    disabled={saving}
                  />
                  <button
                    type="button"
                    className="absolute right-2 top-2 rounded p-1 text-gray-500 hover:text-white"
                    onClick={() => setShowApiSecretFields((v) => !v)}
                    aria-label="Mostrar segredos"
                  >
                    {showApiSecretFields ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                  </button>
                </div>
              </div>
              <div>
                <label className="mb-1 block text-xs text-gray-400">telegram_token</label>
                <div className="relative">
                  <input
                    type={showTgTok ? "text" : "password"}
                    className={NUM_INPUT}
                    value={config.telegram_token ?? ""}
                    onChange={(e) => setStr("telegram_token", e.target.value)}
                    disabled={saving}
                  />
                  <button
                    type="button"
                    className="absolute right-2 top-2 rounded p-1 text-gray-500 hover:text-white"
                    onClick={() => setShowTgTok((v) => !v)}
                  >
                    {showTgTok ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                  </button>
                </div>
              </div>
              <div>
                <label className="mb-1 block text-xs text-gray-400">telegram_chat_id</label>
                <div className="relative">
                  <input
                    type={showTgChat ? "text" : "password"}
                    className={NUM_INPUT}
                    value={config.telegram_chat_id ?? ""}
                    onChange={(e) => setStr("telegram_chat_id", e.target.value)}
                    disabled={saving}
                  />
                  <button
                    type="button"
                    className="absolute right-2 top-2 rounded p-1 text-gray-500 hover:text-white"
                    onClick={() => setShowTgChat((v) => !v)}
                  >
                    {showTgChat ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                  </button>
                </div>
              </div>
            </div>
            <div className="mt-6 flex justify-end gap-2">
              <button
                type="button"
                onClick={() => setCredentialsOpen(false)}
                className="rounded-lg border border-gray-600 px-4 py-2 text-sm text-gray-300 hover:bg-gray-800"
              >
                Cancelar
              </button>
              <button
                type="button"
                disabled={saving}
                onClick={() => {
                  void handleSave().then((ok) => {
                    if (ok) setCredentialsOpen(false);
                  });
                }}
                className="rounded-lg bg-gradient-to-r from-cyan-600 to-emerald-600 px-4 py-2 text-sm font-semibold text-white disabled:opacity-50"
              >
                Gravar credenciais
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
