"use client";

import React, { useEffect, useState, useRef } from "react";
import { fetchWithAuth } from "@/lib/lhnAuth";
import { TrendingUp, TrendingDown, Radio, RefreshCw, Zap, Target, Shield, Activity } from "lucide-react";

interface Signal {
  id?: number;
  timestamp: string;
  par: string;
  /** LONG, SHORT ou qualquer rótulo devolvido pelo motor (sem filtrar no cliente). */
  acao: string;
  preco_entrada: number;
  tp1?: number;
  tp2?: number;
  tp3?: number;
  sl?: number;
  certeza: number;
  fluxo?: number;
  vpin?: number;
  status?: string;
}

const SIGNAL_API = "/api/signals";

// ──────────────────────────────────────────────────────────────
// Formatador de data/hora compacto
// ──────────────────────────────────────────────────────────────
function fmtTs(ts: string): string {
  try {
    const d = new Date(ts);
    return d.toLocaleString("pt-BR", {
      day: "2-digit", month: "2-digit",
      hour: "2-digit", minute: "2-digit", second: "2-digit",
      hour12: false,
    });
  } catch {
    return ts;
  }
}

// ──────────────────────────────────────────────────────────────
// Célula de certeza — cor por nível
// ──────────────────────────────────────────────────────────────
function CertezaBadge({ value }: { value: number }) {
  const v = Number.isFinite(value) ? value : 0;
  const color =
    v >= 85 ? "text-[#0ecb81] border-[#0ecb81]/40 bg-[#0ecb81]/8" :
    v >= 65 ? "text-[#f0b90b] border-[#f0b90b]/40 bg-[#f0b90b]/8" :
    "text-[#f6465d] border-[#f6465d]/40 bg-[#f6465d]/8";
  return (
    <span className={`font-mono font-bold text-[12px] tracking-wide border px-2 py-0.5 rounded ${color}`}>
      {v.toFixed(2)}%
    </span>
  );
}

// ──────────────────────────────────────────────────────────────
// Linha de TP/SL compacta
// ──────────────────────────────────────────────────────────────
function TpSlCell({ tp1, tp2, tp3, sl }: { tp1?: number; tp2?: number; tp3?: number; sl?: number }) {
  return (
    <div className="flex flex-col gap-0.5 text-[10px] font-mono">
      {tp1 && <span className="text-[#0ecb81]">TP1 {tp1.toFixed(4)}</span>}
      {tp2 && <span className="text-[#0ecb81]/70">TP2 {tp2.toFixed(4)}</span>}
      {tp3 && <span className="text-[#0ecb81]/50">TP3 {tp3.toFixed(4)}</span>}
      {sl  && <span className="text-[#f6465d]">SL  {sl.toFixed(4)}</span>}
      {!tp1 && !sl && <span className="text-gray-600">—</span>}
    </div>
  );
}

// ──────────────────────────────────────────────────────────────
// Componente principal
// ──────────────────────────────────────────────────────────────
export function SignalsHistory() {
  const [signals, setSignals]       = useState<Signal[]>([]);
  const [loading, setLoading]       = useState(true);
  const [filter, setFilter]         = useState<"ALL" | "LONG" | "SHORT">("ALL");
  const [search, setSearch]         = useState("");
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);
  const intervalRef = useRef<any>(null);

  const fetchSignals = async () => {
    try {
      const res = await fetchWithAuth(SIGNAL_API, { cache: "no-store" });
      if (!res.ok) {
        console.warn("SignalsHistory: HTTP", res.status);
        return;
      }
      let data;
      try {
        data = await res.json();
      } catch (err) {
        console.warn("SignalsHistory: Falha ao decodificar JSON", err);
        return;
      }
      const raw = Array.isArray(data.signals) ? data.signals : [];
      const norm: Signal[] = raw.map((s: any) => {
        let acao = String(s.acao ?? "").trim();
        if (!acao) {
          const t = String(s.tipo || s.direcao || "").trim();
          if (t) acao = t.toUpperCase();
        } else {
          acao = acao.toUpperCase();
        }
        const certezaNum = Number(s.certeza ?? s.confidence ?? 0);
        return {
          ...s,
          acao,
          par: String(s.par ?? s.symbol ?? ""),
          certeza: Number.isFinite(certezaNum) ? certezaNum : 0,
        } as Signal;
      });
      setSignals([...norm].reverse());
      setLastRefresh(new Date());
    } catch (e) {
      console.warn("SignalsHistory: fetch error", e);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchSignals();
    intervalRef.current = setInterval(fetchSignals, 60000);
    return () => clearInterval(intervalRef.current);
  }, []);

  const filtered = signals.filter(s => {
    if (filter !== "ALL" && s.acao !== filter) return false;
    const parS = String(s.par || "").toLowerCase();
    if (search && !parS.includes(search.toLowerCase())) return false;
    return true;
  });

  const totalLong  = signals.filter(s => s.acao === "LONG").length;
  const totalShort = signals.filter(s => s.acao === "SHORT").length;
  const totalOutros = signals.filter(s => s.acao !== "LONG" && s.acao !== "SHORT").length;
  const avgCert    = signals.length > 0
    ? signals.reduce((acc, s) => acc + (Number.isFinite(s.certeza) ? s.certeza : 0), 0) / signals.length
    : 0;

  return (
    <div className="flex flex-col h-full bg-[#0b0e11] text-white">

      {/* ── Header ── */}
      <div className="px-6 py-4 border-b border-[#2b3139] shrink-0">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-3">
            <div className="p-2.5 bg-[#f0b90b]/10 rounded-xl border border-[#f0b90b]/20">
              <Radio className="w-5 h-5 text-[#f0b90b]" />
            </div>
            <div>
              <h1 className="text-[18px] font-bold text-white tracking-tight">Histórico de Sinais de Negociação</h1>
              <p className="text-gray-500 text-[11px] font-mono mt-0.5">
                {signals.length} sinais registados
                {totalOutros > 0 ? ` (${totalOutros} não-LONG/SHORT)` : ""} · Atualiza a cada 10s
                {lastRefresh && ` · ${lastRefresh.toLocaleTimeString("pt-BR")}`}
              </p>
            </div>
          </div>
          <button
            onClick={fetchSignals}
            className="flex items-center gap-1.5 bg-[#161a1e] hover:bg-[#1e2329] border border-[#2b3139] text-gray-400 hover:text-white px-3 py-2 rounded-lg text-[11px] font-bold uppercase tracking-widest transition-all"
          >
            <RefreshCw className="w-3.5 h-3.5" />
            Atualizar
          </button>
        </div>

        {/* ── KPI Strip ── */}
        <div className="grid grid-cols-4 gap-3 mb-4">
          {[
            { label: "Total Sinais", val: signals.length, accent: "text-white" },
            { label: "LONG",  val: totalLong,  accent: "text-[#0ecb81]" },
            { label: "SHORT", val: totalShort, accent: "text-[#f6465d]" },
            { label: "Certeza Média", val: `${avgCert.toFixed(2)}%`, accent: avgCert >= 70 ? "text-[#0ecb81]" : "text-[#f0b90b]" },
          ].map(k => (
            <div key={k.label} className="bg-[#161a1e] border border-[#2b3139] rounded-xl px-4 py-3 flex flex-col">
              <span className="text-gray-500 text-[10px] uppercase tracking-widest font-bold mb-1">{k.label}</span>
              <span className={`text-xl font-mono font-black ${k.accent}`}>{k.val}</span>
            </div>
          ))}
        </div>

        {/* ── Filtros ── */}
        <div className="flex items-center gap-3">
          <input
            type="text"
            placeholder="Filtrar por par (ex: BTC)"
            value={search}
            onChange={e => setSearch(e.target.value)}
            className="flex-1 bg-[#161a1e] border border-[#2b3139] rounded-lg px-3 py-2 text-[12px] font-mono text-gray-200 placeholder-gray-600 outline-none focus:border-[#f0b90b]/50 transition-colors"
          />
          {(["ALL", "LONG", "SHORT"] as const).map(f => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`px-4 py-2 rounded-lg text-[11px] font-bold uppercase tracking-widest transition-all border ${
                filter === f
                  ? f === "LONG"  ? "bg-[#0ecb81]/10 text-[#0ecb81] border-[#0ecb81]/40"
                  : f === "SHORT" ? "bg-[#f6465d]/10 text-[#f6465d] border-[#f6465d]/40"
                  :                 "bg-[#f0b90b]/10 text-[#f0b90b] border-[#f0b90b]/40"
                  : "bg-[#161a1e] text-gray-500 border-[#2b3139] hover:text-gray-300"
              }`}
            >
              {f}
            </button>
          ))}
        </div>
      </div>

      {/* ── Tabela ── */}
      <div className="flex-1 overflow-y-auto custom-scrollbar">
        {loading ? (
          <div className="flex flex-col items-center justify-center h-full gap-4 text-gray-600">
            <Activity className="w-10 h-10 opacity-20 animate-pulse" />
            <span className="text-sm font-sans">A carregar sinais...</span>
          </div>
        ) : filtered.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full gap-4 text-gray-600">
            <Radio className="w-10 h-10 opacity-20" />
            <span className="text-sm font-sans">Nenhum sinal encontrado.</span>
            <span className="text-xs text-gray-700 font-mono">Os sinais aparecem quando a IA confirma uma entrada.</span>
          </div>
        ) : (
          <table className="w-full text-left border-collapse text-xs">
            <thead className="sticky top-0 z-10 bg-[#161a1e] border-b border-[#2b3139]">
              <tr className="text-gray-500 font-bold uppercase tracking-widest text-[10px]">
                <th className="py-3 px-4">Timestamp</th>
                <th className="py-3 px-4">Par</th>
                <th className="py-3 px-4 text-center">Ação</th>
                <th className="py-3 px-4 text-right">Preço de Entrada</th>
                <th className="py-3 px-4">Saídas (TP/SL)</th>
                <th className="py-3 px-4 text-center">Certeza IA</th>
                <th className="py-3 px-4 text-center">Fluxo</th>
                <th className="py-3 px-4 text-center">Status</th>
              </tr>
            </thead>
            <tbody className="font-mono divide-y divide-[#2b3139]/30">
              {filtered.map((s, idx) => {
                const ac = String(s.acao || "").toUpperCase();
                const isLong = ac === "LONG";
                const isShort = ac === "SHORT";
                const fluxoPct = typeof s.fluxo === "number" ? s.fluxo : (s.vpin ?? null);
                const parDisp = String(s.par || "");
                const baseSym = parDisp.replace(/USDT$/i, "");
                return (
                  <tr
                    key={s.id ?? `${parDisp}-${s.timestamp}-${idx}`}
                    className="hover:bg-[#1e2329]/50 transition-colors group"
                  >
                    <td className="py-3 px-4 text-gray-500 text-[11px] whitespace-nowrap">{fmtTs(s.timestamp)}</td>
                    <td className="py-3 px-4">
                      <div className="flex items-center gap-2">
                        <span className="font-bold text-gray-200 text-[13px]">
                          {baseSym || "—"}{parDisp ? <span className="text-gray-600 font-normal">/USDT</span> : null}
                        </span>
                      </div>
                    </td>
                    <td className="py-3 px-4 text-center">
                      {isLong || isShort ? (
                        <div className={`inline-flex items-center gap-1 px-2.5 py-1 rounded-md border font-bold text-[11px] uppercase tracking-widest ${
                          isLong
                            ? "bg-[#0ecb81]/10 text-[#0ecb81] border-[#0ecb81]/30"
                            : "bg-[#f6465d]/10 text-[#f6465d] border-[#f6465d]/30"
                        }`}>
                          {isLong ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
                          {ac}
                        </div>
                      ) : (
                        <div className="inline-flex items-center gap-1 px-2.5 py-1 rounded-md border border-[#2b3139] bg-[#1e2329]/80 text-gray-300 font-bold text-[11px] uppercase tracking-wide">
                          <Activity className="w-3 h-3 opacity-70" />
                          {ac || "—"}
                        </div>
                      )}
                    </td>
                    <td className="py-3 px-4 text-right text-gray-200 font-bold text-[13px]">
                      {s.preco_entrada > 0 ? s.preco_entrada.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 6 }) : "—"}
                    </td>
                    <td className="py-3 px-4">
                      <TpSlCell tp1={s.tp1} tp2={s.tp2} tp3={s.tp3} sl={s.sl} />
                    </td>
                    <td className="py-3 px-4 text-center">
                      <CertezaBadge value={Number.isFinite(s.certeza) ? s.certeza : 0} />
                    </td>
                    <td className="py-3 px-4 text-center">
                      {fluxoPct != null ? (
                        <span className={`font-bold text-[12px] ${
                          fluxoPct > 65 ? "text-[#0ecb81]" :
                          fluxoPct < 35 ? "text-[#f6465d]" : "text-gray-400"
                        }`}>
                          {fluxoPct.toFixed(1)}%
                        </span>
                      ) : <span className="text-gray-600">—</span>}
                    </td>
                    <td className="py-3 px-4 text-center">
                      {s.status ? (
                        <span className={`text-[10px] font-bold uppercase tracking-wider ${
                          s.status === "WIN"   ? "text-[#0ecb81]" :
                          s.status === "LOSS"  ? "text-[#f6465d]" :
                          s.status === "OPEN"  ? "text-[#f0b90b]" :
                          String(s.status).startsWith("VPIN") ? "text-[#a855f7]" : "text-gray-500"
                        }`}>{String(s.status)}</span>
                      ) : <span className="text-gray-600 text-[10px]">PENDENTE</span>}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
