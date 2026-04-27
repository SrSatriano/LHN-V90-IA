"use client";

import React, { useEffect, useState } from "react";
import { fetchWithAuth } from "@/lib/lhnAuth";
import { Radio, Send, Save, ToggleLeft, ToggleRight, CheckCircle, AlertCircle } from "lucide-react";

interface TransmissionConfig {
  transmissao_ativa: boolean;
  transmissao_token: string;
  transmissao_chat_id: string;
}

const DEFAULT_CFG: TransmissionConfig = {
  transmissao_ativa: false,
  transmissao_token: "",
  transmissao_chat_id: "",
};

export function TransmissionConfig() {
  const [cfg, setCfg] = useState<TransmissionConfig>(DEFAULT_CFG);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [toast, setToast] = useState<{ type: "success" | "error"; msg: string } | null>(null);

  const showToast = (type: "success" | "error", msg: string) => {
    setToast({ type, msg });
    setTimeout(() => setToast(null), 5000);
  };

  useEffect(() => {
    fetchWithAuth("/api/config/transmission")
      .then(r => r.text())
      .then(text => {
        try {
          const data = JSON.parse(text);
          setCfg({ ...DEFAULT_CFG, ...data });
        } catch (e) {
          console.warn("TransmissionConfig parse err:", e);
        }
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, []);

  const salvar = async () => {
    setSaving(true);
    try {
      const res = await fetchWithAuth("/api/config/transmission", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(cfg),
      });
      if (res.ok) showToast("success", "Configuração de transmissão salva.");
      else showToast("error", "Falha ao salvar.");
    } catch { showToast("error", "Erro de conexão."); }
    finally { setSaving(false); }
  };

  const testar = async () => {
    setTesting(true);
    try {
      const res = await fetchWithAuth("/api/config/transmission/test", { method: "POST" });
      let data: any = {};
      try {
        data = await res.json();
      } catch (err) {
        data = { error: "Backend indisponível" };
      }
      if (res.ok && data.status === "ok") showToast("success", "✓ Mensagem de teste enviada!");
      else showToast("error", data.detail || "Falha no envio do teste.");
    } catch { showToast("error", "Erro de conexão."); }
    finally { setTesting(false); }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full bg-[#0b0e11]">
        <div className="w-8 h-8 border-2 border-[#f0b90b]/30 border-t-[#f0b90b] rounded-full animate-spin" />
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full bg-[#0b0e11] text-white overflow-y-auto custom-scrollbar">
      <div className="max-w-2xl mx-auto w-full p-8">

        {/* ── Header ── */}
        <div className="flex items-center gap-4 mb-8">
          <div className="p-3 bg-[#f0b90b]/10 rounded-2xl border border-[#f0b90b]/20">
            <Radio className="w-7 h-7 text-[#f0b90b]" />
          </div>
          <div>
            <h1 className="text-[22px] font-bold tracking-tight">Transmissão Automatizada de Sinais</h1>
            <p className="text-gray-500 text-[13px] mt-0.5">
              Configuração <strong className="text-[#f0b90b]">isolada</strong> do Telegram pessoal — bot dedicado exclusivamente à transmissão de sinais.
            </p>
          </div>
        </div>

        {/* ── Card Principal ── */}
        <div className="bg-[#131722] border border-[#2b3139] rounded-2xl overflow-hidden shadow-xl">

          {/* Toggle Ativar */}
          <div
            className="flex items-center justify-between p-6 border-b border-[#2b3139] cursor-pointer hover:bg-[#161a1e]/50 transition-colors"
            onClick={() => setCfg(c => ({ ...c, transmissao_ativa: !c.transmissao_ativa }))}
          >
            <div>
              <p className="font-bold text-[15px] text-white">Ativar Transmissão de Sinais</p>
              <p className="text-gray-500 text-[12px] mt-0.5">
                Quando ativo, cada sinal confirmado pela IA será enviado automaticamente ao canal configurado.
              </p>
            </div>
            <div className={`text-3xl transition-colors ${cfg.transmissao_ativa ? "text-[#f0b90b]" : "text-gray-600"}`}>
              {cfg.transmissao_ativa
                ? <ToggleRight className="w-10 h-10" />
                : <ToggleLeft  className="w-10 h-10" />}
            </div>
          </div>

          {/* Campos */}
          <div className="p-6 flex flex-col gap-5">

            {/* Token */}
            <div>
              <label className="block text-[11px] text-gray-400 uppercase tracking-widest font-bold mb-2">
                Token do Bot de Sinais (Transmissão)
              </label>
              <input
                type="text"
                value={cfg.transmissao_token}
                onChange={e => setCfg(c => ({ ...c, transmissao_token: e.target.value }))}
                placeholder="YOUR_TELEGRAM_BOT_TOKEN_HERE"
                className="w-full bg-[#0b0e11] border border-[#2b3139] focus:border-[#f0b90b]/60 rounded-xl px-4 py-3 text-[13px] font-mono text-gray-200 placeholder-gray-700 outline-none transition-all"
              />
              <p className="text-gray-600 text-[11px] mt-1.5 font-mono">
                ⚠️ Use um <strong>bot diferente</strong> do seu Telegram pessoal. Crie via @BotFather.
              </p>
            </div>

            {/* Chat ID */}
            <div>
              <label className="block text-[11px] text-gray-400 uppercase tracking-widest font-bold mb-2">
                ID do Chat / Canal de Sinais
              </label>
              <input
                type="text"
                value={cfg.transmissao_chat_id}
                onChange={e => setCfg(c => ({ ...c, transmissao_chat_id: e.target.value }))}
                placeholder="-1001234567890  ou  @meu_canal"
                className="w-full bg-[#0b0e11] border border-[#2b3139] focus:border-[#f0b90b]/60 rounded-xl px-4 py-3 text-[13px] font-mono text-gray-200 placeholder-gray-700 outline-none transition-all"
              />
              <p className="text-gray-600 text-[11px] mt-1.5 font-mono">
                Para grupos/canais o ID começa com -100. Use @username ou o ID numérico.
              </p>
            </div>

            {/* Pré-visualização da mensagem */}
            <div className="bg-[#0b0e11] border border-[#2b3139] rounded-xl p-4">
              <p className="text-[10px] text-gray-500 uppercase tracking-widest font-bold mb-3">Pré-visualização da mensagem de sinal</p>
              <pre className="text-[11px] font-mono text-gray-300 leading-relaxed whitespace-pre-wrap">
{`🟢 *SINAL LONG — BTCUSDT*

💰 *Entrada:* 71.426,00 USDT
🎯 *TP1:* 72.100,00  |  *TP2:* 72.800,00
🛑 *SL:*  70.900,00

🧠 *Certeza IA:* 91.50%
⚡ *Fluxo:* 78.3%

_LHN Sovereign V90 · Sinal automatizado_`}
              </pre>
            </div>

          </div>

          {/* Botões */}
          <div className="flex items-center gap-3 px-6 pb-6">
            <button
              onClick={salvar}
              disabled={saving}
              className="flex-1 flex items-center justify-center gap-2 bg-[#f0b90b] hover:bg-[#d4a017] text-black font-bold px-4 py-3 rounded-xl text-[13px] tracking-wide transition-all disabled:opacity-60 disabled:cursor-not-allowed"
            >
              <Save className="w-4 h-4" />
              {saving ? "Salvando..." : "Salvar Configuração"}
            </button>
            <button
              onClick={testar}
              disabled={testing || !cfg.transmissao_token || !cfg.transmissao_chat_id}
              className="flex-1 flex items-center justify-center gap-2 bg-[#161a1e] hover:bg-[#1e2329] border border-[#2b3139] hover:border-[#f0b90b]/50 text-gray-300 hover:text-[#f0b90b] font-bold px-4 py-3 rounded-xl text-[13px] tracking-wide transition-all disabled:opacity-40 disabled:cursor-not-allowed"
            >
              <Send className="w-4 h-4" />
              {testing ? "Enviando..." : "Enviar Teste"}
            </button>
          </div>
        </div>

        {/* Toast */}
        {toast && (
          <div className={`mt-4 flex items-start gap-3 p-4 rounded-xl border font-mono text-[12px] ${
            toast.type === "success"
              ? "bg-[#0ecb81]/10 border-[#0ecb81]/30 text-[#0ecb81]"
              : "bg-[#f6465d]/10 border-[#f6465d]/30 text-[#f6465d]"
          }`}>
            {toast.type === "success"
              ? <CheckCircle className="w-4 h-4 shrink-0 mt-0.5" />
              : <AlertCircle className="w-4 h-4 shrink-0 mt-0.5" />}
            <span>{toast.msg}</span>
          </div>
        )}

        {/* Aviso de isolamento */}
        <div className="mt-6 bg-[#131722] border border-[#2b3139] rounded-xl p-4 text-[11px] text-gray-500 font-mono">
          <strong className="text-gray-400">🔒 Isolamento garantido:</strong> Este bot e canal são exclusivos para sinais de trading. 
          As credenciais do Telegram pessoal (alertas de risco) estão configuradas em <em>Configuração Mestra → Telegram</em> e são completamente independentes.
        </div>

      </div>
    </div>
  );
}
