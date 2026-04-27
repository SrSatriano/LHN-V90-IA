"use client";

import React, { useState } from "react";
import {
  LayoutDashboard,
  Activity,
  Cpu,
  Terminal,
  Settings,
  ChevronLeft,
  ChevronRight,
  Briefcase,
  BarChart2,
  Layers,
  Radio,
  Send
} from "lucide-react";
import type { TabType } from "../app/page";

interface SidebarProps {
  activeTab: TabType;
  setActiveTab: (t: TabType) => void;
}

export function UnifiedSidebar({ activeTab, setActiveTab }: SidebarProps) {
  const [collapsed, setCollapsed] = useState(false);

  return (
    <aside
      className={`${
        collapsed ? "w-20" : "w-64"
      } bg-[#111827] border-r border-[#1f2937] transition-all duration-300 flex flex-col z-50 shrink-0 h-full overflow-y-auto custom-scrollbar`}
    >
      {/* Header */}
      <div className="h-16 flex items-center justify-center border-b border-[#2A2E39] shrink-0 sticky top-0 bg-[#0b0e11] z-10 shadow-sm">
        {!collapsed ? (
          <div className="flex items-center gap-2">
            {/* Nexus HFT V90: marca LHN Tech Holding no cabeçalho da sidebar (alinhado ao ícone da janela). */}
            <img
              src="/branding/LHN_tech_holding_202604251645.jpeg"
              alt="LHN Tech Holding"
              className="h-7 w-auto shrink-0 rounded object-contain shadow-sm shadow-cyan-900/30"
              draggable={false}
            />
            <span className="font-bold text-xl text-white tracking-widest shrink-0">LHN <span className="text-[#0ecb81]">V90</span></span>
          </div>
        ) : (
          <img
            src="/branding/LHN_tech_holding_202604251645.jpeg"
            alt="LHN"
            className="h-7 w-auto shrink-0 rounded object-contain shadow-sm shadow-cyan-900/30"
            draggable={false}
          />
        )}
      </div>

      <nav className="flex-1 p-3 space-y-6 mt-2">
        {/* === SESSÃO: PLATAFORMA V90 === */}
        <div>
          {!collapsed && (
            <h3 className="px-3 text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-2 border-b border-[#1f2937] pb-1">
              Plataforma V90 Exclusiva
            </h3>
          )}
          <div className="space-y-1">
            <button
              onClick={() => setActiveTab("trading")}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg transition-colors ${
                activeTab === "trading"
                  ? "bg-cyan-900/40 text-cyan-400 border border-cyan-800/50"
                  : "text-gray-400 hover:bg-[#1f2937] hover:text-gray-200"
              }`}
            >
              <LayoutDashboard className="w-5 h-5 shrink-0" />
              {!collapsed && <span className="font-medium text-sm">Painel Central</span>}
            </button>
            <button
              onClick={() => setActiveTab("ia")}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg transition-colors ${
                activeTab === "ia"
                  ? "bg-purple-900/40 text-purple-400 border border-purple-800/50"
                  : "text-gray-400 hover:bg-[#1f2937] hover:text-gray-200"
              }`}
            >
              <Cpu className="w-5 h-5 shrink-0" />
              {!collapsed && <span className="font-medium text-sm">Central IA Nexus</span>}
            </button>
            <button
              onClick={() => setActiveTab("horizonte")}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg transition-colors ${
                activeTab === "horizonte"
                  ? "bg-emerald-900/40 text-emerald-400 border border-emerald-800/50"
                  : "text-gray-400 hover:bg-[#1f2937] hover:text-gray-200"
              }`}
            >
              <Briefcase className="w-5 h-5 shrink-0" />
              {!collapsed && <span className="font-medium text-sm">Gestão Horizonte</span>}
            </button>
          </div>
        </div>

        {/* === SESSÃO: SISTEMA === */}
        <div>
          {!collapsed && (
            <h3 className="px-3 text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-2 border-b border-[#1f2937] pb-1">
              Sistema Operacional
            </h3>
          )}
          <div className="space-y-1">
            {/* NOVO: Fluxo de Ordens */}
            <button
              onClick={() => setActiveTab("orderflow")}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg transition-colors ${
                activeTab === "orderflow"
                  ? "bg-indigo-900/40 text-indigo-400 border border-indigo-800/50"
                  : "text-gray-400 hover:bg-[#1f2937] hover:text-gray-200"
              }`}
            >
              <BarChart2 className="w-5 h-5 shrink-0" />
              {!collapsed && <span className="font-medium text-sm">Fluxo de Ordens (VAP)</span>}
            </button>
            {/* NOVO: Mapa de Liquidez */}
            <button
              onClick={() => setActiveTab("heatmap")}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg transition-colors ${
                activeTab === "heatmap"
                  ? "bg-rose-900/40 text-rose-400 border border-rose-800/50"
                  : "text-gray-400 hover:bg-[#1f2937] hover:text-gray-200"
              }`}
            >
              <Layers className="w-5 h-5 shrink-0" />
              {!collapsed && <span className="font-medium text-sm">Mapa de Liquidez</span>}
            </button>
            <button
              onClick={() => setActiveTab("news")}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg transition-colors ${
                activeTab === "news"
                  ? "bg-orange-900/40 text-orange-400 border border-orange-800/50"
                  : "text-gray-400 hover:bg-[#1f2937] hover:text-gray-200"
              }`}
            >
              <Activity className="w-5 h-5 shrink-0" />
              {!collapsed && <span className="font-medium text-sm">Notícias (News Flow)</span>}
            </button>
            {/* Histórico de Sinais */}
            <button
              onClick={() => (setActiveTab as any)("sinais")}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg transition-colors ${
                (activeTab as string) === "sinais"
                  ? "bg-cyan-900/40 text-cyan-400 border border-cyan-800/50"
                  : "text-gray-400 hover:bg-[#1f2937] hover:text-gray-200"
              }`}
            >
              <Radio className="w-5 h-5 shrink-0" />
              {!collapsed && <span className="font-medium text-sm">Histórico de Sinais</span>}
            </button>
            {/* Transmissão Telegram */}
            <button
              onClick={() => (setActiveTab as any)("transmissao")}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg transition-colors ${
                (activeTab as string) === "transmissao"
                  ? "bg-violet-900/40 text-violet-400 border border-violet-800/50"
                  : "text-gray-400 hover:bg-[#1f2937] hover:text-gray-200"
              }`}
            >
              <Send className="w-5 h-5 shrink-0" />
              {!collapsed && <span className="font-medium text-sm">Transmissão Telegram</span>}
            </button>
            <button
              onClick={() => setActiveTab("terminal")}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg transition-colors ${
                activeTab === "terminal"
                  ? "bg-yellow-900/40 text-yellow-400 border border-yellow-800/50"
                  : "text-gray-400 hover:bg-[#1f2937] hover:text-gray-200"
              }`}
            >
              <Terminal className="w-5 h-5 shrink-0" />
              {!collapsed && <span className="font-medium text-sm">Log de Informações</span>}
            </button>
            <button
              onClick={() => setActiveTab("settings")}
              className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg transition-colors ${
                activeTab === "settings"
                  ? "bg-blue-900/40 text-blue-400 border border-blue-800/50"
                  : "text-gray-400 hover:bg-[#1f2937] hover:text-gray-200"
              }`}
            >
              <Settings className="w-5 h-5 shrink-0" />
              {!collapsed && <span className="font-medium text-sm">Configuração Mestra</span>}
            </button>
          </div>
        </div>
      </nav>

      {/* Collapse Action */}
      <div className="p-3 border-t border-[#1f2937] sticky bottom-0 bg-[#111827]">
        <button
          onClick={() => setCollapsed(!collapsed)}
          className="w-full flex items-center justify-center p-2 rounded-lg bg-[#1f2937] hover:bg-[#374151] text-gray-400 hover:text-white transition-colors border border-[#374151]"
        >
          {collapsed ? <ChevronRight className="w-5 h-5" /> : <ChevronLeft className="w-5 h-5" />}
        </button>
      </div>
    </aside>
  );
}
