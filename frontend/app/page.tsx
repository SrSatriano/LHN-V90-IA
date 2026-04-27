"use client";

import React, { useState } from "react";
import { UnifiedSidebar } from "../components/UnifiedSidebar";
import { CryptoWorkspace } from "../components/CryptoWorkspace";
import { default as NewsFlow } from "./NewsFlow";
import OrderFlow from "./OrderFlow";
import LiquidityMap from "./LiquidityMap";
import { WebSocketProvider } from "./WebSocketContext";
import { SignalsHistory } from "../components/SignalsHistory";
import { TransmissionConfig } from "../components/TransmissionConfig";
import { MasterConfigForm } from "../components/MasterConfigForm";

export type TabType = "trading" | "terminal" | "ia" | "settings" | "horizonte" | "positions" | "news" | "history" | "orderflow" | "heatmap" | "sinais" | "transmissao";

export default function UnifiedAppDashboard() {
  const [activeTab, setActiveTab] = useState<TabType>("trading");

  return (
    <div className="flex min-h-0 min-w-0 w-full flex-1 flex-row overflow-hidden bg-[#0b0f19] font-sans text-gray-200">
      
      {/* 🟢 GLOBAL SIDEBAR (100% CRIPTO) */}
      <UnifiedSidebar 
        activeTab={activeTab} 
        setActiveTab={setActiveTab} 
      />

      {/* 🟢 MAIN WORKSPACE ORCHESTRATOR */}
      <WebSocketProvider>
      <main className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
        {activeTab !== "news" &&
          activeTab !== "orderflow" &&
          activeTab !== "heatmap" &&
          activeTab !== "settings" && (
          <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
            <CryptoWorkspace
              tabContext={activeTab}
              onOpenGovernance={() => setActiveTab("settings")}
            />
          </div>
        )}

        {activeTab === "settings" && (
          <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
            <MasterConfigForm />
          </div>
        )}

        {activeTab === "news" && (
          <div className="flex-1 h-full w-full overflow-y-auto custom-scrollbar relative z-10 bg-[#0b0f19]">
            <NewsFlow />
          </div>
        )}

        {activeTab === "orderflow" && (
          <div className="flex-1 h-full w-full overflow-hidden relative z-10 bg-[#0b0f19]">
            <OrderFlow />
          </div>
        )}

        {activeTab === "heatmap" && (
          <div className="flex-1 h-full w-full overflow-hidden relative z-10 bg-[#0b0f19]">
            <LiquidityMap />
          </div>
        )}

        {(activeTab as string) === "sinais" && (
          <div className="flex-1 h-full w-full overflow-hidden relative z-10 bg-[#0b0e11]">
            <SignalsHistory />
          </div>
        )}

        {(activeTab as string) === "transmissao" && (
          <div className="flex-1 h-full w-full overflow-hidden relative z-10 bg-[#0b0e11]">
            <TransmissionConfig />
          </div>
        )}
      </main>
      </WebSocketProvider>

    </div>
  );
}
