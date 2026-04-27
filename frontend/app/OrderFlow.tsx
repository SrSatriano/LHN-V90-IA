"use client";

import React, { useEffect, useRef, useState } from "react";
import { Activity, Shield, BarChart2 } from "lucide-react";
import { useGlobalWebSocket } from "./WebSocketContext";

export default function OrderFlow() {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const [tapeTrades, setTapeTrades] = useState<any[]>([]);
  const [agressaoNet, setAgressaoNet] = useState<{ time: string; value: number }[]>([]);
  const { isConnected: connected } = useGlobalWebSocket();
  
  // Simulated initial tape data
  useEffect(() => {
    const initialTrades = Array.from({ length: 20 }).map((_, i) => ({
      id: i,
      time: new Date(Date.now() - (20 - i) * 1000).toLocaleTimeString([], { hour12: false }),
      qty: (Math.random() * 5 + 0.1).toFixed(3),
      price: (68000 + Math.random() * 100 - 50).toFixed(1),
      agressor: Math.random() > 0.5 ? "BUY" : "SELL"
    })).reverse();
    setTapeTrades(initialTrades);
    
    // Simulated Net Aggression
    const initialAgg = Array.from({ length: 30 }).map((_, i) => ({
      time: new Date(Date.now() - (30 - i) * 5000).toLocaleTimeString([], { hour12: false }),
      value: Math.floor(Math.random() * 200 - 100)
    }));
    setAgressaoNet(initialAgg);
  }, []);

  // Simulated feed fallback (WS global é compartilhado no provider)
  useEffect(() => {
    // Fallback simulation tick
    const simInterval = setInterval(() => {
      const isBuy = Math.random() > 0.5;
      const newTrade = {
        id: Date.now(),
        time: new Date().toLocaleTimeString([], { hour12: false }),
        qty: (Math.random() * 2 + 0.01).toFixed(3),
        price: (68050 + Math.random() * 20 - 10).toFixed(1),
        agressor: isBuy ? "BUY" : "SELL"
      };
      
      setTapeTrades(prev => [newTrade, ...prev].slice(0, 100)); // Keep last 100 trades
      
      // Update aggression net randomly
      if (Math.random() > 0.7) {
        setAgressaoNet(prev => {
          const next = [...prev, {
            time: new Date().toLocaleTimeString([], { hour12: false }),
            value: Math.floor(Math.random() * 200 - 100)
          }];
          return next.slice(-30);
        });
      }
    }, 800);
    
    return () => {
      clearInterval(simInterval);
    };
  }, []);

  return (
    <div className="flex flex-col h-full bg-[#0b0e11] text-gray-200 font-sans border border-[#2b3139] m-2 rounded-xl overflow-hidden shadow-2xl animate-in fade-in duration-300">
      
      {/* Header */}
      <div className="h-16 flex items-center justify-between px-6 bg-[#161a1e] border-b border-[#2b3139] shrink-0">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-indigo-900/30 rounded-lg">
            <BarChart2 className="w-6 h-6 text-indigo-400" />
          </div>
          <div>
            <h1 className="text-lg font-bold text-white tracking-wide">Fluxo de Ordens (Order Flow)</h1>
            <p className="text-[11px] text-gray-500 uppercase tracking-widest font-mono">Volume at Price & Tape Reading</p>
          </div>
        </div>
        
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2 px-3 py-1.5 bg-[#0b0e11] rounded-lg border border-[#2b3139]">
            <div className={`w-2 h-2 rounded-full ${connected ? 'bg-[#0ecb81] shadow-[0_0_8px_rgba(14,203,129,0.5)]' : 'bg-[#f6465d] shadow-[0_0_8px_rgba(246,70,93,0.5)]'}`}></div>
            <span className="text-xs font-mono font-bold text-gray-400">WS_9002</span>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex overflow-hidden">
        
        {/* Left Side: Chart & Aggression */}
        <div className="flex-[3] flex flex-col border-r border-[#2b3139] overflow-hidden">
          
          {/* Main Chart Area (Candles + VAP) */}
          <div className="flex-[3] relative bg-[#0b0e11]">
            <div className="absolute top-4 left-4 z-10">
              <span className="text-xs font-bold font-mono text-gray-300 px-2 py-1 bg-[#161a1e] border border-[#2b3139] rounded">BTCUSDT.P</span>
            </div>
            {/* Placeholder for Lightweight Charts */}
            <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none">
               <Activity className="w-16 h-16 text-[#2b3139] mb-4 opacity-50" />
               <p className="text-gray-500 font-mono text-sm">Lightweight Charts Engine Loading...</p>
               <p className="text-gray-600 text-xs mt-2">Volume Profile (VAP) Overlay Active</p>
            </div>
          </div>
          
          <div className="h-1 bg-[#2b3139] cursor-ns-resize shrink-0"></div>
          
          {/* Bottom Chart: Net Aggression Histogram */}
          <div className="flex-[1] flex flex-col bg-[#161a1e] relative">
            <div className="absolute top-2 left-4 z-10 text-[10px] font-bold uppercase tracking-widest text-gray-500">
              Agressores Net (Delta V)
            </div>
            
            <div className="flex-1 p-4 flex items-end justify-start gap-1 overflow-hidden mt-6 relative">
              {/* Zero line */}
              <div className="absolute top-1/2 left-0 right-0 h-px bg-[#2b3139] z-0"></div>
              
              {agressaoNet.map((bar, i) => {
                const heightPct = Math.min(100, Math.abs(bar.value));
                const isPos = bar.value >= 0;
                return (
                  <div key={i} className="flex-1 relative h-full group">
                    <div 
                      className={`absolute left-1/2 -translate-x-1/2 w-[80%] rounded-sm transition-all duration-300 z-10
                        ${isPos ? 'bottom-1/2 bg-[#0ecb81] shadow-[0_0_10px_rgba(14,203,129,0.3)]' : 'top-1/2 bg-[#f6465d] shadow-[0_0_10px_rgba(246,70,93,0.3)]'}
                      `}
                      style={{ height: `${heightPct/2}%` }}
                    ></div>
                    {/* Tooltip */}
                    <div className="hidden group-hover:block absolute z-20 top-2 left-1/2 -translate-x-1/2 bg-[#0b0e11] border border-[#2b3139] p-1 rounded text-[10px] text-white font-mono whitespace-nowrap">
                       {bar.time}: {bar.value}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
          
        </div>

        {/* Right Side: Tape Reading (Histórico de Agressão) */}
        <div className="flex-[1] flex flex-col bg-[#161a1e] min-w-[320px]">
          <div className="h-10 border-b border-[#2b3139] flex items-center justify-between px-4 shrink-0 bg-[#0b0e11]">
            <span className="text-xs font-bold text-gray-300 uppercase tracking-widest flex items-center gap-2">
              <Activity className="w-3.5 h-3.5 text-indigo-400" /> Tape Reading
            </span>
          </div>
          
          <div className="flex-1 overflow-hidden flex flex-col">
            <table className="w-full text-left text-[11px] whitespace-nowrap table-fixed">
              <thead className="text-gray-500 font-mono sticky top-0 bg-[#161a1e] shadow-sm z-10">
                <tr>
                  <th className="font-normal py-2 px-3 w-1/4">Hora</th>
                  <th className="font-normal py-2 px-3 w-1/4 text-right">Preço</th>
                  <th className="font-normal py-2 px-3 w-1/4 text-right">Qtd</th>
                  <th className="font-normal py-2 px-3 w-1/4 text-center">Agr</th>
                </tr>
              </thead>
            </table>
            <div className="flex-1 overflow-y-auto custom-scrollbar">
              <table className="w-full text-left text-[11px] whitespace-nowrap font-mono table-fixed">
                <tbody>
                  {tapeTrades.map((trade) => {
                    const isBuy = trade.agressor === "BUY";
                    return (
                      <tr key={trade.id} className="hover:bg-[#1e2329] transition-colors border-b border-[#2b3139]/30">
                        <td className="py-2 px-3 w-1/4 text-gray-500">{trade.time}</td>
                        <td className={`py-2 px-3 w-1/4 text-right font-bold ${isBuy ? 'text-[#0ecb81]' : 'text-[#f6465d]'}`}>
                          {trade.price}
                        </td>
                        <td className="py-2 px-3 w-1/4 text-right text-gray-300">
                          {trade.qty}
                        </td>
                        <td className="py-2 px-3 w-1/4 text-center">
                          <span className={`${isBuy ? 'bg-[#0ecb81]/10 text-[#0ecb81]' : 'bg-[#f6465d]/10 text-[#f6465d]'} px-1.5 py-0.5 rounded text-[9px] border ${isBuy ? 'border-[#0ecb81]/20' : 'border-[#f6465d]/20'} font-bold`}>
                            {trade.agressor}
                          </span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </div>
        
      </div>
    </div>
  );
}
