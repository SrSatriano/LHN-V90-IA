"use client";

import React, { useCallback, useEffect, useRef, useState } from "react";
import { Layers, RefreshCw, ZoomIn, ZoomOut, Filter, Settings } from "lucide-react";
import { useGlobalWebSocket } from "./WebSocketContext";

export default function LiquidityMap() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const prevDepthLenRef = useRef(0);
  const prevCanvasSizeRef = useRef({ width: 0, height: 0 });
  const prevMinSizeRef = useRef(0);
  const [resolution, setResolution] = useState<"10$"|"50$"|"100$">("50$");
  const [minSize, setMinSize] = useState<number>(0);
  const { isConnected: connected, latestMessage } = useGlobalWebSocket();
  const [depthData, setDepthData] = useState<any[]>([]);

  // Consome snapshots L2 do WS global (se disponível) e mantém fallback sintético.
  useEffect(() => {
    if (latestMessage?.orderbook && Array.isArray(latestMessage.orderbook)) {
      const timestamp = Date.now();
      const normalized = latestMessage.orderbook.slice(0, 40).map((level: any, i: number) => ({
        price: Number(level?.price ?? 0),
        size: Number(level?.size ?? 0),
        type: String(level?.type ?? (i < 20 ? "BID" : "ASK")).toUpperCase() === "ASK" ? "ASK" : "BID",
      }));
      setDepthData((prev) => {
        const next = [...prev, { timestamp, levels: normalized }];
        return next.slice(-60);
      });
    }
  }, [latestMessage]);

  useEffect(() => {
    // Mock depth updates
    const simInterval = setInterval(() => {
      const timestamp = Date.now();
      const mockLevels = Array.from({length: 40}).map((_, i) => ({
        price: 68000 + (i - 20) * (resolution === "10$" ? 10 : resolution === "50$" ? 50 : 100),
        size: Math.random() * 10 + (Math.abs(i - 20) < 5 ? 15 : 0), // More size near current price
        type: i < 20 ? "BID" : "ASK"
      }));
      
      setDepthData(prev => {
        const next = [...prev, { timestamp, levels: mockLevels }];
        return next.slice(-60); // Keep last 60 columns (history)
      });
    }, 1000);

    return () => {
      clearInterval(simInterval);
    };
  }, [resolution]);

  const drawColumn = useCallback(
    (
      ctx: CanvasRenderingContext2D,
      snapshot: any,
      x: number,
      colWidth: number,
      rowHeight: number,
      levelsCount: number
    ) => {
      if (!snapshot?.levels) return;
      snapshot.levels.forEach((level: any, rowIdx: number) => {
        if (Number(level.size) < minSize) return;
        const y = (levelsCount - 1 - rowIdx) * rowHeight;
        const intensity = Math.min(1, Number(level.size) / 25);
        let color = "rgba(11, 14, 17, 1)";
        if (intensity > 0) {
          color =
            level.type === "ASK"
              ? `rgba(246, 70, 93, ${intensity * 0.8})`
              : `rgba(14, 203, 129, ${intensity * 0.8})`;
        }
        if (Number(level.size) > 20) {
          color = `rgba(240, 185, 11, ${intensity})`;
        }
        ctx.fillStyle = color;
        ctx.fillRect(x, y, colWidth, rowHeight);
      });
    },
    [minSize]
  );

  // Desenho incremental: desloca histórico e renderiza apenas a nova coluna.
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || depthData.length === 0) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const nextWidth = canvas.parentElement?.clientWidth || 800;
    const nextHeight = canvas.parentElement?.clientHeight || 600;
    const resized =
      prevCanvasSizeRef.current.width !== nextWidth ||
      prevCanvasSizeRef.current.height !== nextHeight;
    if (resized) {
      ctx.canvas.width = nextWidth;
      ctx.canvas.height = nextHeight;
      prevCanvasSizeRef.current = { width: nextWidth, height: nextHeight };
    }

    const width = ctx.canvas.width;
    const height = ctx.canvas.height;

    const colWidth = width / 60; // Max 60 history columns
    const levelsCount = 40;
    const rowHeight = height / levelsCount;
    const dataReset =
      depthData.length < prevDepthLenRef.current ||
      prevDepthLenRef.current === 0 ||
      resized ||
      prevMinSizeRef.current !== minSize;

    if (dataReset) {
      ctx.fillStyle = "#0b0e11";
      ctx.fillRect(0, 0, width, height);
      depthData.forEach((snapshot, colIdx) => {
        drawColumn(ctx, snapshot, colIdx * colWidth, colWidth, rowHeight, levelsCount);
      });
    } else {
      ctx.drawImage(canvas, -colWidth, 0);
      ctx.fillStyle = "#0b0e11";
      ctx.fillRect(width - colWidth, 0, colWidth, height);
      drawColumn(
        ctx,
        depthData[depthData.length - 1],
        width - colWidth,
        colWidth,
        rowHeight,
        levelsCount
      );
    }

    prevDepthLenRef.current = depthData.length;
    prevMinSizeRef.current = minSize;

    // Draw Current Price Line (Mock center)
    ctx.strokeStyle = "rgba(255, 255, 255, 0.4)";
    ctx.setLineDash([5, 5]);
    ctx.beginPath();
    ctx.moveTo(0, height / 2);
    ctx.lineTo(width, height / 2);
    ctx.stroke();
    
  }, [depthData, minSize, drawColumn]);

  return (
    <div className="flex flex-col h-full bg-[#0b0e11] text-gray-200 font-sans border border-[#2b3139] m-2 rounded-xl overflow-hidden shadow-2xl animate-in fade-in duration-300">
      
      {/* Configuration Header */}
      <div className="h-16 flex items-center justify-between px-6 bg-[#161a1e] border-b border-[#2b3139] shrink-0">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-rose-900/30 rounded-lg">
            <Layers className="w-6 h-6 text-rose-400" />
          </div>
          <div>
            <h1 className="text-lg font-bold text-white tracking-wide">Mapa de Liquidez L2</h1>
            <p className="text-[11px] text-gray-500 uppercase tracking-widest font-mono">Heatmap Institucional & Order Book Depth</p>
          </div>
        </div>
        
        <div className="flex items-center gap-4 border border-[#2b3139] p-1.5 rounded-lg bg-[#0b0e11]">
          {/* Controls */}
          <div className="flex items-center gap-2 px-2 border-r border-[#2b3139] pr-4">
            <span className="text-xs font-bold text-gray-500 uppercase">Resolução</span>
            <select 
              value={resolution} 
              onChange={(e) => setResolution(e.target.value as any)}
              className="bg-[#1e2329] text-gray-200 border border-[#2b3139] rounded text-xs px-2 py-1 outline-none"
            >
              <option value="10$">10 USDT</option>
              <option value="50$">50 USDT</option>
              <option value="100$">100 USDT</option>
            </select>
          </div>
          
          <div className="flex items-center gap-2 px-2 border-r border-[#2b3139] pr-4">
            <Filter className="w-3.5 h-3.5 text-gray-500" />
            <input 
              type="number" 
              placeholder="Filtro Médio"
              value={minSize || ''}
              onChange={(e) => setMinSize(Number(e.target.value))}
              className="bg-[#1e2329] text-gray-200 border border-[#2b3139] rounded text-xs w-20 px-2 py-1 outline-none font-mono"
            />
          </div>

          <div className="flex items-center gap-2 px-2 border-r border-[#2b3139] pr-4">
            <button 
              className="hover:text-white text-gray-400 transition-colors p-1"
              title="Aumentar Zoom / Depth"
              onClick={() => console.log('Zoom In')}
            >
              <ZoomIn className="w-4 h-4" />
            </button>
            <button 
              className="hover:text-white text-gray-400 transition-colors p-1"
              title="Diminuir Zoom / Depth"
              onClick={() => console.log('Zoom Out')}
            >
              <ZoomOut className="w-4 h-4" />
            </button>
          </div>

          <button 
            className="flex items-center gap-1.5 px-3 py-1 bg-[#1e2329] hover:bg-[#2b3139] rounded text-xs font-bold text-gray-300 transition-colors mx-2"
            onClick={() => console.log('Reset Z-Axis')}
          >
            <RefreshCw className="w-3.5 h-3.5" />
            Reset Z
          </button>
          
          <div className="flex items-center gap-2 pl-4 border-l border-[#2b3139]">
            <div className={`w-2 h-2 rounded-full ${connected ? 'bg-[#0ecb81] shadow-[0_0_8px_rgba(14,203,129,0.5)]' : 'bg-[#f6465d] shadow-[0_0_8px_rgba(246,70,93,0.5)]'}`}></div>
            <span className="text-xs font-mono font-bold text-gray-400 mr-2">FEED L2</span>
          </div>
        </div>
      </div>

      {/* Main Content Area */}
      <div className="flex-1 flex overflow-hidden relative bg-[#0b0e11]">
        
        {/* Heatmap Canvas Area */}
        <div className="flex-1 relative w-full h-full overflow-hidden">
           <canvas 
             ref={canvasRef} 
             className="absolute top-0 left-0 w-full h-full block"
           />
           
           <div className="absolute top-4 left-4 z-10 pointer-events-none">
              <span className="text-xs font-bold font-mono text-gray-300 px-2 py-1 bg-[#161a1e]/80 border border-[#2b3139] rounded backdrop-blur-sm shadow-xl">BTCUSDT.P</span>
           </div>

           {!connected && (
             <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none bg-black/40 backdrop-blur-[1px]">
               <RefreshCw className="w-12 h-12 text-[#2b3139] mb-4 opacity-50 animate-spin" />
               <p className="text-gray-400 font-mono text-sm tracking-widest shadow-black mix-blend-difference">Sincronizando feed de liquidez (Level 2)...</p>
             </div>
           )}
        </div>
        
        {/* Right Sidebar: Quick Depth View (Scalper Book) */}
        <div className="w-[180px] border-l border-[#2b3139] bg-[#161a1e] flex flex-col shrink-0">
          <div className="p-2 border-b border-[#2b3139] text-center">
             <span className="text-[10px] font-bold uppercase tracking-widest text-gray-500">Order Book L2</span>
          </div>
          <div className="flex-1 overflow-y-auto custom-scrollbar p-2 space-y-0.5">
            {/* ASK Mock */}
            {Array.from({length: 15}).map((_, i) => (
              <div key={`ask-${i}`} className="flex justify-between items-center text-[10px] font-mono group hover:bg-[#2b3139]/50 px-1 rounded relative overflow-hidden">
                <div className="absolute right-0 top-0 bottom-0 bg-[#f6465d]/10 z-0" style={{ width: `${Math.random() * 100}%` }}></div>
                <span className="text-[#f6465d] z-10">{(68080 - i*10).toLocaleString()}</span>
                <span className="text-gray-400 z-10">{(Math.random() * 5).toFixed(3)}</span>
              </div>
            ))}
            
            {/* Spread / Mark Price */}
            <div className="py-2 flex flex-col items-center justify-center my-1 border-t border-b border-[#2b3139]/50 bg-[#1e2329]/50">
               <span className="text-[13px] font-bold text-[#f0b90b] font-mono">67,930.0</span>
               <span className="text-[9px] text-gray-500 uppercase">Spread: 0.1</span>
            </div>
            
            {/* BID Mock */}
            {Array.from({length: 15}).map((_, i) => (
              <div key={`bid-${i}`} className="flex justify-between items-center text-[10px] font-mono group hover:bg-[#2b3139]/50 px-1 rounded relative overflow-hidden">
                <div className="absolute left-0 top-0 bottom-0 bg-[#0ecb81]/10 z-0" style={{ width: `${Math.random() * 100}%` }}></div>
                <span className="text-gray-400 z-10">{(Math.random() * 5).toFixed(3)}</span>
                <span className="text-[#0ecb81] z-10">{(67920 - i*10).toLocaleString()}</span>
              </div>
            ))}
          </div>
        </div>

      </div>
    </div>
  );
}
