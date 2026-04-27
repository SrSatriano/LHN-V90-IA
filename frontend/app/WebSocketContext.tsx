"use client";

import React, { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { getLhnWsProtocols, getLhnWsStreamUrl } from "@/lib/lhnAuth";

type StreamPayload = Record<string, any> | null;

type WebSocketContextValue = {
  isConnected: boolean;
  isReconnecting: boolean;
  latestMessage: StreamPayload;
  /** Anula o último tick WS (ex.: após ZERAR_STATS) até o próximo frame do servidor. */
  clearLatestMessage: () => void;
};

const WebSocketContext = createContext<WebSocketContextValue>({
  isConnected: false,
  isReconnecting: false,
  latestMessage: null,
  clearLatestMessage: () => {},
});

const MAX_BACKOFF_MS = 15000;
const INITIAL_BACKOFF_MS = 1000;

function toNumberOrKeep(value: any): any {
  if (typeof value === "string") {
    const maybeNum = Number(value);
    return Number.isFinite(maybeNum) ? maybeNum : value;
  }
  return value;
}

function sanitizeNonFiniteNumbers(obj: any): any {
  if (obj === null || typeof obj !== "object") {
    if (typeof obj === "number" && (!Number.isFinite(obj) || Number.isNaN(obj))) {
      return 0;
    }
    return obj;
  }
  if (Array.isArray(obj)) {
    return obj.map(sanitizeNonFiniteNumbers);
  }
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(obj)) {
    out[k] = sanitizeNonFiniteNumbers(v);
  }
  return out;
}

function normalizePayload(raw: any): StreamPayload {
  if (!raw || typeof raw !== "object") return null;
  const cleaned = sanitizeNonFiniteNumbers(raw);
  const next: Record<string, any> = { ...cleaned };
  for (const key of ["saldo", "pnl_liquido", "total_profit_usd", "total_loss_usd", "win_rate", "nlp_score", "sentiment_nlp"]) {
    next[key] = toNumberOrKeep(next[key]);
  }
  if (Array.isArray(next.operacoes_finalizadas)) {
    next.operacoes_finalizadas = next.operacoes_finalizadas.slice(0, 50).map((row: any) => {
      if (!row || typeof row !== "object") return row;
      const o = { ...row };
      for (const k of ["pnl", "pnl_pct", "lucro", "profit", "margem_gasta", "certeza"]) {
        if (k in o) (o as any)[k] = toNumberOrKeep((o as any)[k]);
      }
      return o;
    });
  }
  if (Array.isArray(next.historico)) {
    next.historico = next.historico.slice(0, 50).map((row: any) => {
      if (!row || typeof row !== "object") return row;
      const o = { ...row };
      for (const k of ["pnl", "pnl_pct", "lucro", "profit", "margem_gasta", "certeza"]) {
        if (k in o) (o as any)[k] = toNumberOrKeep((o as any)[k]);
      }
      return o;
    });
  }
  if (next.estatisticas && typeof next.estatisticas === "object") {
    const e = { ...next.estatisticas };
    const c = e.consolidado_finalizadas;
    if (c && typeof c === "object") {
      const cc = { ...c };
      for (const k of ["total_trades", "wins", "losses", "winrate", "pnl_liquido"]) {
        if (k in cc) (cc as any)[k] = toNumberOrKeep((cc as any)[k]);
      }
      e.consolidado_finalizadas = cc;
    }
    next.estatisticas = e;
  }
  if (Array.isArray(next.news)) {
    next.news = next.news.slice(0, 50);
  }
  return next;
}

export function WebSocketProvider({ children }: { children: React.ReactNode }) {
  const [isConnected, setIsConnected] = useState(false);
  const [isReconnecting, setIsReconnecting] = useState(false);
  const [latestMessage, setLatestMessage] = useState<StreamPayload>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);
  const backoffMsRef = useRef<number>(INITIAL_BACKOFF_MS);
  const cancelledRef = useRef(false);

  const clearLatestMessage = useCallback(() => {
    setLatestMessage(null);
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;

    const clearReconnectTimer = () => {
      if (reconnectTimerRef.current) {
        window.clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
    };

    const scheduleReconnect = () => {
      if (cancelledRef.current) return;
      clearReconnectTimer();
      const delay = Math.min(backoffMsRef.current, MAX_BACKOFF_MS);
      setIsReconnecting(true);
      reconnectTimerRef.current = window.setTimeout(() => {
        reconnectTimerRef.current = null;
        connect();
      }, delay);
      backoffMsRef.current = Math.min(delay * 2, MAX_BACKOFF_MS);
    };

    const connect = () => {
      if (cancelledRef.current) return;
      try {
        const protos = getLhnWsProtocols();
        const ws = protos?.length
          ? new WebSocket(getLhnWsStreamUrl(), protos)
          : new WebSocket(getLhnWsStreamUrl());
        wsRef.current = ws;
        ws.onopen = () => {
          setIsConnected(true);
          setIsReconnecting(false);
          backoffMsRef.current = INITIAL_BACKOFF_MS;
        };
        ws.onmessage = (event) => {
          try {
            const parsed = JSON.parse(event.data);
            setLatestMessage(normalizePayload(parsed));
          } catch (err) {
            console.error("Erro ao processar payload WS global", err);
          }
        };
        ws.onerror = () => {
          setIsConnected(false);
        };
        ws.onclose = () => {
          setIsConnected(false);
          scheduleReconnect();
        };
      } catch (err) {
        setIsConnected(false);
        scheduleReconnect();
        console.warn("Falha ao abrir WS global", err);
      }
    };

    connect();

    return () => {
      cancelledRef.current = true;
      clearReconnectTimer();
      setIsConnected(false);
      setIsReconnecting(false);
      try {
        wsRef.current?.close();
      } catch {
        // ignore close race during teardown
      }
    };
  }, []);

  const value = useMemo(
    () => ({
      isConnected,
      isReconnecting,
      latestMessage,
      clearLatestMessage,
    }),
    [isConnected, isReconnecting, latestMessage, clearLatestMessage]
  );

  return <WebSocketContext.Provider value={value}>{children}</WebSocketContext.Provider>;
}

export function useGlobalWebSocket() {
  return useContext(WebSocketContext);
}
