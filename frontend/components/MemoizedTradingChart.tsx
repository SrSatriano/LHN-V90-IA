"use client";

import React, {
  forwardRef,
  useCallback,
  useEffect,
  useImperativeHandle,
  useRef,
} from "react";
import {
  CandlestickSeries,
  ColorType,
  createChart,
  LineSeries,
} from "lightweight-charts";
import type { Time } from "lightweight-charts";
import { fetchWithAuth } from "@/lib/lhnAuth";

export type ChartOverlay = { tp?: number; sl?: number } | null;

export type MemoizedTradingChartHandle = {
  /** Nexus HFT V90: atualiza candle ao vivo sem setState no pai (Main Thread). */
  applyLiveTick: (sym: string, chartSymbol: string, price: number) => void;
};

type EnabledFlags = {
  sma20: boolean;
  ema9: boolean;
  ema21: boolean;
  bb: boolean;
};

function MemoizedTradingChartInner(
  {
    symbol,
    timeframe,
    enabledIndicators,
    overlay,
    watchlistReady,
  }: {
    symbol: string;
    timeframe: string;
    enabledIndicators: EnabledFlags;
    overlay: ChartOverlay;
    watchlistReady: boolean;
  },
  ref: React.Ref<MemoizedTradingChartHandle>
) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<ReturnType<typeof createChart> | null>(null);
  const seriesRef = useRef<ReturnType<
    ReturnType<typeof createChart>["addSeries"]
  > | null>(null);
  const lastCandleRef = useRef<{
    time: Time;
    open: number;
    high: number;
    low: number;
    close: number;
  } | null>(null);
  const lastCandleTimeRef = useRef<number>(0);
  const tradeLinesRef = useRef<any[]>([]);
  const symbolRef = useRef(symbol);
  symbolRef.current = symbol;

  useImperativeHandle(
    ref,
    () => ({
      applyLiveTick: (_sym: string, chartSymbol: string, price: number) => {
        if (chartSymbol !== symbolRef.current) return;
        const series = seriesRef.current;
        if (!series || !Number.isFinite(price)) return;
        const lc = lastCandleRef.current;
        if (lc) {
          const updatedCandle = {
            ...lc,
            high: Math.max(lc.high, price),
            low: Math.min(lc.low, price),
            close: price,
          };
          lastCandleRef.current = updatedCandle;
          series.update(updatedCandle);
        } else {
          const tickTime = Math.floor(Date.now() / 1000);
          const bootstrapCandle = {
            time: tickTime as Time,
            open: price,
            high: price,
            low: price,
            close: price,
          };
          lastCandleTimeRef.current = tickTime;
          lastCandleRef.current = bootstrapCandle;
          series.update(bootstrapCandle);
        }
      },
    }),
    []
  );

  useEffect(() => {
    const root = chartContainerRef.current;
    if (!root) return;
    const ro = new ResizeObserver(() => {
      try {
        if (root && chartRef.current) {
          chartRef.current.applyOptions({
            width: root.clientWidth,
            height: root.clientHeight,
          });
        }
      } catch {
        /* ignore */
      }
    });
    ro.observe(root);
    return () => ro.disconnect();
  }, []);

  useEffect(() => {
    let isDisposed = false;
    const el = chartContainerRef.current;
    if (!el) return;

    if (chartRef.current) {
      try {
        chartRef.current.remove();
      } catch {
        /* ignore */
      }
      chartRef.current = null;
      seriesRef.current = null;
    }

    const cw = el.clientWidth;
    const ch = el.clientHeight;
    const chart = createChart(el, {
      width: cw > 80 ? cw : 800,
      height: ch > 80 ? ch : 420,
      layout: {
        background: { type: ColorType.Solid, color: "#0b0f19" },
        textColor: "#9ca3af",
      },
      grid: {
        vertLines: { color: "rgba(42, 46, 57, 0.3)" },
        horzLines: { color: "rgba(42, 46, 57, 0.3)" },
      },
      timeScale: {
        timeVisible: true,
        secondsVisible: false,
      },
    });

    requestAnimationFrame(() => {
      if (isDisposed || !chartRef.current || !chartContainerRef.current) return;
      const w = chartContainerRef.current.clientWidth;
      const h = chartContainerRef.current.clientHeight;
      if (w > 80 && h > 80) {
        chartRef.current.applyOptions({ width: w, height: h });
      }
    });

    const series = chart.addSeries(CandlestickSeries, {
      upColor: "#089981",
      downColor: "#F23645",
      borderVisible: false,
      wickUpColor: "#089981",
      wickDownColor: "#F23645",
    });

    let smaSeries: any = null;
    let ema9Series: any = null;
    let ema21Series: any = null;
    let bbUpperSeries: any = null;
    let bbLowerSeries: any = null;

    if (enabledIndicators.sma20) {
      smaSeries = chart.addSeries(LineSeries, {
        color: "rgba(255, 193, 7, 0.8)",
        lineWidth: 2,
        crosshairMarkerVisible: false,
        priceLineVisible: false,
      });
    }
    if (enabledIndicators.ema9) {
      ema9Series = chart.addSeries(LineSeries, {
        color: "rgba(0, 210, 255, 0.8)",
        lineWidth: 2,
        crosshairMarkerVisible: false,
        priceLineVisible: false,
      });
    }
    if (enabledIndicators.ema21) {
      ema21Series = chart.addSeries(LineSeries, {
        color: "rgba(255, 61, 0, 0.8)",
        lineWidth: 2,
        crosshairMarkerVisible: false,
        priceLineVisible: false,
      });
    }
    if (enabledIndicators.bb) {
      bbUpperSeries = chart.addSeries(LineSeries, {
        color: "rgba(156, 39, 176, 0.5)",
        lineWidth: 1,
        lineStyle: 1,
        crosshairMarkerVisible: false,
        priceLineVisible: false,
      });
      bbLowerSeries = chart.addSeries(LineSeries, {
        color: "rgba(156, 39, 176, 0.5)",
        lineWidth: 1,
        lineStyle: 1,
        crosshairMarkerVisible: false,
        priceLineVisible: false,
      });
    }

    chartRef.current = chart;
    seriesRef.current = series;

    const fetchHistory = async () => {
      try {
        const res = await fetchWithAuth(
          `/api/history/${encodeURIComponent(symbol)}?interval=${encodeURIComponent(timeframe)}`
        );
        let data: any;
        try {
          data = await res.json();
        } catch {
          return;
        }
        if (isDisposed) return;
        if (data && Array.isArray(data)) {
          const formattedData = data.map((d: any) => ({
            time: Number(d.time) as Time,
            open: Number(d.open),
            high: Number(d.high),
            low: Number(d.low),
            close: Number(d.close),
          }));
          series.setData(formattedData);

          if (Object.values(enabledIndicators).some(Boolean)) {
            const smaData: any[] = [];
            const ema9Data: any[] = [];
            const ema21Data: any[] = [];
            const bbUpperData: any[] = [];
            const bbLowerData: any[] = [];
            const smaPeriod = 20;
            let ema9 = 0;
            let ema21 = 0;
            const multiplier9 = 2 / (9 + 1);
            const multiplier21 = 2 / (21 + 1);

            for (let i = 0; i < formattedData.length; i++) {
              const c = formattedData[i].close;
              const t = formattedData[i].time;
              if (enabledIndicators.ema9) {
                if (i === 0) ema9 = c;
                else ema9 = (c - ema9) * multiplier9 + ema9;
                ema9Data.push({ time: t, value: ema9 });
              }
              if (enabledIndicators.ema21) {
                if (i === 0) ema21 = c;
                else ema21 = (c - ema21) * multiplier21 + ema21;
                ema21Data.push({ time: t, value: ema21 });
              }
              if ((enabledIndicators.sma20 || enabledIndicators.bb) && i >= smaPeriod - 1) {
                let sum = 0;
                for (let j = 0; j < smaPeriod; j++) {
                  sum += formattedData[i - j].close;
                }
                const smaVal = sum / smaPeriod;
                if (enabledIndicators.sma20) smaData.push({ time: t, value: smaVal });
                if (enabledIndicators.bb) {
                  let devSum = 0;
                  for (let j = 0; j < smaPeriod; j++) {
                    devSum += Math.pow(formattedData[i - j].close - smaVal, 2);
                  }
                  const stdDev = Math.sqrt(devSum / smaPeriod);
                  bbUpperData.push({ time: t, value: smaVal + stdDev * 2 });
                  bbLowerData.push({ time: t, value: smaVal - stdDev * 2 });
                }
              }
            }
            if (smaSeries) smaSeries.setData(smaData);
            if (ema9Series) ema9Series.setData(ema9Data);
            if (ema21Series) ema21Series.setData(ema21Data);
            if (bbUpperSeries) bbUpperSeries.setData(bbUpperData);
            if (bbLowerSeries) bbLowerSeries.setData(bbLowerData);
          }

          if (formattedData.length > 0) {
            const last = formattedData[formattedData.length - 1];
            lastCandleTimeRef.current = Number(last.time);
            lastCandleRef.current = last;
          }
        }
      } catch {
        /* ignore */
      }
    };

    void fetchHistory();

    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current === chart) {
        chart.applyOptions({
          width: chartContainerRef.current.clientWidth,
          height: chartContainerRef.current.clientHeight,
        });
      }
    };
    window.addEventListener("resize", handleResize);
    return () => {
      isDisposed = true;
      window.removeEventListener("resize", handleResize);
      try {
        tradeLinesRef.current = [];
        if (chartRef.current === chart) {
          chartRef.current = null;
          seriesRef.current = null;
        }
        chart.remove();
      } catch {
        /* ignore */
      }
    };
  }, [symbol, timeframe, enabledIndicators]);

  useEffect(() => {
    const series = seriesRef.current;
    if (!series) return;
    for (const line of tradeLinesRef.current) {
      try {
        series.removePriceLine(line);
      } catch {
        /* ignore */
      }
    }
    tradeLinesRef.current = [];
    if (!overlay) return;
    if (typeof overlay.tp === "number") {
      tradeLinesRef.current.push(
        series.createPriceLine({
          price: overlay.tp,
          color: "#089981",
          lineWidth: 2,
          lineStyle: 1,
          title: "TP",
        })
      );
    }
    if (typeof overlay.sl === "number") {
      tradeLinesRef.current.push(
        series.createPriceLine({
          price: overlay.sl,
          color: "#F23645",
          lineWidth: 2,
          lineStyle: 1,
          title: "SL",
        })
      );
    }
  }, [overlay]);

  return (
    <div ref={chartContainerRef} className="relative h-full w-full flex-1">
      {!watchlistReady ? (
        <div className="absolute inset-0 z-0 flex flex-col items-center justify-center bg-[#0b0e11] text-gray-500">
          <div className="mb-4 h-12 w-12 animate-spin rounded-full border-4 border-[#2b3139] border-t-[#0ecb81]" />
          <span className="font-mono text-sm tracking-widest text-[#0ecb81] animate-pulse shadow-black">
            CALIBRANDO GRÁFICOS TV...
          </span>
        </div>
      ) : null}
    </div>
  );
}

const MemoizedTradingChartForwarded = forwardRef(MemoizedTradingChartInner);

export const MemoizedTradingChart = React.memo(
  MemoizedTradingChartForwarded,
  (prev, next) =>
    prev.symbol === next.symbol &&
    prev.timeframe === next.timeframe &&
    prev.watchlistReady === next.watchlistReady &&
    prev.enabledIndicators.sma20 === next.enabledIndicators.sma20 &&
    prev.enabledIndicators.ema9 === next.enabledIndicators.ema9 &&
    prev.enabledIndicators.ema21 === next.enabledIndicators.ema21 &&
    prev.enabledIndicators.bb === next.enabledIndicators.bb &&
    prev.overlay?.tp === next.overlay?.tp &&
    prev.overlay?.sl === next.overlay?.sl
);

MemoizedTradingChart.displayName = "MemoizedTradingChart";
