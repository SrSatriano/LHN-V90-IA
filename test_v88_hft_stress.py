import asyncio
import os
import random
import statistics
import sys
import time
from types import MethodType

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_DIR = os.path.join(ROOT_DIR, "backend")
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

# Desativa sentinela nativo em teste
os.environ["LHN_DEV_MODE"] = "1"

from server import LHNSovereignV90 FINALBackend

LHN_Sovereign_Backend = LHNSovereignV90 FINALBackend


ASSETS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "1000PEPEUSDT", "LINKUSDT"]
TICK_SIZE_MAP = {
    "BTCUSDT": 0.10,
    "ETHUSDT": 0.01,
    "SOLUSDT": 0.001,
    "1000PEPEUSDT": 0.000001,
    "LINKUSDT": 0.001,
}
BASE_PRICE = {
    "BTCUSDT": 70000.0,
    "ETHUSDT": 3500.0,
    "SOLUSDT": 180.0,
    "1000PEPEUSDT": 0.009,
    "LINKUSDT": 20.0,
}


class MockModel:
    def predict(self, x_input, verbose=0):
        # Probabilidades altas para forcar eventos de sinal
        return [[0.92] for _ in range(len(x_input))]

    def __call__(self, x_input, training=False):
        return self.predict(x_input, verbose=0)

    def save(self, _):
        return None


class MockExchangeClient:
    def __init__(self):
        self.orders = []
        self.leverage_calls = []
        self._series = {asset: self._build_series(asset) for asset in ASSETS}
        self._futures_oi = {asset: 100000.0 for asset in ASSETS}

    def _build_series(self, asset, n=420):
        p = BASE_PRICE[asset]
        out = []
        now_ms = int(time.time() * 1000) - (n * 15 * 60 * 1000)
        for i in range(n):
            drift = 1.0 + (0.0008 if i % 2 == 0 else -0.0005)
            p = max(0.0000001, p * drift)
            o = p * (1 - 0.0005)
            h = p * (1 + 0.0012)
            l = p * (1 - 0.0015)
            c = p
            v = 1000.0 + i
            ct = now_ms + (i * 15 * 60 * 1000) + 899999
            qv = v * c
            tr = 120 + i
            tb = v * 0.52
            tq = qv * 0.51
            ign = 0
            out.append(
                [now_ms + (i * 15 * 60 * 1000), o, h, l, c, v, ct, qv, tr, tb, tq, ign]
            )
        return out

    def futures_exchange_info(self):
        return {
            "symbols": [
                {
                    "symbol": a,
                    "filters": [
                        {"filterType": "LOT_SIZE", "stepSize": "0.001"},
                        {
                            "filterType": "PRICE_FILTER",
                            "tickSize": f"{TICK_SIZE_MAP[a]:.12f}".rstrip("0").rstrip(
                                "."
                            ),
                        },
                    ],
                }
                for a in ASSETS
            ]
        }

    def futures_change_leverage(self, symbol, leverage):
        self.leverage_calls.append((symbol, leverage))
        return {"symbol": symbol, "leverage": leverage}

    def futures_create_order(self, **kwargs):
        self.orders.append(dict(kwargs))
        return {"status": "NEW", **kwargs}

    def futures_klines(self, symbol, interval="15m", limit=400):
        if symbol not in self._series:
            return []
        data = self._series[symbol]
        return data[-limit:]

    def get_historical_klines(self, symbol, interval, start_str=None, klines_type=None):
        if symbol not in self._series:
            return []
        if start_str is None:
            return self._series[symbol]
        start_val = int(start_str)
        return [row for row in self._series[symbol] if int(row[0]) >= start_val]

    def futures_premium_index(self, symbol):
        return {"lastFundingRate": "0.0001"}

    def futures_open_interest(self, symbol):
        cur = self._futures_oi.get(symbol, 100000.0)
        cur *= 1.0005
        self._futures_oi[symbol] = cur
        return {"openInterest": str(cur)}


def percentile(data, p):
    if not data:
        return 0.0
    if len(data) == 1:
        return data[0]
    data_sorted = sorted(data)
    k = (len(data_sorted) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(data_sorted) - 1)
    if f == c:
        return data_sorted[f]
    return data_sorted[f] + (data_sorted[c] - data_sorted[f]) * (k - f)


def is_price_normalized(price, tick_size, tol=1e-9):
    ticks = price / tick_size
    return abs(ticks - round(ticks)) <= tol


async def main():
    backend = LHN_Sovereign_Backend()
    backend.model = MockModel()
    backend.ia_treinada = True
    backend.is_searching = True
    backend.modo_real = True
    backend.cfg["use_mtf_neural"] = True
    backend.cfg["use_trailing_stop"] = True
    backend.cfg["use_kelly"] = True
    backend.cfg["use_ws_orders"] = False
    backend.cfg["use_mc_dropout"] = True
    backend.cfg["use_funding_filter"] = True
    backend.cfg["use_oi_filter"] = True
    backend.cfg["calibracao_temperatura"] = 0.35
    backend.tickers = ASSETS[:]

    mock_client = MockExchangeClient()
    backend.get_binance_client = MethodType(lambda self: mock_client, backend)

    price_state = {a: BASE_PRICE[a] for a in ASSETS}
    latencies_ms = []
    errors = 0
    ticks_total = 100 * len(ASSETS)
    kelly_violations = 0
    tick_norm_violations = 0
    checked_protection_orders = 0

    async def process_asset_tick(asset, cycle_idx):
        nonlocal errors, kelly_violations, tick_norm_violations, checked_protection_orders
        try:
            # Simula variacao de preco HF
            delta = random.uniform(-0.003, 0.004)
            price_state[asset] = max(0.0000001, price_state[asset] * (1.0 + delta))
            tick_price = price_state[asset]

            t_start_ns = time.perf_counter_ns()
            t_tick_ms = int(time.time() * 1000)
            backend.processar_precos_radar(
                {asset: tick_price}, tick_ts_map={asset: t_tick_ms}
            )

            feat = backend._sincronizar_e_extrair_features(asset, mock_client)
            if feat is not None and cycle_idx > 8:
                signal = "LONG" if (cycle_idx % 4 != 0) else "SHORT"
                certainty = 92.0 if signal == "LONG" else 88.0
                with backend._ops_lock:
                    keys_before = set(backend.operacoes_abertas.keys())
                    saldo_before = float(backend.saldo_atual)
                backend.disparar_ordem(
                    asset,
                    signal,
                    tick_price,
                    certainty,
                    backend.cfg.get("margem_entrada", 10.0),
                    feat["atr_val"],
                    tick_ts_ms=t_tick_ms,
                )
                with backend._ops_lock:
                    keys_after = set(backend.operacoes_abertas.keys())
                    new_keys = keys_after - keys_before
                    for k in new_keys:
                        op = backend.operacoes_abertas.get(k, {})
                        cap = saldo_before * 0.03 + 1e-9
                        if float(op.get("margem", 0.0)) > cap:
                            kelly_violations += 1

            t_end_ns = time.perf_counter_ns()
            latencies_ms.append((t_end_ns - t_start_ns) / 1_000_000.0)

            with backend._ops_lock:
                # Forca trailing para validar normalizacao de tick
                if asset in backend.operacoes_abertas:
                    op = backend.operacoes_abertas[asset]
                    forced_price = op["preco"] * (
                        1.015 if op["tipo"] == "LONG" else 0.985
                    )
                else:
                    forced_price = tick_price

            backend.processar_precos_radar(
                {asset: forced_price}, tick_ts_map={asset: int(time.time() * 1000)}
            )

            # Verifica normalizacao nas ordens de protecao enviadas a exchange mock
            recent_orders = (
                mock_client.orders[-6:] if len(mock_client.orders) >= 1 else []
            )
            for od in recent_orders:
                if od.get("symbol") != asset:
                    continue
                if od.get("type") in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
                    checked_protection_orders += 1
                    sp = od.get("stopPrice")
                    if sp is None or not is_price_normalized(
                        float(sp), TICK_SIZE_MAP[asset]
                    ):
                        tick_norm_violations += 1

        except Exception:
            errors += 1

    # 100 ciclos de estresse concorrente
    for cycle in range(100):
        await asyncio.gather(*(process_asset_tick(asset, cycle) for asset in ASSETS))

    processed = ticks_total - errors
    success_rate = (processed / ticks_total) * 100.0 if ticks_total else 0.0
    avg_latency = statistics.mean(latencies_ms) if latencies_ms else 0.0
    p99_latency = percentile(latencies_ms, 99.0)

    print("\n=== LHN Sovereign V90 FINAL - HFT Stress Report ===")
    print(f"{'Metric':<44} | {'Value'}")
    print("-" * 74)
    print(f"{'Ticks processados':<44} | {processed}/{ticks_total}")
    print(f"{'Taxa de sucesso de processamento':<44} | {success_rate:.2f}%")
    print(f"{'Media de latencia fim-a-fim (ms)':<44} | {avg_latency:.3f}")
    print(f"{'Latencia P99 (ms)':<44} | {p99_latency:.3f}")
    print(f"{'Violacoes Kelly/cap 3%':<44} | {kelly_violations}")
    print(f"{'Violacoes normalizacao TickSize (SL)':<44} | {tick_norm_violations}")
    print(f"{'Ordens protecao verificadas':<44} | {checked_protection_orders}")
    print(f"{'Ordens mock registradas':<44} | {len(mock_client.orders)}")

    print("\nIntegridade:")
    print(
        f"- Kelly Criterion aplicado corretamente: {'SIM' if kelly_violations == 0 else 'NAO'}"
    )
    print(
        f"- Normalizacao TickSize aplicada corretamente: {'SIM' if tick_norm_violations == 0 else 'NAO'}"
    )
    print("==============================================\n")

    # Encerramento limpo dos pools persistentes do backend
    backend.is_app_alive = False
    backend.security_thread_running = False
    if hasattr(backend, "_feature_executor") and backend._feature_executor:
        backend._feature_executor.shutdown(wait=False, cancel_futures=True)
    if hasattr(backend, "_train_executor") and backend._train_executor:
        backend._train_executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    asyncio.run(main())
