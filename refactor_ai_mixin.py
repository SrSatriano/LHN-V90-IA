import asyncio
import re

file_path = "D:\\Desenvolvimento\\IA de Finanças\\LHN_Sovereign_Collection-Git\\LHN_Sovereign_V90 - Backup\\backend\\ai_mixin.py"
with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

# Locate the exact start and end index
start_marker = "    def _reiniciar_thread_analise_neural(self):"
end_marker = "    def disparar_ordem("

start_idx = content.find(start_marker)
end_idx = content.find(end_marker, start_idx)

if start_idx == -1 or end_idx == -1:
    print(f"Error finding markers. start: {start_idx}, end: {end_idx}")
    exit(1)

new_content = """    def _reiniciar_thread_analise_neural(self):
        \"\"\"Watchdog: nova geração do loop; o thread antigo sai ao detectar troca de geração.\"\"\"
        with self._analise_restart_lock:
            self._analise_generation = int(getattr(self, "_analise_generation", 0)) + 1
        
        loop = getattr(self, "loop_async", None)
        if loop and loop.is_running():
            import asyncio
            asyncio.run_coroutine_threadsafe(self.loop_analise_neural_async(), loop)
        else:
            self.log_msg("⚠️ [MOTOR] loop_async indisponível. Fallback para Thread isolada bloqueado no V90 Omniscience.")

    def _watchdog_loop_pulso_neural(self):
        \"\"\"Cão de guarda: motor RUNNING e sem pulso >300s → alerta e reinicia loop_analise_neural_async.\"\"\"
        import time
        while getattr(self, "is_app_alive", True):
            try:
                time.sleep(60)
                if not getattr(self, "estrategia_rodando", False):
                    continue
                pulse = getattr(self, "_ultima_pulse_neural_ts", None)
                if pulse is None:
                    continue
                if time.time() - float(pulse) <= 300.0:
                    continue
                now = time.time()
                if (
                    now
                    - float(getattr(self, "_last_watchdog_neural_restart_ts", 0) or 0)
                    < 60.0
                ):
                    continue
                self._last_watchdog_neural_restart_ts = now
                self.log_msg(
                    "⚠️ [WATCHDOG] Pulso neural ausente > 300s (motor RUNNING) — reiniciando loop_analise_neural_async."
                )
                self._reiniciar_thread_analise_neural()
            except Exception:
                import logging
                logger = logging.getLogger("LHN_Engine")
                logger.exception("watchdog_loop_pulso_neural")

    async def loop_analise_neural_async(self):
        import asyncio
        import math
        import pandas as pd
        import numpy as np
        import time
        from datetime import datetime

        my_gen = int(getattr(self, "_analise_generation", 0))
        client = await asyncio.to_thread(self.get_bybit_client)
        while getattr(self, "is_app_alive", True):
            self._ultima_pulse_neural_ts = time.time()
            if int(getattr(self, "_analise_generation", 0)) != my_gen:
                self.log_msg(
                    "🔄 [IA] Geração do loop neural obsoleta — encerrando task antiga."
                )
                return
            if not getattr(self, "is_searching", False) or not self.ia_treinada:
                self._ultima_varredura_ts = time.time()
                await asyncio.sleep(1)
                continue
            if not getattr(self, "modo_sniper_liberado", True):
                self._ultima_varredura_ts = time.time()
                await asyncio.sleep(2)
                continue

            try:
                cycle_sinais = 0
                cycle_ordens = 0
                cycle_consensus_blocked = 0
                cycle_btc_blocked = 0
                cycle_certeza_baixa = 0
                cycle_smart_exits = 0

                t_cycle_start = time.time()
                features_to_predict, ativos_processados = [], []
                current_tickers = list(getattr(self, "tickers", []))
                
                async def extrair_feature_async(a):
                    try:
                        return await asyncio.to_thread(
                            self._sincronizar_e_extrair_features, a, client
                        )
                    except Exception as e:
                        return None
                        
                resultados = await asyncio.gather(*(extrair_feature_async(a) for a in current_tickers))
                
                for res in resultados:
                    if res:
                        features_to_predict.append(res["features"])
                        ativos_processados.append(res)
                        
                if not features_to_predict:
                    await asyncio.sleep(1)
                    continue

                X_input = np.array(features_to_predict)
                use_mc_dropout = bool(self.cfg.get("use_mc_dropout", False))
                pred_std = None
                n_batch = len(ativos_processados)
                m_sniper = getattr(self, "model_sniper", None) or self.model
                m_lateral = getattr(self, "model_lateral", None)
                lateral_ok = m_lateral is not None

                idx_lat, idx_snp = [], []
                for j, res in enumerate(ativos_processados):
                    adx_atual = float(res.get("adx_val", 25.0))
                    if adx_atual < 25.0 and lateral_ok:
                        idx_lat.append(j)
                    else:
                        idx_snp.append(j)

                def _run_batch_predict():
                    if use_mc_dropout:
                        samples = max(
                            2,
                            min(5, int(self.cfg.get("mc_dropout_samples", 5))),
                        )
                        mc_preds = []
                        for _ in range(samples):
                            layer = np.zeros((n_batch, 1))
                            if idx_lat:
                                layer[idx_lat] = np.array(
                                    m_lateral(X_input[idx_lat], training=True)
                                )
                            if idx_snp:
                                layer[idx_snp] = np.array(
                                    m_sniper(X_input[idx_snp], training=True)
                                )
                            mc_preds.append(layer)
                        _predictions = np.mean(mc_preds, axis=0)
                        _pred_std = np.std(mc_preds, axis=0)
                        return _predictions, _pred_std
                    else:
                        _predictions = np.zeros((n_batch, 1))
                        if idx_lat:
                            _predictions[idx_lat] = m_lateral.predict(
                                X_input[idx_lat], verbose=0
                            )
                        if idx_snp:
                            _predictions[idx_snp] = m_sniper.predict(
                                X_input[idx_snp], verbose=0
                            )
                        return _predictions, None

                predictions, pred_std = await asyncio.to_thread(_run_batch_predict)

                bias_btc = 0
                if self.cfg.get("escudo_btc", True):
                    try:
                        k_btc = await asyncio.to_thread(
                            self._binance_futures_klines_safe,
                            client,
                            symbol="BTCUSDT",
                            interval="15m",
                            limit=50,
                        )
                        if k_btc is not None:
                            df_btc = pd.DataFrame(
                                k_btc,
                                columns=[
                                    "t", "o", "h", "l", "c", "v",
                                    "ct", "qv", "tr", "tb", "tq", "i",
                                ],
                            ).astype(float)
                            bias_btc = (
                                1
                                if df_btc["c"].ewm(span=9).mean().iloc[-1]
                                > df_btc["c"].ewm(span=21).mean().iloc[-1]
                                else -1
                            )
                    except Exception:
                        pass

                segundo_atual = datetime.now().second

                for i, res in enumerate(ativos_processados):
                    ativo, df, p = res["ativo"], res["df"], res["p"]
                    temperatura = float(
                        self.cfg.get("calibracao_temperatura", 0.35) or 0.35
                    )
                    temperatura = max(0.05, temperatura)
                    prob_ia = (
                        1
                        / (
                            1
                            + math.exp(
                                -max(
                                    min(
                                        (float(predictions[i][0]) - 0.5) / temperatura,
                                        50,
                                    ),
                                    -50,
                                )
                            )
                        )
                    ) * 100
                    self.ia_cache_probs[ativo] = prob_ia
                    while len(self.ia_cache_probs) > 200:
                        self.ia_cache_probs.pop(next(iter(self.ia_cache_probs)))
                    if pred_std is not None:
                        unc = float(pred_std[i][0])
                        if unc > float(self.cfg.get("mc_uncertainty_max", 0.35)):
                            cycle_certeza_baixa += 1
                            continue

                    import threading
                    threading.Thread(
                        target=self.salvar_estado_db,
                        args=(
                            ativo,
                            res["features"],
                            1 if prob_ia > 50 else 0,
                            "SNIPER_V90 FINAL",
                        ),
                        daemon=True,
                    ).start()

                    votos_long, votos_short = 0, 0
                    delta_rsi = df["c"].diff()
                    rsi = (
                        100
                        - (
                            100
                            / (
                                1
                                + (
                                    delta_rsi.where(delta_rsi > 0, 0)
                                    .rolling(window=self.cfg["rsi_p"])
                                    .mean()
                                    / -delta_rsi.where(delta_rsi < 0, 0)
                                    .rolling(window=self.cfg["rsi_p"])
                                    .mean()
                                    .replace(0, 1)
                                )
                            )
                        )
                    ).iloc[-1]
                    if rsi < self.cfg["rsi_os"]:
                        votos_long += 1
                    elif rsi > self.cfg["rsi_ob"]:
                        votos_short += 1

                    val_emas = [
                        df["c"].ewm(span=self.cfg["emas"][j]).mean().iloc[-1]
                        for j in range(self.cfg["ema_count"])
                    ]
                    if (
                        all(
                            val_emas[j] > val_emas[j + 1]
                            for j in range(len(val_emas) - 1)
                        )
                        if len(val_emas) > 1
                        else (p > val_emas[0])
                    ):
                        votos_long += 1
                    elif (
                        all(
                            val_emas[j] < val_emas[j + 1]
                            for j in range(len(val_emas) - 1)
                        )
                        if len(val_emas) > 1
                        else (p < val_emas[0])
                    ):
                        votos_short += 1

                    if res["macd_val"] > res["macd_sig"]:
                        votos_long += 1
                    elif res["macd_val"] < res["macd_sig"]:
                        votos_short += 1

                    if res["vol_ratio"] > 120:
                        if votos_long > votos_short:
                            votos_long += 1
                        elif votos_short > votos_long:
                            votos_short += 1

                    with self._ops_lock:
                        if ativo in self.operacoes_abertas:
                            op = self.operacoes_abertas[ativo]
                            op["ia_prob"] = prob_ia
                            if self.cfg.get("smart_exit", True):
                                if (op["tipo"] == "LONG" and prob_ia < 20.0) or (
                                    op["tipo"] == "SHORT" and prob_ia > 80.0
                                ):
                                    op["fechar_agora"] = "SAÍDA INTELIGENTE"
                                    cycle_smart_exits += 1
                            continue

                        if len(getattr(self, "operacoes_abertas", {})) >= int(
                            self.cfg.get("max_operacoes_simultaneas", 5)
                        ):
                            continue

                    sinal = (
                        "LONG"
                        if votos_long >= self.cfg["confluencia_min"]
                        else (
                            "SHORT"
                            if votos_short >= self.cfg["confluencia_min"]
                            else None
                        )
                    )
                    if sinal:
                        # V90 FINAL.6: Candle-Sync Entry
                        if segundo_atual < 58:
                            continue
                        cycle_sinais += 1
                        certeza = prob_ia if sinal == "LONG" else (100 - prob_ia)
                        id_sinal = f"{ativo}_{int(df['t'].iloc[-1])}"
                        if id_sinal in self.historico_sinais_vela:
                            continue
                        if certeza < self.cfg.get("winrate_minimo", 80.0):
                            cycle_certeza_baixa += 1
                            continue
                        if hasattr(
                            self, "validar_consenso_triplo"
                        ) and not self.validar_consenso_triplo(ativo, sinal, certeza):
                            cycle_consensus_blocked += 1
                            continue
                        if self.cfg.get("use_funding_filter", False) or self.cfg.get(
                            "use_oi_filter", False
                        ):
                            f_market = self._coletar_filtros_mercado(ativo, client)
                            if self.cfg.get("use_funding_filter", False):
                                fr = float(f_market.get("funding_rate", 0.0))
                                if (sinal == "LONG" and fr > 0.0008) or (
                                    sinal == "SHORT" and fr < -0.0008
                                ):
                                    cycle_consensus_blocked += 1
                                    continue
                            if self.cfg.get("use_oi_filter", False):
                                oi_delta = float(f_market.get("oi_delta_pct", 0.0))
                                if (sinal == "LONG" and oi_delta < 0.0) or (
                                    sinal == "SHORT" and oi_delta > 0.0
                                ):
                                    cycle_consensus_blocked += 1
                                    continue
                        if self.cfg.get("escudo_btc", True) and (
                            (sinal == "LONG" and bias_btc == -1)
                            or (sinal == "SHORT" and bias_btc == 1)
                        ):
                            cycle_btc_blocked += 1
                            continue

                        tick_ts_ms = None
                        with self._ops_lock:
                            tick_ts_ms = getattr(self, "tick_timestamps", {}).get(ativo)
                        self.disparar_ordem(
                            ativo,
                            sinal,
                            p,
                            certeza,
                            self.cfg["margem_entrada"],
                            res["atr_val"],
                            tick_ts_ms=tick_ts_ms,
                        )
                        cycle_ordens += 1
                        self.historico_sinais_vela.append(id_sinal)
                        if len(self.historico_sinais_vela) > 500:
                            self.historico_sinais_vela = self.historico_sinais_vela[-250:]

                lateral_count, trending_count = 0, 0
                if hasattr(self, "get_regime_summary"):
                    lateral_count, trending_count = self.get_regime_summary()

                filtered_total = (
                    len(current_tickers) - len(ativos_processados)
                    if "current_tickers" in dir()
                    else 0
                )
                parts = [
                    f"Radar: {len(ativos_processados) if 'ativos_processados' in dir() else 0}/{len(current_tickers) if 'current_tickers' in dir() else 0} ativos"
                ]
                if lateral_count > 0:
                    parts.append(f"Lateral: {lateral_count}")
                if trending_count > 0:
                    parts.append(f"Trend: {trending_count}")
                if cycle_sinais > 0:
                    parts.append(f"Sinais: {cycle_sinais}")
                if cycle_ordens > 0:
                    parts.append(f"Ordens: {cycle_ordens}")
                if cycle_consensus_blocked > 0:
                    parts.append(f"Bloq.Consenso: {cycle_consensus_blocked}")
                if cycle_btc_blocked > 0:
                    parts.append(f"Bloq.BTC: {cycle_btc_blocked}")
                if cycle_certeza_baixa > 0:
                    parts.append(f"Certeza<Min: {cycle_certeza_baixa}")
                if cycle_smart_exits > 0:
                    parts.append(f"SmartExit: {cycle_smart_exits}")
                with self._ops_lock:
                    ops_count = len(getattr(self, "operacoes_abertas", {}))
                parts.append(f"Abertos: {ops_count}/3")

                self.log_msg(f"🔍 {' | '.join(parts)}")
                if cycle_ordens == 0:
                    hb_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.log_msg(
                        f"[{hb_ts}] 💓 Heartbeat: Motor LHN Nominal. Varredura concluída (Nenhuma confluência detectada)."
                    )

                elapsed_cycle = time.time() - t_cycle_start
                wait_s = max(0.0, 60.0 - elapsed_cycle)
                
                if not hasattr(self, "_stop_event_analise_async"):
                    self._stop_event_analise_async = asyncio.Event()
                
                try:
                    await asyncio.wait_for(self._stop_event_analise_async.wait(), timeout=wait_s)
                    self._stop_event_analise_async.clear()
                except asyncio.TimeoutError:
                    pass

            except Exception as e:
                import traceback
                self.log_msg(f"Erro Crítico no Córtex: {e}\\n{traceback.format_exc()}")
                continue
            finally:
                self._ultima_varredura_ts = time.time()

"""

content = content[:start_idx] + new_content + content[end_idx:]

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)

print(f"Successfully refactored loop_analise_neural to async in {file_path}")
