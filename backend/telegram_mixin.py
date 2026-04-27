import asyncio
import json
import logging
import os
import re
import time

import aiohttp

logger = logging.getLogger(__name__)


class TelegramMixin:
    _MARKDOWN_V2_SPECIAL_RE = re.compile(r"([_\*\[\]\(\)~`>#+\-=|{}\.!])")

    def _escape_markdown_v2(self, valor) -> str:
        """Escapa todos os caracteres especiais exigidos pelo MarkdownV2."""
        return self._MARKDOWN_V2_SPECIAL_RE.sub(
            r"\\\1", str("" if valor is None else valor)
        )

    def _safe_float_telemetry(self, valor, default: float = 0.0) -> float:
        if valor is None:
            return float(default)
        try:
            return float(valor)
        except (TypeError, ValueError):
            return float(default)

    # ─────────────────────────────────────────────────────────────────────────
    # 1. Alerta pessoal do operador (configuração da Configuração Mestra)
    # ─────────────────────────────────────────────────────────────────────────
    async def enviar_alerta_telegram(self, mensagem: str) -> None:
        token = getattr(self, "cfg", {}).get("telegram_token")
        chat_id = getattr(self, "cfg", {}).get("telegram_chat_id")

        if not token or not chat_id:
            logger.debug("[TELEGRAM] Chaves ausentes no cofre. Alerta ignorado.")
            return

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": self._escape_markdown_v2(mensagem),
            "parse_mode": "MarkdownV2",
        }
        timeout = aiohttp.ClientTimeout(total=5)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=timeout) as response:
                    if response.status != 200:
                        erro_txt = await response.text()
                        logger.error(
                            f"[TELEGRAM ERROR] Falha na API: {response.status} - {erro_txt}"
                        )
        except Exception as e:
            logger.error(f"[TELEGRAM EXCEPTION] Falha de rota: {e}")

    def formatar_alerta_fechamento_operacao(
        self, symbol: str, pnl_usd: float, pnl_pct: float, motivo: str
    ) -> str:
        pnl_val = self._safe_float_telemetry(pnl_usd)
        pnl_pct_val = self._safe_float_telemetry(pnl_pct)
        if pnl_val > 0:
            return (
                f"🟢 [OPERAÇÃO ENCERRADA - WIN]\n"
                f"Ativo: {symbol}\n"
                f"Lucro: +{pnl_val:.2f} USDT (+{pnl_pct_val:.2f}%)\n"
                f"Motivo: {motivo}"
            )
        return (
            f"🔴 [OPERAÇÃO ENCERRADA - LOSS]\n"
            f"Ativo: {symbol}\n"
            f"Prejuízo: -{abs(pnl_val):.2f} USDT ({pnl_pct_val:.2f}%)\n"
            f"Motivo: {motivo}"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # 2. Transmissão de Sinais — bot dedicado (completamente isolado)
    # ─────────────────────────────────────────────────────────────────────────
    def _get_transmissao_cfg(self) -> dict:
        """Lê a configuração de transmissão do arquivo dedicado (não do config pessoal)."""
        path = getattr(self, "_arquivo_transmissao_cfg", None)
        if not path:
            workspace = getattr(self, "path_dados", "") or ""
            path = os.path.join(workspace, "LHN_TRANSMISSAO_CFG.json")
            self._arquivo_transmissao_cfg = path
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, TypeError, ValueError):
                logger.exception("[TRANSMISSAO] JSON inválido ao ler config local")
            except Exception:
                logger.exception("[TRANSMISSAO] Falha ao ler config local")
        return {
            "transmissao_ativa": False,
            "transmissao_token": "",
            "transmissao_chat_id": "",
        }

    def salvar_transmissao_cfg(self, cfg: dict) -> None:
        """Persiste a configuração de transmissão no arquivo dedicado."""
        path = getattr(self, "_arquivo_transmissao_cfg", None)
        if not path:
            workspace = getattr(self, "path_dados", "") or ""
            path = os.path.join(workspace, "LHN_TRANSMISSAO_CFG.json")
            self._arquivo_transmissao_cfg = path
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"[TRANSMISSAO] Falha ao salvar config: {e}")

    async def transmitir_sinal_telegram(
        self,
        symbol: str,
        acao: str,  # "LONG" | "SHORT"
        preco_entrada: float,
        certeza: float,
        sl: float | None = None,
        tp1: float | None = None,
        tp2: float | None = None,
        tp3: float | None = None,
        fluxo: float | None = None,
    ) -> bool:
        """
        Transmite um sinal confirmado pela IA para o canal/grupo de sinais dedicado.
        Usa o bot de transmissão (ISOLADO do telegram pessoal do operador).
        Retorna True se enviado com sucesso, False caso contrário.
        """
        cfg = self._get_transmissao_cfg()
        if not cfg.get("transmissao_ativa"):
            return False

        token = cfg.get("transmissao_token", "")
        chat_id = cfg.get("transmissao_chat_id", "")
        if not token or not chat_id:
            logger.debug("[TRANSMISSAO] Bot de sinais não configurado. Sinal ignorado.")
            return False

        certeza_val = self._safe_float_telemetry(certeza)
        preco_entrada_val = self._safe_float_telemetry(preco_entrada)
        acao_txt = str(acao or "").upper()
        emoji_acao = "🟢" if acao_txt == "LONG" else "🔴"
        par_fmt = str(symbol or "").replace("USDT", "/USDT")

        linhas = [
            f"{emoji_acao} *SINAL {acao_txt} — {par_fmt}*",
            "",
            f"💰 *Entrada:* {preco_entrada_val:,.4f} USDT",
        ]

        # TPs
        for i, tp in enumerate([tp1, tp2, tp3], start=1):
            tp_val = self._safe_float_telemetry(tp)
            if tp_val > 0:
                linhas.append(f"🎯 *TP{i}:* {tp_val:,.4f}")

        sl_val = self._safe_float_telemetry(sl)
        if sl_val > 0:
            linhas.append(f"🛑 *SL:*  {sl_val:,.4f}")

        linhas += [
            "",
            f"🧠 *Certeza IA:* {certeza_val:.2f}%",
        ]
        if fluxo is not None:
            fluxo_val = self._safe_float_telemetry(fluxo)
            linhas.append(f"⚡ *Fluxo:* {fluxo_val:.1f}%")

        linhas += [
            "",
            f"🕐 {time.strftime('%d/%m/%Y %H:%M:%S')}",
            "_LHN Sovereign V90 · Sinal automatizado_",
        ]

        mensagem = "\n".join(linhas)
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": self._escape_markdown_v2(mensagem),
            "parse_mode": "MarkdownV2",
        }
        timeout = aiohttp.ClientTimeout(total=8)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=timeout) as response:
                    if response.status == 200:
                        logger.info(
                            f"[TRANSMISSAO] Sinal {acao} {symbol} enviado ({certeza_val:.2f}%)"
                        )
                        return True
                    else:
                        erro_txt = await response.text()
                        logger.error(
                            f"[TRANSMISSAO ERROR] {response.status}: {erro_txt}"
                        )
                        return False
        except Exception as e:
            logger.error(f"[TRANSMISSAO EXCEPTION] {e}")
            return False

    async def enviar_teste_transmissao(self) -> dict:
        """Envia mensagem de teste para validar a configuração do bot de transmissão."""
        cfg = self._get_transmissao_cfg()
        token = (cfg.get("transmissao_token") or "").strip()
        chat_id_raw = cfg.get("transmissao_chat_id", "")
        chat_id = str(chat_id_raw).strip()

        if not token or not chat_id:
            return {"ok": False, "error": "Token ou Chat ID ausente."}

        if ":" in token:
            bot_numeric_id = token.split(":", 1)[0].strip()
            if chat_id == bot_numeric_id:
                return {
                    "ok": False,
                    "error": (
                        "Chat ID inválido: não use o número do início do token do bot "
                        f"({bot_numeric_id}). Esse número é o ID do bot no Telegram, não o chat de destino. "
                        "Use @userinfobot para ver o seu ID de utilizador, ou envie /start ao bot e "
                        "obtenha o chat_id correto; em canais o ID costuma começar por -100."
                    ),
                }

        mensagem = (
            "✅ *LHN Sovereign V90 — Teste de Transmissão*\n\n"
            "Este canal está corretamente configurado como destino de sinais de negociação.\n\n"
            "_Mensagem de verificação automática._"
        )
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": self._escape_markdown_v2(mensagem),
            "parse_mode": "MarkdownV2",
        }
        timeout = aiohttp.ClientTimeout(total=20)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=timeout) as response:
                    txt = await response.text()
                    if response.status == 200:
                        try:
                            body = json.loads(txt)
                            if body.get("ok") is True:
                                return {"ok": True}
                            desc = body.get("description") or txt
                            return {"ok": False, "error": f"Telegram: {desc}"}
                        except json.JSONDecodeError:
                            return {"ok": True}
                    try:
                        body = json.loads(txt)
                        desc = body.get("description", txt)
                    except json.JSONDecodeError:
                        desc = txt
                    return {
                        "ok": False,
                        "error": f"Telegram HTTP {response.status}: {desc}",
                    }
        except asyncio.TimeoutError:
            return {
                "ok": False,
                "error": "Timeout ao contactar api.telegram.org (rede ou firewall).",
            }
        except Exception as e:
            return {"ok": False, "error": str(e)}
