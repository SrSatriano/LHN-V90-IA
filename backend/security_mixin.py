import ctypes
import logging
import os
import shutil
import sys
import threading
import time

# [FIX 12] Flag de ambiente de desenvolvimento.
_DEV_MODE = os.environ.get("LHN_DEV_MODE", "0") == "1"
logger = logging.getLogger(__name__)


class SecurityMixin:
    def __init__(self):
        super().__init__()
        self.security_thread_running = True

        # [Hotfix] Em DEV não carrega DLL nem dispara rotinas que podem encerrar o processo (Cursor/IDE).
        if _DEV_MODE:
            self.shield_module = None
            try:
                if hasattr(self, "log_msg"):
                    self.log_msg(
                        "⚠️ [DEV MODE] Escudo C++ não carregado — LHN_DEV_MODE=1 (sentinela já desativada)."
                    )
            except Exception:
                pass
            return

        # Load Native C++ Kernel Shield
        dll_path = os.path.join(
            os.path.dirname(__file__), "..", "security", "lhn_shield.dll"
        )
        try:
            if os.path.exists(dll_path):
                try:
                    self.shield_module = ctypes.CDLL(dll_path)
                except OSError:
                    temp_dll_path = os.path.join(
                        os.path.dirname(dll_path), "lhn_shield_temp.dll"
                    )
                    try:
                        shutil.copy2(dll_path, temp_dll_path)
                        self.shield_module = ctypes.CDLL(temp_dll_path)
                        if hasattr(self, "log_msg"):
                            self.log_msg(
                                "⚙️ Escudo C++ carregado via cópia temporária (lhn_shield_temp.dll)."
                            )
                    except Exception:
                        raise
                # Define input and output types for the exposed functions
                self.shield_module.ApplyDarkShield.argtypes = [ctypes.c_void_p]
                self.shield_module.ApplyDarkShield.restype = ctypes.c_bool

                self.shield_module.RemoveDarkShield.argtypes = [ctypes.c_void_p]
                self.shield_module.RemoveDarkShield.restype = ctypes.c_bool

                self.shield_module.IsSystemCompromised.argtypes = []
                self.shield_module.IsSystemCompromised.restype = ctypes.c_bool
            else:
                self.shield_module = None
                if hasattr(self, "erro_msg"):
                    self.erro_msg(
                        "⚠️ LHN_SHIELD.DLL não encontrado. Segurança desabilitada."
                    )
        except Exception as e:
            self.shield_module = None
            if hasattr(self, "erro_msg"):
                self.erro_msg(
                    f"Falha Crítica: LHN_SHIELD.DLL C++ Kernel não encontrado: {e}"
                )
            else:
                print(f"Falha na segurança (ignorada): {e}")

    def blindar_janela_contra_espionagem(self):
        """
        Ativa o Anti-Screen Capture (WDA_MONITOR) nativo via C++.
        Como o Headless FastAPI não tem janela nativa (só terminal),
        procuramos a janela de console preta do servidor para aplicar WDA_MONITOR.
        Falhas de HWND/DLL não devem interromper o motor (headless / sem console).
        """
        if not self.shield_module:
            return

        if os.name != "nt":
            logger.debug(
                "Escudo C++: ignorado em sistema não-Windows (VPS Linux/POSIX)."
            )
            return

        try:
            hwnd = ctypes.windll.kernel32.GetConsoleWindow()
            if not hwnd:
                hwnd = ctypes.windll.user32.GetForegroundWindow()
            if not hwnd:
                logger.debug(
                    "Escudo C++: HWND não disponível (normal em serviço/sem console)."
                )
                return
            try:
                result = self.shield_module.ApplyDarkShield(ctypes.c_void_p(hwnd))
            except Exception as dll_err:
                logger.debug("ApplyDarkShield ignorado: %s", dll_err)
                return
            if result:
                if hasattr(self, "log_msg"):
                    self.log_msg(
                        "🛡️ Escudo Escuro Nativo C++ (WDA_MONITOR) Ativado no Backend."
                    )
            else:
                logger.debug(
                    "ApplyDarkShield retornou false (DLL/HWND); motor continua sem escudo visual."
                )
        except Exception as e:
            logger.debug("blindar_janela (não fatal): %s", e)

    def iniciar_sentinela_anti_hacker(self):
        """Dispara a thread oculta que vigia a memória continuamente."""
        # [FIX 12] Em modo desenvolvimento, o sentinela é desabilitado.
        if _DEV_MODE:
            if hasattr(self, "log_msg"):
                self.log_msg(
                    "⚠️ [DEV MODE] Sentinela Anti-Hacker nativo C++ desabilitado (LHN_DEV_MODE=1)."
                )
            return

        t = threading.Thread(target=self._loop_anti_debugger, daemon=True)
        t.start()
        if hasattr(self, "log_msg"):
            self.log_msg(
                "🚔 Módulo C++ Kernel de Patrulha em Memória V90 FINAL operante nas sombras."
            )

    def _loop_anti_debugger(self):
        """
        Delega para a DLL C++ verificar hooks ou injects no processo.
        """
        while self.security_thread_running:
            if self.shield_module:
                is_compromised = self.shield_module.IsSystemCompromised()
                if is_compromised:
                    print(
                        "\n[CRÍTICO EXTREMO - C++ KERNEL] INTRUSÃO DETECTADA NA MEMÓRIA."
                    )

                    # [FIX 12] Salvar modelo neural antes do kill para evitar corrupção
                    try:
                        _done = threading.Event()

                        def _salvar_cerebro_seguro():
                            try:
                                if (
                                    hasattr(self, "model")
                                    and self.model
                                    and hasattr(self, "arquivo_cerebro")
                                ):
                                    self.model.save(self.arquivo_cerebro)
                            except Exception:
                                logger.exception(
                                    "security_model_save_failed | ts=%s | ativo=%s | payload=%s",
                                    int(time.time() * 1000),
                                    "GLOBAL",
                                    {
                                        "arquivo_cerebro": getattr(
                                            self, "arquivo_cerebro", None
                                        )
                                    },
                                )
                            finally:
                                _done.set()

                        threading.Thread(
                            target=_salvar_cerebro_seguro, daemon=True
                        ).start()
                        _done.wait(timeout=2.0)
                    except Exception:
                        logger.exception(
                            "security_save_wait_failed | ts=%s | ativo=%s | payload=%s",
                            int(time.time() * 1000),
                            "GLOBAL",
                            {},
                        )

                    if hasattr(self, "api_key"):
                        self.api_key = "YOUR_API_KEY_HERE"
                    if hasattr(self, "api_secret"):
                        self.api_secret = "YOUR_API_SECRET_HERE"

                    print(
                        "[CRÍTICO EXTREMO - C++ KERNEL] TENTANDO SALVAR CONFIGURAÇÕES ANTES DO ABORTO..."
                    )
                    if hasattr(self, "salvar_configuracoes_gerais"):
                        try:
                            self.salvar_configuracoes_gerais()
                        except Exception:
                            logger.exception(
                                "security_save_config_gerais_failed | ts=%s | ativo=%s | payload=%s",
                                int(time.time() * 1000),
                                "GLOBAL",
                                {},
                            )
                    if hasattr(self, "salvar_configuracoes_conta"):
                        try:
                            self.salvar_configuracoes_conta()
                        except Exception:
                            logger.exception(
                                "security_save_config_conta_failed | ts=%s | ativo=%s | payload=%s",
                                int(time.time() * 1000),
                                "GLOBAL",
                                {},
                            )

                    print("[CRÍTICO EXTREMO - C++ KERNEL] DESTRUINDO AMBIENTE API...")
                    from tensorflow.keras import backend as K

                    K.clear_session()

                    # [FIX 12] sys.exit em vez de os._exit: permite atexit handlers
                    sys.exit(1)
            time.sleep(2)
