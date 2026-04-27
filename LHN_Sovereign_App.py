"""
Lançador desktop (pywebview) — camada de interface nativa.
O frontend Next.js deve estar em execução na porta definida em FRONTEND_PORT
(alinhada com frontend/package.json: dev / start).
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import webview

# Manter igual a `next dev` / `next start` em frontend/package.json (-p)
FRONTEND_PORT = 9090
BACKEND_PORT = 9002
SHUTDOWN_URL = f"http://127.0.0.1:{BACKEND_PORT}/api/shutdown"

WINDOW_W = 1600
WINDOW_H = 900

# Zoom global no <html> (Chromium/WebView2). O frontend usa width/height 100% (evita 100vw)
# para nao cortar o painel direito. Faixas finas podem aparecer: mesma cor do fundo.
# Ajuste via env: set LHN_DESKTOP_ZOOM=0.82
ZOOM_DEFAULT = float(os.environ.get("LHN_DESKTOP_ZOOM", "0.88"))
ZOOM_STEP = 0.05
ZOOM_MIN = 0.55
ZOOM_MAX = 1.25

if getattr(sys, "frozen", False):
    _ROOT = Path(sys.executable).resolve().parent
else:
    _ROOT = Path(__file__).resolve().parent

STORAGE_DIR = _ROOT / "desktop_data"
APP_URL = f"http://127.0.0.1:{FRONTEND_PORT}"
WINDOW_TITLE = "LHN SOVEREIGN V90 - Terminal Institucional"


def _load_dotenv_project_root() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv(_ROOT / ".env", override=False)
    except ImportError:
        pass


def _post_backend_graceful_shutdown(timeout_sec: float = 120.0) -> None:
    """
    Bloqueia até o FastAPI gravar checkpoint e sair (ou até timeout).
    Não usa taskkill no backend — evita corrupção SQLite / córtex.
    """
    _load_dotenv_project_root()
    key = (os.environ.get("LHN_API_KEY") or "").strip()
    body = json.dumps({}).encode("utf-8")
    req = urllib.request.Request(
        SHUTDOWN_URL,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    if key:
        req.add_header("X-API-Key", key)
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            _ = resp.read()
    except urllib.error.HTTPError as e:
        if e.code not in (200, 401):
            try:
                e.read()
            except Exception:
                pass
    except (urllib.error.URLError, TimeoutError, OSError):
        # Conexão recusada: backend já terminou ou não estava a correr.
        pass


def _terminate_listeners_on_port(port: int) -> None:
    """Encerra apenas processos em LISTEN na porta (ex.: node/next) — stateless."""
    try:
        import psutil
    except ImportError:
        return
    current = os.getpid()
    candidates: set[int] = set()
    try:
        for conn in psutil.net_connections(kind="tcp"):
            if conn.status != psutil.CONN_LISTEN:
                continue
            la = getattr(conn, "laddr", None)
            if la is None or la.port != port:
                continue
            pid = conn.pid
            if pid and pid != current:
                candidates.add(int(pid))
    except (psutil.AccessDenied, AttributeError):
        return
    for pid in candidates:
        try:
            p = psutil.Process(pid)
            p.terminate()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    time.sleep(0.4)


def _desktop_shell_js() -> str:
    """Zoom no documentElement, atalhos Ctrl+/-/0 e F11."""
    return f"""(function() {{
  if (window.__lhnDesktopShell) return;
  window.__lhnDesktopShell = true;
  var DEFAULT_ZOOM = {ZOOM_DEFAULT};
  var STEP = {ZOOM_STEP};
  var MIN = {ZOOM_MIN};
  var MAX = {ZOOM_MAX};
  var z = DEFAULT_ZOOM;
  function applyZoom() {{
    document.documentElement.style.zoom = String(z);
  }}
  applyZoom();
  document.addEventListener('keydown', function(e) {{
    if (e.key === 'F11') {{
      e.preventDefault();
      e.stopImmediatePropagation();
      if (window.pywebview && window.pywebview.api && window.pywebview.api.lhn_toggle_fullscreen) {{
        var _p = window.pywebview.api.lhn_toggle_fullscreen();
        if (_p && typeof _p.then === 'function') {{
          _p.catch(function () {{}});
        }}
      }}
      return;
    }}
    if (!e.ctrlKey || e.altKey || e.metaKey) return;
    var k = e.key;
    var code = e.code;
    if (k === '+' || k === '=' || code === 'NumpadAdd') {{
      e.preventDefault();
      z = Math.min(MAX, Math.round((z + STEP) * 100) / 100);
      applyZoom();
    }} else if (k === '-' || k === '_' || code === 'NumpadSubtract') {{
      e.preventDefault();
      z = Math.max(MIN, Math.round((z - STEP) * 100) / 100);
      applyZoom();
    }} else if (k === '0' || code === 'Digit0' || code === 'Numpad0') {{
      e.preventDefault();
      z = DEFAULT_ZOOM;
      applyZoom();
    }}
  }}, true);
}})();"""


def main() -> None:
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)

    win = webview.create_window(
        WINDOW_TITLE,
        APP_URL,
        width=WINDOW_W,
        height=WINDOW_H,
        background_color="#0b0f19",
    )

    def lhn_toggle_fullscreen() -> None:
        win.toggle_fullscreen()

    win.expose(lhn_toggle_fullscreen)

    def on_loaded() -> None:
        win.run_js(_desktop_shell_js())

    def on_closing() -> bool:
        try:
            _post_backend_graceful_shutdown()
        finally:
            _terminate_listeners_on_port(FRONTEND_PORT)
        return True

    win.events.loaded += on_loaded
    win.events.closing += on_closing

    webview.start(
        private_mode=False,
        storage_path=str(STORAGE_DIR),
    )


if __name__ == "__main__":
    main()
