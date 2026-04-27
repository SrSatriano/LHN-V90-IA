#include <windows.h>
#include <iostream>

extern "C" {

    // Função 1: Aplicar SetWindowDisplayAffinity pelo HWND (Escudo Anti-OBS/Print)
    __declspec(dllexport) bool __stdcall ApplyDarkShield(HWND hwnd) {
        if (hwnd == NULL) return false;
        
        // Tenta primeiro WDA_EXCLUDEFROMCAPTURE (0x11, Win10+), fallback para WDA_MONITOR (0x01)
        BOOL result = SetWindowDisplayAffinity(hwnd, 0x11);
        if (!result) {
            result = SetWindowDisplayAffinity(hwnd, 0x01);
        }
        return result != 0;
    }

    // Função 2: Remover o Escudo
    __declspec(dllexport) bool __stdcall RemoveDarkShield(HWND hwnd) {
        if (hwnd == NULL) return false;
        
        // WDA_NONE (0x00) restaura
        BOOL result = SetWindowDisplayAffinity(hwnd, 0x00);
        return result != 0;
    }

    // Função 3: Checar se existe um Debugger Ativo (Anti-Hacker Mínimo C++)
    __declspec(dllexport) bool __stdcall IsSystemCompromised() {
        if (IsDebuggerPresent()) {
            return true;
        }

        BOOL isDebuggerPresent = FALSE;
        CheckRemoteDebuggerPresent(GetCurrentProcess(), &isDebuggerPresent);
        if (isDebuggerPresent) {
            return true;
        }

        return false;
    }

}
