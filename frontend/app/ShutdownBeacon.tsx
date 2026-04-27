"use client";

import { useEffect } from "react";
import { withApiKeyQuery } from "@/lib/lhnAuth";

/**
 * Ao fechar a aba, envia POST /api/shutdown (via sendBeacon) para o backend
 * persistir snapshot neural + sandbox e encerrar o processo Python.
 * Usa URL relativa para o rewrite do Next.js → FastAPI :9002.
 */
export function ShutdownBeacon() {
  useEffect(() => {
    const path = withApiKeyQuery("/api/shutdown");
    const fire = () => {
      try {
        if (typeof navigator !== "undefined" && navigator.sendBeacon) {
          navigator.sendBeacon(
            path,
            new Blob([], { type: "application/json" })
          );
        }
      } catch {
        /* ignore */
      }
    };
    window.addEventListener("beforeunload", fire);
    window.addEventListener("pagehide", fire);
    return () => {
      window.removeEventListener("beforeunload", fire);
      window.removeEventListener("pagehide", fire);
    };
  }, []);

  return null;
}
