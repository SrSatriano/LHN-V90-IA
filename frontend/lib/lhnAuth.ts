/**
 * Chave compartilhada com o backend (LHN_API_KEY / NEXT_PUBLIC_LHN_API_KEY).
 * Em produção, defina a mesma sequência nas variáveis de ambiente do Python e do Next.js.
 */

export function getLhnApiKey(): string | undefined {
  const k = (process.env.NEXT_PUBLIC_LHN_API_KEY || "").trim();
  return k || undefined;
}

export function lhnAuthHeaders(): Record<string, string> {
  const k = getLhnApiKey();
  if (!k) return {};
  return { "X-API-Key": k };
}

export function mergeAuthHeaders(init?: RequestInit): Headers {
  const h = new Headers(init?.headers);
  const k = getLhnApiKey();
  if (k) h.set("X-API-Key", k);
  return h;
}

export async function fetchWithAuth(
  input: RequestInfo | URL,
  init?: (RequestInit & { timeoutMs?: number }) | undefined
): Promise<Response> {
  const timeoutMs = init?.timeoutMs;
  let finalInit = { ...init, headers: mergeAuthHeaders(init) };

  try {
    if (!timeoutMs || timeoutMs <= 0) {
      return await fetch(input, finalInit);
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      finalInit.signal = controller.signal;
      return await fetch(input, finalInit);
    } finally {
      clearTimeout(timer);
    }
  } catch (error: any) {
    // Intercepta TypeError: Failed to fetch para evitar que a UI quebre
    console.warn(`[fetchWithAuth] Falha de conexão na rota ${input}: ${error?.message || error}`);
    return new Response(
      JSON.stringify({ 
        detail: "Backend indisponível ou falha de rede. Verifique se o Motor de Inteligência está rodando."
      }),
      {
        status: 503,
        statusText: "Service Unavailable",
        headers: { "Content-Type": "application/json" }
      }
    );
  }
}

/** Anexa ?token= quando há chave (ex.: navigator.sendBeacon, que não envia headers). */
export function withApiKeyQuery(path: string): string {
  const k = getLhnApiKey();
  if (!k) return path;
  const sep = path.includes("?") ? "&" : "?";
  return `${path}${sep}token=${encodeURIComponent(k)}`;
}

/** URL do stream WS (sem token na query; auth via Sec-WebSocket-Protocol). */
export function getLhnWsStreamUrl(): string {
  const raw = (process.env.NEXT_PUBLIC_LHN_WS_URL || "").trim();
  return raw || "ws://127.0.0.1:9002/stream";
}

/** Subprotocolos negociados: identificador + chave LHN (não vai na URL). */
export function getLhnWsProtocols(): string[] | undefined {
  const k = getLhnApiKey();
  if (!k) return undefined;
  return ["lhn.auth.v1", k];
}
