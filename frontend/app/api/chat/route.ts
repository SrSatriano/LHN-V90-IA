import { NextResponse } from "next/server";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 600;

const NEXUS_URL = (process.env.LHN_NEXUS_URL || "http://127.0.0.1:9001").replace(
  /\/+$/,
  ""
);
const CHAT_TIMEOUT_MS = Number(process.env.LHN_NEXUS_TIMEOUT_MS || 600_000);

export async function POST(req: Request) {
  let payload: unknown;
  try {
    payload = await req.json();
  } catch {
    return NextResponse.json(
      { detail: "Payload JSON invalido para /api/chat." },
      { status: 400 }
    );
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), CHAT_TIMEOUT_MS);

  try {
    const upstream = await fetch(`${NEXUS_URL}/api/chat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: controller.signal,
      cache: "no-store",
    });

    const text = await upstream.text();
    let data: unknown = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch {
      data = { detail: text || "Resposta invalida do sidecar Nexus." };
    }

    return NextResponse.json(data, { status: upstream.status });
  } catch (err) {
    const isAbort =
      err instanceof DOMException
        ? err.name === "AbortError"
        : String((err as Error)?.name || "").includes("Abort");
    return NextResponse.json(
      {
        detail: isAbort
          ? `Timeout aguardando resposta do Nexus (${Math.round(
              CHAT_TIMEOUT_MS / 1000
            )}s).`
          : `Falha de conexao com Nexus em ${NEXUS_URL}/api/chat.`,
      },
      { status: isAbort ? 504 : 503 }
    );
  } finally {
    clearTimeout(timer);
  }
}
