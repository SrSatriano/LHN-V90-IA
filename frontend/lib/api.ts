import axios from "axios";
import { getLhnApiKey } from "./lhnAuth";

/**
 * Cliente HTTP para o backend LHN (Next.js reescreve /api → FastAPI :9002 em dev).
 */
export const api = axios.create({
  baseURL: "",
  headers: { "Content-Type": "application/json" },
  timeout: 120_000,
});

api.interceptors.request.use((config) => {
  const k = getLhnApiKey();
  if (k) {
    config.headers = config.headers || {};
    (config.headers as Record<string, string>)["X-API-Key"] = k;
  }
  return config;
});
