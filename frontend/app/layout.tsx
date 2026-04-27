import type { Metadata } from "next";
import { Inter, Fira_Code } from "next/font/google";
import "./globals.css";
import { ShutdownBeacon } from "./ShutdownBeacon";

const inter = Inter({ subsets: ["latin"], variable: "--font-inter" });
const firaCode = Fira_Code({ subsets: ["latin"], variable: "--font-fira-code" });

export const metadata: Metadata = {
  title: "LHN Sovereign V90 NEXUS",
  description: "Elite HFT Hybrid Platform",
  // Nexus HFT V90: ícone da aba / barra de tarefas (substitui globo genérico do browser).
  icons: {
    icon: [
      {
        url: "/branding/LHN_tech_holding_202604251645.jpeg",
        type: "image/jpeg",
      },
    ],
    apple: "/branding/LHN_tech_holding_202604251645.jpeg",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="pt-BR" className="h-full" suppressHydrationWarning>
      <body
        className={`${inter.variable} ${firaCode.variable} flex min-h-0 flex-col overflow-hidden bg-[#0b0f19] font-sans text-gray-200 antialiased h-full`}
        suppressHydrationWarning
      >
        <ShutdownBeacon />
        {/* App Router: shell ocupa o viewport; filhos usam flex-1 / min-h-0 na cadeia. */}
        <div
          id="app-shell"
          className="flex min-h-0 min-w-0 w-full flex-1 flex-col overflow-hidden"
        >
          {children}
        </div>
      </body>
    </html>
  );
}
