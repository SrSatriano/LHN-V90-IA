# LHN Sovereign V90

**Estação de trading quantitativo em cripto** — backend **Python/FastAPI**, painel **Next.js**, integração **Bybit V5** (REST + WebSocket), módulos de **IA (Keras/TensorFlow)**, camadas de **risco**, telemetria e integrações auxiliares. Publiquei em **source-available** (PolyForm Noncommercial): dá para estudar e experimentar; **não** é licença para terceiros lucrar com o código sem acordo comigo.

Sou o **SrSatriano** ([GitHub](https://github.com/SrSatriano)). Abaixo descrevo **como eu arquitetei o sistema**, **o que cada parte faz** e **o que você pode esperar** ao rodar — com a honestidade de quem escreveu o motor.

---

## Índice (PT)

- [Resumo](#resumo)
- [Arquitetura geral](#arquitetura-geral)
- [Backend: composição e ciclo de vida](#backend-composição-e-ciclo-de-vida)
- [Motor assíncrono e tarefas paralelas](#motor-assíncrono-e-tarefas-paralelas)
- [Mercado: Bybit e túneis de dados](#mercado-bybit-e-túneis-de-dados)
- [Threads e loops “em paralelo” ao orquestrador](#threads-e-loops-em-paralelo-ao-orquestrador)
- [Inteligência artificial](#inteligência-artificial)
- [Risco, ordens e governança](#risco-ordens-e-governança)
- [API HTTP, WebSocket e autenticação](#api-http-websocket-e-autenticação)
- [Frontend (painel)](#frontend-painel)
- [Nexus, Telegram e módulo C++](#nexus-telegram-e-módulo-c)
- [Workspace, persistência e segredos](#workspace-persistência-e-segredos)
- [O que funciona / o que é dependente de configuração](#o-que-funciona--o-que-é-dependente-de-configuração)
- [Pré-requisitos e instalação](#pré-requisitos-e-instalação)
- [Variáveis de ambiente](#variáveis-de-ambiente)
- [Testes](#testes)
- [Licença e aviso](#licença-e-aviso)
- [English (detailed summary)](#english-detailed-summary)

---

## Resumo

O **LHN Sovereign V90** é um **terminal local** para **pesquisa, simulação e execução controlada** em **perpétuos** cripto (foco **Bybit linear USDT**). O fluxo típico:

1. O **backend** sobe, carrega configuração, prepara **workspace**, valida modo (simulação / testnet / real conforme sua configuração).
2. Com o “Start” vindo do painel (comando via API), entram em cena **loops de mercado** (WebSocket + fallbacks), **análise**, **IA**, **gestão de risco** e, se você habilitar, **ordens** na corretora.
3. O **frontend** consome **REST** e um **WebSocket** em `/stream` para estado em tempo quase real (posições, logs, métricas agregadas).
4. Peças opcionais: **Nexus** (LLM em sidecar), **Telegram**, **oracle Binance** e **tracker on-chain** em modo leitura, **escudo C++** se compilar.

O repositório público **não** traz credenciais, bancos de produção, *weights* de modelo, logs ou *build* de frontend — só código e modelos de configuração. O resto é gerado no seu disco (veja `.gitignore`).

---

## Arquitetura geral

```text
┌─────────────────────────────────────────────────────────────────┐
│                     Next.js (9090)                               │
│  Abas: trading, settings, news, orderflow, heatmap, sinais, …   │
│  WebSocketContext + chamadas REST (X-API-Key se LHN_API_KEY)    │
└──────────────────────────┬────────────────────────────────────┘
                           │ HTTP / WS
┌──────────────────────────▼────────────────────────────────────┐
│              FastAPI — server.py (9002)                        │
│  LHNSovereignV90Backend = Core + Engine + AI + Security + TG  │
│  RiskManager, OrderService, NeuralNetworkPipeline (fachadas)   │
└──────┬──────────────────┬──────────────────┬──────────────────┘
       │                  │                  │
       ▼                  ▼                  ▼
  Bybit V5          Keras/TF            SQLite / Parquet
  REST + WS         treino + inferência  (workspace, replay, etc.)
```

A classe principal do backend é **`LHNSovereignV90Backend`**, herdando em cadeia:

| Mixin / módulo | Papel (resumo) |
|----------------|----------------|
| `CoreMixin` | Estado global, saldo, config, workspace, logging, flags de modo. |
| `EngineMixin` | WebSockets Bybit, loops de mercado, *guardian*, arbitragem, ordens, *submit* de tarefas. |
| `AIMixin` | Modelos, *forja*, *replay*, *features*, treinos e integração com *pipeline*. |
| `SecurityMixin` | Camada de “blindagem” / integridade (quando ativa + DLL opcional). |
| `TelegramMixin` | Alertas e sinais (MarkdownV2, fallbacks numéricos). |

`RiskManager`, `OrderService` e `NeuralNetworkPipeline` são **fachadas** finas que delegam para o *bot* — úteis para evoluir sem espalhar lógica no `server.py`.

---

## Backend: composição e ciclo de vida

- **`lifespan` (FastAPI):** instancia o `lhn_bot`, regista *hooks* (ex.: após fechamento de ordem para broadcast WebSocket) e faz *cleanup* no encerramento.
- **Inicialização do bot:** `config` padrão + `configurar_workspace_autonomo`, `RiskManager` / `OrderService` / `NeuralNetworkPipeline`, tentativa de carregar **módulo C++** se existir, `inicializar_engine` (tickers a partir do *master*), `inicializar_ia` (flags `ia_treinada`, `treinamento_concluido`, etc.).
- **`iniciar_servicos_background`:** abre um **event loop** em **thread** dedicada (`_rodar_loop_async`) e agenda `iniciar_motor_assincrono`, notícias/NLP, *top* de ativos, *leverage brackets*, *comitê gestor*, *watchdog* neural, **loop de arbitragem**, `ligar_cerebro_ia`, `iniciar_bot`. O motor fica em **standby** até o painel mandar ignição; a “busca” não liga sozinha ao subir o processo (desenho intencional V90).

---

## Motor assíncrono e tarefas paralelas

O **`iniciar_motor_assincrono`** cria um **event loop** próprio; em **Linux** eu tento instalar **`uvloop`** para menos latência. O núcleo é o **`orchestrator_assincrono`**, que depois de garantir o canal de **ordens** (WebSocket privado / estado) dispara, em paralelo, tarefas supervisionadas (reinício automático se uma cair):

| Tarefa (nome interno) | Função |
|------------------------|--------|
| `websocket` | `loop_websocket_nativo` — túneis de dados públicos Bybit (ver abaixo). |
| `relatorio_diario` | Rotina de relatório diário. |
| `guardiao_shadow` | *Guardian* em sombra: monitora posições com lógica de toxicidade/VPIN, *grace period* após abertura, *hard stop* com prioridade, *scale-out* / pânico conforme regras. |
| `tribunal_quantitativo` | Julgamento / estatísticas de desempenho (arena, recuperação, etc.). |
| `rest_price_fallback` | Fallback de preço via REST se o feed WebSocket atrasar. |
| `retry_close_orders` | *Retry* de fechamentos pendentes. |
| `binance_oracle_tunnel5` | **Só leitura:** *book ticker* futuros Binance (BTC) em paralelo — não envia ordem. |
| `onchain_tracker_tunnel6` | Polling *on-chain* (camada de serviço, não bloqueia o WS Bybit). |
| `evolucao_neural_1h` | Ciclo de ~1h ligado a aprendizado contínuo / *experience replay*. |
| `experience_replay` | *Loop* de *replay* de experiências. |

Cada tarefa roda dentro de `_orchestrator_supervised` para o processo não morrer silenciosamente por um *exception* isolado.

---

## Mercado: Bybit e túneis de dados

- **Produção pública Bybit V5 (linear):** o `loop_websocket_nativo` monta ciclos com múltiplos **consumidores assíncronos**: de forma resumida nos logs como **Túnel 1** (preços / *tickers*), **2** (*order book*), **3** (fluxo/VPIN a partir de trades públicos), **4** (*kline*, ex. 15m) — tudo alinhado ao universo de símbolos filtrado para *linear* válido.
- **Processo isolado:** o fluxo de *tickers* passa por um **processo** `multiprocessing` que lê o WebSocket e joga dados em *queues*; o *asyncio* do motor só drena filas. Isso reduz contenção de **GIL** no caminho quente.
- **Reconexão:** em falhas transitórias de WebSocket (incl. ruído de rede no Windows), há *backoff* exponencial e classificação de erros — não fica em *tight loop* derrubando a pilha.
- **REST / rate limit** (`bybit_helpers`): *retry*, pausa alinhada a cabeçalhos de *rate limit* da Bybit quando disponíveis, e espaçamento em buscas de *klines* em lote para não estourar cota.
- **Ordens (privado):** o orquestrador chama `_ensure_ws_orders_connection` para manter o canal de ordens coerente com a sessão; fechamento gracioso dá `_close_ws_orders_connection`.

---

## Threads e loops “em paralelo” ao orquestrador

Além do *loop* assíncrono central, o backend agenda **tarefas em *thread pool*** do *bot* (método `submit_background_task`), entre elas (conforme exista no `engine`):

- **`loop_radar_rest`:** radar / amostragem via REST.
- **`loop_comite_gestor`:** “comitê” macro (filtros de regime, decisões de alto nível).
- **`loop_ia_arbitragem`:** *pairs trading* / convergência — com **saída sensível a taxa** (PnL combinado tem de cobrir *roundtrip* de taxas *taker*).
- **Notícias / NLP global:** `loop_noticias_global` (RSS, pontuação de *sentiment*, lexicon de pânico, etc. — detalhado no `config` e no motor).
- **Top 100 ativos, *leverage brackets*:** alimenta listas e limites.
- **Watchdog** do pulso neural — evita que o *cérebro* pare sem log.

A **ignição** da “busca neural” e o estado `is_searching` são controlados pelo **painel** vía **`/api/command`**, não por um *flag* solto no boot.

---

## Inteligência artificial

- **Modelos Keras/TensorFlow:** *snippets* de *forja* inicial (`forjar_ia_sniper_nexus` ou fallback `treinar_ia` em *thread*), *guardião* histórico (`forjar_guardiao_historico`), *replay* de *buffer* e ajustes incrementais. Uso de **`ThreadPoolExecutor`** para *features* e treino, para não bloquear o *loop* principal de forma grosseira.
- **Alinhamento de dimensões:** o *Data Lake* / *replay* pode fornecer vetores com dimensão diferente da que o modelo espera; há coerção (*padding* / *truncate*) para o tamanho esperado antes de `continuous_learn` / *fit*.
- **Governança de *drawdown* (sessão):** métricas de *drawdown* *máximo* e decisões críticas ligadas a saldo só entram no jogo depois de **treino concluído** (`treinamento_concluido`), **saldo real** confirmado quando em modo real, e feed de preço WebSocket ativo — reduz *drawdown fantasma* pós boot.
- **Mínimo de amostras:** ajustes de *auto-cura* / *arena* que olham *win rate* exigem um mínimo de operações fechadas na janela (constante `MIN_TRADES_FOR_EVALUATION = 5` no *engine*), para não reagir a 1–2 trades.
- **Pipeline:** `NeuralNetworkPipeline` expõe `ligar_cerebro_ia` / `treinar_ia` delegando ao *mixin* — ponto único se um dia trocar o *backend* neural.

---

## Risco, ordens e governança

- **`RiskManager`:** por exemplo, bloqueia abrir ordem se o *feed* WebSocket estiver “fresco” além de `ws_feed_stale_sec` (kill switch de dados velhos) e aplica teto de notional com `max_order_usd` na config.
- **`order_execution_gate`:** validação de *qty* (passo mínimo da Bybit) e regras de abertura *linear*.
- **Guardião (VPIN / toxicidade):** acções agressivas (*panic sell*, *scale-out*) respeitam **período de graça** após abertura da posição, **exceto** se o *hard stop* tiver sido atingido — alivia *whipsaw* de micro-oscilações.
- **Arbitragem por taxas:** fecho só quando o PnL estimado **supera** o custo *roundtrip* de taxas do par.
- **Encerramento seguro / snapshot:** `POST /api/safe-shutdown` e `POST /api/shutdown` permitem *flatten*, fechamento de WS e rotinas de persistência; `POST /api/snapshot` aciona *checkpoint* neural (conforme implementação).

---

## API HTTP, WebSocket e autenticação

**Base:** `http://127.0.0.1:9002` (ajuste com `LHN_BACKEND_PORT` se mudar).

Principais **rotas** (a maioria exige autenticação se `LHN_API_KEY` estiver definida):

| Método | Caminho | Uso |
|--------|---------|-----|
| `GET` | `/api/health` | Saúde do serviço. |
| `POST` | `/api/command` | Comandos do painel (iniciar/pausar busca, *panic*, etc.). |
| `GET`/`POST` | `/api/config` | Lê e grava configuração *master*. |
| `GET` | `/api/history/{symbol}` | Histórico (proxy/ajuste de candles). |
| `GET` | `/api/historico-operacoes` (alias com underscore) | Histórico de operações. |
| `POST` | `/api/saldo` | Sincroniza *saldo* / estado financeiro. |
| `GET` | `/api/market/tickers` | *Proxy* de *tickers* Bybit. |
| `GET` | `/api/signals` | Sinais. |
| `GET`/`POST` | `/api/config/transmission` + `/test` | Configuração e teste de **Telegram**. |
| `POST` | `/api/chat` | Encaminha conversa ao **Nexus** (sidecar embutido ou 9001). |
| `POST` | `/api/safe-shutdown` | *Shutdown* com opções (ex. *flatten*). |
| `POST` | `/api/shutdown` | *Graceful shutdown*. |
| `POST` | `/api/snapshot` | *Snapshot* neural. |
| `POST` | `/api/close` | Fechamento de posição supervisionado. |
| `WS` | `/stream` | *Stream* de status completo (logs, posições, agregados). **Handshake:** se `LHN_API_KEY` estiver setada, o cliente envia a chave via subprotocolo WebSocket (ver `lhn_auth.websocket_subprotocol_token_ok`). |

**Autenticação `LHN_API_KEY` (opcional):** se a variável **não** estiver vazia, as rotas *HTTP* exigem `X-API-Key`, *Bearer* ou `?token=`; o *frontend* pode enviar a mesma chave. Se estiver vazia, fica em **modo teste local** sem chave.

---

## Frontend (painel)

- **Stack:** **Next.js** (App Router), **React**, **TypeScript** na maior parte, um componente **News** em **JSX**.
- **Página principal (`app/page.tsx`):** **barra lateral** `UnifiedSidebar` com abas; área central troca o conteúdo: **CryptoWorkspace** (trading, IA, terminal, *horizonte*, posições, histórico…), **MasterConfigForm** (definições), **NewsFlow**, **OrderFlow**, **LiquidityMap** (*heatmap*), **SignalsHistory**, **TransmissionConfig** (Telegram).
- **`WebSocketContext`:** mantém a sessão `ws://…/stream` e estado derivado para os componentes.
- **Variáveis `NEXT_*`:** devem apontar para o host/porta reais do backend (ver `.env.example`).

---

## Nexus, Telegram e módulo C++

- **Nexus (`nexus_chat.py`, porta padrão 9001):** motor conversacional (Qwen2.5 Instruct em modo *lazy*), pode rodar como **sidecar** ou ser acionado via backend. O `POST /api/chat` fala com o *sidecar* quando configurado. **Exige** RAM/GPU condizentes; senão, use *light* ou deixe desligado.
- **Telegram (`TelegramMixin`):** alertas e sinais; texto escapado para **MarkdownV2**; números com conversão segura (*telemetry* quebrada não derruba o envio).
- **`security/` + `compile.bat`:** C++ opcional; se gerar `lhn_shield.dll` e o *loader* achar, entram rotinas de *hardening*; sem DLL, o backend continua, só sem esse extra.

---

## Workspace, persistência e segredos

- **Pasta `Workspace_LHN`:** raiz lógica para *datasets*, SQLite de *replay*, *checkpoints*, etc. (o *git* não deve versionar esses *blob*s).
- **`.env`:** copiado de `.env.example` — **Bybit**, modo simulação/testnet/real, Telegram, caminhos. **Nunca** *commit* de `.env` real.
- **Persistência de segredos:** `lhn_secrets_persist` / criptografia local opcional (`LHN_AES_KEY`).
- **Log *sanitize*:** utilitário para reduzir vazamento de dados sensíveis em log.

---

## O que funciona / o que é dependente de configuração

| Item | Comportamento |
|------|---------------|
| Cotação e *order book* em tempo quase real | Depende de WS Bybit + tickers configurados; sem rede ou com IP bloqueado, cai *fallback* REST/estado degradado. |
| Ordens reais | Exige chaves **Unified** com permissão correta, modo real ativo, risco validado. **Sempre** teste em *testnet* / simulação. |
| IA com pesos fortes | Sem *weights* no *clone* público, a *forja* gera/ajusta a partir de *pipeline* e dados do **seu** workspace. |
| Nexus / LLM | Depende de modelo baixado, memória, e opcionalmente *GPU*. |
| Binance oracle / *on-chain* | **Auxiliares**; não substituem o preço de negociação Bybit. |
| Autenticação API LHN | Opcional; recomendada se o backend for exposto fora de `127.0.0.1`. |

---

## Pré-requisitos e instalação

1. **Python 3.11+**, **Node.js 20+**, `npm`. No **Windows** uso o `INICIAR_SISTEMA_INTEGRADO.bat`; no Linux/*macOS* os mesmos passos com shell.
2. Clonar o repositório e, na raiz:  
   `python -m venv .venv` → ativar → `pip install -r requirements.txt`
3. `copy .env.example .env` e preencher (Bybit, Telegram, URLs do frontend, modo).
4. (Opcional) `set LHN_API_KEY=...` no ambiente se quiser *lock* no backend.
5. Subir: **`INICIAR_SISTEMA_INTEGRADO.bat`** *ou* seguir **`INICIAR_MANUAL.txt`** (backend `uvicorn` em `9002`, frontend `npm run dev` em `9090`, Nexus opcional `9001`).

**Portas padrão**

| Serviço | Porta |
|--------|-------|
| Nexus (*sidecar*) | 9001 |
| FastAPI | 9002 |
| Next.js | 9090 |

**Painel:** [http://127.0.0.1:9090](http://127.0.0.1:9090)

---

## Variáveis de ambiente

O modelo completo está em **`.env.example`**. Em resumo: host/porta do backend, **`LHN_WORKSPACE_DIR`**, `LHN_MODE`, `LHN_USE_BYBIT_TESTNET`, chaves `BYBIT_*`, Telegram, `LHN_AES_KEY` opcional, e do lado do *build* frontend as `NEXT_PUBLIC_API_BASE_URL` e `NEXT_PUBLIC_WS_URL` coerentes com o backend.

**Opcional (não no exemplo, mas suportado):** `LHN_API_KEY` para fechar o *API* e o *WebSocket* com a mesma chave.

---

## Testes

Na pasta **`tests/`** existem, entre outros, verificações ligadas ao *guardião* de risco e latência/GIL — uteis em CI local:

```text
pytest tests/
```

(Ative a venv antes e instale *dev dependencies* se o projeto as listar; caso contrário `pytest` usa o que estiver no ambiente.)

---

## Licença e aviso

- **Licença:** **PolyForm Noncommercial 1.0.0** — leia o arquivo **`LICENSE`**, **`COMMERCIAL_USE.md`** e **`NOTICE`**. Uso comercial exige **contrato à parte** comigo.
- **Menu de licença do GitHub:** a PolyForm muitas vezes **não** aparece no *dropdown* de criação de repositório; o que vale é o texto em **`LICENSE`** committado.
- **Risco:** *derivatives* cripto não são brincadeira. Eu forneço **software de engenharia e pesquisa**; quem roda é responsável por perdas, configuração da corretora e conformidade. Comece em **simulação** / *testnet*.

---

# English (detailed summary)

**LHN Sovereign V90** is a **local quantitative trading workstation** for **crypto perpetuals**, built around a **Python/FastAPI** backend, **Next.js** operator UI, **Bybit V5** (REST + WebSocket), **Keras/TensorFlow** AI paths, and layered **risk** and **telemetry**. I am **SrSatriano** on GitHub. This repo is **source-available** under the **PolyForm Noncommercial 1.0.0** license (see `LICENSE`); it is **not** a “free-for-all commercial” grant.

**Architecture**

- The backend class **`LHNSovereignV90Backend`** composes **`CoreMixin`**, **`EngineMixin`**, **`AIMixin`**, **`SecurityMixin`**, and **`TelegramMixin`**, plus thin facades: **`RiskManager`**, **`OrderService`**, **`NeuralNetworkPipeline`**.
- On startup, **`iniciar_servicos_background`** schedules: async **engine** (orchestrated WebSocket *tunnels*, *guardian*, *tribunal*, REST *fallbacks*, *binance read-only oracle*, *onchain tracker*, *experience replay*, neural hourly cycle), **macro committee**, **statistical arbitrage loop**, **AI brain**, and **`iniciar_bot`**. The system stays in **standby** until the UI sends a **start** command over **`/api/command`**.

**Async engine (`orchestrator_assincrono`)** runs supervised tasks in parallel, including: native **Bybit public WS** (tickers, order book, **VPIN**/trade flow, klines) with a **multiprocess** *queue* path to reduce GIL pressure, **guardian** (*grace period* vs *hard stop*), **tribunal**, **REST price fallback**, **order close retries**, and auxiliary loops.

**AI** uses **Keras/TensorFlow**, *forge* and *replay* paths, *thread pools* for features/training, **feature-vector coercion** to model dimensions, and governance around **drawdown** (after real balance + training + live *feed* where applicable) and a **minimum trade count** before *auto-*recovery-style decisions.

**API:** FastAPI routes under `/api/*` plus **WebSocket** `/stream` for the dashboard; optional **`LHN_API_KEY`** for HTTP and WS subprotocol auth. See the Portuguese section for the **endpoint table** and **ports** (9001 Nexus, 9002 API, 9090 UI).

**Nexus** (`nexus_chat.py`, default **9001**) is an optional **LLM sidecar**. **Telegram** uses **MarkdownV2** escaping. **`security/`** has optional C++ for a native DLL. Runtime **SQLite/Parquet/model weights** live under your **workspace** — not in the public *clone*.

**How to run:** `python -m venv .venv` → `pip install -r requirements.txt` → copy **`.env.example`** to **`.env`**, set **Bybit** / **Telegram** / `NEXT_PUBLIC_*` URLs → run **`INICIAR_SISTEMA_INTEGRADO.bat`** or **`INICIAR_MANUAL.txt`**. Start in **simulation** or **testnet** before *live* money.

**Disclaimer:** Crypto *derivatives* are **high risk**. I ship this for **engineering and research**; the **operator** is responsible for *exchange* configuration, *compliance*, and *losses*.

**License / GitHub note:** the PolyForm license may **not** appear in GitHub’s *license* template *dropdown*; the **committed `LICENSE` file** is what governs.

---

<p align="center">
  <sub><strong>LHN Sovereign V90</strong> — SrSatriano</sub>
</p>
