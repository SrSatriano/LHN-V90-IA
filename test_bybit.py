import os
from dotenv import load_dotenv
from pybit.unified_trading import HTTP

# Força o carregamento do arquivo físico .env
load_dotenv()

API_KEY = YOUR_SECRET_HERE
API_SECRET = YOUR_SECRET_HERE

print(f"--- TESTE DE CONEXÃO BYBIT ---")
print(f"Chave encontrada no .env: {'SIM' if API_KEY else 'NÃO'} (Início: {API_KEY[:5]}...)")

if not API_KEY:
    print("ERRO INTERNO: O V90 não está conseguindo ler o arquivo .env! Verifique se ele existe e se as chaves estão lá.")
else:
    try:
        # ATENÇÃO: Verifique se testnet=False ou True bate com a sua chave
        session = HTTP(testnet=False, api_key=API_KEY, api_secret=API_SECRET)
        bal = session.get_wallet_balance(accountType="UNIFIED", coin="USDT")
        saldo = bal['result']['list'][0]['coin'][0]['walletBalance']
        print(f"✅ SUCESSO ABSOLUTO! Conexão estabelecida. Saldo Real: US$ {saldo}")
    except Exception as e:
        print(f"❌ FALHA DE AUTENTICAÇÃO NA BYBIT: {e}")
        print("MOTIVOS COMUNS: Chave Testnet vs Mainnet, IP Bloqueado ou Falta de Permissão Unified Trading.")
