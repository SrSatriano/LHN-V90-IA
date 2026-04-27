import asyncio

import aiohttp

# Suas credenciais extraídas dos logs anteriores
TOKEN = YOUR_SECRET_HERE
CHAT_ID = YOUR_SECRET_HERE


async def testar():
    print(f"🚀 Tentando enviar mensagem para o ID: {CHAT_ID}...")
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": "🚨 NEXUS: Se você ler isso, o problema NÃO É o Telegram, é o código do Bot!",
        "parse_mode": "Markdown",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as resp:
                resultado = await resp.json()
                if resultado.get("ok"):
                    print("✅ SUCESSO! Verifique seu celular.")
                else:
                    print(f"❌ ERRO DO TELEGRAM: {resultado}")
    except Exception as e:
        print(f"💥 ERRO DE REDE/CÓDIGO: {e}")


if __name__ == "__main__":
    asyncio.run(testar())
