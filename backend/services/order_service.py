"""Encapsula envio e ciclo de vida de ordens (delega ao bot)."""


class OrderService:
    __slots__ = ("_bot",)

    def __init__(self, bot):
        self._bot = bot

    def executar_ordem_real(self, ativo, lado, margem, alavancagem, preco_atual):
        return self._bot.executar_ordem_real(
            ativo, lado, margem, alavancagem, preco_atual
        )

    def fechar_ordem_real(self, ativo, lado_original, qty):
        return self._bot.fechar_ordem_real(ativo, lado_original, qty)
