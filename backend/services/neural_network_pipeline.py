"""Pipeline neural — delegação para AIMixin (evolução futura)."""


class NeuralNetworkPipeline:
    __slots__ = ("_bot",)

    def __init__(self, bot):
        self._bot = bot

    def ligar_cerebro_ia(self):
        if hasattr(self._bot, "ligar_cerebro_ia"):
            return self._bot.ligar_cerebro_ia()

    def treinar_ia(self):
        if hasattr(self._bot, "treinar_ia"):
            return self._bot.treinar_ia()
