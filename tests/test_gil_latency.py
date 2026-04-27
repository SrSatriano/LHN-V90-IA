import asyncio
import time
import pytest

# Simula uma operação pesada de Keras/TensorFlow que ocupa a CPU e agarra o GIL
def heavy_cpu_task():
    # Loop sintético denso para estressar a CPU (bloquearia o event loop se não for isolado em Processo)
    start_time = time.time()
    count = 0
    while time.time() - start_time < 0.2:  # 200ms
        count += 1
    return count

@pytest.mark.asyncio
async def test_gil_latency_on_event_loop():
    """
    Testa se uma predição síncrona bloqueia o processamento do WebSocket.
    O event loop não deve atrasar mais que 50ms para focar nos WebSockets.
    """
    latencies = []

    async def mock_websocket_heartbeat():
        for i in range(10):
            t0 = time.time()
            await asyncio.sleep(0.01)  # Intervalo ideal de 10ms
            t1 = time.time()
            latency = (t1 - t0) * 1000  # em ms
            if i > 0:  # Ignora a primeira iteração para o warm-up
                latencies.append(latency)

    async def mock_keras_prediction():
        await asyncio.sleep(0.05)  # Espera o WS começar as leituras
        
        # Solução Aplicada: ProcessPoolExecutor rodando a tarefa em um processo Windows separado
        from concurrent.futures import ProcessPoolExecutor
        loop = asyncio.get_running_loop()
        with ProcessPoolExecutor(max_workers=1) as pool:
            await loop.run_in_executor(pool, heavy_cpu_task)

    await asyncio.gather(
        mock_websocket_heartbeat(),
        mock_keras_prediction()
    )

    # Identificar picos de delay (O teste irá focar no máximo delay ocorrido)
    max_latency = max(latencies)
    
    # Valida usando o threshold de Ouro (50ms para alta frequência)
    assert max_latency < 50.0, f"⚠️ FALHA HFT: GIL bloqueou o Event Loop em {max_latency:.2f}ms (Threshold = 50ms)."
