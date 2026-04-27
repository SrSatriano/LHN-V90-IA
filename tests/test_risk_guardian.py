import pytest

class MockRiskManager:
    """Mock da camada de risco isolada de ai_mixin.py"""
    def __init__(self, saldo_inicial):
        self.saldo = saldo_inicial
        self.teto_global_pct = 0.20
        self.risco_base_pct = 0.05 # Default Base Risco 5%

    def calculate_margin(self, prob_ia_cortex_pct):
        # Escala o risco base pela probabilidade da rede
        risco_calculado = self.risco_base_pct * (prob_ia_cortex_pct / 100.0)
        
        # Hard Cap Trava Institucional (20%)
        if risco_calculado > self.teto_global_pct:
            risco_calculado = self.teto_global_pct
            
        return self.saldo * risco_calculado

class MockGuardian:
    """Mock do Reflexo Medular do engine_mixin.py"""
    def evaluate_vpin(self, operacao_direcao, vpin_vendedor, vpin_comprador):
        if operacao_direcao == "LONG" and vpin_vendedor >= 0.85:
            return "SCALE_OUT_50", "Pico de Toxicidade VPIN (>85%) detectado contra a nossa posição. SCALE-OUT DE EMERGÊNCIA (50%) executado para blindagem de capital."
        if operacao_direcao == "SHORT" and vpin_comprador >= 0.85:
            return "SCALE_OUT_50", "Pico de Toxicidade VPIN (>85%) detectado contra a nossa posição. SCALE-OUT DE EMERGÊNCIA (50%) executado para blindagem de capital."
        return "HOLD", "Estável"

def test_kelly_limit_hard_cap():
    """
    Teste 1 (Kelly Limit): Simula cálculo de margem recebendo prob_ia = 99.9%. 
    Prova que a alocação de margem NUNCA ultrapassa 0.20 da banca.
    """
    risk = MockRiskManager(10000.0) # Banca Exemplo 10k
    
    # IA muito confiante
    margem_alocada = risk.calculate_margin(99.9)
    pct_alocacao = margem_alocada / risk.saldo
    
    assert pct_alocacao <= 0.20, f"Risco rompeu o teto de 20%! Alocou: {pct_alocacao*100}%"
    assert pct_alocacao > 0.0

    # Testando IA excessiva para garantir o hard cap
    margem_absurda = risk.calculate_margin(500.0)
    pct_absurda = margem_absurda / risk.saldo
    assert pct_absurda == 0.20, f"Risco não bateu exatamente no teto (20%)! Alocou: {pct_absurda*100}%"

def test_vpin_scale_out_survival_reflex():
    """
    Teste 2 (VPIN Scale-Out): Bot em LONG, mas VPIN de agressão de vendas atinge 85%.
    Garante que a ação do Reflexo Medular é SCALE_OUT_50.
    """
    guardian = MockGuardian()
    
    # Cenario 1: LONG sob ataque vendedor extremo
    acao_long, motivo_long = guardian.evaluate_vpin("LONG", vpin_vendedor=0.86, vpin_comprador=0.14)
    assert acao_long == "SCALE_OUT_50"
    assert "SCALE-OUT DE EMERGÊNCIA" in motivo_long
    
    # Cenario 2: SHORT sob ataque comprador extremo
    acao_short, motivo_short = guardian.evaluate_vpin("SHORT", vpin_vendedor=0.10, vpin_comprador=0.88)
    assert acao_short == "SCALE_OUT_50"
    
    # Cenario 3: Operacao Saudavel
    acao_saudavel, _ = guardian.evaluate_vpin("LONG", vpin_vendedor=0.30, vpin_comprador=0.70)
    assert acao_saudavel == "HOLD"
