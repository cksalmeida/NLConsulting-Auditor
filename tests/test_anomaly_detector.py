"""
Testes unitários dos detectores de anomalia.
Execute com: python -m pytest tests/ -v
(ou sem pytest: python tests/test_anomaly_detector.py)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from anomaly_detector import (
    detect_nf_duplicada, detect_cnpj_divergente,
    detect_nf_apos_pagamento, detect_aprovador_desconhecido,
    detect_status_invalido, detect_status_inconsistente,
    detect_campo_ausente_ou_corrompido, detect_fornecedor_sem_historico,
    detect_valor_fora_faixa, run_all_detectors,
    build_baseline, _parse_money,
)


# Fixture: dataset fictício mínimo mas realista (20 docs do mesmo fornecedor)
DOCS_BASE = [
    *[{
        "arquivo": f"DOC_{i:04d}.txt",
        "NUMERO_DOCUMENTO": f"NF-{i:05d}",
        "FORNECEDOR": "TechSoft Ltda",
        "CNPJ_FORNECEDOR": "11.222.333/0001-81",  # CNPJ válido
        "APROVADO_POR": "Maria Silva",
        "VALOR_BRUTO": "R$ 5.000,00",
        "DATA_EMISSAO_NF": "10/01/2024",
        "DATA_PAGAMENTO": "15/01/2024",
        "STATUS": "PAGO",
        "HASH_VERIFICACAO": f"NLC{i:07d}",
    } for i in range(1, 21)],
]


# ---------------------------------------------------------------
# Regras originais
# ---------------------------------------------------------------

def test_nf_duplicada_detecta_mesmo_numero_e_fornecedor():
    docs = [
        {"arquivo": "a.txt", "NUMERO_DOCUMENTO": "NF-100", "FORNECEDOR": "X"},
        {"arquivo": "b.txt", "NUMERO_DOCUMENTO": "NF-100", "FORNECEDOR": "X"},
        {"arquivo": "c.txt", "NUMERO_DOCUMENTO": "NF-100", "FORNECEDOR": "Y"},  # fornecedor diferente → não é dup
    ]
    anomalies = detect_nf_duplicada(docs)
    arquivos = {a.arquivo for a in anomalies}
    assert arquivos == {"a.txt", "b.txt"}, f"Esperado a.txt e b.txt, recebido {arquivos}"
    print("✅ test_nf_duplicada_detecta_mesmo_numero_e_fornecedor")


def test_cnpj_divergente_usa_canonico():
    docs = DOCS_BASE + [
        {"arquivo": "x.txt", "FORNECEDOR": "TechSoft Ltda",
         "CNPJ_FORNECEDOR": "99.999.999/0001-99"},
    ]
    baseline = build_baseline(docs)
    anomalies = detect_cnpj_divergente(docs, baseline)
    assert any(a.arquivo == "x.txt" for a in anomalies), "Deveria detectar CNPJ divergente"
    print("✅ test_cnpj_divergente_usa_canonico")


def test_nf_apos_pagamento():
    docs = [
        {"arquivo": "ok.txt", "DATA_EMISSAO_NF": "10/01/2024", "DATA_PAGAMENTO": "15/01/2024"},
        {"arquivo": "bad.txt", "DATA_EMISSAO_NF": "20/01/2024", "DATA_PAGAMENTO": "15/01/2024"},
    ]
    anomalies = detect_nf_apos_pagamento(docs)
    assert len(anomalies) == 1
    assert anomalies[0].arquivo == "bad.txt"
    print("✅ test_nf_apos_pagamento")


def test_aprovador_desconhecido():
    docs = DOCS_BASE + [
        {"arquivo": "intruso.txt", "APROVADO_POR": "João Ninguém", "FORNECEDOR": "TechSoft Ltda"},
    ]
    baseline = build_baseline(docs)
    anomalies = detect_aprovador_desconhecido(docs, baseline)
    assert any(a.arquivo == "intruso.txt" and "João Ninguém" in a.valor_evidencia
               for a in anomalies)
    print("✅ test_aprovador_desconhecido")


def test_status_invalido_truncado():
    docs = [
        {"arquivo": "truncado.txt", "STATUS": "PAG"},
        {"arquivo": "ok.txt", "STATUS": "PAGO"},
    ]
    anomalies = detect_status_invalido(docs)
    assert len(anomalies) == 1 and anomalies[0].arquivo == "truncado.txt"
    print("✅ test_status_invalido_truncado")


def test_status_inconsistente_pendente_com_pagamento():
    docs = [
        {"arquivo": "inc.txt", "STATUS": "PENDENTE", "DATA_PAGAMENTO": "10/01/2024"},
        {"arquivo": "ok.txt", "STATUS": "PENDENTE", "DATA_PAGAMENTO": ""},
    ]
    anomalies = detect_status_inconsistente(docs)
    assert len(anomalies) == 1
    print("✅ test_status_inconsistente_pendente_com_pagamento")


def test_campo_ausente():
    docs = [
        {"arquivo": "ok.txt", "NUMERO_DOCUMENTO": "NF-1", "FORNECEDOR": "X",
         "VALOR_BRUTO": "R$ 100", "HASH_VERIFICACAO": "ABC"},
        {"arquivo": "sem_hash.txt", "NUMERO_DOCUMENTO": "NF-1", "FORNECEDOR": "X",
         "VALOR_BRUTO": "R$ 100"},
    ]
    anomalies = detect_campo_ausente_ou_corrompido(docs)
    assert len(anomalies) == 1 and anomalies[0].arquivo == "sem_hash.txt"
    print("✅ test_campo_ausente")


def test_fornecedor_sem_historico():
    docs = DOCS_BASE + [
        {"arquivo": "novo.txt", "FORNECEDOR": "Fornecedor Fantasma SA",
         "CNPJ_FORNECEDOR": "00.000.000/0001-00"},
    ]
    baseline = build_baseline(docs)
    anomalies = detect_fornecedor_sem_historico(docs, baseline)
    assert any(a.arquivo == "novo.txt" for a in anomalies)
    print("✅ test_fornecedor_sem_historico")


# ---------------------------------------------------------------
# Limiares adaptativos
# ---------------------------------------------------------------

def test_limiar_adaptativo_lote_pequeno():
    """Em lotes pequenos, o limiar deve ser 2, não 5."""
    docs = [
        {"arquivo": f"doc{i}.txt", "FORNECEDOR": f"Forn{i}", "APROVADO_POR": f"Aprov{i}"}
        for i in range(10)
    ]
    baseline = build_baseline(docs)
    assert baseline.limiar_fornecedor_raro == 2, f"Esperado 2, recebido {baseline.limiar_fornecedor_raro}"
    assert baseline.limiar_aprovador_raro == 2, f"Esperado 2, recebido {baseline.limiar_aprovador_raro}"
    print("✅ test_limiar_adaptativo_lote_pequeno")


def test_limiar_adaptativo_lote_grande():
    """Em lotes grandes (100+), o limiar deve atingir o máximo (5/3)."""
    docs = [{"arquivo": f"d{i}.txt", "FORNECEDOR": "X", "APROVADO_POR": "Y"} for i in range(100)]
    baseline = build_baseline(docs)
    assert baseline.limiar_fornecedor_raro == 5, f"Esperado 5, recebido {baseline.limiar_fornecedor_raro}"
    assert baseline.limiar_aprovador_raro == 3, f"Esperado 3, recebido {baseline.limiar_aprovador_raro}"
    print("✅ test_limiar_adaptativo_lote_grande")


def test_confianca_reduzida_lote_pequeno():
    """Anomalias de baseline em lote pequeno devem ter confiança 'Medio'."""
    docs = [
        {"arquivo": "a.txt", "FORNECEDOR": "Novo Forn", "APROVADO_POR": "Novo Aprov"},
    ]
    baseline = build_baseline(docs)
    anom_forn = detect_fornecedor_sem_historico(docs, baseline)
    anom_aprov = detect_aprovador_desconhecido(docs, baseline)
    assert all(a.confianca == "Medio" for a in anom_forn), "Confiança deveria ser Medio em lote pequeno"
    assert all(a.confianca == "Medio" for a in anom_aprov), "Confiança deveria ser Medio em lote pequeno"
    print("✅ test_confianca_reduzida_lote_pequeno")


# ---------------------------------------------------------------
# _parse_money — formatos BR e US
# ---------------------------------------------------------------

def test_parse_money_formato_br():
    assert _parse_money("R$ 15.000,00") == 15000.0
    assert _parse_money("1.500,50") == 1500.5
    assert _parse_money("R$ 500,00") == 500.0
    print("✅ test_parse_money_formato_br")


def test_parse_money_formato_us():
    assert _parse_money("15,000.00") == 15000.0
    assert _parse_money("1,500.50") == 1500.5
    print("✅ test_parse_money_formato_us")


def test_parse_money_sem_decimal():
    assert _parse_money("15000") == 15000.0
    assert _parse_money("R$ 500") == 500.0
    print("✅ test_parse_money_sem_decimal")


def test_parse_money_nulo():
    assert _parse_money(None) is None
    assert _parse_money("") is None
    assert _parse_money("N/A") is None
    print("✅ test_parse_money_nulo")



# ---------------------------------------------------------------
# detect_valor_fora_faixa — Z-score
# ---------------------------------------------------------------

# 12 docs do mesmo fornecedor com valores próximos (~5000-5200)
# para que o baseline compute média e desvio padrão
_DOCS_ZSCORE_BASE = [
    {
        "arquivo": f"zscore_{i:02d}.txt",
        "FORNECEDOR": "Empresa Regular Ltda",
        "VALOR_BRUTO": f"R$ {5000 + (i % 3) * 100},00",
    }
    for i in range(12)  # valores: 5000, 5100, 5200, repetindo → média ~5100, dp ~82
]


def test_valor_fora_faixa_detecta_outlier():
    """Valor com Z-score >> 3 deve ser flagado."""
    docs = _DOCS_ZSCORE_BASE + [
        {"arquivo": "outlier.txt", "FORNECEDOR": "Empresa Regular Ltda",
         "VALOR_BRUTO": "R$ 50.000,00"},
    ]
    baseline = build_baseline(docs)
    anomalies = detect_valor_fora_faixa(docs, baseline)
    arquivos = {a.arquivo for a in anomalies}
    assert "outlier.txt" in arquivos, "Outlier deveria ser detectado"
    print("✅ test_valor_fora_faixa_detecta_outlier")


def test_valor_fora_faixa_nao_flaga_normal():
    """Valor dentro da faixa não deve gerar anomalia."""
    docs = _DOCS_ZSCORE_BASE  # todos os valores estão dentro da faixa
    baseline = build_baseline(docs)
    anomalies = detect_valor_fora_faixa(docs, baseline)
    assert len(anomalies) == 0, f"Esperado 0 anomalias, recebido {len(anomalies)}"
    print("✅ test_valor_fora_faixa_nao_flaga_normal")


def test_valor_fora_faixa_requer_minimo_10_amostras():
    """Com menos de 10 documentos do fornecedor, a regra não dispara."""
    docs = [
        {"arquivo": f"p{i}.txt", "FORNECEDOR": "Forn Pequeno",
         "VALOR_BRUTO": "R$ 1.000,00"}
        for i in range(5)  # apenas 5 amostras — abaixo do mínimo
    ] + [
        {"arquivo": "outlier.txt", "FORNECEDOR": "Forn Pequeno",
         "VALOR_BRUTO": "R$ 999.999,00"},
    ]
    baseline = build_baseline(docs)
    anomalies = detect_valor_fora_faixa(docs, baseline)
    assert len(anomalies) == 0, "Não deve disparar com < 10 amostras"
    print("✅ test_valor_fora_faixa_requer_minimo_10_amostras")


def test_valor_fora_faixa_desvio_zero_nao_divide():
    """Quando todos os valores são iguais (dp=0), nenhuma divisão por zero."""
    docs = [
        {"arquivo": f"d{i}.txt", "FORNECEDOR": "Forn Fixo",
         "VALOR_BRUTO": "R$ 5.000,00"}
        for i in range(12)
    ]
    baseline = build_baseline(docs)
    anomalies = detect_valor_fora_faixa(docs, baseline)
    assert len(anomalies) == 0, "dp=0 não deve gerar anomalia"
    print("✅ test_valor_fora_faixa_desvio_zero_nao_divide")


# ---------------------------------------------------------------
# Testes de borda — lote vazio e campos None
# ---------------------------------------------------------------

def test_detectores_com_lista_vazia():
    """Todos os detectores devem retornar [] sem exceção para lista vazia."""
    baseline = build_baseline([])
    assert detect_nf_duplicada([]) == []
    assert detect_cnpj_divergente([], baseline) == []
    assert detect_fornecedor_sem_historico([], baseline) == []
    assert detect_nf_apos_pagamento([]) == []
    assert detect_aprovador_desconhecido([], baseline) == []
    assert detect_valor_fora_faixa([], baseline) == []
    assert detect_status_invalido([]) == []
    assert detect_status_inconsistente([]) == []
    assert detect_campo_ausente_ou_corrompido([]) == []
    print("✅ test_detectores_com_lista_vazia")


def test_detectores_com_campos_none():
    """Documentos com todos os campos None não devem causar exceções."""
    docs = [{"arquivo": f"vazio{i}.txt"} for i in range(3)]
    baseline = build_baseline(docs)
    # Nenhuma dessas chamadas deve lançar exceção
    detect_nf_duplicada(docs)
    detect_cnpj_divergente(docs, baseline)
    detect_fornecedor_sem_historico(docs, baseline)
    detect_nf_apos_pagamento(docs)
    detect_aprovador_desconhecido(docs, baseline)
    detect_valor_fora_faixa(docs, baseline)
    detect_status_invalido(docs)
    detect_status_inconsistente(docs)
    detect_campo_ausente_ou_corrompido(docs)
    print("✅ test_detectores_com_campos_none")


def test_build_baseline_lista_vazia():
    """build_baseline com lista vazia deve retornar Baseline válido."""
    baseline = build_baseline([])
    assert baseline.total_docs == 0
    assert baseline.limiar_fornecedor_raro == 2
    assert baseline.limiar_aprovador_raro == 2
    assert baseline.cnpj_canonico_por_fornecedor == {}
    assert baseline.aprovadores_conhecidos == set()
    assert baseline.fornecedores_conhecidos == set()
    print("✅ test_build_baseline_lista_vazia")


def test_run_all_detectors_nao_quebra_com_vazio():
    """Orquestrador completo com lista vazia não deve lançar exceção."""
    anomalies, baseline = run_all_detectors([], [])
    assert anomalies == []
    assert baseline.total_docs == 0
    print("✅ test_run_all_detectors_nao_quebra_com_vazio")


if __name__ == "__main__":
    # Regras originais
    test_nf_duplicada_detecta_mesmo_numero_e_fornecedor()
    test_cnpj_divergente_usa_canonico()
    test_nf_apos_pagamento()
    test_aprovador_desconhecido()
    test_status_invalido_truncado()
    test_status_inconsistente_pendente_com_pagamento()
    test_campo_ausente()
    test_fornecedor_sem_historico()
    # Limiares adaptativos
    test_limiar_adaptativo_lote_pequeno()
    test_limiar_adaptativo_lote_grande()
    test_confianca_reduzida_lote_pequeno()
    # _parse_money
    test_parse_money_formato_br()
    test_parse_money_formato_us()
    test_parse_money_sem_decimal()
    test_parse_money_nulo()
    # Z-score
    test_valor_fora_faixa_detecta_outlier()
    test_valor_fora_faixa_nao_flaga_normal()
    test_valor_fora_faixa_requer_minimo_10_amostras()
    test_valor_fora_faixa_desvio_zero_nao_divide()
    # Borda
    test_detectores_com_lista_vazia()
    test_detectores_com_campos_none()
    test_build_baseline_lista_vazia()
    test_run_all_detectors_nao_quebra_com_vazio()
    print("\n🎉 Todos os testes passaram.")
