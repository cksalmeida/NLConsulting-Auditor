"""
Testes unitários de extractor.py.
Execute com: python -m pytest tests/ -v
(ou sem pytest: python tests/test_extractor.py)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extractor import (
    parse_deterministic, read_file_safe, extract_document,
    ExtractionResult, EXPECTED_FIELDS,
)


# ---------------------------------------------------------------
# Auxiliar: mock de Claude que sempre falha
# ---------------------------------------------------------------

class _FailingClaude:
    def extract(self, raw_text, filename):
        raise RuntimeError("Simulated API failure")


# ---------------------------------------------------------------
# parse_deterministic
# ---------------------------------------------------------------

DOC_COMPLETO = "\n".join([
    "TIPO_DOCUMENTO: Nota Fiscal",
    "NUMERO_DOCUMENTO: NF-00123",
    "DATA_EMISSAO: 10/01/2024",
    "FORNECEDOR: Empresa ABC Ltda",
    "CNPJ_FORNECEDOR: 11.222.333/0001-81",
    "DESCRICAO_SERVICO: Consultoria de TI",
    "VALOR_BRUTO: R$ 15.000,00",
    "DATA_PAGAMENTO: 20/01/2024",
    "DATA_EMISSAO_NF: 10/01/2024",
    "APROVADO_POR: Maria Silva",
    "BANCO_DESTINO: Banco do Brasil",
    "STATUS: PAGO",
    "HASH_VERIFICACAO: NLC0012300",
])


def test_parse_deterministic_separador_dois_pontos():
    fields = parse_deterministic(DOC_COMPLETO)
    assert fields["NUMERO_DOCUMENTO"] == "NF-00123"
    assert fields["FORNECEDOR"] == "Empresa ABC Ltda"
    assert fields["VALOR_BRUTO"] == "R$ 15.000,00"
    assert fields["STATUS"] == "PAGO"
    print("✅ test_parse_deterministic_separador_dois_pontos")


def test_parse_deterministic_separador_igual():
    raw = "NUMERO_DOCUMENTO = NF-00456\nFORNECEDOR = Tech Ltda\nVALOR_BRUTO = R$ 1.000,00"
    fields = parse_deterministic(raw)
    assert fields["NUMERO_DOCUMENTO"] == "NF-00456"
    assert fields["FORNECEDOR"] == "Tech Ltda"
    assert fields["VALOR_BRUTO"] == "R$ 1.000,00"
    print("✅ test_parse_deterministic_separador_igual")


def test_parse_deterministic_chave_com_espaco():
    """Chave com espaço deve ser normalizada para underscore."""
    raw = "NUMERO DOCUMENTO: NF-789\nTIPO DOCUMENTO: Recibo"
    fields = parse_deterministic(raw)
    assert fields["NUMERO_DOCUMENTO"] == "NF-789"
    assert fields["TIPO_DOCUMENTO"] == "Recibo"
    print("✅ test_parse_deterministic_chave_com_espaco")


def test_parse_deterministic_valor_com_dois_pontos():
    """Dois pontos no valor não devem quebrar o parsing."""
    raw = "BANCO_DESTINO: Bradesco: Agência 1234\nSTATUS: PAGO"
    fields = parse_deterministic(raw)
    assert fields["BANCO_DESTINO"] == "Bradesco: Agência 1234"
    assert fields["STATUS"] == "PAGO"
    print("✅ test_parse_deterministic_valor_com_dois_pontos")


def test_parse_deterministic_campo_desconhecido_ignorado():
    raw = "NUMERO_DOCUMENTO: NF-001\nCAMPO_INEXISTENTE: algum valor\nSTATUS: PAGO"
    fields = parse_deterministic(raw)
    assert fields["NUMERO_DOCUMENTO"] == "NF-001"
    assert fields["STATUS"] == "PAGO"
    assert "CAMPO_INEXISTENTE" not in fields
    print("✅ test_parse_deterministic_campo_desconhecido_ignorado")


def test_parse_deterministic_documento_vazio():
    fields = parse_deterministic("")
    assert all(v is None for v in fields.values())
    assert set(fields.keys()) == set(EXPECTED_FIELDS)
    print("✅ test_parse_deterministic_documento_vazio")


def test_parse_deterministic_linha_sem_valor():
    """Linha com chave mas sem valor não deve sobrescrever None."""
    raw = "NUMERO_DOCUMENTO:\nFORNECEDOR: Empresa X"
    fields = parse_deterministic(raw)
    assert fields["NUMERO_DOCUMENTO"] is None
    assert fields["FORNECEDOR"] == "Empresa X"
    print("✅ test_parse_deterministic_linha_sem_valor")


def test_parse_deterministic_todos_campos_extraidos():
    fields = parse_deterministic(DOC_COMPLETO)
    nulos = [k for k, v in fields.items() if v is None]
    assert nulos == [], f"Campos não extraídos: {nulos}"
    print("✅ test_parse_deterministic_todos_campos_extraidos")


# ---------------------------------------------------------------
# read_file_safe
# ---------------------------------------------------------------

def test_read_file_safe_utf8_valido():
    conteudo = "FORNECEDOR: Empresa Ação Ltda\nSTATUS: PAGO"
    raw = conteudo.encode("utf-8")
    texto, teve_problema = read_file_safe(raw)
    assert teve_problema is False
    assert "Ação" in texto
    print("✅ test_read_file_safe_utf8_valido")


def test_read_file_safe_cp1252_preserva_euro():
    """
    Byte 0x80 é € em cp1252 mas caractere de controle em latin-1.
    Com cp1252, o contexto ao redor deve ser preservado intacto.
    """
    # Constrói bytes diretamente: 0x80 é inválido em UTF-8 e é € em cp1252
    raw = b"FORNECEDOR: Empresa " + bytes([0x80]) + b"uro SA\nSTATUS: PAGO"
    texto, teve_problema = read_file_safe(raw)
    assert teve_problema is True
    assert "uro SA" in texto   # contexto ao redor do símbolo deve estar intacto
    assert "PAGO" in texto
    print("✅ test_read_file_safe_cp1252_preserva_euro")


def test_read_file_safe_bytes_controle_removidos():
    """
    Bytes de controle são removidos no caminho de fallback (arquivo não-UTF8).
    0x80 força o fallback; 0x01 e 0x02 são controles que devem ser limpos.
    """
    # 0x80 invalida UTF-8 → vai para o fallback cp1252 onde a limpeza é aplicada
    raw = bytes([0x80]) + b"STATUS: " + bytes([0x01, 0x02]) + b"PAGO\nFORNECEDOR: X"
    texto, teve_problema = read_file_safe(raw)
    assert teve_problema is True
    assert "\x01" not in texto
    assert "\x02" not in texto
    assert "PAGO" in texto
    print("✅ test_read_file_safe_bytes_controle_removidos")


def test_read_file_safe_aceita_bytearray():
    raw = bytearray("STATUS: PAGO".encode("utf-8"))
    texto, teve_problema = read_file_safe(raw)
    assert teve_problema is False
    assert "PAGO" in texto
    print("✅ test_read_file_safe_aceita_bytearray")


# ---------------------------------------------------------------
# extract_document — caminho regex (sem Claude)
# ---------------------------------------------------------------

def test_extract_document_caminho_feliz_regex():
    result = extract_document(DOC_COMPLETO, "doc.txt")
    assert result.source == "regex"
    assert result.confidence == "Alto"
    assert result.error is None
    assert result.fields["NUMERO_DOCUMENTO"] == "NF-00123"
    assert result.fields["FORNECEDOR"] == "Empresa ABC Ltda"
    print("✅ test_extract_document_caminho_feliz_regex")


def test_extract_document_campos_criticos_ausentes():
    """Sem FORNECEDOR e VALOR_BRUTO → confiança Baixo e observação informativa."""
    raw = "NUMERO_DOCUMENTO: NF-001\nSTATUS: PAGO"
    result = extract_document(raw, "incompleto.txt")
    assert result.confidence == "Baixo"
    assert "FORNECEDOR" in result.observations or "VALOR_BRUTO" in result.observations
    print("✅ test_extract_document_campos_criticos_ausentes")


def test_extract_document_encoding_issue_sem_claude():
    """encoding_issue=True sem Claude → observação registrada, source=regex."""
    result = extract_document(DOC_COMPLETO, "enc.txt", encoding_issue=True)
    assert result.source == "regex"
    assert "encoding" in result.observations.lower()
    print("✅ test_extract_document_encoding_issue_sem_claude")


def test_extract_document_force_ai_sem_claude_usa_regex():
    """force_ai=True mas claude=None → degradação graciosa para regex."""
    result = extract_document(DOC_COMPLETO, "doc.txt", claude=None, force_ai=True)
    assert result.source == "regex"
    assert result.error is None
    print("✅ test_extract_document_force_ai_sem_claude_usa_regex")


def test_extract_document_claude_falha_usa_regex():
    """Se o Claude lançar exceção → fallback para regex com error registrado."""
    raw = "NUMERO_DOCUMENTO: NF-999\nSTATUS: PAGO"  # campos críticos ausentes → need_ai=True
    result = extract_document(raw, "falha.txt", claude=_FailingClaude())
    assert result.source == "regex (fallback: IA falhou)"
    assert result.confidence == "Baixo"
    assert result.error is not None
    assert "Simulated API failure" in result.error
    print("✅ test_extract_document_claude_falha_usa_regex")


# ---------------------------------------------------------------
# ExtractionResult.to_dict
# ---------------------------------------------------------------

def test_extraction_result_to_dict_estrutura():
    result = extract_document(DOC_COMPLETO, "doc.txt")
    d = result.to_dict()

    # Todos os EXPECTED_FIELDS devem existir como chaves de topo
    for campo in EXPECTED_FIELDS:
        assert campo in d, f"Campo ausente no dict: {campo}"

    # Metadados devem existir
    for meta in ["confianca_extracao", "fonte_extracao", "versao_prompt",
                 "modelo_ia", "latencia_ms", "erro_extracao"]:
        assert meta in d, f"Metadado ausente: {meta}"

    assert d["arquivo"] == "doc.txt"
    print("✅ test_extraction_result_to_dict_estrutura")


if __name__ == "__main__":
    # parse_deterministic
    test_parse_deterministic_separador_dois_pontos()
    test_parse_deterministic_separador_igual()
    test_parse_deterministic_chave_com_espaco()
    test_parse_deterministic_valor_com_dois_pontos()
    test_parse_deterministic_campo_desconhecido_ignorado()
    test_parse_deterministic_documento_vazio()
    test_parse_deterministic_linha_sem_valor()
    test_parse_deterministic_todos_campos_extraidos()
    # read_file_safe
    test_read_file_safe_utf8_valido()
    test_read_file_safe_cp1252_preserva_euro()
    test_read_file_safe_bytes_controle_removidos()
    test_read_file_safe_aceita_bytearray()
    # extract_document
    test_extract_document_caminho_feliz_regex()
    test_extract_document_campos_criticos_ausentes()
    test_extract_document_encoding_issue_sem_claude()
    test_extract_document_force_ai_sem_claude_usa_regex()
    test_extract_document_claude_falha_usa_regex()
    # ExtractionResult
    test_extraction_result_to_dict_estrutura()
    print("\n🎉 Todos os testes de extractor passaram.")
