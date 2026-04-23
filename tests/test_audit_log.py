"""
Testes unitários de audit_log.py.
Execute com: python -m pytest tests/ -v
(ou sem pytest: python tests/test_audit_log.py)
"""
import sys, os, json, csv, io
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from audit_log import AuditLog


def _make_log_with_events() -> AuditLog:
    log = AuditLog()
    log.log_read("doc1.txt", 1024, encoding_ok=True)
    log.log_extract("doc1.txt", fonte="regex", confianca="Alto",
                    versao_prompt="v1.3.0", modelo="-", latency_ms=5)
    log.log_detect("doc1.txt", regra="NF_DUPLICADA",
                   descricao="NF duplicada", campo_evidencia="NUMERO_DOCUMENTO",
                   confianca="Alto")
    log.log_error("doc2.txt", "Erro simulado de leitura")
    return log


# ---------------------------------------------------------------
# log_summary
# ---------------------------------------------------------------

def test_log_summary_cria_evento_summary():
    log = AuditLog()
    log.log_summary(
        total_docs=50,
        total_anomalias=7,
        anomalias_por_regra={"NF_DUPLICADA": 3, "CNPJ_INVALIDO": 4},
        erros=1,
        modelo="claude-sonnet-4-6",
        versao_prompt="v1.3.0",
        duracao_seg=12.5,
    )
    assert len(log.entries) == 1
    entry = log.entries[0]
    assert entry.evento == "SUMMARY"
    assert "docs=50" in entry.detalhe
    assert "anomalias=7" in entry.detalhe
    assert "erros=1" in entry.detalhe
    assert "duracao=12.5s" in entry.detalhe
    assert "NF_DUPLICADA=3" in entry.detalhe
    assert "CNPJ_INVALIDO=4" in entry.detalhe
    assert entry.modelo_ia == "claude-sonnet-4-6"
    assert entry.versao_prompt == "v1.3.0"
    print("✅ test_log_summary_cria_evento_summary")


def test_log_summary_regras_ordenadas_alfabeticamente():
    """Regras no detalhe devem estar ordenadas para output determinístico."""
    log = AuditLog()
    log.log_summary(
        total_docs=10, total_anomalias=3,
        anomalias_por_regra={"Z_REGRA": 1, "A_REGRA": 2},
        erros=0, modelo="-", versao_prompt="v1.0", duracao_seg=1.0,
    )
    detalhe = log.entries[0].detalhe
    pos_a = detalhe.index("A_REGRA")
    pos_z = detalhe.index("Z_REGRA")
    assert pos_a < pos_z, "Regras devem estar em ordem alfabética"
    print("✅ test_log_summary_regras_ordenadas_alfabeticamente")


def test_log_summary_sem_anomalias():
    """Sumário com lote limpo não deve quebrar."""
    log = AuditLog()
    log.log_summary(
        total_docs=20, total_anomalias=0, anomalias_por_regra={},
        erros=0, modelo="-", versao_prompt="v1.0", duracao_seg=2.0,
    )
    assert log.entries[0].evento == "SUMMARY"
    assert "anomalias=0" in log.entries[0].detalhe
    print("✅ test_log_summary_sem_anomalias")


# ---------------------------------------------------------------
# log_export
# ---------------------------------------------------------------

def test_log_export_grava_evento():
    log = AuditLog()
    log.log_export("xlsx", 100)
    assert len(log.entries) == 1
    assert log.entries[0].evento == "EXPORT"
    assert "100" in log.entries[0].detalhe
    assert "xlsx" in log.entries[0].detalhe
    print("✅ test_log_export_grava_evento")


# ---------------------------------------------------------------
# to_csv_bytes
# ---------------------------------------------------------------

def test_to_csv_bytes_decodificavel():
    log = _make_log_with_events()
    data = log.to_csv_bytes()
    assert isinstance(data, bytes)
    # BOM + conteúdo
    text = data.decode("utf-8-sig")
    assert len(text) > 0
    print("✅ test_to_csv_bytes_decodificavel")


def test_to_csv_bytes_tem_cabecalho_e_linhas():
    log = _make_log_with_events()
    text = log.to_csv_bytes().decode("utf-8-sig")
    reader = list(csv.DictReader(io.StringIO(text)))
    # 4 eventos: READ, EXTRACT, DETECT, ERROR
    assert len(reader) == 4
    # Colunas essenciais presentes
    for col in ["timestamp", "arquivo", "evento", "detalhe"]:
        assert col in reader[0], f"Coluna ausente: {col}"
    print("✅ test_to_csv_bytes_tem_cabecalho_e_linhas")


def test_to_csv_bytes_log_vazio():
    log = AuditLog()
    data = log.to_csv_bytes()
    assert data == b""
    print("✅ test_to_csv_bytes_log_vazio")


def test_to_csv_bytes_eventos_corretos():
    log = _make_log_with_events()
    text = log.to_csv_bytes().decode("utf-8-sig")
    reader = list(csv.DictReader(io.StringIO(text)))
    eventos = [r["evento"] for r in reader]
    assert eventos == ["READ", "EXTRACT", "DETECT", "ERROR"]
    print("✅ test_to_csv_bytes_eventos_corretos")


# ---------------------------------------------------------------
# to_jsonl_bytes
# ---------------------------------------------------------------

def test_to_jsonl_bytes_cada_linha_e_json_valido():
    log = _make_log_with_events()
    data = log.to_jsonl_bytes()
    lines = data.decode("utf-8").strip().split("\n")
    assert len(lines) == 4
    for line in lines:
        obj = json.loads(line)           # não deve lançar exceção
        assert "timestamp" in obj
        assert "evento" in obj
    print("✅ test_to_jsonl_bytes_cada_linha_e_json_valido")


def test_to_jsonl_bytes_ordem_preservada():
    log = _make_log_with_events()
    lines = log.to_jsonl_bytes().decode("utf-8").strip().split("\n")
    eventos = [json.loads(l)["evento"] for l in lines]
    assert eventos == ["READ", "EXTRACT", "DETECT", "ERROR"]
    print("✅ test_to_jsonl_bytes_ordem_preservada")


def test_to_jsonl_bytes_summary_incluido():
    log = AuditLog()
    log.log_read("f.txt", 512, True)
    log.log_summary(
        total_docs=1, total_anomalias=0, anomalias_por_regra={},
        erros=0, modelo="regex-only", versao_prompt="v1.3.0", duracao_seg=0.1,
    )
    lines = log.to_jsonl_bytes().decode().strip().split("\n")
    eventos = [json.loads(l)["evento"] for l in lines]
    assert "SUMMARY" in eventos
    print("✅ test_to_jsonl_bytes_summary_incluido")


# ---------------------------------------------------------------
# __len__
# ---------------------------------------------------------------

def test_len_reflete_numero_de_entradas():
    log = _make_log_with_events()
    assert len(log) == 4
    log.log_export("csv", 10)
    assert len(log) == 5
    print("✅ test_len_reflete_numero_de_entradas")


if __name__ == "__main__":
    # log_summary
    test_log_summary_cria_evento_summary()
    test_log_summary_regras_ordenadas_alfabeticamente()
    test_log_summary_sem_anomalias()
    # log_export
    test_log_export_grava_evento()
    # to_csv_bytes
    test_to_csv_bytes_decodificavel()
    test_to_csv_bytes_tem_cabecalho_e_linhas()
    test_to_csv_bytes_log_vazio()
    test_to_csv_bytes_eventos_corretos()
    # to_jsonl_bytes
    test_to_jsonl_bytes_cada_linha_e_json_valido()
    test_to_jsonl_bytes_ordem_preservada()
    test_to_jsonl_bytes_summary_incluido()
    # __len__
    test_len_reflete_numero_de_entradas()
    print("\n🎉 Todos os testes de audit_log passaram.")
