"""
anomaly_detector.py
===================
Detecção de anomalias em documentos financeiros. Cada detector é uma função
que recebe:
  - o documento atual (dict de campos extraídos)
  - o "universo" de todos os documentos (dicts) — pra comparações entre docs

e retorna uma lista de Anomaly. As regras são determinísticas e explicáveis
(cada anomalia aponta o campo-evidência), para que o auditor humano possa
investigar rapidamente.

Design: separamos DETECÇÃO (aqui) da EXTRAÇÃO (extractor.py). A IA extrai
os campos; as regras de auditoria são código tradicional — mais barato,
rastreável, testável e determinístico.
"""
from __future__ import annotations
import re
import statistics
from dataclasses import dataclass, asdict, field
from datetime import datetime
from collections import Counter, defaultdict


# Gravidade: Alto / Médio / Baixo — reflete o peso na avaliação do briefing
GRAVIDADE_ALTA = "Alto"
GRAVIDADE_MEDIA = "Medio"
GRAVIDADE_BAIXA = "Baixo"

# Abaixo desse total de docs, regras dependentes de baseline histórico
# são emitidas com confiança reduzida ("Medio" em vez de "Alto").
_LIMIAR_CONFIANCA_BASELINE = 50

# Lotes com menos de 3 documentos não têm histórico suficiente para as
# regras de fornecedor/aprovador — todas as ocorrências são "únicas" por definição.
_MIN_DOCS_REGRAS_BASELINE = 3


@dataclass
class Anomaly:
    """Uma anomalia detectada em um documento."""
    arquivo: str
    regra: str                 # código da regra (ex: "NF_DUPLICADA")
    descricao: str             # texto humano
    campos_evidencia: str      # quais campos sustentam a anomalia
    valor_evidencia: str       # valor(es) específicos que dispararam
    gravidade: str             # Alto | Medio | Baixo
    confianca: str             # Alto | Medio | Baixo — da detecção, não da extração

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------

def _parse_date(s: str | None) -> datetime | None:
    if not s:
        return None
    s = s.strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _parse_money(s: str | None) -> float | None:
    """
    Converte string monetária para float, detectando o formato antes de converter.

    Formatos suportados:
      BR: "R$ 15.000,00"  → vírgula é o separador decimal
      US: "15,000.00"     → ponto é o separador decimal
      Inteiro: "15000"    → sem separador decimal
    """
    if not s:
        return None
    cleaned = re.sub(r"[^\d,.\-]", "", s)
    if not cleaned:
        return None

    if re.search(r",\d{1,2}$", cleaned):
        # Formato BR: vírgula no final é o decimal → remove pontos, troca vírgula
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif re.search(r"\.\d{1,2}$", cleaned):
        # Formato US: ponto no final é o decimal → apenas remove vírgulas de milhar
        cleaned = cleaned.replace(",", "")
    else:
        # Sem separador decimal identificável: remove todos os separadores
        cleaned = re.sub(r"[,.]", "", cleaned)

    try:
        return float(cleaned)
    except ValueError:
        return None


def _normalize_cnpj(s: str | None) -> str | None:
    """Retorna apenas os 14 dígitos do CNPJ, ou None se inválido."""
    if not s:
        return None
    digits = re.sub(r"\D", "", s)
    return digits if len(digits) == 14 else None



# ---------------------------------------------------------------
# Listas de referência (baselines) — computadas do próprio dataset
# ---------------------------------------------------------------

@dataclass
class Baseline:
    """Perfil agregado do dataset, usado como referência pras regras."""
    cnpj_canonico_por_fornecedor: dict[str, str]
    aprovadores_conhecidos: set[str]
    fornecedores_conhecidos: set[str]
    stats_valor_por_fornecedor: dict[str, tuple[float, float]]  # (média, desvio_padrão)
    total_docs: int = 0
    limiar_fornecedor_raro: int = 5
    limiar_aprovador_raro: int = 3


def build_baseline(docs: list[dict]) -> Baseline:
    """
    Constrói o perfil agregado a partir dos documentos extraídos.

    Os limiares de "fornecedor raro" e "aprovador raro" são adaptativos:
    escalam com o tamanho do lote para evitar 100% de falsos positivos
    em lotes pequenos onde ninguém atinge os limiares fixos.
    """
    total = len(docs)

    # Limiares adaptativos: mínimo 2, máximo histórico (5/3), escalam com o lote.
    limiar_forn = max(2, min(5, total // 20))
    limiar_aprov = max(2, min(3, total // 30))

    cnpj_counter: dict[str, Counter] = defaultdict(Counter)
    aprov_counter: Counter = Counter()
    forn_counter: Counter = Counter()
    valores_por_forn: dict[str, list[float]] = defaultdict(list)

    for d in docs:
        forn = d.get("FORNECEDOR")
        cnpj = d.get("CNPJ_FORNECEDOR")
        aprov = d.get("APROVADO_POR")
        valor = _parse_money(d.get("VALOR_BRUTO"))

        if forn:
            forn_counter[forn] += 1
            if cnpj:
                cnpj_counter[forn][cnpj] += 1
            if valor is not None:
                valores_por_forn[forn].append(valor)
        if aprov:
            aprov_counter[aprov] += 1

    cnpj_canon = {
        forn: cnt.most_common(1)[0][0]
        for forn, cnt in cnpj_counter.items()
        if cnt
    }
    aprov_conhecidos = {a for a, q in aprov_counter.items() if q >= limiar_aprov}
    forn_conhecidos = {f for f, q in forn_counter.items() if q >= limiar_forn}

    stats = {}
    for forn, vals in valores_por_forn.items():
        if len(vals) >= 10:
            stats[forn] = (statistics.mean(vals), statistics.pstdev(vals))

    return Baseline(
        cnpj_canonico_por_fornecedor=cnpj_canon,
        aprovadores_conhecidos=aprov_conhecidos,
        fornecedores_conhecidos=forn_conhecidos,
        stats_valor_por_fornecedor=stats,
        total_docs=total,
        limiar_fornecedor_raro=limiar_forn,
        limiar_aprovador_raro=limiar_aprov,
    )


# ---------------------------------------------------------------
# Detectores individuais
# ---------------------------------------------------------------

def detect_nf_duplicada(docs: list[dict]) -> list[Anomaly]:
    """Mesmo NUMERO_DOCUMENTO + mesmo FORNECEDOR em dois ou mais arquivos."""
    key_to_files: dict[tuple[str, str], list[str]] = defaultdict(list)
    for d in docs:
        num = d.get("NUMERO_DOCUMENTO")
        forn = d.get("FORNECEDOR")
        if num and forn:
            key_to_files[(num, forn)].append(d["arquivo"])

    anomalies: list[Anomaly] = []
    for (num, forn), files in key_to_files.items():
        if len(files) > 1:
            outros = sorted(files)
            for arq in outros:
                duplicatas = [f for f in outros if f != arq]
                anomalies.append(Anomaly(
                    arquivo=arq,
                    regra="NF_DUPLICADA",
                    descricao=f"NF {num} do fornecedor '{forn}' aparece em {len(files)} arquivos",
                    campos_evidencia="NUMERO_DOCUMENTO, FORNECEDOR",
                    valor_evidencia=f"NF={num}; FORN={forn}; também em: {', '.join(duplicatas)}",
                    gravidade=GRAVIDADE_ALTA,
                    confianca="Alto",
                ))
    return anomalies


def detect_cnpj_divergente(docs: list[dict], baseline: Baseline) -> list[Anomaly]:
    """CNPJ diferente do CNPJ canônico (mais frequente) daquele fornecedor."""
    anomalies: list[Anomaly] = []
    for d in docs:
        forn = d.get("FORNECEDOR")
        cnpj = d.get("CNPJ_FORNECEDOR")
        if not forn or not cnpj:
            continue
        canon = baseline.cnpj_canonico_por_fornecedor.get(forn)
        if canon and _normalize_cnpj(cnpj) != _normalize_cnpj(canon):
            anomalies.append(Anomaly(
                arquivo=d["arquivo"],
                regra="CNPJ_DIVERGENTE",
                descricao=f"CNPJ do fornecedor '{forn}' difere do padrão histórico",
                campos_evidencia="CNPJ_FORNECEDOR, FORNECEDOR",
                valor_evidencia=f"CNPJ recebido={cnpj}; CNPJ canônico={canon}",
                gravidade=GRAVIDADE_ALTA,
                confianca="Alto",
            ))
    return anomalies



def detect_fornecedor_sem_historico(docs: list[dict], baseline: Baseline) -> list[Anomaly]:
    """
    Fornecedor que aparece menos que o limiar adaptativo do lote.
    Confiança reduzida para lotes pequenos, onde o baseline é pouco representativo.
    """
    if baseline.total_docs < _MIN_DOCS_REGRAS_BASELINE:
        return []
    forn_count = Counter(d.get("FORNECEDOR") for d in docs if d.get("FORNECEDOR"))
    confianca = "Alto" if baseline.total_docs >= _LIMIAR_CONFIANCA_BASELINE else "Medio"
    anomalies: list[Anomaly] = []
    for d in docs:
        forn = d.get("FORNECEDOR")
        if not forn:
            continue
        if forn not in baseline.fornecedores_conhecidos:
            anomalies.append(Anomaly(
                arquivo=d["arquivo"],
                regra="FORNECEDOR_SEM_HISTORICO",
                descricao=(
                    f"Fornecedor '{forn}' aparece apenas {forn_count[forn]}x no lote "
                    f"(limiar: {baseline.limiar_fornecedor_raro})"
                ),
                campos_evidencia="FORNECEDOR",
                valor_evidencia=f"Fornecedor={forn}; ocorrências={forn_count[forn]}",
                gravidade=GRAVIDADE_ALTA,
                confianca=confianca,
            ))
    return anomalies


def detect_nf_apos_pagamento(docs: list[dict]) -> list[Anomaly]:
    """DATA_EMISSAO_NF posterior a DATA_PAGAMENTO — indica retroatividade suspeita."""
    anomalies: list[Anomaly] = []
    for d in docs:
        emi = _parse_date(d.get("DATA_EMISSAO_NF"))
        pag = _parse_date(d.get("DATA_PAGAMENTO"))
        if emi and pag and emi > pag:
            delta = (emi - pag).days
            anomalies.append(Anomaly(
                arquivo=d["arquivo"],
                regra="NF_APOS_PAGAMENTO",
                descricao=f"NF emitida {delta} dia(s) após o pagamento",
                campos_evidencia="DATA_EMISSAO_NF, DATA_PAGAMENTO",
                valor_evidencia=f"Emissão NF={d.get('DATA_EMISSAO_NF')}; Pagamento={d.get('DATA_PAGAMENTO')}",
                gravidade=GRAVIDADE_ALTA,
                confianca="Alto",
            ))
    return anomalies


def detect_aprovador_desconhecido(docs: list[dict], baseline: Baseline) -> list[Anomaly]:
    """
    Aprovador fora da lista de aprovadores recorrentes no lote.
    Confiança reduzida para lotes pequenos, onde o baseline é pouco representativo.
    """
    if baseline.total_docs < _MIN_DOCS_REGRAS_BASELINE:
        return []
    confianca = "Alto" if baseline.total_docs >= _LIMIAR_CONFIANCA_BASELINE else "Medio"
    anomalies: list[Anomaly] = []
    for d in docs:
        aprov = d.get("APROVADO_POR")
        if aprov and aprov not in baseline.aprovadores_conhecidos:
            anomalies.append(Anomaly(
                arquivo=d["arquivo"],
                regra="APROVADOR_DESCONHECIDO",
                descricao=(
                    f"Aprovador '{aprov}' fora da lista de aprovadores recorrentes "
                    f"(limiar: {baseline.limiar_aprovador_raro}x)"
                ),
                campos_evidencia="APROVADO_POR",
                valor_evidencia=f"Aprovador={aprov}",
                gravidade=GRAVIDADE_MEDIA,
                confianca=confianca,
            ))
    return anomalies


def detect_valor_fora_faixa(docs: list[dict], baseline: Baseline, z: float = 3.0) -> list[Anomaly]:
    """Z-score > 3 em relação à média histórica daquele fornecedor (requer ≥10 amostras)."""
    anomalies: list[Anomaly] = []
    for d in docs:
        forn = d.get("FORNECEDOR")
        valor = _parse_money(d.get("VALOR_BRUTO"))
        if not forn or valor is None:
            continue
        stats = baseline.stats_valor_por_fornecedor.get(forn)
        if not stats:
            continue
        media, dp = stats
        if dp == 0:
            continue
        zscore = abs(valor - media) / dp
        if zscore > z:
            anomalies.append(Anomaly(
                arquivo=d["arquivo"],
                regra="VALOR_FORA_FAIXA",
                descricao=f"Valor com z-score={zscore:.1f} (>{z}) vs média do fornecedor",
                campos_evidencia="VALOR_BRUTO, FORNECEDOR",
                valor_evidencia=f"Valor=R$ {valor:,.2f}; média fornecedor=R$ {media:,.2f}; dp=R$ {dp:,.2f}",
                gravidade=GRAVIDADE_MEDIA,
                confianca="Medio",
            ))
    return anomalies


STATUS_VALIDOS = {"PAGO", "CANCELADO", "ESTORNADO", "PENDENTE"}


def detect_status_invalido(docs: list[dict]) -> list[Anomaly]:
    """STATUS fora do vocabulário conhecido (ex: 'PAG' truncado)."""
    anomalies: list[Anomaly] = []
    for d in docs:
        status = d.get("STATUS")
        if status and status.strip().upper() not in STATUS_VALIDOS:
            anomalies.append(Anomaly(
                arquivo=d["arquivo"],
                regra="STATUS_INVALIDO",
                descricao=f"STATUS '{status}' não consta no vocabulário válido",
                campos_evidencia="STATUS",
                valor_evidencia=f"STATUS={status}; válidos={sorted(STATUS_VALIDOS)}",
                gravidade=GRAVIDADE_MEDIA,
                confianca="Alto",
            ))
    return anomalies


def detect_status_inconsistente(docs: list[dict]) -> list[Anomaly]:
    """
    PENDENTE com DATA_PAGAMENTO preenchida é contradição lógica (se está
    pendente, ainda não foi pago). CANCELADO e ESTORNADO com pagamento são
    esperados (cancelamento/estorno é posterior ao pagamento).
    """
    anomalies: list[Anomaly] = []
    for d in docs:
        status = (d.get("STATUS") or "").strip().upper()
        pag = d.get("DATA_PAGAMENTO")
        if status == "PENDENTE" and pag:
            anomalies.append(Anomaly(
                arquivo=d["arquivo"],
                regra="STATUS_INCONSISTENTE",
                descricao="Documento marcado PENDENTE mas com DATA_PAGAMENTO preenchida",
                campos_evidencia="STATUS, DATA_PAGAMENTO",
                valor_evidencia=f"STATUS=PENDENTE; DATA_PAGAMENTO={pag}",
                gravidade=GRAVIDADE_BAIXA,
                confianca="Baixo",
            ))
    return anomalies


def detect_campo_ausente_ou_corrompido(docs: list[dict]) -> list[Anomaly]:
    """Campos críticos faltando — o arquivo está mal-formado."""
    criticos = ["NUMERO_DOCUMENTO", "FORNECEDOR", "VALOR_BRUTO", "HASH_VERIFICACAO"]
    anomalies: list[Anomaly] = []
    for d in docs:
        faltando = [c for c in criticos if not d.get(c)]
        if faltando:
            anomalies.append(Anomaly(
                arquivo=d["arquivo"],
                regra="CAMPO_AUSENTE",
                descricao=f"Campos críticos ausentes: {', '.join(faltando)}",
                campos_evidencia=", ".join(faltando),
                valor_evidencia=f"Ausentes: {faltando}",
                gravidade=GRAVIDADE_MEDIA,
                confianca="Alto",
            ))
    return anomalies


def detect_encoding_problema(extraction_results: list) -> list[Anomaly]:
    """Baseado nas obs da extração — arquivo teve bytes corrompidos."""
    anomalies: list[Anomaly] = []
    for r in extraction_results:
        obs = (r.observations or "").lower()
        raw = r.raw_excerpt or ""
        if ("encoding" in obs or "corromp" in obs or "mojibake" in obs
                or "�" in raw):
            anomalies.append(Anomaly(
                arquivo=r.filename,
                regra="ENCODING_INVALIDO",
                descricao="Arquivo contém bytes corrompidos ou encoding não-padrão",
                campos_evidencia="arquivo bruto",
                valor_evidencia=f"Obs: {r.observations[:120]}",
                gravidade=GRAVIDADE_MEDIA,
                confianca="Alto",
            ))
    return anomalies


# ---------------------------------------------------------------
# Orquestrador
# ---------------------------------------------------------------

def run_all_detectors(docs: list[dict], extraction_results: list) -> tuple[list[Anomaly], Baseline]:
    """
    Roda todas as regras e retorna a lista consolidada + o baseline usado.
    docs = lista de dicts de campos extraídos (um por arquivo)
    extraction_results = lista de ExtractionResult (pra regra de encoding)
    """
    baseline = build_baseline(docs)

    all_anomalies: list[Anomaly] = []
    all_anomalies += detect_nf_duplicada(docs)
    all_anomalies += detect_cnpj_divergente(docs, baseline)
    all_anomalies += detect_fornecedor_sem_historico(docs, baseline)
    all_anomalies += detect_nf_apos_pagamento(docs)
    all_anomalies += detect_aprovador_desconhecido(docs, baseline)
    all_anomalies += detect_valor_fora_faixa(docs, baseline)
    all_anomalies += detect_status_invalido(docs)
    all_anomalies += detect_status_inconsistente(docs)
    all_anomalies += detect_campo_ausente_ou_corrompido(docs)
    all_anomalies += detect_encoding_problema(extraction_results)

    return all_anomalies, baseline
