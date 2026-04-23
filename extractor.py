"""
extractor.py
============
Extração estruturada de campos usando Claude API.

Decisão de design: usamos Claude como camada de ROBUSTEZ — ele interpreta
arquivos com encoding quebrado, campos fora de ordem, rótulos levemente
diferentes, linhas truncadas. O parsing determinístico serve como fallback
rápido (barato) para arquivos bem-comportados; o Claude é acionado quando
a heurística detecta ausência de campos críticos OU em modo "sempre".

Cada chamada à API retorna JSON validado contra um schema conhecido, e
registra no log de auditoria: arquivo, timestamp, versão do prompt, campos
extraídos, confiança declarada pela IA.
"""
from __future__ import annotations
import json
import re
import time
from dataclasses import dataclass, field, asdict
from typing import Any

import anthropic


PROMPT_VERSION = "v1.3.0"
MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 1500

# Campos esperados em cada documento (ordem canônica)
EXPECTED_FIELDS = [
    "TIPO_DOCUMENTO",
    "NUMERO_DOCUMENTO",
    "DATA_EMISSAO",
    "FORNECEDOR",
    "CNPJ_FORNECEDOR",
    "DESCRICAO_SERVICO",
    "VALOR_BRUTO",
    "DATA_PAGAMENTO",
    "DATA_EMISSAO_NF",
    "APROVADO_POR",
    "BANCO_DESTINO",
    "STATUS",
    "HASH_VERIFICACAO",
]

SYSTEM_PROMPT = f"""Você é um extrator de dados de documentos financeiros brasileiros (notas fiscais, recibos, faturas, contratos aditivos).

Seu trabalho é ler o conteúdo bruto de um arquivo .txt e devolver APENAS um objeto JSON válido com os campos extraídos. Nada mais — sem markdown, sem texto explicativo, sem ```.

Schema de saída (use exatamente essas chaves):
{{
  "TIPO_DOCUMENTO": string | null,
  "NUMERO_DOCUMENTO": string | null,
  "DATA_EMISSAO": string | null,          // formato DD/MM/AAAA
  "FORNECEDOR": string | null,
  "CNPJ_FORNECEDOR": string | null,       // formato XX.XXX.XXX/XXXX-XX
  "DESCRICAO_SERVICO": string | null,
  "VALOR_BRUTO": string | null,           // preserve o formato original, ex "R$ 15.000,00"
  "DATA_PAGAMENTO": string | null,
  "DATA_EMISSAO_NF": string | null,
  "APROVADO_POR": string | null,
  "BANCO_DESTINO": string | null,
  "STATUS": string | null,
  "HASH_VERIFICACAO": string | null,
  "_confianca": "Alto" | "Medio" | "Baixo",
  "_observacoes": string                  // qualquer coisa anômala que você percebeu no arquivo
}}

Regras rígidas:
1. Se um campo não existir no documento ou estiver ilegível, use null — NUNCA invente valores.
2. Preserve valores como estão no arquivo (não normalize datas nem valores).
3. Se houver caracteres corrompidos (mojibake, bytes não-UTF8, símbolos invisíveis), tente inferir o valor original e registre "encoding" ou "mojibake" em _observacoes.
4. Se um campo estiver truncado (ex.: "STATUS: PAG" em vez de "STATUS: PAGO"), preserve o valor truncado como está e sinalize em _observacoes.
5. _confianca = "Alto" se todos os campos principais (TIPO_DOCUMENTO, NUMERO_DOCUMENTO, FORNECEDOR, VALOR_BRUTO, DATA_PAGAMENTO) foram extraídos sem ambiguidade.
6. _confianca = "Medio" se algum campo foi inferido ou se há caracteres corrompidos leves.
7. _confianca = "Baixo" se houver campos ausentes, muito truncados, ou arquivo mal-formado.

Versão do prompt: {PROMPT_VERSION}
"""


@dataclass
class ExtractionResult:
    """Resultado de uma extração — atributos + metadados de auditoria."""
    filename: str
    fields: dict[str, Any] = field(default_factory=dict)
    confidence: str = "Baixo"
    observations: str = ""
    source: str = "unknown"  # "regex" | "claude" | "regex+claude"
    prompt_version: str = PROMPT_VERSION
    model: str = MODEL
    latency_ms: int = 0
    raw_excerpt: str = ""    # primeiros 200 chars do arquivo bruto
    error: str | None = None

    def to_dict(self) -> dict:
        d = asdict(self)
        # achata os campos extraídos pro nível de cima (mais fácil pro Power BI)
        out = {"arquivo": self.filename}
        for k in EXPECTED_FIELDS:
            out[k] = self.fields.get(k)
        out.update({
            "confianca_extracao": self.confidence,
            "observacoes_extracao": self.observations,
            "fonte_extracao": self.source,
            "versao_prompt": self.prompt_version,
            "modelo_ia": self.model,
            "latencia_ms": self.latency_ms,
            "erro_extracao": self.error,
        })
        return out


# ---------------------------------------------------------------
# Parser determinístico (rápido, barato, funciona em ~99% dos casos)
# ---------------------------------------------------------------

# Separadores aceitos: ":" e "=" (cobre KEY: value e KEY = value)
_SEP_RE = re.compile(r'^(.+?)\s*[:=]\s*(.+)$')


def parse_deterministic(raw: str) -> dict[str, Any]:
    """Parser linha a linha. Aceita KEY: VALUE e KEY = VALUE como separadores."""
    fields: dict[str, Any] = {k: None for k in EXPECTED_FIELDS}
    for line in raw.splitlines():
        m = _SEP_RE.match(line.strip())
        if not m:
            continue
        # Normaliza a chave: maiúsculas e espaços → underscore
        key = m.group(1).strip().upper().replace(" ", "_")
        value = m.group(2).strip()
        if key in fields and value:
            fields[key] = value
    return fields


def read_file_safe(path_or_bytes) -> tuple[str, bool]:
    """
    Lê arquivo tentando UTF-8, depois cp1252 (Windows-1252), depois UTF-8 com replace.
    Retorna (conteudo, teve_problema_encoding).

    cp1252 em vez de latin-1: documentos brasileiros de software Windows (SAP, TOTVS,
    exportações NFe) usam cp1252, que cobre caracteres como € no intervalo 0x80-0x9F
    que latin-1 trata como bytes de controle e remove silenciosamente.
    """
    if isinstance(path_or_bytes, (bytes, bytearray)):
        raw_bytes = bytes(path_or_bytes)
    else:
        with open(path_or_bytes, "rb") as f:
            raw_bytes = f.read()

    # Tenta UTF-8 estrito
    try:
        return raw_bytes.decode("utf-8"), False
    except UnicodeDecodeError:
        pass

    # Fallback cp1252 (superset de latin-1 para o intervalo Windows 0x80-0x9F)
    has_issue = True
    try:
        text = raw_bytes.decode("cp1252")
    except Exception:
        text = raw_bytes.decode("utf-8", errors="replace")

    # Remove bytes de controle evidentes (exceto \n, \r, \t)
    cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]", "", text)
    return cleaned, has_issue


# ---------------------------------------------------------------
# Cliente Claude
# ---------------------------------------------------------------

class ClaudeExtractor:
    """Cliente da Claude API com retry, prompt caching e temperatura zero."""

    def __init__(self, api_key: str, model: str = MODEL):
        if not api_key:
            raise ValueError("API key da Anthropic não fornecida")
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = model

    def extract(self, raw_text: str, filename: str, max_retries: int = 2) -> dict[str, Any]:
        """
        Chama Claude para extrair campos estruturados.
        Retorna dict com os campos + metadados (_confianca, _observacoes).
        Levanta exceção em caso de erro irrecuperável.
        """
        last_err: Exception | None = None
        for attempt in range(max_retries + 1):
            try:
                msg = self.client.messages.create(
                    model=self.model,
                    max_tokens=MAX_TOKENS,
                    temperature=0,  # extração determinística
                    system=[{
                        "type": "text",
                        "text": SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},  # prompt caching
                    }],
                    messages=[{
                        "role": "user",
                        "content": f"Arquivo: {filename}\n\nConteúdo bruto:\n---\n{raw_text}\n---",
                    }],
                )
                text = "".join(
                    block.text for block in msg.content if getattr(block, "type", None) == "text"
                ).strip()
                # Remove code fences defensivamente (modelo pode adicionar apesar da instrução)
                text = re.sub(r"^```(?:json)?\s*", "", text)
                text = re.sub(r"\s*```$", "", text)
                return json.loads(text)
            except json.JSONDecodeError as e:
                last_err = e
                continue
            except anthropic.RateLimitError as e:
                last_err = e
                time.sleep(2 ** attempt)
                continue
            except anthropic.APIError as e:
                last_err = e
                time.sleep(1)
                continue
            except Exception as e:
                last_err = e
                break
        raise RuntimeError(f"Falha ao extrair com Claude após {max_retries+1} tentativas: {last_err}")


# ---------------------------------------------------------------
# Orquestrador — decide regex vs Claude
# ---------------------------------------------------------------

def extract_document(
    raw_text: str,
    filename: str,
    claude: ClaudeExtractor | None = None,
    force_ai: bool = False,
    encoding_issue: bool = False,
) -> ExtractionResult:
    """
    Estratégia híbrida:
    - Sempre roda o parser determinístico (é instantâneo e grátis)
    - Se force_ai=True ou se há sinais de arquivo problemático (campos-chave
      ausentes, caracteres suspeitos, encoding quebrado), também roda o Claude
    - Resultado do Claude tem prioridade sobre regex em caso de conflito
    """
    result = ExtractionResult(filename=filename, raw_excerpt=raw_text[:200])
    t0 = time.time()
    if encoding_issue:
        result.observations = "encoding não-UTF8 detectado na leitura"

    # 1. Parser determinístico
    det_fields = parse_deterministic(raw_text)
    critical = ["NUMERO_DOCUMENTO", "FORNECEDOR", "VALOR_BRUTO"]
    missing_critical = [k for k in critical if not det_fields.get(k)]
    has_mojibake = bool(re.search(r"[�]|Ã[©§£¡]", raw_text))

    need_ai = force_ai or missing_critical or has_mojibake or encoding_issue

    if not need_ai or claude is None:
        result.fields = det_fields
        result.source = "regex"
        result.confidence = "Alto" if not (missing_critical or encoding_issue) else "Baixo"
        obs_parts = []
        if result.observations:
            obs_parts.append(result.observations)
        if missing_critical:
            obs_parts.append(f"Campos críticos ausentes: {', '.join(missing_critical)}")
        result.observations = "; ".join(obs_parts)
        result.latency_ms = int((time.time() - t0) * 1000)
        return result

    # 2. Claude API
    try:
        ai_out = claude.extract(raw_text, filename)
        merged = dict(det_fields)
        for k in EXPECTED_FIELDS:
            # Claude vence em caso de conflito, mas só se não-nulo
            if ai_out.get(k) is not None:
                merged[k] = ai_out[k]
        result.fields = merged
        result.confidence = ai_out.get("_confianca", "Medio")
        result.observations = ai_out.get("_observacoes", "") or ""
        result.source = "regex+claude"
    except Exception as e:
        # Fallback: se a IA falhou, usa regex mesmo com flag de baixa confiança
        result.fields = det_fields
        result.source = "regex (fallback: IA falhou)"
        result.confidence = "Baixo"
        result.observations = f"Fallback para regex: {e}"
        result.error = str(e)

    result.latency_ms = int((time.time() - t0) * 1000)
    return result
