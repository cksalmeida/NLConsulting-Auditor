"""
audit_log.py
============
Log de auditoria rastreável. Cada ação do pipeline (leitura, extração IA,
detecção de regra) é registrada com timestamp, arquivo, e detalhes.

O log é exportável separadamente do resultado principal — é o artefato que
permite a um auditor entender "por que esta anomalia foi sinalizada" e
reproduzir a análise.
"""
from __future__ import annotations
import csv
import io
import json
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Iterable


@dataclass
class LogEntry:
    timestamp: str           # ISO 8601
    arquivo: str
    evento: str              # READ | EXTRACT | DETECT | ERROR | EXPORT
    detalhe: str
    regra: str = ""          # código da regra, quando aplicável
    campo_evidencia: str = ""
    confianca: str = ""
    versao_prompt: str = ""
    modelo_ia: str = ""
    latencia_ms: int = 0

    @classmethod
    def now(cls, **kwargs) -> "LogEntry":
        return cls(timestamp=datetime.utcnow().isoformat(timespec="seconds") + "Z", **kwargs)


class AuditLog:
    """Log append-only em memória, exportável para CSV e JSONL."""

    def __init__(self):
        self.entries: list[LogEntry] = []

    def log(self, **kwargs) -> None:
        self.entries.append(LogEntry.now(**kwargs))

    # Eventos de alto nível — açúcar sintático
    def log_read(self, arquivo: str, tamanho: int, encoding_ok: bool) -> None:
        self.log(
            arquivo=arquivo,
            evento="READ",
            detalhe=f"Lido {tamanho} bytes; encoding_ok={encoding_ok}",
        )

    def log_extract(self, arquivo: str, fonte: str, confianca: str,
                    versao_prompt: str, modelo: str, latency_ms: int,
                    observacoes: str = "") -> None:
        self.log(
            arquivo=arquivo,
            evento="EXTRACT",
            detalhe=f"Fonte={fonte}; obs={observacoes[:200]}",
            confianca=confianca,
            versao_prompt=versao_prompt,
            modelo_ia=modelo,
            latencia_ms=latency_ms,
        )

    def log_detect(self, arquivo: str, regra: str, descricao: str,
                   campo_evidencia: str, confianca: str) -> None:
        self.log(
            arquivo=arquivo,
            evento="DETECT",
            detalhe=descricao,
            regra=regra,
            campo_evidencia=campo_evidencia,
            confianca=confianca,
        )

    def log_error(self, arquivo: str, erro: str) -> None:
        self.log(arquivo=arquivo, evento="ERROR", detalhe=erro[:500])

    def log_export(self, tipo: str, quantidade: int) -> None:
        self.log(
            arquivo="-",
            evento="EXPORT",
            detalhe=f"Preparado para download: {quantidade} registros como {tipo}",
        )

    def log_summary(
        self,
        total_docs: int,
        total_anomalias: int,
        anomalias_por_regra: dict[str, int],
        erros: int,
        modelo: str,
        versao_prompt: str,
        duracao_seg: float,
    ) -> None:
        """Entrada de fechamento da sessão — visão consolidada de toda a execução."""
        regras_str = "; ".join(f"{k}={v}" for k, v in sorted(anomalias_por_regra.items()))
        self.log(
            arquivo="-",
            evento="SUMMARY",
            detalhe=(
                f"docs={total_docs}; anomalias={total_anomalias}; erros={erros}; "
                f"duracao={duracao_seg:.1f}s; modelo={modelo}; prompt={versao_prompt}; "
                f"regras=[{regras_str}]"
            ),
            modelo_ia=modelo,
            versao_prompt=versao_prompt,
        )

    def to_csv_bytes(self) -> bytes:
        buf = io.StringIO()
        if not self.entries:
            return b""
        fieldnames = list(asdict(self.entries[0]).keys())
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        for e in self.entries:
            writer.writerow(asdict(e))
        return buf.getvalue().encode("utf-8-sig")  # BOM pro Excel abrir certo

    def to_jsonl_bytes(self) -> bytes:
        lines = [json.dumps(asdict(e), ensure_ascii=False) for e in self.entries]
        return ("\n".join(lines) + "\n").encode("utf-8")

    def __len__(self) -> int:
        return len(self.entries)
