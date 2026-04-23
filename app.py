"""
app.py — Auditor de Documentos com IA (Streamlit)
================================================
NLConsulting · Processo Seletivo 2026 · Tarefa de Casa

Interface web para:
  1. Receber upload de .zip ou múltiplos .txt
  2. Extrair campos via Claude API (com fallback determinístico)
  3. Detectar anomalias
  4. Exibir tabela de resultados com flags
  5. Exportar CSV/Excel (base para Power BI)
  6. Exportar log de auditoria

Executar localmente:
  streamlit run app.py

Requer variável de ambiente ANTHROPIC_API_KEY (ou configurada via
.streamlit/secrets.toml em ambiente de deploy).
"""
from __future__ import annotations

from dataclasses import asdict
import io
import os
import time
import zipfile
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()  # carrega .env antes de qualquer leitura de os.environ

import altair as alt
import pandas as pd
import streamlit as st

from extractor import (
    ClaudeExtractor, ExtractionResult, extract_document, read_file_safe,
    EXPECTED_FIELDS, PROMPT_VERSION, MODEL,
)
from anomaly_detector import run_all_detectors
from audit_log import AuditLog


def _log_entry_to_dict(e):
    return asdict(e)


# Glossário das regras de detecção — exibido como legenda na UI
REGRAS_DESCRICAO = {
    "NF_DUPLICADA":             ("🔴 Alta",  "Mesmo número de NF e fornecedor em mais de um arquivo"),
    "CNPJ_DIVERGENTE":          ("🔴 Alta",  "CNPJ diferente do padrão histórico daquele fornecedor"),
    "FORNECEDOR_SEM_HISTORICO": ("🔴 Alta",  "Fornecedor com poucas ocorrências no lote (possível empresa fantasma)"),
    "NF_APOS_PAGAMENTO":        ("🔴 Alta",  "Nota fiscal emitida após a data de pagamento"),
    "APROVADOR_DESCONHECIDO":   ("🟠 Média", "Aprovador não consta na lista de aprovadores recorrentes"),
    "VALOR_FORA_FAIXA":         ("🟠 Média", "Valor com Z-score > 3 em relação à média histórica do fornecedor"),
    "STATUS_INVALIDO":          ("🟠 Média", "STATUS fora do vocabulário válido (PAGO, CANCELADO, ESTORNADO, PENDENTE)"),
    "CAMPO_AUSENTE":            ("🟠 Média", "Campos críticos ausentes: NUMERO_DOCUMENTO, FORNECEDOR, VALOR_BRUTO ou HASH"),
    "ENCODING_INVALIDO":        ("🟠 Média", "Arquivo com bytes corrompidos ou encoding não-padrão"),
    "STATUS_INCONSISTENTE":     ("🟡 Baixa", "PENDENTE com data de pagamento preenchida (contradição lógica)"),
}


# =================================================================
# Configuração da página
# =================================================================

st.set_page_config(
    page_title="Auditor de Documentos com IA · NLConsulting",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)


# =================================================================
# CSS customizado (identidade visual)
# =================================================================

st.markdown("""
<style>
  .main > div { padding-top: 1.5rem; }
  h1 { color: #1e3a5f; font-weight: 700; }
  .metric-card {
    background: #f7f9fc;
    border-left: 4px solid #1e3a5f;
    padding: 1rem 1.25rem;
    border-radius: 4px;
    margin-bottom: 0.5rem;
  }
  .anomaly-alta { color: #c92a2a; font-weight: 600; }
  .anomaly-media { color: #e67700; font-weight: 600; }
  .anomaly-baixa { color: #1864ab; }
  .stDownloadButton button { background: #1e3a5f; color: white; }
  footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# =================================================================
# Sidebar — configuração
# =================================================================

with st.sidebar:
    st.markdown("### ⚙️ Configurações")

    # API key: secrets (deploy) → env (local) → input manual
    # Se a chave já está configurada via ambiente, oculta o campo para não expô-la na UI
    api_key = ""
    key_from_env = False
    try:
        api_key = st.secrets.get("ANTHROPIC_API_KEY", "") or ""
        if api_key:
            key_from_env = True
    except Exception:
        pass
    if not api_key:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if api_key:
            key_from_env = True

    if key_from_env:
        st.success("🔑 Chave configurada via ambiente")
    else:
        api_key = st.text_input(
            "Anthropic API Key",
            type="password",
            help="Sua chave da Claude API. Pode ser configurada via st.secrets ou variável de ambiente.",
        )

    st.markdown("---")
    st.markdown("### 🧪 Modo de extração")
    ai_mode = st.radio(
        "Quando acionar a IA?",
        options=[
            "Híbrido (recomendado)",
            "Sempre IA",
            "Apenas regex (sem IA)",
        ],
        index=0,
        help=(
            "**Híbrido**: regex primeiro; Claude só nos arquivos problemáticos. "
            "**Sempre IA**: Claude em todos. **Apenas regex**: sem IA (pra testar rápido)."
        ),
    )

    sample_limit = st.number_input(
        "Limite de arquivos por execução",
        min_value=1, max_value=5000, value=1000, step=50,
        help="Para testes rápidos, limite a quantidade processada.",
    )

    st.markdown("---")
    st.caption(f"**Modelo IA**: `{MODEL}`")
    st.caption(f"**Versão do prompt**: `{PROMPT_VERSION}`")


# =================================================================
# Estado da sessão
# =================================================================

for key, default in [
    ("processed", False),
    ("df_docs", None),
    ("df_anomalies", None),
    ("audit_log", None),
    ("stats", {}),
    ("excel_bytes", None),
    ("jsonl_bytes", None),
]:
    if key not in st.session_state:
        st.session_state[key] = default


def _reset_session():
    """Limpa todos os resultados e filtros para uma nova análise."""
    for key in ["processed", "df_docs", "df_anomalies", "audit_log", "excel_bytes", "jsonl_bytes"]:
        st.session_state[key] = False if key == "processed" else None
    st.session_state["stats"] = {}
    # Reseta chaves de filtros persistentes
    for fkey in ["filtro_so_anomalias", "filtro_fornecedor", "filtro_gravidade", "filtro_regra"]:
        if fkey in st.session_state:
            del st.session_state[fkey]


# =================================================================
# Header
# =================================================================

st.title("Auditor de Documentos com IA")
st.caption(
    "Leia um lote de notas fiscais, recibos e faturas · Extraia campos com "
    "Claude · Detecte anomalias · Exporte para Power BI"
)


# =================================================================
# Upload
# =================================================================

st.markdown("### Upload dos arquivos")
uploaded = st.file_uploader(
    "Envie um arquivo .zip ou múltiplos .txt",
    type=["zip", "txt"],
    accept_multiple_files=True,
    help="Tamanho máximo recomendado: 50 MB. Arquivos suspeitos são sinalizados, não rejeitados.",
)


def _collect_files(uploaded_files) -> list[tuple[str, bytes]]:
    """Retorna lista de (filename, bytes) tanto pra zip quanto pra txts diretos."""
    out: list[tuple[str, bytes]] = []
    for f in uploaded_files:
        f.seek(0)  # garante leitura completa mesmo se o arquivo já foi lido antes
        if f.name.lower().endswith(".zip"):
            try:
                with zipfile.ZipFile(io.BytesIO(f.read())) as zf:
                    for name in zf.namelist():
                        if name.lower().endswith(".txt") and not name.startswith("__MACOSX"):
                            out.append((os.path.basename(name), zf.read(name)))
            except zipfile.BadZipFile:
                st.error(f"Arquivo '{f.name}' não é um .zip válido.")
        elif f.name.lower().endswith(".txt"):
            out.append((f.name, f.read()))
    return out


# Preview: conta arquivos antes de processar para dar feedback imediato ao usuário
if uploaded:
    preview = _collect_files(uploaded)
    n_preview = len(preview)
    if n_preview > 0:
        st.caption(f"📁 **{n_preview}** arquivo(s) .txt encontrado(s) e pronto(s) para processamento.")
    else:
        st.warning("Nenhum .txt encontrado nos arquivos enviados.")

btn_col, reset_col = st.columns([3, 1])
with btn_col:
    process_btn = st.button("▶️  Processar arquivos", type="primary", disabled=not uploaded)
with reset_col:
    if st.session_state.processed:
        if st.button("🗑️ Nova análise", use_container_width=True):
            _reset_session()
            st.rerun()


# =================================================================
# Pipeline de processamento
# =================================================================

def run_pipeline(files: list[tuple[str, bytes]], api_key: str, ai_mode: str, limit: int):
    """Extrai + detecta + popula session_state."""
    t_inicio = time.time()
    log = AuditLog()

    claude = None
    use_ai = ai_mode != "Apenas regex (sem IA)"
    if use_ai:
        if not api_key:
            st.error("⚠️ API key da Anthropic é obrigatória no modo IA. "
                     "Configure na sidebar ou mude para 'Apenas regex'.")
            return
        try:
            claude = ClaudeExtractor(api_key=api_key)
        except Exception as e:
            st.error(f"Falha ao inicializar cliente Claude: {e}")
            return

    files = files[:limit]
    total = len(files)
    force_ai = ai_mode == "Sempre IA"

    progress = st.progress(0.0, text=f"Processando 0 de {total}...")
    status_box = st.empty()

    extraction_results: list[ExtractionResult] = []
    errors_count = 0

    # Atualiza a barra ~20 vezes independente do tamanho do lote
    update_interval = max(1, total // 20)

    for i, (fname, raw_bytes) in enumerate(files):
        try:
            raw_text, had_encoding_issue = read_file_safe(raw_bytes)
            log.log_read(fname, len(raw_bytes), not had_encoding_issue)

            result = extract_document(
                raw_text=raw_text,
                filename=fname,
                claude=claude if use_ai else None,
                force_ai=force_ai,
                encoding_issue=had_encoding_issue,
            )
            if had_encoding_issue and "encoding" not in (result.observations or "").lower():
                result.observations = (result.observations + "; encoding não-UTF8 detectado").strip("; ")

            extraction_results.append(result)
            log.log_extract(
                arquivo=fname,
                fonte=result.source,
                confianca=result.confidence,
                versao_prompt=result.prompt_version,
                modelo=result.model if "claude" in result.source else "-",
                latency_ms=result.latency_ms,
                observacoes=result.observations,
            )
            if result.error:
                log.log_error(fname, result.error)
        except Exception as e:
            errors_count += 1
            log.log_error(fname, str(e))
            extraction_results.append(ExtractionResult(
                filename=fname, error=str(e), confidence="Baixo",
                observations=f"Erro no pipeline: {e}",
            ))

        if (i + 1) % update_interval == 0 or (i + 1) == total:
            pct = (i + 1) / total
            progress.progress(pct, text=f"Processando {i+1} de {total}...")

    status_box.info("🔎 Detectando anomalias...")

    rows = [r.to_dict() for r in extraction_results]
    df_docs = pd.DataFrame(rows)

    anomalies, baseline = run_all_detectors(rows, extraction_results)
    for a in anomalies:
        log.log_detect(
            arquivo=a.arquivo, regra=a.regra, descricao=a.descricao,
            campo_evidencia=a.campos_evidencia, confianca=a.confianca,
        )

    df_anom = pd.DataFrame([a.to_dict() for a in anomalies]) if anomalies else pd.DataFrame(
        columns=["arquivo", "regra", "descricao", "campos_evidencia",
                 "valor_evidencia", "gravidade", "confianca"]
    )

    if not df_anom.empty:
        anom_por_arquivo = df_anom.groupby("arquivo").agg(
            total_anomalias=("regra", "count"),
            regras_disparadas=("regra", lambda s: "; ".join(sorted(set(s)))),
            gravidade_maxima=("gravidade", lambda s: _max_grav(s)),
        ).reset_index()
        df_docs = df_docs.merge(anom_por_arquivo, on="arquivo", how="left")
        df_docs["total_anomalias"] = df_docs["total_anomalias"].fillna(0).astype(int)
        df_docs["regras_disparadas"] = df_docs["regras_disparadas"].fillna("")
        df_docs["gravidade_maxima"] = df_docs["gravidade_maxima"].fillna("")
    else:
        df_docs["total_anomalias"] = 0
        df_docs["regras_disparadas"] = ""
        df_docs["gravidade_maxima"] = ""

    # Sumário da sessão — deve ser o último evento antes dos exports
    duracao = time.time() - t_inicio
    anomalias_por_regra: dict[str, int] = {}
    if not df_anom.empty:
        anomalias_por_regra = df_anom["regra"].value_counts().to_dict()
    modelo_usado = MODEL if ai_mode != "Apenas regex (sem IA)" else "regex-only"
    log.log_summary(
        total_docs=total,
        total_anomalias=len(anomalies),
        anomalias_por_regra=anomalias_por_regra,
        erros=errors_count,
        modelo=modelo_usado,
        versao_prompt=PROMPT_VERSION,
        duracao_seg=duracao,
    )

    # Registra todos os formatos disponíveis para download
    log.log_export("xlsx", len(df_docs))
    log.log_export("csv_documentos", len(df_docs))
    log.log_export("csv_anomalias", len(df_anom))
    log.log_export("csv_log", len(log.entries))
    log.log_export("jsonl_log", len(log.entries))

    # Gera Excel e JSONL uma única vez — evita re-geração a cada rerender da UI
    excel_buf = io.BytesIO()
    with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
        df_docs.to_excel(writer, sheet_name="documentos", index=False)
        if not df_anom.empty:
            df_anom.to_excel(writer, sheet_name="anomalias", index=False)
        log_df = pd.DataFrame([_log_entry_to_dict(e) for e in log.entries])
        log_df.to_excel(writer, sheet_name="audit_log", index=False)

    st.session_state.df_docs = df_docs
    st.session_state.df_anomalies = df_anom
    st.session_state.audit_log = log
    st.session_state.excel_bytes = excel_buf.getvalue()
    st.session_state.jsonl_bytes = log.to_jsonl_bytes()
    st.session_state.stats = {
        "total_arquivos": total,
        "total_anomalias": len(anomalies),
        "erros": errors_count,
        "arquivos_com_anomalia": int((df_docs["total_anomalias"] > 0).sum()),
        "fornecedores_conhecidos": len(baseline.fornecedores_conhecidos),
        "aprovadores_conhecidos": len(baseline.aprovadores_conhecidos),
    }
    st.session_state.processed = True
    progress.empty()
    status_box.success(f"✅ Concluído: {total} arquivos · {len(anomalies)} anomalias · {errors_count} erros")


def _max_grav(series) -> str:
    order = {"Alto": 3, "Medio": 2, "Baixo": 1, "": 0}
    return max(series, key=lambda s: order.get(s, 0))


if process_btn and uploaded:
    files = _collect_files(uploaded)
    if not files:
        st.warning("Nenhum .txt encontrado nos arquivos enviados.")
    else:
        with st.spinner("Iniciando pipeline..."):
            run_pipeline(files, api_key, ai_mode, int(sample_limit))


# =================================================================
# Resultados (só aparecem após processar)
# =================================================================

if st.session_state.processed:
    stats = st.session_state.stats
    df_docs: pd.DataFrame = st.session_state.df_docs
    df_anom: pd.DataFrame = st.session_state.df_anomalies
    log: AuditLog = st.session_state.audit_log

    st.markdown("---")
    st.markdown("### Resumo")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total arquivos", stats["total_arquivos"])
    c2.metric("Total anomalias", stats["total_anomalias"])
    c3.metric("Arquivos com ≥1 anomalia", stats["arquivos_com_anomalia"])
    c4.metric("Erros de processamento", stats["erros"])
    c5.metric("Linhas no log", len(log))

    # -----------------------------------------------------------
    # Gráficos com Altair (cores semânticas + ordenação)
    # -----------------------------------------------------------
    if not df_anom.empty:
        col_a, col_b = st.columns(2)

        with col_a:
            st.markdown("**Anomalias por regra**")
            regra_df = df_anom["regra"].value_counts().reset_index()
            regra_df.columns = ["regra", "total"]
            chart_regra = (
                alt.Chart(regra_df)
                .mark_bar(color="#1e3a5f")
                .encode(
                    x=alt.X("total:Q", title="Ocorrências"),
                    y=alt.Y("regra:N", sort="-x", title=None),
                    tooltip=[
                        alt.Tooltip("regra:N", title="Regra"),
                        alt.Tooltip("total:Q", title="Ocorrências"),
                    ],
                )
                .properties(height=alt.Step(30))
            )
            st.altair_chart(chart_regra, use_container_width=True)

        with col_b:
            st.markdown("**Anomalias por gravidade**")
            grav_df = df_anom["gravidade"].value_counts().reset_index()
            grav_df.columns = ["gravidade", "total"]
            color_scale = alt.Scale(
                domain=["Alto", "Medio", "Baixo"],
                range=["#c92a2a", "#e67700", "#1864ab"],
            )
            chart_grav = (
                alt.Chart(grav_df)
                .mark_bar()
                .encode(
                    x=alt.X("gravidade:N", sort=["Alto", "Medio", "Baixo"],
                            title=None, axis=alt.Axis(labelAngle=0)),
                    y=alt.Y("total:Q", title="Ocorrências"),
                    color=alt.Color("gravidade:N", scale=color_scale, legend=None),
                    tooltip=[
                        alt.Tooltip("gravidade:N", title="Gravidade"),
                        alt.Tooltip("total:Q", title="Ocorrências"),
                    ],
                )
                .properties(height=200)
            )
            st.altair_chart(chart_grav, use_container_width=True)

    # -----------------------------------------------------------
    # Tabela principal (arquivos)
    # -----------------------------------------------------------
    st.markdown("### Resultados por arquivo")

    filtro_col1, filtro_col2, filtro_col3 = st.columns(3)
    with filtro_col1:
        so_anomalias = st.checkbox(
            "Mostrar apenas arquivos com anomalia",
            value=False,
            key="filtro_so_anomalias",
        )
    with filtro_col2:
        forn_opts = ["(todos)"] + sorted(df_docs["FORNECEDOR"].dropna().unique().tolist())
        filtro_forn = st.selectbox("Fornecedor", forn_opts, key="filtro_fornecedor")
    with filtro_col3:
        grav_opts = ["(todas)", "Alto", "Medio", "Baixo"]
        filtro_grav = st.selectbox("Gravidade máxima", grav_opts, key="filtro_gravidade")

    view = df_docs.copy()
    if so_anomalias:
        view = view[view["total_anomalias"] > 0]
    if filtro_forn != "(todos)":
        view = view[view["FORNECEDOR"] == filtro_forn]
    if filtro_grav != "(todas)":
        view = view[view["gravidade_maxima"] == filtro_grav]

    st.dataframe(
        view,
        use_container_width=True,
        height=400,
        column_config={
            "arquivo": st.column_config.TextColumn("Arquivo", width="small"),
            "total_anomalias": st.column_config.NumberColumn("Flags", width="small"),
            "regras_disparadas": st.column_config.TextColumn("Regras", width="medium"),
        },
    )

    # -----------------------------------------------------------
    # Tabela detalhada de anomalias
    # -----------------------------------------------------------
    st.markdown("### Anomalias detalhadas")
    if df_anom.empty:
        st.info("Nenhuma anomalia detectada.")
    else:
        regra_opts = ["(todas)"] + sorted(df_anom["regra"].unique().tolist())
        filtro_regra = st.selectbox("Filtrar por regra", regra_opts, key="filtro_regra")
        view_a = df_anom if filtro_regra == "(todas)" else df_anom[df_anom["regra"] == filtro_regra]
        st.dataframe(view_a, use_container_width=True, height=350)

        with st.expander("📖 Legenda das regras de detecção"):
            for codigo, (grav, desc) in REGRAS_DESCRICAO.items():
                st.markdown(f"**`{codigo}`** {grav} — {desc}")

    # -----------------------------------------------------------
    # Exportações
    # -----------------------------------------------------------
    st.markdown("### Exportar resultados")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    csv_docs = df_docs.to_csv(index=False).encode("utf-8-sig")
    csv_anom = df_anom.to_csv(index=False).encode("utf-8-sig") if not df_anom.empty else b""

    col_d1, col_d2, col_d3 = st.columns(3)
    with col_d1:
        st.download_button(
            "📄 CSV — Documentos",
            data=csv_docs,
            file_name=f"auditor_documentos_{ts}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with col_d2:
        st.download_button(
            "🚩 CSV — Anomalias",
            data=csv_anom,
            file_name=f"auditor_anomalias_{ts}.csv",
            mime="text/csv",
            disabled=df_anom.empty,
            use_container_width=True,
        )
    with col_d3:
        st.download_button(
            "📊 Excel — Todas as abas",
            data=st.session_state.excel_bytes,
            file_name=f"auditor_completo_{ts}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            help="Documentos + Anomalias + Log numa única planilha. Use esta como fonte do Power BI.",
        )

    col_d4, col_d5, _ = st.columns(3)
    with col_d4:
        st.download_button(
            "📋 Log de auditoria (CSV)",
            data=log.to_csv_bytes(),
            file_name=f"auditor_log_{ts}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with col_d5:
        st.download_button(
            "🗂️ Log de auditoria (JSONL)",
            data=st.session_state.jsonl_bytes,
            file_name=f"auditor_log_{ts}.jsonl",
            mime="application/jsonl",
            use_container_width=True,
            help="Formato JSONL — uma linha JSON por evento. Ideal para ingestão em Splunk, Datadog, Elastic.",
        )

    st.markdown("---")
    with st.expander("ℹ️ Próximo passo: dashboard Power BI"):
        st.markdown("""
        1. Baixe o Excel acima (**auditor_completo_*.xlsx**).
        2. Abra o Power BI Desktop → **Obter Dados** → **Excel** → selecione o arquivo.
        3. Importe as 3 abas: `documentos`, `anomalias`, `audit_log`.
        4. Siga o guia em `powerbi/INSTRUCOES_POWERBI.md` no repositório.
        """)


# =================================================================
# Empty state
# =================================================================

if not st.session_state.processed and not uploaded:
    st.info("👈 Envie um `.zip` com os .txt (ou selecione múltiplos .txt diretamente) e clique em **Processar**.")
    with st.expander("📖 O que este app faz"):
        st.markdown("""
        - **Lê** documentos financeiros em texto (notas fiscais, recibos, faturas, contratos aditivos).
        - **Extrai** os campos usando Claude (Anthropic) com fallback determinístico por regex.
        - **Detecta** 10 tipos de anomalia: NF duplicada, CNPJ divergente,
          fornecedor sem histórico, NF emitida após pagamento, aprovador desconhecido,
          valor fora da faixa, STATUS inválido, STATUS inconsistente, campo ausente, encoding inválido.
        - **Exporta** CSV e Excel prontos para Power BI, com log de auditoria rastreável.

        **Rastreabilidade**: cada linha do log registra arquivo, timestamp, regra disparada,
        campo-evidência, grau de confiança, versão do prompt e modelo usado.
        """)
