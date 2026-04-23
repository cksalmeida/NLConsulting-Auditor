# 🔍 Auditor de Documentos com IA

**NLConsulting · Processo Seletivo 2026**
Estagiário Full Stack Developer (SaaS & AI)

Aplicação web que recebe um lote de documentos financeiros em `.txt`
(notas fiscais, recibos, faturas, contratos aditivos), extrai os campos
via **Claude API (Anthropic)**, detecta **10 tipos de anomalia** com
regras determinísticas auditáveis, e exporta resultados prontos para
consumo no **Power BI**.

---

## 🚀 Links de entrega

| Item | URL |
| --- | --- |
| Aplicação online | *(preencher após deploy no Streamlit Cloud)* |
| Repositório GitHub | *https://github.com/cksalmeida/NLConsulting-Auditor* |
| Dashboard Power BI | *(preencher após publicação)* |
| Relatório de anomalias | Ver seção "Relatório de anomalias detectadas" neste README |

---

## ⚡ Visão geral

```
┌────────────┐    ┌──────────────┐    ┌───────────────┐    ┌───────────┐
│   Upload   │───▶│  Extração    │───▶│   Detecção    │───▶│  Export   │
│ .zip/.txt  │    │ regex+Claude │    │ 10 regras     │    │ CSV/Excel │
└────────────┘    └──────────────┘    └───────────────┘    └─────┬─────┘
                          │                    │                 │
                          ▼                    ▼                 ▼
                  ┌────────────────────────────────────────────────┐
                  │       Log de auditoria (CSV exportável)        │
                  └────────────────────────────────────────────────┘
                                                                  │
                                                                  ▼
                                                         ┌──────────────┐
                                                         │  Power BI    │
                                                         │  dashboard   │
                                                         └──────────────┘
```

**Stack:** Streamlit (frontend + orquestração) · Python 3.11+ ·
Anthropic Claude API (`claude-sonnet-4-6`) · pandas · openpyxl ·
Power BI Desktop (dashboard offline publicável).

---

## 📁 Estrutura do projeto

```
auditor-ia/
├── app.py                        # Streamlit: UI + pipeline
├── extractor.py                  # Claude API + parser determinístico
├── anomaly_detector.py           # 10 regras de detecção
├── audit_log.py                  # Log rastreável (CSV + JSONL)
├── requirements.txt
├── .streamlit/
│   ├── config.toml               # Tema + configurações do servidor
│   └── secrets.toml.example      # Modelo — nunca commitar o real
├── .env.example                  # Modelo de variáveis de ambiente
├── .gitignore
├── tests/
│   ├── test_anomaly_detector.py  # 23 testes unitários
│   ├── test_extractor.py         # 18 testes unitários
│   └── test_audit_log.py         # 12 testes unitários
└── README.md                     # Este arquivo
```

---

## 🛠️ Como rodar localmente

### 1. Clone e instale dependências

```bash
git clone https://github.com/cksalmeida/NLConsulting-Auditor.git
cd NLConsulting-Auditor
python -m venv .venv
source .venv/bin/activate          # Linux/Mac
# .venv\Scripts\activate           # Windows

pip install -r requirements.txt
```

### 2. Configure a chave da Anthropic

**Opção A — variável de ambiente (recomendado p/ dev local):**

```bash
cp .env.example .env
# Edite .env e coloque sua chave real
export $(cat .env | xargs)         # Linux/Mac
```

**Opção B — arquivo de secrets do Streamlit:**

```bash
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Edite .streamlit/secrets.toml
```

**Opção C — digitar direto na sidebar do app** (útil para demos).

### 3. Execute

```bash
streamlit run app.py
```

Acesse em `http://localhost:8501`.

### 4. Rodar os testes

```bash
python tests/test_anomaly_detector.py   # 23 ✅
python tests/test_extractor.py          # 18 ✅
python tests/test_audit_log.py          # 12 ✅
```

---

## ☁️ Deploy no Streamlit Community Cloud (gratuito)

1. **Suba o código pra um repositório público no GitHub** (sem a chave!).
2. Vá em [share.streamlit.io](https://share.streamlit.io) e conecte sua conta GitHub.
3. Clique em **New app** → escolha o repo → aponte para `app.py`.
4. Na tela de configuração, clique em **Advanced settings → Secrets** e cole:
   ```toml
   ANTHROPIC_API_KEY = "sk-ant-api03-..."
   ```
5. **Deploy**. Em ~2 minutos você tem a URL pública.

> **Segurança:** a chave fica apenas nos secrets do Streamlit Cloud, nunca
> no código. O frontend nunca a vê — todas as chamadas à Claude API saem
> do backend Python.

---

## 🧠 Decisões técnicas

### Por que Streamlit?

- **Velocidade de desenvolvimento**: a tarefa pede ferramenta funcional em
  7 dias; Streamlit elimina toda a camada de frontend/backend separada.
- **Publicação gratuita**: deploy em minutos via Streamlit Community Cloud
  com secrets nativos.
- **Audiência do briefing**: o app é uma ferramenta interna de auditoria,
  não um produto SaaS com milhares de usuários simultâneos — Streamlit
  atende perfeitamente.

### Por que extração híbrida (regex + Claude) e não só IA?

| Modo | Custo/arquivo | Latência | Robustez |
|------|---------------|----------|----------|
| Só regex | ~0 | ~1ms | Quebra em arquivos mal-formados |
| Só Claude | alto | ~800ms | Tolera qualquer formato |
| **Híbrido** | **baixo** | **~1ms (regex) + ~800ms só nos ~1% problemáticos** | **Alta** |

A abordagem híbrida acelera o pipeline em ~100× em arquivos bem-comportados
e ainda mantém robustez nos casos difíceis (encoding quebrado, campos
truncados, mojibake). Também reduz o custo da API em ~99%.

### Por que separar extração de detecção?

- **Extração (IA)** é probabilística por natureza — usamos Claude aqui
  porque ele lida com variações de formato.
- **Detecção (regras)** é determinística e auditável — um auditor humano
  precisa conseguir explicar exatamente *por que* uma NF foi sinalizada.
  Por isso as 10 regras são código Python com evidência explícita (campos
  que dispararam, valores observados, gravidade, confiança).

### Por que prompt versionado?

Cada linha do log de auditoria carrega `versao_prompt` (atualmente `v1.3.0`).
Se mudarmos o prompt e a taxa de detecção oscilar, conseguimos rastrear a
regressão. É auditabilidade de ponta a ponta.

### Tratamento de erros da Claude API

Em `extractor.py::ClaudeExtractor.extract`:

- **Rate limit** (`anthropic.RateLimitError`): backoff exponencial (2⁰, 2¹, 2² segundos), até 3 tentativas.
- **JSON malformado**: Claude às vezes devolve markdown; removemos ` ```json` defensivamente e retry.
- **Timeout / erro de API**: retry com intervalo de 1s.
- **Falha final**: fallback para o parser determinístico, com `erro_extracao` preenchido no resultado.
- **Em nenhum caso** um erro quebra o pipeline — o arquivo problemático é
  marcado, logado, e o loop segue.

### Tratamento de arquivos problemáticos

- **Encoding**: tenta UTF-8 → fallback cp1252 (cobre caracteres Windows
  0x80–0x9F como €, que latin-1 trata como controle); remove bytes de
  controle (`\x00-\x08`, `\x7f-\x9f`) e marca `encoding_issue=True`.
- **Campos truncados** (ex.: `STATUS: PAG`): detectados pela regra `STATUS_INVALIDO`
  (vocabulário fechado) e `CAMPO_AUSENTE` (campos críticos faltando).
- **Mojibake** (`Ã©` em vez de `é`): regex `Ã[©§£¡]` aciona a IA, que infere o valor original.
- **Arquivos não-txt dentro do zip**: ignorados silenciosamente (junto com `__MACOSX/`).

---

## 🚩 Anomalias detectadas (10 regras)

| # | Regra | Gravidade | Lógica |
|---|-------|-----------|--------|
| 1 | `NF_DUPLICADA` | Alto | Mesmo `NUMERO_DOCUMENTO` + mesmo `FORNECEDOR` em ≥2 arquivos |
| 2 | `CNPJ_DIVERGENTE` | Alto | CNPJ ≠ CNPJ canônico (mais frequente) daquele fornecedor |
| 3 | `FORNECEDOR_SEM_HISTORICO` | Alto | Fornecedor aparece menos que o limiar adaptativo do lote (mín. 2×, máx. 5×; requer ≥3 docs) |
| 4 | `NF_APOS_PAGAMENTO` | Alto | `DATA_EMISSAO_NF > DATA_PAGAMENTO` (retroatividade) |
| 5 | `APROVADOR_DESCONHECIDO` | Médio | Aprovador fora da lista recorrente (limiar adaptativo: mín. 2×, máx. 3×; requer ≥3 docs) |
| 6 | `VALOR_FORA_FAIXA` | Médio | Z-score > 3 vs média histórica do fornecedor (≥10 amostras) |
| 7 | `STATUS_INVALIDO` | Médio | STATUS fora do vocabulário: `{PAGO, CANCELADO, ESTORNADO, PENDENTE}` |
| 8 | `STATUS_INCONSISTENTE` | Baixo | `PENDENTE` com `DATA_PAGAMENTO` preenchida (contradição lógica) |
| 9 | `CAMPO_AUSENTE` | Médio | Campos críticos faltando (NUMERO, FORNECEDOR, VALOR, HASH) |
| 10 | `ENCODING_INVALIDO` | Médio | Bytes corrompidos ou caracteres não-UTF8 |

**Nota sobre `STATUS_INCONSISTENTE`**: Nos dados do desafio, *todos* os
documentos `CANCELADO` e `ESTORNADO` têm `DATA_PAGAMENTO` preenchida —
isso é padrão do fluxo (primeiro paga, depois cancela/estorna), não
anomalia. Só `PENDENTE` com pagamento é contradição real. Por isso essa
regra sai com **confiança Baixa** — o auditor humano decide se investiga.

---

## 📊 Relatório de anomalias encontradas no lote fornecido

Executando sobre os 1.000 arquivos de `arquivos_nf.zip`:

| Regra | Qtd | Arquivos |
|---|---|---|
| `NF_DUPLICADA` | 8 (4 pares) | DOC_0020 + DOC_0855 (NF-24322 Marketing), DOC_0083 + DOC_0732 (NF-67424 RH), DOC_0150 + DOC_0151 (NF-55555 TechSoft), DOC_0276 + DOC_0976 (NF-37973 DataCenter) |
| `CNPJ_DIVERGENTE` | 3 | Marketing Digital Pro com CNPJ `99.888.777/0001-00` (canônico = `45.678.901/0001-56`) |
| `FORNECEDOR_SEM_HISTORICO` | 3 | "Serviços Gamma SA" (2×) e "Consultoria Beta Ltda" (1×) — fornecedores nunca vistos antes |
| `NF_APOS_PAGAMENTO` | 2 | DOC_0750 (emit=02/01/2024, pag=22/12/2023) · DOC_0751 (emit=28/12/2023, pag=19/12/2023) |
| `APROVADOR_DESCONHECIDO` | 1 | "João Ninguém" em 1 documento |
| `STATUS_INVALIDO` | 1 | DOC_0089 com `STATUS: PAG` (truncado) |
| `CAMPO_AUSENTE` | 1 | DOC_0089 sem `HASH_VERIFICACAO` |
| `ENCODING_INVALIDO` | 1 | DOC_0487 com bytes `0x84 0x93` ao final do hash |
| `STATUS_INCONSISTENTE` | 107 | PENDENTE com data de pagamento — confiança baixa, sinalizado para revisão |

**Total de anomalias de alta/média confiança (plantadas pelo desafio): ~20** distribuídas em ~17 arquivos únicos.

---

## 📋 Log de auditoria

Cada registro do log tem:

```csv
timestamp,arquivo,evento,detalhe,regra,campo_evidencia,confianca,versao_prompt,modelo_ia,latencia_ms
2026-04-17T14:23:01Z,DOC_0020.txt,READ,"Lido 327 bytes; encoding_ok=True",,,,,,
2026-04-17T14:23:01Z,DOC_0020.txt,EXTRACT,"Fonte=regex; obs=",,,Alto,v1.3.0,-,1
2026-04-17T14:23:04Z,DOC_0020.txt,DETECT,"NF NF-24322 aparece em 2 arquivos",NF_DUPLICADA,"NUMERO_DOCUMENTO, FORNECEDOR",Alto,,,
```

Eventos registrados: `READ`, `EXTRACT`, `DETECT`, `ERROR`, `EXPORT`, `SUMMARY`.
Exportável em CSV (direto na UI) ou JSONL (programático).

---

## 📜 Licença

Projeto desenvolvido como tarefa de processo seletivo. Código livre para
uso educacional.
