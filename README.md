# Price Data Pipeline

Pipeline de dados automatizado para ingestão, processamento e disponibilização de tabelas de preços de saúde, seguindo arquitetura em camadas (RAW → SILVER → GOLD), orquestrado com Apache Airflow.

---

## Visão Geral

O projeto contém duas DAGs independentes que monitoram fontes externas, detectam atualizações, processam os dados em camadas e entregam os arquivos tratados via Slack.

| DAG | Fonte | Frequência |
|---|---|---|
| `planserv_data_pipeline` | API Planserv (Maida Health) | A cada 12 horas |
| `cmed_tuss_pipeline` | ANVISA/CMED + ANS/TUSS | A cada 12 horas |

---

## Pipelines

### planserv_data_pipeline

Processa as tabelas de **Materiais e OPME** e **Medicamentos** publicadas pela Planserv via API.

**Fluxo de tasks:**

```
extract_metadata → check_if_new → download_files → generate_silver → generate_gold → send_to_slack → update_state
                               ↘ stop_pipeline
```

**Etapas:**
- `extract_metadata` — consulta a API e extrai URL, nome e `updatedAt` de cada arquivo
- `check_if_new` — compara `updatedAt` com o estado salvo; pula se não houver mudança
- `download_files` — baixa os arquivos XLSX para a camada RAW
- `generate_silver` — normaliza encoding, remove linhas vazias e salva como TXT pipe-delimited
- `generate_gold` — seleciona colunas relevantes por índice para cada tipo
- `send_to_slack` — envia os arquivos GOLD com nome formatado (`TIPO_MESANO.txt`)
- `update_state` — persiste `updatedAt` em `state/state.json`

---

### cmed_tuss_pipeline

Cruza a tabela **CMED PMC** (preços de medicamentos da ANVISA) com a **Tabela 20 TUSS** (terminologia ANS), enriquecendo os itens CMED com o código TUSS correspondente.

**Fluxo de tasks:**

```
extract_cmed_url → check_if_new → download_files → generate_silver → generate_gold → send_to_slack → update_state
                               ↘ stop_pipeline
```

**Etapas:**
- `extract_cmed_url` — faz scraping da página ANVISA/CMED para localizar a URL do arquivo PMC mais recente
- `check_if_new` — compara a URL com a salva em `state/cmed_tuss_state.json`; pula se não houver mudança
- `download_files` — baixa o XLSX da CMED (com retry e validação de integridade) e o ZIP do TUSS, extraindo a tabela 20 de medicamentos
- `generate_silver` — normaliza ambos os arquivos para TXT pipe-delimited
- `generate_gold` — realiza left join CMED × TUSS pelo campo REGISTRO; gera também um arquivo separado com os itens sem correspondência no TUSS
- `send_to_slack` — envia o arquivo GOLD (`CMED_PMC_MESANO.txt`) e o relatório de itens sem correspondência
- `update_state` — persiste a URL processada em `state/cmed_tuss_state.json`

Em caso de falha em qualquer task de processamento, uma notificação é enviada automaticamente ao Slack.

---

## Arquitetura de Camadas

```
   Fonte externa
        │
        ▼
   RAW  — arquivos originais sem modificação
        │
        ▼
 SILVER — normalização: encoding UTF-8, remoção de linhas vazias,
          separador pipe (|), remoção de quebras de linha internas
        │
        ▼
   GOLD — seleção de colunas / enriquecimento com regra de negócio
        │
        ▼
  Slack — entrega dos arquivos para consumo
```

---

## Estrutura do Projeto

```
.
├── dags/
│   ├── planserv_pipeline.py
│   ├── cmed_tuss_pipeline.py
│   └── downloads/
│       ├── planserv/
│       │   ├── raw/
│       │   │   ├── materiais/
│       │   │   └── medicamentos/
│       │   ├── silver/
│       │   │   ├── materiais/
│       │   │   └── medicamentos/
│       │   └── gold/
│       │       ├── materiais/
│       │       └── medicamentos/
│       └── cmed_tuss/
│           ├── raw/
│           │   ├── cmed/
│           │   └── tuss/
│           ├── silver/
│           │   ├── cmed/
│           │   └── tuss/
│           └── gold/
├── state/
│   ├── state.json           # estado do planserv_data_pipeline
│   └── cmed_tuss_state.json # estado do cmed_tuss_pipeline
├── docker-compose.yml
└── README.md
```

---

## Stack

- Python 3
- Apache Airflow 2.8.1
- Docker + Docker Compose
- PostgreSQL 15 (metadados do Airflow)
- Pandas + openpyxl
- Requests + BeautifulSoup4 + lxml
- slack_sdk

---

## Configuração

### Variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto com:

```env
SLACK_BOT_TOKEN=xoxb-seu-token-aqui
SLACK_CHANNEL=ID_DO_CANAL
```

> `SLACK_CHANNEL` tem como padrão `C0AGNL16ZS4` caso não seja definido.

O bot do Slack precisa das permissões `chat:write` e `files:write`.

---

## Como Executar

```bash
# 1. Subir o ambiente
docker-compose up -d

# 2. Inicializar o banco e criar usuário admin
#    (executado automaticamente pelo serviço airflow-init)

# 3. Acessar a UI do Airflow
http://localhost:8080
# usuário: admin | senha: admin

# 4. Ativar as DAGs desejadas e executar manualmente ou aguardar o agendamento
```

---

## Controle de Estado

Cada pipeline mantém um arquivo JSON em `state/` para evitar reprocessamento desnecessário:

- **Planserv:** compara o campo `updatedAt` retornado pela API por arquivo
- **CMED/TUSS:** compara a URL do arquivo PMC detectado na página da ANVISA

Se nenhuma mudança for detectada, a execução termina na task `stop_pipeline` sem processar ou notificar.
