from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timezone
import json
import requests
import os
import re
import zipfile
import io
import pandas as pd
from bs4 import BeautifulSoup
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

CMED_PAGE_URL = "https://www.gov.br/anvisa/pt-br/assuntos/medicamentos/cmed/precos"
TUSS_ZIP_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/terminologia_unificada_saude_suplementar_TUSS/TUSS.zip"
STATE_PATH = "/opt/airflow/state/cmed_tuss_state.json"
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "C0AGNL16ZS4")

BASE_DOWNLOAD_DIR = "/opt/airflow/dags/downloads/cmed_tuss"
RAW_CMED_DIR = os.path.join(BASE_DOWNLOAD_DIR, "raw", "cmed")
RAW_TUSS_DIR = os.path.join(BASE_DOWNLOAD_DIR, "raw", "tuss")
SILVER_CMED_DIR = os.path.join(BASE_DOWNLOAD_DIR, "silver", "cmed")
SILVER_TUSS_DIR = os.path.join(BASE_DOWNLOAD_DIR, "silver", "tuss")
GOLD_DIR = os.path.join(BASE_DOWNLOAD_DIR, "gold")

# Índices de join: REGISTRO
CMED_JOIN_COL = 4
TUSS_JOIN_COL = 7

# Índice da coluna código TUSS a ser adicionada na CMED
TUSS_CODE_COL = 0


def extrair_url_cmed(**context):

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    response = requests.get(CMED_PAGE_URL, timeout=30, headers=headers)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")

    url_cmed = None
    for link in soup.find_all("a", href=True):
        href = link["href"]
        # Identifica o arquivo PMC pelo padrão xls_conformidade_site_*.xlsx
        if "xls_conformidade_site_" in href and "@@download/file" in href:
            url_cmed = href
            break

    if not url_cmed:
        raise Exception("Link do arquivo PMC (CMED) não encontrado na página.")

    if not url_cmed.startswith("http"):
        url_cmed = "https://www.gov.br" + url_cmed

    print(f"URL CMED encontrada: {url_cmed}")
    context["ti"].xcom_push(key="url_cmed", value=url_cmed)


def verificar_se_novo(**context):

    url_cmed = context["ti"].xcom_pull(key="url_cmed", task_ids="extract_cmed_url")

    estado = {}

    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, "r") as f:
                conteudo = f.read().strip()
                if conteudo:
                    estado = json.loads(conteudo)
        except json.JSONDecodeError:
            print("state.json inválido — recriando.")

    url_salva = estado.get("cmed", {}).get("url")

    if url_salva == url_cmed:
        print("CMED sem alteração. Encerrando pipeline.")
        return "stop_pipeline"

    print(f"Nova versão da CMED detectada.")
    return "download_files"


def baixar_arquivos(**context):

    url_cmed = context["ti"].xcom_pull(key="url_cmed", task_ids="extract_cmed_url")

    # Extrai nome do arquivo da URL (parte antes de /@@download/file)
    partes = url_cmed.rstrip("/").split("/")
    nome_cmed = next((p for p in reversed(partes) if p.endswith(".xlsx")), "cmed_pmc.xlsx")

    os.makedirs(RAW_CMED_DIR, exist_ok=True)
    caminho_cmed = os.path.join(RAW_CMED_DIR, nome_cmed)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }

    for tentativa in range(1, 4):
        print(f"Baixando CMED PMC: {nome_cmed}... (tentativa {tentativa}/3)")
        r = requests.get(url_cmed, stream=True, timeout=300, headers=headers)
        r.raise_for_status()

        content_type = r.headers.get("Content-Type", "")
        print(f"Content-Type recebido: {content_type}")
        if "html" in content_type.lower():
            raise Exception(
                f"Servidor retornou HTML em vez do arquivo XLSX. "
                f"Content-Type: {content_type}. URL: {url_cmed}"
            )

        with open(caminho_cmed, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

        tamanho = os.path.getsize(caminho_cmed)
        print(f"Arquivo salvo: {tamanho} bytes")

        if zipfile.is_zipfile(caminho_cmed):
            print("Arquivo validado com sucesso.")
            break

        if tentativa < 3:
            print(f"Arquivo incompleto ({tamanho} bytes). Retentando...")
        else:
            raise Exception(
                f"Download da CMED incompleto após 3 tentativas. "
                f"Último tamanho: {tamanho} bytes. O servidor pode estar cortando a conexão."
            )

    print("Baixando TUSS ZIP...")
    r = requests.get(TUSS_ZIP_URL, stream=True, timeout=120)
    r.raise_for_status()

    zip_content = io.BytesIO()
    for chunk in r.iter_content(chunk_size=8192):
        zip_content.write(chunk)
    zip_content.seek(0)

    os.makedirs(RAW_TUSS_DIR, exist_ok=True)
    caminho_tuss = os.path.join(RAW_TUSS_DIR, "tb_20_medicamento.csv")

    with zipfile.ZipFile(zip_content) as zf:
        arquivos_zip = zf.namelist()
        print(f"Arquivos no ZIP: {arquivos_zip}")

        # Localiza a tabela 20 de medicamentos
        tuss_file = next(
            (n for n in arquivos_zip if "20" in n and "medicamento" in n.lower()),
            None
        )

        if not tuss_file:
            raise Exception(
                f"Tabela 20 (medicamentos) não encontrada no ZIP. "
                f"Arquivos disponíveis: {arquivos_zip}"
            )

        with zf.open(tuss_file) as f_in, open(caminho_tuss, "wb") as f_out:
            f_out.write(f_in.read())

    print(f"TUSS extraído: {caminho_tuss}")

    context["ti"].xcom_push(key="caminho_cmed", value=caminho_cmed)
    context["ti"].xcom_push(key="caminho_tuss", value=caminho_tuss)
    context["ti"].xcom_push(key="nome_cmed", value=nome_cmed)


def gerar_silver(**context):

    caminho_cmed = context["ti"].xcom_pull(key="caminho_cmed", task_ids="download_files")
    caminho_tuss = context["ti"].xcom_pull(key="caminho_tuss", task_ids="download_files")

    # --- Silver CMED ---
    os.makedirs(SILVER_CMED_DIR, exist_ok=True)
    nome_silver_cmed = os.path.basename(caminho_cmed).replace(".xlsx", ".txt")
    caminho_silver_cmed = os.path.join(SILVER_CMED_DIR, nome_silver_cmed)

    df_cmed = pd.read_excel(caminho_cmed, engine="openpyxl", dtype=str)

    # Ajustar o iloc conforme estrutura real do arquivo após primeira inspeção
    df_cmed = df_cmed.iloc[0:]
    df_cmed = df_cmed.replace(r'^\s*$', '', regex=True)
    df_cmed = df_cmed.dropna(how="all")
    df_cmed = df_cmed.fillna("")
    df_cmed = df_cmed.astype(str)
    df_cmed = df_cmed.replace(r"\n", " ", regex=True)
    df_cmed = df_cmed.replace(r"\r", " ", regex=True)
    df_cmed = df_cmed.replace('"', '', regex=True)

    df_cmed.to_csv(caminho_silver_cmed, sep="|", index=False, header=False, encoding="utf-8")
    print(f"SILVER CMED gerado: {caminho_silver_cmed}")

    # --- Silver TUSS ---
    os.makedirs(SILVER_TUSS_DIR, exist_ok=True)
    caminho_silver_tuss = os.path.join(SILVER_TUSS_DIR, "tb_20_medicamento.txt")

    # Arquivo TUSS: CSV separado por ponto e vírgula, codificação UTF-8
    df_tuss = pd.read_csv(caminho_tuss, sep=";", dtype=str, encoding="utf-8")

    df_tuss = df_tuss.replace(r'^\s*$', '', regex=True)
    df_tuss = df_tuss.dropna(how="all")
    df_tuss = df_tuss.fillna("")
    df_tuss = df_tuss.astype(str)
    df_tuss = df_tuss.replace(r"\n", " ", regex=True)
    df_tuss = df_tuss.replace(r"\r", " ", regex=True)
    df_tuss = df_tuss.replace('"', '', regex=True)

    df_tuss.to_csv(caminho_silver_tuss, sep="|", index=False, header=False, encoding="utf-8")
    print(f"SILVER TUSS gerado: {caminho_silver_tuss}")

    context["ti"].xcom_push(key="caminho_silver_cmed", value=caminho_silver_cmed)
    context["ti"].xcom_push(key="caminho_silver_tuss", value=caminho_silver_tuss)


def gerar_gold(**context):

    caminho_silver_cmed = context["ti"].xcom_pull(key="caminho_silver_cmed", task_ids="generate_silver")
    caminho_silver_tuss = context["ti"].xcom_pull(key="caminho_silver_tuss", task_ids="generate_silver")
    nome_cmed = context["ti"].xcom_pull(key="nome_cmed", task_ids="download_files")

    df_cmed = pd.read_csv(caminho_silver_cmed, sep="|", header=None, dtype=str, encoding="utf-8")
    df_tuss = pd.read_csv(caminho_silver_tuss, sep="|", header=None, dtype=str, encoding="utf-8")

    # Normaliza a coluna de join (REGISTRO) para comparação sem espaços
    df_cmed[CMED_JOIN_COL] = df_cmed[CMED_JOIN_COL].str.strip()
    df_tuss[TUSS_JOIN_COL] = df_tuss[TUSS_JOIN_COL].str.strip()

    # Left join: mantém todos os itens da CMED
    df_gold = df_cmed.merge(
        df_tuss[[TUSS_JOIN_COL, TUSS_CODE_COL]],
        left_on=CMED_JOIN_COL,
        right_on=TUSS_JOIN_COL,
        how="left",
        suffixes=("", "_tuss")
    )

    # Renomeia a coluna código TUSS adicionada
    col_tuss_code = f"{TUSS_CODE_COL}_tuss" if f"{TUSS_CODE_COL}_tuss" in df_gold.columns else TUSS_CODE_COL
    df_gold = df_gold.rename(columns={col_tuss_code: "codigo_tuss"})

    # Remove coluna duplicada do join (REGISTRO do TUSS)
    if TUSS_JOIN_COL in df_gold.columns:
        df_gold = df_gold.drop(columns=[TUSS_JOIN_COL])

    df_gold["codigo_tuss"] = df_gold["codigo_tuss"].fillna("")

    # Separa itens sem correspondência no TUSS
    df_sem_correspondencia = df_cmed[
        ~df_cmed[CMED_JOIN_COL].isin(df_tuss[TUSS_JOIN_COL])
    ].copy()

    os.makedirs(GOLD_DIR, exist_ok=True)

    # Monta nome do arquivo gold com padrão CMED_MESANO
    match = re.search(r'(\d{4})(\d{2})', nome_cmed)
    if match:
        ano = match.group(1)
        mes = match.group(2)
        sufixo = f"{mes}{ano}"
    else:
        sufixo = datetime.now(timezone.utc).strftime("%m%Y")

    caminho_gold = os.path.join(GOLD_DIR, f"CMED_PMC_{sufixo}.txt")
    caminho_sem_corr = os.path.join(GOLD_DIR, f"CMED_SEM_CORRESPONDENCIA_{sufixo}.txt")

    df_gold.to_csv(caminho_gold, sep="|", index=False, header=False, encoding="utf-8")
    print(f"GOLD gerado: {caminho_gold}")

    df_sem_correspondencia.to_csv(caminho_sem_corr, sep="|", index=False, header=False, encoding="utf-8")
    qtd_sem_corr = len(df_sem_correspondencia)
    print(f"Sem correspondência: {qtd_sem_corr} itens → {caminho_sem_corr}")

    context["ti"].xcom_push(key="caminho_gold", value=caminho_gold)
    context["ti"].xcom_push(key="caminho_sem_corr", value=caminho_sem_corr)
    context["ti"].xcom_push(key="qtd_sem_corr", value=qtd_sem_corr)
    context["ti"].xcom_push(key="sufixo", value=sufixo)


def notificar_erro_slack(context):

    slack_token = os.environ.get("SLACK_BOT_TOKEN")
    if not slack_token:
        print("SLACK_BOT_TOKEN não configurado — notificação de erro ignorada.")
        return

    client = WebClient(token=slack_token)

    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    exec_date = context.get("execution_date")
    exception = context.get("exception")

    mensagem = (
        f":red_circle: *Erro no pipeline {dag_id}*\n"
        f"*Task:* `{task_id}`\n"
        f"*Execução:* {exec_date}\n"
        f"*Motivo:* {exception}"
    )

    try:
        client.chat_postMessage(channel=SLACK_CHANNEL, text=mensagem)
    except Exception as e:
        print(f"Falha ao enviar notificação de erro para o Slack: {e}")


def enviar_para_slack(**context):

    slack_token = os.environ.get("SLACK_BOT_TOKEN")
    if not slack_token:
        raise Exception("SLACK_BOT_TOKEN não configurado.")

    client = WebClient(token=slack_token)

    caminho_gold = context["ti"].xcom_pull(key="caminho_gold", task_ids="generate_gold")
    caminho_sem_corr = context["ti"].xcom_pull(key="caminho_sem_corr", task_ids="generate_gold")
    qtd_sem_corr = context["ti"].xcom_pull(key="qtd_sem_corr", task_ids="generate_gold")
    sufixo = context["ti"].xcom_pull(key="sufixo", task_ids="generate_gold")

    nome_gold = f"CMED_PMC_{sufixo}.txt"
    nome_sem_corr = f"CMED_SEM_CORRESPONDENCIA_{sufixo}.txt"

    # Envia arquivo gold
    try:
        client.files_upload_v2(
            channel=SLACK_CHANNEL,
            file=caminho_gold,
            title=nome_gold,
            initial_comment=f"Tabela CMED PMC atualizada com código TUSS: {nome_gold}"
        )
        print(f"Gold enviado: {nome_gold}")
    except SlackApiError as e:
        raise Exception(f"Erro Slack ao enviar gold: {e.response['error']}")

    # Envia mensagem com quantidade de itens sem correspondência + arquivo
    try:
        client.files_upload_v2(
            channel=SLACK_CHANNEL,
            file=caminho_sem_corr,
            title=nome_sem_corr,
            initial_comment=(
                f":warning: *{qtd_sem_corr} itens da CMED sem correspondência no TUSS.*\n"
                f"Segue arquivo com os registros não encontrados:"
            )
        )
        print(f"Sem correspondência enviado: {qtd_sem_corr} itens")
    except SlackApiError as e:
        raise Exception(f"Erro Slack ao enviar sem correspondência: {e.response['error']}")


def atualizar_estado(**context):

    url_cmed = context["ti"].xcom_pull(key="url_cmed", task_ids="extract_cmed_url")

    estado = {}

    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, "r") as f:
                conteudo = f.read().strip()
                if conteudo:
                    estado = json.loads(conteudo)
        except json.JSONDecodeError:
            print("state.json inválido — recriando.")

    estado["cmed"] = {
        "url": url_cmed,
        "processed_at": datetime.now(timezone.utc).isoformat()
    }

    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(estado, f, indent=4)

    print("Estado atualizado.")


with DAG(
    dag_id="cmed_tuss_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 */12 * * *",
    catchup=False,
    max_active_runs=1
) as dag:

    extract_url = PythonOperator(
        task_id="extract_cmed_url",
        python_callable=extrair_url_cmed
    )

    check_file = BranchPythonOperator(
        task_id="check_if_new",
        python_callable=verificar_se_novo
    )

    download = PythonOperator(
        task_id="download_files",
        python_callable=baixar_arquivos,
        trigger_rule="none_failed_min_one_success"
    )

    silver = PythonOperator(
        task_id="generate_silver",
        python_callable=gerar_silver,
        on_failure_callback=notificar_erro_slack
    )

    gold = PythonOperator(
        task_id="generate_gold",
        python_callable=gerar_gold,
        on_failure_callback=notificar_erro_slack
    )

    send_slack = PythonOperator(
        task_id="send_to_slack",
        python_callable=enviar_para_slack,
        on_failure_callback=notificar_erro_slack
    )

    update = PythonOperator(
        task_id="update_state",
        python_callable=atualizar_estado
    )

    stop = EmptyOperator(task_id="stop_pipeline")

    extract_url >> check_file

    check_file >> stop
    check_file >> download

    download >> silver >> gold >> send_slack >> update
