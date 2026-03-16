from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timezone
import json
import requests
import os
import pandas as pd
import re
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

API_URL = "https://pv-strapi-api-live.maida.health/api/tabelas?sort[1]=ordem:asc&populate=*"
BASE_URL = "https://pv-strapi-api-live.maida.health"
STATE_PATH = "/opt/airflow/state/state.json"
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "C0AGNL16ZS4")


BASE_DOWNLOAD_DIR = "/opt/airflow/dags/downloads/planserv"
RAW_DIR = os.path.join(BASE_DOWNLOAD_DIR, "raw")
SILVER_DIR = os.path.join(BASE_DOWNLOAD_DIR, "silver")
GOLD_DIR = os.path.join(BASE_DOWNLOAD_DIR, "gold")



def extrair_metadata_planserv(**context):

    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()

    json_data = response.json()
    data = json_data.get("data", [])

    arquivos_metadata = []

    for item in data:

        attributes = item.get("attributes", {})
        ordem = attributes.get("ordem")

        if ordem not in (1, 2):
            continue

        arquivo_data = attributes.get("arquivo", {}).get("data")
        if not arquivo_data:
            continue

        arquivo_attr = arquivo_data.get("attributes", {})

        url_relativa = arquivo_attr.get("url")
        nome_arquivo = arquivo_attr.get("name")
        updated_at = arquivo_attr.get("updatedAt")

        if not url_relativa or not nome_arquivo or not updated_at:
            continue

        # Define tipo baseado na ordem
        if ordem == 1:
            tipo = "materiais"
        elif ordem == 2:
            tipo = "medicamentos"

        arquivos_metadata.append({
            "tipo": tipo,
            "nome": nome_arquivo,
            "url": BASE_URL + url_relativa,
            "updatedAt": updated_at
        })

    if not arquivos_metadata:
        raise Exception("Nenhum arquivo válido encontrado na API.")

    context["ti"].xcom_push(
        key="arquivos_metadata",
        value=arquivos_metadata
    )

    print("Metadata extraída com sucesso:")
    for a in arquivos_metadata:
        print(a)


def baixar_arquivos(**context):

    arquivos = context["ti"].xcom_pull(
        key="novos_arquivos",
        task_ids="check_if_new"
    )

    if not arquivos:
        raise Exception("Nenhum arquivo para download.")

    arquivos_baixados = []

    for item in arquivos:

        tipo = item["tipo"]
        url = item["url"]
        nome = item["nome"]

        pasta_destino = os.path.join(RAW_DIR, tipo)
        os.makedirs(pasta_destino, exist_ok=True)

        caminho = os.path.join(pasta_destino, nome)

        print(f"Baixando {nome}...")

        r = requests.get(url, stream=True, timeout=60)
        r.raise_for_status()

        with open(caminho, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

        arquivos_baixados.append({
            "tipo": tipo,
            "nome": nome,
            "caminho": caminho,
            "updatedAt": item["updatedAt"]
        })

    context["ti"].xcom_push(
        key="arquivos_baixados",
        value=arquivos_baixados
    )






def gerar_silver(**context):

    arquivos = context["ti"].xcom_pull(
        key="arquivos_baixados",
        task_ids="download_files"
    )

    for item in arquivos:

        tipo = item["tipo"]
        caminho_excel = item["caminho"]

        pasta_destino = os.path.join(SILVER_DIR, tipo)
        os.makedirs(pasta_destino, exist_ok=True)

        nome_arquivo = os.path.basename(caminho_excel).replace(".xlsx", ".txt")
        caminho_saida = os.path.join(pasta_destino, nome_arquivo)

        df = pd.read_excel(
            caminho_excel,
            engine="openpyxl",
            dtype=str
        )

        if tipo == "materiais":
            silver_materiais(df, caminho_saida)
        else:
            silver_medicamentos(df, caminho_saida)



def silver_materiais(df, caminho_saida):

    # A planilha de materiais possui 1 linha de cabeçalho antes dos dados reais
    df = df.iloc[1:]
    df = df.replace(r'^\s*$', '', regex=True)
    df = df.dropna(how="all")

    df.iloc[:, 0] = pd.to_numeric(df.iloc[:, 0], errors="coerce")
    df = df.dropna(subset=[df.columns[0]])
    df.iloc[:, 0] = df.iloc[:, 0].astype(int)

    df = df.fillna("")
    df = df.astype(str)

    df = df.replace(r"\n", " ", regex=True)
    df = df.replace(r"\r", " ", regex=True)
    df = df.replace('"', '', regex=True)

    df.to_csv(
        caminho_saida,
        sep="|",
        index=False,
        header=False,
        encoding="utf-8"
    )

    print(f"SILVER MATERIAIS gerado: {caminho_saida}")

    
    
def silver_medicamentos(df, caminho_saida):

    # A planilha de medicamentos possui 4 linhas de cabeçalho antes dos dados reais
    df = df.iloc[4:]
    df = df.replace(r'^\s*$', '', regex=True)
    df = df.dropna(how="all")
    
    df = df.dropna(subset=[df.columns[0]])

    df = df.fillna("")
    df = df.astype(str)

    df = df.replace(r"\n", " ", regex=True)
    df = df.replace(r"\r", " ", regex=True)
    df = df.replace('"', '', regex=True)

    df.to_csv(
        caminho_saida,
        sep="|",
        index=False,
        header=False,
        encoding="utf-8"
    )

    print(f"SILVER MEDICAMENTOS gerado: {caminho_saida}")
    
    
def gerar_gold(**context):

    arquivos = context["ti"].xcom_pull(
        key="arquivos_baixados",
        task_ids="download_files"
    )

    for item in arquivos:

        tipo = item["tipo"]
        nome_arquivo = item["nome"].replace(".xlsx", ".txt")

        caminho_silver = os.path.join(SILVER_DIR, tipo, nome_arquivo)

        pasta_destino = os.path.join(GOLD_DIR, tipo)
        os.makedirs(pasta_destino, exist_ok=True)

        caminho_saida = os.path.join(pasta_destino, nome_arquivo)

        df = pd.read_csv(
            caminho_silver,
            sep="|",
            header=None,
            dtype=str,
            encoding="utf-8"
        )

        if tipo == "materiais":
            gold_materiais(df, caminho_saida)
        else:
            gold_medicamentos(df, caminho_saida)




def gold_materiais(df, caminho_saida):

    indices = [7, 8, 12, 12, 1, 2, 3, 0, 9]

    max_index = max(indices)

    if max_index >= df.shape[1]:
        raise IndexError(
            f"O arquivo possui apenas {df.shape[1]} colunas, "
            f"mas o índice {max_index} foi solicitado."
        )

    df_gold = df.iloc[:, indices]

    df_gold.to_csv(
        caminho_saida,
        sep="|",
        header=False,
        index=False,
        encoding="utf-8"
    )

    print(f"GOLD MATERIAIS gerado: {caminho_saida}")
    
def gold_medicamentos(df, caminho_saida):
    
    
    indices = [2, 3, 4, 6, 6]

    max_index = max(indices)

    if max_index >= df.shape[1]:
        raise IndexError(
            f"O arquivo possui apenas {df.shape[1]} colunas, "
            f"mas o índice {max_index} foi solicitado."
        )

    # Seleciona as colunas desejadas (incluindo duplicação do índice 6)
    df_gold = df.iloc[:, indices].copy()

    # Adiciona campo em branco
    df_gold["col_blank"] = ""

    # Adiciona dois campos fixos com zero
    df_gold["col_zero_1"] = "0"
    df_gold["col_zero_2"] = "0"


    # Você define os índices depois
    df_gold.to_csv(
        caminho_saida,
        sep="|",
        header=False,
        index=False,
        encoding="utf-8"
    )

    print(f"GOLD MEDICAMENTOS gerado: {caminho_saida}")




def verificar_se_novo(**context):

    arquivos = context["ti"].xcom_pull(
        key="arquivos_metadata",
        task_ids="extract_metadata"
    )

    if not arquivos:
        print("Nenhum arquivo retornado da metadata.")
        return "stop_pipeline"

    estado = {}

    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, "r") as f:
                conteudo = f.read().strip()
                if conteudo:
                    estado = json.loads(conteudo)
        except json.JSONDecodeError:
            print("state.json inválido — recriando.")

    estado_planserv = estado.get("planserv", {})

    novos_arquivos = []


    for item in arquivos:

        nome = item["nome"]
        updated_at_api = item["updatedAt"]
        updated_at_salvo = estado_planserv.get(nome, {}).get("updatedAt")

        if not updated_at_salvo:
            print(f"Arquivo novo detectado: {nome}")
            novos_arquivos.append(item)
            continue

        dt_api = datetime.fromisoformat(updated_at_api.replace("Z", "+00:00"))
        dt_salvo = datetime.fromisoformat(updated_at_salvo.replace("Z", "+00:00"))

        if dt_api > dt_salvo:
            print(f"Arquivo atualizado detectado: {nome}")
            novos_arquivos.append(item)
        else:
            print(f"Arquivo sem alteração: {nome}")

    if not novos_arquivos:
        return "stop_pipeline"

    context["ti"].xcom_push(
        key="novos_arquivos",
        value=novos_arquivos
    )

    return "download_files"



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


def formatar_nome_slack(tipo, updated_at):
    # Deriva mês e ano do updatedAt ISO (ex: "2026-03-15T10:00:00.000Z")
    dt = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
    mes = str(dt.month).zfill(2)
    ano = str(dt.year)[2:]
    prefixo = "MAT" if tipo == "materiais" else "MED"
    return f"PLANSERV_{prefixo}_{mes}_{ano}.txt"


def enviar_gold_para_slack(**context):

    slack_token = os.environ.get("SLACK_BOT_TOKEN")
    if not slack_token:
        raise Exception("SLACK_BOT_TOKEN não configurado.")

    client = WebClient(token=slack_token)

    arquivos = context["ti"].xcom_pull(
        key="arquivos_baixados",
        task_ids="download_files"
    )

    if not arquivos:
        print("Nenhum arquivo para enviar.")
        return

    for item in arquivos:

        tipo = item["tipo"]
        nome_arquivo = item["nome"].replace(".xlsx", ".txt")
        nome_slack = formatar_nome_slack(tipo, item["updatedAt"])

        caminho_gold = os.path.join(GOLD_DIR, tipo, nome_arquivo)

        if not os.path.exists(caminho_gold):
            print(f"Arquivo não encontrado: {caminho_gold}")
            continue


        try:
            client.files_upload_v2(
                channel=SLACK_CHANNEL,
                file=caminho_gold,
                title=nome_slack,
                initial_comment=f"Arquivo atualizado: {nome_slack}"
            )

            print(f"Enviado: {nome_arquivo}")

        except SlackApiError as e:
            raise Exception(f"Erro Slack: {e.response['error']}")





def atualizar_estado(**context):

    novos = context["ti"].xcom_pull(
        key="novos_arquivos",
        task_ids="check_if_new"
    )

    if not novos:
        print("Nada para atualizar.")
        return

    estado = {}

    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, "r") as f:
                conteudo = f.read().strip()
                if conteudo:
                    estado = json.loads(conteudo)
        except json.JSONDecodeError:
            print("state.json inválido — recriando.")

    if "planserv" not in estado:
        estado["planserv"] = {}

    for item in novos:
        estado["planserv"][item["nome"]] = {
            "updatedAt": item["updatedAt"],
            "processed_at": datetime.now(timezone.utc).isoformat()
        }

    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(estado, f, indent=4)

    print("Estado atualizado com updatedAt.")




with DAG(
    dag_id="planserv_data_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 */12 * * *",
    catchup=False,
    max_active_runs=1
) as dag:


    extract_metadata = PythonOperator(
        task_id="extract_metadata",
        python_callable=extrair_metadata_planserv
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
        python_callable=gerar_silver
    )

    gold = PythonOperator(
        task_id="generate_gold",
        python_callable=gerar_gold,
        on_failure_callback=notificar_erro_slack
    )
    
    send_slack = PythonOperator(
        task_id="send_to_slack",
        python_callable=enviar_gold_para_slack
    )



    update = PythonOperator(
        task_id="update_state",
        python_callable=atualizar_estado
    )

    stop = EmptyOperator(task_id="stop_pipeline")

    extract_metadata >> check_file

    check_file >> stop
    check_file >> download

    download >> silver >> gold >> send_slack >> update

