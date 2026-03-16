from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime
import json
import os
import re
import requests
import urllib.parse
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


URL = "https://assinantes.brasindice.com.br/index.php"
DOWNLOAD_DIR = "/opt/airflow/dags/downloads/brasindice"
STATE_PATH = "/opt/airflow/state/state.json"
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "C0AGNL16ZS4")

TIPOS = [
    {
        "tipo": "Medicamentos",
        "sufixo": "Med",
        "colunas": ["codigo", "descricao", "pre_pmc", "pre_pfab", "tiss", "tuss", "ean"],
    },
    {
        "tipo": "Materiais, Dietas e Outros",
        "sufixo": "Mat",
        "colunas": ["codigo", "descricao", "pre_pmc", "pre_pfab", "tiss", "tuss"],
    },
    {
        "tipo": "Dietas e Nutrição",
        "sufixo": "Die",
        "colunas": ["codigo", "descricao", "pre_pmc", "pre_pfab", "tiss", "tuss"],
    },
    {
        "tipo": "Outros Fármacos",
        "sufixo": "Out",
        "colunas": ["codigo", "descricao", "pre_pmc", "pre_pfab", "tiss", "tuss"],
    },
    {
        "tipo": "Soluções Parenterais",
        "sufixo": "Sol",
        "colunas": ["codigo", "descricao", "pre_pmc", "pre_pfab", "tiss", "tuss"],
    },
]

ALL_COLS = [
    "codigo", "descricao", "pre_pmc", "pre_pfab", "tiss", "tuss", "ean",
    "anvisa", "ggrem", "generico", "hierarquia", "restrito", "oncologico", "icms0", "liberado",
]


def obter_cookie():
    try:
        return Variable.get("BRASINDICE_COOKIE")
    except Exception:
        raise Exception("Variável BRASINDICE_COOKIE não configurada no Airflow (Admin → Variables).")


def build_payload(tipo, colunas):
    data = [
        ("module", "Brasindice"),
        ("action", "CustomExport"),
        ("step", "run"),
        ("sugar_body_only", "true"),
        ("to_csv", "true"),
        ("tipo_sel", tipo),
        ("tipo_sel_ac", tipo),
        ("tipo", tipo),
        ("estado_sel", "BA"),
        ("estado_sel_ac", "BA"),
        ("estado", "BA"),
        ("decimais_sel", "2"),
        ("decimais_sel_ac", "2"),
        ("decimais", "2"),
        ("formato_sel", "Delimitado"),
        ("formato_sel_ac", "Delimitado"),
        ("formato", "Delimitado"),
    ]
    for col in ALL_COLS:
        data.append((col, "0"))
        if col in colunas:
            data.append((col, "1"))
    return data


def extrair_edicao(content_disposition):
    match = re.search(r'Edi[^\s]*\s+(\d+)', content_disposition, re.IGNORECASE)
    if match:
        return match.group(1)
    raise Exception(f"Não foi possível extrair edição de: {content_disposition}")


def carregar_estado():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def salvar_estado(estado):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(estado, f, ensure_ascii=False, indent=4)


def notificar_cookie_expirado():
    slack_token = os.environ.get("SLACK_BOT_TOKEN")
    if not slack_token:
        print("SLACK_BOT_TOKEN não configurado.")
        return
    client = WebClient(token=slack_token)
    mensagem = (
        "*:warning: Cookie do Brasíndice expirado*\n\n"
        "O cookie de autenticação expirou. Para renovar:\n\n"
        "1. Acesse https://assinantes.brasindice.com.br e faça login\n"
        "2. Abra o DevTools (F12) → aba *Application* → *Cookies* → `assinantes.brasindice.com.br`\n"
        "3. Copie o valor de `ck_login_id_20`\n"
        "4. No Airflow UI: *Admin → Variables → BRASINDICE_COOKIE* → cole o novo valor e salve\n"
        "5. Reexecute a DAG `brasindice_pipeline`"
    )
    try:
        client.chat_postMessage(channel=SLACK_CHANNEL, text=mensagem)
    except SlackApiError as e:
        print(f"Falha ao notificar Slack: {e}")


def notificar_erro_slack(context):
    slack_token = os.environ.get("SLACK_BOT_TOKEN")
    if not slack_token:
        return
    client = WebClient(token=slack_token)
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    error = str(context.get("exception", "Erro desconhecido"))
    try:
        client.chat_postMessage(
            channel=SLACK_CHANNEL,
            text=f":x: Erro no pipeline Brasíndice\nDAG: `{dag_id}` | Task: `{task_id}`\n```{error}```"
        )
    except SlackApiError as e:
        print(f"Falha ao notificar erro no Slack: {e}")


def checar_edicao(**context):
    cookie = obter_cookie()

    tipo_probe = TIPOS[0]
    body = urllib.parse.urlencode(
        build_payload(tipo_probe["tipo"], tipo_probe["colunas"]),
        encoding="iso-8859-1"
    )

    session = requests.Session()
    session.cookies.set("ck_login_id_20", cookie, domain="assinantes.brasindice.com.br")

    r = session.post(
        URL,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=60,
        stream=True,
    )
    r.raise_for_status()

    content_type = r.headers.get("Content-Type", "")
    if "text/html" in content_type:
        r.close()
        notificar_cookie_expirado()
        raise Exception("Cookie do Brasíndice expirado. Slack notificado.")

    edicao = extrair_edicao(r.headers.get("Content-Disposition", ""))
    r.close()

    estado = carregar_estado()
    if estado.get("brasindice", {}).get("ultima_edicao") == edicao:
        print(f"Edição {edicao} já processada. Nenhuma atualização.")
        return "sem_atualizacao"

    context["ti"].xcom_push(key="edicao", value=edicao)
    return "baixar_arquivos"


def baixar_arquivos(**context):
    cookie = obter_cookie()
    edicao = context["ti"].xcom_pull(key="edicao", task_ids="checar_edicao")

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    session = requests.Session()
    session.cookies.set("ck_login_id_20", cookie, domain="assinantes.brasindice.com.br")

    arquivos = []

    for t in TIPOS:
        body = urllib.parse.urlencode(
            build_payload(t["tipo"], t["colunas"]),
            encoding="iso-8859-1"
        )

        r = session.post(
            URL,
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=120,
        )
        r.raise_for_status()

        nome = f"{edicao}{t['sufixo']}.txt"
        caminho = os.path.join(DOWNLOAD_DIR, nome)

        with open(caminho, "wb") as f:
            f.write(r.content)

        print(f"Baixado: {nome}")
        arquivos.append({"nome": nome, "caminho": caminho})

    context["ti"].xcom_push(key="arquivos", value=arquivos)


def enviar_slack(**context):
    slack_token = os.environ.get("SLACK_BOT_TOKEN")
    if not slack_token:
        raise Exception("SLACK_BOT_TOKEN não configurado.")

    client = WebClient(token=slack_token)
    edicao = context["ti"].xcom_pull(key="edicao", task_ids="checar_edicao")
    arquivos = context["ti"].xcom_pull(key="arquivos", task_ids="baixar_arquivos")

    for arq in arquivos:
        try:
            client.files_upload_v2(
                channel=SLACK_CHANNEL,
                file=arq["caminho"],
                title=arq["nome"],
                initial_comment=f"Brasíndice Edição {edicao}: {arq['nome']}"
            )
            print(f"Enviado: {arq['nome']}")
        except SlackApiError as e:
            raise Exception(f"Erro Slack ao enviar {arq['nome']}: {e.response['error']}")


def atualizar_estado(**context):
    edicao = context["ti"].xcom_pull(key="edicao", task_ids="checar_edicao")
    estado = carregar_estado()
    estado.setdefault("brasindice", {})["ultima_edicao"] = edicao
    salvar_estado(estado)
    print(f"Estado atualizado: edição {edicao}")


with DAG(
    dag_id="brasindice_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 8 1,15 * *",
    catchup=False,
    tags=["brasindice"],
) as dag:

    checar = BranchPythonOperator(
        task_id="checar_edicao",
        python_callable=checar_edicao,
        on_failure_callback=notificar_erro_slack,
    )

    sem_atualizacao = EmptyOperator(task_id="sem_atualizacao")

    baixar = PythonOperator(
        task_id="baixar_arquivos",
        python_callable=baixar_arquivos,
        on_failure_callback=notificar_erro_slack,
    )

    enviar = PythonOperator(
        task_id="enviar_slack",
        python_callable=enviar_slack,
        on_failure_callback=notificar_erro_slack,
    )

    atualizar = PythonOperator(
        task_id="atualizar_estado",
        python_callable=atualizar_estado,
        on_failure_callback=notificar_erro_slack,
    )

    checar >> [baixar, sem_atualizacao]
    baixar >> enviar >> atualizar
