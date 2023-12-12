import streamlit as st
import json
import hvac
import re
import pandas as pd
import os
import pytz
import streamlit_authenticator as stauth

from datetime import datetime, timedelta
from io import BytesIO, StringIO
from google.cloud import storage
from google.oauth2 import service_account
from zipfile import ZipFile, ZIP_DEFLATED

st.set_page_config(page_title="Validador do GTFS", page_icon='./favicon.ico')

client = hvac.Client(
    url=os.getenv('VAULT_URL'),
    token=os.getenv('VAULT_TOKEN'),
)
config = client.secrets.kv.read_secret_version('gtfs-validator', raise_on_deleted_version=True)['data']['data']

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['preauthorized']
)

os_columns = [
    "Serviço",
    "Vista",
    "Consórcio",
    "Extensão de Ida",
    "Extensão de Volta",
    "Horário Inicial Dia Útil",
    "Horário Fim Dia Útil",
    "Partidas Ida Dia Útil",
    "Partidas Volta Dia Útil",
    "Viagens Dia Útil",
    "Quilometragem Dia Útil",
    "Partidas Ida Sábado",
    "Partidas Volta Sábado",
    "Viagens Sábado",
    "Quilometragem Sábado",
    "Partidas Ida Domingo",
    "Partidas Volta Domingo",
    "Viagens Domingo",
    "Quilometragem Domingo",
    "Partidas Ida Ponto Facultativo",
    "Partidas Volta Ponto Facultativo",
    "Viagens Ponto Facultativo",
    "Quilometragem Ponto Facultativo"
]


def os_sheets(os_file):
    df = pd.read_excel(os_file, None)
    return len(df.keys())

def check_os_filetype(os_file):
    try:
        pd.read_excel(os_file)
        return True
    except ValueError:
        return False

def check_os_filename(os_file):
    pattern = re.compile(r'^os_\d{4}-\d{2}-\d{2}.xlsx$')
    return bool(pattern.match(os_file.name))


def check_os_columns(os_df):
    cols = sorted(list(os_df.columns))
    return bool(cols == sorted(os_columns))


def check_os_columns_order(os_df):
    cols = list(os_df.columns)
    return bool(cols == os_columns)


def check_gtfs_filename(gtfs_file):
    pattern = re.compile(r'^gtfs_\d{4}-\d{2}-\d{2}.zip$')
    return bool(pattern.match(gtfs_file.name))


def change_feed_info_dates(file: bytes, os_initial_date: datetime, os_final_date: datetime) -> bytes:

    # Descompacta o arquivo zip direto na memoria
    input_zip = ZipFile(BytesIO(file), 'r')
    files: dict[str, bytes] = {name: input_zip.read(name) for name in input_zip.namelist()}

    # Transforma os bytes em arquivo
    file_text = str(files['feed_info.txt'],'utf-8')    
    string_buffer = StringIO(file_text) 

    # Muda a data
    df = pd.read_csv(string_buffer)
    df['feed_start_date'] = os_initial_date.strftime('%Y%m%d')
    df['feed_end_date'] = os_final_date.strftime('%Y%m%d')

    # Transforma o dataframe em csv na memoria
    string_buffer = StringIO() 
    df.to_csv(string_buffer, index=False, lineterminator='\r\n')
    
    files['feed_info.txt'] = bytes(string_buffer.getvalue(), encoding='utf8')

    # Compacta o arquivo novamente
    zip_buffer = BytesIO()
    with ZipFile(zip_buffer, "w", ZIP_DEFLATED, False) as zip_file:
        for file_name, file_data in files.items():
            zip_file.writestr(file_name, BytesIO(file_data).getvalue())

    return zip_buffer


def upload_to_gcs(file_name: str, file_data: BytesIO) -> None:
    json_acct_info = json.loads(os.getenv('STORAGE_CREDENTIALS'), strict=False)
    credentials = service_account.Credentials.from_service_account_info(
        json_acct_info)
    storage_client = storage.Client(credentials=credentials, project='rj-smtr')
    bucket = storage_client.bucket('gtfs-validator-files')
    blob = bucket.blob(file_name)
    blob.upload_from_file(file_data)

def main():
    st.title(":pencil: Validação GTFS e OS")
    st.caption("Envie ambos os arquivos para iniciar o processo de validação. Ao final, se tudo estiver correto, você pode confirmar para subir os dados no data lake!")

    os_file = st.file_uploader("Insira o arquivo OS:", type=["xlsx"])
    gtfs_file = st.file_uploader("Insira o arquivo GTFS:", type=["zip"])

    # Check OS and GTFS files
    if os_file and gtfs_file:

        if not check_os_filetype(os_file):
            st.warning(
                ":warning: O nome do arquivo OS não é do tipo correto! Transforme o arquivo no formato .xlsx do Excel.")
            return
        
        os_sheets = pd.read_excel(os_file, None)

        if len(os_sheets) == 1:
            os_df = os_sheets.popitem()[1]
        else:
            st.warning(
                "O arquivo possui mais de uma aba, selecione a aba que contém os dados")
            options = ['Selecione a aba'] + list(os_sheets)
            actual_sheet = st.selectbox('Selecione a aba', options)
            if actual_sheet == 'Selecione a aba':
                return
            os_df = os_sheets[actual_sheet]

        viagens_cols = ["Viagens Dia Útil", "Viagens Sábado",
                        "Viagens Domingo", "Viagens Ponto Facultativo"]
        km_cols = ["Quilometragem Dia Útil", "Quilometragem Sábado",
                    "Quilometragem Domingo", "Quilometragem Ponto Facultativo"]


        if not check_os_columns(os_df):
            st.warning(
                ":warning: O arquivo OS não contém as colunas esperadas!")
            return

        for col in viagens_cols + km_cols:
            os_df[col] = (
                os_df[col].astype(str)
                .str.strip()
                .str.replace("—", "0")
                .str.replace(",", ".")
                .astype(float)
                .fillna(0)
                
            )
            os_df[col] = os_df[col].astype(float)

        st.success(
            ":white_check_mark: O arquivo OS contém as colunas esperadas!")

        if not check_os_columns_order(os_df):
            st.warning(
                f":warning: O arquivo OS contém as colunas esperadas, porém não segue a ordem esperada: {os_columns}")

        # Check dates
        st.subheader("Confirme por favor os itens abaixo:")

        os_initial_date = st.date_input(
            "Qual a data **inicial** de vigência da OS?", value=None
        )

        os_final_date = st.date_input(
            "Qual a data **final** de vigência da OS?", value=None
        )

        if (os_initial_date is not None) and (os_final_date is not None):
            
            os_check_initial = False

            if os_initial_date > datetime.now().date():
                st.warning(
                    ":warning: ATENÇÃO: Você está subindo uma OS cuja operação já começou! Prossiga se é isso mesmo, senão revise as datas escolhidas."
                )

                os_delay_choice = st.radio(
                    "Escolha o motivo para o atraso da OS:",
                    ["Retificação", "Correção"],
                    index=None
                )

                os_delay_description = st.text_input(
                    "Adicione uma observação para explicar o motivo:"
                )
                if (os_delay_choice is not None) and os_delay_description:
                    os_check_initial = True
            else:
                os_check_initial = True
            
            if not os_check_initial:
                return
            
            # Check data
            st.subheader(
                ":face_with_monocle: Ótimo! Verifique os dados antes de subir:")

            # TODO: Partidas x Extensão, Serviços OS x GTFS (routes, trips, shapes), Extensão OS x GTFS"

            # Numero de servicos por consorcio
            tb = pd.DataFrame(os_df.groupby(
                "Consórcio")["Serviço"].count())
            tb.loc["Total"] = tb.sum()
            st.table(tb)

            # Numero de viagens por consorcio
            tb = pd.DataFrame(os_df.groupby(
                "Consórcio")[viagens_cols].sum())
            tb.loc["Total"] = tb.sum()
            st.table(tb.style.format("{:.1f}"))

            # Numero de KM por consorcio
            tb = pd.DataFrame(os_df.groupby(
                "Consórcio")[km_cols].sum())
            tb.loc["Total"] = tb.sum()
            st.table(tb.style.format("{:.3f}"))

            if st.button('Enviar', type="primary"):
                now = datetime.now(pytz.timezone('America/Sao_Paulo'))
                today_str = now.strftime('%Y-%m-%d')
                now_str = now.isoformat()

                # Gera um .csv direto na memoria
                string_buffer = StringIO() 
                os_df.to_csv(string_buffer, index=False, sep=',')
                string_buffer = bytes(string_buffer.getvalue(), encoding='utf8')
                os_filename = f'data={today_str}/os-{st.session_state["username"]}-{now_str}.csv'
                stringio_os = BytesIO(string_buffer)
                upload_to_gcs(os_filename, stringio_os)

                gtfs_filename = f'data={today_str}/gtfs-{st.session_state["username"]}-{now_str}.zip'
                stringio_gtfs = change_feed_info_dates(gtfs_file.getvalue(), os_initial_date, os_final_date)
                stringio_gtfs = BytesIO(stringio_gtfs.getvalue())
                upload_to_gcs(gtfs_filename, stringio_gtfs)

                st.write('Enviado')

if __name__ == "__main__":
    authenticator.login('Login', 'main')
    if st.session_state["authentication_status"]:
        authenticator.logout('Logout', 'main', key='unique_key')
        st.write(f'Bem vinda(o) *{st.session_state["name"]}*!')
        main()
    elif st.session_state["authentication_status"] is False:
        st.error('Usuário ou senha incorreta')
    elif st.session_state["authentication_status"] is None:
        st.warning('Por favor insira seu nome de usuário e senha')
