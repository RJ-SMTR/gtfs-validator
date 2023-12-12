import streamlit as st
import json
import hvac
import re
import pandas as pd
import os
import pytz
import streamlit_authenticator as stauth

from datetime import datetime, timedelta
from io import BytesIO
from google.cloud import storage
from google.oauth2 import service_account

client = hvac.Client(
    url=os.getenv('VAULT_URL'),
    token=os.getenv('VAULT_TOKEN'),
)
config = client.secrets.kv.read_secret_version('gtfs-validator')['data']['data']

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


def check_os_filename(os_file):
    pattern = re.compile(r'^os_\d{4}-\d{2}-\d{2}.csv$')
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


def main():
    st.title(":pencil: Validação GTFS e OS")
    st.caption("Envie ambos os arquivos para iniciar o processo de validação. Ao final, se tudo estiver correto, você pode confirmar para subir os dados no data lake!")

    os_file = st.file_uploader("Insira o arquivo OS:", type=["csv"])
    gtfs_file = st.file_uploader("Insira o arquivo GTFS:", type=["zip"])

    # Check OS and GTFS files
    if os_file and gtfs_file:

            os_df = pd.read_csv(os_file)

            viagens_cols = ["Viagens Dia Útil", "Viagens Sábado",
                            "Viagens Domingo", "Viagens Ponto Facultativo"]
            km_cols = ["Quilometragem Dia Útil", "Quilometragem Sábado",
                       "Quilometragem Domingo", "Quilometragem Ponto Facultativo"]

            for col in viagens_cols + km_cols:
                os_df[col] = (
                    os_df[col]
                    .str.strip()
                    .str.replace("—", "0")
                    .str.replace(".", "")
                    .str.replace(",", ".")
                    .astype(float)
                )

            if not check_os_columns(os_df):
                st.warning(
                    ":warning: O arquivo OS não contém as colunas esperadas!")

            else:
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
                            "Adicione uma observação para explicar o motivo:",
                            index=None
                        )
                        if (os_delay_choice is not None) and (os_delay_description is not None):
                            os_check_initial = True
                    else:
                        os_check_initial = True
                    
                    if os_check_initial is True:
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
                            json_acct_info = json.loads(os.getenv('STORAGE_CREDENTIALS'), strict=False)
                            credentials = service_account.Credentials.from_service_account_info(
                                json_acct_info)
                            storage_client = storage.Client(credentials=credentials, project='rj-smtr')
                            bucket = storage_client.bucket('gtfs-validator-files')
                            blob_os = bucket.blob(f'data={today_str}/os-{st.session_state["username"]}-{now_str}.csv')
                            blob_gtfs = bucket.blob(f'data={today_str}/gtfs-{st.session_state["username"]}-{now_str}.zip')
                            stringio_os = BytesIO(os_file.getvalue())
                            stringio_gtfs = BytesIO(gtfs_file.getvalue())
                            blob_os.upload_from_file(stringio_os)
                            blob_gtfs.upload_from_file(stringio_gtfs)
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
