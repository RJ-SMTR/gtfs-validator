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

def read_stream(stream: bytes) -> pd.DataFrame:
    file_text = str(stream,'utf-8')    
    string_buffer = StringIO(file_text) 
    return pd.read_csv(string_buffer)

def get_shapes(shapes: pd.DataFrame) -> pd.DataFrame:
    
    #Identifica pontos finais
    max_pt_shapes = shapes.groupby("shape_id").shape_pt_sequence.idxmax()

    #Identifica pontos iniciais
    min_pt_shapes = shapes.groupby("shape_id").shape_pt_sequence.idxmin()

    #Realiza merge entre dataframes
    shapes_min_max = shapes.loc[max_pt_shapes].merge(shapes.loc[min_pt_shapes], on='shape_id', how='left', suffixes=('_max', '_min'))

    #Identifica shapes circulares (mesma coordenada de início e término)
    #shapes_min_max['flag_circular'] = (shapes_min_max['shape_pt_lat_max'] == shapes_min_max['shape_pt_lat_min']) & (shapes_min_max['shape_pt_lon_max'] == shapes_min_max['shape_pt_lon_min'])
    # Arredondamento para 4ª casa decimal - Caso 651 e 652
    shapes_min_max['flag_circular'] = (
        (round(shapes_min_max['shape_pt_lat_max'],4) == round(shapes_min_max['shape_pt_lat_min'],4)) 
        & (round(shapes_min_max['shape_pt_lon_max'],4) == round(shapes_min_max['shape_pt_lon_min'],4))
    )
    
    # filtra shapes não circulares
    shapes_final = shapes[~shapes.shape_id.isin(shapes_min_max[shapes_min_max['flag_circular'] == True]['shape_id'].to_list())]

    # filtra shapes circulares
    shapes_circulares = shapes[shapes.shape_id.isin(shapes_min_max[shapes_min_max['flag_circular'] == True]['shape_id'].to_list())]

    # identifica metade do trajeto circular
    shapes_c_breakpoint = round(shapes_circulares.groupby("shape_id").shape_pt_sequence.max()/2, 0).to_dict()

    # separa metades do trajeto em ida + volta 
    shapes_circulares = pd.DataFrame()

    for idx in shapes_c_breakpoint:
        aux = shapes[shapes.shape_id == idx]

        shapes_final = pd.concat([shapes_final, aux])

        aux.loc[aux.shape_pt_sequence <= shapes_c_breakpoint[idx], "shape_id"] = f"{idx}_0"
        aux.loc[aux.shape_pt_sequence > shapes_c_breakpoint[idx], "shape_id"] = f"{idx}_1"
        aux["shape_pt_sequence"] = aux.groupby("shape_id").cumcount()

        # adiciona ida + volta na tabela final
        shapes_final = pd.concat([shapes_final, aux])
    return shapes_final

def get_trips(file: bytes) -> pd.DataFrame:

    # Descompacta o arquivo zip direto na memoria
    input_zip = ZipFile(BytesIO(file), 'r')
    files: dict[str, bytes] = {name: input_zip.read(name) for name in input_zip.namelist()}
    trips = read_stream(files['trips.txt'])
    agency = read_stream(files['agency.txt'])
    routes = read_stream(files['routes.txt'])
    shapes = read_stream(files['shapes.txt'])
    shapes_final = get_shapes(shapes)
    
    trips = trips.merge(routes, how='left', on='route_id').merge(agency, how='left', on='agency_id')
    
    trips_qh = trips.copy()
    # Serão considerados os trip_id de sábado para as linhas abaixo
    linhas_sab = ["SP601"]
    trips_qh = trips_qh.sort_values(by=["service_id"], ascending=False)
    trips_qh = trips_qh[((~(trips_qh['trip_short_name'].isin(linhas_sab)) &
                          (trips_qh['service_id'].str.startswith(("U_R", "U_O"), na=False))) | 
                         ((trips_qh['trip_short_name'].isin(linhas_sab)) & 
                          (trips_qh['service_id'].str.startswith(("S_R", "S_O"), na=False))))]
    trips_qh = (trips_qh[trips_qh['shape_id'].isin(list(set(shapes_final['shape_id'].to_list())))]
            .sort_values(['trip_short_name', 'service_id', 'shape_id', 'direction_id'])
            .drop_duplicates(['trip_short_name', 'direction_id']))
    
    trips_agg = (pd.pivot_table(trips_qh, values='trip_id', index = ['trip_short_name'], 
                            columns='direction_id', aggfunc='first')
             .rename_axis(None, axis=1)
             .reset_index()
             .rename(columns={'trip_short_name': 'servico', 0: 'trip_id_ida', 1: 'trip_id_volta'})
             [['servico', 'trip_id_ida', 'trip_id_volta']]
             .sort_values(by=['servico']))
    return trips_agg

def get_board(quadro: pd.DataFrame):
    columns = {'Serviço': 'servico',
           'Vista': 'vista',
           'Consórcio': 'consorcio',
           'Consorcio': 'consorcio',
           'Horário Inicial': 'horario_inicio',
           'Horário Início': 'horario_inicio',
           'Horário início': 'horario_inicio',
           'Horário Inicial Dia Útil': 'horario_inicio',
           'Horário Fim Dia Útil': 'horario_fim',
           'Horário Fim': 'horario_fim',
           'Horário fim': 'horario_fim',
           'Partidas Ida Dia Útil': 'partidas_ida_du',
           'Partidas ida dia útil': 'partidas_ida_du',
           'Partidas Ida\n(DU)': 'partidas_ida_du',
           'Partidas Volta Dia Útil': 'partidas_volta_du',
           'Partidas volta dia útil': 'partidas_volta_du',
           'Partidas Volta\n(DU)': 'partidas_volta_du',
           'Extensão de Ida': 'extensao_ida',
           'Extensão de ida': 'extensao_ida',
           'Ext.\nIda': 'extensao_ida',
           'Extensão de Volta': 'extensao_volta',
           'Extensão de volta': 'extensao_volta',
           'Ext.\nVolta': 'extensao_volta',
           'Viagens Dia Útil': 'viagens_du',
           'Viagens dia útil': 'viagens_du',
           'Viagens\n(DU)': 'viagens_du',
           'Quilometragem Dia Útil': 'km_dia_util',
           'Quilometragem dia útil': 'km_dia_util',
           'KM\n(DU)': 'km_dia_util',
           'Quilometragem Sábado': 'km_sabado',
           'Quilometragem sábado': 'km_sabado',
           'KM\n(SAB)': 'km_sabado',
           'Quilometragem Domingo': 'km_domingo',
           'Quilometragem domingo': 'km_domingo',
           'KM\n(DOM)': 'km_domingo',
           'Partida Ida Ponto Facultativo': 'partidas_ida_pf',
           'Partidas Ida Ponto Facultativo': 'partidas_ida_pf',
           'Partidas Ida\n(FAC)': 'partidas_ida_pf',
           'Partida Volta Ponto Facultativo': 'partidas_volta_pf',
           'Partidas Volta Ponto Facultativo': 'partidas_volta_pf',
           'Partidas Volta\n(FAC)': 'partidas_volta_pf',   
           'Viagens Ponto Facultativo': 'viagens_pf',
           'Viagens\n(FAC)': 'viagens_pf',
           'Quilometragem Ponto Facultativo': 'km_pf',
           'KM\n(FAC)': 'km_pf'}

    quadro = quadro.rename(columns = columns)

    # corrige nome do servico
    quadro["servico"] = quadro["servico"].astype(str)
    quadro["servico"] = quadro["servico"].str.extract(r"([A-Z]+)").fillna("") + quadro[
        "servico"
    ].str.extract(r"([0-9]+)")
    quadro = quadro[list(set(columns.values()))]

    quadro = quadro.replace("—", 0)

    #Ajusta colunas numéricas
    numeric_cols = quadro.columns.difference(["servico", "vista", "consorcio", "horario_inicio", "horario_fim", "extensao_ida", "extensao_volta"]).to_list()
    quadro[numeric_cols] = quadro[numeric_cols].astype(str)
    quadro[numeric_cols] = quadro[numeric_cols].apply(lambda x: x.str.replace(".", ""))
    quadro[numeric_cols] = quadro[numeric_cols].apply(lambda x: x.str.replace(",", "."))
    quadro[numeric_cols] = quadro[numeric_cols].apply(pd.to_numeric)

    extensao_cols = ["extensao_ida", "extensao_volta"]
    quadro[extensao_cols] = quadro[extensao_cols].astype(str)
    quadro[extensao_cols] = quadro[extensao_cols].apply(lambda x: x.str.replace(",00", ""))
    quadro[extensao_cols] = quadro[extensao_cols].apply(pd.to_numeric)

    quadro["extensao_ida"] = quadro["extensao_ida"]/1000
    quadro["extensao_volta"] = quadro["extensao_volta"]/1000

    # Ajusta colunas com hora
    hora_cols = [coluna for coluna in quadro.columns if "horario" in coluna]
    quadro[hora_cols] = quadro[hora_cols].astype(str)

    for hora_col in hora_cols:
        quadro[hora_col] = quadro[hora_col].apply(lambda x: x.split(" ")[1] if " " in x else x)

    # Ajusta colunas de km
    hora_cols = [coluna for coluna in quadro.columns if "km" in coluna]

    for hora_col in hora_cols:
        quadro[hora_col] = quadro[hora_col]/100
    return quadro

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
                ":warning: O arquivo OS não é do tipo correto! Transforme o arquivo no formato .xlsx do Excel.")
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
            "Qual a data **inicial** de vigência da OS?", value=None, format="DD/MM/YYYY"
        )

        os_final_date = st.date_input(
            "Qual a data **final** de vigência da OS?", value=None, format="DD/MM/YYYY"
        )

        if os_initial_date and os_final_date:
            if os_initial_date > os_final_date:
                st.warning(
                    ":warning: ATENÇÃO: A data inicial escolhida é posterior a data final. Revise as datas e tente novamente."
                )
                return

            if (datetime.now().date() > os_initial_date):
                st.warning(
                    ":warning: ATENÇÃO: Você está subindo uma OS cuja operação já começou! Prossiga se é isso mesmo, senão revise as datas escolhidas."
                )

        os_type = st.radio(
            "Escolha o tipo da OS:",
            ["Regular", "Extraordinária - Retificação", "Extraordinária - Correção", "Extraordinária - Verão"],
            index=None
        )

        os_description = st.text_input(
            "Adicione uma breve descrição da OS:"
        )

        if os_initial_date and os_final_date and os_type and os_description:
            
            trips_agg = get_trips(gtfs_file.getvalue())
            quadro = get_board(os_df)
            quadro_merged = quadro.merge(trips_agg, on='servico', how='left')
            if len(quadro_merged[(quadro_merged["trip_id_ida"].isna()) & (quadro_merged["trip_id_volta"].isna())]) > 0:
                st.warning(
                    ":warning: ATENÇÃO: Existem trip_ids nulas"
                )
                st.table(quadro_merged[(quadro_merged["trip_id_ida"].isna()) & (quadro_merged["trip_id_volta"].isna())])
                return
            if len(quadro_merged[((quadro_merged["partidas_ida_du"] > 0) & (quadro_merged["trip_id_ida"].isna())) | 
              ((quadro_merged["partidas_volta_du"] > 0) & (quadro_merged["trip_id_volta"].isna()))].sort_values('servico')) > 0:
                st.warning(
                    ":warning: ATENÇÃO: Existem viagens com ida e volta que possuem trip_ids nulas"
                )
                st.table(quadro_merged[((quadro_merged["partidas_ida_du"] > 0) & (quadro_merged["trip_id_ida"].isna())) | 
              ((quadro_merged["partidas_volta_du"] > 0) & (quadro_merged["trip_id_volta"].isna()))].sort_values('servico'))
                return
            if len(quadro_merged[((quadro_merged["extensao_ida"] == 0) & ~(quadro_merged["trip_id_ida"].isna())) | 
              ((quadro_merged["extensao_volta"] == 0) & ~(quadro_merged["trip_id_volta"].isna()))]) > 0:
                st.warning(
                    ":warning: ATENÇÃO: Existem viagens programadas sem extensão definida"
                )
                st.table(quadro_merged[((quadro_merged["extensao_ida"] == 0) & ~(quadro_merged["trip_id_ida"].isna())) | 
              ((quadro_merged["extensao_volta"] == 0) & ~(quadro_merged["trip_id_volta"].isna()))])
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



