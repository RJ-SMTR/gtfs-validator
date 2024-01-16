import streamlit as st
import json
import hvac
import re
import pandas as pd
import os
import pytz
import streamlit_authenticator as stauth

from datetime import datetime, timedelta, time
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
    "Quilometragem Ponto Facultativo",
    'Horário Inicial Dia Útil',
    'Horário Fim Dia Útil',
    'Horário Inicial Sábado',
    'Horário Fim Sábado',
    'Horário Inicial Domingo',
    'Horário Fim Domingo'
]

def get_null_km_total(os_df: pd.DataFrame) -> pd.DataFrame:
    trips = {
        'du': {'going_trips_qty': 'Partidas Ida Dia Útil', 'returning_trips_qty': 'Partidas Volta Dia Útil', 'total_km': 'Quilometragem Dia Útil', 'total_km_calc': 'Quilometragem calculada DU'},
        'saturday': {'going_trips_qty': 'Partidas Ida Sábado', 'returning_trips_qty': 'Partidas Volta Sábado', 'total_km': 'Quilometragem Sábado', 'total_km_calc': 'Quilometragem calculada Sábado'},
        'sunday': {'going_trips_qty': 'Partidas Ida Domingo', 'returning_trips_qty': 'Partidas Volta Domingo', 'total_km': 'Quilometragem Domingo', 'total_km_calc': 'Quilometragem calculada Domingo'},
    }
    filters = []
    
    for day in trips:
        trip = trips[day]
        going_trips_qty = trip['going_trips_qty']
        total_km = trip['total_km']
        returning_trips_qty = trip['returning_trips_qty']
        filter1 = (os_df[going_trips_qty] != 0) & ((os_df['Extensão de Ida'] == 0) | (os_df[total_km] == 0))
        filter2 = (os_df[returning_trips_qty] != 0) & ((os_df['Extensão de Volta'] == 0) | (os_df[total_km] == 0))
        filters.append((filter1 | filter2))

    columns = ['Serviço', 'Vista', 'Consórcio', 'Extensão de Ida', 'Extensão de Volta',
                "Partidas Ida Dia Útil", "Partidas Volta Dia Útil",
                "Partidas Ida Sábado", "Partidas Volta Sábado",
                "Partidas Ida Domingo", "Partidas Volta Domingo",
                'Quilometragem Dia Útil', 
                'Quilometragem Sábado', 
                'Quilometragem Domingo', ]
    return os_df[filters[0] | filters[1] | filters[2]][columns]

def get_run_distance(os_df: pd.DataFrame) -> pd.DataFrame:
    trips = {
        'du': {'going_trips_qty': 'Partidas Ida Dia Útil', 'returning_trips_qty': 'Partidas Volta Dia Útil', 'total_km': 'Quilometragem Dia Útil', 'total_km_calc': 'Quilometragem calculada DU'},
        'saturday': {'going_trips_qty': 'Partidas Ida Sábado', 'returning_trips_qty': 'Partidas Volta Sábado', 'total_km': 'Quilometragem Sábado', 'total_km_calc': 'Quilometragem calculada Sábado'},
        'sunday': {'going_trips_qty': 'Partidas Ida Domingo', 'returning_trips_qty': 'Partidas Volta Domingo', 'total_km': 'Quilometragem Domingo', 'total_km_calc': 'Quilometragem calculada Domingo'},
    }
    filters = []
    os_copy = os_df.copy()
    
    for day in trips:
        trip = trips[day]

        going_trips_qty = trip['going_trips_qty']
        returning_trips_qty = trip['returning_trips_qty']
        total_km_calc = trip['total_km_calc']
        total_km = trip['total_km']

        going_run = os_df['Extensão de Ida'] * os_df[going_trips_qty].astype(int)
        returning_run = os_df['Extensão de Volta'] * os_df[returning_trips_qty].astype(int)

        total_length = ((going_run + returning_run)/1000).round(2)

        os_copy[total_km_calc] = total_length
        filters.append((abs(os_copy[total_km] - os_copy[total_km_calc] > 0.01)))
    
    columns = ['Serviço', 'Vista', 'Consórcio', 'Extensão de Ida', 'Extensão de Volta',
                "Partidas Ida Dia Útil", "Partidas Volta Dia Útil",
                "Partidas Ida Sábado", "Partidas Volta Sábado",
                "Partidas Ida Domingo", "Partidas Volta Domingo",
                'Quilometragem Dia Útil', 'Quilometragem calculada DU',
                'Quilometragem Sábado', 'Quilometragem calculada Sábado',
                'Quilometragem Domingo', 'Quilometragem calculada Domingo']
    return os_copy[filters[0] | filters[1] | filters[2]][columns]


def get_clock_problems(os_df: pd.DataFrame) -> pd.DataFrame:
    trips = {
        'du': {'start': 'Horário Inicial Dia Útil', 'end': 'Horário Fim Dia Útil'},
        'saturday': {'start': 'Horário Inicial Sábado', 'end': 'Horário Fim Sábado'},
        'sunday': {'start': 'Horário Inicial Domingo', 'end': 'Horário Fim Domingo'},
    }
    filters = []

    for day in trips:
        trip = trips[day]
        start = trip['start']
        end = trip['end']
        filter = (os_df[start] > os_df[end])
        filters.append(filter)
    
    columns = ['Serviço', 'Vista', 'Consórcio',
               'Horário Inicial Dia Útil', 'Horário Fim Dia Útil',
               'Horário Inicial Sábado', 'Horário Fim Sábado',
                'Horário Inicial Domingo', 'Horário Fim Domingo'
               ]
    
    return os_df[filters[0] | filters[1] | filters[2]][columns]


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

def cast_to_timedelta(entry: object) -> timedelta:
    if isinstance(entry, datetime):
        hours = entry.day * 24 + entry.hour
        minutes = entry.minute
        seconds = entry.second
        return timedelta(hours=hours, minutes=minutes, seconds=seconds)
    elif isinstance(entry, time):
        return timedelta(hours=entry.hour, minutes=entry.minute, seconds=entry.second)
   
    return timedelta(seconds=0)

def get_df_from_zip(file: bytes, filename: str) -> pd.DataFrame:
    # Descompacta o arquivo zip direto na memoria
    input_zip = ZipFile(BytesIO(file), 'r')
    files: dict[str, bytes] = {name: input_zip.read(name) for name in input_zip.namelist()}
    df = read_stream(files[filename])
    return df

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

def check_gtfs_trip_absence(os_df: pd.DataFrame, trips: pd.DataFrame) -> list[dict]:
    day_type = {
        "Partidas Ida Dia Útil": ('U_', 0),
        "Partidas Volta Dia Útil": ('U_', 1),
        "Partidas Ida Sábado": ('S_', 0),
        "Partidas Volta Sábado": ('S_', 1),
        "Partidas Ida Domingo": ('D_', 0),
        "Partidas Volta Domingo": ('D_', 1),
        # "Partidas Ida Domingo": ('D_REG', 0), TODO: transformar em U_OBRA, S_OBRA, D_OBRA, 
        # "Partidas Volta Domingo": ('D_REG', 1)
    }
    os_df = os_df[['Serviço', "Partidas Ida Dia Útil",
    "Partidas Volta Dia Útil",
              "Partidas Ida Sábado",
    "Partidas Volta Sábado",
              "Partidas Ida Domingo",
    "Partidas Volta Domingo"]]
    errors = []
    for _, service in os_df.iterrows():        
        for column in os_df.columns:

            if column == 'Serviço':
                continue

            service_id, direction_id = day_type[column]
            value = service[column]
            
            if isinstance(value, int):
                value = float(value)
            elif isinstance(value, float):
                pass
            elif isinstance(value, str):
                value = value.replace(',', '.')
                try:
                    value = float(value)
                except:
                    value = 0
            else:
                value = 0

            try:
                trip_short_name = f'{int(service["Serviço"]):03d}'
            except:
                trip_short_name = service['Serviço']
            
            trip_filter = (trips['trip_short_name'] == trip_short_name)
            service_filter = (trips['service_id'].str.startswith(service_id))
            direction_filter = (trips['direction_id'] == direction_id)
            total = len(trips[trip_filter & service_filter & direction_filter])

            if not (value == 0 or (total > 0 and value > 0)):
                errors.append({'Serviço': trip_short_name, 'service_id': service_id, 'direction_id': direction_id})
    return pd.DataFrame(errors)


def check_os_filename(os_file):
    pattern = re.compile(r'^os_\d{4}-\d{2}-\d{2}.xlsx$')
    return bool(pattern.match(os_file.name))


def check_os_columns(os_df):
    set_os_columns = set(os_columns)
    set_os_df_columns = set(os_df.columns)
    return set_os_columns.issubset(set_os_df_columns)

def reorder_columns(os_df):
    return os_df[os_columns]

def check_duplicates(os_df: pd.DataFrame) -> pd.DataFrame:
    return os_df[os_df.duplicated(['Serviço'],keep=False)]


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

def get_weekdays(calendar, service_id):
        row = calendar[calendar['service_id'] == service_id].iloc[0]
        active_days = []
        if row['monday'] == 1:
            active_days.append(0)
        if row['tuesday'] == 1:
            active_days.append(1)
        if row['wednesday'] == 1:
            active_days.append(2)
        if row['thursday'] == 1:
            active_days.append(3)
        if row['friday'] == 1:
            active_days.append(4)
        if row['saturday'] == 1:
            active_days.append(5)
        if row['sunday'] == 1:
            active_days.append(6)

        return set(active_days)


def check_conflicting_service_ids(trips: pd.DataFrame, calendar: pd.DataFrame, calendar_dates: pd.DataFrame):   

    start_date = pd.to_datetime(calendar['start_date'].astype(str), format="%Y%m%d")
    calendar['start_date'] = start_date

    end_date = pd.to_datetime(calendar['end_date'].astype(str), format="%Y%m%d")
    calendar['end_date'] = end_date

    calendar_dates['date'] = pd.to_datetime(calendar_dates['date'].astype(str), format="%Y%m%d")

    start = min(start_date)
    end = max(end_date)

    period = pd.date_range(start=start, end=end)

    new_calendar = pd.DataFrame()
    new_calendar['period'] = period
    new_calendar['service'] = ''

    for _, row in calendar.iterrows():
        active_days = []
        if row['monday'] == 1:
            active_days.append(0)
        if row['tuesday'] == 1:
            active_days.append(1)
        if row['wednesday'] == 1:
            active_days.append(2)
        if row['thursday'] == 1:
            active_days.append(3)
        if row['friday'] == 1:
            active_days.append(4)
        if row['saturday'] == 1:
            active_days.append(5)
        if row['sunday'] == 1:
            active_days.append(6)
        
        new_calendar.loc[(new_calendar['period'] >= row['start_date']) & (new_calendar['period'] <= row['end_date']) & (new_calendar['period'].dt.dayofweek.isin(active_days)), 'service'] += row['service_id'] + ','

    for _, row in calendar_dates.iterrows():
        if row['exception_type'] == 1:
            new_calendar.loc[(new_calendar['period'] == row['date']), 'service'] += row['service_id'] + ','
        elif row['exception_type'] == 2:
            
            services = set(new_calendar.loc[(new_calendar['period'] == row['date']), 'service'].iloc[0].split(','))

            if row['service_id'] in services:
                services.remove('')
                services.remove(row['service_id'])            
                new_calendar.loc[(new_calendar['period'] == row['date']), 'service'] = ','.join(services) + ','

    nc_uniq = new_calendar['service'].unique()
    trip_short_names = trips['trip_short_name'].unique()

    errors = []
    for trip_short_name in trip_short_names:        
        going = trips[(trips['trip_short_name'] == trip_short_name) & (trips['direction_id'] == 0)]['service_id'].unique()        
        returning = trips[(trips['trip_short_name'] == trip_short_name) & (trips['direction_id'] == 0)]['service_id'].unique()
        
        for data in nc_uniq:             
            going_dict = {}
            returning_dict = {}
            
            for item in going:            
                if item in data:
                    going_dict[item] = get_weekdays(calendar, item)
                    
            for item in returning:
                if item in data:
                    returning_dict[item] = get_weekdays(calendar, item)

            for item1 in going_dict:
                for item2 in going_dict:
                    if item1 != item2 and going_dict[item1].intersection(going_dict[item2]):
                        errors.append((trip_short_name, 0, item1, item2))
            
            for item1 in returning_dict:
                for item2 in returning_dict:
                    if item1 != item2 and returning_dict[item1].intersection(returning_dict[item2]):
                        errors.append((trip_short_name, 1, item1, item2))

    return errors

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
                        "Viagens Domingo", "Viagens Ponto Facultativo",
                        "Partidas Ida Dia Útil", "Partidas Volta Dia Útil",
                        "Partidas Ida Sábado", "Partidas Volta Sábado",
                        "Partidas Ida Domingo", "Partidas Volta Domingo"
                        ]
        km_cols = ["Quilometragem Dia Útil", "Quilometragem Sábado",
                    "Quilometragem Domingo", "Quilometragem Ponto Facultativo"]


        if not check_os_columns(os_df):
            st.warning(
                ":warning: O arquivo OS não contém as colunas esperadas!")
            set_os_columns = set(os_columns)
            set_os_df_columns = set(os_df.columns)
            set_os_columns.difference(set_os_df_columns)
            st.warning(set_os_columns.difference(set_os_df_columns))
            return

        for col in viagens_cols + km_cols:
            for i, row in os_df.iterrows():
                if isinstance(row[col], float):
                    pass
                elif isinstance(row[col], int):
                    os_df.loc[i, col] = float(row[col])
                elif isinstance(row[col], str):
                    value = row[col]
                    try:
                        value = float(value.replace('.', '').replace(',', '.'))
                    except:
                        value = 0.0
                    os_df.loc[i, col] = value
                else:
                    os_df.loc[i, col] = 0.0
            os_df[col] = os_df[col].astype(float)

            # os_df[col] = (
            #     os_df[col].astype(str)
            #     .str.strip()
            #     .str.replace("—", "0")
            #     .str.replace(".", "")
            #     .str.replace(",", ".")
            #     .astype(float)
            #     .fillna(0)
                
            # )
            
        
        time_cols = [
                    'Horário Inicial Dia Útil', 'Horário Fim Dia Útil',
                    'Horário Inicial Sábado', 'Horário Fim Sábado',
                    'Horário Inicial Domingo', 'Horário Fim Domingo'
                     ]
        
        for col in time_cols:
            os_df[col] = pd.to_timedelta(os_df[col].apply(cast_to_timedelta))
        # st.dataframe(os_df.style.format(func, thousands='.', decimal=','))

        st.success(
            ":white_check_mark: O arquivo OS contém as colunas esperadas!")
        os_df = reorder_columns(os_df)

        clock_problems = get_clock_problems(os_df)        
        if not clock_problems.empty:
            st.warning(
                ":warning: Horário inicial menor que horário final nas seguintes linhas:")
            st.dataframe(clock_problems)
            # return
        
        null_km_total = get_null_km_total(os_df)
        if not null_km_total.empty:
            st.warning(
                ":warning: Serviço/sentido com viagens mas sem extensão ou quilometragem nas seguintes linhas:")
            st.dataframe(null_km_total)
            # return
        
        duplicates = check_duplicates(os_df)
        if not duplicates.empty:
            st.warning(
                ":warning: O arquivo OS contém as seguintes colunas duplicadas:")
            st.table(duplicates)
            # return
        
        trips_df = get_df_from_zip(gtfs_file.getvalue(), 'trips.txt')
        calendar_df = get_df_from_zip(gtfs_file.getvalue(), 'calendar.txt')
        calendar_dates_df = get_df_from_zip(gtfs_file.getvalue(), 'calendar_dates.txt')
        errors = check_gtfs_trip_absence(os_df, trips_df)

        if not errors.empty:
            # error_msg = '\n'.join(map(lambda x : f'Serviço: {x["Serviço"]} | service_id: {x["service_id"]} | direction_id: {x["direction_id"]}\n', errors))
            st.warning(
                f":warning: Serviços/sentidos sem trip no GTFS:")
            st.dataframe(errors)
            # return
        
        conflicting_services = check_conflicting_service_ids(trips_df, calendar_df, calendar_dates_df)
        print('conflicting_services', conflicting_services)
        if conflicting_services:
            st.warning(
                f":warning: Serviços conflitantes:")
            st.warning(str(conflicting_services))

        run_distance = get_run_distance(os_df)
        if not run_distance.empty:
            st.warning(
                ":warning: As seguintes linhas têm quilometragem diferente de extensão x viagens:")
            func = lambda x: f'{float(x):,.2f}' if isinstance(x, int) else f'{x:,.2f}' if isinstance(x, float) else x
            st.dataframe(run_distance.style.format(func, thousands='.', decimal=','))
            # return
        
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

            # if st.button('Enviar', type="primary"):
            #     now = datetime.now(pytz.timezone('America/Sao_Paulo'))
            #     today_str = now.strftime('%Y-%m-%d')
            #     now_str = now.isoformat()

            #     # Gera um .csv direto na memoria
            #     string_buffer = StringIO() 
            #     os_df.to_csv(string_buffer, index=False, sep=',')
            #     string_buffer = bytes(string_buffer.getvalue(), encoding='utf8')
            #     os_filename = f'data={today_str}/os-{st.session_state["username"]}-{now_str}.csv'
            #     stringio_os = BytesIO(string_buffer)
            #     upload_to_gcs(os_filename, stringio_os)

            #     gtfs_filename = f'data={today_str}/gtfs-{st.session_state["username"]}-{now_str}.zip'
            #     stringio_gtfs = change_feed_info_dates(gtfs_file.getvalue(), os_initial_date, os_final_date)
            #     stringio_gtfs = BytesIO(stringio_gtfs.getvalue())
            #     upload_to_gcs(gtfs_filename, stringio_gtfs)

            #     st.write('Enviado')

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



