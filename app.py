import streamlit as st
import filecmp
import re
import pandas as pd
from datetime import timedelta

# TODO: Add an authenticator
# TODO: Add button to upload file to GCS

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

        if not check_os_filename(os_file):
            st.warning(":warning: O nome do arquivo OS não está no formato esperado!")
        else:
            st.success(":white_check_mark: O nome do arquivo OS está no formato esperada!")

            os_df = pd.read_csv(os_file)

            viagens_cols = ["Viagens Dia Útil","Viagens Sábado","Viagens Domingo","Viagens Ponto Facultativo"]
            km_cols = ["Quilometragem Dia Útil","Quilometragem Sábado","Quilometragem Domingo","Quilometragem Ponto Facultativo"]

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
                st.warning(":warning: O arquivo OS não contém as colunas esperadas!")
            
            else:
                st.success(":white_check_mark: O arquivo OS contém as colunas esperadas!")

                if not check_os_columns_order(os_df):
                    st.warning(f":warning: O arquivo OS contém as colunas esperadas, porém não segue a ordem esperada: {os_columns}")

                # Check dates
                st.subheader("Confirme por favor os itens abaixo:")

                os_initial_date = pd.to_datetime(os_file.name.split('_')[1].split(".")[0])
                check_initial_date = st.radio(
                    f"A data **inicial** de vigência da OS é **{os_initial_date.strftime('%d/%m/%Y')}**?",
                    ["Não", "Sim"],
                    index=None
                )

                os_final_date = os_initial_date + timedelta(days=15)
                check_final_date = st.radio(
                    f"A data **final** de vigência da OS é **{os_final_date.strftime('%d/%m/%Y')}**?",
                    ["Não", "Sim"],
                    index=None
                )

                if check_final_date == "Não":
                    os_final_date = st.date_input("Qual deve ser a data final de vigência da OS?", value=None)
                    if os_final_date:
                        check_final_date = "Sim"

                if check_initial_date == "Não":
                    st.warning("Verifique o arquivo enviado e tente novamente!")
                    
                
                # Check data
                if check_final_date == "Sim" and check_initial_date == "Sim":
                    st.subheader(":face_with_monocle: Ótimo! Verifique os dados antes de subir:")

                    # TODO: Partidas x Extensão, Serviços OS x GTFS (routes, trips, shapes), Extensão OS x GTFS"
                    
                    # Numero de servicos por consorcio
                    tb = pd.DataFrame(os_df.groupby("Consórcio")["Serviço"].count())
                    tb.loc["Total"] = tb.sum()
                    st.table(tb)

                    # Numero de viagens por consorcio
                    tb = pd.DataFrame(os_df.groupby("Consórcio")[viagens_cols].sum())
                    tb.loc["Total"] = tb.sum()
                    st.table(tb.style.format("{:.1f}"))

                    # Numero de KM por consorcio
                    tb = pd.DataFrame(os_df.groupby("Consórcio")[km_cols].sum())
                    tb.loc["Total"] = tb.sum()
                    st.table(tb.style.format("{:.3f}"))
    
if __name__ == "__main__":
    main()