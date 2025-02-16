import os
import zipfile
import pandas as pd

# Directorios de entrada y salida
DIR_INPUT = "files/input/"
DIR_OUTPUT = "files/output/"

# Asegurar la existencia del directorio de salida
os.makedirs(DIR_OUTPUT, exist_ok=True)

def clean_campaign_data():
    """Extrae y procesa datos de campañas desde archivos comprimidos en ZIP."""

    # Obtener la lista de archivos ZIP en el directorio de entrada
    zip_archives = [archivo for archivo in os.listdir(DIR_INPUT) if archivo.endswith(".zip")]

    # Listas para almacenar los DataFrames procesados
    clientes = []
    campanas = []
    economia = []

    # Iterar sobre cada archivo ZIP
    for archivo_zip in zip_archives:
        with zipfile.ZipFile(os.path.join(DIR_INPUT, archivo_zip), 'r') as zip_ref:
            for archivo in zip_ref.namelist():
                if archivo.endswith(".csv"):
                    with zip_ref.open(archivo) as file:
                        data = pd.read_csv(file)

                        # Procesamiento de client.csv
                        df_clientes = data[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()

                        df_clientes['job'] = df_clientes['job'].str.replace(".", "", regex=False)
                        df_clientes['job'] = df_clientes['job'].str.replace("-", "_", regex=False)
                        df_clientes['education'] = df_clientes['education'].str.replace(".", "_", regex=False)
                        df_clientes['education'] = df_clientes['education'].replace("unknown", pd.NA)
                        df_clientes['credit_default'] = df_clientes['credit_default'].apply(lambda valor: 1 if valor == "yes" else 0)
                        df_clientes['mortgage'] = df_clientes['mortgage'].apply(lambda valor: 1 if valor == "yes" else 0)

                        clientes.append(df_clientes)

                        # Procesamiento de campaign.csv
                        df_campanas = data[['client_id', 'number_contacts', 'contact_duration',
                                            'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome',
                                            'day', 'month']].copy()

                        df_campanas['previous_outcome'] = df_campanas['previous_outcome'].apply(lambda valor: 1 if valor == "success" else 0)
                        df_campanas['campaign_outcome'] = df_campanas['campaign_outcome'].apply(lambda valor: 1 if valor == "yes" else 0)
                        df_campanas['last_contact_date'] = pd.to_datetime(
                            "2022-" + df_campanas['month'].astype(str) + "-" + df_campanas['day'].astype(str),
                            format="%Y-%b-%d",
                            errors="coerce"
                        ).dt.strftime("%Y-%m-%d")
                        df_campanas.drop(columns=['day', 'month'], inplace=True)

                        campanas.append(df_campanas)

                        # Procesamiento de economics.csv
                        df_economia = data[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
                        economia.append(df_economia)

    # Guardar los DataFrames resultantes en archivos CSV
    pd.concat(clientes).to_csv(os.path.join(DIR_OUTPUT, "client.csv"), index=False)
    pd.concat(campanas).to_csv(os.path.join(DIR_OUTPUT, "campaign.csv"), index=False)
    pd.concat(economia).to_csv(os.path.join(DIR_OUTPUT, "economics.csv"), index=False)
