import pandas as pd

# CONFIGURACIÓN DE LA API
# URL de la base de datos de transporte de EE.UU. (Socrata API)
BASE_URL = "https://data.transportation.gov/resource/ymmm-mwzp.csv"
STATE_NY = 36  # Código de estado para Nueva York
CHUNK_SIZE = 50000

def extract_ny_traffic_data():
    """
    Extrae todos los registros de tráfico para el estado de Nueva York
    usando paginación debido al límite de la API (1000).
    """
    offset = 0
    dfs = []
    n = 1

    print("Iniciando descarga de datos...")

    while True:
        # Construcción de la consulta usando SODA (Socrata Query Language)
        # Filtramos por state_cd=36 (NY) y manejamos chunks por el tamaño establecido
        query_url = (
            f"{BASE_URL}"
            f"?$where=state_cd={STATE_NY}"
            f"&$limit={CHUNK_SIZE}"
            f"&$offset={offset}"
        )

        try:
            df_chunk = pd.read_csv(query_url)
            
            # Si el dataframe está vacío, finalizamos con todos los datos registrados
            if df_chunk.empty:
                break

            dfs.append(df_chunk)
            print(f"Procesando chunk {n}")
            n += 1
            
            # Incrementamos el punto de inicio para la siguiente petición
            offset += CHUNK_SIZE
            
        except:
            print("Error al descargar los datos")
            break

    # Consolidamos todos los fragmentos en un solo DataFrame
    return pd.concat(dfs, ignore_index=True)

# EJECUCIÓN
if __name__ == "__main__":
    df_ny = extract_ny_traffic_data()

    # Verificación rápida de la integridad de los datos
    print("\nRESUMEN DE DATOS")
    print(f"Total de registros: {len(df_ny)}")
    print(f"Estados detectados: {df_ny['state_cd'].unique()}")
    print(f"Años cubiertos: {df_ny['year'].unique()}")

    df_ny.to_parquet("/home/elias/Escritorio/traffic-analysis/data/raw/traffic/tmas/ny/2020/tmas_traffic_ny_2020.parquet", index=False)

    # Importante instalar pyarrow, fastparquet
