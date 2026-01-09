import pandas as pd
from pathlib import Path

# --- CONFIGURACIÓN ---
# URL de la API de colisiones de vehículos motorizados de NYC
BASE_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.csv"
LIMIT = 50000

def fetch_nyc_collisions_2020():
    """
    Extrae los datos de colisiones de tráfico en NYC para el año 2020
    utilizando paginación para manejar el volumen de datos.
    """
    offset = 0
    all_chunks = []
    n = 1
   
   # WHERE clause ya URL-encoded
    where_encoded = (
    "crash_date%20between%20"
    "'2020-01-01T00:00:00.000'%20and%20"
    "'2020-12-31T23:59:59.999'"
    )

    while True:
        # Consulta filtrando por el rango de fechas de 2020
        query_url = (
        f"{BASE_URL}"
        f"?$where={where_encoded}"
        f"&$limit={LIMIT}"
        f"&$offset={offset}"
        )

        print(f"Procesando chunk {n}")
        df_chunk = pd.read_csv(query_url)
        n += 1

        if df_chunk.empty:
            break

        all_chunks.append(df_chunk)
        offset += LIMIT

    print("Uniendo fragmentos de datos...")
    return pd.concat(all_chunks, ignore_index=True)

# --- PUNTO DE ENTRADA PRINCIPAL ---
if __name__ == "__main__":

    df_collisions = fetch_nyc_collisions_2020()
    # Verificación rápida de la integridad de los datos
    print("\nRESUMEN DE DATOS")
    print(f"Total de registros: {len(df_collisions)}")
    print("Vista general:")
    print(df_collisions.head(5))

    df_collisions.to_parquet("data/raw/incidents/nyc/2020/nyc_motor_vehicle_collisions_2020.parquet", index=False)
    print(f"Archivo guardado exitosamente")