from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('join_traffic_stations').getOrCreate()

df_traffic = spark.read.parquet(
    "data/processed/traffic/tmas/ny/2020"
)

df_stations = spark.read.parquet(
    "data/processed/stations/tmas/ny"
)

df = df_traffic.join(df_stations, on='station_id', how='left')
# Ordenamos el dataset
df = df.select('station_id', 'latitude', 'longitude', 'rural_urban', 'date', 'year', 'month', 'day', 'day_of_week', 'week_day', 'hours', 'veh_count')

# Verificamos integridad de los datos: 
n = 0
for columna in df.columns:
    nulls = df.filter(col(columna).isNull()).count()
    print(f"Nulos en {columna}: ", nulls)
    n += nulls

print(F"\nRegistros totales: {df.count()}")
print(f"Nulos totales: {n}")
print(f"Porcentaje de datos integros: {100-(n*100/df.count())}")

# Manejo de datos espaciales faltantes:
# Después de unir el tráfico con los metadatos de las estaciones, algunos registros no tienen
# latitud o longitud porque no todas las estaciones TMAS están geolocalizadas.
# Dado que las coordenadas espaciales son necesarias para el análisis geoespacial y la cartografía,
# los registros sin latitud/longitud se excluyen del conjunto de datos final.

df_geo = df.filter((col("latitude").isNotNull()) & (col("longitude").isNotNull()))

df.select("station_id").distinct().count()
df_geo.select("station_id").distinct().count()

print(f"""
De las 166 estaciones de monitoreo de tráfico, 144 tenían coordenadas geográficas válidas.
Las estaciones sin latitud/longitud fueron excluidas del conjunto de datos geoespacial, representando aproximadamente el 13% de las estaciones. Esta decisión fue tomada para 
garantizar un análisis espacial preciso.
""")

# Verificamos integridad de los datos: 
n = 0
for columna in df_geo.columns:
    nulls = df_geo.filter(col(columna).isNull()).count()
    print(f"Nulos en {columna}: ", nulls)
    n += nulls

print(F"\nRegistros totales: {df_geo.count()}")
print(f"Nulos totales: {n}")
print(f"Porcentaje de datos integros: {100-(n*100/df_geo.count())}")

df_geo.write \
    .mode("overwrite") \
    .parquet("data/curated/traffic_enriched/ny/2020")
