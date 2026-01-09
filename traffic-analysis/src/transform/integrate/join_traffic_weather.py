from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('join_traffic_weather').getOrCreate()

df_traffic = spark.read.parquet('data/curated/traffic_enriched/ny/2020', header=True, inferSchema=True)
df_weather = spark.read.parquet('data/processed/weather/ny/2020', header=True, inferSchema=True)

df = df_traffic.join(df_weather, on='date', how='left')

df.show(5)

# Verificamos integridad de los datos: 
n = 0
for columna in df.columns:
    nulls = df.filter(col(columna).isNull()).count()
    print(f"Nulos en {columna}: ", nulls)
    n += nulls

print(F"\nRegistros totales: {df.count()}")
print(f"Nulos totales: {n}")
print(f"Porcentaje de datos integros: {100-(n*100/df.count())}")


df.write.mode("overwrite").parquet("data/curated/traffic_weather_enriched/ny/2020")