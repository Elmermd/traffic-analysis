from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('join_traffic_weather_incidents').getOrCreate()

df_traffic_weather = spark.read.parquet('data/curated/traffic_weather_enriched/ny/2020', header=True, inferSchema=True)
df_daily_incidents = spark.read.parquet('data/processed/incidents/ny/2020/daily', header=True, inferSchema=True)

df = df_traffic_weather.join(df_daily_incidents, on='date', how='left')

# Verificamos integridad de los datos: 
n = 0
for columna in df.columns:
    nulls = df.filter(col(columna).isNull()).count()
    print(f"Nulos en {columna}: ", nulls)
    n += nulls

print(F"\nRegistros totales: {df.count()}")
print(f"Nulos totales: {n}")
print(f"Porcentaje de datos integros: {100-(n*100/df.count())}")

df.write.mode("overwrite").parquet("data/curated/traffic_weather_incidents/ny/2020")

# -----------------------------------------------------------
# Export final curated dataset to CSV (Power BI)
# -----------------------------------------------------------
df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("data/curated/traffic_weather_incidents/ny/2020_csv")


