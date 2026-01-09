from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum,avg,to_date

spark = SparkSession.builder.appName('clean_weather').getOrCreate()

df_raw = spark.read.parquet('data/raw/weather/openmeteo/ny/2020/weather_ny_daily_2020.parquet', header=True, inferSchema=True)
df_raw.show()
df_raw.printSchema()

n = 0
for columna in df_raw.columns:
    nulls = df_raw.filter(col(columna).isNull()).count()
    print(f"Nulos en {columna}: ", nulls)
    n += nulls

print(F"\nRegistros totales: {df_raw.count()}")
print(f"Nulos totales: {n}")
print(f"Porcentaje de datos integros: {100-(n*100/df_raw.count())}")

# Análisis de duplicados
print("\nAnálisis de duplicados...")
print("Registros duplicados:")
df_dup = df_raw.groupBy(df_raw.columns).count().filter(col('count') > 1)
df_dup.show(5)

df_clean = df_raw

# Guardado como Parquet
df_clean.write.mode("overwrite").parquet("data/processed/weather/ny/2020")

print("\nDatos climáticos de NY guardados en Parquet.")
print("\nEsquema:")
print(df_clean.show(5))
print("\nEstructura:")
print(df_clean.printSchema())
