from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, coalesce, count, sum, to_date

spark = SparkSession.builder.appName('clean_incidents').getOrCreate()

df_raw = spark.read.parquet('data/raw/incidents/nyc/2020/nyc_motor_vehicle_collisions_2020.parquet', header=True, inferSchema=True)
print("\nEstructura cruda:")
df_raw.printSchema()

# Seleccionamos las columnas útiles para el análisis, eliminando ruido en los datos: 
print("Eliminando ruido...")
df = df_raw.select('crash_date',
                   'crash_time',
                   'borough',
                   'latitude',
                   'longitude',
                   'number_of_persons_injured',
                   'number_of_persons_killed'
)
print("Ruido eliminado.")

print("\nEstandarización de columnas...")
df = df.withColumnRenamed('number_of_persons_injured','persons_injured')
df = df.withColumnRenamed('number_of_persons_killed','persons_killed')
print("Estandarización completada")

# Transformamos tipos
print("Transformando tipos de datos...")
df = df.withColumn(
    'date',
    to_date(col('crash_date'))
)
df = df.drop('crash_date')

print("Completado.")

print("\nEsquema de los datos: ")
df.show(5)

print("\nValidación de nulos...")
n=0
for columna in df.columns:
    nulls = df.filter(col(columna).isNull()).count()
    print(f"Nulos en {columna}: ", nulls)
    n += nulls

print(F"\nRegistros totales: {df.count()}")
print(f"Nulos totales: {n}")
print(f"Porcentaje de datos integros: {100-(n*100/df.count())}")

#Manejo de nulos:
#Los registros que carecen de latitud o longitud se eliminan, ya que la
#información espacial es necesaria para el análisis geoespacial y las
#uniones con datos de tráfico.
#Los valores nulos en campos no críticos (por ejemplo: borough, crash_time)
#se preservan.

print("\nEliminación de nulos críticos...")
df = df.filter((col('latitude').isNotNull()) & (col('longitude').isNotNull()))

n=0
for columna in df.columns:
    nulls = df.filter(col(columna).isNull()).count()
    print(f"Nulos en {columna}: ", nulls)
    n += nulls

print("Eliminación completada.")

print(f"\nRegistros totales: {df.count()}")
print(f"Nulos totales: {n}")

# Análisis de duplicados
print("\nAnálisis de duplicados...")
df_dup = df.groupBy(df.columns).count().filter(col('count') > 1)
df_dup.show(5)

# Manejo de duplicados:
# En el conjunto de datos de incidentes, cada registro representa un único evento de colisión.
# Por lo tanto, los registros idénticos indican duplicados reales.
# Se eliminan los duplicados exactos para evitar el conteo doble de incidentes.

df_nondup = df.dropDuplicates()
print("Registros duplicados eliminados correctamente.")


# Ordenamos
df_clean = df_nondup.orderBy([col('date'), col('crash_time')])

# -----------------------------------------------------------
# Write processed (domain-cleaned incidents)
# -----------------------------------------------------------
df_clean.write \
    .mode("overwrite") \
    .parquet("data/processed/incidents/ny/2020")

# -----------------------------------------------------------
# Write curated (geospatial-ready incidents)
# -----------------------------------------------------------
df_clean.write \
    .mode("overwrite") \
    .parquet("data/curated/incidents_geospatial/ny/2020")

# -----------------------------------------------------------
# Export curated geospatial incidents to CSV (Power BI)
# -----------------------------------------------------------
df_clean.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("data/curated/incidents_geospatial/ny/2020_csv")


df_daily = (
    df_clean
    .groupBy("date")
    .agg(
        count("*").alias("total_incidents"),
        sum("persons_injured").alias("total_injured"),
        sum("persons_killed").alias("total_killed")
    )
)

df_daily.write.mode("overwrite").parquet("data/processed/incidents/ny/2020/daily")

print("\nIncidents daily NY guardados en Parquet.")
print("\nEsquema:")
print(df_daily.show(5))
print("\nEstructura:")
print(df_daily.printSchema())
