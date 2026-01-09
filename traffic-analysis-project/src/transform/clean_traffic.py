from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, coalesce, count, sum, make_date

spark = SparkSession.builder.appName("clean_traffic").getOrCreate()

df_raw = spark.read.parquet('data/raw/traffic/tmas/ny/2020/tmas_traffic_ny_2020.parquet', header=True, inferSchema=True)

print("\nEsquema inicial:")
print(df_raw.show(5))
print("\nEstructura:")
print(df_raw.printSchema())



# Eliminamos ruido: 
print("Eliminando ruido...")
df_drop_col = df_raw.drop(
    'unnamed_column',
    'state_cd',
    'record_type',
    'fsystem_cd',
    'direction',
    'lane',
    'restrictions'
)

# Validación de nulos
print("Validación de nulos...")
n=0
for columna in df_drop_col.columns:
    nulls = df_drop_col.filter(col(columna).isNull()).count()
    print(f"Nulos en {columna}: ", nulls)
    n += nulls

print(f"\nNulos totales: {n}")
if n > 0:
    print("Se requiere un tratamiento de valores nulos (imputación, eliminación, etc.)")
else:
    print("No se encontró ningún valor nulo en los campos iterados, por lo que, no es necesario realizar imputaciones o filtrado.")

# Análisis de duplicados
print("\nAnálisis de duplicados...")
print("Registros duplicados:")
df_dup = df_drop_col.groupBy(df_drop_col.columns).count().filter(col('count') > 1).show(10)

print(""" 
      NOTA IMPORTANTE:
      En TMAS, múltiples registros pueden existir para la misma estación,
      fecha y hora debido a reportes por carril o dirección.
      Estos registros no representan duplicados erróneos, sino conteos parciales.
      Por lo tanto, se agregan los registros sumando veh_count para obtener
      el volumen real de tráfico a nivel estación-hora.
""")

df_agg = (
    df_drop_col
    .groupBy("station_id", "rural_urban","year", "month", "day","day_of_week","week_day" , "hours")
    .agg(
        sum("veh_count").alias("veh_count")
    )
)

# Creamos columa temporal ('date'):
df_temp = df_agg.withColumn('year', col('year')+ 2000)
df_temp = df_temp.withColumn('date', make_date(col('year'),col('month'), col('day')))

# Ordenamos
df_temp = df_temp.select('station_id', 'rural_urban', 'date', 'year', 'month', 'day', 'day_of_week', 'week_day', 'hours', 'veh_count')
df_temp= df_temp.orderBy([col('station_id'), col('month'), col('day'), col('hours')])
df_temp.show(5)

df_temp.write.mode("overwrite").parquet("data/processed/traffic/tmas/ny/2020")
