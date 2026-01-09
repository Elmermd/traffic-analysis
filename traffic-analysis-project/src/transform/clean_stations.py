from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('transform_stations').getOrCreate()

df_raw = spark.read.csv(
    'data/raw/stations/tmas/tmas_stations.csv',
    header=True,
    inferSchema=True
)

print("Esquema inicial:")
print(df_raw.show(5))
print("\nEstructura:")
print(df_raw.printSchema())

# Eliminamos ruido
print("Eliminando ruido...")
df_drop_col = df_raw.drop(
    'OBJECTID',
    'state_fips',
    'functional_class',
    'x',
    'y'
)

# Transformamos tipos
print("Transformando tipos de datos...")
df_dtypes = df_drop_col.withColumn(
    'station_id',
    col('station_id').cast("long")
)

# Filtramos NY
print("Filtrando por NY...")
df_filter = df_dtypes.filter(col('state') == 'NY')

# Identificamos, analizamos y eliminamos duplicados
print("Análisis de duplicados...")
print("Registros duplicados:")
df_dup = df_filter.groupBy(df_filter.columns).count().filter(col('count') > 1).show()

df_nondup = df_filter.dropDuplicates()
print("Registros duplicados eliminados.")

# Validación de nulos
print("Validación de nulos...")
for columna in df_nondup.columns:
    print(f"Nulos en {columna}: ", df_nondup.filter(col(columna).isNull()).count())

# Guardado como Parquet

df_nondup.write.mode("overwrite").parquet("data/processed/stations/tmas/ny")

print("\nStations NY guardadas en Parquet.")
print("\nEsquema:")
print(df_nondup.show(5))
print("\nEstructura:")
print(df_nondup.printSchema())



