# ==============================================================
# 🚗 Análisis en lote y tiempo real del tráfico vehicular en Bogotá
# Autor: Tatiana Vanegas
# ==============================================================
# Tecnologías: PySpark, Kafka, Parquet, JSON
# ==============================================================
# Este script contiene:
#   1. Procesamiento Batch de datos históricos de tráfico
#   2. Procesamiento Streaming (en tiempo real) desde Kafka
# ==============================================================

# ===================== IMPORTACIONES ==========================
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, avg, count, from_json, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# ===================== CONFIGURAR SPARK ========================
spark = SparkSession.builder \
    .appName("AnalisisTraficoBogota") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==============================================================
# 🧩 1. PROCESAMIENTO BATCH (Lote)
# ==============================================================

# ---- Cargar datos históricos (CSV) ----
ruta_csv = "data/trafico_bogota_historico.csv"  # ejemplo de ruta
df_trafico = spark.read.option("header", True).option("inferSchema", True).csv(ruta_csv)

print("✅ Datos históricos cargados:")
df_trafico.show(5)

# ---- Conversión de tipos ----
df_trafico = df_trafico.withColumn("fecha_hora", col("fecha_hora").cast(TimestampType()))
df_trafico = df_trafico.withColumn("velocidad_promedio", col("velocidad_promedio").cast(DoubleType()))

# ---- Limpieza: eliminar valores nulos o inconsistentes ----
df_trafico = df_trafico.dropna(subset=["fecha_hora", "localidad", "velocidad_promedio"])

# ---- Función para clasificar niveles de congestión ----
def clasificar_congestion(df, velocidad_col='velocidad_promedio'):
    """
    Clasifica el nivel de congestión basado en la velocidad promedio.
    - Bajo: >= 30 km/h
    - Medio: entre 15 y 30 km/h
    - Alto: < 15 km/h
    """
    return df.withColumn(
        'nivel_congestion',
        when(df[velocidad_col] >= 30, 'Bajo')
        .when((df[velocidad_col] >= 15) & (df[velocidad_col] < 30), 'Medio')
        .otherwise('Alto')
    )

df_trafico = clasificar_congestion(df_trafico)

# ---- Agregaciones por hora y localidad ----
df_agrupado = df_trafico.groupBy("localidad", window("fecha_hora", "1 hour")) \
    .agg(
        avg("velocidad_promedio").alias("velocidad_media"),
        count("*").alias("total_registros")
    )

print("✅ Datos agregados por hora y localidad:")
df_agrupado.show(5)

# ---- Almacenamiento en formato Parquet ----
df_agrupado.write.mode("overwrite").parquet("output/trafico_bogota_batch.parquet")
print("✅ Resultados batch guardados en Parquet.")

# ==============================================================
# ⚡ 2. PROCESAMIENTO EN TIEMPO REAL (Streaming con Kafka)
# ==============================================================

# ---- Esquema de los datos entrantes ----
schema_stream = StructType([
    StructField("fecha_hora", TimestampType(), True),
    StructField("localidad", StringType(), True),
    StructField("velocidad_promedio", DoubleType(), True)
])

# ---- Leer el stream desde Kafka ----
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "trafico_bogota") \
    .option("startingOffsets", "latest") \
    .load()

# ---- Convertir el valor binario en JSON legible ----
df_stream_values = df_stream.selectExpr("CAST(value AS STRING) as json")

df_datos = df_stream_values.select(from_json(col("json"), schema_stream).alias("data")).select("data.*")

# ---- Clasificar la congestión en tiempo real ----
df_datos_clasificado = clasificar_congestion(df_datos)

# ---- Calcular promedios por ventana de 1 minuto ----
df_resultado = df_datos_clasificado.groupBy(
    window(col("fecha_hora"), "1 minute"),
    col("localidad")
).agg(
    avg("velocidad_promedio").alias("velocidad_media"),
    count("*").alias("total_eventos")
)

# ---- Mostrar resultados en consola ----
consulta = df_resultado.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

print("  Streaming iniciado... Esperando datos de Kafka en el topic 'trafico_bogota'.")

consulta.awaitTermination()

# ============================================================== 
# FIN DEL SCRIPT
# ============================================================== 



