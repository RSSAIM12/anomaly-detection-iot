from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

# Créer une session Spark
spark = SparkSession.builder \
    .appName("KafkaToHDFS") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("INFO")


spark.sparkContext.setLogLevel("WARN")

# Lire les messages depuis Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "iot-data") \
    .option("startingOffsets", "latest") \
    .load()

# Parser les données
df_parsed = df.selectExpr("CAST(value AS STRING)").select(
    split(col("value"), ",").alias("fields")
).select(
    col("fields")[0].alias("timestamp"),
    col("fields")[1].cast("float").alias("temperature"),
    col("fields")[2].cast("float").alias("pressure"),
    col("fields")[3].cast("float").alias("vibration"),
    col("fields")[4].cast("float").alias("humidity"),
    col("fields")[5].alias("equipment"),
    col("fields")[6].alias("location")
)

# Afficher le schéma pour vérification
df_parsed.printSchema()

# Écriture dans HDFS (corrigé avec ton user "fatma")
query_hdfs = df_parsed.writeStream \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/user/fatma/iot_data/") \
    .option("checkpointLocation", "hdfs://localhost:9000/user/fatma/iot_checkpoint/") \
    .outputMode("append") \
    .start()

# Affichage dans la console pour débogage
query_console = df_parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query_hdfs.awaitTermination()
query_console.awaitTermination()
