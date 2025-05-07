from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, StringType

# === NOUVEAU ===
from influxdb import InfluxDBClient

# === Création SparkSession ===
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingAnomalyDetection") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# === Chargement du modèle entraîné ===
model = PipelineModel.load("model/")

# === Définition du schéma Kafka ===
# === Définition du schéma Kafka ===
schema = StructType([
    StructField("timestamp", StringType()),  # إذا كانت جاية من CSV
    StructField("temperature", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("vibration", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("equipment", StringType()),
    StructField("location", StringType()),
    StructField("faulty", DoubleType())
])



# === Lecture du flux Kafka ===
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "iot-topic") \
    .option("startingOffsets", "latest") \
    .load()

# === Conversion JSON + sélection colonnes ===
value_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# === Application du modèle ===
features_df = model.transform(value_df)

# === Extraction des anomalies ===
anomalies = features_df.filter(features_df.prediction == 1) \
    .withColumn("timestamp", current_timestamp())

# === 🔥 Fonction pour écrire vers InfluxDB ===
def save_to_influxdb(df, epoch_id):
    client = InfluxDBClient(host='localhost', port=8086, database='iot_db')
    json_body = []
    for row in df.toLocalIterator():
        print(row) 
        json_body.append({
            "measurement": "anomalies",
            "fields": {
                "temperature": row["temperature"],
                "pressure": row["pressure"],
                "vibration": row["vibration"],
                "humidity": row["humidity"],
                "equipment":row["equipment"],
                "faulty": row["faulty"],
                "location":row["location"]
            },
            "time": row["timestamp"].isoformat()
        })

    client.write_points(json_body)
    client.close()

# === Écriture vers InfluxDB (streaming) ===
# الكتابة الأولى: نحو InfluxDB
anomalies.drop("features", "rawPrediction", "probability").writeStream \
    .foreachBatch(save_to_influxdb) \
    .outputMode("append") \
    .option("checkpointLocation", "hdfs://localhost:9000/user/sanaa/checkpoint_influx/") \
    .start()

# الكتابة الثانية: نحو HDFS (بصيغة CSV)
# نحيدو العمود "features" قبل الكتابة
anomalies.drop("features", "rawPrediction", "probability").writeStream \
    .format("csv") \
    .option("path", "hdfs://localhost:9000/user/sanaa/anomalies_output/") \
    .option("checkpointLocation", "hdfs://localhost:9000/user/sanaa/checkpoint_hdfs/") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
# === Vider InfluxDB measurement ===

