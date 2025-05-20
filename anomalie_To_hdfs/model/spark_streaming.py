from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import from_json, col ,current_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, StringType




# إنشاء SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingAnomalyDetection") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# تحميل الموديل المدرب
model = PipelineModel.load("model/")

# Schema ديال الداتا الجاية من Kafka
schema = StructType([
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType())
])

# قراءة Stream من Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "iot-topic") \
    .option("startingOffsets", "latest") \
    .load()

# تحويل القيمة إلى JSON
value_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")


# تطبيق الموديل
features_df = model.transform(value_df)

# استخراج Anomalies
anomalies = features_df.filter(features_df.prediction == 1)\
    .withColumn("timestamp", current_timestamp())

# كتابة النتائج فـ ملف temporary باش web_app يقرأه
query = anomalies.select("temperature", "humidity", "timestamp") \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "anomalies_output/") \
    .option("checkpointLocation", "checkpoint/") \
    .option("startingOffsets", "latest")\
    .start()

query.awaitTermination()
