from pyspark.sql import SparkSession
import sqlite3

# 1. إنشاء SparkSession
spark = SparkSession.builder \
    .appName("ExportAnomaliesToSQLite") \
    .getOrCreate()

# 2. قراءة البيانات من HDFS (بدون header)
df = spark.read.option("header", False).csv("hdfs://localhost:9000/user/sanaa/anomalies_output/")

# 3. تسمية الأعمدة
df = df.toDF("timestamp", "temperature", "pressure", "vibration", "humidity", "equipment", "location", "faulty")

# 4. تحويل إلى DataFrame في Pandas
pandas_df = df.toPandas()

# 5. تصحيح الأنواع (optional)
pandas_df["temperature"] = pandas_df["temperature"].astype(float)
pandas_df["pressure"] = pandas_df["pressure"].astype(float)
pandas_df["vibration"] = pandas_df["vibration"].astype(float)
pandas_df["humidity"] = pandas_df["humidity"].astype(float)
pandas_df["faulty"] = pandas_df["faulty"].astype(float)

# 6. الاتصال بـ SQLite
conn = sqlite3.connect("anomalies.db")

# 7. كتابة البيانات في جدول SQLite
pandas_df.to_sql("anomalies", conn, if_exists="append", index=False)

# 8. إغلاق الاتصال
conn.close()
