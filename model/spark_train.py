from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.sql.functions import when

# إنشاء SparkSession
spark = SparkSession.builder.appName("TrainAnomalyModel").getOrCreate()

# قراءة ملف CSV
data = spark.read.csv('data_To.csv', header=True, inferSchema=True)

# تحضير الأعمدة
data = data.withColumn("temperature", data['temperature'].cast('double'))\
           .withColumn("humidity", data['humidity'].cast('double'))

# إنشاء label: إذا temperature > 100 ==> 1 else 0
data = data.withColumn("label", when(data.temperature > 100, 1).otherwise(0))

# تجميع الخصائص
assembler = VectorAssembler(inputCols=["temperature", "humidity"], outputCol="features")

# نموذج Random Forest
rf = RandomForestClassifier(featuresCol='features', labelCol='label')

# بناء Pipeline
pipeline = Pipeline(stages=[assembler, rf])

# تدريب النموذج
model = pipeline.fit(data)

# حفظ النموذج
model.write().overwrite().save("model/")

print("✅ Model trained and saved successfully!")
spark.stop()
