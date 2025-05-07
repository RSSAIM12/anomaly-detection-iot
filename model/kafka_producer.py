import csv
import time
from kafka import KafkaProducer
import json

# إعداد Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# فتح ملف CSV وقراءة البيانات سطر بسطر
with open('data_To.csv', 'r') as file:
    reader = csv.DictReader(file)
    
    for row in reader:
        # تحويل القيم لأرقام فحال temperature etc
        data = {
            "timestamp": row["timestamp"],
            "temperature": float(row["temperature"]),
            "pressure": float(row["pressure"]),
            "vibration": float(row["vibration"]),
            "humidity": float(row["humidity"]),
            "equipment": row["equipment"],
            "location": row["location"],
            "faulty": float(row["faulty"])
        }

        # إرسال البيانات إلى Kafka
        producer.send('iot-topic', value=data)
        print("Sent:", data)

        # تأخير باش تحاكي streaming حقيقي
        time.sleep(1)

producer.flush()
producer.close()
