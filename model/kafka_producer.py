from kafka import KafkaProducer
import json
import time
import random

# إعداد Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    # توليد بيانات عشوائية
    data = {
        "temperature": round(random.uniform(80, 120), 2),
        "humidity": round(random.uniform(30, 90), 2)
    }
    # إرسال البيانات لـ Kafka
    producer.send('iot-topic', value=data)
    print(f"✅ Sent: {data}")
    
    # كل 2 ثانية صيفط داتا جديدة
    time.sleep(2)
