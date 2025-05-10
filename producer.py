# producer_ml_anomalies.py
from kafka import KafkaProducer
import pandas as pd
from sklearn.ensemble import IsolationForest
import time

# Configuration Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Chargement des données
data = pd.read_csv('data_To.csv')
features = data[['temperature', 'pressure', 'vibration', 'humidity']]

# Entraînement du modèle
model = IsolationForest(contamination=0.1, random_state=42)
model.fit(features)

# Détection des anomalies
data['anomaly'] = model.predict(features)

for index, row in data.iterrows():
    if row['anomaly'] == -1:  # -1 indique une anomalie
        line = f"{row['timestamp']},{row['temperature']},{row['pressure']}," \
               f"{row['vibration']},{row['humidity']},{row['equipment']},{row['location']}"
        producer.send('iot-data', value=line.encode('utf-8'))
        print(f"Sent ML-detected anomaly: {line}")
        time.sleep(1)
