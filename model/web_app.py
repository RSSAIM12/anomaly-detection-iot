import os
import pandas as pd
from flask import Flask, render_template_string
from flask_socketio import SocketIO, emit

app = Flask(__name__)
socketio = SocketIO(app)

@app.route('/')
def home():
    html = '''
    <head>
      <script src="https://cdn.socket.io/4.4.1/socket.io.min.js"></script>
      <script>
        var socket = io();
        socket.on('new_data', function(data) {
          var table = document.getElementById('anomaly-table');
          var row = table.insertRow();
          var cell1 = row.insertCell(0);
          var cell2 = row.insertCell(1);
          var cell3 = row.insertCell(2);
          cell1.innerHTML = data.temperature;
          cell2.innerHTML = data.humidity;
          cell3.innerHTML = data.timestamp;
        });
      </script>
    </head>
    <body>
    <h1>Detected Anomalies</h1>
    <table border="1" id="anomaly-table">
      <tr>
        <th>Temperature</th>
        <th>Humidity</th>
        <th>Temps</th>
      </tr>
    </table>
    </body>
    '''
    return render_template_string(html)

def monitor_anomalies():
    anomalies_folder = 'hdfs://localhost:9000/user/sanaa/anomalies_output/'
    processed_files = set()

    while True:
        if os.path.exists(anomalies_folder):
            for filename in os.listdir(anomalies_folder):
                if filename.endswith(".csv") and filename not in processed_files:
                    filepath = os.path.join(anomalies_folder, filename)
                    if os.path.getsize(filepath) > 0:
                        try:
                            df = pd.read_csv(filepath, header=None, names=["temperature", "humidity", "timestamp"])

                            for index, row in df.iterrows():
                                socketio.emit('new_data', {
                                    'temperature': row['temperature'],
                                    'humidity': row['humidity'],
                                    'timestamp': row['timestamp']
                                })
                        except Exception as e:
                            print(f"Error reading {filename}: {e}")
                        processed_files.add(filename)
        socketio.sleep(5)  # كل 5 ثواني تعاود تشيك على ملفات جدد
  # كل 5 ثواني تعاود تشيك على ملفات جدد

if __name__ == '__main__':
    socketio.start_background_task(monitor_anomalies)
    socketio.run(app, host="0.0.0.0", port=5000)
