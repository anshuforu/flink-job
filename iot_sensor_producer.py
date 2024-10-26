from kafka import KafkaProducer
import json
import time
import random

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['<bootstrap-broker-endpoint>'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Simulate IoT sensor data
def generate_sensor_data():
    return {
        'device_id': f'device_{random.randint(1, 100)}',
        'temperature': random.uniform(20.0, 100.0),
        'pressure': random.uniform(800, 1200),
        'humidity': random.uniform(30, 90),
        'timestamp': time.time()
    }

if __name__ == "__main__":
    while True:
        sensor_data = generate_sensor_data()
        producer.send('iot-sensor-topic', sensor_data)
        print(f'Sent: {sensor_data}')
        time.sleep(1)  # Sending data every second
