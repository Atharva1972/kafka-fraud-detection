import pandas as pd
import json
import time
from kafka import KafkaProducer

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load dataset
df = pd.read_csv('creditcard.csv')

print(f"Starting producer... sending {len(df)} rows to 'raw-data' topic")
print("Press Ctrl+C to stop\n")

# Send each row as a Kafka message
for index, row in df.iterrows():
    message = row.to_dict()
    producer.send('raw-data', value=message)
    print(f"Sent row {index} | Amount: ${message['Amount']:.2f} | Class: {int(message['Class'])}")
    time.sleep(1)  # 1 row per second

producer.flush()
print("Done!")