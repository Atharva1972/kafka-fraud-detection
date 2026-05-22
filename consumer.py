import json
from kafka import KafkaConsumer

# Connect to predictions topic
consumer = KafkaConsumer(
    'predictions',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("=" * 50)
print("  Fraud Detection - Live Predictions Console")
print("=" * 50)
print("Waiting for predictions...\n")

for message in consumer:
    result = message.value
    label = result.get('label', 'UNKNOWN')
    amount = result.get('Amount', 0)
    prob = result.get('fraud_probability', 0)
    prediction = result.get('prediction', -1)

    print(f"[{'FRAUD' if prediction == 1 else 'OK'}] "
          f"Amount: ${amount:.2f} | "
          f"Label: {label} | "
          f"Fraud Probability: {prob:.2%}")