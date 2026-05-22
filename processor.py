import asyncio
asyncio.set_event_loop(asyncio.new_event_loop())

import faust
import joblib
import json
import numpy as np

# Load the pre-trained model
model = joblib.load('fraud_model.joblib')

# Define the Faust app
app = faust.App(
    'fraud-detector',
    broker='kafka://localhost:9092',
)

# Define input and output topics (no value_type — handle raw bytes)
raw_topic = app.topic('raw-data', value_serializer='raw')
predictions_topic = app.topic('predictions', value_serializer='raw')

# Define the Faust agent (Streams processor)
@app.agent(raw_topic)
async def detect_fraud(stream):
    async for message in stream:
        try:
            # Decode raw bytes to dict
            record = json.loads(message)

            # Extract features (drop Class column)
            features = {k: v for k, v in record.items() if k != 'Class'}
            feature_values = np.array(list(features.values())).reshape(1, -1)

            # Run ML prediction
            prediction = model.predict(feature_values)[0]
            probability = model.predict_proba(feature_values)[0][1]

            # Build output message
            result = {
                'Amount': record.get('Amount'),
                'prediction': int(prediction),
                'fraud_probability': round(float(probability), 4),
                'label': 'FRAUD' if prediction == 1 else 'LEGITIMATE'
            }

            # Send to predictions topic
            await predictions_topic.send(value=json.dumps(result).encode())
            print(f"Processed | Amount: ${result['Amount']:.2f} | {result['label']} | Prob: {result['fraud_probability']}")

        except Exception as e:
            print(f"Error processing record: {e}")

if __name__ == '__main__':
    app.main()