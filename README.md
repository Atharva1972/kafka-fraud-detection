# Real-Time Credit Card Fraud Detection with Apache Kafka + Faust

## Dataset
- **Name:** Credit Card Fraud Detection
- **Source:** https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud
- **Task:** Classify transactions as fraudulent (1) or legitimate (0)

## Streams Library
- **Python + Faust** (faust-streaming 0.11.3)

## ML Model
- **Algorithm:** Logistic Regression (scikit-learn)
- **Trained offline** in `train_model.ipynb`
- **Saved as:** `fraud_model.joblib`
- **Accuracy:** ~99%
- **F1 Score (fraud class):** ~0.73

## Setup

### Requirements
- Python 3.11
- Docker Desktop

### Install dependencies
```bash
python -m venv venv
venv\Scripts\activate
pip install faust-streaming scikit-learn pandas kafka-python joblib aiokafka==0.11.0
```

### Start Kafka
```bash
docker-compose up -d
```

## How to Run

Open 3 terminals, activate venv in each, then:

**Terminal 1 — Faust Streams Processor:**
```bash
python -m faust -A processor worker -l info
```

**Terminal 2 — Output Consumer:**
```bash
python consumer.py
```

**Terminal 3 — Producer:**
```bash
python producer.py
```

## Components
- `producer.py` — Reads CSV rows, publishes to `raw-data` topic at 1 row/second
- `processor.py` — Faust agent consumes `raw-data`, runs ML model, publishes to `predictions`
- `consumer.py` — Reads `predictions` topic and prints results to console

## Video Demo
https://youtu.be/1baaUbIdeIk