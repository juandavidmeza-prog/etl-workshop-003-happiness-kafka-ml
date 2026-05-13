# ============================================================
# Workshop 003 - ETL Process using Apache Kafka + Machine Learning
# Step 05: Kafka Producer
# ============================================================

import os
import json
import time
import pandas as pd

from kafka import KafkaProducer


# ------------------------------------------------------------
# 1. Define project paths and Kafka configuration
# ------------------------------------------------------------

PROCESSED_DATA_PATH = "data/processed"
STREAM_FILE = os.path.join(PROCESSED_DATA_PATH, "happiness_stream_data.csv")

KAFKA_TOPIC = "happiness_topic"
KAFKA_SERVER = "localhost:9092"


# ------------------------------------------------------------
# 2. Create Kafka producer
# ------------------------------------------------------------

def create_producer():
    """
    This function creates a Kafka Producer.

    The value_serializer converts Python dictionaries into JSON bytes,
    because Kafka sends messages as bytes.
    """

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )

    print("Kafka Producer created successfully.")

    return producer


# ------------------------------------------------------------
# 3. Load stream data
# ------------------------------------------------------------

def load_stream_data():
    """
    This function loads the prepared testing data that will be streamed.
    """

    df = pd.read_csv(STREAM_FILE)

    print("Stream data loaded successfully.")
    print("Dataset shape:")
    print(df.shape)

    return df


# ------------------------------------------------------------
# 4. Send records to Kafka
# ------------------------------------------------------------

def send_records(producer, df):
    """
    This function sends each dataframe row as one Kafka message.
    """

    total_records = len(df)

    for index, row in df.iterrows():
        message = row.to_dict()

        producer.send(KAFKA_TOPIC, value=message)

        print(f"Sent record {index + 1}/{total_records}: {message}")

        # Small pause to simulate streaming data
        time.sleep(0.2)

    producer.flush()

    print("\nAll records were sent successfully.")


# ------------------------------------------------------------
# 5. Main execution
# ------------------------------------------------------------

if __name__ == "__main__":
    kafka_producer = create_producer()
    stream_data = load_stream_data()
    send_records(kafka_producer, stream_data)