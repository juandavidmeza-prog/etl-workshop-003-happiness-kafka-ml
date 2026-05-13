# ============================================================
# Workshop 003 - ETL Process using Apache Kafka + Machine Learning
# Step 06: Kafka Consumer, model prediction and database load
# ============================================================

import os
import json
import sqlite3
import joblib
import pandas as pd

from kafka import KafkaConsumer


# ------------------------------------------------------------
# 1. Define paths and Kafka configuration
# ------------------------------------------------------------

MODELS_PATH = "models"
DATA_PATH = "data"

MODEL_FILE = os.path.join(MODELS_PATH, "happiness_model.pkl")
DATABASE_FILE = os.path.join(DATA_PATH, "happiness_predictions.db")

KAFKA_TOPIC = "happiness_topic"
KAFKA_SERVER = "localhost:9092"


# ------------------------------------------------------------
# 2. Load trained model
# ------------------------------------------------------------

def load_model():
    """
    This function loads the trained model package.

    The package contains:
    - the trained regression model
    - the feature columns used during training
    """

    model_package = joblib.load(MODEL_FILE)

    model = model_package["model"]
    feature_columns = model_package["features"]

    print("Model loaded successfully.")
    print("Features used by the model:")
    print(feature_columns)

    return model, feature_columns


# ------------------------------------------------------------
# 3. Create database and table
# ------------------------------------------------------------

def create_database():
    """
    This function creates the SQLite database and predictions table.
    If the table already exists, it keeps it.
    """

    connection = sqlite3.connect(DATABASE_FILE)
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS happiness_predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gdp_per_capita REAL,
            social_support REAL,
            life_expectancy REAL,
            freedom REAL,
            generosity REAL,
            government_corruption REAL,
            year INTEGER,
            actual_happiness_score REAL,
            predicted_happiness_score REAL
        )
        """
    )

    connection.commit()
    connection.close()

    print("Database and table ready:")
    print(DATABASE_FILE)


# ------------------------------------------------------------
# 4. Create Kafka consumer
# ------------------------------------------------------------

def create_consumer():
    """
    This function creates a Kafka Consumer that listens to happiness_topic.
    The value_deserializer converts JSON bytes back into a Python dictionary.
    """

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="happiness_prediction_group",
        value_deserializer=lambda value: json.loads(value.decode("utf-8"))
    )

    print("Kafka Consumer created successfully.")
    print(f"Listening to topic: {KAFKA_TOPIC}")

    return consumer


# ------------------------------------------------------------
# 5. Save prediction into database
# ------------------------------------------------------------

def save_prediction(record, prediction):
    """
    This function inserts one prediction result into SQLite.
    """

    connection = sqlite3.connect(DATABASE_FILE)
    cursor = connection.cursor()

    cursor.execute(
        """
        INSERT INTO happiness_predictions (
            gdp_per_capita,
            social_support,
            life_expectancy,
            freedom,
            generosity,
            government_corruption,
            year,
            actual_happiness_score,
            predicted_happiness_score
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record["gdp_per_capita"],
            record["social_support"],
            record["life_expectancy"],
            record["freedom"],
            record["generosity"],
            record["government_corruption"],
            int(record["year"]),
            record["actual_happiness_score"],
            float(prediction)
        )
    )

    connection.commit()
    connection.close()


# ------------------------------------------------------------
# 6. Consume messages and predict
# ------------------------------------------------------------

def consume_and_predict(consumer, model, feature_columns):
    """
    This function receives records from Kafka, predicts the happiness score,
    and saves the result in the database.
    """

    print("\nWaiting for messages...")
    print("Press Ctrl + C to stop the consumer.\n")

    try:
        for message in consumer:
            record = message.value

            input_data = pd.DataFrame([record])[feature_columns]

            prediction = model.predict(input_data)[0]

            save_prediction(record, prediction)

            print("Record received and saved.")
            print("Actual happiness score:", record["actual_happiness_score"])
            print("Predicted happiness score:", round(float(prediction), 4))
            print("-" * 50)

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")


# ------------------------------------------------------------
# 7. Main execution
# ------------------------------------------------------------

if __name__ == "__main__":
    trained_model, model_features = load_model()
    create_database()
    kafka_consumer = create_consumer()
    consume_and_predict(kafka_consumer, trained_model, model_features)