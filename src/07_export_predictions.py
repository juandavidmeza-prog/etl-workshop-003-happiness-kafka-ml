# ============================================================
# Workshop 003 - ETL Process using Apache Kafka + Machine Learning
# Step 07: Export predictions from SQLite database
# ============================================================

import os
import sqlite3
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


# ------------------------------------------------------------
# 1. Define project paths
# ------------------------------------------------------------

DATA_PATH = "data"
REPORTS_PATH = "reports"

DATABASE_FILE = os.path.join(DATA_PATH, "happiness_predictions.db")
OUTPUT_FILE = os.path.join(REPORTS_PATH, "kafka_predictions_export.csv")
METRICS_FILE = os.path.join(REPORTS_PATH, "kafka_prediction_metrics.csv")


# ------------------------------------------------------------
# 2. Load predictions from SQLite
# ------------------------------------------------------------

def load_predictions():
    """
    This function reads all predictions stored by the Kafka Consumer
    from the SQLite database.
    """

    connection = sqlite3.connect(DATABASE_FILE)

    query = """
    SELECT
        gdp_per_capita,
        social_support,
        life_expectancy,
        freedom,
        generosity,
        government_corruption,
        year,
        actual_happiness_score,
        predicted_happiness_score
    FROM happiness_predictions
    """

    df = pd.read_sql_query(query, connection)

    connection.close()

    print("Predictions loaded successfully from SQLite.")
    print("Dataset shape:")
    print(df.shape)

    return df


# ------------------------------------------------------------
# 3. Calculate prediction metrics
# ------------------------------------------------------------

def calculate_metrics(df):
    """
    This function calculates the model performance using the values
    stored in the database.
    """

    y_actual = df["actual_happiness_score"]
    y_predicted = df["predicted_happiness_score"]

    mae = mean_absolute_error(y_actual, y_predicted)
    mse = mean_squared_error(y_actual, y_predicted)
    rmse = mse ** 0.5
    r2 = r2_score(y_actual, y_predicted)

    metrics_df = pd.DataFrame({
        "metric": ["MAE", "MSE", "RMSE", "R2 Score"],
        "value": [
            round(mae, 4),
            round(mse, 4),
            round(rmse, 4),
            round(r2, 4)
        ]
    })

    print("\nKafka prediction metrics:")
    print(metrics_df)

    return metrics_df


# ------------------------------------------------------------
# 4. Save outputs
# ------------------------------------------------------------

def save_outputs(df, metrics_df):
    """
    This function exports the database predictions and metrics to CSV files.
    """

    os.makedirs(REPORTS_PATH, exist_ok=True)

    df.to_csv(OUTPUT_FILE, index=False)
    metrics_df.to_csv(METRICS_FILE, index=False)

    print("\nPredictions exported successfully:")
    print(OUTPUT_FILE)

    print("\nMetrics exported successfully:")
    print(METRICS_FILE)


# ------------------------------------------------------------
# 5. Main execution
# ------------------------------------------------------------

if __name__ == "__main__":
    predictions_data = load_predictions()
    metrics = calculate_metrics(predictions_data)
    save_outputs(predictions_data, metrics)