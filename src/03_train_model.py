# ============================================================
# Workshop 003 - ETL Process using Apache Kafka + Machine Learning
# Step 03: Model training and evaluation
# ============================================================

import os
import json
import joblib
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


# ------------------------------------------------------------
# 1. Define project paths
# ------------------------------------------------------------

PROCESSED_DATA_PATH = "data/processed"
MODELS_PATH = "models"
REPORTS_PATH = "reports"

INPUT_FILE = os.path.join(PROCESSED_DATA_PATH, "happiness_clean.csv")
MODEL_FILE = os.path.join(MODELS_PATH, "happiness_model.pkl")
TEST_PREDICTIONS_FILE = os.path.join(REPORTS_PATH, "test_predictions.csv")
METRICS_FILE = os.path.join(REPORTS_PATH, "performance_metrics.json")


# ------------------------------------------------------------
# 2. Load clean dataset
# ------------------------------------------------------------

def load_data():
    """
    This function loads the clean dataset generated in the ETL process.
    """

    df = pd.read_csv(INPUT_FILE)

    print("Clean dataset loaded successfully.")
    print("Dataset shape:")
    print(df.shape)

    return df


# ------------------------------------------------------------
# 3. Select features and target
# ------------------------------------------------------------

def prepare_features(df):
    """
    This function selects the input features and the target variable.

    Important:
    happiness_rank is not used as a feature because it is highly related
    to happiness_score. Using it could create data leakage.
    """

    feature_columns = [
        "gdp_per_capita",
        "social_support",
        "life_expectancy",
        "freedom",
        "generosity",
        "government_corruption",
        "year"
    ]

    target_column = "happiness_score"

    X = df[feature_columns]
    y = df[target_column]

    print("\nSelected features:")
    print(feature_columns)

    print("\nTarget variable:")
    print(target_column)

    return X, y, feature_columns


# ------------------------------------------------------------
# 4. Split data into train and test sets
# ------------------------------------------------------------

def split_data(X, y):
    """
    This function splits the dataset into:
    - 70% training data
    - 30% testing data

    random_state is used to make the result reproducible.
    """

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.30,
        random_state=42
    )

    print("\nTrain-test split completed.")
    print("Training rows:", X_train.shape[0])
    print("Testing rows:", X_test.shape[0])

    return X_train, X_test, y_train, y_test


# ------------------------------------------------------------
# 5. Train regression model
# ------------------------------------------------------------

def train_model(X_train, y_train):
    """
    This function trains a Linear Regression model.
    Linear Regression was selected because it is simple, interpretable,
    and suitable as a first regression model for this workshop.
    """

    model = LinearRegression()
    model.fit(X_train, y_train)

    print("\nLinear Regression model trained successfully.")

    return model


# ------------------------------------------------------------
# 6. Evaluate model
# ------------------------------------------------------------

def evaluate_model(model, X_test, y_test):
    """
    This function evaluates the model using common regression metrics:
    - MAE: Mean Absolute Error
    - MSE: Mean Squared Error
    - RMSE: Root Mean Squared Error
    - R2 Score: proportion of variance explained by the model
    """

    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    rmse = mse ** 0.5
    r2 = r2_score(y_test, y_pred)

    metrics = {
        "model": "Linear Regression",
        "train_size_percentage": 70,
        "test_size_percentage": 30,
        "mae": round(mae, 4),
        "mse": round(mse, 4),
        "rmse": round(rmse, 4),
        "r2_score": round(r2, 4)
    }

    print("\nModel evaluation metrics:")
    print(metrics)

    return y_pred, metrics


# ------------------------------------------------------------
# 7. Save model
# ------------------------------------------------------------

def save_model(model, feature_columns):
    """
    This function saves the trained model as a .pkl file.
    The feature list is saved together with the model so the Kafka consumer
    knows exactly which columns must be used for prediction.
    """

    os.makedirs(MODELS_PATH, exist_ok=True)

    model_package = {
        "model": model,
        "features": feature_columns
    }

    joblib.dump(model_package, MODEL_FILE)

    print("\nModel saved successfully:")
    print(MODEL_FILE)


# ------------------------------------------------------------
# 8. Save metrics and test predictions
# ------------------------------------------------------------

def save_outputs(X_test, y_test, y_pred, metrics):
    """
    This function saves:
    - Model performance metrics as JSON
    - Test predictions as CSV
    """

    os.makedirs(REPORTS_PATH, exist_ok=True)

    # Save metrics
    with open(METRICS_FILE, "w") as file:
        json.dump(metrics, file, indent=4)

    print("\nPerformance metrics saved successfully:")
    print(METRICS_FILE)

    # Save predictions
    predictions_df = X_test.copy()
    predictions_df["actual_happiness_score"] = y_test.values
    predictions_df["predicted_happiness_score"] = y_pred

    predictions_df.to_csv(TEST_PREDICTIONS_FILE, index=False)

    print("\nTest predictions saved successfully:")
    print(TEST_PREDICTIONS_FILE)


# ------------------------------------------------------------
# 9. Main execution
# ------------------------------------------------------------

if __name__ == "__main__":
    happiness_data = load_data()

    X, y, feature_columns = prepare_features(happiness_data)

    X_train, X_test, y_train, y_test = split_data(X, y)

    model = train_model(X_train, y_train)

    y_pred, metrics = evaluate_model(model, X_test, y_test)

    save_model(model, feature_columns)

    save_outputs(X_test, y_test, y_pred, metrics)