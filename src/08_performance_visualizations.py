# ============================================================
# Workshop 003 - ETL Process using Apache Kafka + Machine Learning
# Step 08: Performance visualizations
# ============================================================

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# ------------------------------------------------------------
# 1. Define project paths
# ------------------------------------------------------------

REPORTS_PATH = "reports"
FIGURES_PATH = "reports/figures"

PREDICTIONS_FILE = os.path.join(REPORTS_PATH, "kafka_predictions_export.csv")


# ------------------------------------------------------------
# 2. Load predictions
# ------------------------------------------------------------

def load_predictions():
    """
    This function loads the exported Kafka predictions.
    """

    df = pd.read_csv(PREDICTIONS_FILE)

    print("Kafka predictions loaded successfully.")
    print("Dataset shape:")
    print(df.shape)

    return df


# ------------------------------------------------------------
# 3. Create performance visualizations
# ------------------------------------------------------------

def create_visualizations(df):
    """
    This function creates visualizations to compare actual and predicted
    happiness scores.
    """

    os.makedirs(FIGURES_PATH, exist_ok=True)

    # Calculate residuals
    df["error"] = df["actual_happiness_score"] - df["predicted_happiness_score"]
    df["absolute_error"] = df["error"].abs()

    # --------------------------------------------------------
    # 3.1 Actual vs Predicted Happiness Score
    # --------------------------------------------------------
    plt.figure(figsize=(8, 5))
    sns.scatterplot(
        data=df,
        x="actual_happiness_score",
        y="predicted_happiness_score"
    )

    min_value = min(df["actual_happiness_score"].min(), df["predicted_happiness_score"].min())
    max_value = max(df["actual_happiness_score"].max(), df["predicted_happiness_score"].max())

    plt.plot([min_value, max_value], [min_value, max_value], linestyle="--")
    plt.title("Actual vs Predicted Happiness Score")
    plt.xlabel("Actual Happiness Score")
    plt.ylabel("Predicted Happiness Score")
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_PATH, "actual_vs_predicted_happiness.png"))
    plt.close()

    # --------------------------------------------------------
    # 3.2 Error Distribution
    # --------------------------------------------------------
    plt.figure(figsize=(8, 5))
    sns.histplot(df["error"], kde=True)
    plt.title("Prediction Error Distribution")
    plt.xlabel("Prediction Error")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_PATH, "prediction_error_distribution.png"))
    plt.close()

    # --------------------------------------------------------
    # 3.3 Absolute Error Distribution
    # --------------------------------------------------------
    plt.figure(figsize=(8, 5))
    sns.histplot(df["absolute_error"], kde=True)
    plt.title("Absolute Error Distribution")
    plt.xlabel("Absolute Error")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_PATH, "absolute_error_distribution.png"))
    plt.close()

    print("\nPerformance visualizations saved successfully in:")
    print(FIGURES_PATH)


# ------------------------------------------------------------
# 4. Main execution
# ------------------------------------------------------------

if __name__ == "__main__":
    predictions_data = load_predictions()
    create_visualizations(predictions_data)