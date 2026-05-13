# ============================================================
# Workshop 003 - ETL Process using Apache Kafka + Machine Learning
# Step 02: Exploratory Data Analysis and Visualizations
# ============================================================

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# ------------------------------------------------------------
# 1. Define project paths
# ------------------------------------------------------------

PROCESSED_DATA_PATH = "data/processed"
REPORTS_PATH = "reports"
FIGURES_PATH = "reports/figures"

INPUT_FILE = os.path.join(PROCESSED_DATA_PATH, "happiness_clean.csv")
DESCRIPTIVE_STATS_FILE = os.path.join(REPORTS_PATH, "descriptive_statistics.csv")
CORRELATION_FILE = os.path.join(REPORTS_PATH, "correlation_matrix.csv")


# ------------------------------------------------------------
# 2. Load clean dataset
# ------------------------------------------------------------

def load_clean_data():
    """
    This function loads the clean dataset created in the previous ETL step.
    """

    df = pd.read_csv(INPUT_FILE)

    print("Clean dataset loaded successfully.")
    print("Dataset shape:")
    print(df.shape)

    print("\nDataset columns:")
    print(df.columns.tolist())

    return df


# ------------------------------------------------------------
# 3. Generate descriptive statistics
# ------------------------------------------------------------

def generate_descriptive_statistics(df):
    """
    This function generates descriptive statistics for the numeric columns.
    The output is saved as a CSV file in the reports folder.
    """

    os.makedirs(REPORTS_PATH, exist_ok=True)

    descriptive_stats = df.describe()
    descriptive_stats.to_csv(DESCRIPTIVE_STATS_FILE)

    print("\nDescriptive statistics saved successfully:")
    print(DESCRIPTIVE_STATS_FILE)

    print("\nDescriptive statistics preview:")
    print(descriptive_stats)


# ------------------------------------------------------------
# 4. Generate correlation matrix
# ------------------------------------------------------------

def generate_correlation_matrix(df):
    """
    This function calculates the correlation between numeric variables.
    It helps us identify which variables are more related to happiness_score.
    """

    numeric_df = df.select_dtypes(include=["int64", "float64"])
    correlation_matrix = numeric_df.corr()

    correlation_matrix.to_csv(CORRELATION_FILE)

    print("\nCorrelation matrix saved successfully:")
    print(CORRELATION_FILE)

    print("\nCorrelation with happiness_score:")
    print(correlation_matrix["happiness_score"].sort_values(ascending=False))

    return correlation_matrix


# ------------------------------------------------------------
# 5. Save visualizations
# ------------------------------------------------------------

def save_visualizations(df, correlation_matrix):
    """
    This function creates and saves the main visualizations for the EDA.
    """

    os.makedirs(FIGURES_PATH, exist_ok=True)

    # ------------------------------
    # 5.1 Happiness score distribution
    # ------------------------------
    plt.figure(figsize=(8, 5))
    sns.histplot(df["happiness_score"], kde=True)
    plt.title("Distribution of Happiness Score")
    plt.xlabel("Happiness Score")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_PATH, "happiness_score_distribution.png"))
    plt.close()

    # ------------------------------
    # 5.2 GDP vs Happiness Score
    # ------------------------------
    plt.figure(figsize=(8, 5))
    sns.scatterplot(data=df, x="gdp_per_capita", y="happiness_score", hue="year", palette="viridis")
    plt.title("GDP per Capita vs Happiness Score")
    plt.xlabel("GDP per Capita")
    plt.ylabel("Happiness Score")
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_PATH, "gdp_vs_happiness.png"))
    plt.close()

    # ------------------------------
    # 5.3 Social Support vs Happiness Score
    # ------------------------------
    plt.figure(figsize=(8, 5))
    sns.scatterplot(data=df, x="social_support", y="happiness_score", hue="year", palette="viridis")
    plt.title("Social Support vs Happiness Score")
    plt.xlabel("Social Support")
    plt.ylabel("Happiness Score")
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_PATH, "social_support_vs_happiness.png"))
    plt.close()

    # ------------------------------
    # 5.4 Life Expectancy vs Happiness Score
    # ------------------------------
    plt.figure(figsize=(8, 5))
    sns.scatterplot(data=df, x="life_expectancy", y="happiness_score", hue="year", palette="viridis")
    plt.title("Life Expectancy vs Happiness Score")
    plt.xlabel("Life Expectancy")
    plt.ylabel("Happiness Score")
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_PATH, "life_expectancy_vs_happiness.png"))
    plt.close()

    # ------------------------------
    # 5.5 Average happiness by year
    # ------------------------------
    happiness_by_year = df.groupby("year")["happiness_score"].mean().reset_index()

    plt.figure(figsize=(8, 5))
    sns.lineplot(data=happiness_by_year, x="year", y="happiness_score", marker="o")
    plt.title("Average Happiness Score by Year")
    plt.xlabel("Year")
    plt.ylabel("Average Happiness Score")
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_PATH, "average_happiness_by_year.png"))
    plt.close()

    # ------------------------------
    # 5.6 Correlation heatmap
    # ------------------------------
    plt.figure(figsize=(10, 7))
    sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm", fmt=".2f")
    plt.title("Correlation Matrix")
    plt.tight_layout()
    plt.savefig(os.path.join(FIGURES_PATH, "correlation_matrix.png"))
    plt.close()

    print("\nVisualizations saved successfully in:")
    print(FIGURES_PATH)


# ------------------------------------------------------------
# 6. Main execution
# ------------------------------------------------------------

if __name__ == "__main__":
    happiness_data = load_clean_data()
    generate_descriptive_statistics(happiness_data)
    correlation_matrix = generate_correlation_matrix(happiness_data)
    save_visualizations(happiness_data, correlation_matrix)