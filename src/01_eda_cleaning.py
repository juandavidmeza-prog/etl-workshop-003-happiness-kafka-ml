# ============================================================
# Workshop 003 - ETL Process using Apache Kafka + Machine Learning
# Step 01: EDA, cleaning and integration of happiness datasets
# ============================================================

import os
import pandas as pd
import numpy as np


# ------------------------------------------------------------
# 1. Define project paths
# ------------------------------------------------------------

RAW_DATA_PATH = "data/raw"
PROCESSED_DATA_PATH = "data/processed"
OUTPUT_FILE = os.path.join(PROCESSED_DATA_PATH, "happiness_clean.csv")


# ------------------------------------------------------------
# 2. Function to standardize column names
# ------------------------------------------------------------

def standardize_columns(df, year):
    """
    This function receives a dataframe and the year of the file.
    Since the five CSV files have different column names, this
    function renames them into one common structure.
    """

    column_mapping = {
        # Country
        "Country": "country",
        "Country or region": "country",

        # Region
        "Region": "region",

        # Rank
        "Happiness Rank": "happiness_rank",
        "Happiness.Rank": "happiness_rank",
        "Overall rank": "happiness_rank",

        # Target variable
        "Happiness Score": "happiness_score",
        "Happiness.Score": "happiness_score",
        "Score": "happiness_score",

        # GDP
        "Economy (GDP per Capita)": "gdp_per_capita",
        "Economy..GDP.per.Capita.": "gdp_per_capita",
        "GDP per capita": "gdp_per_capita",

        # Social support / family
        "Family": "social_support",
        "Social support": "social_support",

        # Health
        "Health (Life Expectancy)": "life_expectancy",
        "Health..Life.Expectancy.": "life_expectancy",
        "Healthy life expectancy": "life_expectancy",

        # Freedom
        "Freedom": "freedom",
        "Freedom to make life choices": "freedom",

        # Corruption
        "Trust (Government Corruption)": "government_corruption",
        "Trust..Government.Corruption.": "government_corruption",
        "Perceptions of corruption": "government_corruption",

        # Generosity
        "Generosity": "generosity",

        # Other columns from some years
        "Dystopia Residual": "dystopia_residual",
        "Dystopia.Residual": "dystopia_residual",
        "Standard Error": "standard_error",
        "Lower Confidence Interval": "lower_confidence_interval",
        "Upper Confidence Interval": "upper_confidence_interval",
        "Whisker.high": "whisker_high",
        "Whisker.low": "whisker_low"
    }

    df = df.rename(columns=column_mapping)

    # Add year column because each CSV represents one year
    df["year"] = year

    return df


# ------------------------------------------------------------
# 3. Load all CSV files
# ------------------------------------------------------------

def load_all_files():
    """
    This function reads all CSV files from data/raw.
    It extracts the year from the file name and stores all
    dataframes in a list.
    """

    dataframes = []

    for file_name in os.listdir(RAW_DATA_PATH):
        if file_name.endswith(".csv"):
            file_path = os.path.join(RAW_DATA_PATH, file_name)

            # Example: 2015.csv -> 2015
            year = int(file_name.replace(".csv", ""))

            print(f"Reading file: {file_name}")

            df = pd.read_csv(file_path)
            df = standardize_columns(df, year)

            dataframes.append(df)

    return dataframes


# ------------------------------------------------------------
# 4. Integrate datasets
# ------------------------------------------------------------

def integrate_data(dataframes):
    """
    This function concatenates the five yearly datasets into
    one single dataframe.
    """

    df_all = pd.concat(dataframes, ignore_index=True)

    print("\nIntegrated dataset shape:")
    print(df_all.shape)

    return df_all


# ------------------------------------------------------------
# 5. Select useful columns
# ------------------------------------------------------------

def select_columns(df):
    """
    This function keeps only the columns that are useful for
    EDA, model training and prediction.
    """

    selected_columns = [
        "country",
        "region",
        "year",
        "happiness_rank",
        "happiness_score",
        "gdp_per_capita",
        "social_support",
        "life_expectancy",
        "freedom",
        "generosity",
        "government_corruption"
    ]

    # Some files do not have the region column, especially 2018 and 2019.
    # For that reason, we only keep columns that really exist.
    existing_columns = [col for col in selected_columns if col in df.columns]

    df = df[existing_columns]

    return df


# ------------------------------------------------------------
# 6. Clean data
# ------------------------------------------------------------

def clean_data(df):
    """
    This function performs the main cleaning tasks:
    - Remove duplicated rows
    - Standardize text columns
    - Convert numeric columns
    - Fill missing values
    """

    print("\nMissing values before cleaning:")
    print(df.isnull().sum())

    # Remove duplicated country-year rows
    df = df.drop_duplicates(subset=["country", "year"])

    # Standardize country names
    df["country"] = df["country"].astype(str).str.strip()

    # If region exists, clean it. If it does not exist for some rows,
    # we fill it as Unknown.
    if "region" in df.columns:
        df["region"] = df["region"].astype(str).str.strip()
        df["region"] = df["region"].replace("nan", np.nan)
        df["region"] = df["region"].fillna("Unknown")
    else:
        df["region"] = "Unknown"

    # Numeric columns used by the model
    numeric_columns = [
        "year",
        "happiness_rank",
        "happiness_score",
        "gdp_per_capita",
        "social_support",
        "life_expectancy",
        "freedom",
        "generosity",
        "government_corruption"
    ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    # Fill missing numeric values using the median by year.
    # This is better than using a global median because each year can behave differently.
    for column in numeric_columns:
        df[column] = df.groupby("year")[column].transform(
            lambda x: x.fillna(x.median())
        )

    print("\nMissing values after cleaning:")
    print(df.isnull().sum())

    return df


# ------------------------------------------------------------
# 7. Handle outliers using IQR
# ------------------------------------------------------------

def handle_outliers(df):
    """
    This function handles extreme values using the IQR method.
    Instead of deleting rows, we cap values to the lower and upper limits.
    This keeps the dataset complete and avoids losing countries.
    """

    columns_to_check = [
        "happiness_score",
        "gdp_per_capita",
        "social_support",
        "life_expectancy",
        "freedom",
        "generosity",
        "government_corruption"
    ]

    for column in columns_to_check:
        q1 = df[column].quantile(0.25)
        q3 = df[column].quantile(0.75)
        iqr = q3 - q1

        lower_limit = q1 - 1.5 * iqr
        upper_limit = q3 + 1.5 * iqr

        df[column] = df[column].clip(lower=lower_limit, upper=upper_limit)

    return df


# ------------------------------------------------------------
# 8. Save clean dataset
# ------------------------------------------------------------

def save_clean_data(df):
    """
    This function saves the clean integrated dataset into
    data/processed/happiness_clean.csv
    """

    os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)

    df.to_csv(OUTPUT_FILE, index=False)

    print("\nClean dataset saved successfully:")
    print(OUTPUT_FILE)

    print("\nFinal dataset preview:")
    print(df.head())

    print("\nFinal dataset shape:")
    print(df.shape)


# ------------------------------------------------------------
# 9. Main execution
# ------------------------------------------------------------

if __name__ == "__main__":
    dataframes = load_all_files()
    happiness_data = integrate_data(dataframes)
    happiness_data = select_columns(happiness_data)
    happiness_data = clean_data(happiness_data)
    happiness_data = handle_outliers(happiness_data)
    save_clean_data(happiness_data)