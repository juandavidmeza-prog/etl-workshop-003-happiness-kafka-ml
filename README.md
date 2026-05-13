# Workshop 003 - ETL Process using Apache Kafka + Machine Learning

## Project Overview

This project was developed for the ETL course as part of Workshop 003: ETL Process using Apache Kafka + Machine Learning.

The objective of the project is to build an end-to-end predictive data pipeline for Happiness Score prediction. The system integrates data cleaning, exploratory data analysis, feature selection, regression model training, Kafka-based data streaming, model-based prediction, and database storage.

The project uses five CSV files containing World Happiness data from 2015 to 2019. After cleaning and integrating the datasets, a regression model is trained to predict the Happiness Score using selected numerical features. Then, Kafka is used to stream testing records one by one, and a consumer loads the trained model to generate predictions and store them in a SQLite database.

---

## Technologies Used

The project uses the following technologies:

- Python
- Pandas
- NumPy
- Matplotlib
- Seaborn
- Scikit-learn
- Joblib
- Apache Kafka
- Docker
- SQLite
- Kafka-Python

---

## Project Structure

```text
etl-workshop-003-happiness-kafka-ml/
│
├── data/
│   ├── raw/
│   │   ├── 2015.csv
│   │   ├── 2016.csv
│   │   ├── 2017.csv
│   │   ├── 2018.csv
│   │   └── 2019.csv
│   │
│   ├── processed/
│   │   ├── happiness_clean.csv
│   │   └── happiness_stream_data.csv
│   │
│   └── happiness_predictions.db
│
├── models/
│   └── happiness_model.pkl
│
├── reports/
│   ├── figures/
│   ├── correlation_matrix.csv
│   ├── descriptive_statistics.csv
│   ├── kafka_prediction_metrics.csv
│   ├── kafka_predictions_export.csv
│   ├── performance_metrics.json
│   ├── summary_report.md
│   └── test_predictions.csv
│
├── src/
│   ├── 01_eda_cleaning.py
│   ├── 02_eda_visualizations.py
│   ├── 03_train_model.py
│   ├── 04_prepare_stream_data.py
│   ├── 05_kafka_producer.py
│   ├── 06_kafka_consumer.py
│   ├── 07_export_predictions.py
│   └── 08_performance_visualizations.py
│
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.mdx