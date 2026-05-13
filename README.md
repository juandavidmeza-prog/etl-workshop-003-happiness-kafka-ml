# Workshop 003 - ETL Process using Apache Kafka + Machine Learning
Juan David Meza - 2240129
## Project Overview
This project was developed for the **ETL Workshop 003: ETL Process using Apache Kafka + Machine Learning**.
The main objective of the project is to build a complete data pipeline that takes World Happiness Report data, cleans and transforms it, sends the information through Apache Kafka, and uses a machine learning model to predict the happiness score of each record.
In simple words, the project simulates a real data engineering process: first the data is prepared, then it is sent record by record through Kafka, and finally a trained model receives that information and generates predictions. The results are stored in a SQLite database and can be analyzed later through metrics, reports, and visualizations.
The dataset used in this project contains information from the World Happiness Report for different years. These files include variables related to happiness, such as economic level, social support, health, freedom, generosity, and perception of corruption.
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
- kafka-python
- GitHub
---
## ETL Pipeline Explanation
This project follows an ETL process. ETL means **Extract, Transform, and Load**.
The idea was to take the original happiness data, clean it, prepare it, send it through Kafka, and then use it with a machine learning model to make predictions.
The complete process works like this:

```text
The project begins with the raw happiness data. 
I cleaned and analyzed the information, selected the most useful variables, and trained a regression model. 
Then, Kafka was used to send and receive the data record by record. 
The trained model predicted the happiness score, and the results were stored in SQLite for reports and visualizations.
In this pipeline, the data does not only stay in a CSV file. It is transformed, streamed, predicted, stored, and analyzed. This makes the project closer to a real data engineering and machine learning workflow.

---
## Data Source
The data comes from the "World Happiness Report" datasets from different years, mainly from 2015 to 2019.
Each CSV file contains information about countries and different factors that may influence happiness. Some of these factors are related to the economy, family or social support, health, freedom, generosity, and trust in institutions.
The objective was to use these variables to predict the **happiness score**, which is the main target variable of the project.

---
## Data Cleaning and Transformation
The first technical step was to clean and transform the raw data.
The original files were stored in the `data/raw/` folder. Since the project uses data from different years, some files had different column names or slightly different structures. For that reason, the data had to be standardized before using it.
During this step, the project did the following tasks:
MINI FLOWCHART (Composition):
Raw CSV files
     ↓
Review the data structure
     ↓
Clean and organize the columns
     ↓
Remove unnecessary information
     ↓
Check missing values
     ↓
Keep the most useful numerical variables
     ↓
Create one cleaner dataset
     ↓
Save the final data in data/processed/

This step was important because the machine learning model needs organized and consistent data. If the data is not clean, the predictions can be incorrect or the code can fail.

---
## EDA / Analysis
The exploratory data analysis was used to understand the dataset before training the model.
In this part, the project reviewed the behavior of the variables and generated reports to better understand the data. The goal was not only to train a model, but first to understand what kind of information was available.

The EDA process included:

- Reviewing the main columns of the dataset.
- Checking descriptive statistics.
- Looking at the relationship between the happiness score and the other variables.
- Creating correlation outputs.
- Generating visualizations to make the data easier to understand.
- Identifying which variables could be more useful for prediction.
This analysis helped to confirm that variables such as economy, social support, health, freedom, generosity, and corruption perception can be useful when trying to explain or predict the happiness score.
---
## Feature Selection
Feature selection means choosing the variables that will be used by the model to make predictions.
In this project, the target variable was:
```text
Happiness Score
```
This is the value that the model tries to predict.
The selected features were variables related to social, economic, and personal well-being factors. These variables were chosen because they have a logical relationship with happiness and also appear in the World Happiness Report data.
The main idea was not to use every column without thinking. Instead, the project selected the columns that made more sense for predicting the happiness score.

Some examples of relevant variables are:
- Economy / GDP per capita
- Family or social support
- Health / life expectancy
- Freedom
- Generosity
- Trust or perception of corruption
These variables were used as input data for the regression model.

---
## Kafka Producer
The Kafka Producer is the part of the project that sends the data.
After the data was cleaned and prepared, the Producer reads the transformed dataset and sends each row as an individual message to a Kafka topic.
This is important because it simulates a streaming environment. Instead of processing all the data at the same time, the data is sent record by record, similar to how real systems receive information continuously.

In simple terms:
```text
The Producer reads the processed data and sends it to Kafka.
```
This part connects the ETL process with the streaming architecture.

---
## Kafka Consumer
The Kafka Consumer is the part of the project that receives the data.
The Consumer listens to the Kafka topic and receives the messages sent by the Producer. Each message represents one record from the transformed dataset.
After receiving the record, the Consumer prepares it and sends it to the trained machine learning model so the model can generate a prediction.
In simple terms:
```text
The Producer sends the data, and the Consumer receives it.
```
This connection between Producer and Consumer is one of the most important parts of the project because it shows how Kafka can be used to move data through a pipeline.
---
## Model Training
The machine learning part of the project uses a regression model.
A regression model was selected because the objective is to predict a numerical value: the happiness score.
During the training process, the cleaned dataset was divided into input variables and target variable. The selected features were used as the input, and the happiness score was used as the value that the model needed to learn how to predict.

After training the model, it was saved as a  ".pkl" file inside the "models/" folder.

This is useful because the model does not need to be trained again every time the project runs. Instead, the saved model can be loaded later and used directly for predictions.

---
## Model Loading and Prediction
After the model was trained and saved, the next step was to use it for predictions.
The saved model is loaded from the `.pkl` file. Then, when the Kafka Consumer receives a new record, the model takes the selected features from that record and predicts the happiness score.
The process works like this:

```text
Kafka Consumer receives a record
     ↓
The record is prepared with the selected features
     ↓
The trained .pkl model is loaded
     ↓
The model predicts the happiness score
     ↓
The prediction is stored in SQLite
```

This part shows how the project connects streaming data with machine learning predictions.

---

## Database and Storage

The predictions generated by the model are stored in a SQLite database.

This makes it possible to keep the results after the Consumer processes the incoming records. Instead of only printing the predictions in the terminal, the project saves them so they can be reviewed later.

The database is useful for:
- Saving prediction results.
- Reviewing incoming records.
- Comparing real and predicted values.
- Creating reports.
- Generating metrics and visualizations.
---
## KPIs and Visualizations
The project includes reports, metrics, and visualizations to evaluate the behavior of the model and the data.
These outputs help to understand if the model is making reasonable predictions and how close the predicted happiness scores are to the real values.

The "reports/" folder contains files related to:
- Descriptive statistics.
- Correlation analysis.
- Prediction results.
- Kafka prediction metrics.
- Model performance metrics.
- Visualizations.
This part is important because it allows the results to be interpreted more easily. The project is not only about running code, but also about understanding what the results mean.
---
## Main Challenges
During the project, there were several challenges that had to be solved.
One of the main challenges was configuring Kafka correctly. Kafka requires the Producer, Consumer, and topic to work properly, so the connection between these components was an important part of the process.
Another challenge was making sure the Producer and Consumer communicated correctly. The Producer had to send the records one by one, and the Consumer had to receive them without breaking the prediction process.

There were also challenges with file paths, because the project uses different folders for raw data, processed data, models, reports, and source code. Keeping the paths organized was necessary so each script could find the correct files.

Another important challenge was cleaning and transforming data from different years. Since the datasets did not always have the exact same structure, the information had to be standardized before training the model.

The model training process also required attention because the selected features had to match the data used later during prediction. If the columns were different, the model could not predict correctly.

Finally, organizing the GitHub repository was also important. The project needed to be clear and easy to understand, not only for running the code but also for explaining the complete pipeline.
---
## Assumptions
This project was built based on some assumptions.
The first assumption is that the selected variables from the World Happiness Report have a relationship with the happiness score.
The second assumption is that the happiness score can be predicted using a regression model because it is a continuous numerical value.

The third assumption is that variables such as economy, health, social support, freedom, generosity, and corruption perception are relevant for understanding the happiness level of a country.

Another assumption is that the cleaned and transformed dataset is good enough to train the model and generate useful predictions.

The project also assumes that the streaming process with Kafka can represent how new happiness records could arrive in a real system.

---
## How to Run the Project
To run the project, first install the required libraries:
```bash
pip install -r requirements.txt
```
Then start Kafka using Docker:
```bash
docker-compose up -d
```
After Kafka is running, execute the scripts in order:
```bash
python src/01_data_cleaning.py
python src/02_eda_visualization.py
python src/03_train_model.py
python src/04_prepare_stream_data.py
python src/05_kafka_producer.py
python src/06_kafka_consumer.py
python src/07_export_predictions.py
python src/08_performance_visualization.py
```
The Producer and Consumer should be executed when Kafka is already active. The Producer sends the data, and the Consumer receives it and generates the predictions.
---
## Final Conclusion
This project shows a complete ETL and machine learning workflow using Apache Kafka.
The data starts as raw CSV files, then it is cleaned, transformed, analyzed, and prepared for machine learning. After that, Kafka is used to simulate the movement of data in real time. The trained model receives each record and predicts the happiness score.
The project helped demonstrate how data engineering and machine learning can work together in the same pipeline. It also showed the importance of organizing the repository, cleaning the data properly, selecting relevant features, and documenting each part of the process.
In conclusion, this project is not only a model training exercise. It is a complete pipeline that connects data cleaning, Kafka streaming, machine learning prediction, database storage, and result analysis.
## Project Structure Final Part (Distribution)
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
