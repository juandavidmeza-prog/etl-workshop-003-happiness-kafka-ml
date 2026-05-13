# Summary Report - Workshop 003  
# Reporte resumen - Taller 003
# JUANDAVIDMEZA - 2240129
## 1. Project Overview  
## 1. Descripción general del proyecto

### English

This project presents a complete ETL pipeline connected with Apache Kafka and a Machine Learning model.

The main purpose of the project is to predict the **Happiness Score** of different countries using historical data from 2015 to 2019. To do this, we worked through the full data process: loading the original CSV files, cleaning and organizing the information, exploring the data, selecting the most useful variables, training a regression model, sending data through Kafka, generating predictions, and finally storing the results in a database.

In simple terms, the project starts with raw happiness datasets and ends with a system that can receive records through Kafka, use a trained model to predict the happiness score, and save both the real and predicted values in SQLite.

### Español

Este proyecto presenta un flujo completo de ETL conectado con Apache Kafka y un modelo de Machine Learning.

El objetivo principal es predecir el **Happiness Score** de diferentes países usando datos históricos entre 2015 y 2019. Para lograrlo, se realizó todo el proceso de datos: cargar los archivos CSV originales, limpiar y organizar la información, explorar los datos, seleccionar las variables más relevantes, entrenar un modelo de regresión, enviar datos mediante Kafka, generar predicciones y finalmente guardar los resultados en una base de datos.

En palabras más simples, el proyecto empieza con bases de datos crudas sobre felicidad mundial y termina con un sistema capaz de recibir registros por Kafka, usar un modelo entrenado para predecir el puntaje de felicidad y guardar tanto el valor real como el valor predicho en SQLite.

---

## 2. Dataset Description  
## 2. Descripción de los datos

### English

The project uses five CSV files, where each file represents one year of happiness data:

- `2015.csv`
- `2016.csv`
- `2017.csv`
- `2018.csv`
- `2019.csv`

These files contain information about different countries and their happiness scores. They also include variables that may help explain those scores, such as GDP per capita, social support, life expectancy, freedom, generosity, and perception of government corruption.

One important detail is that the files did not use exactly the same column names across all years. Because of that, one of the first steps was to standardize the column names so all datasets could be joined into one consistent table.

After the cleaning process, the final dataset contains:

- 782 rows
- 11 columns

### Español

El proyecto utiliza cinco archivos CSV, donde cada archivo representa un año de datos sobre felicidad:

- `2015.csv`
- `2016.csv`
- `2017.csv`
- `2018.csv`
- `2019.csv`

Estos archivos contienen información de diferentes países y sus puntajes de felicidad. También incluyen variables que pueden ayudar a explicar esos puntajes, como el PIB per cápita, el apoyo social, la esperanza de vida, la libertad, la generosidad y la percepción de corrupción del gobierno.

Un detalle importante es que los archivos no tenían exactamente los mismos nombres de columnas en todos los años. Por eso, uno de los primeros pasos fue estandarizar los nombres de las columnas para poder unir todas las bases en una sola tabla consistente.

Después del proceso de limpieza, la base final quedó con:

- 782 filas
- 11 columnas

---

## 3. ETL Process  
## 3. Proceso ETL

### Extract  
### Extracción

#### English

In the extraction step, the five CSV files were loaded from the `data/raw` folder.

Each file was read using Pandas. Since every file represented a different year, the year was taken from the file name and added as a new column called `year`. This was useful because it allowed the final dataset to keep the time reference of each record.

#### Español

En la etapa de extracción, se cargaron los cinco archivos CSV desde la carpeta `data/raw`.

Cada archivo fue leído usando Pandas. Como cada archivo representaba un año diferente, se tomó el año desde el nombre del archivo y se agregó como una nueva columna llamada `year`. Esto fue útil porque permitió conservar la referencia temporal de cada registro dentro de la base final.

---

### Transform  
### Transformación

#### English

In the transformation step, the goal was to make the data clean, consistent, and ready to use.

The main transformations were:

- Standardizing column names.
- Joining the five datasets into one dataframe.
- Removing duplicated records by country and year.
- Cleaning text values.
- Converting numerical columns to the correct format.
- Filling missing numerical values with the median of each year.
- Filling missing region values as `Unknown`.
- Handling outliers using the IQR method.

These transformations were necessary because the original files came from different years and were not completely uniform. After this step, the dataset was much more reliable for analysis and model training.

#### Español

En la etapa de transformación, el objetivo fue dejar los datos limpios, consistentes y listos para ser usados.

Las principales transformaciones fueron:

- Estandarizar los nombres de las columnas.
- Unir las cinco bases de datos en un solo dataframe.
- Eliminar registros duplicados por país y año.
- Limpiar valores de texto.
- Convertir las columnas numéricas al formato correcto.
- Rellenar valores numéricos faltantes usando la mediana de cada año.
- Rellenar regiones faltantes como `Unknown`.
- Manejar valores atípicos usando el método IQR.

Estas transformaciones fueron necesarias porque los archivos originales venían de diferentes años y no estaban completamente uniformes. Después de este paso, la base quedó mucho más confiable para el análisis y el entrenamiento del modelo.

---

### Load  
### Carga

#### English

After cleaning and transforming the data, the final dataset was saved in the processed data folder.

The output file was:

```text
data/processed/happiness_clean.csv