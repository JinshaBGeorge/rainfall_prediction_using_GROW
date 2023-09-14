# Using GROW to Predict Rainfall

## Project Aim

The **Using GROW to Predict Rainfall** project is a citizen science initiative in Europe that engages citizens in collecting data on soil moisture. The primary objective of this project is to enhance the accessibility and utility of the data collected through GROW for the purpose of predicting rainfall more effectively. The project can be broadly categorized into two main components: Data Processing and Prediction Model.

## Data Sources

The project utilizes the following data sources:

1. **GROW Soil Moisture Data:** This dataset can be accessed [here](https://discovery.dundee.ac.uk/en/datasets/grow-soil-moisture-data) and contains approximately 15 GB of soil moisture data.

2. **Historical NASA Weather Data API:** Data from the NASA Weather Data API, available [here](https://power.larc.nasa.gov/api/), is used to complement the GROW data.

## Data Processing (ETL and Data Modeling)

In the Data Processing phase, the project undergoes the following steps:

- **Data Extraction:** The large GROW soil moisture dataset is extracted from its source.

- **Data Transformation:** The extracted data is transformed into third normal form (3NF), ensuring data consistency and integrity.

- **Data Loading:** The processed data is loaded into a cloud-based SQL database for efficient storage and retrieval.

- **Data Modeling:** The data is further modeled into a relational schema, facilitating structured analysis and integration with meteorological data.

- **Integration with Meteorological Data:** To provide more insightful predictions, the GROW data is integrated with meteorological data.

## Predictive Model

The predictive model in this project is a sequential time series neural network, leveraging Long Short-Term Memory (LSTM) layers. The key elements of the predictive model are as follows:

- **Model Inputs:** The model is trained using two primary inputs - Soil Moisture data collected through GROW and historical precipitation data.

- **Rainfall Forecasting:** The primary function of the model is to forecast rainfall based on the input data. Users can select a specific location for rainfall prediction.

- **Validation Accuracy:** The trained model achieves a validation accuracy of approximately 60%, making it a reliable tool for rainfall prediction.
