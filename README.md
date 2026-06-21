# MobilityIQ — Taxi Demand Forecasting & Analytics Platform

## Project Overview

This project builds an end-to-end data engineering and machine learning pipeline to forecast taxi demand across New York City zones.

The pipeline ingests historical taxi trip data, transforms it into time-series features, trains forecasting models and display predictions through an interactive dashboard.

The platform has since been extended with a governed semantic layer (Snowflake Cortex Analyst) so users can query demand, revenue, and seasonality data in plain English — turning the forecasting pipeline into a self-service analytics platform.

The goal of the project is to demonstrate production-style workflows involving:
* Data Engineering
* Feature Engineering
* Time Series Forecasting
* Machine Learning in the Data Warehouse
* Semantic Layer & Natural Language Analytics
* Interactive Data Visualization
* The system forecasts hourly taxi demand for each NYC taxi zone and allows users to explore predictions through a live dashboard, or ask questions about demand, revenue, and seasonality directly in plain English.

Dashboard link -  https://nyc-taxi-demand-forecast-happzg759c7qzkbmfvruuyf.streamlit.app/

## Tech Stack

* Cloud and Tools - AWS Lambda, AWS S3, Snowflake, dbt
* Languages and Libraries - SQL, Python, Snowpark, Pandas, Plotly
* ML Model - LightGBM, XGBoost
* Semantic Layer / NL Query - Snowflake Cortex Analyst
* Visualization - Streamlit
* Deployment - Streamlit Community Cloud, Github 

## Architecture

<img width="1920" height="1080" alt="image" src="https://github.com/user-attachments/assets/c9fa4f0d-3972-45a7-93ee-d7efa6161c80" />

Note: this diagram reflects the core forecasting pipeline. See the "Semantic Layer & Natural Language Analytics" section below for the analytics layer built on top of it.

## Project Objectives

To build an end-to-end Data and ML pipeline using modern tech stack, to forecast 7-day hourly taxi demand across NYC zones, and to expose that data — along with historical revenue and seasonality patterns — through a governed semantic layer that supports both BI dashboards and natural language queries.

## Dataset

The dataset used in this project is the NYC Taxi Trip Records dataset provided by the New York City Taxi and Limousine Commission.
Link - https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Data Dictionary - https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

The dataset contains:

* Pickup datetime
* Dropoff datetime
* Passenger count
* Trip distance
* Pickup zone
* Dropoff zone
* Fare information

For this project, the data was aggregated to hourly demand per zone.

## Data Pipeline

The pipline consists of several stages:

### 1. Data Extraction

A simple python code is used to extract data from the source which is run using AWS Lambda. (Example of code is in extract_data.py - https://github.com/mukulsagvekar/nyc-taxi-demand-forecast/blob/main/extract_data.py). As there is a lag of 2 or 3 months from the source, this functions extracts data which is 2 or 3 months old from the current run date and a cron job can be used to automate it. The data is loaded in a S3 bucket.

### 2. Data Ingestion

As the frequency of data is monthly, Snowpipe is not used, instead a copy command is run using dbt macro to ingest the data from S3 to RAW schema. the data is semi-structured, so it is stored as a varient in raw table along with metadata for logging purpose.

<img width="1364" height="473" alt="image" src="https://github.com/user-attachments/assets/e01514e2-59a6-40b1-a0ee-c44866c5bf40" />

### 3. Data Transformation

* Data is first flatten and stored into curated layer in trips_flatten table
<img width="1649" height="475" alt="image" src="https://github.com/user-attachments/assets/9d439a25-4e59-4dfe-8679-748968e53149" />

* Then data is cleaned (remove invalid records) and only required columns are taken, and stored in trips_cleaned table.

<img width="1232" height="471" alt="image" src="https://github.com/user-attachments/assets/4fb257a0-e1ab-4156-a036-e3a80b645ff0" />

* This cleaned data is then aggregated and transformed into time series data and stored in Analytics Layer. During this transformation, the missing timestamps are filled by creating a grid of zone x timestamps for the whole data range and cross joined with the cleaned data so it fills the missed timestamps per zone.

<img width="1649" height="469" alt="image" src="https://github.com/user-attachments/assets/a28f4d67-f5d0-4ceb-9c93-edb2ccfd4321" />

* Then the feaetures such as time features(hour, day, dayofweek, month, quarter, year. dayofyear, is_holiday), lag features (lag_1, lag_24, lag_168), and rolling features (rolling_avg_24h, rolling_avg_7d, rolling_std_24h) are extracted to find short-term trends, daily seasonality, and weekly patterns.
  
<img width="1648" height="476" alt="image" src="https://github.com/user-attachments/assets/89f9430f-5649-4757-9c12-b04e26b15302" />

## Semantic Layer & Natural Language Analytics

On top of the curated Snowflake tables, a semantic layer was added using Snowflake Cortex Analyst, so business users can ask questions in plain English without writing SQL.

### Why

The original dashboard answered one question well: "what's forecasted demand by zone?" Real analytics use cases need more — revenue, fares, seasonality — and hardcoding a new SQL query for every new question doesn't scale. A semantic layer defines what each metric means once, in one governed place, so both the dashboard and the AI chat agree on the same numbers.

### What was built

* Three dbt marts purpose-built for the semantic layer, each joined to a zone/borough lookup table:
  * `mart_demand_forecast` — forecasted demand by zone, borough, and hour
  * `mart_trip_facts` — historical trip-level revenue, fare, tip, and distance
  * `mart_demand_seasonality` — holiday lift, weekend/weekday patterns, and precomputed rolling 24h/7d averages
* A Cortex Analyst semantic model YAML, staged in Snowflake, defining dimensions, measures, and verified example queries for each mart
* An "Ask MobilityIQ" chat interface built into the Streamlit app, calling the Cortex Analyst REST API, with the auto-generated SQL shown for transparency and results auto-charted where applicable

### Example questions

* "What's the average fare in Manhattan vs Queens?"
* "Is demand higher on holidays vs regular days?"
* "Which zone generates the most revenue per trip?"
* "Which boroughs have the highest average forecasted taxi demand?"

### Known limitations (by design)

* No forecast-accuracy metrics (MAE/MAPE) — actuals aren't yet stored alongside predictions, so accuracy can't be calculated against this schema
* No anomaly detection — rolling standard deviation is computed but not yet surfaced as an alert or flag
* No driver/vehicle-level questions — not present in the source data
* No real-time questions — the pipeline is monthly batch, not streaming

## ML Model

The forecasting model used in this project is LightGBM, a gradient boosting framework designed for efficient machine learning.
While training the model, LightGBM performed better for this panel time series data, it handled zone_id better - understanding the timeseries patter according to zones. 

Below is the importance give to features by this model

<img width="440" height="409" alt="image" src="https://github.com/user-attachments/assets/88366fbe-5c9b-4577-85c9-8948f2e750d6" />



The model selection and training is this notebook - https://github.com/mukulsagvekar/nyc-taxi-demand-forecast/blob/main/nyctaxi_demand_forecasting.ipynb

## Forecasting Strategy

The future demand is predicted using recursive forecasting. As for the future we do not have time, lag and rolling features.
Steps:
1. Use latest observed demand values
2. Predict next hour demand
3. Append prediction to dataset
4. Generate new lag features
5. Predict next hour again

This continues for 168 hours (7 days)

## Dashboard

Forecasted data is stored in snowflake table. The predictions are visualized using Streamlit.

The dashboard allows users to:
* View taxi demand trends
* Explore forecasts by zone
* Visualize geographic demand heatmaps
* Filter demand by time range
* Ask questions in plain English via the "Ask MobilityIQ" chat panel, powered by Snowflake Cortex Analyst

Features include:
* Interactive zone filters
* Demand trend charts
* Geographic heatmaps of NYC taxi zones
* Natural language Q&A over demand, revenue, and seasonality data, with generated SQL shown for transparency

## Future Improvements

Possible improvements include:
* Real-time data ingestion
* Automated model retraining
* Deep learning models for time series
* Anomaly detection for abnormal demand spikes
* Event-aware forecasting (weather, holidays)
* Store actuals alongside forecasts to enable forecast-accuracy tracking (MAE/MAPE, bias)
* Expand the semantic layer with anomaly-flagging measures

## Example Use Cases

This system could be used for:
* Taxi fleet demand planning
* Ride-sharing optimization
* City transportation planning
* Urban mobility analytics
* Self-service revenue and seasonality analysis for non-technical stakeholders