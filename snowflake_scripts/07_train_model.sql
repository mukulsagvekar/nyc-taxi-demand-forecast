use schema nyctaxi.feature_store;


-- create stage to save model
create stage nyctaxi.feature_store.ml_models;

-- store procedure to train and save model
create or replace procedure nyctaxi.feature_store.train_lgbm_model()
returns string
language python
runtime_version = 3.10
packages = ('snowflake-snowpark-python', 'lightgbm', 'pandas', 'joblib', 'scikit-learn')
handler = 'run'
as
$$

import pandas as pd
import joblib
from lightgbm import LGBMRegressor

def run(session):

    df = session.table('nyctaxi.feature_store.train_data').to_pandas()

    features = [
        'ZONE_ID',
        'HOUR',
        'DAYOFWEEK',
        'DAYOFMONTH',
        'MONTH',
        'QUARTER',
        'YEAR',
        'DAYOFYEAR',
        'IS_HOLIDAY',
        'LAG_1',
        'LAG_24',
        'LAG_168',
        'ROLLING_AVG_24H',
        'ROLLING_AVG_7D', 
        'ROLLING_STD_24H'
    ]

    target = 'TRIP_COUNT'

    X = df[features]
    y = df[target]

    model = LGBMRegressor(
        subsample = 0.8,
        num_leaves = 50,
        n_estimators = 400,
        min_child_samples = 30,
        max_depth = 8,
        learning_rate = 0.1,
        colsample_bytree = 0.8
    )

    model.fit(X, y)

    joblib.dump(model, "/tmp/lgbm_model.pkl")

    session.file.put(
        "/tmp/lgbm_model.pkl",
        "@ML_MODELS",
        overwrite=True
    )

    return "Model trained and saved"

$$;

-- call
call nyctaxi.feature_store.train_lgbm_model();

list @nyctaxi.feature_store.ml_models;


-- procedure to forcast
create or replace procedure forecast_7d()
returns string
language python
runtime_version = 3.10
packages = ('snowflake-snowpark-python', 'lightgbm', 'pandas', 'joblib', 'scikit-learn')
HANDLER = 'run'
AS
$$

import pandas as pd
import joblib
import numpy as np

def recursive_forecast(df, holidays, model, hours):

    df = df.copy()

    df = df.sort_values(["ZONE_ID","PICKUP_DATETIME"])

    holidays = holidays.copy()

    preds = []

    last_time = df.PICKUP_DATETIME.max()
    zones = df.ZONE_ID.unique()

    for step in range(hours):

        next_time = last_time + pd.Timedelta(hours=1)

        rows = []

        for zone in zones:

            zone_df = df[df.ZONE_ID == zone]

            lag_1 = zone_df.iloc[-1]["TRIP_COUNT"]
            lag_24 = zone_df.iloc[-24]["TRIP_COUNT"]
            lag_168 = zone_df.iloc[-168]["TRIP_COUNT"]

            rolling_avg_24h = zone_df.tail(24)["TRIP_COUNT"].mean()
            rolling_avg_7d = zone_df.tail(168)["TRIP_COUNT"].mean()
            rolling_std_24h = zone_df.tail(24)["TRIP_COUNT"].std()

            rows.append({
                "ZONE_ID": zone,
                "HOUR": next_time.hour,
                "DAYOFWEEK": next_time.dayofweek,
                'DAYOFMONTH': next_time.day,
                "MONTH": next_time.month,
                "QUARTER": next_time.quarter,
                "YEAR": next_time.year,
                "DAYOFYEAR": next_time.dayofyear,
                "IS_HOLIDAY": 0,
                "LAG_1": lag_1,
                "LAG_24": lag_24,
                "LAG_168": lag_168,
                "ROLLING_AVG_24H": rolling_avg_24h,
                "ROLLING_AVG_7D": rolling_std_24h,
                "ROLLING_STD_24H": rolling_avg_7d
            })

        future_df = pd.DataFrame(rows)

        features = ['ZONE_ID', 'HOUR', 'DAYOFWEEK', 'DAYOFMONTH', 'MONTH', 'QUARTER', 'YEAR', 'DAYOFYEAR', 
        'IS_HOLIDAY', 'LAG_1', 'LAG_24', 'LAG_168', 'ROLLING_AVG_24H', 'ROLLING_AVG_7D', 'ROLLING_STD_24H'
		]
		
        future_df["TRIP_COUNT"] = model.predict(future_df[features])

        df = pd.concat([df,future_df],ignore_index=True)

        preds.append(future_df)

        last_time = next_time

    return pd.concat(preds)

def run(session):

    files = session.file.get("@NYCTAXI.FEATURE_STORE.ML_MODELS/lgbm_model.pkl","/tmp")

    model_path = "/tmp/" + files[0].file

    model = joblib.load(model_path)

    df = session.table("nyctaxi.feature_store.train_data").to_pandas()
    holidays = session.table("NYCTAXI.CURATED.HOLIDAYS").to_pandas()

    forecast = recursive_forecast(df, holidays, model, 168)

    session.write_pandas(
        forecast,
        "ZONE_HOURLY_FORECAST",
        auto_create_table=True,
        overwrite=True
    )

    return "7 day forecast completed"

$$;


call nyctaxi.feature_store.forecast_7d();

select * from NYCTAXI.FEATURE_STORE.ZONE_HOURLY_FORECAST where zone_id = 100;

select max(pickup_datetime) from nyctaxi.feature_store.train_data ;--2025-11-23 23:00:00.000