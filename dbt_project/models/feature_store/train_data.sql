{{ config(materialized='view') }}

with t as(
    select 
        zone_id, 
        pickup_datetime,
        trip_count, 
        hour, 
        dayofweek, 
        dayofmonth,
        month,
        quarter,
        year,
        dayofyear,
        is_holiday,
        lag_1,
        lag_24,
        lag_168,
        rolling_avg_24h,
        rolling_avg_7d,
        rolling_std_24h
    from {{ ref('hourly_features') }}
    where rolling_std_24h is not null
)
select * from t