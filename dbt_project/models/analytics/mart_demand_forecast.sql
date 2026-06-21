{{ config(materialized='table') }}

-- Semantic-layer mart: forecasted demand by zone, borough, and hour.
-- Source: FEATURE_STORE.ZONE_HOURLY_FORECAST + RAW.TAXI_ZONE_LOOKUP
-- Grain: one row per zone_id per forecasted hour.

with forecast as (

    select
        zone_id,
        year,
        month,
        dayofmonth,
        hour,
        trip_count
    from  {{ source('feature_store', 'zone_hourly_forecast') }}

),

zones as (

    select
        locationid as zone_id,
        zone as zone_name,
        borough
    from {{ source('raw', 'taxi_zone_lookup') }}

),

final as (

    select
        f.zone_id,
        z.zone_name,
        z.borough,
        timestamp_from_parts(f.year, f.month, f.dayofmonth, f.hour, 0, 0) as forecast_datetime,
        date(timestamp_from_parts(f.year, f.month, f.dayofmonth, f.hour, 0, 0)) as forecast_date,
        f.hour as forecast_hour,
        dayofweek(timestamp_from_parts(f.year, f.month, f.dayofmonth, f.hour, 0, 0)) as forecast_dayofweek,
        ceil(f.trip_count) as predicted_demand
    from forecast f
    inner join zones z
        on f.zone_id = z.zone_id

)

select * from final