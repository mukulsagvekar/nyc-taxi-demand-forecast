{{ config(materialized='table') }}

-- Semantic-layer mart: demand with calendar context (holiday, day-of-week, rolling trend).
-- Source: hourly_features + RAW.TAXI_ZONE_LOOKUP
-- Grain: one row per zone_id per hour.

with features as (

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
        is_holiday,
        rolling_avg_24h,
        rolling_avg_7d,
        rolling_std_24h
    from {{ ref('hourly_features') }}

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
        f.pickup_datetime,
        f.hour,
        f.dayofweek,
        case
            when f.dayofweek in (0, 6) then true
            else false
        end as is_weekend,
        f.is_holiday,
        f.trip_count,
        f.rolling_avg_24h,
        f.rolling_avg_7d,
        f.rolling_std_24h
    from features f
    inner join zones z
        on f.zone_id = z.zone_id

)

select * from final