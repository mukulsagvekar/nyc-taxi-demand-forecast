{{ config(materialized='table') }}

-- Semantic-layer mart: trip-level revenue and fare facts by zone, borough, and hour.
-- Source: trips_cleaned + RAW.TAXI_ZONE_LOOKUP
-- Grain: one row per trip.
-- NOTE: tip_amount requires the trips_flatten.sql alias fix
--       (data:tip_amount -> tip_amount, not trip_amount) AND
--       trips_cleaned.sql must select tip_amount before this mart will work.

with trips as (

    select
        zone_id,
        pickup_hour,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount
    from {{ ref('trips_cleaned') }}

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
        t.zone_id,
        z.zone_name,
        z.borough,
        t.pickup_hour,
        date(t.pickup_hour) as pickup_date,
        hour(t.pickup_hour) as pickup_hour_of_day,
        dayofweek(t.pickup_hour) as pickup_dayofweek,
        t.trip_distance,
        t.fare_amount,
        t.tip_amount,
        t.total_amount,
        case when t.trip_distance > 0
            then t.total_amount / t.trip_distance
            else null
        end as revenue_per_mile,
        case when t.fare_amount > 0
            then t.tip_amount / t.fare_amount
            else null
        end as tip_rate
    from trips t
    inner join zones z
        on t.zone_id = z.zone_id

)

select * from final