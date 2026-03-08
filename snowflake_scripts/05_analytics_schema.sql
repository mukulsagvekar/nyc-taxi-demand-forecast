use schema nyctaxi.analytics;

-- fact hourly script
with base as(
    select
        zone_id,
        pickup_hour as pickup_datetime,
        is_holiday,
        count(*) as trip_count,
        load_timestamp,
        to_timestamp_ntz(current_timestamp) as update_timestamp
    from NYCTAXI.CURATED.TRIPS_CLEANED
    group by 1,2,3,5,6
),
time_bounds as(
    select 
        min(pickup_datetime) as min_ts, 
        max(pickup_datetime) as max_ts 
    from base
),
all_hours as (
select
    dateadd(
        hour,
        seq4(),
        (select min_ts from time_bounds)
    ) as pickup_datetime
from table(generator(rowcount => 100000))
where pickup_datetime <= (select max_ts from time_bounds)
), 
zones as (
    select 1 as zone_id
    //distinct zone_id
    //from base
),
zone_hours as (
    select
        z.zone_id,
        h.pickup_datetime
    from zones z
    cross join all_hours h
)
select
    zh.zone_id,
    zh.pickup_datetime,
    coalesce(b.is_holiday, 0) as is_holiday,
    coalesce(b.trip_count, 0) as trip_count, 
    load_timestamp,
    //coalesce(b.load_timestamp, to_timestamp_ntz(current_timestamp)) as load_timestamp,
    to_timestamp_ntz(current_timestamp) as update_timestamp
from zone_hours zh
left join base b
    on zh.zone_id = b.zone_id
    and zh.pickup_datetime = b.pickup_datetime
where date(zh.pickup_datetime) = '2024-10-14'; --4418680

select zone_id, count(*) from NYCTAXI.ANALYTICS.FACT_TRIP_HOURLY group by zone_id; --1775809

select 4418680 - 1775809;



-- fact table sq;
select
    pickup_location_id as zone_id,
    pickup_hour,
    pickup_hourofday,
    pickup_dayofmonth,
    pickup_dayofweek,
    pickup_month,
    pickup_quarter,
    is_holiday,
    count(*) as trip_count,
    avg(fare_amount) as avg_fare,
    sum(total_amount) as total_revenue,
    load_timestamp,
    to_timestamp_ntz(current_timestamp) as update_timestamp
from NYCTAXI.CURATED.TRIPS_CLEANED
group by 1,2,3,4,5,6,7,8,12,13;

select * from nyctaxi.analytics.fact_trip_hourly where year(pickup_hour) != 2024;

SELECT 
    MIN(pickup_hour),
    MAX(pickup_hour),
    COUNT(*) AS total_rows
FROM ANALYTICS.fact_trip_hourly;

SELECT 
    zone_id,
    COUNT(distinct pickup_hour) AS hourly_points
FROM ANALYTICS.fact_trip_hourly
GROUP BY zone_id;

select count(distinct pickup_hour) FROM ANALYTICS.fact_trip_hourly;

SELECT 
    MIN(trip_count),
    MAX(trip_count),
    AVG(trip_count),
    STDDEV(trip_count)
FROM ANALYTICS.fact_trip_hourly;

SELECT 
    pickup_hourofday,
    AVG(trip_count)
FROM ANALYTICS.fact_trip_hourly
GROUP BY pickup_hourofday
ORDER BY pickup_hourofday;

SELECT 
    pickup_dayofweek,
    AVG(trip_count)
FROM ANALYTICS.fact_trip_hourly
GROUP BY pickup_dayofweek
ORDER BY pickup_dayofweek;

COPY INTO @NYCTAXI.RAW.unload_stage
FROM nyctaxi.ANALYTICS.fact_trip_hourly;

select zone_id, pickup_datetime, trip_count from nyctaxi.ANALYTICS.fact_trip_hourly limit 10;

COPY INTO 's3://nyc-yellow-taxi-trips-data/nyc_taxi_data_hourly_2024_2025.csv'
FROM (select zone_id, pickup_datetime, trip_count from nyctaxi.ANALYTICS.fact_trip_hourly)
STORAGE_INTEGRATION = s3_int
FILE_FORMAT = (
    TYPE = CSV
    COMPRESSION = NONE
    FILE_EXTENSION = NONE
    SKIP_HEADER = 0
)
SINGLE = TRUE
MAX_FILE_SIZE = 5368709120; -- Max size of 5 GB for S3



/*select count(*) from nyctaxi.curated.trips_flatten limit 10; --35632825

create or replace temporary table nyctaxi.curated.cleaned_trips as
select
    pickup_location_id,
    DATE_TRUNC('hour', pickup_datetime) AS pickup_hour,
    hour(pickup_datetime) as pickup_hourofday,
    day(pickup_datetime) as pickup_dayofmonth,
    dayofweek(pickup_datetime) as pickup_dayofweek,
    month(pickup_datetime) as pickup_month,
    quarter(pickup_datetime) as pickup_quarter,
    trip_distance,
    fare_amount,
    total_amount
from nyctaxi.curated.trips_flatten; where trip_distance>0;
order by pickup_datetime asc;

select count(*) from nyctaxi.curated.cleaned_trips;--35632825

select * from nyctaxi.curated.cleaned_trips limit 10;

select
    pickup_location_id as zone_id,
    pickup_hour,
    pickup_hourofday,
    pickup_dayofmonth,
    pickup_dayofweek,
    pickup_month,
    pickup_quarter,
    count(*) as trip_count,
    avg(fare_amount) as avg_fare,
    sum(total_amount) as total_revenue,
    load_timestamp,
    to_timestamp_ntz(current_timestamp) as update_timestamp
from nyctaxi.curated.trips_cleaned
group by 1,2,3,4,5,6,7,11,12;--882406


with t as (
    select
        pickup_location_id as zone_id,
        pickup_hour,
        pickup_hourofday,
        pickup_dayofmonth,
        pickup_dayofweek,
        pickup_month,
        pickup_quarter,
        count(*) as trip_count,
        avg(fare_amount) as avg_fare,
        sum(total_amount) as total_revenue,
        load_timestamp,
        to_timestamp_ntz(current_timestamp) as update_timestamp
    from nyctaxi.curated.trips_cleaned
    group by 1,2,3,4,5,6,7,11,12
)
select * from t ;

-- drop table nyctaxi.analytics.fact_trip_hourly;*/