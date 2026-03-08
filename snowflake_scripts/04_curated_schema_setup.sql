select *, data:Airport_fee::number(20,2) as airport_fee,
from nyctaxi.raw.trips where airport_fee is null
limit 10;

-- faltten sql
select 
    data:VendorID::number as vendor_id,
    data:tpep_pickup_datetime::varchar::timestamp_ntz as pickup_datetime,
    data:tpep_dropoff_datetime::varchar::timestamp_ntz as dropoff_datetime,
    data:passenger_count::number as passenger_count,
    data:trip_distance::number(20,2) as trip_distance,
    data:RatecodeID::number as rate_code_id,
    data:store_and_fwd_flag::boolean as store_and_fwd_flag,
    data:PULocationID::number as pickup_location_id,
    data:DOLocationID::number as dropoff_location_id,
    data:payment_type::number as payment_type,
    data:fare_amount::number(20,2) as fare_amount,
    data:extra::number(20,2) as extra,
    data:mta_tax::number(20,2) as mta_tax,
    data:tip_amount::number(20,2) as trip_amount,
    data:tolls_amount::number(20,2) as tolls_amount,
    data:improvement_surcharge::number(20,2) as improvement_surcharge,
    data:total_amount::number(20,2) as total_amount,
    data:congestion_surcharge::number(20,2) as congestion_surcharge,
    data:Airport_fee::number(20,2) as airport_fee,
    load_timestamp,
    to_timestamp_ntz(current_timestamp) as update_timestamp
from nyctaxi.raw.trips
    where airport_fee is null
limit 10;

select count(*) from nyctaxi.curated.trips where total_amount > 0 and passenger_count != 0 and passenger_count is not null limit 10; --7052769 --5462997

select * from nyctaxi.curated.trips where rate_code_id is not null;--1347086

-- trips_cleaned sql

select
    t1.pickup_location_id as zone_id,
    DATE_TRUNC('hour', t1.pickup_datetime) AS pickup_hour,
    t1.trip_distance,
    t1.fare_amount,
    t1.total_amount,
    case 
        when t2.holiday_date is not null then 1
        else 0
    end as is_holiday,
    t1.load_timestamp,
    to_timestamp_ntz(current_timestamp) as update_timestamp
from NYCTAXI.CURATED.TRIPS_FLATTEN t1 
left join  NYCTAXI.CURATED.HOLIDAYS t2
on date(t1.pickup_datetime) = t2.holiday_date
where is_holiday = 1
limit 10;


with t as(
    select
        pickup_location_id,
        DATE_TRUNC('hour', pickup_datetime) AS pickup_hour,
        hour(pickup_datetime) as pickup_hourofday,
        day(pickup_datetime) as pickup_dayofmonth,
        dayofweek(pickup_datetime) as pickup_dayofweek,
        month(pickup_datetime) as pickup_month,
        quarter(pickup_datetime) as pickup_quarter,
        case 
            when date(pickup_datetime) in (select distinct holiday_date from NYCTAXI.CURATED.HOLIDAYS) then 1
            else 0
        end as is_holiday,
        trip_distance,
        fare_amount,
        total_amount,
        load_timestamp,
        to_timestamp_ntz(current_timestamp) as update_timestamp
    from NYCTAXI.CURATED.TRIPS_FLATTEN limit 10
)
select * from t;