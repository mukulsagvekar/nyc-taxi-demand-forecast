with t as(
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
    from {{ source('raw', 'trips') }}
    where trip_distance>0 and passenger_count != 0 and total_amount>0 and 
    year(pickup_datetime) between to_decimal(split(split(file_name, '_')[2],'-')[0]) - 1 and 
    to_decimal(split(split(file_name, '_')[2],'-')[0])
)
select * from t 

{% if is_incremental() %}
    where load_timestamp > (select max(load_timestamp) from {{ this }})
{% endif %}