with t as(
    select
        pickup_location_id as zone_id,
        DATE_TRUNC('hour', pickup_datetime) AS pickup_hour,
        trip_distance,
        fare_amount,
        total_amount,
        load_timestamp,
        to_timestamp_ntz(current_timestamp) as update_timestamp
    from {{ ref('trips_flatten') }} 
)

select * from t

{% if is_incremental() %}
    where load_timestamp > (select max(load_timestamp) from {{ this }})
{% endif %}