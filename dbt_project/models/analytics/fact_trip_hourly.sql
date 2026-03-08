with base as(
    select
        zone_id,
        pickup_hour as pickup_datetime,
        count(*) as trip_count,
        load_timestamp,
        to_timestamp_ntz(current_timestamp) as update_timestamp
    from {{ ref('trips_cleaned') }}
    group by 1,2,4,5
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
    select
        distinct zone_id
    from base
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
    coalesce(b.trip_count, 0) as trip_count, 
    coalesce(b.load_timestamp, to_timestamp_ntz(current_timestamp)) as load_timestamp,
    to_timestamp_ntz(current_timestamp) as update_timestamp
from zone_hours zh
left join base b
    on zh.zone_id = b.zone_id
    and zh.pickup_datetime = b.pickup_datetime

{% if is_incremental() %}
    where load_timestamp > (select max(load_timestamp) from {{ this }})
{% endif %}