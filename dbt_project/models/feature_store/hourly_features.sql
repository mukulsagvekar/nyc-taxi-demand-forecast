with t as (
    select
        t1.zone_id,
        t1.pickup_datetime,
        t1.trip_count,
        hour(t1.pickup_datetime) as hour,
        dayofweek(t1.pickup_datetime) as dayofweek,
        day(t1.pickup_datetime) as dayofmonth,
        month(t1.pickup_datetime) as month,
        quarter(t1.pickup_datetime) as quarter,
        year(t1.pickup_datetime) as year,
        dayofyear(t1.pickup_datetime) as dayofyear,
        case 
            when t2.holiday_date is not null then 1 else 0 
        end as is_holiday,
        lag(t1.trip_count, -1) over(partition by t1.zone_id order by t1.pickup_datetime) as lag_1,
        lag(t1.trip_count, -24) over(partition by t1.zone_id order by t1.pickup_datetime) as lag_24,
        lag(t1.trip_count, -168) over(partition by t1.zone_id order by t1.pickup_datetime) as lag_168,
        avg(t1.trip_count) over(partition by t1.zone_id order by t1.pickup_datetime rows between 24 preceding and 1 preceding) as rolling_avg_24h,
        avg(t1.trip_count) over(partition by t1.zone_id order by t1.pickup_datetime rows between 168 preceding and 1 preceding) as rolling_avg_7d,
        stddev(t1.trip_count) over(partition by t1.zone_id order by t1.pickup_datetime rows between 24 preceding and 1 preceding) as rolling_std_24h,
        t1.load_timestamp,
        to_timestamp_ntz(current_timestamp) as update_timestamp
    from {{ ref('fact_trip_hourly') }} t1
    left join {{ ref('holidays') }} t2
    on date(t1.pickup_datetime) = t2.holiday_date
    qualify lag_168 IS NOT NULL
)

select * from t

{% if is_incremental() %}
    where load_timestamp > (select max(load_timestamp) from {{ this }})
{% endif %}