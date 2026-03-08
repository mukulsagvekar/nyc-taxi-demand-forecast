use database nyctaxi;

select 
    t1.pickup_datetime, 
    case 
        when t2.holiday_date is not null then 1 else 0 
    end as is_holiday,
    t2.holiday_name
    from nyctaxi.analytics.fact_trip_hourly  t1
left join NYCTAXI.CURATED.HOLIDAYS t2
on date(t1.pickup_datetime) = t2.holiday_date;


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
from nyctaxi.analytics.fact_trip_hourly t1
left join NYCTAXI.CURATED.HOLIDAYS t2
on date(t1.pickup_datetime) = t2.holiday_date
qualify lag_168 IS NOT NULL;




select * from NYCTAXI.CURATED.TRIPS_CLEANED limit 10;

select * from NYCTAXI.ANALYTICS.FACT_TRIP_HOURLY limit 10;

select 
    zone_id,
    dateadd(hour,
            seq4() + 1,
            (select max(pickup_hour) from NYCTAXI.ANALYTICS.FACT_TRIP_HOURLY)) as pickup_hour
from (select distinct zone_id from NYCTAXI.ANALYTICS.FACT_TRIP_HOURLY),
table(generator(rowcount => 168));

-- view for train data
select 
    zone_id, 
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
from NYCTAXI.FEATURE_STORE.HOURLY_FEATURES 
where rolling_std_24h is not null;

