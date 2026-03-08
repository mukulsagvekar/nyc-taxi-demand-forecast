with t as(
    select 
    holiday_name, 
    to_date(holiday_date, 'yyyy/mm/dd') as holiday_date
    from {{ ref('city_holidays') }}
)
select * from t