use schema nyctaxi.raw;

-- create raw table
create or replace transient table nyctaxi.raw.trips(
    data variant,
    file_name string,
    file_row_number number,
    load_timestamp timestamp_ntz
);

-- copy statement
copy into nyctaxi.raw.trips (data, file_name, file_row_number, load_timestamp)
from (
    select t.$1, metadata$filename, metadata$file_row_number, metadata$file_last_modified from @nyctaxi.raw.nyctaxi_stage/raw/ t
); 

select * from nyctaxi.curated.trips where trip_distance>0 and passenger_count != 0 and total_amount>0 limit 10;--5523402 --5478725

select count(*) from nyctaxi.raw.trips; --41169720--85587316


