use database nyctaxi;
use schema nyctaxi.raw;
use warehouse compute_wh;

-- storage integration
CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::536285030347:role/snowflake-role'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://movie-lens-data-bucket/', 's3://nyc-yellow-taxi-trips-data/');
  
DESC INTEGRATION s3_int;

-- file format
create or replace file format parquet_ff
  type = parquet;
  auto_compression = true;

-- stage
create or replace stage nyctaxi_stage
  url = 's3://nyc-yellow-taxi-trips-data/'
  storage_integration = s3_int
  file_format = parquet_ff;

create or replace stage unload_stage
  url = 's3://nyc-yellow-taxi-trips-data/'
  storage_integration = s3_int
  file_format = (type = 'csv');
   
list @nyctaxi_stage/raw/;

select $1, metadata$filename, metadata$file_row_number, metadata$file_last_modified from @nyctaxi_stage limit 5; --3475226

