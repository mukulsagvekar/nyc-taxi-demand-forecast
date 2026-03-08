-- 1. use admin role
use role ACCOUNTADMIN;

-- 2. create transform role and assign it to accountadmin
create role if not exists transform;
grant role transform to role accountadmin;

-- 3. use compute warehouse
grant operate on warehouse compute_wh to role transform;

-- 4. create dbt user and assign to transform role
create user if not exists dbt
  password = 'dbt@6598'
  login_name = 'dbt'
  must_change_password = false
  default_warehouse = 'compute_warehouse'
  default_role = transform
  default_namespace = 'nyctaxi.raw'
  comment = 'dbt user used for data transformation';

alter user dbt set type = legacy_service;

grant role transform to user dbt;

-- 5. create db and schema
create database if not exists nyctaxi;
create schema if not exists nyctaxi.raw;

-- 6. grant all permissions to tranform role
grant all on warehouse compute_wh to role transform;
grant all on database nyctaxi to role transform;
grant all on all schemas in database nyctaxi to role transform;
grant all on future schemas in database nyctaxi to role transform;
grant all on all tables in schema nyctaxi.raw to role transform;
grant all on future tables in schema nyctaxi.raw to role transform;

grant all on stage nyctaxi.raw.nyctaxi_stage to role transform;

grant all on file format nyctaxi.raw.parquet_ff to role transform;



