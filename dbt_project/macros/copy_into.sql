{% macro copy_into_macro() %}
  {% set copy_stmt %}
    copy into nyctaxi.raw.trips(data, file_name, file_row_number, load_timestamp)
    from (
        select t.$1, metadata$filename, metadata$file_row_number, metadata$file_last_modified from @nyctaxi.raw.nyctaxi_stage/raw/ t
    ); 
  {% endset %}

  {% do run_query(copy_stmt) %}
{% endmacro %}