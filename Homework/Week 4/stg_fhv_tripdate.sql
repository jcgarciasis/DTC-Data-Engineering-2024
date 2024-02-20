{{config(materialized = "view") }}

with tripdata as 
(
  select *

  from {{ source('staging','fhv_tripdata') }}
  Where extract(year from pickup_datetime) = 2019
)

Select 
    dispatching_base_num
    ,pickup_datetime
    ,dropOff_datetime
    ,PUlocationID
    ,DOlocationID
    ,SR_Flag
    ,Affiliated_base_number

from tripdata

-- dbt build --m <model.sql> --vars 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
