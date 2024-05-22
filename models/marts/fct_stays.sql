-- Incremental model configs
{{
    config(
        materialized = 'incremental'
        ,unique_key = 'stay_id'     
    )
}}
  
-- Load latest staged data with some overlap to capture late arrivals (late data arrivals - not arrival of customers), assuming that load or ingestion is done at least once per day.
with stays as (
    
  select * from {{ ref('stg_source__stays') }}
  {% if is_incremental() %}
    where loaded_date >= (select max(loaded_date) from {{this}})
  {% endif %}

)

with source_data as (
  select *from {{ source('source_name', 'table_name') }}
  {% if is_incremental() %}
    where loaded_date >= current_date - 1
  {% endif %}
)
    
-- Make calculations and other preparations
,final as (

  select

  from 
    stays
  
)

select * from final
