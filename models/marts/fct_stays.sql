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
    
-- Make calculations and other preparations
,final as (

  select
    stay_id
    ,split_part(regexp_replace(name,'(Ms\\. |Mr\\. |Dr\\. | MD| PhD| DDS| DVM| II)',''), ' ', 1)        as guest_first_name
    ,split_part(regexp_replace(name,'(Ms\\. |Mr\\. |Dr\\. | MD| PhD| DDS| DVM| II)',''), ' ', -1 )      as guest_last_name
    ,date_from_parts(year, month_of_year, day_of_month) as arrival_date
    date of departure
    day of week of arrival
    day of week of departure
    email domain (e.g. @gmail.com, etc.)
  from 
    stays
  
)

select * from final
