-- Incremental model configs
{{
    config(
        materialized = 'incremental'
        ,unique_key = 'stay_id'     
    )
}}
  
-- Load staged stays
with stays as (
  select * from {{ ref('stg_source__stays') }}

)

-- Make calculations and other preparations
,final as (

  select

  from 
    stays
  
)

select * from final
