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
    ,date_from_parts(arrival_year, month(to_date(left(month_of_year, 3), 'Mon')), arrival_day_of_month) as arrival_date
    ,arrival_date + stays_in_weekend_nights + stays_in_week_nights                                      as departure_date
    ,dayname(arrival_date)                                                                              as arrival_date_name
    ,dayname(departure_date)                                                                            as departure_date_name
    ,regexp_substr(email, '@.*')                                                                        as email_domain
  from 
    stays
  
)

select * from final
