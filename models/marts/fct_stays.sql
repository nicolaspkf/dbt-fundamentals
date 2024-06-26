-- Load latest staged data with some overlap to capture late arrivals (late data arrivals - not arrival of customers), assuming that load or ingestion is done at least once per day.
with stays as (
    
  select * from {{ ref('stg_source__stays') }}
  {% if is_incremental() %}
    where loaded_date >= (select max(loaded_date) from {{ this }})
  {% endif %}

)
    
-- Extract first and last name. Create arrival_date column from input data and calculate departure date. Extract the customer's email domain.
,final as (

  select
    stay_id
    ,split_part(regexp_replace(name,'(Ms\\. |Mr\\. |Dr\\. | MD| PhD| DDS| DVM| II)',''), ' ', 1)        as guest_first_name
    ,split_part(regexp_replace(name,'(Ms\\. |Mr\\. |Dr\\. | MD| PhD| DDS| DVM| II)',''), ' ', -1)       as guest_last_name
    ,date_from_parts(arrival_year, month(to_date(left(month_of_year, 3), 'Mon')), arrival_day_of_month) as arrival_date
    ,arrival_date + stays_in_weekend_nights + stays_in_week_nights                                      as departure_date
    ,dayname(arrival_date)                                                                              as arrival_day_name
    ,dayname(departure_date)                                                                            as departure_day_name
    ,regexp_substr(email, '@.*')                                                                        as email_domain
  from 
    stays
  
)

select * from final
