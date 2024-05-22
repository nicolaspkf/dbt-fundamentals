-- Load latest source data with some overlap to capture late arrivals (late data arrivals - not arrival of customers)
with source_data as (
  select *from {{ source('source_name', 'table_name') }}
  {% if is_incremental() %}
    where loaded_date >= current_date - 1
  {% endif %}
)

-- Unpack the source data into columns, rename columns to fit business logic, cast to relevant data types 
with final as (
  
  select
    ,{{ dbt_utils.generate_surrogate_key([*fields that identify the given row*]) }}
                                       as stay_id                    -- generate a primary key for the table if it does not exist in the source table. 
    ,hotel::text                       as hotel
    ,is_cancelled::int                 as canecelled_flag            -- Changed boolean-like naming to ´flag-like naming. 
    ,lead_time::int                    as lead_time_sec              -- indicate time unit in naming
    ,arrival_date_year::int            as arrival_date_year
    ,arrival_date_month::int           as arrival_date_month
    ,arrival_date_week_number::int     as arrival_date_week_number
    ,arrival_date_day_of_month::int    as arrival_date_day_of_month
    ,stays_in_weekend_nights::int      as stays_in_weekend_nights
    ,stays_in_week_nights::int         as stays_in_week_nights
  
    -------- SELECT, CAST AND RENAME THE REST OF THE COLUMNS FROM THE SOURCE --------
    ,adults
    ,children
    ,babies
    ,meal
    ,country
    ,market_segment
    ,distribution_channel
    ,is_repeated_guest
    ,previous_cancellations
    ,previous_bookings_not_canceled
    ,reserved_room_type
    ,assigned_room_type
    ,booking_changes
    ,deposit_type
    ,agent
    ,company
    ,days_in_waiting_list
    ,customer_type
    ,adr
    ,required_car_parking_spaces
    ,total_of_special_requests
    ,reservation_status
    ,reservation_status_date
    ,name
    ,email
    ,phone-number
    ,credit_card

  from
    {{ source('source_name', 'table_name') }}
  
)

select * from final
