with source as (
    select * from {{ source('raw_ext', 'web_analytics_raw') }}
),

cleaned as (
    select
        customer_id::integer                            as customer_id,
        product_id::integer                             as product_id,
        session_id::varchar                             as session_id,
        page_url::varchar                               as page_url,
        event_type::varchar                             as event_type,
        convert_timezone('UTC', event_timestamp)::timestamp_ntz as event_timestamp,
        _loaded_at,
        _file_name,
        current_timestamp()                             as dbt_loaded_at
    from source
    where customer_id is not null
      and product_id is not null
      and session_id is not null
      and event_timestamp is not null
)

select * from cleaned
-- ci 