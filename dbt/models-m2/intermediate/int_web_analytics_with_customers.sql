with events as (
    select * from {{ ref('stg_web_analytics') }}
),

customers as (
    select * from {{ ref('stg_adventure_db__customers') }}
),

joined as (
    select
        -- Event fields
        events.customer_id,
        events.session_id,
        events.product_id,
        events.page_url,
        events.event_type,
        events.event_timestamp,
        events.dbt_loaded_at,

        -- Customer context
        customers.first_name,
        customers.last_name,
        customers.full_name,
        customers.email_address,
        customers.city,
        customers.state_province,
        customers.country_region

    from events
    left join customers
        -- customer_id in stg_web_analytics is integer; cast to string to match
        -- stg_adventure_db__customers where customerid was cast as string
        on events.customer_id::string = customers.customer_id
)

select * from joined