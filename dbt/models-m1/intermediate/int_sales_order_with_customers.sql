with

sales_orders as (
    select * from {{ ref('stg_ecom__sales_orders') }}
),

customers as (
    select * from {{ ref('stg_adventure_db__customers') }}
),

joined as (
    select
        -- Order fields
        so.sales_order_id,
        so.sales_order_number,
        so.order_date,
        so.ship_date,
        so.status,
        so.online_order_flag,
        so.sub_total,
        so.tax_amt,
        so.freight,
        so.total_due,
        so.shipping_method,
        so.delivery_estimate_days,

        -- Customer fields
        so.customer_id,
        c.first_name,
        c.last_name,
        c.full_name,
        c.email_address,
        c.address_line_1,
        c.address_line_2,
        c.city,
        c.state_province,
        c.country_region,
        c.postal_code

    from sales_orders so
    left join customers c
        on so.customer_id = c.customer_id
)

select * from joined