with

orders as (
    select * from {{ source('raw_ext', 'orders_raw') }}
),

order_details as (
    select * from {{ source('raw_ext', 'order_details_raw') }}
),

aggregated_details as (
    select
        sales_order_id,
        array_agg(
            object_construct(
                'CarrierTrackingNumber', carrier_tracking_number,
                'LineTotal',             line_total::float,
                'ModifiedDate',          last_modified::date::string,
                'OrderQty',              order_qty::integer,
                'ProductID',             product_id::string,
                'SalesOrderDetailID',    sales_order_detail_id::string,
                'SpecialOfferID',        special_offer_id::string,
                'UnitPrice',             unit_price::float,
                'UnitPriceDiscount',     unit_price_discount::float
            )
        ) as order_details
    from order_details
    group by sales_order_id
),

renamed as (
    select
        o.sales_order_id                                        as sales_order_id,
        o.customer_id                                           as customer_id,
        o.account_number                                        as account_number,
        NULLIF(o.bill_to_address_id::string, 'NaN')::number     as bill_to_address_id,
        o.comment                                               as comment,
        o.credit_card_approval_code                             as credit_card_approval_code,
        NULLIF(o.credit_card_id::string, 'NaN')::number         as credit_card_id,
        NULLIF(o.currency_rate_id::string, 'NaN')::number       as currency_rate_id,
        null::string                                            as delivery_estimate,
        o.due_date                                              as due_date,
        o.freight                                               as freight,
        o.last_modified::date                                   as modified_date,
        o.online_order_flag::integer                            as online_order_flag,
        o.order_date                                            as order_date,
        d.order_details::variant                                as order_details,
        o.purchase_order_number                                 as purchase_order_number,
        NULLIF(o.revision_number::string, 'NaN')::number        as revision_number,
        o.sales_order_number                                    as sales_order_number,
        NULLIF(o.sales_person_id::string, 'NaN')::number        as sales_person_id,
        o.ship_date                                             as ship_date,
        NULLIF(o.ship_method_id::string, 'NaN')::number         as ship_method_id,
        NULLIF(o.ship_to_address_id::string, 'NaN')::number     as ship_to_address_id,
        NULLIF(o.status::string, 'NaN')::number                 as status,
        o.sub_total                                             as sub_total,
        o.tax_amt                                               as tax_amt,
        NULLIF(o.territory_id::string, 'NaN')::number           as territory_id,
        o.total_due                                             as total_due
    from orders o
    left join aggregated_details d
        on o.sales_order_id = d.sales_order_id
)

select * from renamed