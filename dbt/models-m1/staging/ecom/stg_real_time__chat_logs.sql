with

source as (
    select * from {{ source('raw_ext', 'chat_logs_raw') }}
),

renamed as (
    select
        raw:_id::string             as chat_id,
        raw:session_id::string      as session_id,
        raw:customer_id::string     as customer_id,
        raw:message::string         as message,
        raw:last_modified::timestamp as last_modified
    from source
)

select * from renamed