with source as (
    select * from {{ source('raw', 'raw_binance_trades') }}
),

deduplicated as (
    select
        trade_id,
        symbol,
        price,
        qty,
        quote_qty,
        trade_time,
        is_buyer_maker,
        ingested_at,
        row_number() over (
            partition by symbol, trade_id
            order by ingested_at desc
        ) as rn
    from source
    where trade_id  is not null
      and symbol    is not null
      and price     is not null
      and qty       > 0
),

staged as (
    select
        trade_id::bigint                                     as trade_id,
        symbol,
        price::numeric(24, 8)                               as price,
        qty::numeric(36, 8)                                 as qty,
        quote_qty::numeric(36, 8)                           as quote_qty,
        to_timestamp(trade_time / 1000.0) at time zone 'UTC' as trade_time,
        is_buyer_maker,
        ingested_at
    from deduplicated
    where rn = 1
)

select * from staged