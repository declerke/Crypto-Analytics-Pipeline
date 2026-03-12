with source as (
    select * from {{ source('raw', 'raw_binance_tickers') }}
),

staged as (
    select
        symbol,
        price_change::numeric(24, 8)           as price_change,
        price_change_percent::numeric(10, 4)   as price_change_percent,
        weighted_avg_price::numeric(24, 8)     as weighted_avg_price,
        prev_close_price::numeric(24, 8)       as prev_close_price,
        last_price::numeric(24, 8)             as last_price,
        last_qty::numeric(36, 8)               as last_qty,
        bid_price::numeric(24, 8)              as bid_price,
        ask_price::numeric(24, 8)              as ask_price,
        open_price::numeric(24, 8)             as open_price,
        high_price::numeric(24, 8)             as high_price,
        low_price::numeric(24, 8)              as low_price,
        volume::numeric(36, 8)                 as volume,
        quote_volume::numeric(36, 8)           as quote_volume,
        to_timestamp(open_time  / 1000.0) at time zone 'UTC' as open_time,
        to_timestamp(close_time / 1000.0) at time zone 'UTC' as close_time,
        trade_count::bigint                    as trade_count,
        ingested_at
    from source
    where symbol     is not null
      and last_price is not null
)

select * from staged