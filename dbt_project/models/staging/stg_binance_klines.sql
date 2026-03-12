with source as (
    select * from {{ source('raw', 'raw_binance_klines') }}
),

deduplicated as (
    select
        symbol,
        open_time,
        open,
        high,
        low,
        close,
        volume,
        close_time,
        quote_asset_volume,
        number_of_trades,
        taker_buy_base_asset_volume,
        taker_buy_quote_asset_volume,
        ingested_at,
        row_number() over (
            partition by symbol, open_time
            order by ingested_at desc
        ) as rn
    from source
    where symbol is not null
      and open_time is not null
      and close is not null
      and volume >= 0
),

staged as (
    select
        symbol,
        to_timestamp(open_time  / 1000.0) at time zone 'UTC' as open_time,
        to_timestamp(close_time / 1000.0) at time zone 'UTC' as close_time,
        open::numeric(24, 8)                                  as open_price,
        high::numeric(24, 8)                                  as high_price,
        low::numeric(24, 8)                                   as low_price,
        close::numeric(24, 8)                                 as close_price,
        volume::numeric(36, 8)                                as volume,
        quote_asset_volume::numeric(36, 8)                    as quote_asset_volume,
        number_of_trades::bigint                              as number_of_trades,
        taker_buy_base_asset_volume::numeric(36, 8)           as taker_buy_base_volume,
        taker_buy_quote_asset_volume::numeric(36, 8)          as taker_buy_quote_volume,
        ingested_at
    from deduplicated
    where rn = 1
)

select * from staged