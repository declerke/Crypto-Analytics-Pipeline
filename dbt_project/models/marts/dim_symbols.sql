with tickers as (
    select * from {{ ref('stg_binance_tickers') }}
),

latest as (
    select
        *,
        row_number() over (
            partition by symbol
            order by ingested_at desc
        ) as rn
    from tickers
),

dim as (
    select
        symbol,
        last_price,
        price_change,
        price_change_percent,
        weighted_avg_price,
        prev_close_price,
        bid_price,
        ask_price,
        open_price,
        high_price,
        low_price,
        volume                          as volume_24h,
        quote_volume                    as quote_volume_24h,
        trade_count                     as trade_count_24h,
        open_time                       as window_open_time,
        close_time                      as window_close_time,
        (high_price - low_price)        as price_range_24h,
        round(
            (high_price - low_price) / nullif(low_price, 0) * 100, 4
        )                               as volatility_24h_pct,
        ingested_at                     as last_refreshed_at
    from latest
    where rn = 1
)

select * from dim