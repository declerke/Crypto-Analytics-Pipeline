with staged as (
    select * from {{ ref('stg_binance_klines') }}
),

enriched as (
    select
        md5(symbol || extract(epoch from open_time)::text) as candle_id,
        symbol,
        open_time,
        close_time,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        quote_asset_volume,
        number_of_trades,
        taker_buy_base_volume,
        taker_buy_quote_volume,
        (close_price - open_price)                                       as price_change,
        round(
            (close_price - open_price) / nullif(open_price, 0) * 100, 4
        )                                                                 as price_change_pct,
        (high_price - low_price)                                         as candle_range,
        round(
            (high_price - low_price)  / nullif(low_price,  0) * 100, 4
        )                                                                 as candle_range_pct,
        case
            when close_price >= open_price then 'bullish'
            else 'bearish'
        end                                                               as candle_direction,
        date_trunc('day',  open_time)                                    as trade_date,
        date_trunc('hour', open_time)                                    as trade_hour,
        ingested_at
    from staged
)

select * from enriched