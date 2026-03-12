with klines as (
    select * from {{ ref('stg_binance_klines') }}
),

hourly_aggregates as (
    select
        symbol,
        date_trunc('hour', open_time)                              as trade_hour,
        count(*)                                                   as candle_count,
        avg(close_price)                                           as avg_close_price,
        min(low_price)                                             as hour_low,
        max(high_price)                                            as hour_high,
        first_value(open_price) over (
            partition by symbol, date_trunc('hour', open_time)
            order by open_time asc
            rows between unbounded preceding and unbounded following
        )                                                          as hour_open,
        last_value(close_price) over (
            partition by symbol, date_trunc('hour', open_time)
            order by open_time asc
            rows between unbounded preceding and unbounded following
        )                                                          as hour_close,
        sum(volume)                                                as hour_volume,
        sum(quote_asset_volume)                                    as hour_quote_volume,
        sum(number_of_trades)                                      as hour_trade_count
    from klines
    group by
        symbol,
        date_trunc('hour', open_time),
        open_time,
        open_price,
        close_price
),

distinct_hours as (
    select distinct
        symbol,
        trade_hour,
        candle_count,
        avg_close_price,
        hour_low,
        hour_high,
        hour_open,
        hour_close,
        hour_volume,
        hour_quote_volume,
        hour_trade_count
    from hourly_aggregates
),

with_metrics as (
    select
        symbol,
        trade_hour,
        candle_count,
        avg_close_price,
        hour_open,
        hour_high,
        hour_low,
        hour_close,
        hour_volume,
        hour_quote_volume,
        hour_trade_count,
        (hour_high - hour_low)                                      as price_range,
        round(
            (hour_high - hour_low) / nullif(hour_low, 0) * 100, 4
        )                                                           as volatility_pct,
        (hour_close - hour_open)                                    as price_change,
        round(
            (hour_close - hour_open) / nullif(hour_open, 0) * 100, 4
        )                                                           as price_change_pct
    from distinct_hours
)

select * from with_metrics