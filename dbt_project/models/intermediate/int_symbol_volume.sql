with kline_daily as (
    select
        symbol,
        date_trunc('day', open_time)  as trade_date,
        sum(volume)                   as kline_base_volume,
        sum(quote_asset_volume)       as kline_quote_volume,
        count(*)                      as candle_count,
        avg(close_price)              as avg_close_price,
        max(high_price)               as daily_high,
        min(low_price)                as daily_low
    from {{ ref('stg_binance_klines') }}
    group by 1, 2
),

trade_daily as (
    select
        symbol,
        date_trunc('day', trade_time) as trade_date,
        sum(qty)                      as trade_base_qty,
        sum(quote_qty)                as trade_quote_qty,
        count(*)                      as trade_count,
        avg(price)                    as avg_trade_price,
        sum(price * qty)              as total_trade_value
    from {{ ref('stg_binance_trades') }}
    group by 1, 2
),

joined as (
    select
        coalesce(k.symbol,     t.symbol)     as symbol,
        coalesce(k.trade_date, t.trade_date) as trade_date,
        coalesce(k.kline_base_volume,  0)    as kline_base_volume,
        coalesce(k.kline_quote_volume, 0)    as kline_quote_volume,
        coalesce(k.candle_count,       0)    as candle_count,
        coalesce(k.avg_close_price,    0)    as avg_close_price,
        coalesce(k.daily_high,         0)    as daily_high,
        coalesce(k.daily_low,          0)    as daily_low,
        coalesce(t.trade_base_qty,     0)    as trade_base_qty,
        coalesce(t.trade_quote_qty,    0)    as trade_quote_qty,
        coalesce(t.trade_count,        0)    as trade_count,
        coalesce(t.avg_trade_price,    0)    as avg_trade_price,
        coalesce(t.total_trade_value,  0)    as total_trade_value
    from kline_daily k
    full outer join trade_daily t
        on  k.symbol     = t.symbol
        and k.trade_date = t.trade_date
)

select * from joined