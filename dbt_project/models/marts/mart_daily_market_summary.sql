with daily_base as (
    select
        symbol,
        trade_date,
        avg(close_price)              as avg_close_price,
        max(high_price)               as daily_high,
        min(low_price)                as daily_low,
        sum(volume)                   as total_base_volume,
        sum(quote_asset_volume)       as total_quote_volume,
        sum(number_of_trades)         as total_trade_count,
        first_value(open_price) over (
            partition by symbol, trade_date
            order by open_time asc
            rows between unbounded preceding and unbounded following
        )                             as day_open,
        last_value(close_price) over (
            partition by symbol, trade_date
            order by open_time asc
            rows between unbounded preceding and unbounded following
        )                             as day_close
    from {{ ref('fct_crypto_candles') }}
    group by
        symbol,
        trade_date,
        open_time,
        open_price,
        close_price
),

daily_distinct as (
    select distinct
        symbol,
        trade_date,
        avg_close_price,
        daily_high,
        daily_low,
        total_base_volume,
        total_quote_volume,
        total_trade_count,
        day_open,
        day_close
    from daily_base
),

with_derived as (
    select
        symbol,
        trade_date,
        avg_close_price,
        daily_high,
        daily_low,
        day_open,
        day_close,
        total_base_volume,
        total_quote_volume,
        total_trade_count,
        (daily_high - daily_low)                                        as daily_price_range,
        round(
            (daily_high - daily_low) / nullif(daily_low, 0) * 100, 4
        )                                                               as daily_volatility_pct,
        (day_close - day_open)                                          as daily_price_change,
        round(
            (day_close - day_open) / nullif(day_open, 0) * 100, 4
        )                                                               as daily_price_change_pct,
        round(
            avg(avg_close_price) over (
                partition by symbol
                order by trade_date
                rows between 6 preceding and current row
            ), 8
        )                                                               as rolling_7day_avg_price,
        row_number() over (
            partition by trade_date
            order by total_base_volume desc
        )                                                               as volume_rank_on_day
    from daily_distinct
)

select * from with_derived