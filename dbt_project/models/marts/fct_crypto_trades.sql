with staged as (
    select * from {{ ref('stg_binance_trades') }}
),

enriched as (
    select
        md5(symbol || trade_id::text)          as trade_key,
        trade_id,
        symbol,
        trade_time,
        price,
        qty,
        quote_qty,
        is_buyer_maker,
        (price * qty)                          as trade_value,
        case
            when is_buyer_maker then 'sell'
            else 'buy'
        end                                    as trade_side,
        date_trunc('day',  trade_time)         as trade_date,
        date_trunc('hour', trade_time)         as trade_hour,
        ingested_at
    from staged
)

select * from enriched