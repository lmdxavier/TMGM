WITH date_range AS (
    -- Generate all dates between June 1, 2020 and September 30, 2020
    SELECT generate_series(
        '2020-06-01'::date, 
        '2020-09-30'::date, 
        '1 day'::interval
    )::date AS dt_report
),

valid_users as (
SELECT
t.login_hash,
t.server_hash,
symbol,
currency
FROM trades t
INNER JOIN users u ON t.login_hash = u.login_hash
WHERE u.enable = 1
GROUP BY 1,2,3,4
),

users_by_day as (
SELECT dt_report, login_hash, server_hash, symbol, currency
FROM date_range
CROSS JOIN valid_users),

volume_p7d as (
SELECT
u.dt_report, u.login_hash, u.server_hash, u.symbol, u.currency, coalesce(sum(t.volume),0) as sum_volume_prev_7d
FROM users_by_day u
LEFT JOIN trades t
ON date(t.close_time) <= u.dt_report and date(t.close_time) >= (u.dt_report :: date - '6 day'::interval)
and u.login_hash = t.login_hash
and u.server_hash = t.server_hash
and u.symbol = t.symbol
GROUP BY 1,2,3,4,5),

volume_pall as (
SELECT u.dt_report, u.login_hash, u.server_hash, u.symbol, u.currency, sum_volume_prev_7d, coalesce(sum(t.volume),0) as sum_volume_prev_all
FROM volume_p7d u
LEFT JOIN trades t
ON date(t.close_time) <= u.dt_report
and u.login_hash = t.login_hash
and u.server_hash = t.server_hash
and u.symbol = t.symbol
GROUP BY 1,2,3,4,5,6),

first_trade as (
SELECT u.dt_report, u.login_hash, u.server_hash, u.symbol, u.currency, sum_volume_prev_7d, sum_volume_prev_all, min(t.close_time) as date_first_trade
FROM volume_pall u
LEFT JOIN trades t
ON u.login_hash = t.login_hash
and u.server_hash = t.server_hash
and u.symbol = t.symbol
GROUP BY 1,2,3,4,5,6,7),

volume_2020_08 as (
SELECT u.dt_report, u.login_hash, u.server_hash, u.symbol, u.currency, sum_volume_prev_7d, sum_volume_prev_all, date_first_trade, coalesce(sum(t.volume),0) AS sum_volume_2020_08
FROM first_trade u
LEFT JOIN trades t
ON date(t.close_time) <= u.dt_report :: date and date_trunc('month', t.close_time) = '2020-08-01'::date
and u.login_hash = t.login_hash
and u.server_hash = t.server_hash
and u.symbol = t.symbol
GROUP BY 1,2,3,4,5,6,7,8),

volume_p7d_rank as (
SELECT u.*, coalesce(sum(t.volume),0) as sum_volume_prev_7d_rank
FROM volume_2020_08 u
LEFT JOIN trades t
ON date(t.close_time) <= u.dt_report :: date and date(t.close_time) >= (u.dt_report :: date - '6 day'::interval)
and u.login_hash = t.login_hash
and u.symbol = t.symbol
GROUP BY 1,2,3,4,5,6,7,8,9),

count_p7d_rank as (
SELECT u.*, coalesce(count(t.ticket_hash),0) as count_trade_prev_7d_rank
FROM volume_p7d_rank u
LEFT JOIN trades t
ON date(t.close_time) <= u.dt_report :: date and date(t.close_time) >= (u.dt_report :: date - '6 day'::interval)
and u.login_hash = t.login_hash
GROUP BY 1,2,3,4,5,6,7,8,9,10),

ranked as (
SELECT
*,
DENSE_RANK() OVER (PARTITION BY login_hash, symbol ORDER BY sum_volume_prev_7d_rank DESC) AS rank_volume_symbol_prev_7d,
DENSE_RANK() OVER (PARTITION BY login_hash ORDER BY count_trade_prev_7d_rank DESC) AS rank_count_prev_7d,
ROW_NUMBER() OVER (ORDER BY dt_report, login_hash, server_hash, symbol) AS row_number
FROM count_p7d_rank),

ordered as (
SELECT 
dt_report,
login_hash,
server_hash,
symbol,
currency,
sum_volume_prev_7d,
sum_volume_prev_all,
rank_volume_symbol_prev_7d,
rank_count_prev_7d,
sum_volume_2020_08,
date_first_trade,
row_number
FROM ranked
ORDER BY row_number DESC)

SELECT 
row_number() over() as id, *
FROM ordered