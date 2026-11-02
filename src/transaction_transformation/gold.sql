--gold
CREATE OR REFRESH MATERIALIZED VIEW gold_transactions_v2 
COMMENT "Aggregated transactions by account and month"
AS
SELECT
  account_number,
  CAST(date_trunc('MONTH', transaction_date) AS DATE) as month,
  sum(amount) as amount,
  count(*) as n_transactions,
  AVG(amount) as avg_transaction
FROM silver_transactions_v2
GROUP BY account_number, date_trunc('MONTH', transaction_date)
ORDER BY date_trunc('MONTH', transaction_date) desc;