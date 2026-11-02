CREATE OR REFRESH MATERIALIZED VIEW silver_transactions_v2(
    CONSTRAINT valid_date   EXPECT (transaction_date IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT has_amount   EXPECT (amount IS NOT NULL)
)
COMMENT "Cleaned and deduped transactions with column types enforced"
AS 
WITH numbered AS (
  -- 1. Number identical rows within each file
  SELECT
    _source_file, _ingested_at,
    account_type, account_number, transaction_date, cheque_number,
    description_1, description_2, amountCAD, amountUSD,
    row_number() OVER (
      PARTITION BY _source_file,
                   account_type, account_number, transaction_date, cheque_number,
                   description_1, description_2, amountCAD, amountUSD
      ORDER BY _ingested_at
    ) AS occurrence
  FROM bronze_transactions_v2
),
first_seen AS (
  -- 2. Across files, keep the first file that produced each (business key, occurrence)
  SELECT
    _source_file, _ingested_at,
    account_type, account_number, transaction_date, cheque_number,
    description_1, description_2, amountCAD, amountUSD, occurrence,
    row_number() OVER (
      PARTITION BY account_type, account_number, transaction_date, cheque_number,
                   description_1, description_2, amountCAD, amountUSD,
                   occurrence
      ORDER BY _ingested_at, _source_file
    ) AS file_rank
  FROM numbered
)
SELECT
    sha2(to_json(named_struct(
    'account_type',     account_type,
    'account_number',   account_number,
    'transaction_date', transaction_date,
    'cheque_number',    cheque_number,
    'description_1',    description_1,
    'description_2',    description_2,
    'amountCAD',        amountCAD,
    'amountUSD',        amountUSD,
    'occurrence',       occurrence
  )), 256) AS transaction_id,
    account_type,
    account_number,
    try_to_date(transaction_date, 'M/d/yyyy') AS transaction_date,
    description_1 AS description,
    description_2 as extra_details,
    try_cast(amountCAD as decimal(10,2)) * (-1) as amount
FROM first_seen
WHERE file_rank = 1 and not (description_1 like "AUTOMATIC PAYMENT%" and try_cast(amountCAD as decimal(10,2)) > 0);