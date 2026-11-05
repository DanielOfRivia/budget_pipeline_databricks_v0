--Declarative pipeline
------------------------------------------------------------------------------------------------
--bronze
CREATE OR REFRESH STREAMING TABLE bronze_transactions_v2 (
    CONSTRAINT clean_parse  EXPECT (_rescued_data IS NULL) 
)
COMMENT "Raw transaction based on csv extracts from the bank"
AS
SELECT
    `Account Type`          AS account_type,
    `Account Number`        AS account_number,
    `Transaction Date`      AS transaction_date,
    `Cheque Number`         AS cheque_number,
    `Description 1`         AS description_1,
    `Description 2`         AS description_2,
    `CAD$`                  AS amountCAD,
    `USD$`                  AS amountUSD,
    _metadata.file_path     AS _source_file,
    current_timestamp()     AS _ingested_at,
    _rescued_data
FROM STREAM read_files(
  '${source_volume_path}download-transactions*.csv',
  format => 'csv',
  header => true,
  schema => '`Account Type` STRING, `Account Number` STRING, `Transaction Date` STRING, `Cheque Number` STRING, `Description 1` STRING, `Description 2` STRING, `CAD$` STRING,`USD$` STRING',
  enforceSchema => false,
  rescuedDataColumn => '_rescued_data'
);