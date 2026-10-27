-- Staging -> curated SCD type 2 MERGE for one business date.
-- Run by Composer (BigQueryInsertJobOperator); Jinja: {{ params.project }}, {{ ds }}, {{ run_id }}.
--
-- * new deal                -> insert current row
-- * tracked attribute change -> close current row (valid_to = business_date - 1), insert new current row
-- * unchanged               -> no-op (so re-running a date is idempotent)
-- Wrapped in a transaction: either the whole day applies or nothing does.

DECLARE v_business_date DATE DEFAULT DATE '{{ ds }}';
DECLARE open_end DATE DEFAULT DATE '9999-12-31';

CREATE TEMP TABLE src AS
SELECT
  s.*,
  TO_HEX(MD5(TO_JSON_STRING(STRUCT(
    s.deal_version, s.deal_name, s.borrower_party_id, s.product_type, s.deal_role, s.currency,
    s.deal_amount, s.bank_share_amount, s.trade_date, s.settlement_date, s.deal_status
  )))) AS row_hash
FROM `{{ params.project }}.cma_staging.deal_stg` AS s
WHERE s.business_date = v_business_date;

-- Guard: an empty staging partition means the upstream load did not run; never "close" every deal.
IF (SELECT COUNT(*) FROM src) = 0 THEN
  RAISE USING MESSAGE = FORMAT('No staging rows for %t; refusing to run SCD2 merge', v_business_date);
END IF;

BEGIN
  BEGIN TRANSACTION;

MERGE `{{ params.project }}.cma_curated.deal_history` AS tgt
USING (
  -- Rows that change or are new must both close the old row (match on deal_id)
  -- and insert a new row (deal_id NULL forces NOT MATCHED). Standard SCD2 union trick.
  SELECT src.deal_id AS merge_key, src.* FROM src
  UNION ALL
  SELECT CAST(NULL AS STRING) AS merge_key, src.*
  FROM src
  JOIN `{{ params.project }}.cma_curated.deal_history` AS cur
    ON cur.deal_id = src.deal_id AND cur.is_current
  WHERE cur.row_hash != src.row_hash
) AS s
ON tgt.deal_id = s.merge_key AND tgt.is_current

WHEN MATCHED AND tgt.row_hash != s.row_hash THEN UPDATE SET
  valid_to   = DATE_SUB(v_business_date, INTERVAL 1 DAY),
  is_current = FALSE,
  updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED BY TARGET THEN INSERT (
  deal_sk, deal_id, deal_version, deal_name, borrower_party_id, borrower_name, borrower_lei, borrower_country,
  borrower_sector, party_resolved, product_type, deal_role, currency, deal_amount, bank_share_amount,
  bank_share_pct, fx_rate_to_usd, deal_amount_usd, bank_share_usd, trade_date, settlement_date, booking_system,
  deal_status, row_hash, valid_from, valid_to, is_current, source_system, run_id, updated_at
) VALUES (
  CONCAT(s.deal_id, '|', CAST(v_business_date AS STRING)), s.deal_id, s.deal_version, s.deal_name, s.borrower_party_id,
  s.borrower_name, s.borrower_lei, s.borrower_country, s.borrower_sector, s.party_resolved, s.product_type,
  s.deal_role, s.currency, s.deal_amount, s.bank_share_amount, s.bank_share_pct, s.fx_rate_to_usd,
  s.deal_amount_usd, s.bank_share_usd, s.trade_date, s.settlement_date, s.booking_system, s.deal_status,
  s.row_hash, v_business_date, open_end, TRUE, s.source_system, '{{ run_id }}', CURRENT_TIMESTAMP()
);

  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  -- Roll back, then re-raise so the Airflow task fails with the real cause
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = FORMAT('SCD2 merge failed for %t: %s', v_business_date, @@error.message);
END;
