-- Post-load DQ assertions on the curated layer for {{ ds }}.
-- Every check is recorded in cma_audit.sql_assertions (evidence), then blocking checks
-- are asserted: any failure raises, fails the Airflow task and stops downstream data products.

DECLARE v_business_date DATE DEFAULT DATE '{{ ds }}';

CREATE TEMP TABLE checks AS
SELECT 'one_current_row_per_deal' AS check_name, TRUE AS blocking, COUNT(*) AS failing_rows
FROM (
  SELECT deal_id FROM `{{ params.project }}.cma_curated.deal_history`
  WHERE is_current GROUP BY deal_id HAVING COUNT(*) > 1
)
UNION ALL
SELECT 'no_overlapping_validity', TRUE, COUNT(*)
FROM `{{ params.project }}.cma_curated.deal_history` a
JOIN `{{ params.project }}.cma_curated.deal_history` b
  ON a.deal_id = b.deal_id AND a.deal_sk < b.deal_sk
 AND a.valid_from <= b.valid_to AND b.valid_from <= a.valid_to
UNION ALL
-- Every staged deal for the day must be current in curated (completeness: staging -> curated)
SELECT 'staged_deals_are_current', TRUE, COUNT(*)
FROM `{{ params.project }}.cma_staging.deal_stg` s
LEFT JOIN `{{ params.project }}.cma_curated.deal_history` h
  ON h.deal_id = s.deal_id AND h.is_current
WHERE s.business_date = v_business_date AND h.deal_id IS NULL
UNION ALL
-- Value reconciliation: USD totals of today's staged deals match their current curated rows
SELECT 'staged_vs_curated_usd_amount', TRUE,
  IF(ABS(IFNULL(SUM(s.deal_amount_usd), 0) - IFNULL(SUM(h.deal_amount_usd), 0)) > 0.01, 1, 0)
FROM `{{ params.project }}.cma_staging.deal_stg` s
LEFT JOIN `{{ params.project }}.cma_curated.deal_history` h
  ON h.deal_id = s.deal_id AND h.is_current
WHERE s.business_date = v_business_date
UNION ALL
SELECT 'bank_share_not_above_deal', TRUE, COUNTIF(bank_share_amount > deal_amount)
FROM `{{ params.project }}.cma_curated.deal_history` WHERE is_current
UNION ALL
-- Non-blocking: unresolved borrowers are tracked and alerted on, not blocked
SELECT 'unresolved_borrowers', FALSE, COUNTIF(NOT party_resolved)
FROM `{{ params.project }}.cma_curated.deal_history` WHERE is_current;

INSERT INTO `{{ params.project }}.cma_audit.sql_assertions` (run_id, check_name, business_date, failing_rows, passed, checked_at)
SELECT '{{ run_id }}', check_name, v_business_date, failing_rows, failing_rows = 0, CURRENT_TIMESTAMP()
FROM checks;

ASSERT (SELECT COUNTIF(blocking AND failing_rows > 0) FROM checks) = 0
  AS 'Blocking DQ assertions failed for {{ ds }} - see cma_audit.sql_assertions for run {{ run_id }}';
