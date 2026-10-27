-- Front-office data product: monthly bank league-table volumes by product, sector, country and role.
-- Rebuilds only the month containing {{ ds }} (partition-scoped DELETE + INSERT in one transaction),
-- so daily re-runs are idempotent and cheap. Consumed by Tableau via cma_curated.league_table_monthly.

DECLARE v_month DATE DEFAULT DATE_TRUNC(DATE '{{ ds }}', MONTH);

BEGIN
  BEGIN TRANSACTION;

  DELETE FROM `{{ params.project }}.cma_curated.league_table_monthly` WHERE month = v_month;

  INSERT INTO `{{ params.project }}.cma_curated.league_table_monthly`
  WITH monthly AS (
    SELECT
      DATE_TRUNC(trade_date, MONTH)        AS month,
      product_type,
      COALESCE(borrower_sector, 'UNKNOWN') AS borrower_sector,
      COALESCE(borrower_country, 'UNKNOWN') AS borrower_country,
      deal_role,
      COUNT(DISTINCT deal_id)              AS deal_count,
      SUM(deal_amount_usd)                 AS total_deal_usd,
      SUM(bank_share_usd)                  AS bank_share_usd,
      ROUND(AVG(bank_share_pct), 4)        AS avg_bank_share_pct
    FROM `{{ params.project }}.cma_curated.deal_current`
    WHERE DATE_TRUNC(trade_date, MONTH) IN (v_month, DATE_SUB(v_month, INTERVAL 1 MONTH))
      AND deal_status != 'CANCELLED'
    GROUP BY 1, 2, 3, 4, 5
  )
  SELECT
    cur.month, cur.product_type, cur.borrower_sector, cur.borrower_country, cur.deal_role,
    cur.deal_count, cur.total_deal_usd, cur.bank_share_usd, cur.avg_bank_share_pct,
    prev.bank_share_usd AS bank_share_usd_prev_month,
    ROUND(SAFE_DIVIDE(cur.bank_share_usd - prev.bank_share_usd, prev.bank_share_usd) * 100, 2) AS mom_change_pct,
    CURRENT_TIMESTAMP() AS refreshed_at,
    '{{ run_id }}' AS run_id
  FROM monthly AS cur
  LEFT JOIN monthly AS prev
    ON  prev.month = DATE_SUB(cur.month, INTERVAL 1 MONTH)
    AND prev.product_type = cur.product_type
    AND prev.borrower_sector = cur.borrower_sector
    AND prev.borrower_country = cur.borrower_country
    AND prev.deal_role IS NOT DISTINCT FROM cur.deal_role
  WHERE cur.month = v_month;

  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = FORMAT('league table build failed for %t: %s', v_month, @@error.message);
END;
