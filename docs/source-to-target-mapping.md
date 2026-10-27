# Source-to-target mapping: syndicated deals

Source: booking-system EOD extract `deals_<yyyymmdd>.csv` (LoanIQ / Murex), one row per deal version.
Target: `cma_curated.deal` (Hive, partitioned by `business_date`) and `cma_staging.deal_stg` →
`cma_curated.deal_history` (BigQuery, SCD2). Owner: CMA Data Office. Classification: Confidential.

| Target column | Source | Transformation | Rule(s) | On failure |
|---|---|---|---|---|
| deal_id | deal_id | trim | not null | quarantine |
| deal_version | deal_version | trim → INT; keep max version per deal_id | digits only | quarantine; older versions counted as superseded |
| deal_name | deal_name | trim, blank → NULL | - | - |
| borrower_party_id | borrower_party_id | trim | not null; must exist in party reference (Warn, ≤ 2%) | quarantine / flag `party_resolved = false` |
| borrower_name, borrower_lei, borrower_country, borrower_sector | party reference | left join on party_id (active parties) | - | NULL when unresolved |
| party_resolved | derived | `party_id` found in reference | - | - |
| product_type | product_type | trim, upper | not null; in (SYNDICATED_LOAN, BOND, ECM, LEVERAGED_LOAN, PRIVATE_PLACEMENT) | quarantine |
| deal_role | deal_role | trim, upper | in (BOOKRUNNER, MANDATED_LEAD_ARRANGER, ARRANGER, PARTICIPANT, AGENT) | quarantine |
| currency | currency | trim, upper | not null; ISO-4217 pattern `^[A-Z]{3}$` | quarantine |
| deal_amount | deal_amount | → DECIMAL(20,2) | not null; numeric; > 0 | quarantine |
| bank_share_amount | bank_share_amount | → DECIMAL(20,2) | numeric; ≤ deal_amount | quarantine |
| bank_share_pct | derived | bank_share_amount / deal_amount × 100, 4 dp | 0–100 (gate) | block write |
| fx_rate_to_usd | FX reference | latest rate on or before business_date for currency | not null (gate) | block write |
| deal_amount_usd, bank_share_usd | derived | amount × fx_rate_to_usd, 2 dp | reconciled in notebook | - |
| trade_date | trade_date | `yyyy-MM-dd` → DATE | not null; ISO pattern; real date | quarantine |
| settlement_date | settlement_date | `yyyy-MM-dd` → DATE | ISO pattern; ≥ trade_date | quarantine |
| booking_system, deal_status | same | trim, upper | status in (MANDATED, LAUNCHED, PRICED, SIGNED, CLOSED, CANCELLED) | quarantine |
| last_updated_ts | last_updated_ts | → TIMESTAMP | tie-break for latest version | - |
| source_system, run_id, ingested_at | pipeline | lineage | - | - |
| business_date | run parameter | partition key | - | - |

Dataset-level gate before every curated write: minimum row count, `deal_id` unique, FX coverage,
referential integrity (Warn), share percentage range. Rule names are identical across Spark
(`DealRules`) and Beam (`DealRecordValidator`) and appear verbatim in `_reject_reasons`.
