-- BigQuery DDL for the CMA deal platform.
-- Rendered by Ansible (roles/gcp_deploy) with {{ project }} / {{ location }} and applied with `bq query`.
-- Every statement is idempotent so deployments and rollbacks can be re-run.

CREATE SCHEMA IF NOT EXISTS `{{ project }}.cma_raw`        OPTIONS (location = '{{ location }}', description = 'Bronze: data as received');
CREATE SCHEMA IF NOT EXISTS `{{ project }}.cma_ref`        OPTIONS (location = '{{ location }}', description = 'Reference data: party, FX');
CREATE SCHEMA IF NOT EXISTS `{{ project }}.cma_staging`    OPTIONS (location = '{{ location }}', description = 'Validated, enriched loads awaiting MERGE', default_table_expiration_days = 35);
CREATE SCHEMA IF NOT EXISTS `{{ project }}.cma_curated`    OPTIONS (location = '{{ location }}', description = 'Gold: governed CMA data products');
CREATE SCHEMA IF NOT EXISTS `{{ project }}.cma_quarantine` OPTIONS (location = '{{ location }}', description = 'Rejected records with reasons', default_table_expiration_days = 90);
CREATE SCHEMA IF NOT EXISTS `{{ project }}.cma_audit`      OPTIONS (location = '{{ location }}', description = 'Run, DQ and reconciliation evidence');

-- ---------------------------------------------------------------- reference
CREATE TABLE IF NOT EXISTS `{{ project }}.cma_ref.party` (
  party_id STRING NOT NULL, legal_name STRING, lei STRING, country_code STRING, sector STRING, is_active BOOL
) OPTIONS (description = 'Party master extract');

CREATE TABLE IF NOT EXISTS `{{ project }}.cma_ref.fx_rate` (
  currency STRING NOT NULL, rate_to_usd NUMERIC(18, 8) NOT NULL, rate_date DATE NOT NULL
) PARTITION BY rate_date CLUSTER BY currency;

-- ---------------------------------------------------------------- staging (written by Spark on Dataproc)
CREATE TABLE IF NOT EXISTS `{{ project }}.cma_staging.deal_stg` (
  deal_id STRING NOT NULL, deal_version INT64 NOT NULL, deal_name STRING,
  borrower_party_id STRING NOT NULL, borrower_name STRING, borrower_lei STRING, borrower_country STRING, borrower_sector STRING,
  party_resolved BOOL NOT NULL, product_type STRING NOT NULL, deal_role STRING, currency STRING NOT NULL,
  deal_amount NUMERIC(20, 2) NOT NULL, bank_share_amount NUMERIC(20, 2), bank_share_pct NUMERIC(9, 4),
  fx_rate_to_usd NUMERIC(18, 8), deal_amount_usd NUMERIC(20, 2), bank_share_usd NUMERIC(20, 2),
  trade_date DATE NOT NULL, settlement_date DATE, booking_system STRING, deal_status STRING,
  last_updated_ts TIMESTAMP, source_system STRING, run_id STRING, ingested_at TIMESTAMP,
  business_date DATE NOT NULL
)
PARTITION BY business_date
CLUSTER BY deal_id
OPTIONS (require_partition_filter = TRUE, partition_expiration_days = 35);

CREATE TABLE IF NOT EXISTS `{{ project }}.cma_staging.deal_intraday`
LIKE `{{ project }}.cma_staging.deal_stg`;

-- ---------------------------------------------------------------- curated: SCD2 history + current view
CREATE TABLE IF NOT EXISTS `{{ project }}.cma_curated.deal_history` (
  deal_sk STRING NOT NULL OPTIONS (description = 'Surrogate key: deal_id + valid_from'),
  deal_id STRING NOT NULL, deal_version INT64 NOT NULL, deal_name STRING,
  borrower_party_id STRING NOT NULL, borrower_name STRING, borrower_lei STRING, borrower_country STRING, borrower_sector STRING,
  party_resolved BOOL, product_type STRING NOT NULL, deal_role STRING, currency STRING NOT NULL,
  deal_amount NUMERIC(20, 2) NOT NULL, bank_share_amount NUMERIC(20, 2), bank_share_pct NUMERIC(9, 4),
  fx_rate_to_usd NUMERIC(18, 8), deal_amount_usd NUMERIC(20, 2), bank_share_usd NUMERIC(20, 2),
  trade_date DATE NOT NULL, settlement_date DATE, booking_system STRING, deal_status STRING,
  row_hash STRING NOT NULL OPTIONS (description = 'Hash of tracked attributes - a change creates a new version'),
  valid_from DATE NOT NULL, valid_to DATE NOT NULL, is_current BOOL NOT NULL,
  source_system STRING, run_id STRING, updated_at TIMESTAMP
)
PARTITION BY valid_from
CLUSTER BY is_current, borrower_party_id, product_type
OPTIONS (description = 'CMA deals, SCD type 2. Owner: CMA Data Office. Classification: Confidential.');

CREATE OR REPLACE VIEW `{{ project }}.cma_curated.deal_current` AS
SELECT * EXCEPT (valid_to, is_current)
FROM `{{ project }}.cma_curated.deal_history`
WHERE is_current;

CREATE TABLE IF NOT EXISTS `{{ project }}.cma_curated.deal_history_migrated` (
  deal_id STRING, deal_version INT64, deal_name STRING, borrower_party_id STRING, borrower_name STRING,
  borrower_lei STRING, borrower_country STRING, borrower_sector STRING, party_resolved BOOL, product_type STRING,
  deal_role STRING, currency STRING, deal_amount NUMERIC(20, 2), bank_share_amount NUMERIC(20, 2),
  bank_share_pct NUMERIC(9, 4), fx_rate_to_usd NUMERIC(18, 8), deal_amount_usd NUMERIC(20, 2),
  bank_share_usd NUMERIC(20, 2), trade_date DATE, settlement_date DATE, booking_system STRING, deal_status STRING,
  last_updated_ts TIMESTAMP, source_system STRING, run_id STRING, ingested_at TIMESTAMP, business_date DATE
) PARTITION BY business_date
OPTIONS (description = 'Hadoop cma_curated.deal history migrated by HiveToBigQueryMigrationJob');

-- Front-office data product: bank league-table volumes
CREATE TABLE IF NOT EXISTS `{{ project }}.cma_curated.league_table_monthly` (
  month DATE NOT NULL, product_type STRING NOT NULL, borrower_sector STRING, borrower_country STRING, deal_role STRING,
  deal_count INT64, total_deal_usd NUMERIC, bank_share_usd NUMERIC, avg_bank_share_pct NUMERIC,
  bank_share_usd_prev_month NUMERIC, mom_change_pct NUMERIC, refreshed_at TIMESTAMP, run_id STRING
) PARTITION BY month CLUSTER BY product_type, borrower_sector;

-- ---------------------------------------------------------------- raw / quarantine / audit
CREATE TABLE IF NOT EXISTS `{{ project }}.cma_raw.deal_event_stream` (
  deal_id STRING, deal_version STRING, deal_name STRING, borrower_party_id STRING, product_type STRING,
  deal_role STRING, currency STRING, deal_amount STRING, bank_share_amount STRING, trade_date STRING,
  settlement_date STRING, booking_system STRING, deal_status STRING, last_updated_ts STRING,
  _corrupt_record STRING, event_id STRING, event_type STRING, event_ts TIMESTAMP,
  kafka_topic STRING, kafka_partition INT64, kafka_offset INT64, kafka_timestamp TIMESTAMP, ingest_date DATE
) PARTITION BY ingest_date CLUSTER BY event_id;

-- Written by the Dataflow pipeline (Pub/Sub -> Beam -> BigQuery Storage Write API)
CREATE TABLE IF NOT EXISTS `{{ project }}.cma_raw.deal_event_dataflow` (
  deal_id STRING NOT NULL, deal_version INT64, deal_name STRING, borrower_party_id STRING, product_type STRING,
  deal_role STRING, currency STRING, deal_amount NUMERIC(20, 2), bank_share_amount NUMERIC(20, 2),
  trade_date DATE, settlement_date DATE, booking_system STRING, deal_status STRING, last_updated_ts TIMESTAMP,
  event_id STRING NOT NULL, event_type STRING, event_ts TIMESTAMP, pubsub_message_id STRING, processed_at TIMESTAMP
) PARTITION BY DATE(processed_at) CLUSTER BY deal_id
OPTIONS (description = 'Validated deal events from Pub/Sub via Dataflow; rejects go to the dead-letter topic');

CREATE TABLE IF NOT EXISTS `{{ project }}.cma_quarantine.deal_reject` (
  deal_id STRING, deal_version STRING, deal_name STRING, borrower_party_id STRING, product_type STRING,
  deal_role STRING, currency STRING, deal_amount STRING, bank_share_amount STRING, trade_date STRING,
  settlement_date STRING, booking_system STRING, deal_status STRING, last_updated_ts STRING,
  _raw_record STRING, _reject_reasons ARRAY<STRING>, _rejected_at TIMESTAMP,
  event_id STRING, event_type STRING, event_ts TIMESTAMP,
  kafka_topic STRING, kafka_partition INT64, kafka_offset INT64, kafka_timestamp TIMESTAMP,
  business_date DATE, run_id STRING, source_system STRING
) PARTITION BY business_date;

CREATE TABLE IF NOT EXISTS `{{ project }}.cma_audit.dq_results` (
  rule STRING, dimension STRING, severity STRING, checked INT64, failed INT64, failureRatio FLOAT64,
  threshold FLOAT64, passed BOOL, dataset STRING, run_id STRING, business_date DATE, evaluated_at TIMESTAMP
) PARTITION BY business_date CLUSTER BY dataset, passed;

CREATE TABLE IF NOT EXISTS `{{ project }}.cma_audit.migration_log` (
  run_id STRING, table_name STRING, partition_date DATE, rows INT64, status STRING, detail STRING, logged_at TIMESTAMP
) PARTITION BY DATE(logged_at);

CREATE TABLE IF NOT EXISTS `{{ project }}.cma_audit.stream_batch` (
  query_name STRING, batch_id INT64, events INT64, rejected INT64, committed_at TIMESTAMP
) PARTITION BY DATE(committed_at) CLUSTER BY query_name, batch_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.cma_audit.sql_assertions` (
  run_id STRING, check_name STRING, business_date DATE, failing_rows INT64, passed BOOL, checked_at TIMESTAMP
) PARTITION BY business_date;
