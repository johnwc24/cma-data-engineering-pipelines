-- Hive DDL for the CMA deal pipeline.
-- Applied by Ansible (roles/hadoop_deploy) with:  beeline -u $HIVE_JDBC --hivevar env_suffix=_dev -f ddl.hql
-- All statements are idempotent (IF NOT EXISTS) so deployments can be re-run safely.

CREATE DATABASE IF NOT EXISTS cma_ref${env_suffix};
CREATE DATABASE IF NOT EXISTS cma_curated${env_suffix};
CREATE DATABASE IF NOT EXISTS cma_quarantine${env_suffix};
CREATE DATABASE IF NOT EXISTS cma_audit${env_suffix};

-- ---------------------------------------------------------------- reference
CREATE TABLE IF NOT EXISTS cma_ref${env_suffix}.party (
  party_id      STRING,
  legal_name    STRING,
  lei           STRING,
  country_code  STRING,
  sector        STRING,
  is_active     BOOLEAN
)
COMMENT 'Party (borrower/issuer) reference, sourced from the party master'
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS cma_ref${env_suffix}.fx_rate (
  currency      STRING,
  rate_to_usd   DECIMAL(18,8),
  rate_date     DATE
)
COMMENT 'End-of-day FX rates to USD'
STORED AS PARQUET;

-- ---------------------------------------------------------------- curated
CREATE TABLE IF NOT EXISTS cma_curated${env_suffix}.deal (
  deal_id            STRING        COMMENT 'Deal identifier from the booking system',
  deal_version       INT           COMMENT 'Latest amendment version loaded',
  deal_name          STRING,
  borrower_party_id  STRING,
  borrower_name      STRING,
  borrower_lei       STRING,
  borrower_country   STRING,
  borrower_sector    STRING,
  party_resolved     BOOLEAN       COMMENT 'FALSE when the borrower is not in party reference',
  product_type       STRING,
  deal_role          STRING        COMMENT 'Bank role on the deal',
  currency           STRING,
  deal_amount        DECIMAL(20,2) COMMENT 'Total deal size, deal currency',
  bank_share_amount  DECIMAL(20,2) COMMENT 'Bank allocation, deal currency',
  bank_share_pct     DECIMAL(9,4),
  fx_rate_to_usd     DECIMAL(18,8),
  deal_amount_usd    DECIMAL(20,2),
  bank_share_usd     DECIMAL(20,2),
  trade_date         DATE,
  settlement_date    DATE,
  booking_system     STRING,
  deal_status        STRING,
  last_updated_ts    TIMESTAMP,
  source_system      STRING,
  run_id             STRING        COMMENT 'Pipeline run that loaded the row (lineage)',
  ingested_at        TIMESTAMP
)
COMMENT 'Curated CMA deals, one row per deal per business date (latest version)'
PARTITIONED BY (business_date DATE)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'data.owner'='CMA Data Office', 'data.classification'='CONFIDENTIAL');

-- ---------------------------------------------------------------- quarantine
CREATE TABLE IF NOT EXISTS cma_quarantine${env_suffix}.deal_reject (
  deal_id            STRING,
  deal_version       STRING,
  deal_name          STRING,
  borrower_party_id  STRING,
  product_type       STRING,
  deal_role          STRING,
  currency           STRING,
  deal_amount        STRING,
  bank_share_amount  STRING,
  trade_date         STRING,
  settlement_date    STRING,
  booking_system     STRING,
  deal_status        STRING,
  last_updated_ts    STRING,
  `_raw_record`      STRING        COMMENT 'Original line for malformed records',
  `_reject_reasons`  STRING        COMMENT 'Pipe-delimited DQ rule names',
  `_rejected_at`     TIMESTAMP,
  run_id             STRING,
  source_system      STRING
)
COMMENT 'Deals rejected by validation; fix at source and replay'
PARTITIONED BY (business_date DATE)
STORED AS PARQUET
TBLPROPERTIES ('retention.days'='90');

-- ---------------------------------------------------------------- audit
CREATE TABLE IF NOT EXISTS cma_audit${env_suffix}.dq_results (
  rule           STRING,
  dimension      STRING,
  severity       STRING,
  checked        BIGINT,
  failed         BIGINT,
  failureRatio   DOUBLE,
  threshold      DOUBLE,
  passed         BOOLEAN,
  dataset        STRING,
  run_id         STRING,
  business_date  DATE,
  evaluated_at   TIMESTAMP
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS cma_audit${env_suffix}.pipeline_run (
  run_id           STRING,
  job_name         STRING,
  env              STRING,
  status           STRING,
  input_rows       BIGINT,
  loaded_rows      BIGINT,
  rejected_rows    BIGINT,
  superseded_rows  BIGINT,
  started_at       TIMESTAMP,
  finished_at      TIMESTAMP,
  business_date    DATE
)
STORED AS PARQUET;

-- ---------------------------------------------------------------- streaming (kafka-streaming module)
CREATE DATABASE IF NOT EXISTS cma_raw${env_suffix};

CREATE TABLE IF NOT EXISTS cma_raw${env_suffix}.deal_event (
  deal_id STRING, deal_version STRING, deal_name STRING, borrower_party_id STRING, product_type STRING,
  deal_role STRING, currency STRING, deal_amount STRING, bank_share_amount STRING, trade_date STRING,
  settlement_date STRING, booking_system STRING, deal_status STRING, last_updated_ts STRING,
  `_corrupt_record` STRING  COMMENT 'Original payload when the event JSON was unreadable',
  event_id STRING, event_type STRING, event_ts TIMESTAMP,
  kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT, kafka_timestamp TIMESTAMP
)
COMMENT 'Every Kafka deal event as received (bronze), for audit and replay'
PARTITIONED BY (business_date DATE)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS cma_curated${env_suffix}.deal_intraday
LIKE cma_curated${env_suffix}.deal;

CREATE TABLE IF NOT EXISTS cma_audit${env_suffix}.stream_batch (
  query_name STRING, batch_id BIGINT, events BIGINT, rejected BIGINT, committed_at TIMESTAMP
)
COMMENT 'Micro-batch ledger: a replayed batch_id is skipped to avoid double loading'
STORED AS PARQUET;
