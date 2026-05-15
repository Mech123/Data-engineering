-- SensorOps Lakehouse table definitions
-- These tables are designed as Unity Catalog managed Delta tables.
-- No LOCATION clause is used, so Databricks creates managed tables.

USE CATALOG sensorops_dev;

-- ============================================================
-- Bronze tables
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze.sensor_raw_cycles (
    cycle_id BIGINT,
    asset_id STRING,
    sensor_id STRING,
    physical_quantity STRING,
    unit STRING,
    sampling_rate_hz INT,
    expected_reading_count INT,
    actual_reading_count INT,
    readings ARRAY<DOUBLE>,
    source_file_name STRING,
    source_file_path STRING,
    ingestion_timestamp TIMESTAMP,
    ingestion_date DATE,
    structure_status STRING
)
USING DELTA
COMMENT 'Bronze table preserving raw sensor readings as arrays at one row per cycle per sensor';

CREATE TABLE IF NOT EXISTS bronze.profile_raw (
    cycle_id BIGINT,
    cooler_condition_pct INT,
    valve_condition_pct INT,
    internal_pump_leakage INT,
    hydraulic_accumulator_bar INT,
    stable_flag INT,
    source_file_name STRING,
    ingestion_timestamp TIMESTAMP,
    ingestion_date DATE
)
USING DELTA
COMMENT 'Bronze table preserving raw profile target labels at one row per cycle';

-- ============================================================
-- Silver tables
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.sensor_cycle_features (
    cycle_id BIGINT,
    asset_id STRING,
    sensor_id STRING,
    physical_quantity STRING,
    unit STRING,
    sampling_rate_hz INT,
    reading_count INT,
    reading_min DOUBLE,
    reading_max DOUBLE,
    reading_mean DOUBLE,
    reading_stddev DOUBLE,
    reading_first DOUBLE,
    reading_last DOUBLE,
    reading_range DOUBLE,
    has_null_reading BOOLEAN,
    structure_valid BOOLEAN,
    processed_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver feature table with cleaned statistical features per cycle and sensor';

CREATE TABLE IF NOT EXISTS silver.condition_labels (
    cycle_id BIGINT,
    cooler_condition_pct INT,
    cooler_condition_label STRING,
    valve_condition_pct INT,
    valve_condition_label STRING,
    internal_pump_leakage INT,
    pump_leakage_label STRING,
    hydraulic_accumulator_bar INT,
    accumulator_condition_label STRING,
    stable_flag INT,
    stable_label STRING,
    processed_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver table with human-readable hydraulic component condition labels';

CREATE TABLE IF NOT EXISTS silver.sensor_quality_issues (
    issue_id STRING,
    cycle_id BIGINT,
    sensor_id STRING,
    issue_type STRING,
    issue_description STRING,
    expected_value STRING,
    actual_value STRING,
    detected_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Silver quarantine table for invalid or suspicious sensor records';

-- ============================================================
-- Gold tables
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.sensor_health_kpis (
    asset_id STRING,
    sensor_id STRING,
    physical_quantity STRING,
    total_cycles BIGINT,
    avg_reading_mean DOUBLE,
    min_reading_min DOUBLE,
    max_reading_max DOUBLE,
    avg_reading_stddev DOUBLE,
    calculated_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold table with sensor-level health KPI summaries';

CREATE TABLE IF NOT EXISTS gold.component_condition_summary (
    component_name STRING,
    condition_value INT,
    condition_label STRING,
    cycle_count BIGINT,
    percentage_of_cycles DOUBLE,
    calculated_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold table summarizing component condition distribution';

CREATE TABLE IF NOT EXISTS gold.maintenance_risk_cycles (
    cycle_id BIGINT,
    asset_id STRING,
    risk_score DOUBLE,
    risk_level STRING,
    risk_reasons ARRAY<STRING>,
    recommended_action STRING,
    calculated_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold table assigning maintenance risk to each hydraulic system cycle';

CREATE TABLE IF NOT EXISTS gold.maintenance_alerts (
    alert_id STRING,
    cycle_id BIGINT,
    asset_id STRING,
    risk_level STRING,
    risk_score DOUBLE,
    alert_reason STRING,
    recommended_action STRING,
    created_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Gold table containing dashboard-ready maintenance alerts';
