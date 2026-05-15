# Lakehouse Table Design

## Project

SensorOps: Hydraulic System Condition Monitoring Lakehouse using Azure Databricks

## Design Goal

The goal is to convert raw hydraulic sensor matrices into a production-style Lakehouse model using Bronze, Silver, and Gold layers.

The raw dataset contains one file per sensor. Each row represents one 60-second hydraulic system load cycle. Each column inside the row represents a measurement point within that cycle.

## Medallion Architecture

### Bronze Layer

Bronze stores raw data with minimal transformation.

Purpose:

- Preserve raw sensor readings
- Keep source metadata
- Support reprocessing
- Track ingestion time
- Avoid losing original signal shape

### Silver Layer

Silver stores cleaned, validated, and enriched data.

Purpose:

- Validate expected reading counts
- Convert raw sensor arrays into statistical features
- Join profile labels
- Create human-readable component condition labels
- Capture data quality issues

### Gold Layer

Gold stores business-ready data products.

Purpose:

- Build maintenance risk indicators
- Summarize component condition
- Create dashboard KPIs
- Support analytics and reporting

---

## Table Design

## Bronze Tables

### bronze.sensor_raw_cycles

Grain: one row per cycle per sensor

Purpose: preserve raw sensor readings as arrays.

Important columns:

- cycle_id
- asset_id
- sensor_id
- physical_quantity
- unit
- sampling_rate_hz
- expected_reading_count
- actual_reading_count
- readings
- source_file_name
- source_file_path
- ingestion_timestamp
- ingestion_date
- structure_status

Design reason:

The raw readings are stored as an array because each cycle contains a full 60-second signal sequence. This preserves the raw signal without exploding the data too early.

---

### bronze.profile_raw

Grain: one row per cycle

Purpose: preserve original target labels from profile.txt.

Important columns:

- cycle_id
- cooler_condition_pct
- valve_condition_pct
- internal_pump_leakage
- hydraulic_accumulator_bar
- stable_flag
- ingestion_timestamp
- ingestion_date
- source_file_name

Design reason:

This table keeps the raw target labels exactly as provided by the source dataset.

---

## Silver Tables

### silver.sensor_cycle_features

Grain: one row per cycle per sensor

Purpose: create cleaned statistical features from raw sensor arrays.

Important columns:

- cycle_id
- asset_id
- sensor_id
- physical_quantity
- unit
- sampling_rate_hz
- reading_count
- reading_min
- reading_max
- reading_mean
- reading_stddev
- reading_first
- reading_last
- reading_range
- has_null_reading
- structure_valid
- processed_timestamp

Design reason:

This table converts raw signal arrays into analytics-ready features. These features can be used for dashboards, machine health scoring, and ML models.

---

### silver.condition_labels

Grain: one row per cycle

Purpose: convert numeric profile labels into readable condition labels.

Important columns:

- cycle_id
- cooler_condition_pct
- cooler_condition_label
- valve_condition_pct
- valve_condition_label
- internal_pump_leakage
- pump_leakage_label
- hydraulic_accumulator_bar
- accumulator_condition_label
- stable_flag
- stable_label
- processed_timestamp

Design reason:

Business users and dashboards should not depend only on numeric condition codes. Silver creates understandable labels.

---

### silver.sensor_quality_issues

Grain: one row per detected data quality issue

Purpose: store invalid or suspicious records.

Important columns:

- issue_id
- cycle_id
- sensor_id
- issue_type
- issue_description
- expected_value
- actual_value
- detected_timestamp

Design reason:

Bad records should not silently disappear. A quarantine table supports transparency and troubleshooting.

---

## Gold Tables

### gold.sensor_health_kpis

Grain: one row per sensor

Purpose: provide sensor-level health and signal summaries.

Important columns:

- asset_id
- sensor_id
- physical_quantity
- total_cycles
- avg_reading_mean
- min_reading_min
- max_reading_max
- avg_reading_stddev
- calculated_timestamp

---

### gold.component_condition_summary

Grain: one row per component condition

Purpose: show distribution of component health conditions.

Important columns:

- component_name
- condition_value
- condition_label
- cycle_count
- percentage_of_cycles
- calculated_timestamp

---

### gold.maintenance_risk_cycles

Grain: one row per cycle

Purpose: assign a maintenance risk score to each cycle.

Important columns:

- cycle_id
- asset_id
- risk_score
- risk_level
- risk_reasons
- recommended_action
- calculated_timestamp

---

### gold.maintenance_alerts

Grain: one row per generated alert

Purpose: provide dashboard-ready maintenance alerts.

Important columns:

- alert_id
- cycle_id
- asset_id
- risk_level
- risk_score
- alert_reason
- recommended_action
- created_timestamp

---

## Managed vs External Table Decision

The raw downloaded source files are external files and should remain outside Git.

The Bronze, Silver, and Gold Delta tables will be created as Unity Catalog managed tables in Databricks.

Reason:

- Managed tables are simpler for this project.
- Unity Catalog can govern the tables.
- Databricks manages table storage and lifecycle.
- This follows the recommended approach for newly created tables.

---

## Optimization Planning

Optimization will be implemented later after the tables contain data.

Planned optimization concepts:

- Delta Lake table history
- Time travel
- OPTIMIZE
- VACUUM
- Liquid clustering or clustering strategy
- Small-file problem explanation
- Query performance comparison before and after optimization

---

## Exam Concepts Covered

This design prepares us for the following Databricks Data Engineer Associate concepts:

- Medallion architecture
- Bronze, Silver, Gold data layers
- Managed vs external tables
- Delta table design
- Data quality and quarantine pattern
- Table grain definition
- Incremental ingestion planning
- Optimization planning
- Governance planning with Unity Catalog
