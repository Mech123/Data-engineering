-- SensorOps Lakehouse catalog and schema setup
-- This script is intended for Azure Databricks with Unity Catalog enabled.

CREATE CATALOG IF NOT EXISTS sensorops_dev
COMMENT 'Development catalog for the SensorOps industrial IoT Lakehouse project';

CREATE SCHEMA IF NOT EXISTS sensorops_dev.bronze
COMMENT 'Bronze layer for raw sensor and profile data';

CREATE SCHEMA IF NOT EXISTS sensorops_dev.silver
COMMENT 'Silver layer for cleaned, validated, and enriched sensor data';

CREATE SCHEMA IF NOT EXISTS sensorops_dev.gold
COMMENT 'Gold layer for business-ready maintenance and health analytics';

CREATE SCHEMA IF NOT EXISTS sensorops_dev.governance
COMMENT 'Governance objects such as masks, filters, and access-control helper functions';
