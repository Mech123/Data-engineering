# SensorOps: Hydraulic System Condition Monitoring Lakehouse using Azure Databricks

## Project Goal

This project builds an end-to-end industrial sensor data engineering pipeline using Azure Databricks.

The project uses real condition-monitoring sensor data from a hydraulic test rig and processes it through a production-style Lakehouse architecture.

## Business Scenario

A manufacturing company wants to monitor hydraulic machines and detect component degradation using sensor data.

The platform should:

- Ingest raw industrial sensor data
- Store raw data in a Bronze Delta layer
- Clean and validate sensor records in a Silver layer
- Build Gold tables for machine health KPIs
- Generate maintenance risk indicators
- Optimize Delta tables for analytics
- Secure access using Unity Catalog
- Orchestrate pipelines using Databricks Workflows
- Deploy code using CI/CD

## Dataset

Dataset: Condition Monitoring of Hydraulic Systems

The dataset contains real sensor measurements from a hydraulic test rig, including pressure, motor power, volume flow, temperature, vibration, cooling efficiency, cooling power, and efficiency factor.

Target labels describe the condition of hydraulic components such as cooler, valve, pump leakage, and hydraulic accumulator.

## Technology Stack

- Azure Databricks
- Azure Data Lake Storage Gen2
- PySpark
- Delta Lake
- Auto Loader
- Structured Streaming
- Unity Catalog
- Databricks Workflows
- Databricks Asset Bundles
- GitHub Actions
- Databricks SQL / Power BI

## Current Implementation Status

### Completed

- Project repository structure
- Real hydraulic system dataset acquisition
- Dataset structure profiling
- Lakehouse Bronze/Silver/Gold table design
- SQL catalog, schema, and table definition scripts
- Ingestion-ready Bronze source JSONL preparation
- Bronze source validation checks
- Local pytest test suite

### Next Step

Implement Bronze ingestion in Azure Databricks using Auto Loader and Delta Lake.
