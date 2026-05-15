# Bronze Notebooks

## 01_bronze_autoloader_ingestion.py

This Databricks notebook ingests prepared hydraulic sensor JSONL landing files into Bronze Delta tables.

## Source Paths

- sensor_raw_cycles JSONL files
- profile_raw JSONL files

## Target Tables

- sensorops_dev.bronze.sensor_raw_cycles
- sensorops_dev.bronze.profile_raw

## Key Concepts

- Auto Loader
- cloudFiles
- Structured Streaming
- checkpointLocation
- schemaLocation
- availableNow trigger
- Delta table sink
- Bronze layer ingestion
