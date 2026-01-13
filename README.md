# Fabric Analytics Roadshow Lab

Hands-on workshop that builds a streaming lakehouse and dimensional warehouse for the fictitious **McMillan Industrial Group** using Microsoft Fabric. You will trigger a production-style Spark pipeline, explore streaming data as it lands in the lakehouse, and then shape it into a gold warehouse model with T-SQL.

## What You Build
- **Landing → Bronze → Silver**: Structured Streaming pipelines that ingest JSON/Parquet files, add audit metadata, and flatten nested shipment payloads ([workspace/analytics-roadshow-lab/1_ExploreData.Notebook/notebook-content.py](workspace/analytics-roadshow-lab/1_ExploreData.Notebook/notebook-content.py)).
- **Silver → Gold (Warehouse)**: Dimensional model with schemas `dim` and `fact`, type 1/2-ready dimensions, and fact tables for orders and shipments, loaded via stored procedures and incremental watermarks ([workspace/analytics-roadshow-lab/2_ModelData.Notebook/notebook-content.sql](workspace/analytics-roadshow-lab/2_ModelData.Notebook/notebook-content.sql)).

## Scenario
McMillan Industrial Group manufactures and distributes industrial equipment. The lab data simulates customers, orders, items, shipments, and scan events generated continuously by field devices and warehouse systems. You will turn this streaming raw data into analytics-ready gold tables.

## Lab Flow
1) **Start the streaming job** to generate synthetic data and hydrate Landing/Bronze/Silver.
2) **Explore data in Spark**: preview landing files, flatten nested shipment JSON, and observe exactly-once incremental processing.
3) **Model in the warehouse**: create schemas and tables, load with MERGE-based stored procedures driven by an `etl_tracking` watermark table, and verify row counts.
4) **Analyze**: use the gold tables for downstream analytics and measure query performance with the Native Execution Engine.

## Repository Layout
- `workspace/analytics-roadshow-lab/1_ExploreData.Notebook/` — Spark notebook that explores landing data, demonstrates Structured Streaming, and builds bronze/silver tables.
- `workspace/analytics-roadshow-lab/2_ModelData.Notebook/` — SQL warehouse notebook that creates `dim`/`fact` tables, loads gold via stored procedures, and runs incremental ETL.
- `workspace/parameter.yml` — workspace parameters for Fabric deployment.
- `setup/` — dependencies and setup assets.

## Skills Practiced
- Streaming ingestion with Spark Structured Streaming and Delta Lake on Fabric.
- Medallion architecture design (Landing, Bronze, Silver, Gold).
- Dimensional modeling with surrogate keys and SCD-ready dimensions.
- Incremental ETL using MERGE and watermarking.

