<img src="https://github.com/microsoft/fabric-analytics-roadshow-lab/blob/main/assets/images/spark/analytics.png?raw=true"
     width="80"
     align="left"
     style="margin-right:0px; padding-top:20px;" />

<h1 style="border-bottom: none; padding-bottom: 0; margin-bottom: 0;">
  Fabric Analytics Roadshow Lab
</h1>

A hands-on workshop that builds a streaming lakehouse and dimensional warehouse for the fictitious _McMillan Industrial Group_ using Microsoft Fabric. You will trigger a production-style Spark pipeline, explore streaming data as it lands in the lakehouse, shape it into a gold warehouse model with T-SQL, and then build an agent to explore the data.

## Agenda (approx.)
> [!IMPORTANT]
>
> | Start–End | Duration | Focus |
> | --- | --- | --- |
> | 00:00–00:15 | 15m | Kickoff, goals, lab installation |
> | 00:15–01:15 | 60m | Module 1: Processing streaming data with Fabric Spark |
> | 01:15–01:30 | 15m | Break |
> | 01:30–02:30 | 60m | Module 2: Building a dimensional model with Fabric Data Warehouse |
> | 02:30–02:45 | 15m | Break |
> | 02:45–03:15 | 30m | Module 3: Creating a data agent |
> | 03:15–04:00 | 45m | Wrap-up and Q&A |

## Install the Lab in Fabric (Notebook, 2 cells)
**Step 1: Create a Fabric Notebook**

In your Fabric workspace, create a new Notebook (PySpark or Python runtime).

**Step 2: Run installer**

Copy, paste, and the run the below in a Notebook cell.

```python
%pip install fabric-jumpstart --quiet
```

```
import fabric_jumpstart
fabric_jumpstart.install('analytics-roadshow-lab')
```

> [!NOTE]
>
> It will take approximately 8 minutes to install all lab content into your workspace. You will see a success message once done.

## What You Will Build
- **Landing → Bronze → Silver**: Structured Streaming pipelines that ingest JSON/Parquet files, add audit metadata, and flatten nested shipment payloads ([workspace/analytics-roadshow-lab/1_ExploreData.Notebook/notebook-content.py](workspace/analytics-roadshow-lab/1_ExploreData.Notebook/notebook-content.py)).
- **Gold (Warehouse)**: Dimensional model with schemas `dim` and `fact`, type 1/2-ready dimensions, and fact tables for orders and shipments, loaded via stored procedures and incremental watermarks.
- **Data Pipeline** (optional): Refresh your gold model via calling stored procedures orchestrated via a Data Factory Pipeline.
- **Data Agent**: An agent that can answer questions over the warehouse with curated context and few-shots.

## Scenario
McMillan Industrial Group manufactures and distributes industrial equipment. The lab data simulates customers, orders, items, shipments, and scan events generated continuously by field devices and warehouse systems. You will turn this streaming raw data into analytics-ready gold tables.

## Lab Flow
1) **Start the streaming job** to generate synthetic data and hydrate Landing/Bronze/Silver.
2) **Explore data in Spark**: preview landing files, flatten nested shipment JSON, and observe exactly-once incremental processing.
3) **Model in the warehouse**: create schemas and tables, load with MERGE-based stored procedures driven by an `etl_tracking` watermark table, and verify row counts.
3) **Orchestrate**: run a pipeline and monitor for completion.
4) **Build an agent**: create a Data Agent to provide powerful context and quickly answer questions about your data.

## Skills Practiced
- Streaming ingestion with Spark Structured Streaming and Delta Lake on Fabric.
- Medallion architecture design (Landing, Bronze, Silver, Gold).
- Dimensional modeling with surrogate keys and SCD-ready dimensions.
- Incremental ETL using MERGE and watermarking.
- Data Agent creation and tuning.
