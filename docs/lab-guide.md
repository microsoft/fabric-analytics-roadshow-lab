# Lab Guide - Fabric Analytics Roadshow

Welcome to the Fabric Analytics Roadshow! This hands-on lab will guide you through building data solutions with Microsoft Fabric Spark and Warehouse.

## Prerequisites

Before starting this lab, ensure you have:
- [ ] Access to a Microsoft Fabric workspace
- [ ] Contributor or Admin permissions in the workspace
- [ ] Basic knowledge of SQL and Python
- [ ] Lab materials downloaded or cloned

## Lab Setup

### Step 1: Deploy Lab Assets

1. Open your Fabric workspace
2. Import the `setup/deploy.ipynb` notebook
3. Run all cells to deploy lab materials
4. Verify that all artifacts are created successfully

### Step 2: Verify Your Environment

Check that you can see:
- Lakehouse: `RoadshowLakehouse`
- Warehouse: `RoadshowWarehouse`
- Notebooks in your workspace

## Module 1: Data Ingestion with Spark

In this module, you'll learn to:
- Load data from various sources
- Use Spark DataFrames for data manipulation
- Implement incremental load patterns

### Exercise 1.1: Load Sample Data
1. Open notebook: `01-data-ingestion/01-load-sample-data.ipynb`
2. Follow the instructions in the notebook
3. Complete the exercises

### Exercise 1.2: Incremental Loads
1. Open notebook: `01-data-ingestion/02-incremental-load.ipynb`
2. Implement incremental loading logic
3. Test with sample data

## Module 2: Data Transformation

Learn to transform data efficiently using Spark.

### Exercise 2.1: Working with DataFrames
1. Open notebook: `02-data-transformation/01-spark-dataframes.ipynb`
2. Practice DataFrame operations
3. Complete the exercises

### Exercise 2.2: Delta Tables
1. Open notebook: `02-data-transformation/02-delta-tables.ipynb`
2. Create and manage Delta tables
3. Explore time travel features

### Exercise 2.3: Spark SQL
1. Open notebook: `02-data-transformation/03-spark-sql.ipynb`
2. Write SQL queries against Spark tables
3. Compare performance with DataFrame API

## Module 3: Fabric Warehouse

Build analytical models in the Warehouse.

### Exercise 3.1: Create Warehouse Objects
1. Open notebook: `03-warehouse/01-create-warehouse-objects.ipynb`
2. Create tables, views, and schemas
3. Load data into warehouse tables

### Exercise 3.2: Data Modeling
1. Open notebook: `03-warehouse/02-data-modeling.ipynb`
2. Design star schema models
3. Implement slowly changing dimensions

### Exercise 3.3: Query Optimization
1. Open notebook: `03-warehouse/03-query-optimization.ipynb`
2. Analyze query plans
3. Apply optimization techniques

## Module 4: Advanced Topics

Explore advanced integration patterns.

### Exercise 4.1: Spark-Warehouse Integration
1. Open notebook: `04-advanced/01-spark-warehouse-integration.ipynb`
2. Query warehouse from Spark
3. Move data between Lakehouse and Warehouse

### Exercise 4.2: Performance Tuning
1. Open notebook: `04-advanced/02-performance-tuning.ipynb`
2. Implement caching strategies
3. Optimize partition strategies

## Troubleshooting

If you encounter issues, refer to:
- [Troubleshooting Guide](troubleshooting.md)
- Instructor for assistance
- [Fabric Documentation](https://learn.microsoft.com/fabric/)

## Next Steps

After completing this lab:
1. Review solutions in the `/solutions` directory
2. Explore the reference materials in `/assets/reference`
3. Apply these concepts to your own projects

## Feedback

Please provide feedback on:
- What worked well
- What could be improved
- Topics you'd like to learn more about

Thank you for participating in the Fabric Analytics Roadshow!
