# Instructor Guide - Fabric Analytics Roadshow

## Lab Overview

**Duration:** 4 hours  
**Target Audience:** Data engineers, analysts, and architects  
**Prerequisites:** Basic SQL and Python knowledge, Azure account

## Lab Objectives

By the end of this lab, participants will be able to:
1. Create and configure Fabric workspaces, Lakehouses, and Warehouses
2. Use Spark notebooks for data ingestion and transformation
3. Work with Delta tables and optimize performance
4. Build warehouse models and write efficient SQL queries
5. Integrate Spark and Warehouse workloads

## Timing Breakdown

### Module 1: Data Ingestion (60 minutes)
- **15 min:** Introduction and setup
- **20 min:** Loading sample data with Spark
- **25 min:** Incremental load patterns

### Module 2: Data Transformation (75 minutes)
- **25 min:** Spark DataFrames and operations
- **25 min:** Working with Delta tables
- **25 min:** Spark SQL queries

### Break (15 minutes)

### Module 3: Warehouse (75 minutes)
- **25 min:** Creating warehouse objects
- **25 min:** Data modeling concepts
- **25 min:** Query optimization techniques

### Module 4: Advanced Topics (45 minutes)
- **25 min:** Spark-Warehouse integration patterns
- **20 min:** Performance tuning and best practices

### Wrap-up (10 minutes)
- Q&A and next steps

## Setup Instructions

### Before the Lab
1. Ensure all participants have:
   - Access to a Fabric workspace
   - Contributor permissions
   - Lab materials downloaded

2. Run the deployment notebook to verify setup
3. Test sample queries and notebooks

### Day-of Checklist
- [ ] Presentation slides ready
- [ ] Demo environment tested
- [ ] All notebooks execute successfully
- [ ] Sample data loaded
- [ ] Backup environment available

## Teaching Tips

### Common Issues
1. **Permission errors:** Verify workspace access
2. **Package installation:** Pre-load common packages
3. **Performance:** Use smaller datasets for demos

### Engagement Strategies
- Hands-on exercises after each concept
- Pair programming for complex tasks
- Real-world examples from various industries

### Key Concepts to Emphasize
- Medallion architecture (Bronze/Silver/Gold)
- Delta Lake benefits
- When to use Spark vs. Warehouse
- Performance optimization patterns

## Solutions

Complete solutions are available in the `/solutions` directory. Share these after participants attempt the exercises.

## Additional Resources

- [Microsoft Fabric Documentation](https://learn.microsoft.com/fabric/)
- [Spark Documentation](https://spark.apache.org/docs/latest/)
- [Delta Lake Documentation](https://docs.delta.io/)

## Feedback Collection

After the lab, collect feedback on:
- Content clarity and relevance
- Pacing and timing
- Hands-on exercises
- Instructor effectiveness
- Suggested improvements
