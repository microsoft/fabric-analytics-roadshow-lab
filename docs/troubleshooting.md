# Troubleshooting Guide

This guide covers common issues and their solutions for the Fabric Analytics Roadshow lab.

## Deployment Issues

### Issue: Deployment Notebook Fails to Run

**Symptoms:**
- Import errors for required packages
- Permission denied errors
- Workspace not found

**Solutions:**
1. Verify you have Contributor permissions in the workspace
2. Check that you're running the notebook in a Fabric environment
3. Ensure the config.json file is in the same directory
4. Try restarting the notebook kernel

### Issue: Artifacts Not Created

**Symptoms:**
- Lakehouse or Warehouse missing after deployment
- Notebooks not appearing in workspace

**Solutions:**
1. Check the deployment notebook output for errors
2. Verify workspace capacity and resource limits
3. Manually create missing artifacts through the Fabric UI
4. Re-run the deployment notebook

## Data Loading Issues

### Issue: Sample Data Not Found

**Symptoms:**
- File not found errors
- Empty DataFrames

**Solutions:**
1. Check that sample data files are in the `/data/sample` directory
2. Verify file paths are correct in notebooks
3. Download larger datasets as specified in `/data/sample/README.md`
4. Check file permissions

### Issue: Data Loading Timeout

**Symptoms:**
- Long-running cells that never complete
- Memory errors

**Solutions:**
1. Use smaller sample datasets for testing
2. Increase Spark executor memory settings
3. Partition large files before loading
4. Use incremental loading patterns

## Spark Issues

### Issue: Spark Session Not Starting

**Symptoms:**
- "SparkSession not found" errors
- Kernel appears stuck

**Solutions:**
1. Restart the notebook kernel
2. Check Fabric capacity status
3. Verify Spark pool is running
4. Wait a few minutes and retry

### Issue: Package Import Errors

**Symptoms:**
- "ModuleNotFoundError" messages
- Import statements fail

**Solutions:**
1. Install required packages using `%pip install package_name`
2. Restart kernel after installing packages
3. Check package versions in requirements.txt
4. Use built-in Fabric libraries when possible

## Warehouse Issues

### Issue: Cannot Connect to Warehouse

**Symptoms:**
- Connection timeout errors
- Authentication failures

**Solutions:**
1. Verify the warehouse exists in your workspace
2. Check that you have appropriate permissions
3. Ensure the warehouse is not paused
4. Try refreshing the connection

### Issue: Query Performance Problems

**Symptoms:**
- Queries taking too long to execute
- Timeouts on large queries

**Solutions:**
1. Check query execution plan
2. Add appropriate indexes
3. Partition large tables
4. Use aggregation and filtering to reduce data size
5. Review the query optimization notebook

## Permission Issues

### Issue: Access Denied Errors

**Symptoms:**
- "Permission denied" messages
- Cannot create or modify artifacts

**Solutions:**
1. Verify workspace role (need Contributor or Admin)
2. Check Fabric capacity permissions
3. Contact workspace administrator
4. Ensure you're in the correct workspace

## Performance Issues

### Issue: Notebooks Running Slowly

**Symptoms:**
- Cells take excessive time to execute
- Frequent timeouts

**Solutions:**
1. Use smaller datasets for testing
2. Optimize Spark configurations
3. Check Fabric capacity utilization
4. Close unused notebooks and artifacts
5. Review the performance tuning notebook

## General Tips

### Best Practices
- Save your work frequently
- Test with small datasets first
- Check error messages carefully
- Use print/display statements for debugging
- Review notebook outputs for warnings

### Getting Help
1. Check this troubleshooting guide first
2. Review relevant Fabric documentation
3. Ask the instructor during lab sessions
4. Check the [Microsoft Fabric Community](https://community.fabric.microsoft.com/)

### Additional Resources
- [Microsoft Fabric Documentation](https://learn.microsoft.com/fabric/)
- [Spark Documentation](https://spark.apache.org/docs/latest/)
- [T-SQL Reference](https://learn.microsoft.com/sql/t-sql/)

## Still Having Issues?

If you continue to experience problems:
1. Document the exact error message
2. Note the steps to reproduce the issue
3. Check if others are experiencing the same issue
4. Contact the lab instructor or support team
