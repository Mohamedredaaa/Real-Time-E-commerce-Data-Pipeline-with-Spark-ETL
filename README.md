
# Real-Time E-commerce Data Pipeline with Spark ETL

## Project Overview

This ETL project simulates a real-time data processing pipeline for a hypothetical e-commerce platform called **ShopEase**. The project leverages Apache Spark to process data related to user activities, transactions, inventory updates, and customer feedback, enabling the platform to optimize analytics and support real-time insights.

## Project Architecture

The pipeline processes both historical (batch) and real-time data using Spark, covering these key steps:
1. **Data Ingestion**: Import data in both batch and streaming modes.
2. **Data Transformation**: Cleanse, anonymize, and enrich data to make it analysis-ready.
3. **Real-Time Processing**: Analyze user interactions in real-time, compute metrics, and set up alerts.
4. **Data Storage**: Store processed data in formats suitable for downstream analysis.

## Data Sources

The following data sources simulate the ShopEase platform data:
- **User Activity Logs**: Clickstream data capturing user interactions.
- **Transaction Records**: Details of purchases, refunds, and returns.
- **Inventory Updates**: Information about stock levels, restocks, and product updates.
- **Customer Feedback**: Reviews and ratings provided by customers.

## ETL Pipeline Steps

### 1. Data Ingestion
- **Batch Data**: Loads historical data in CSV and JSON formats.
- **Real-Time Data**: Simulates real-time user activity by ingesting data from Kafka topics.

### 2. Data Processing and Transformation
#### a. Using RDDs
- **Data Cleansing**: Filters incomplete records in transaction data.
- **Anonymization**: Masks user IDs for privacy.

#### b. Using DataFrames
- **Data Standardization**: Cleans and normalizes inventory data.
- **Data Joining**: Merges user activity logs with transaction records to analyze behaviors leading to purchases.

#### c. Using Spark SQL
- Executes SQL queries to compute:
  - Top 10 most-purchased products.
  - Monthly revenue trends.
  - Inventory turnover rates.

### 3. Real-Time Streaming (Optional)
- **Metrics Computed**: Tracks active users per minute, conversion rates, and alerts for unusual user activity.

### 4. Data Storage
- **Parquet Format**: Used for batch-processed data in a local data lake.
- **Redis or PostgreSQL**: Stores real-time metrics for low-latency access.

### 5. Performance Optimization
- **Caching**: Intermediate DataFrames are cached where needed.
- **Spark Configuration Tuning**: Includes partition adjustments, memory configurations, and join strategy optimization.

## Setup Instructions

1. **Install Dependencies**:
   - Python 3.x
   - Apache Spark
   - Kafka (if simulating real-time data ingestion)

2. **Initialize Spark Session**:
   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.appName("ShopEase_ETL").getOrCreate()
   ```

3. **Load Data**:
   - CSV files:
     ```python
     transactions_df = spark.read.csv("/path/to/transactions.csv", header=True, inferSchema=True)
     ```
   - JSON files:
     ```python
     inventory_df = spark.read.json("/path/to/inventory.json", multiLine=True)
     ```

## Usage

Run the pipeline by executing the provided Jupyter Notebook cells in sequence.

## SQL Queries Examples

### Top 10 Most Purchased Products
```sql
SELECT product_id, COUNT(*) as purchase_count
FROM transactions
GROUP BY product_id
ORDER BY purchase_count DESC
LIMIT 10
```

## Project Questions

1. **Data Ingestion**: How did batch and streaming data ingestion differ?
2. **Data Quality**: Explain the transformations used to ensure data quality.
3. **Spark SQL Operations**: What insights were derived using Spark SQL?

## Future Enhancements
- Additional real-time streaming metrics.
- Further optimization for larger datasets.
- Integration with a data visualization tool.
