
# ShopEase ETL Project

This ETL project leverages Apache Spark for data processing, transformation, and storage to analyze sales, inventory, and customer feedback data for an e-commerce store called ShopEase.

## Project Overview

The ShopEase ETL project processes large-scale transaction, inventory, and customer feedback data to extract meaningful insights and store the transformed data in a structured format for further analysis and visualization. 

Key project features:
- Data Cleaning and Transformation: Clean, anonymize, and standardize multiple datasets.
- Data Enrichment: Join transaction data with inventory data for detailed analysis.
- Data Analytics: Run SQL-based queries for monthly revenue trends, top products, and inventory turnover rates.
- Data Storage: Store processed data in Parquet format for efficient retrieval.
- Visualization: Generate dashboards to display top products, revenue trends, and inventory turnover heatmaps.

## Dataset Details

1. **Transactions Data** (`large_transactions.csv`): Contains transactional information, including transaction IDs, user IDs, product IDs, quantity, and amounts.
2. **Inventory Data** (`large_inventory.json`): Includes product details like product name, category, stock level, and price.
3. **Customer Feedback Data** (`large_customer_feedback.csv`): Contains feedback data which may be used for sentiment analysis.

## ETL Steps

### Step 1: Data Loading
- **Transactions**: Load from CSV with header and inferred schema.
- **Inventory**: Load from JSON with multi-line support.
- **Customer Feedback**: Load from CSV with header and inferred schema.

### Step 2: Data Cleaning and Anonymization
- **Transactions**: Filter invalid transactions (e.g., null transaction IDs or negative amounts).
- **Anonymization**: Anonymize user IDs in transaction data using SHA-256 hashing.
- **Inventory**: Drop entries with missing stock levels and standardize product names.

### Step 3: Data Transformation
- **Join Data**: Combine transaction and inventory data based on `product_id` to create a comprehensive dataset for analysis.

### Step 4: Data Analysis (SQL Queries)
1. **Monthly Revenue Trends**: Calculate monthly revenue by aggregating transaction data.
2. **Top 10 Products**: Identify the most purchased products within a given timeframe.
3. **Inventory Turnover Rates**: Compute sales turnover rate per product to analyze stock movement.

### Step 5: Data Storage
- **Parquet Format**: Store the transformed transaction, inventory, and joined data in Parquet format for efficient data retrieval.

### Step 6: Data Visualization
1. **Top Products Bar Chart**: Display top-selling products based on quantity sold.
2. **Revenue Trend Line Chart**: Visualize monthly revenue trends over the past year.
3. **Inventory Turnover Heatmap**: Generate a heatmap to display turnover rates across product categories.

## Performance Optimization Techniques

- **Caching**: Cache intermediate DataFrames to enhance performance during transformations.
- **Repartitioning**: Repartition large DataFrames for optimized join operations.
- **Broadcast Join**: Apply broadcast joins for smaller DataFrames to minimize shuffling.

## Project Structure

```
ShopEase_ETL/
│
├── data/ # Contains datasets in CSV and JSON formats
│   ├── large_transactions.csv
│   ├── large_inventory.json
│   └── large_customer_feedback.csv
│
├── notebooks/ # Jupyter notebooks for project development and testing
│   └── ShopEase_ETL.ipynb
│
├── output/ # Processed data in Parquet format
│   ├── cleaned_transactions.parquet
│   ├── cleaned_inventory.parquet
│   └── joined_data.parquet
│
└── README.md # Project documentation
```

## How to Run the Project

1. **Setup Spark Session**: Configure SparkSession in your Jupyter environment.
2. **Execute ETL Notebook**: Run each cell in `ShopEase_ETL.ipynb` sequentially.
3. **View Output Data**: Check the Parquet files in the `output/` directory.
4. **Visualization**: Run visualization cells in the notebook to view graphical insights.

## Dependencies

- **Apache Spark**: For distributed data processing.
- **Python Libraries**: `hashlib`, `matplotlib`, `seaborn` for data anonymization and visualization.

## Future Enhancements

- Implement sentiment analysis on customer feedback data.
- Integrate a live dashboard for real-time ETL monitoring.
- Add advanced analytics, such as predictive stock analysis.

## License

This project is licensed under the MIT License.
