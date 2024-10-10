from google.cloud import bigquery

# Initialize BigQuery client
client = bigquery.Client()

# Define the table IDs
tables = [
    "dbt-playground-421906.ecommerce.ecommerce_orders",
    "dbt-playground-421906.ecommerce.ecommerce_customers",
    "dbt-playground-421906.ecommerce.ecommerce_interactions",
    "dbt-playground-421906.ecommerce.ecommerce_transactions"
]

# Define the timestamp column names for each table
timestamp_columns = {
    "dbt-playground-421906.ecommerce.ecommerce_orders": "order_timestamp",
    "dbt-playground-421906.ecommerce.ecommerce_customers": "registration_timestamp",
    "dbt-playground-421906.ecommerce.ecommerce_interactions": "interaction_timestamp",
    "dbt-playground-421906.ecommerce.ecommerce_transactions": "payment_date"
}

# Define the cutoff date
cutoff_date = "2024-10-01"

# Iterate over each table and execute the delete query
for table_id in tables:
    timestamp_column = timestamp_columns[table_id]
    query = f"""
    DELETE FROM `{table_id}`
    WHERE {timestamp_column} >= '{cutoff_date}'
    """
    # Execute the query
    query_job = client.query(query)
    query_job.result()  # Wait for the job to complete
    print(f"Deleted rows from {table_id} where {timestamp_column} >= {cutoff_date}")