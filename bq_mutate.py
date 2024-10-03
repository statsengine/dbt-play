from google.cloud import bigquery
import random
import pandas as pd
from datetime import timedelta

# Set a seed for reproducibility
random.seed(42)

# Set up BigQuery client
client = bigquery.Client()

# Define your table ID: project_id.dataset_id.table_id
table_id = "dbt-playground-421906.ecommerce.ecommerce_transactions"

# Number of transactions to mutate
n_mutate = 1000  # Number of rows to mutate

# SQL query to select transaction IDs with "Failure" status
failure_transactions_query = f"""
    SELECT transaction_id, payment_amount, payment_method, payment_date
    FROM `{table_id}`
    WHERE payment_status = 'Failure'
    LIMIT {n_mutate}
"""

# Run the query to get a subset of "Failure" transactions
failure_transactions = client.query(failure_transactions_query).to_dataframe()

if failure_transactions.empty:
    print("No transactions with payment_status 'Failure' found.")
else:
    print(f"Mutating {len(failure_transactions)} transactions from 'Failure' to 'Success'...")

    # Simulate mutation: Update the payment status to "Success" and optionally modify other fields
    failure_transactions['payment_status'] = 'Success'

    # Optional: You can also modify other fields if needed (e.g., payment_amount, payment_date)
    # Simulate increased payment amount for testing
    failure_transactions['payment_amount'] += failure_transactions['payment_amount'].apply(lambda x: random.uniform(0, 100))
    failure_transactions['payment_date'] = failure_transactions['payment_date'] + timedelta(days=1)  # Shift the date forward by 1 day

    # Convert the Pandas Timestamp to Python datetime
    failure_transactions['payment_date'] = pd.to_datetime(failure_transactions['payment_date']).dt.to_pydatetime()

    # Convert the DataFrame to BigQuery rows
    rows_to_update = [
        {
            "transaction_id": row["transaction_id"],
            "payment_status": row["payment_status"],
            "payment_amount": row["payment_amount"],
            "payment_method": row["payment_method"],
            "payment_date": row["payment_date"]
        }
        for _, row in failure_transactions.iterrows()
    ]

    # Before inserting rows, convert Timestamp objects to ISO format strings
    for row in rows_to_update:
        if 'payment_date' in row and isinstance(row['payment_date'], pd.Timestamp):
            row['payment_date'] = row['payment_date'].isoformat()

    # Create a BigQuery job configuration to update the rows
    table = client.get_table(table_id)  # Retrieve the table
    job = client.insert_rows_json(table, rows_to_update)  # Insert the mutated rows

    # Check for any errors during the update
    if job == []:
        print(f"{n_mutate} rows successfully mutated from 'Failure' to 'Success'.")
    else:
        print("Errors occurred while mutating the rows:")
        print(job)