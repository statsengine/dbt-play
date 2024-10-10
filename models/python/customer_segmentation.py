import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import uuid
from datetime import datetime

def model(dbt, session):
    dbt.config(
        materialized="table",
        on_schema_change="sync_all_columns",
        packages=["numpy", "pandas", "scikit-learn"]
    )

    # Generate unique run ID and timestamp
    run_id = str(uuid.uuid4())
    timestamp = datetime.utcnow()

    # Load the staging models
    customers_df = dbt.ref("stg_customers").toPandas()
    orders_df = dbt.ref("stg_orders").toPandas()
    products_df = dbt.ref("stg_products").toPandas()

    # Merge orders with products to get order details
    orders_df = orders_df.merge(products_df[['product_id', 'price']], on="product_id", how="left")

    # Calculate total amount if not available
    orders_df['amount'] = orders_df['price'] * orders_df['quantity'] * (1 - orders_df['discount_percentage'] / 100)

    # Calculate current date and RFM (Recency, Frequency, Monetary) metrics
    current_date = pd.to_datetime(orders_df['order_date']).max() + pd.Timedelta(days=1)
    rfm = orders_df.groupby('user_id').agg({
        'order_date': lambda x: (current_date - pd.to_datetime(x).max()).days,  # Recency
        'order_id': 'nunique',  # Frequency
        'amount': 'sum'  # Monetary (Total spent)
    }).reset_index().rename(columns={'order_date': 'recency', 'order_id': 'frequency', 'amount': 'monetary'})

    # Merge RFM with customer data
    customer_features_df = customers_df.merge(rfm, on='user_id', how='left')

    # Handle missing values
    customer_features_df.fillna({'recency': (current_date - pd.to_datetime(customer_features_df['registration_date'])).dt.days,
                                 'frequency': 0, 'monetary': 0}, inplace=True)

    # Features for clustering
    features = customer_features_df[['age', 'recency', 'frequency', 'monetary']].astype('float64')

    # Scale features
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features)

    # Apply KMeans clustering
    kmeans = KMeans(n_clusters=4, random_state=42)
    customer_features_df['customer_segment'] = kmeans.fit_predict(scaled_features)

    # Evaluation Metrics
    inertia = kmeans.inertia_

    # Print cluster information
    print(f"Inertia: {inertia}")
    
    # Add run ID
    customer_features_df['run_id'] = run_id

    # Return the resulting dataframe with customer segments
    return customer_features_df[['user_id', 'customer_segment', 'age', 'recency', 'frequency', 'monetary', 'run_id']]