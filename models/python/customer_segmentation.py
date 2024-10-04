import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score, davies_bouldin_score
import uuid
from datetime import datetime

def model(dbt, session):
    dbt.config(
        materialized="table",
        on_schema_change="sync_all_columns",
        packages=["numpy==1.23.1", "pandas", "scikit-learn"]
    )

    # Generate a unique run ID and timestamp
    run_id = str(uuid.uuid4())
    timestamp = datetime.utcnow()

    # Load the staging models
    customers_df = dbt.ref("stg_customers").toPandas()
    orders_df = dbt.ref("stg_orders").toPandas()
    products_df = dbt.ref("stg_products").toPandas()

    # Merge orders with products to get product details
    orders_products_df = orders_df.merge(
        products_df[['product_id', 'price', 'product_category']],
        on="product_id",
        how="left"
    )

    # If amount is not calculated, compute it
    if 'amount' not in orders_products_df.columns or orders_products_df['amount'].isnull().all():
        orders_products_df['amount'] = (
            orders_products_df['price'] * orders_products_df['quantity'] * (1 - orders_products_df['discount_percentage'] / 100)
        )

    # Convert date columns to datetime
    orders_products_df['order_date'] = pd.to_datetime(orders_products_df['order_date'])
    customers_df['registration_date'] = pd.to_datetime(customers_df['registration_date'])

    # Calculate current date
    current_date = orders_products_df['order_date'].max() + pd.Timedelta(days=1)

    # Compute RFM metrics
    rfm = orders_products_df.groupby('user_id').agg({
        'order_date': lambda x: (current_date - x.max()).days,  # Recency
        'order_id': 'nunique',  # Purchase Count
        'amount': 'sum'  # Total Spent
    }).reset_index().rename(columns={
        'order_date': 'recency',
        'order_id': 'purchase_count',
        'amount': 'total_spent'
    })

    # Total quantity purchased
    total_quantity = orders_products_df.groupby('user_id')['quantity'].sum().reset_index().rename(columns={'quantity': 'total_quantity'})

    # Average discount percentage
    avg_discount = orders_products_df.groupby('user_id')['discount_percentage'].mean().reset_index().rename(columns={'discount_percentage': 'avg_discount_percentage'})

    # Unique products purchased
    unique_products = orders_products_df.groupby('user_id')['product_id'].nunique().reset_index().rename(columns={'product_id': 'unique_products_purchased'})

    # Preferred product category
    preferred_category = orders_products_df.groupby(['user_id', 'product_category']).size().reset_index(name='purchase_count_temp')
    preferred_category = preferred_category.loc[preferred_category.groupby('user_id')['purchase_count_temp'].idxmax()][['user_id', 'product_category']].rename(columns={'product_category': 'preferred_category'})

    # Tenure
    tenure = orders_products_df.groupby('user_id').agg({
        'order_date': lambda x: (current_date - x.min()).days
    }).reset_index().rename(columns={'order_date': 'tenure'})

    # Merge all features
    customer_features_df = customers_df.merge(rfm, on='user_id', how='left')
    customer_features_df = customer_features_df.merge(total_quantity, on='user_id', how='left')
    customer_features_df = customer_features_df.merge(avg_discount, on='user_id', how='left')
    customer_features_df = customer_features_df.merge(unique_products, on='user_id', how='left')
    customer_features_df = customer_features_df.merge(preferred_category, on='user_id', how='left')
    customer_features_df = customer_features_df.merge(tenure, on='user_id', how='left')

    # Handle missing values
    customer_features_df['recency'] = customer_features_df['recency'].fillna((current_date - customer_features_df['registration_date']).dt.days)
    customer_features_df['purchase_count'] = customer_features_df['purchase_count'].fillna(0)
    customer_features_df['total_spent'] = customer_features_df['total_spent'].fillna(0)
    customer_features_df['total_quantity'] = customer_features_df['total_quantity'].fillna(0)
    customer_features_df['avg_discount_percentage'] = customer_features_df['avg_discount_percentage'].fillna(0)
    customer_features_df['unique_products_purchased'] = customer_features_df['unique_products_purchased'].fillna(0)
    customer_features_df['preferred_category'] = customer_features_df['preferred_category'].fillna('Unknown')
    customer_features_df['tenure'] = customer_features_df['tenure'].fillna((current_date - customer_features_df['registration_date']).dt.days)

    # Compute avg_order_value
    customer_features_df['avg_order_value'] = customer_features_df['total_spent'] / customer_features_df['purchase_count']

    # Handle division by zero and fill NaN values
    customer_features_df['avg_order_value'] = customer_features_df['avg_order_value'].replace([np.inf, -np.inf], 0)
    customer_features_df['avg_order_value'] = customer_features_df['avg_order_value'].fillna(0)

    # One-hot encode 'sex'
    sex_dummies = pd.get_dummies(customer_features_df['sex'], prefix='sex')
    customer_features_df = pd.concat([customer_features_df, sex_dummies], axis=1)

    # One-hot encode 'preferred_category'
    category_dummies = pd.get_dummies(customer_features_df['preferred_category'], prefix='category')
    customer_features_df = pd.concat([customer_features_df, category_dummies], axis=1)

    # Select features for clustering
    features = customer_features_df[[
        'age',
        'recency',
        'purchase_count',
        'total_spent',
        'avg_order_value',
        'total_quantity',
        'avg_discount_percentage',
        'unique_products_purchased',
        'tenure'
    ] + list(sex_dummies.columns) + list(category_dummies.columns)]

    # Ensure all features are float64
    features = features.astype('float64')

    # Scale features
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features)

    # Apply KMeans clustering
    n_clusters = 4
    random_state = 42
    kmeans = KMeans(n_clusters=n_clusters, random_state=random_state)
    customer_features_df['customer_segment'] = kmeans.fit_predict(scaled_features)

    # Calculate evaluation metrics
    inertia = kmeans.inertia_
    silhouette_avg = silhouette_score(scaled_features, kmeans.labels_)
    db_score = davies_bouldin_score(scaled_features, kmeans.labels_)

    # Calculate cluster sizes
    cluster_sizes = pd.Series(kmeans.labels_).value_counts().to_dict()

    # Print evaluation metrics and cluster information
    print(f"Inertia: {inertia}")
    print(f"Silhouette Score: {silhouette_avg}")
    print(f"Davies-Bouldin Score: {db_score}")
    print("Cluster Sizes:")
    for cluster, size in cluster_sizes.items():
        print(f"Cluster {cluster}: {size}")

    # Include run_id in the output dataframe
    customer_features_df['run_id'] = run_id

    # Prepare metrics data
    metrics_data = [
        (run_id, timestamp, 'inertia', float(inertia)),
        (run_id, timestamp, 'silhouette_score', float(silhouette_avg)),
        (run_id, timestamp, 'davies_bouldin_score', float(db_score)),
        (run_id, timestamp, 'n_clusters', float(n_clusters)),
        (run_id, timestamp, 'random_state', float(random_state))
    ]

    # Include cluster sizes
    for cluster, size in cluster_sizes.items():
        metrics_data.append((run_id, timestamp, f'cluster_{cluster}_size', float(size)))

    # Define the schema for the metrics DataFrame
    metrics_columns = ['run_id', 'timestamp', 'metric_name', 'metric_value']

    # Convert metrics data to a PySpark DataFrame
    metrics_df = session.createDataFrame(metrics_data, metrics_columns)

    # Write metrics to BigQuery
    metrics_df.write.format('bigquery') \
        .option('table', 'dbt-playground-421906.dev_ecommerce_python_models.customer_segmentation_metrics') \
        .mode('append') \
        .save()

    # Return the resulting dataframe with customer segments
    output_columns = [
        'user_id',
        'customer_segment',
        'age',
        'sex',
        'recency',
        'purchase_count',
        'total_spent',
        'avg_order_value',
        'total_quantity',
        'avg_discount_percentage',
        'unique_products_purchased',
        'preferred_category',
        'tenure',
        'run_id'
    ]
    return customer_features_df[output_columns]