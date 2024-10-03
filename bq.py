import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from google.cloud import bigquery

# Set a seed for reproducibility
seed_value = 42
np.random.seed(seed_value)
random.seed(seed_value)

# Set up BigQuery client
client = bigquery.Client()

# Function to drop a table if it exists
def drop_table_if_exists(client, table_id):
    try:
        client.get_table(table_id)  # Check if the table exists
        print(f"Table {table_id} exists. Deleting it...")
        client.delete_table(table_id)  # Drop the table
        print(f"Table {table_id} deleted.")
    except Exception as e:
        print(f"Table {table_id} does not exist or error occurred: {str(e)}")

# Set parameters for the dataset
n_orders = 210000  # Number of orders
n_users = 21000  # Number of unique users
n_products = 1000  # Number of unique products
start_date = datetime(2023, 1, 1)
end_date = datetime(2024, 1, 1)

# User IDs
user_ids = np.random.randint(1, n_users + 1, size=n_orders)

# Order IDs (unique)
order_ids = np.arange(1, n_orders + 1)

# Product IDs
product_ids = np.random.randint(1, n_products + 1, size=n_orders)

lambda_quantity = 12  # Mean of the distribution

# Generate quantities using a Poisson distribution
quantities = np.random.poisson(lambda_quantity, size=n_orders)

# Clip the quantities to ensure they are between 1 and 25
quantities = np.clip(quantities, 1, 30).astype(int)  # Cast to integer since quantities should be whole numbers

# Product dataset
product_categories = ['Electronics', 'Books', 'Sport', 'Games', 'Toys']
category_weights = [0.25, 0.05, 0.10, 0.40, 0.20]  # Electronics are more common

# Normal distribution for prices, mean around 5000, std deviation around 3000
mean_price = 900
std_dev_price = 600

# Generate prices using a normal distribution
prices = np.random.normal(mean_price, std_dev_price, size=n_products)

# Clip the prices to ensure they are between 10 and 10,000
prices = np.clip(prices, 10, 10000)

# Create a DataFrame for products
product_data = pd.DataFrame({
    'product_id': np.arange(1, n_products + 1),
    'product_category': np.random.choice(product_categories, p=category_weights, size=n_products),
    'price': np.round(prices, 2)  # Round to 2 decimal places
})

# Create a dictionary to map product_id to price
product_price_dict = dict(zip(product_data['product_id'], product_data['price']))

# Calculate the base amount (before discount) using quantity and product price
base_amounts = quantities * np.array([product_price_dict[pid] for pid in product_ids])

# Generate random discounts between 0% and 25%
discounts = np.random.uniform(0, 0.25, size=n_orders)

# Calculate the final amount after applying the discount
amounts_after_discount = base_amounts * (1 - discounts)

# Skew order dates to weekends (weekends have 50% higher chance)
weekend_days = [5, 6]  # Saturday and Sunday
weekday_days = [0, 1, 2, 3, 4]
order_dates = []
for _ in range(n_orders):
    day = random.choices(weekend_days + weekday_days, weights=[1.5, 1.5] + [1] * 5)[0]
    random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    order_date = random_date.replace(day=(random_date + timedelta(days=(day - random_date.weekday()) % 7)).day)
    order_dates.append(order_date)

# Define time windows with weights for online shopping behavior
# Mornings (7 AM to 9 AM), Afternoons (12 PM to 2 PM), Evenings (6 PM to 11 PM)
time_windows = [
    (0, 6),
    (7, 9),   # Morning shopping hours
    (10, 17),
    (18, 23)  # Evening shopping hours
]

time_window_weights = [0.025, 0.2, 0.075, 0.6]  # Evenings have the highest probability

# Generate realistic order timestamps based on shopping patterns
order_timestamps = []
for order_date in order_dates:
    selected_window = random.choices(time_windows, weights=time_window_weights, k=1)[0]
    hour = random.randint(selected_window[0], selected_window[1])
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    timestamp = order_date + timedelta(hours=hour, minutes=minute, seconds=second)
    order_timestamps.append(timestamp)

# Create the ecommerce orders DataFrame with the discounted amounts and timestamps
ecommerce_data = pd.DataFrame({
    'user_id': user_ids,
    'order_id': order_ids,
    'product_id': product_ids,
    'quantity': quantities,
    'base_amount': np.round(base_amounts, 2),  # Original price before discount
    'discount_percentage': np.round(discounts * 100, 2),  # Store discount as a percentage
    'amount': np.round(amounts_after_discount, 2),  # Final amount after discount
    'order_date': order_dates,
    'order_timestamp': order_timestamps  # Add the realistic order timestamps
})

# Customer dataset
n_customers = n_users  # Same number of users as before
cities = ['Stockholm', 'New York', 'Tokyo', 'Paris', 'London', 'Bejing']
city_weights = [0.3, 0.2, 0.1, 0.15, 0.1, 0.15]  # Stockholm is larger, thus more common
sexes = ['Male', 'Female', 'Other']
sex_weights = [0.60, 0.36, 0.04]  # Male and Female are more common

# Generate random customer details
customer_ids = np.arange(1, n_customers + 1)
user_names = ['user_' + str(i) for i in customer_ids]
emails = ['user' + str(i) + '@example.com' for i in customer_ids]

# Normal distribution for ages (mean 35, std 10)
ages = np.clip(np.random.normal(35, 10, size=n_customers), 18, 70).astype(int)

# Skewed distribution for sex and cities
sexes = np.random.choice(['Male', 'Female', 'Other'], p=sex_weights, size=n_customers)
cities = np.random.choice(cities, p=city_weights, size=n_customers)
addresses = ['Address_' + str(i) for i in customer_ids]

# Create a DataFrame for customers
customer_data = pd.DataFrame({
    'user_id': customer_ids,
    'user_name': user_names,
    'email': emails,
    'age': ages,
    'sex': sexes,
    'address': addresses,
    'city': cities
})

# Create a payment transactions dataset
payment_methods = ['Credit Card', 'PayPal', 'Gift Card', 'Bank Transfer']
payment_statuses = ['Success', 'Failure']

# Generate random payment data
transactions = pd.DataFrame({
    'transaction_id': np.arange(1, n_orders + 1),
    'order_id': order_ids,
    'payment_method': np.random.choice(payment_methods, size=n_orders),
    'payment_status': np.random.choice(payment_statuses, size=n_orders),
    'payment_date': order_timestamps,  # Payments made at the same time as the order
    'payment_amount': amounts_after_discount  # Final amount after discount
})

# Define your table IDs: project_id.dataset_id.table_id
ecommerce_table_id = "dbt-playground-421906.ecommerce.ecommerce_orders"
customer_table_id = "dbt-playground-421906.ecommerce.ecommerce_customers"
product_table_id = "dbt-playground-421906.ecommerce.ecommerce_products"
payment_table_id = "dbt-playground-421906.ecommerce.ecommerce_transactions"

# Drop tables if they exist
drop_table_if_exists(client, ecommerce_table_id)
drop_table_if_exists(client, customer_table_id)
drop_table_if_exists(client, product_table_id)
drop_table_if_exists(client, payment_table_id)

# Load ecommerce data
ecommerce_job = client.load_table_from_dataframe(ecommerce_data, ecommerce_table_id)
ecommerce_job.result()

# Load customer data
customer_job = client.load_table_from_dataframe(customer_data, customer_table_id)
customer_job.result()

# Load product data
product_job = client.load_table_from_dataframe(product_data, product_table_id)
product_job.result()

# Load payment transactions data
payment_job = client.load_table_from_dataframe(transactions, payment_table_id)
payment_job.result()

print("Datasets uploaded to BigQuery successfully.")