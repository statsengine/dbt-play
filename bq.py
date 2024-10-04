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
n_users = 10000  # Number of unique users (increased for more data)
n_orders = n_users * np.random.randint(2, 20)  # Each user makes 2 to 10 orders on average
n_products = 2000  # Number of unique products (increased)

start_date = datetime(2022, 1, 1)
end_date = datetime(2024, 1, 1)

# **Customer Segments**
customer_segments = ['High Spender', 'Regular Buyer', 'Bargain Hunter', 'Occasional Shopper']
segment_weights = [0.1, 0.5, 0.2, 0.2]  # Assign probabilities to each segment

# Assign each user to a segment
user_segments = np.random.choice(customer_segments, size=n_users, p=segment_weights)

# Create a DataFrame for customers
customer_ids = np.arange(1, n_users + 1)
customer_data = pd.DataFrame({
    'user_id': customer_ids,
    'segment': user_segments
})

# **Customer Characteristics**
# Expand cities and adjust weights
cities = ['Stockholm', 'New York', 'Tokyo', 'Paris', 'London', 'Beijing', 'Berlin', 'Sydney', 'Toronto', 'Dubai']
city_weights = [0.15, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.05]

# Expand sexes and adjust weights
sexes = ['Male', 'Female', 'Non-binary', 'Other']
sex_weights = [0.58, 0.38, 0.02, 0.02]

# Generate random customer details
user_names = ['user_' + str(i) for i in customer_ids]
emails = ['user' + str(i) + '@example.com' for i in customer_ids]

# Age distribution using a mixture of Gaussians to simulate different age groups
age_means = [25, 45, 65]
age_stds = [5, 10, 5]
age_weights = [0.4, 0.5, 0.1]
ages = np.random.choice(
    [int(np.clip(np.random.normal(mean, std), 18, 90)) for mean, std in zip(age_means, age_stds)],
    size=n_users,
    p=age_weights
)

# Assign sexes and cities
customer_data['user_name'] = user_names
customer_data['email'] = emails
customer_data['age'] = ages
customer_data['sex'] = np.random.choice(sexes, size=n_users, p=sex_weights)
customer_data['address'] = ['Address_' + str(i) for i in customer_ids]
customer_data['city'] = np.random.choice(cities, size=n_users, p=city_weights)

# Generate random registration dates for customers
# Assuming customers registered between 'start_date' and 'end_date'
registration_dates = pd.to_datetime(np.random.choice(
    pd.date_range(start=start_date - timedelta(days=365), end=end_date - timedelta(days=1)),
    size=n_users
))

# Add 'registration_date' to customer_data
customer_data['registration_date'] = registration_dates

# **Product Data**
product_categories = ['Electronics', 'Books', 'Sport', 'Games', 'Toys', 'Clothing', 'Home & Kitchen', 'Beauty', 'Automotive', 'Grocery']
category_weights = [0.15, 0.1, 0.1, 0.15, 0.1, 0.1, 0.1, 0.05, 0.05, 0.1]  # Adjusted weights

# Price ranges for different categories
category_price_ranges = {
    'Electronics': (100, 5000),
    'Books': (50, 200),
    'Sport': (20, 1000),
    'Games': (10, 600),
    'Toys': (5, 200),
    'Clothing': (10, 500),
    'Home & Kitchen': (100, 10000),
    'Beauty': (5, 200),
    'Automotive': (50, 500000),
    'Grocery': (1, 200)
}

# Assign categories to products
product_data = pd.DataFrame({
    'product_id': np.arange(1, n_products + 1),
    'product_category': np.random.choice(product_categories, size=n_products, p=category_weights)
})

# Assign prices based on category price ranges
prices = []
for category in product_data['product_category']:
    price_range = category_price_ranges[category]
    price = np.random.uniform(price_range[0], price_range[1])
    prices.append(round(price, 2))

product_data['price'] = prices

# **Product Preferences Based on Demographics**
# Create preference scores for each user and category
user_preferences = {}
for idx, row in customer_data.iterrows():
    user_id = row['user_id']
    age = row['age']
    sex = row['sex']
    segment = row['segment']

    # Base preference scores
    preferences = {}
    for category in product_categories:
        base_score = np.random.uniform(0, 1)

        # Adjust based on segment
        if segment == 'High Spender':
            base_score *= np.random.uniform(1.2, 1.5)
        elif segment == 'Bargain Hunter':
            base_score *= np.random.uniform(0.8, 1.0)
        elif segment == 'Occasional Shopper':
            base_score *= np.random.uniform(0.5, 0.8)

        # Adjust based on age
        if category == 'Electronics' and age < 35:
            base_score *= 1.2
        if category == 'Books' and age > 50:
            base_score *= 1.3

        # Adjust based on sex
        if category == 'Beauty' and sex == 'Female':
            base_score *= 1.5
        if category == 'Automotive' and sex == 'Male':
            base_score *= 1.2

        preferences[category] = base_score

    # Normalize preferences
    total = sum(preferences.values())
    for category in preferences:
        preferences[category] /= total

    user_preferences[user_id] = preferences

# **Order Generation**
orders_list = []

for idx, user in customer_data.iterrows():
    user_id = user['user_id']
    segment = user['segment']
    age = user['age']
    sex = user['sex']

    # Determine number of orders for this user
    if segment == 'High Spender':
        num_orders = np.random.negative_binomial(10, 0.5) + 10  # More orders
    elif segment == 'Regular Buyer':
        num_orders = np.random.negative_binomial(5, 0.5) + 5
    elif segment == 'Bargain Hunter':
        num_orders = np.random.negative_binomial(3, 0.5) + 3
    else:  # Occasional Shopper
        num_orders = np.random.negative_binomial(1, 0.5) + 1

    num_orders = max(1, num_orders)  # Ensure at least one order

    for _ in range(num_orders):
        # Randomly select a date
        random_days = random.randint(0, (end_date - start_date).days)
        order_date = start_date + timedelta(days=random_days)

        # Adjust order_date based on seasonality
        seasonal_multiplier = 1.0
        if order_date.month in [11, 12]:  # Holiday season
            seasonal_multiplier = np.random.uniform(1.5, 2.0)
        elif order_date.month in [6, 7, 8]:  # Summer
            seasonal_multiplier = np.random.uniform(1.2, 1.5)
        elif order_date.month in [1, 2]:  # After holidays
            seasonal_multiplier = np.random.uniform(0.5, 0.8)

        # Determine number of items in this order
        num_items = int(np.random.lognormal(mean=1.5, sigma=0.8) * seasonal_multiplier)
        num_items = max(1, num_items)  # Ensure at least one item

        # Select products based on user preferences
        categories = list(user_preferences[user_id].keys())
        preferences = list(user_preferences[user_id].values())
        chosen_categories = np.random.choice(categories, size=num_items, p=preferences)

        for category in chosen_categories:
            # Choose a random product from the selected category
            products_in_category = product_data[product_data['product_category'] == category]
            product = products_in_category.sample(n=1).iloc[0]

            # Quantity purchased
            quantity = int(np.random.lognormal(mean=0.5, sigma=0.6))
            quantity = max(1, min(quantity, 20))  # Between 1 and 10

            # Base amount
            base_amount = product['price'] * quantity

            # Discount
            if segment == 'Bargain Hunter':
                discount = np.random.uniform(0.1, 0.5)  # Higher discounts
            else:
                discount = np.random.uniform(0, 0.3)

            amount = base_amount * (1 - discount)

            # Order timestamp
            hour = random.randint(0, 23)
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            order_timestamp = order_date + timedelta(hours=hour, minutes=minute, seconds=second)

            # Create order record
            orders_list.append({
                'user_id': user_id,
                'order_id': len(orders_list) + 1,
                'product_id': product['product_id'],
                'quantity': quantity,
                'base_amount': round(base_amount, 2),
                'discount_percentage': round(discount * 100, 2),
                'amount': round(amount, 2),
                'order_date': order_date.date(),
                'order_timestamp': order_timestamp
            })

# Convert orders list to DataFrame
ecommerce_data = pd.DataFrame(orders_list)

# **Payment Transactions**
payment_methods = ['Credit Card', 'PayPal', 'Gift Card', 'Bank Transfer', 'Mobile Payment']
payment_method_weights = [0.5, 0.2, 0.1, 0.1, 0.1]  # Adjusted weights

payment_statuses = ['Success', 'Failure']
payment_status_weights = [0.97, 0.03]  # 97% success rate

# Create a payment transactions dataset
transactions = ecommerce_data[['order_id', 'amount', 'order_timestamp']].copy()
transactions['transaction_id'] = transactions.index + 1
transactions['payment_method'] = np.random.choice(payment_methods, size=len(transactions), p=payment_method_weights)
transactions['payment_status'] = np.random.choice(payment_statuses, size=len(transactions), p=payment_status_weights)
transactions['payment_date'] = transactions['order_timestamp']
transactions['payment_amount'] = transactions['amount']

# **Final Customer Data**
# Remove temporary 'segment' column if not needed
# customer_data = customer_data.drop(columns=['segment'])

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