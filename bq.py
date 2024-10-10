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
n_users = 5000
n_products = 1000
start_date = datetime(2019, 9, 4)
end_date = datetime(2024, 10, 4)

# Generate customer data
# Device types to assign to transactions

def generate_customer_data():
    print("Generating customer data...")
    customer_segments = ['High Spender', 'Regular Buyer', 'Bargain Hunter', 'Occasional Shopper']
    segment_weights = [0.1, 0.5, 0.2, 0.2]
    user_segments = np.random.choice(customer_segments, size=n_users, p=segment_weights)

    customer_ids = np.arange(1, n_users + 1)
    user_names = ['user_' + str(i) for i in customer_ids]
    emails = ['user' + str(i) + '@example.com' for i in customer_ids]

    age_means = [25, 45, 65]
    age_stds = [5, 10, 5]
    age_weights = [0.4, 0.5, 0.1]
    ages = np.random.choice(
        [int(np.clip(np.random.normal(mean, std), 18, 90)) for mean, std in zip(age_means, age_stds)],
        size=n_users,
        p=age_weights
    )

    sexes = ['Male', 'Female', 'Non-binary', 'Other']
    sex_weights = [0.58, 0.38, 0.02, 0.02]
    cities = ['Stockholm', 'New York', 'Tokyo', 'Paris', 'London', 'Beijing', 'Berlin', 'Sydney', 'Toronto', 'Dubai']
    city_weights = [0.15, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.05]

    registration_timestamps = []
    total_days = (end_date - start_date).days

    for i in range(n_users):
        day_index = int((i / (n_users - 1)) ** 2 * total_days)
        registration_date = start_date + timedelta(days=day_index)
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        registration_timestamp = registration_date + timedelta(hours=hour, minutes=minute, seconds=second)
        registration_timestamps.append(registration_timestamp)

    # Behavioral profiles assignment
    behavioral_profiles = ['Explorer', 'Brand Loyalist', 'Deal Seeker']
    profile_weights = [0.3, 0.5, 0.2]
    behavioral_profiles_assigned = np.random.choice(behavioral_profiles, size=n_users, p=profile_weights)

    # Device type assignment based on behavioral profile
    device_types = []
    for profile in behavioral_profiles_assigned:
        if profile == 'Explorer':
            device_type = np.random.choice(['Mobile', 'Tablet'], p=[0.7, 0.3])  # Explorers more impulsive on mobile
        elif profile == 'Brand Loyalist':
            device_type = 'Desktop'  # Brand Loyalist prefers desktop for more detailed reviews
        else:
            device_type = np.random.choice(['Mobile', 'Desktop'], p=[0.5, 0.5])  # Deal Seekers use both
        device_types.append(device_type)

    customer_data = pd.DataFrame({
        'user_id': customer_ids,
        'user_name': user_names,
        'email': emails,
        'age': ages,
        'sex': np.random.choice(sexes, size=n_users, p=sex_weights),
        'address': ['Address_' + str(i) for i in customer_ids],
        'city': np.random.choice(cities, size=n_users, p=city_weights),
        'segment': user_segments,
        'behavioral_profile': behavioral_profiles_assigned,
        'device_type': device_types,  # Assigned device type
        'registration_timestamp': registration_timestamps
    })
    print("Customer data generation complete.")
    return customer_data

# Generate product data
def generate_product_data():
    print("Generating product data...")
    product_categories = ['Electronics', 'Books', 'Sport', 'Games', 'Toys', 'Clothing', 'Home & Kitchen', 'Beauty', 'Automotive', 'Grocery']
    category_weights = [0.15, 0.1, 0.1, 0.15, 0.1, 0.1, 0.1, 0.05, 0.05, 0.1]

    brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE']
    colors = ['Red', 'Blue', 'Green', 'Black', 'White']

    category_price_ranges = {
        'Electronics': (100, 5000),
        'Books': (5, 100),
        'Sport': (20, 1000),
        'Games': (10, 600),
        'Toys': (5, 200),
        'Clothing': (10, 400),
        'Home & Kitchen': (100, 10000),
        'Beauty': (5, 200),
        'Automotive': (50, 10000),
        'Grocery': (1, 200)
    }

    product_data = pd.DataFrame({
        'product_id': np.arange(1, n_products + 1),
        'product_category': np.random.choice(product_categories, size=n_products, p=category_weights),
        'brand': np.random.choice(brands, size=n_products),
        'color': np.random.choice(colors, size=n_products)
    })

    prices = []
    for category in product_data['product_category']:
        price_range = category_price_ranges[category]
        price = np.random.uniform(price_range[0], price_range[1])
        prices.append(round(price, 2))

    product_data['price'] = prices

    materials = ['Cotton', 'Leather', 'Plastic', 'Metal', 'Wood']
    sizes = ['S', 'M', 'L', 'XL']
    styles = ['Casual', 'Formal', 'Sporty', 'Vintage', 'Modern']
    ratings = [1, 2, 3, 4, 5]

    product_data['material'] = np.random.choice(materials, size=n_products)
    product_data['size'] = np.random.choice(sizes, size=n_products)
    product_data['style'] = np.random.choice(styles, size=n_products)
    product_data['rating'] = np.random.choice(ratings, size=n_products, p=[0.1, 0.1, 0.2, 0.3, 0.3])

    print("Product data generation complete.")
    return product_data

# Generate interaction data
def generate_interaction_data(customer_data, product_data):
    print("Generating interaction data...")

    age_brand_preferences = {
        'young': ['BrandA', 'BrandB'],
        'middle': ['BrandC', 'BrandD'],
        'senior': ['BrandE']
    }

    sex_color_preferences = {
        'Male': ['Black', 'Blue'],
        'Female': ['Red', 'White'],
        'Non-binary': ['Green', 'Blue'],
        'Other': ['Black', 'White']
    }

    city_brand_preferences = {
        'Stockholm': ['BrandA', 'BrandB'],
        'New York': ['BrandA', 'BrandC'],
        'Tokyo': ['BrandB', 'BrandD'],
        'Paris': ['BrandE', 'BrandA'],
        'London': ['BrandB', 'BrandC'],
        'Beijing': ['BrandD', 'BrandE'],
        'Berlin': ['BrandA', 'BrandD'],
        'Sydney': ['BrandB', 'BrandE'],
        'Toronto': ['BrandC', 'BrandA'],
        'Dubai': ['BrandD', 'BrandB']
    }

    interaction_list = []

    for idx, user in customer_data.iterrows():
        user_id = user['user_id']
        age = user['age']
        sex = user['sex']
        city = user['city']
        profile = user['behavioral_profile']

        # Determine age group
        if age < 30:
            age_group = 'young'
        elif age < 60:
            age_group = 'middle'
        else:
            age_group = 'senior'

        preferred_brands = list(set(age_brand_preferences[age_group] + city_brand_preferences.get(city, [])))
        preferred_colors = sex_color_preferences[sex]

        # Set exploration rates per profile
        if profile == 'Explorer':
            exploration_rate = 0.3  # Explorers still explore, but prefer known products
        elif profile == 'Brand Loyalist':
            exploration_rate = 0.02  # Brand Loyalists rarely explore
        elif profile == 'Deal Seeker':
            exploration_rate = 0.15  # Deal Seekers explore products but prefer deals

        num_interactions = np.random.poisson(8) + 5  # Reduced range of interactions

        for _ in range(num_interactions):
            interaction_day = np.random.randint(0, (end_date - start_date).days)
            interaction_date = start_date + timedelta(days=interaction_day)

            # Product selection based on exploration or preferred brands/colors
            if np.random.rand() < (1 - exploration_rate):
                product_pool = product_data[
                    (product_data['brand'].isin(preferred_brands)) &
                    (product_data['color'].isin(preferred_colors))
                ]
            else:
                product_pool = product_data.sample(n=100)

            product = product_pool.sample(n=1).iloc[0]

            # Focus interactions on purchases and cart actions
            interaction_type = np.random.choice(
                ['purchase', 'add_to_cart', 'view', 'click', 'dislike'],
                p=[0.5, 0.3, 0.1, 0.05, 0.05]  # Prioritize purchases and carts
            )

            interaction_timestamp = interaction_date + timedelta(
                hours=np.random.randint(0, 24),
                minutes=np.random.randint(0, 60),
                seconds=np.random.randint(0, 60)
            )

            interaction_list.append({
                'user_id': user_id,
                'product_id': product['product_id'],
                'interaction_type': interaction_type,
                'interaction_timestamp': interaction_timestamp
            })

        # Generate dislikes based on non-preferred brands/colors
        num_negative_interactions = int(num_interactions * 0.2)
        for _ in range(num_negative_interactions):
            negative_products = product_data[
                (~product_data['brand'].isin(preferred_brands)) &
                (~product_data['color'].isin(preferred_colors))
            ]
            if not negative_products.empty:
                product = negative_products.sample(n=1).iloc[0]
                interaction_type = 'dislike'
                interaction_day = np.random.randint(0, (end_date - start_date).days)
                interaction_date = start_date + timedelta(days=interaction_day)
                interaction_timestamp = interaction_date + timedelta(
                    hours=np.random.randint(0, 24),
                    minutes=np.random.randint(0, 60)
                )
                interaction_list.append({
                    'user_id': user_id,
                    'product_id': product['product_id'],
                    'interaction_type': interaction_type,
                    'interaction_timestamp': interaction_timestamp
                })

    interaction_data = pd.DataFrame(interaction_list)
    print("Interaction data generation complete.")
    return interaction_data

# Generate order data
def generate_order_data(customer_data, product_data):
    print("Generating order data...")
    orders_list = []

    holiday_periods = {
        (11, 12): (1.4, 1.8),
        (6, 7, 8): (1.1, 1.3),
        (1, 2): (0.7, 0.9)
    }

    trend_factor = 0.0001

    age_brand_preferences = {
        'young': ['BrandA', 'BrandB'],
        'middle': ['BrandC', 'BrandD'],
        'senior': ['BrandE']
    }

    for idx, user in customer_data.iterrows():
        user_id = user['user_id']
        segment = user['segment']
        age = user['age']
        registration_timestamp = user['registration_timestamp']
        profile = user['behavioral_profile']

        if age < 30:
            age_group = 'young'
        elif age < 60:
            age_group = 'middle'
        else:
            age_group = 'senior'

        preferred_brands = list(set(age_brand_preferences[age_group]))
        exploration_rate = 0.05 if profile == 'Brand Loyalist' else 0.3

        base_orders = {
            'High Spender': np.random.negative_binomial(10, 0.5) + 20,
            'Regular Buyer': np.random.negative_binomial(5, 0.5) + 5,
            'Bargain Hunter': np.random.negative_binomial(3, 0.5) + 3,
            'Occasional Shopper': np.random.poisson(2)
        }

        num_orders = base_orders[segment]
        num_orders += int(trend_factor * idx)
        num_orders = max(1, num_orders)

        for _ in range(num_orders):
            # Calculate the number of days after registration, ensuring valid range
            days_diff = (end_date - registration_timestamp).days
            if days_diff <= 0:
                random_days = 0  # If the range is invalid (end_date <= registration_date), keep the same date
            else:
                random_days = random.randint(0, days_diff)

            order_date = registration_timestamp + timedelta(days=random_days)

            seasonal_multiplier = 1.0
            for months, multiplier_range in holiday_periods.items():
                if order_date.month in months:
                    seasonal_multiplier = np.random.uniform(*multiplier_range)
                    break

            noise = np.random.normal(1.0, 0.1)
            num_items = int(np.random.lognormal(mean=1.2, sigma=0.7) * seasonal_multiplier * noise)
            num_items = max(1, num_items)

            for _ in range(num_items):
                if np.random.rand() < (1 - exploration_rate):
                    product_pool = product_data[
                        (product_data['brand'].isin(preferred_brands))
                    ]
                    product = product_pool.sample(n=1).iloc[0]
                else:
                    product = product_data.sample(n=1).iloc[0]

                quantity = int(np.random.lognormal(mean=0.7, sigma=0.5))
                quantity = max(1, min(quantity, 20))

                base_amount = product['price'] * quantity
                discount = np.random.uniform(0.3, 0.5) if segment == 'Bargain Hunter' else np.random.uniform(0.05, 0.15)
                amount = base_amount * (1 - discount)

                order_timestamp = order_date + timedelta(
                    hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59)
                )

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

    ecommerce_data = pd.DataFrame(orders_list)
    print("Order data generation complete.")
    return ecommerce_data

# Generate transaction data
def generate_transaction_data(ecommerce_data, customer_data):
    print("Generating transaction data...")
    
    # Define payment options and weights
    payment_methods = ['Credit Card', 'PayPal', 'Gift Card', 'Bank Transfer', 'Mobile Payment']
    payment_method_weights = [0.5, 0.2, 0.1, 0.1, 0.1]  # Probabilities for randomly choosing payment methods
    
    # Set success rate high, but introduce some failures
    payment_statuses = ['Success', 'Failure']
    payment_status_weights = [0.97, 0.03]  # 97% success

    # Transaction DataFrame initially based on ecommerce orders
    transactions = ecommerce_data[['order_id', 'amount', 'order_timestamp', 'user_id']].copy()
    
    # Map segment behavior based on customer preference
    segment_payment_preferences = {
        'High Spender': ['Credit Card', 'Mobile Payment'],
        'Regular Buyer': ['Credit Card', 'PayPal'],
        'Bargain Hunter': ['PayPal', 'Gift Card'],
        'Occasional Shopper': ['Credit Card', 'Bank Transfer']
    }
    
    # Apply payment method based on user segment in the customer_data
    # Linking transactions by user_id back to users' profiles in customer_data for segment-specific preferences
    transactions['payment_method'] = transactions['user_id'].apply(
        lambda x: np.random.choice(segment_payment_preferences[
            customer_data.loc[customer_data['user_id'] == x, 'segment'].values[0]], p=[0.7, 0.3])
    )

    # Apply payment status (e.g., occasional high-value failures)
    transactions['payment_status'] = transactions['amount'].apply(
        lambda x: 'Failure' if x > 2000 and np.random.rand() > 0.9 else np.random.choice(payment_statuses, p=payment_status_weights)
    )
    
    # Add unique transaction_id, using the length of current transactions
    transactions['transaction_id'] = np.arange(1, len(transactions) + 1)
    
    # Payment details (same as amount for simplicity)
    transactions['payment_date'] = transactions['order_timestamp']
    transactions['payment_amount'] = transactions['amount']  # For now, it's equal to order amount
    
    print("Transaction data generation complete.")
    return transactions

# Generate datasets
customer_data = generate_customer_data()
product_data = generate_product_data()
interaction_data = generate_interaction_data(customer_data, product_data)
ecommerce_data = generate_order_data(customer_data, product_data)
transaction_data = generate_transaction_data(ecommerce_data, customer_data)

# Define your table IDs: project_id.dataset_id.table_id
ecommerce_table_id = "dbt-playground-421906.ecommerce.ecommerce_orders"
customer_table_id = "dbt-playground-421906.ecommerce.ecommerce_customers"
product_table_id = "dbt-playground-421906.ecommerce.ecommerce_products"
interaction_table_id = "dbt-playground-421906.ecommerce.ecommerce_interactions"
transaction_table_id = "dbt-playground-421906.ecommerce.ecommerce_transactions"

# Drop tables if they exist
drop_table_if_exists(client, ecommerce_table_id)
drop_table_if_exists(client, customer_table_id)
drop_table_if_exists(client, product_table_id)
drop_table_if_exists(client, interaction_table_id)
drop_table_if_exists(client, transaction_table_id)

# Load datasets into BigQuery
ecommerce_job = client.load_table_from_dataframe(ecommerce_data, ecommerce_table_id)
ecommerce_job.result()

customer_job = client.load_table_from_dataframe(customer_data, customer_table_id)
customer_job.result()

product_job = client.load_table_from_dataframe(product_data, product_table_id)
product_job.result()

interaction_job = client.load_table_from_dataframe(interaction_data, interaction_table_id)
interaction_job.result()

transaction_job = client.load_table_from_dataframe(transaction_data, transaction_table_id)
transaction_job.result()

print("Datasets uploaded to BigQuery successfully.")