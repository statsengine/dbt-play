import pandas as pd
from prophet import Prophet  # or from prophet import Prophet, depending on the version
from google.cloud import bigquery
from google.oauth2 import service_account
from pandas_gbq import to_gbq

# Define your project ID and BigQuery dataset information
project_id = 'dbt-playground-421906'
dataset_id = 'dev_ecommerce'

# Initialize a BigQuery client
client = bigquery.Client(project=project_id)

def model(dbt, session):
    query = """
        SELECT order_date, amount
        FROM `dbt-playground-421906.dev_ecommerce_staging.stg_orders`
    """
    stg_orders_df = client.query(query).to_dataframe()

    # Calculate total revenue per day
    daily_revenue_df = stg_orders_df.groupby("order_date").agg(
        total_revenue=pd.NamedAgg(column="amount", aggfunc="sum")
    ).reset_index()

    # Prepare data for Prophet
    daily_revenue_df.rename(columns={"order_date": "ds", "total_revenue": "y"}, inplace=True)

    # Initialize and fit the Prophet model
    model = Prophet()
    model.fit(daily_revenue_df)

    # Make future dataframe for predictions
    future = model.make_future_dataframe(periods=30)  # Forecasting 30 days into the future
    forecast = model.predict(future)

    # Save the forecast results locally as a CSV
    forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].to_csv("output/revenue_forecast.csv", index=False)

    # Prepare the forecast dataframe for BigQuery (remove 'ds' column if needed)
    forecast_bq_df = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
    forecast_bq_df['ds'] = pd.to_datetime(forecast_bq_df['ds'])  # Ensure 'ds' is a datetime object

    # Write forecast results to BigQuery
    to_gbq(
        forecast_bq_df,
        destination_table='dev_ecommerce.revenue_forecast',  # BigQuery destination table
        project_id=project_id,
        if_exists='replace'  # Options: 'fail', 'replace', 'append'
    )

    # Return the forecast DataFrame
    return forecast

if __name__ == "__main__":
    model()