"""
Extracts cleaned customer data from BigQuery (`analytics_coffee_distributor.customer_ltv`)
"""

from google.cloud import bigquery
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import json

# -------------------------------------------------------------------------
# 1. BigQuery Setup
# -------------------------------------------------------------------------
PROJECT_ID = "<YOUR_PROJECT_ID>"  # Replace with your GCP project
ANALYTICS_TABLE = "analytics_coffee_distributor.customer_ltv"
OUTPUT_TABLE = "analytics_coffee_distributor.customer_segments"

client = bigquery.Client(project=PROJECT_ID)

# -------------------------------------------------------------------------
# 2. Extract Data
# -------------------------------------------------------------------------
query = f"""
SELECT 
    customer_id,
    company_name,
    country,
    lifetime_revenue,
    total_orders
FROM `{ANALYTICS_TABLE}`
WHERE lifetime_revenue IS NOT NULL AND total_orders IS NOT NULL
"""
df_customers = client.query(query).to_dataframe()

if df_customers.empty:
    raise ValueError("No data found in customer_ltv table. Check your BigQuery dataset.")

print(f"Loaded {len(df_customers)} customers from {ANALYTICS_TABLE}.")