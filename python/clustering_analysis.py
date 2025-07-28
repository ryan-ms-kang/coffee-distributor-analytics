"""
Customer Segmentation Script for Coffee Bean Distributor
--------------------------------------------------------
1. Extracts cleaned customer data from BigQuery (`analytics_coffee_distributor.customer_ltv`).
2. Prepares features (lifetime revenue, total orders, geographic region).
3. Performs feature scaling and clustering (K-Means).
4. Assigns each customer to a segment (e.g., High-Value, Mid-Tier, Low-Tier).
5. Writes the resulting customer segments back into BigQuery (`analytics_coffee_distributor.customer_segments`).
"""

from google.cloud import bigquery
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import json

# BigQuery Setup
PROJECT_ID = "<coffee-market-analysis>" 
ANALYTICS_TABLE = "analytics_coffee_distributor.customer_ltv"
OUTPUT_TABLE = "analytics_coffee_distributor.customer_segments"

client = bigquery.Client(project=PROJECT_ID)

# Extract Data
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

# Feature Engineering
# Region encoding: Group countries into regions (example for global expansion)
region_map = {
    "USA": "North America",
    "Canada": "North America",
    "Brazil": "South America",
    "Germany": "Europe",
    "Japan": "Asia",
    "Australia": "Oceania"
}
df_customers["region"] = df_customers["country"].map(region_map).fillna("Other")

# Numerical features for clustering
features = df_customers[["lifetime_revenue", "total_orders"]].copy()

# Add log scaling for skewed revenue distribution
features["log_revenue"] = np.log1p(features["lifetime_revenue"])

# Final feature matrix
X = features[["log_revenue", "total_orders"]].values

# Standardization
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# K-Means Clustering
# Choosing 3 clusters: High-Value, Mid-Tier, Low-Tier
kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
df_customers["segment_id"] = kmeans.fit_predict(X_scaled)

# Map cluster IDs to labels based on average revenue
cluster_summary = df_customers.groupby("segment_id")["lifetime_revenue"].mean().sort_values()
segment_map = {cluster_id: f"Segment_{i+1}" for i, cluster_id in enumerate(cluster_summary.index)}
df_customers["segment_label"] = df_customers["segment_id"].map(segment_map)
df_segments = df_customers[[
    "customer_id",
    "company_name",
    "country",
    "region",
    "lifetime_revenue",
    "total_orders",
    "segment_id",
    "segment_label"
]]

print("Cluster Summary (Average Lifetime Revenue by Segment):")
print(df_segments.groupby("segment_label")["lifetime_revenue"].mean())

# Write Segments Back to BigQuery
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE"  # Overwrite each run
)

job = client.load_table_from_dataframe(df_segments, OUTPUT_TABLE, job_config=job_config)
job.result() 

print(f"Customer segments successfully written to {OUTPUT_TABLE}.")
