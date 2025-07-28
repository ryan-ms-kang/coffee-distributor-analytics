# To keep the original data private, generate fake data to best replicate the analysis process.

import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker() # T1
Faker.seed(42)
random.seed(42)

# Utility function to generate dates within a range
def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

# ---- 1. Salesforce Accounts ----
def generate_salesforce_accounts(num_accounts=500):
    industries = ['Food & Beverage', 'Wholesale', 'Retail', 'Distribution', 'Manufacturing']
    countries = ['USA', 'Canada', 'Japan', 'Germany', 'Brazil', 'Australia']
    
    accounts = []
    for i in range(num_accounts):
        accounts.append({
            'Id': f'ACC{i+1000}',
            'Name': fake.company(),
            'BillingCountry': random.choice(countries),
            'Industry': random.choice(industries),
            'AnnualRevenue': round(random.uniform(1_000_000, 50_000_000), 2),
            'CreatedDate': fake.date_between(start_date='-5y', end_date='today').isoformat()
        })
    return pd.DataFrame(accounts)

# ---- 2. Salesforce Opportunities (Orders) ----
def generate_salesforce_opportunities(accounts_df, num_opps=5000):
    stages = ['Prospecting', 'Qualification', 'Proposal', 'Negotiation', 'Closed Won', 'Closed Lost']
    opps = []
    for i in range(num_opps):
        account = accounts_df.sample(1).iloc[0]
        close_date = fake.date_between(start_date='-2y', end_date='today')
        stage = random.choices(stages, weights=[10, 15, 20, 15, 30, 10])[0]
        opps.append({
            'Id': f'OPP{i+2000}',
            'AccountId': account['Id'],
            'StageName': stage,
            'CloseDate': close_date.isoformat(),
            'TotalAmount': round(random.uniform(5000, 500_000), 2) if stage == 'Closed Won' else 0.0,
            'Probability': 100 if stage == 'Closed Won' else random.randint(10, 90)
        })
    return pd.DataFrame(opps)

# ---- 3. NetSuite Products (Items) ----
def generate_netsuite_items(num_items=200):
    categories = ['Single Origin', 'Blend', 'Decaf', 'Organic', 'Specialty']
    items = []
    for i in range(num_items):
        items.append({
            'ItemId': f'ITEM{i+3000}',
            'Name': f"{random.choice(categories)} Coffee {fake.word().title()}",
            'Category': random.choice(categories),
            'UnitCost': round(random.uniform(5, 25), 2),
            'ListPrice': round(random.uniform(10, 50), 2)
        })
    return pd.DataFrame(items)

# ---- 4. NetSuite Inventory ----
def generate_netsuite_inventory(items_df, num_records=10_000):
    warehouses = ['East Coast Warehouse', 'West Coast Warehouse', 'Europe Warehouse', 'Asia Warehouse']
    inventory = []
    for i in range(num_records):
        item = items_df.sample(1).iloc[0]
        inventory.append({
            'InventoryId': f'INV{i+4000}',
            'ItemId': item['ItemId'],
            'Warehouse': random.choice(warehouses),
            'QuantityAvailable': random.randint(0, 1000),
            'ReorderPoint': random.randint(100, 300),
            'LastUpdated': fake.date_time_between(start_date='-6mo', end_date='now').isoformat()
        })
    return pd.DataFrame(inventory)

# ---- Generate Data ----
accounts_df = generate_salesforce_accounts()
opportunities_df = generate_salesforce_opportunities(accounts_df)
items_df = generate_netsuite_items()
inventory_df = generate_netsuite_inventory(items_df)

# ---- Save CSVs ----
accounts_df.to_csv('salesforce_accounts.csv', index=False)
opportunities_df.to_csv('salesforce_opportunities.csv', index=False)
items_df.to_csv('netsuite_items.csv', index=False)
inventory_df.to_csv('netsuite_inventory.csv', index=False)

print("Datasets generated and saved as CSV files:")
print(" - salesforce_accounts.csv")
print(" - salesforce_opportunities.csv")
print(" - netsuite_items.csv")
print(" - netsuite_inventory.csv")
