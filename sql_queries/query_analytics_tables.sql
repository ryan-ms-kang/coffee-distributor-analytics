-- Create cleaned customers table
CREATE OR REPLACE TABLE analytics_coffee_distributor.customers AS
SELECT
  Id AS customer_id,
  Name AS company_name,
  BillingCountry AS country,
  Industry AS industry,
  AnnualRevenue,
  DATE(CreatedDate) AS created_date
FROM raw_coffee_distributor.salesforce_accounts
WHERE BillingCountry IS NOT NULL;

-- Create cleaned orders table (only closed won deals)
CREATE OR REPLACE TABLE analytics_coffee_distributor.orders AS
SELECT
  Id AS order_id,
  AccountId AS customer_id,
  StageName,
  DATE(CloseDate) AS close_date,
  TotalAmount,
  Probability
FROM raw_coffee_distributor.salesforce_opportunities
WHERE StageName = 'Closed Won';
