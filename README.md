# Coffee Distributor Analytics Pipeline

## **Overview**
A global coffee bean distributor (anonymized for public display) wanted to modernize its analytics by integrating **Salesforce CRM** and **NetSuite ERP** data into a single, automated pipeline.  
The goal was to gain visibility into **customer segments, retention trends, and inventory health** through real-time dashboards.

This work implements a **cloud-based data pipeline** and analytics layer using:
- **Airbyte** for ETL ingestion.
- **BigQuery** as the data warehouse.
- **Airflow** for orchestration.
- **Python (scikit-learn)** for customer segmentation (K-means clustering).
- **BI views** for reporting and dashboards.

---

## **Architecture**
```text
Salesforce / NetSuite  -->  Airbyte  -->  BigQuery (Raw)
                                    -->  BigQuery (Analytics)
                                    -->  Python Segmentation  -->  Customer Segments Table
                                    -->  BI Views (Revenue by Segment, Inventory Health)
                                    -->  Looker Studio / Tableau Dashboards
