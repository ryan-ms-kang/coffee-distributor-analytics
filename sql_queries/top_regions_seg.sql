-- BI View: Top Regions by Segment
CREATE OR REPLACE VIEW `analytics_coffee_distributor.bi_top_regions` AS
SELECT
  segment_label,
  region,
  COUNT(DISTINCT customer_id) AS num_customers,
  SUM(o.TotalAmount) AS total_revenue,
  RANK() OVER (PARTITION BY segment_label ORDER BY SUM(o.TotalAmount) DESC) AS revenue_rank
FROM `analytics_coffee_distributor.customer_segments` cs
LEFT JOIN `analytics_coffee_distributor.orders` o
  ON cs.customer_id = o.customer_id
GROUP BY segment_label, region;
