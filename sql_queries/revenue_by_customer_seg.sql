-- Revenue by Customer Segment
CREATE OR REPLACE VIEW `analytics_coffee_distributor.bi_revenue_by_segment` AS
SELECT
  segment_label,
  COUNT(DISTINCT customer_id) AS total_customers,
  COUNT(order_id) AS total_orders,
  SUM(o.TotalAmount) AS total_revenue,
  AVG(o.TotalAmount) AS avg_order_value
FROM `analytics_coffee_distributor.customer_segments` cs
LEFT JOIN `analytics_coffee_distributor.orders` o
  ON cs.customer_id = o.customer_id
GROUP BY segment_label
ORDER BY total_revenue DESC;
