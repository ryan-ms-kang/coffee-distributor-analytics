-- BI View: Segment vs Inventory
CREATE OR REPLACE VIEW `analytics_coffee_distributor.bi_segment_inventory_alignment` AS
SELECT
  cs.segment_label,
  p.product_category,
  COUNT(DISTINCT cs.customer_id) AS customer_count,
  SUM(o.TotalAmount) AS total_revenue,
  ih.stock_status
FROM `analytics_coffee_distributor.customer_segments` cs
LEFT JOIN `analytics_coffee_distributor.orders` o
  ON cs.customer_id = o.customer_id
LEFT JOIN `analytics_coffee_distributor.products` p
  ON TRUE  -- Simulated join for segment-category mapping
LEFT JOIN `analytics_coffee_distributor.bi_inventory_health` ih
  ON p.product_id = ih.product_id
GROUP BY cs.segment_label, p.product_category, ih.stock_status
ORDER BY cs.segment_label, total_revenue DESC;
