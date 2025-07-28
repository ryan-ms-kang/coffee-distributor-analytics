-- BI View: Inventory Health
CREATE OR REPLACE VIEW `analytics_coffee_distributor.bi_inventory_health` AS
SELECT
  i.product_id,
  p.product_name,
  p.product_category,
  i.Warehouse,
  SUM(i.QuantityAvailable) AS total_quantity,
  AVG(i.ReorderPoint) AS avg_reorder_point,
  CASE
    WHEN SUM(i.QuantityAvailable) < AVG(i.ReorderPoint) THEN 'Low Stock'
    ELSE 'Sufficient Stock'
  END AS stock_status,
  CURRENT_DATE() AS report_date
FROM `analytics_coffee_distributor.inventory` i
LEFT JOIN `analytics_coffee_distributor.products` p
  ON i.product_id = p.product_id
GROUP BY i.product_id, p.product_name, p.product_category, i.Warehouse;
