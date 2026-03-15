SELECT
  p.category,
  COUNT(DISTINCT i.order_id)              AS order_count,
  SUM(i.unit_price)                       AS total_revenue,
  AVG(i.unit_price)                       AS avg_price,
  ROUND(AVG(r.review_score), 2)           AS avg_review_score,
  RANK() OVER (ORDER BY SUM(i.unit_price) DESC) AS revenue_rank
FROM ecommerce_db.staging.stg_order_items  i
JOIN ecommerce_db.staging.stg_products     p ON i.product_id = p.product_id
JOIN ecommerce_db.staging.stg_orders       o ON i.order_id   = o.order_id
LEFT JOIN ecommerce_db.staging.stg_order_reviews r ON i.order_id = r.order_id
WHERE o.order_status = 'delivered'
GROUP BY p.category
ORDER BY total_revenue DESC
LIMIT 20;
