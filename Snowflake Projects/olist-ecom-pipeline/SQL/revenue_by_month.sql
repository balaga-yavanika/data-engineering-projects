-- Monthly revenue trend

SELECT
  DATE_TRUNC('month', ordered_at)::DATE        AS order_month,
  COUNT(DISTINCT order_id)                      AS total_orders,
  SUM(total_payment)                            AS gross_revenue,
  AVG(total_payment)                            AS avg_order_value,
  SUM(SUM(total_payment)) OVER (
    ORDER BY DATE_TRUNC('month', ordered_at)
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )                                             AS cumulative_revenue
FROM ecommerce_db.marts.fct_orders
WHERE order_status = 'delivered'
GROUP BY 1
ORDER BY 1;
