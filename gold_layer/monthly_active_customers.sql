CREATE VIEW monthly_active_customers AS
SELECT
    d.month,
    d.year,
    COUNT(DISTINCT s.cst_id) AS active_customers
FROM
    fact_sales s   
JOIN
    dim_date d ON s.sls_date_id = d.date_key
GROUP BY
    d.month,
    d.year
ORDER BY
    d.month ASC;