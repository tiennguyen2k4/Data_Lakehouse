CREATE VIEW sales_by_region AS
SELECT
    d.year,
    c.country,
    SUM(s.sls_price) AS total_revenue,
    SUM(s.sls_quantity) AS total_quantity
FROM
    fact_sales s
JOIN
    dim_customer c ON s.cst_id = c.cst_id  
JOIN
    dim_date d ON s.sls_date_id = d.date_key
GROUP BY
    d.year,
    c.country
ORDER BY
    d.year ASC;