CREATE VIEW revenue_by_month_year AS
SELECT
    d.month,
    d.year,
    p.prd_id,
    SUM(s.sls_price) AS revenue
FROM 
    fact_sales s
JOIN
    dim_product p ON s.prd_id = p.prd_id
JOIN
    dim_date d ON s.sls_date_id = d.date_key
GROUP BY
    d.month,
    d.year,
    p.prd_id;

