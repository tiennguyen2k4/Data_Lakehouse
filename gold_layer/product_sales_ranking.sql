CREATE VIEW product_sales_ranking AS
SELECT
    d.year,
    p.prd_id,
    p.prd_key,
    SUM(s.sls_quantity) AS total_quantity,
    DENSE_RANK() OVER (PARTITION BY d.year ORDER BY SUM(s.sls_quantity) DESC) AS sales_rank_quantity,
    SUM(s.sls_price) AS total_revenue,
    DENSE_RANK() OVER (PARTITION BY d.year ORDER BY SUM(s.sls_price) DESC) AS sales_rank_revenue
FROM
    fact_sales s   
JOIN
    dim_product p ON s.prd_id = p.prd_id
JOIN
    dim_date d ON s.sls_date_id = d.date_key
GROUP BY
    d.year,
    p.prd_id,
    p.prd_key;
    