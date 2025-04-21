CREATE VIEW potential_customer AS
SELECT
    c.cst_id,
    c.full_name,
    c.cst_birthday,
    c.age,
    c.country,
    SUM(s.sls_price) AS total_revenue,
    DENSE_RANK() OVER (ORDER BY SUM(s.sls_price) DESC) AS revenue_rank
FROM
    fact_sales s
JOIN
    dim_customer c ON s.cst_id = c.cst_id
GROUP BY
    c.cst_id,
    c.full_name,
    c.cst_birthday,
    c.age,
    c.country;