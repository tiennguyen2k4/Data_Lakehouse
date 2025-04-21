CREATE VIEW customer_segmentation_age AS
SELECT
    CASE
        WHEN c.age < 18 THEN 'Under 18'
        WHEN c.age BETWEEN 18 AND 25 THEN '18-25'
        WHEN c.age BETWEEN 26 AND 40 THEN '26-40'
        WHEN c.age BETWEEN 41 AND 50 THEN '41-50'
        ELSE 'Over 51'
    END AS age_group,
    d.year,
    c.country,
    COUNT(DISTINCT c.cst_id) AS customer_count,
    SUM(s.sls_price) AS total_revenue
FROM
    fact_sales s
JOIN
    dim_customer c ON s.cst_id = c.cst_id
JOIN
    dim_date d ON s.sls_date_id = d.date_key
GROUP BY
    age_group,
    c.country,
    d.year
ORDER BY 
    d.year ASC;
