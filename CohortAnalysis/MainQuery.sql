/* 
This is the complete query used to generate the cohort analysis. The last query, which would output the pivoted final values, is ommited in this document and is replaced by the statement
"select * from values_pre_pivot" since depending on the value of interest this last query would change. The possible variations of this last query are added in another document.
*/

WITH first_purchase AS
(
SELECT
     ARRAY_AGG(rs ORDER BY InvoiceDate ASC LIMIT 1)[OFFSET(0)] AS a
FROM 
    `cohortanalysis-343816.RetailSales.retailsales` AS rs
GROUP BY 
    CustomerID
), first_cohort AS
(
SELECT 
    a.InvoiceDate, a.CustomerID
FROM 
    first_purchase 
),
all_cohort_dates AS
(
SELECT 
    *   
FROM 
     UNNEST
        (GENERATE_DATE_ARRAY
            ( 
                (SELECT DATE_TRUNC(DATE(MIN(InvoiceDate)),month)
                    FROM `cohortanalysis-343816.RetailSales.retailsales`) 
                ,(SELECT DATE_TRUNC(DATE(MAX(InvoiceDate)),month) 
                    FROM `cohortanalysis-343816.RetailSales.retailsales`), 
                 INTERVAL 1 month
            ) 
        ) AS month
), cohorted_purchases AS
(
SELECT 
    cd.*,rs.*,DATE_TRUNC(DATE(fc.InvoiceDate),month) AS initial_cohort,DATE_DIFF(DATE(rs.InvoiceDate), DATE(fc.InvoiceDate), month ) AS period_diff
FROM 
    all_cohort_dates  AS cd
LEFT JOIN 
    `cohortanalysis-343816.RetailSales.retailsales` AS rs ON DATE_TRUNC(DATE(rs.InvoiceDate),month) = cd.month
LEFT JOIN 
    first_cohort AS fc ON fc.CustomerID=rs.CustomerID
WHERE 
    rs.CustomerID IS NOT NULL
),
values_pre_pivot_total AS
(
SELECT 
    initial_cohort, period_diff, count(DISTINCT CustomerID) AS distinct_customers, SUM(Quantity*UnitPrice) AS total_purchases
FROM 
    cohorted_purchases
GROUP BY 
    initial_cohort, period_diff
ORDER BY 
    initial_cohort ASC, period_diff ASC
),
values_pre_pivot AS
(
SELECT
    vpp.initial_cohort,period_diff,vpp.distinct_customers,vpp.total_purchases, round(vpp.distinct_customers/sub.distinct_customers,2) AS customer_retention, round(vpp.total_purchases/sub.total_purchases,2) AS purchase_retention
FROM 
    values_pre_pivot_total AS vpp
LEFT JOIN 
    (
        SELECT 
            initial_cohort,
            distinct_customers,
            total_purchases
        FROM
            values_pre_pivot_total
        WHERE 
            period_diff=0
    ) AS sub
ON sub.initial_cohort=vpp.initial_cohort
)
SELECT
    * 
FROM values_pre_pivot

