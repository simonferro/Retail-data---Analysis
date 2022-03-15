**Approach**

We can start by analyzing the purchasing behavior of customers through a cohort analysis, which can be done based on the raw tables using any SQL engine.

For this, the final result must be a table where each row n contains the starting date of a group of customers and each column m contains the difference in periods between the starting 
date and the respective column. Each cell represents the amount/percentage of users/dollars that started in period n and are still present in the period m.

Therefore, the first thing we can do in SQL is calculate the first_purchase for each user. In this case, I have used a function from bigquery called ARRAY_AGG. It is important to mention that
the choice of period for this analysis is critical. If we choose a very short period, such as days, the values will fluctuate a lot and will be very small compared to the initial cohort size. If
on the other hand we use years as the periods, the timeframe might not be long enough to get meaningful insights. In this analysis we will use months as period.

Using ARRAY_AGG we can extract the row representing the first purchase for each user:

WITH first_purchase AS 
( 
SELECT 
     ARRAY_AGG(rs ORDER BY InvoiceDate ASC LIMIT 1)[OFFSET(0)] AS a 
FROM  
    `cohortanalysis-343816.RetailSales.retailsales` AS rs 
GROUP BY  
    CustomerID 
) 
 
And afterwards get the date and customerId from that first purchase. 
 
, first_cohort AS 
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
    vpp.initial_cohort, 
    period_diff, 
    vpp.distinct_customers, 
    vpp.total_purchases, 
    round(vpp.distinct_customers/sub.distinct_customers,2) AS customer_retention, 
    round(vpp.total_purchases/sub.total_purchases,2) AS purchase_retention 
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
FROM -- without this inner select the pivot also gets total_purchases and each total purchase creates an additional row 
    (SELECT   
        initial_cohort, 
        period_diff,    
        customer_retention 
    FROM  
        values_pre_pivot) 
PIVOT 
    (SUM(customer_retention) FOR period_diff IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13))  AS period 

