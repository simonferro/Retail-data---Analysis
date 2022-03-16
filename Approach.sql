/* Approach

We can start by analyzing the purchasing behavior of customers through a cohort analysis, which can be done based on the raw tables using any SQL engine.

For this, the final result must be a table where each row n represents a group of customers with a common starting date and each column m contains the difference in periods between the starting 
date and the respective column. Each cell represents the amount/percentage of users/dollars that started in period n and are still present in the period m. 

In this file, I will go through the process of creating the cohort analysis and link the respective query for that step immediately below. Keep in mind that the actual
SQL code you would run would have all the specific steps together, and I prefer writing each step as a subquery. You can see this file in the CohortAnalysis folder.

The first thing we can do in SQL is calculate the first_purchase for each user. In this case, I have used a function from bigquery called ARRAY_AGG. It is important to mention that
the choice of period for this analysis is critical. If we choose a very short period, such as days, the values will fluctuate a lot and will be very small compared to the initial cohort size. If
on the other hand we use years as the periods, the timeframe might not be long enough to get meaningful insights. In this analysis we will use months as period.

Using ARRAY_AGG we can extract the row representing the first purchase for each user. This will return the first row in chronological order for each user:

*/ 

WITH first_purchase AS 
( 
SELECT 
      ARRAY_AGG(rs ORDER BY InvoiceDate ASC LIMIT 1)[OFFSET(0)] AS a 
FROM  
    `cohortanalysis-343816.RetailSales.retailsales` AS rs 
GROUP BY  
    CustomerID 
) 
 
/* 
From these rows, we can get the date and customerId of that first purchase in order to user them later on.
*/
 
, first_cohort AS 
( 
SELECT  
    a.InvoiceDate, a.CustomerID 
FROM  
    first_purchase  
), 

/*

An important caveat is that we need to make sure that for every cohort, every period n inside the timeframe analyzed is displayed. If the cohort is very small or the amount of time analyzed is very large, 
there might be cases where a period n has no purchases. This would mean that when generating the final table, there would be no cell for period n for that specific cohort.
Therefore, what we need to do is generate an array of all possible dates between the first invoice date and the last date we want to analyze, which in this case is the date of
the last invoice. This will ensure that for every cohort, we have every period available, even if the value (for amount sold or for customers retained) is 0.

*/
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
), 

/*
Now that we have all possible cohort dates, we can use this as a basis for starting to generate our final output. We take this list of all possible dates and join the original 
table with all invoices to it. Finally, we also join the date of the first invoice for that customer which we calculated before. The output would therefore be a table where
every invoice has an additional column called initial cohort, which has the period (month) where that customer first purchased, and another column called period_diff, which is the difference
in periods (months) between that invoice and the date of the first purchase. Additionally, since we joined this information to the list of all possible dates mentioned above, any month
that did not have any invoice available will still be present.

*/

cohorted_purchases AS 
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

/*
Lastly, from this "augmented" table of invoices above we can now group by the initial cohort and the period difference, such that we have an aggregate of all distinct customers
and total amount purchased for every combination of initial cohort (first month of purchase) and period difference (months after that initial purchase). The output of this
will be a table with four columns: The date (month) of the initial cohort, the period difference since that date, and the amount of distinct customers and total purchases for
that combination of cohort and period difference.

*/

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

/*
As a last step before pivoting, we take this input from above and we join it with the values for the first period for every cohort. This way, we can use the values from the joined table 
in order to calculate percentages. What this means is that for any row with initial_cohort x and period difference y, we "attach" two additional columns, which are the amount
of distinct customers that purchased for that initial cohort when the period difference was 0 (in the initial month), and the total amount purchased in that first month. That way,
we can divide the values of total customers at period n and total amount purchased at period n by the value of total customers at period 0 and total amount purchased at period 0, such
that we can calculate retention.

*/

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

/*
To generate the final input, from the table above we get the initial cohort, the period difference, and the column for value of interest, which could be: Total customers retained, percentage
of customers retained, total dollars retained or percentage of dollars retained. If we just took these 3 columns we would still have a table in long format (every row has a value
for initial cohort, period difference and the value of interest). Since we need a table in wide format, we must pivot the values. This allows us to have a table where the rows are the 
initial cohort, the columns are the period difference, and the value of each cell is the value of interest.

*/

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
    
/*
If we need to change the value of interest, we just need to change the third column we extract from the select statement above

*/

