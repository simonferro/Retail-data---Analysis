/*
 This document showcases all the possible variations for the last query used for pivotting the values into the final format. 
 The complete query can be accessed in the document "MainQuery", which contains all the code used to generate the cohorts, except for this final query which varies depending
 on which value is to be showcased in the cohort analysis.
*/



/*
  Showing customer retention as percentage
*/ 
......
Rest of query above
.......
SELECT
    * 
FROM -- without this inner select the pivot function also gets the other columns from the "values_pre_pivot" CTE and creates an additional row for each different combination
of values
    (SELECT 
        initial_cohort,
        period_diff,   
        customer_retention
    FROM 
        values_pre_pivot)
PIVOT
    (SUM(customer_retention) FOR period_diff IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13))  AS period
    

/*
  Showing dollar retention as percentage
*/ 
......
Rest of query above
.......
SELECT
    * 
FROM -- without this inner select the pivot function also gets the other columns from the "values_pre_pivot" CTE and creates an additional row for each different combination
of values
    (SELECT 
        initial_cohort,
        period_diff,   
        purchase_retention
    FROM 
        values_pre_pivot)
PIVOT
    (SUM(purchase_retention) FOR period_diff IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13))  AS period
    
    
 /*
  Showing total customer retention
*/ 
......
Rest of query above
.......
SELECT
    * 
FROM -- without this inner select the pivot function also gets the other columns from the "values_pre_pivot" CTE and creates an additional row for each different combination
of values
    (SELECT 
        initial_cohort,
        period_diff,   
        vpp.distinct_customers
    FROM 
        values_pre_pivot)
PIVOT
    (SUM(vpp.distinct_customers) FOR period_diff IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13))  AS period
    
    
 /*
  Showing total dollar retention
*/ 
......
Rest of query above
.......
SELECT
    * 
FROM -- without this inner select the pivot function also gets the other columns from the "values_pre_pivot" CTE and creates an additional row for each different combination
of values
    (SELECT 
        initial_cohort,
        period_diff,   
        vpp.total_purchases
    FROM 
        values_pre_pivot)
PIVOT
    (SUM(vpp.total_purchases) FOR period_diff IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13))  AS period
    
    

