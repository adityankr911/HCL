-- b) Customers who changed policy type (current & previous)
WITH ordered AS (
          SELECT f.Customer_Id, f.Policy_Id, p.Policy_Type, f.Policy_Start_Dt,
                 ROW_NUMBER() OVER (PARTITION BY f.Customer_Id ORDER BY f.Policy_Start_Dt DESC) rn
          FROM fact_policy_txn f
          LEFT JOIN dim_policy p ON f.Policy_Id = p.Policy_Id
        )
        SELECT cur.Customer_Id,
               cur.Policy_Id AS current_policy_id, cur.Policy_Type AS current_policy_type,
               prev.Policy_Id AS previous_policy_id, prev.Policy_Type AS previous_policy_type
        FROM ordered cur
        LEFT JOIN ordered prev
          ON cur.Customer_Id = prev.Customer_Id AND prev.rn = cur.rn + 1
        WHERE cur.rn = 1 AND prev.Policy_Type IS NOT NULL AND cur.Policy_Type <> prev.Policy_Type;


-- c) Total policy amount by all customers and regions
SELECT Customer_Id, SUM(Total_Policy_Amt) AS Total_Policy_Amt
        FROM fact_policy_txn
        GROUP BY Customer_Id;

--d) Total policy amount for policy type = 'Auto'
SELECT SUM(f.Total_Policy_Amt) AS total_auto
        FROM fact_policy_txn f
        LEFT JOIN dim_policy p ON f.Policy_Id = p.Policy_Id
        WHERE p.Policy_Type = 'Auto';

-- e) Total policy amount: East+West & Quarterly in 2012
SELECT SUM(Total_Policy_Amt) AS total_east_west_quarterly_2012
        FROM fact_policy_txn
        WHERE Region IN ('East','West')
          AND Policy_Term = 'Quarterly'
          AND substr(Policy_Start_Dt,1,4) = '2012';

-- f) Customers whose marital status changed
SELECT Customer_Id
        FROM dim_customer
        GROUP BY Customer_Id
        HAVING COUNT(DISTINCT Maritial_Status) > 1;

-- g) Display all regions' customer + policy + address + policy details
SELECT f.*, p.Policy_Name, p.Policy_Type, c.Customer_Name, c.Country, c.State, c.City, c.Postal_Code
        FROM fact_policy_txn f
        LEFT JOIN dim_policy p ON f.Policy_Id = p.Policy_Id
        LEFT JOIN dim_customer c ON f.Customer_Id = c.Customer_Id;




