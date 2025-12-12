-- Create database
CREATE DATABASE InsuranceDB;
USE InsuranceDB;
-- DROP table master;
-- Create master table
CREATE TABLE master (
    Customer_ID               INT ,
    Customer_Name             VARCHAR(200),
    Customer_Segment          VARCHAR(100),
    Maritial_Status           VARCHAR(50),
    Gender                    VARCHAR(20),
    DOB                       DATE,
    Effective_Start_Dt        DATE,
    Effective_End_Dt          DATE,

    Policy_Type_Id            INT,
    Policy_Type               VARCHAR(100),
    Policy_Type_Desc          VARCHAR(300),
    
    Policy_Id                 VARCHAR(100),
    Policy_Name               VARCHAR(200),
    Premium_Amt               DECIMAL(15,2),
    Policy_Term               VARCHAR(100),
    Policy_Start_Dt           DATE,
    Policy_End_Dt             DATE,
    Next_Premium_Dt           DATE,
    Actual_Premium_Paid_Dt    DATE,
    
    Country                   VARCHAR(100),
    Region                    VARCHAR(100),
    State_Province            VARCHAR(100),
    City                      VARCHAR(100),
    Postal_Code               VARCHAR(20),
    
    Total_Policy_Amt          DECIMAL(18,2),
    Premium_Amt_Paid_TillDate DECIMAL(18,2)
);

LOAD DATA INFILE '/var/lib/mysql-files/Central_day0.csv'
INTO TABLE master
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
    Customer_ID,
    Customer_Name,
    Customer_Segment,
    Maritial_Status,
    Gender,
    DOB,
    Effective_Start_Dt,
    Effective_End_Dt,
    Policy_Type_Id,
    Policy_Type,
    Policy_Type_Desc,
    Policy_Id,
    Policy_Name,
    Premium_Amt,
    Policy_Term,
    Policy_Start_Dt,
    Policy_End_Dt,
    Next_Premium_Dt,
    Actual_Premium_Paid_Dt,
    Country,
    Region,
    State_Province,
    City,
    Postal_Code,
    Total_Policy_Amt,
    Premium_Amt_Paid_TillDate
);
