d_# HCL_Hackathon
# Team DDEE-BOYZ
Collaborators are 
@mygit021 - Harsh Raj - 21EE02004
@adityankr911 - Aditya Roy - 21EE02005
@ramateja1234 - Bollepalli RamaTeja - 21EE02008

# Data Warehouse Schema Documentation

This document describes the dimensional model used in the insurance analytics data warehouse.  
The warehouse follows a classic **star schema** with multiple dimension tables,  
**three Slowly Changing Dimensions (SCD Type 2)**—customer, policy, and address—and a central fact table for policy transactions.

The design supports:

- Historical tracking of customer attributes  
- Historical tracking of policy details  
- Historical tracking of address changes  
- Region-based reporting  
- Date-based analytics  
- Detailed premium and transaction metrics  

---

# Dimension Tables

---

## 1. dim_date (Conformed Date Dimension)

**Grain:** One row per calendar date.

This table provides standardized date attributes used across the entire warehouse for time-based reporting.

**Key Attributes:**
- `date_key` (YYYYMMDD, PK)
- `date`
- `day`, `month`, `month_name`, `quarter`, `year`
- `is_weekend`
- `fiscal_year`
- `load_ts`

**Purpose:** Enables consistent filtering and aggregation by all date-related properties such as month, quarter, and fiscal year.

---

## 2. dim_region

**Grain:** Region.

Contains business regions such as EAST, WEST, NORTH, and SOUTH.  
Referenced by the address dimension and the fact table.

**Key Attributes:**
- `region_sk` (PK)
- `region_code`
- `region_name`
- `country` (default: United States)
- `load_ts`

**Purpose:** Provides a standard reference for region-based reporting.

---

## 3. dim_address_scd (SCD Type 2)

**Grain:** Address record at a specific point in time.

This dimension maintains **historical versions** of customer addresses.  
Whenever a customer's address changes, a new row is created.

**Key Attributes:**
- `address_sk` (PK surrogate)
- `address_id` (natural key from source)
- `country`, `region`, `state`, `city`, `postal_code`
- `full_address`
- `region_sk` (FK → dim_region)
- `effective_from` (start of this version)
- `effective_to` (end of this version; NULL if current)
- `is_current` (TRUE for active address)
- `row_hash` (used for change detection)
- `load_ts`

**Purpose:** Supports accurate historical address tracking and point-in-time reporting for customer attributes and transactions.

---

## 4. dim_policy_type

**Grain:** Policy type (e.g., Auto, Home, Life, Health).

A static lookup dimension containing the list of valid policy categories.

**Key Attributes:**
- `policy_type_sk` (PK)
- `policy_type_code`
- `policy_type_name`
- `description`
- `load_ts`

**Purpose:** Standard classification of policies used for reporting and segmentation.

---

## 5. dim_customer_scd (SCD Type 2)

**Grain:** Customer at a specific point in time.

Tracks historical changes in customer attributes such as segment, marital status, and **address**.  
When any tracked attribute changes, a new record is inserted and older records are marked inactive.

**Key Attributes:**
- `customer_sk` (PK surrogate)
- `customer_id` (business key)
- `first_name`, `last_name`, `gender`, `date_of_birth`
- `segment`
- `marital_status`
- `address_sk` (FK → dim_address_scd)
- `effective_from`, `effective_to`, `is_current`
- `row_hash`
- `load_ts`, `source_region`, `source_file`

**Purpose:** Preserves full customer history for accurate point-in-time reporting and analytics.

---

## 6. dim_policy_scd (SCD Type 2)

**Grain:** Policy at a specific point in time.

Tracks historical changes in policy details such as policy type, term, status, and premium amount.  
A new row is added when tracked attributes change.

**Key Attributes:**
- `policy_sk` (PK)
- `policy_id`
- `policy_name`
- `policy_type_sk` (FK → dim_policy_type)
- `term`
- `start_date`, `completion_date`
- `status`
- `customer_sk` (FK → dim_customer_scd)
- `premium_amount`
- `effective_from`, `effective_to`, `is_current`
- `row_hash`
- `load_ts`, `source_region`, `source_file`

**Purpose:** Supports historical reconstruction of policy states and accurate reporting at any point in the policy lifecycle.

---

# Fact Table

---

## fact_policy_txn

**Grain:** One row per payment or financial transaction (e.g., premium installment).

This table captures all monetary activities associated with policies, including premiums, late fees, and payment status.

**Key Relationships:**
- `policy_sk` → dim_policy_scd (policy version valid at transaction date)
- `customer_sk` → dim_customer_scd (customer version valid at transaction date)
- `date_key` → dim_date
- `region_sk` → dim_region
- `policy_type_sk` → dim_policy_type

**Key Measures & Attributes:**
- `transaction_amount`
- `premium_amount`
- `premium_paid`
- `late_fee`
- `days_late`
- `payment_status`
- `payment_method`
- `installment_number`
- `is_reversal`
- `load_ts`, `source_region`, `source_file`

### Point-in-Time Dimension Linking Rule

To ensure the fact table references the correct SCD2 version:

```sql
date_key BETWEEN effective_from AND COALESCE(effective_to, '9999-12-31')


![System Architecture](erd.png)

