
d_# HCL_Hackathon
# Team DDEE-BOYZ
Collaborators are 
@mygit021 - Harsh Raj - 21EE02004
@adityankr911 - Aditya Roy - 21EE02005
@ramateja1234 - Bollepalli RamaTeja - 21EE02008


# Data Warehouse Schema Documentation

This document describes the dimensional model designed for an insurance analytics data warehouse. The schema follows a star-schema architecture with conformed dimensions, surrogate keys, and Slowly Changing Dimensions (SCD) to support historical reporting.

---

# Overview

The warehouse contains:

- Multiple dimension tables describing customers, policies, dates, regions, and addresses.
- Two SCD Type 2 dimensions that maintain historical attribute changes.
- A central fact table storing policy transaction–level information.
- Conformed dimensions ensuring consistent reporting across time, geography, and customer segments.

The design supports premium analytics, delinquency tracking, customer segmentation, and policy lifecycle reporting.

---

# Dimension Tables

---

## 1. dim_date  
**Grain:** One row per calendar date.

A conformed date dimension used across the warehouse for all time-based analyses.

**Key Attributes:**
- `date_key` (YYYYMMDD, PK)  
- `date`  
- `day`, `month`, `month_name`, `quarter`, `year`  
- `is_weekend`  
- `fiscal_year`  
- `load_ts`

**Purpose:** Enables reporting by date, month, quarter, year, and fiscal periods.

---

## 2. dim_region  
**Grain:** One row per business region.

**Key Attributes:**
- `region_sk` (PK)  
- `region_code` (EAST, WEST, NORTH, SOUTH)  
- `region_name`  
- `country` (default: United States)  
- `load_ts`

**Purpose:** Standardized lookup for region-based reporting.

---

## 3. dim_address  
**Grain:** One row per address record (SCD1).

**Key Attributes:**
- `address_sk` (PK)  
- `address_id` (natural key from source)  
- `country`, `region`, `state`, `city`, `postal_code`  
- `full_address`  
- `region_sk` (FK → dim_region)  
- `load_ts`

**Purpose:** Provides centralized reference for customer address information.

---

## 4. dim_policy_type  
**Grain:** One row per policy category.

**Key Attributes:**
- `policy_type_sk` (PK)  
- `policy_type_code`  
- `policy_type_name`  
- `description`  
- `load_ts`

**Purpose:** Defines standard policy classifications such as Auto, Home, Life, and Health.

---

## 5. dim_customer_scd (SCD Type 2)  
**Grain:** Customer at a specific point in time.

Tracks historical changes in customer attributes.

**Key Attributes:**
- `customer_sk` (PK)  
- `customer_id` (business key)  
- `first_name`, `last_name`, `gender`, `date_of_birth`  
- `segment`, `marital_status`  
- `address_sk` (FK → dim_address)  
- `effective_from`, `effective_to`, `is_current`  
- `row_hash`  
- `load_ts`, `source_region`, `source_file`

**Purpose:** Preserves customer attribute history for accurate point-in-time reporting.

---

## 6. dim_policy_scd (SCD Type 2)  
**Grain:** Policy at a specific point in time.

Tracks historical changes in policy attributes such as policy type, term, premium, and status.

**Key Attributes:**
- `policy_sk` (PK)  
- `policy_id`  
- `policy_name`  
- `policy_type_sk` (FK → dim_policy_type)  
- `term`, `start_date`, `completion_date`  
- `status` (Active, Cancelled, Expired)  
- `customer_sk` (FK → dim_customer_scd)  
- `premium_amount`  
- `effective_from`, `effective_to`, `is_current`  
- `row_hash`  
- `load_ts`, `source_region`, `source_file`

**Purpose:** Supports reconstruction of policy history at any point in its lifecycle.

---

# Fact Table

---

## fact_policy_txn  
**Grain:** One row per policy payment or financial transaction.

Stores detailed premium-related data, late fees, payment classification, and financial metrics.

**Key Attributes:**
- `fact_txn_sk` (PK)  
- `txn_id`  
- `policy_sk` (FK → dim_policy_scd)  
- `customer_sk` (FK → dim_customer_scd)  
- `date_key` (FK → dim_date)  
- `region_sk` (FK → dim_region)  
- `policy_type_sk` (FK → dim_policy_type)  
- `installment_number`  
- `transaction_amount`, `policy_amount`, `premium_amount`  
- `premium_paid`, `late_fee`, `days_late`  
- `payment_status`, `payment_method`  
- `is_reversal`  
- `load_ts`, `source_region`, `source_file`

![System Architecture](erd.png)

