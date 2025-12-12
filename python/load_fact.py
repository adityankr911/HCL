#!/usr/bin/env python3
"""
src/etl/load_fact.py

Robustly build fact_policy_txn by joining prepared staging with dims.
Handles missing columns gracefully.
Usage:
    python -m src.etl.load_fact --db sqlite:///data/insurance.db
"""
import argparse
import pandas as pd
from src.utils.db import get_engine, to_sql, read_sql

def load_fact(engine):
    # Read prepared staging transactions
    try:
        txn = read_sql("SELECT * FROM stg_transactions_prepared", engine)
    except Exception:
        print("stg_transactions_prepared not found or empty. Please run transform.py first.")
        txn = pd.DataFrame()

    # Defensive: ensure txn exists
    if txn.empty:
        print("No transactions to load into fact. Exiting load_fact.")
        return

    # Read dims if available
    try:
        policy = read_sql("SELECT * FROM dim_policy", engine)
    except Exception:
        policy = pd.DataFrame()

    try:
        cust_dim = read_sql("SELECT * FROM dim_customer WHERE is_current=1", engine)
    except Exception:
        cust_dim = pd.DataFrame()

    try:
        addr = read_sql("SELECT * FROM dim_address", engine)
    except Exception:
        addr = pd.DataFrame()

    # Merge policy info onto txn (on Policy_Id when present)
    if not policy.empty and 'Policy_Id' in txn.columns and 'Policy_Id' in policy.columns:
        df = txn.merge(policy, on='Policy_Id', how='left', suffixes=('','_policy'))
    else:
        df = txn.copy()

    # If customer dim exists, merge current customer attributes (region/country etc.)
    if not cust_dim.empty and 'Customer_Id' in df.columns and 'Customer_Id' in cust_dim.columns:
        # pick only the descriptive fields we need from cust_dim
        cust_fields = [c for c in ['Customer_Id','Customer_Name','Country','Region','State','City','Postal_Code'] if c in cust_dim.columns]
        df = df.merge(cust_dim[cust_fields], on='Customer_Id', how='left', suffixes=('','_cust'))
    else:
        # no customer dim available; continue with txn columns
        pass

    # If address dim exists and txn (or merged df) has all address cols, attempt merge to get Address_Id
    addr_cols = ['Country','Region','State','City','Postal_Code']
    # check which addr cols exist in df
    existing_addr_cols = [c for c in addr_cols if c in df.columns]
    if not addr.empty and len(existing_addr_cols) == len(addr_cols):
        # safe to merge on full address composite
        df = df.merge(addr, on=addr_cols, how='left', suffixes=('','_addr'))
    else:
        # Attempt to merge on Region only (if available) to bring in Region_Id
        if not addr.empty and 'Region' in df.columns and 'Region' in addr.columns:
            if 'Region_Id' in addr.columns:
                # keep unique region->Region_Id mapping
                reg_map = addr[['Region','Region_Id']].drop_duplicates()
                df = df.merge(reg_map, on='Region', how='left')
        # else - skip address merge

    # Build fact table with the measures we expect (use columns that exist)
    fact_cols = []
    # Required natural keys if present
    for c in ['Customer_Id','Policy_Id']:
        if c in df.columns:
            fact_cols.append(c)

    # Common event/date columns
    for c in ['Policy_Term','Policy_Start_Dt','Next_Premium_Dt','Actual_Premium_Paid_Dt']:
        if c in df.columns:
            fact_cols.append(c)

    # Measures
    for c in ['Premium_Amt','Premium_Amt_Paid_TillDate','Total_Policy_Amt']:
        if c in df.columns:
            fact_cols.append(c)

    # Region (useful for query filters)
    if 'Region' in df.columns:
        fact_cols.append('Region')

    # If none of the expected columns exist, include everything (fallback)
    if len(fact_cols) == 0:
        print("Warning: none of the expected fact columns found; writing all columns as fact.")
        fact = df.copy()
    else:
        # keep unique set preserving order
        seen = set()
        fact_cols_clean = []
        for x in fact_cols:
            if x not in seen:
                fact_cols_clean.append(x); seen.add(x)
        fact = df[fact_cols_clean].copy()

    # Optional: ensure correct dtypes (attempt conversions)
    for dcol in ['Policy_Start_Dt','Next_Premium_Dt','Actual_Premium_Paid_Dt']:
        if dcol in fact.columns:
            fact[dcol] = pd.to_datetime(fact[dcol], errors='coerce', infer_datetime_format=True)

    # Write fact table
    to_sql(fact, "fact_policy_txn", engine)
    print(f"Loaded fact_policy_txn with {len(fact)} rows and columns: {list(fact.columns)}")

def main(db_uri):
    engine = get_engine(db_uri)
    load_fact(engine)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load fact_policy_txn")
    parser.add_argument("--db", required=True, help="Database URI, e.g. sqlite:///data/insurance.db")
    args = parser.parse_args()
    main(args.db)
