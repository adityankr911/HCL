import argparse
from src.utils.db import get_engine, to_sql, read_sql
import pandas as pd

def build_dims(engine):
    cust = read_sql("SELECT * FROM stg_customers", engine)
    addr = cust[['Country','Region','State','City','Postal_Code']].drop_duplicates().reset_index(drop=True)
    addr['Address_Id'] = addr.index + 1
    to_sql(addr, "dim_address", engine)

    reg = addr[['Region']].drop_duplicates().reset_index(drop=True)
    reg['Region_Id'] = reg.index + 1
    to_sql(reg, "dim_region", engine)
    print("dim_address and dim_region built.")

def prepare_fact_stage(engine):
    txn = read_sql("SELECT * FROM stg_transactions", engine)
    to_sql(txn, "stg_transactions_prepared", engine)
    print("Prepared transactions for fact loading.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    args = parser.parse_args()
    engine = get_engine(args.db)
    build_dims(engine)
    prepare_fact_stage(engine)
