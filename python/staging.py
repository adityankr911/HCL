"""Stage raw data: normalize column names, dates, drop duplicates"""
import argparse
from src.utils.db import get_engine, to_sql, read_sql
from src.utils.dates import parse_date

def stage(engine):
    cust = read_sql("SELECT * FROM stg_customers_raw", engine)
    cust['DOB'] = parse_date(cust['DOB'])
    cust = cust.drop_duplicates()
    to_sql(cust, "stg_customers", engine)

    policy = read_sql("SELECT * FROM stg_policy_raw", engine)
    policy['Policy_Start_Dt'] = parse_date(policy['Policy_Start_Dt'])
    policy['Policy_End_Dt'] = parse_date(policy['Policy_End_Dt'])
    policy = policy.drop_duplicates()
    to_sql(policy, "stg_policy", engine)

    txn = read_sql("SELECT * FROM stg_transactions_raw", engine)
    txn['Policy_Start_Dt'] = parse_date(txn['Policy_Start_Dt'])
    txn['Next_Premium_Dt'] = parse_date(txn['Next_Premium_Dt'])
    if 'Actual_Premium_Paid_Dt' in txn.columns:
        txn['Actual_Premium_Paid_Dt'] = parse_date(txn['Actual_Premium_Paid_Dt'])
    to_sql(txn, "stg_transactions", engine)

    print("Staging complete: stg_customers, stg_policy, stg_transactions")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    args = parser.parse_args()
    engine = get_engine(args.db)
    stage(engine)
