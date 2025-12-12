import argparse, os
from src.utils.db import get_engine, to_sql
from src.utils.io import read_csv

def ingest_examples(ex_dir, engine):
    cust = read_csv(os.path.join(ex_dir, "sample_customer.csv"))
    policy = read_csv(os.path.join(ex_dir, "sample_policy.csv"))
    txn = read_csv(os.path.join(ex_dir, "sample_transactions.csv"))

    cust.columns = [c.strip() for c in cust.columns]
    policy.columns = [c.strip() for c in policy.columns]
    txn.columns = [c.strip() for c in txn.columns]

    to_sql(cust, "stg_customers_raw", engine)
    to_sql(policy, "stg_policy_raw", engine)
    to_sql(txn, "stg_transactions_raw", engine)
    print("Ingested sample files into staging raw tables.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--examples", required=True)
    parser.add_argument("--db", required=True)
    args = parser.parse_args()

    engine = get_engine(args.db)
    ingest_examples(args.examples, engine)
