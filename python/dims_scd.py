import argparse
import pandas as pd
from src.utils.db import get_engine, to_sql, read_sql


def upsert_dim_customer(engine):
    """
    Upsert customers into dim_customer using a simple SCD2 approach:
      - Keep historical rows with valid_from / valid_to and is_current flag.
      - If customer doesn't exist -> insert as current.
      - If attributes changed -> expire old row and insert new current row.
    """
    stg = read_sql("SELECT * FROM stg_customers", engine)

    # Ensure staging has expected column names (defensive)
    expected_cols = [
        'Customer_Id', 'Customer_Name', 'Customer_Segment', 'Maritial_Status', 'Gender',
        'DOB', 'Country', 'Region', 'State', 'City', 'Postal_Code'
    ]
    for c in expected_cols:
        if c not in stg.columns:
            stg[c] = pd.NA

    # Try reading existing dim_customer
    try:
        cur = read_sql("SELECT * FROM dim_customer", engine)
    except Exception:
        # Create empty dataframe with expected columns
        cur = pd.DataFrame(columns=[
            'Customer_Id', 'Customer_Name', 'Customer_Segment', 'Maritial_Status', 'Gender',
            'DOB', 'Country', 'Region', 'State', 'City', 'Postal_Code',
            'valid_from', 'valid_to', 'is_current'
        ])

    out = cur.copy()
    now = pd.Timestamp.now()

    # Iterate staging rows and perform SCD logic
    for _, row in stg.iterrows():
        cid = row['Customer_Id']

        # find current row for this customer if exists
        existing = out[(out['Customer_Id'] == cid) & (out.get('is_current') == 1)]

        # Case 1: No existing (new customer) -> insert new current row
        if existing.empty:
            new = row.to_dict()
            new.update({'valid_from': now, 'valid_to': pd.NaT, 'is_current': 1})
            out = pd.concat([out, pd.DataFrame([new])], ignore_index=True)
            continue

        # Case 2: Exists -> compare attributes to detect change
        attrs = [
            'Customer_Name', 'Customer_Segment', 'Maritial_Status', 'Gender', 'DOB',
            'Country', 'Region', 'State', 'City', 'Postal_Code'
        ]

        # normalize existing values for comparison (take first current row)
        ex_row = existing.iloc[0]
        changed = False
        for a in attrs:
            # Treat NaN/NaT carefully
            ex_val = ex_row.get(a)
            stg_val = row.get(a)
            # Normalize to string for safe comparison (None/NaN -> 'nan' ; so use pd.isna)
            if pd.isna(ex_val) and pd.isna(stg_val):
                continue
            if str(ex_val) != str(stg_val):
                changed = True
                break

        if changed:
            # expire old current row(s)
            idx = out[(out['Customer_Id'] == cid) & (out.get('is_current') == 1)].index
            out.loc[idx, 'is_current'] = 0
            out.loc[idx, 'valid_to'] = now

            # insert new current record
            new = row.to_dict()
            new.update({'valid_from': now, 'valid_to': pd.NaT, 'is_current': 1})
            out = pd.concat([out, pd.DataFrame([new])], ignore_index=True)
        # else: no change -> nothing to do

    # Write dim_customer back to DB (replace)
    to_sql(out, "dim_customer", engine)
    print("dim_customer upserted. Rows:", len(out))


def build_policy_dims(engine):
    """
    Build dim_policy and dim_policy_type from staging policy table.
    """
    p = read_sql("SELECT * FROM stg_policy", engine)

    if p.empty:
        print("No staging policy data found (stg_policy empty). Skipping policy dims.")
        return

    # dim_policy: unique policies
    p_unique = p.drop_duplicates(subset=['Policy_Id']).copy()
    to_sql(p_unique, "dim_policy", engine)
    # dim_policy_type: lookup on policy type id
    if 'Policy_Type_Id' in p.columns:
        pt = p[['Policy_Type_Id', 'Policy_Type', 'Policy_Type_Desc']].drop_duplicates(subset=['Policy_Type_Id']).copy()
        to_sql(pt, "dim_policy_type", engine)
    else:
        print("Policy_Type_Id not found in staging policy; dim_policy_type not created.")
    print("dim_policy and dim_policy_type built.")


def main(db_uri):
    engine = get_engine(db_uri)
    upsert_dim_customer(engine)
    build_policy_dims(engine)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build dims and perform SCD upsert for customers")
    parser.add_argument("--db", required=True, help="Database URI, e.g. sqlite:///data/insurance.db")
    args = parser.parse_args()
    main(args.db)
