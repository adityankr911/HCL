"""Streamlit app for Insurance DWH MVP"""
import streamlit as st
import pandas as pd
import subprocess
from sqlalchemy import create_engine, inspect, text

st.set_page_config(page_title="Insurance DWH Demo", layout="wide")
DEFAULT_DB_URI = "sqlite:///data/insurance.db"

def get_engine(db_uri):
    try:
        engine = create_engine(db_uri, future=True)
        with engine.begin() as conn:
            conn.execute(text("select 1"))
        return engine
    except Exception as e:
        st.error(f"Cannot connect to DB: {e}")
        return None

def list_tables(engine):
    try:
        insp = inspect(engine)
        return insp.get_table_names()
    except Exception as e:
        st.error(f"Error listing tables: {e}")
        return []

def read_table(engine, table, limit=1000):
    try:
        return pd.read_sql_table(table, con=engine).head(limit)
    except Exception as e:
        st.error(f"Error reading table {table}: {e}")
        return pd.DataFrame()

def run_sql(engine, sql):
    try:
        return pd.read_sql_query(sql, con=engine)
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()

def run_etl_pipeline():
    cmds = [
        ["python", "src/etl/ingestion.py", "--examples", "examples", "--db", "sqlite:///data/insurance.db"],
        ["python", "src/etl/staging.py", "--db", "sqlite:///data/insurance.db"],
        ["python", "src/etl/dims_scd.py", "--db", "sqlite:///data/insurance.db"],
        ["python", "src/etl/transform.py", "--db", "sqlite:///data/insurance.db"],
        ["python", "src/etl/load_fact.py", "--db", "sqlite:///data/insurance.db"]
    ]
    for c in cmds:
        st.info(f"Running: {' '.join(c)}")
        proc = subprocess.run(c, capture_output=True, text=True)
        if proc.returncode != 0:
            st.error(f"Step failed: {' '.join(c)}\n\nstdout:\n{proc.stdout}\n\nstderr:\n{proc.stderr}")
            return False
    st.success("ETL pipeline finished successfully.")
    return True

# Sidebar
st.sidebar.title("Settings")
db_uri = st.sidebar.text_input("Database URI", value=DEFAULT_DB_URI)
engine = get_engine(db_uri)

st.sidebar.markdown("---")
if st.sidebar.button("Run ETL pipeline (ingest -> dims -> fact)"):
    if st.sidebar.checkbox("Confirm: I want to run ETL now"):
        with st.spinner("Running ETL..."):
            run_etl_pipeline()
        engine = get_engine(db_uri)

st.sidebar.markdown("---")
st.sidebar.markdown("### Quick links")
st.sidebar.markdown("- Use **Preview** to inspect tables")
st.sidebar.markdown("- Use **Queries** to run prebuilt analytics")

st.title("Insurance DWH — Demo UI")

if engine is None:
    st.warning("Provide a valid DB URI in the left panel (default is sqlite:///data/insurance.db)")
    st.stop()

tabs = st.tabs(["Preview", "Queries (b-g)", "Custom SQL", "Download"])

with tabs[0]:
    st.header("Table preview")
    tables = list_tables(engine)
    if not tables:
        st.info("No tables found — run the ETL pipeline first (sidebar).")
    else:
        table_sel = st.selectbox("Select table to preview", tables)
        if table_sel:
            limit = st.slider("Rows to display", 10, 500, 200)
            df = read_table(engine, table_sel, limit=limit)
            st.dataframe(df)
            csv = df.to_csv(index=False)
            st.download_button("Download CSV", data=csv, file_name=f"{table_sel}.csv")

with tabs[1]:
    st.header("Prebuilt queries (b-g)")
    queries = {
        "b) Customers who changed policy type (current & previous)": """WITH ordered AS (
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
        WHERE cur.rn = 1 AND prev.Policy_Type IS NOT NULL AND cur.Policy_Type <> prev.Policy_Type;""",
        "c) Total policy amount by all customers and regions": """SELECT Customer_Id, SUM(Total_Policy_Amt) AS Total_Policy_Amt
        FROM fact_policy_txn
        GROUP BY Customer_Id;""",
        "d) Total policy amount for policy type = 'Auto'": """SELECT SUM(f.Total_Policy_Amt) AS total_auto
        FROM fact_policy_txn f
        LEFT JOIN dim_policy p ON f.Policy_Id = p.Policy_Id
        WHERE p.Policy_Type = 'Auto';""",
        "e) Total policy amount: East+West & Quarterly in 2012": """SELECT SUM(Total_Policy_Amt) AS total_east_west_quarterly_2012
        FROM fact_policy_txn
        WHERE Region IN ('East','West')
          AND Policy_Term = 'Quarterly'
          AND substr(Policy_Start_Dt,1,4) = '2012';""",
        "f) Customers whose marital status changed": """SELECT Customer_Id
        FROM dim_customer
        GROUP BY Customer_Id
        HAVING COUNT(DISTINCT Maritial_Status) > 1;""",
        "g) Display all regions' customer + policy + address + policy details": """SELECT f.*, p.Policy_Name, p.Policy_Type, c.Customer_Name, c.Country, c.State, c.City, c.Postal_Code
        FROM fact_policy_txn f
        LEFT JOIN dim_policy p ON f.Policy_Id = p.Policy_Id
        LEFT JOIN dim_customer c ON f.Customer_Id = c.Customer_Id;"""}
    sel = st.selectbox("Choose a prebuilt query", list(queries.keys()))
    if sel:
        sql = queries[sel]
        st.code(sql, language="sql")
        if st.button("Run selected query"):
            with st.spinner("Running query..."):
                df = run_sql(engine, sql)
                st.dataframe(df)
                st.download_button("Download result CSV", data=df.to_csv(index=False), file_name="query_result.csv")

with tabs[2]:
    st.header("Custom SQL")
    custom_sql = st.text_area("SQL", height=200)
    if st.button("Run SQL"):
        if custom_sql.strip() == "":
            st.warning("Enter a SQL statement")
        else:
            df = run_sql(engine, custom_sql)
            st.dataframe(df)
            st.download_button("Download CSV", data=df.to_csv(index=False), file_name="custom_sql.csv")

with tabs[3]:
    st.header("Export tables")
    tables = list_tables(engine)
    for t in tables:
        if st.button(f"Export {t}"):
            df = read_table(engine, t, limit=1000000)
            st.download_button(f"Download {t}.csv", data=df.to_csv(index=False), file_name=f"{t}.csv")
