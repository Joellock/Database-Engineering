import pandas as pd
import pyodbc
import numpy as np
import warnings

warnings.filterwarnings("ignore", category=UserWarning)

server = r'LAPTOP-UATOMV5O\SQLEXPRESS'
database = 'dwh'
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

try:
    conn = pyodbc.connect(conn_str)
    
    # Extract
    cust_df = pd.read_sql("SELECT * FROM transformation.cust_info_cleaned", conn)
    az12_df = pd.read_sql("SELECT * FROM transformation.cst_az12_info_cleaned", conn)
    loc_df = pd.read_sql("SELECT * FROM transformation.loc_info_cleaned", conn)

    print("✅ Transformation tables loaded.")

    # Merge Customer with AZ12
    df = pd.merge(cust_df, az12_df, left_on="cst_id", right_on="cid", how="left")
    
    # Merge result with Location (adding suffixes to avoid 'cid' conflicts)
    df = pd.merge(df, loc_df, left_on="cst_id", right_on="cid", how="left", suffixes=('', '_loc'))

    # Build the Dimension DataFrame
    dim_customers = pd.DataFrame({
        "customer_id": df["cst_id"],
        "customer_number": df["cst_key"],
        "first_name": df["cst_firstname"],
        "last_name": df["cst_lastname"],
        "country": df["country"],
        "marital_status": df["cst_marital_status"],
        "gender": df["cst_gndr"],       
        "birthdate": df["gender_loc"] if "gender_loc" in df.columns else df["gender"], # Logic check
        "create_date": df["cst_create_date"]
    })
    
    # Note: If 'bdate' was the column name in AZ12, use df["bdate"] for birthdate instead
    if "bdate" in df.columns:
        dim_customers["birthdate"] = df["bdate"]

    # Add Surrogate Key
    dim_customers = dim_customers.sort_values("customer_id").reset_index(drop=True)
    dim_customers.insert(0, "customer_key", dim_customers.index + 1)

    # Load
    cursor = conn.cursor()
    cursor.execute("IF OBJECT_ID('curated.dim_customers', 'U') IS NOT NULL DROP TABLE curated.dim_customers")
    cursor.execute("""
        CREATE TABLE curated.dim_customers (
            customer_key INT, 
            customer_id INT, 
            customer_number VARCHAR(100),
            first_name VARCHAR(255), 
            last_name VARCHAR(255), 
            country VARCHAR(100),
            marital_status VARCHAR(50), 
            gender VARCHAR(50), 
            birthdate DATE, 
            create_date DATE
        )
    """)
    
    cursor.fast_executemany = True
    data_to_insert = [tuple(x) for x in dim_customers.replace({np.nan: None}).to_numpy()]
    cursor.executemany("INSERT INTO curated.dim_customers VALUES (?,?,?,?,?,?,?,?,?,?)", data_to_insert)
    
    conn.commit()
    print("✅ Success! curated.dim_customers loaded.")

except Exception as e:
    print(f"❌ Error: {e}")
finally:
    if 'conn' in locals():
        conn.close()