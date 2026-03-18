import pandas as pd
import pyodbc
import numpy as np
import warnings

# Ignore the SQLAlchemy warning to keep the terminal clean
warnings.filterwarnings("ignore", category=UserWarning)

# 1. Connection settings
server = r'LAPTOP-UATOMV5O\SQLEXPRESS'
database = 'dwh'
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Create transformation schema if it doesn't exist
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'transformation')
        BEGIN
            EXEC('CREATE SCHEMA transformation')
        END
    """)
    conn.commit()

    # --- EXTRACT ---
    query = "SELECT * FROM Ingestion.sales_details"
    df = pd.read_sql(query, conn)
    print(f"✅ Data extracted: {len(df)} rows found.")

    # --- TRANSFORM ---
    
    # Standardize Customer IDs: Last 5 digits as Integer (matches your other tables)
    df['sls_cust_id'] = df['sls_cust_id'].astype(str).str.strip().str[-5:]
    df['sls_cust_id'] = pd.to_numeric(df['sls_cust_id'], errors='coerce').fillna(0).astype(int)

    # Convert Ship and Due dates to datetime for logic processing
    df["sls_ship_dt"] = pd.to_datetime(df["sls_ship_dt"].astype(str), format="%Y%m%d", errors="coerce")
    df["sls_due_dt"] = pd.to_datetime(df["sls_due_dt"].astype(str), format="%Y%m%d", errors="coerce")

    # Fix sls_order_dt using grouping logic
    def fix_order_dt(group):
        # A valid date in your source is exactly 8 digits
        s_dt = group["sls_order_dt"].astype(str).str.strip()
        valid_mask = s_dt.str.len() == 8
        valid_vals = group.loc[valid_mask, "sls_order_dt"]
        
        if valid_vals.empty:
            # Fallback: Ship date minus 1 day
            fixed = group["sls_ship_dt"] - pd.Timedelta(days=1)
            group["sls_order_dt"] = fixed
        else:
            # FIX: Use str() on the scalar max value instead of .astype(str)
            max_val = valid_vals.max()
            max_valid = pd.to_datetime(str(max_val), format="%Y%m%d", errors='coerce')
            group.loc[~valid_mask, "sls_order_dt"] = max_valid
        return group

    # Apply the date fixing logic per Order Number
    df = df.groupby("sls_ord_num", group_keys=False).apply(fix_order_dt, include_groups=False)

    # Ensure sls_order_dt is datetime format
    df["sls_order_dt"] = pd.to_datetime(df["sls_order_dt"], errors="coerce")

    # Financial Logic: Fix Sales and Price inconsistencies
    # Convert columns to numeric first to avoid errors
    df["sls_sales"] = pd.to_numeric(df["sls_sales"], errors='coerce')
    df["sls_price"] = pd.to_numeric(df["sls_price"], errors='coerce')
    df["sls_quantity"] = pd.to_numeric(df["sls_quantity"], errors='coerce')

    # 1. Both 0 or less -> treat as Null
    df.loc[(df["sls_sales"] <= 0) & (df["sls_price"] <= 0), ["sls_sales", "sls_price"]] = np.nan

    # 2. If Price is invalid but Sales is valid -> Recalculate Price (Sales / Quantity)
    invalid_price = df["sls_price"].isna() | (df["sls_price"] <= 0)
    valid_sales = df["sls_sales"].notna() & (df["sls_sales"] > 0)
    df.loc[invalid_price & valid_sales, "sls_price"] = df["sls_sales"] / df["sls_quantity"]

    # 3. If Sales is invalid but Price is valid -> Recalculate Sales (Price * Quantity)
    invalid_sales = df["sls_sales"].isna() | (df["sls_sales"] <= 0)
    valid_price = df["sls_price"].notna() & (df["sls_price"] > 0)
    df.loc[invalid_sales & valid_price, "sls_sales"] = df["sls_price"] * df["sls_quantity"]

    # 4. If Sales equals Price but Quantity > 1 -> Recalculate Sales
    wrong_sales = (df["sls_sales"] == df["sls_price"]) & (df["sls_quantity"] > 1)
    df.loc[wrong_sales, "sls_sales"] = df["sls_price"] * df["sls_quantity"]

    # Final Date Formatting for SQL Server insertion (YYYY-MM-DD)
    for col in ["sls_order_dt", "sls_ship_dt", "sls_due_dt"]:
        df[col] = df[col].dt.strftime('%Y-%m-%d')

    # --- LOAD ---
    cursor.execute("""
        IF OBJECT_ID('transformation.sales_details_cleaned', 'U') IS NOT NULL
            DROP TABLE transformation.sales_details_cleaned;
        
        CREATE TABLE transformation.sales_details_cleaned (
            sls_ord_num VARCHAR(50),
            sls_prd_key VARCHAR(50),
            sls_cust_id INT,
            sls_order_dt DATE,
            sls_ship_dt DATE,
            sls_due_dt DATE,
            sls_sales DECIMAL(10,2),
            sls_quantity INT,
            sls_price DECIMAL(10,2)
        );
    """)

    # High-speed upload pattern (matches your other scripts)
    insert_sql = "INSERT INTO transformation.sales_details_cleaned VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    
    # Handle NaNs for SQL and convert to list of tuples
    df_to_upload = df.replace({np.nan: None})
    data_to_insert = [tuple(x) for x in df_to_upload.to_numpy()]
    
    cursor.fast_executemany = True 
    cursor.executemany(insert_sql, data_to_insert)

    conn.commit()
    print(f"🚀 Success! {len(df_to_upload)} cleaned rows saved to 'transformation.sales_details_cleaned'.")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if 'conn' in locals(): 
        conn.close()
        print("🔌 Connection closed.")