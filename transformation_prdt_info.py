import pandas as pd
import pyodbc
import numpy as np

server = r'LAPTOP-UATOMV5O\SQLEXPRESS'
database = 'dwh'
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Create the 'transformation' schema if it doesn't exist
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'transformation')
        BEGIN
            EXEC('CREATE SCHEMA transformation')
        END
    """)
    conn.commit()

    # --- EXTRACT ---
    query = "SELECT * FROM schema1.prd_info"
    df = pd.read_sql(query, conn)
    print(f"✅ Data extracted: {len(df)} rows found.")

    # --- TRANSFORM ---
    
    # 1. Handle cat_id and update prd_key
    # Create cat_id: first 5 chars with underscore (e.g., CO-RF -> CO_RF)
    df['cat_id'] = df['prd_key'].str[:5].str.replace('-', '_')
    
    # Update prd_key: Remove first 6 characters (the first 5 + the hyphen)
    # Example: CO-RF-FR-R92B-58 becomes FR-R92B-58
    df['prd_key'] = df['prd_key'].str[6:]

    # 2. prd_cost: change null to 0
    df['prd_cost'] = pd.to_numeric(df['prd_cost'], errors='coerce').fillna(0)

    # 3. prd_line: Map codes and handle nulls
    line_map = {'M': 'Mountain', 'S': 'Sport', 'R': 'Road', 'T': 'Touring'}
    df['prd_line'] = df['prd_line'].str.strip().map(line_map).fillna('Other')

    # 4. Date Logic (t-1 of the following record)
    df['prd_start_dt'] = pd.to_datetime(df['prd_start_dt'])
    df['prd_end_dt'] = pd.to_datetime(df['prd_end_dt'], errors='coerce')



    # Calculate the fixed end date (next start date minus 1 day) within each product group
    # Using groupby('prd_key') ensures we don't accidentally use the date of a different product
    df['prd_end_dt_fixed'] = df.groupby('prd_nm')['prd_start_dt'].shift(-1) - pd.Timedelta(days=1)

    # Logic: If a "next" record exists, use that date. 
    # If not (the product is current), keep the original end date (which is Null)
    df['prd_end_dt'] = df['prd_end_dt_fixed'].fillna(df['prd_end_dt'])

    # Format dates back to string for SQL
    df['prd_start_dt'] = df['prd_start_dt'].dt.strftime('%Y-%m-%d')
    df['prd_end_dt'] = df['prd_end_dt'].dt.strftime('%Y-%m-%d')

    # --- LOAD ---
    
    # Select columns in specific order for SQL insertion
    cols = ['prd_id', 'prd_key', 'prd_nm', 'prd_cost', 'prd_line', 'prd_start_dt', 'prd_end_dt', 'cat_id']
    df_to_upload = df[cols].copy()
    
    # Ensure Numeric types and handle NaNs for SQL
    df_to_upload['prd_id'] = pd.to_numeric(df_to_upload['prd_id'], errors='coerce').fillna(0).astype(int)
    df_to_upload = df_to_upload.replace({np.nan: None})

    cursor.execute("""
        IF OBJECT_ID('transformation.prd_info_cleaned', 'U') IS NOT NULL
            DROP TABLE transformation.prd_info_cleaned;
        
        CREATE TABLE transformation.prd_info_cleaned (
            prd_id INT,
            prd_key VARCHAR(100),
            prd_nm VARCHAR(255),
            prd_cost FLOAT,
            prd_line VARCHAR(50),
            prd_start_dt DATE,
            prd_end_dt DATE,
            cat_id VARCHAR(50)
        );
    """)

    insert_sql = "INSERT INTO transformation.prd_info_cleaned VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    data_to_insert = [tuple(x) for x in df_to_upload.to_numpy()]
    
    cursor.fast_executemany = True 
    cursor.executemany(insert_sql, data_to_insert)

    conn.commit()
    print(f"🚀 Success! {len(df_to_upload)} cleaned rows saved to 'transformation.prd_info_cleaned'.")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if 'conn' in locals(): 
        conn.close()
        print("🔌 Connection closed.")