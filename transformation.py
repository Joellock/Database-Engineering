import pandas as pd
import pyodbc
import numpy as np

server = r'LAPTOP-UATOMV5O\SQLEXPRESS'
database = 'dwh'
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Create the 'transformation' schema
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'transformation')
        BEGIN
            EXEC('CREATE SCHEMA transformation')
        END
    """)
    conn.commit()

    # --- EXTRACT ---
    query = "SELECT * FROM schema1.cust_info"
    df = pd.read_sql(query, conn)
    print(f"✅ Data extracted: {len(df)} rows found.")

    # --- TRANSFORM ---
    df['cst_firstname'] = df['cst_firstname'].astype(str).str.strip()
    df['cst_lastname'] = df['cst_lastname'].astype(str).str.strip()

    gender_map = {'M': 'Male', 'F': 'Female'}
    df['cst_gndr'] = df['cst_gndr'].map(gender_map).fillna(df['cst_gndr']).fillna('N/A')
    df['cst_marital_status'] = df['cst_marital_status'].fillna('N/A')

    df['cst_key'] = 'AW000' + df['cst_id'].astype(str)
    df['cst_create_date'] = pd.to_datetime(df['cst_create_date'], errors='coerce').dt.strftime('%Y-%m-%d')
    df = df.drop_duplicates(subset=['cst_id'])

    # --- LOAD ---
    cols = ['cst_id', 'cst_key', 'cst_firstname', 'cst_lastname', 'cst_marital_status', 'cst_gndr', 'cst_create_date']
    df_to_upload = df[cols].copy()
    df_to_upload['cst_id'] = pd.to_numeric(df_to_upload['cst_id'], errors='coerce').fillna(0).astype(int)
    df_to_upload = df_to_upload.replace({np.nan: None})

    cursor.execute("""
        IF OBJECT_ID('transformation.cust_info_cleaned', 'U') IS NOT NULL
            DROP TABLE transformation.cust_info_cleaned;
        
        CREATE TABLE transformation.cust_info_cleaned (
            cst_id INT,
            cst_key VARCHAR(100),
            cst_firstname VARCHAR(255),
            cst_lastname VARCHAR(255),
            cst_marital_status VARCHAR(50),
            cst_gndr VARCHAR(50),
            cst_create_date DATE
        );
    """)

    insert_sql = "INSERT INTO transformation.cust_info_cleaned VALUES (?, ?, ?, ?, ?, ?, ?)"
    data_to_insert = [tuple(x) for x in df_to_upload.to_numpy()]
    
    cursor.fast_executemany = True 
    cursor.executemany(insert_sql, data_to_insert)

    conn.commit()
    print(f"🚀 Success! {len(df_to_upload)} cleaned rows saved to 'transformation.cust_info_cleaned'.")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if 'conn' in locals(): 
        conn.close()
        print("🔌 Connection closed.")