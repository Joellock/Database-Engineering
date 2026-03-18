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
    df = pd.read_csv('LOC_A101.csv')
    print(f"✅ Data extracted: {len(df)} rows found.")

    # --- TRANSFORM ---
    # 1. Fix ID: Extract last 5 digits and convert to clean Integer (removes .0)
    df['CID'] = df['CID'].astype(str).str.strip().str[-5:]
    df['CID'] = pd.to_numeric(df['CID'], errors='coerce').fillna(0).astype(int)

    # 2. Fix Countries: Trim whitespace and map codes to full names
    df['CNTRY'] = df['CNTRY'].astype(str).str.strip()
    country_map = {
        'DE': 'Germany', 
        'De': 'Germany',
        'US': 'United States', 
        'USA': 'United States'
    }
    df['CNTRY'] = df['CNTRY'].replace(country_map)

    # 3. Handle NULLs and empty strings for Country
    df['CNTRY'] = df['CNTRY'].replace(['', 'nan', 'None'], np.nan).fillna('N/A')

    # --- LOAD ---
    # Note: cid is now defined as INT to match your joining strategy
    cursor.execute("""
        IF OBJECT_ID('transformation.loc_info_cleaned', 'U') IS NOT NULL
            DROP TABLE transformation.loc_info_cleaned;
        
        CREATE TABLE transformation.loc_info_cleaned (
            cid INT,
            country VARCHAR(100)
        );
    """)

    insert_sql = "INSERT INTO transformation.loc_info_cleaned VALUES (?, ?)"
    # Ensure data is clean for upload
    data_to_insert = [tuple(x) for x in df.to_numpy()]
    
    cursor.fast_executemany = True 
    cursor.executemany(insert_sql, data_to_insert)

    conn.commit()
    print(f"🚀 Success! {len(df)} rows saved to 'transformation.loc_info_cleaned'.")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if 'conn' in locals(): 
        conn.close()
        print("🔌 Connection closed.")