import pandas as pd
import pyodbc
import numpy as np

server = r'LAPTOP-UATOMV5O\SQLEXPRESS'
database = 'dwh'
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Ensure the 'transformation' schema exists
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'transformation')
        BEGIN
            EXEC('CREATE SCHEMA transformation')
        END
    """)
    conn.commit()

    # --- EXTRACT ---
    # Load the local CSV file
    df = pd.read_csv('CUST_AZ12.csv')
    print(f"✅ Data extracted: {len(df)} rows found.")

    # --- TRANSFORM ---
    # 1. Fix CID: Keep only the last 5 digits and convert to clean Integer (removes .0)
    # This turns 'NASAW00011000' into 11000
    df['CID'] = df['CID'].astype(str).str.strip().str[-5:]
    df['CID'] = pd.to_numeric(df['CID'], errors='coerce').fillna(0).astype(int)

    # 2. Fix Gender Consistency (GEN column)
    df['GEN'] = df['GEN'].astype(str).str.strip()
    gender_map = {
        'M': 'Male', 'M ': 'Male', 'Male': 'Male',
        'F': 'Female', 'F ': 'Female', 'Female': 'Female'
    }
    df['GEN'] = df['GEN'].replace(gender_map)
    
    # 3. Handle empty strings, 'nan', and NULLs in Gender
    df['GEN'] = df['GEN'].replace(['', 'nan', 'None'], np.nan).fillna('N/A')

    # --- LOAD ---
    # Explicitly create transformation.cst_az12_info_cleaned
    cursor.execute("""
        IF OBJECT_ID('transformation.cst_az12_info_cleaned', 'U') IS NOT NULL
            DROP TABLE transformation.cst_az12_info_cleaned;
        
        CREATE TABLE transformation.cst_az12_info_cleaned (
            cid INT,
            bdate DATE,
            gender VARCHAR(50)
        );
    """)

    insert_sql = "INSERT INTO transformation.cst_az12_info_cleaned VALUES (?, ?, ?)"
    # Convert dataframe to list of tuples for pyodbc
    data_to_insert = [tuple(x) for x in df.to_numpy()]
    
    cursor.fast_executemany = True 
    cursor.executemany(insert_sql, data_to_insert)

    conn.commit()
    print(f"🚀 Success! Cleaned data saved to 'transformation.cst_az12_info_cleaned'.")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    if 'conn' in locals(): 
        conn.close()
        print("🔌 Connection closed.")