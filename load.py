import pandas as pd
import pyodbc

# 1. Connection Details
server = r'LAPTOP-UATOMV5O\SQLEXPRESS'
database = 'dwh'
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    print("Successfully connected to dwh for loading.")

    # Mapping your specific files to the tables we just created
    files_to_load = [
        ('cust_info.csv', 'cust_info'),
        ('prd_info.csv', 'prd_info'),
        ('sales_details.csv', 'sales_details'),
        ('CUST_AZ12.csv', 'CUST_AZ12'),
        ('LOC_A101.csv', 'LOC_A101'),
        ('PX_CAT_G1V2.csv', 'PX_CAT_G1V2')
    ]

    for file_name, table_name in files_to_load:
        df = pd.read_csv(file_name)
        
        # --- Date Cleaning for SQL Server ---
        for col in df.columns:
            if 'date' in col.lower() or 'dt' in col.lower() or col == 'BDATE':
                # Converts various formats (like 20101229 or DD-MM-YYYY) to YYYY-MM-DD
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Prepare the SQL Insert query dynamically based on file headers
        columns = ", ".join(df.columns)
        placeholders = ", ".join(['?' for _ in df.columns])
        insert_query = f"INSERT INTO schema1.{table_name} ({columns}) VALUES ({placeholders})"

        for index, row in df.iterrows():
            # Convert NaN/Empty values to None so SQL treats them as NULL
            cursor.execute(insert_query, [None if pd.isna(x) else x for x in row])
        
        print(f"✅ Loaded {len(df)} rows into schema1.{table_name}")

    conn.commit()
    print("\n🎉 ALL DATA LOADED SUCCESSFULLY!")

except Exception as e:
    print(f"❌ Error during load: {e}")

finally:
    if 'conn' in locals():
        conn.close()