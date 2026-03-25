import pandas as pd
import pyodbc
import numpy as np
import warnings

# Suppress the pandas/SQLAlchemy warning
warnings.filterwarnings("ignore", category=UserWarning)

server = r'LAPTOP-UATOMV5O\SQLEXPRESS'
database = 'dwh'
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # 1. Ensure curated schema exists
    cursor.execute("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'curated') BEGIN EXEC('CREATE SCHEMA curated') END")
    conn.commit()

    # 2. Extract Data
    prd_df = pd.read_sql("SELECT * FROM transformation.prd_info_cleaned", conn)
    cat_df = pd.read_sql("SELECT * FROM Ingestion.px_cat_g1v2", conn)

    # 3. Identify the ID column in Category table
    # Your tutor's code used 'id', but in your SQL it might be 'G1V2_ID' or 'cat_id'
    # This logic automatically finds the right column name to prevent the 'id' error
    cat_id_col = [col for col in cat_df.columns if 'id' in col.lower()][0]
    print(f"🔗 Linking Product 'cat_id' to Category '{cat_id_col}'")

    # 4. Join
    df = pd.merge(left=prd_df, right=cat_df, how="left", left_on="cat_id", right_on=cat_id_col)

    # 5. Build the Dimension Table
    dim_products = pd.DataFrame({
        "product_number": df["prd_key"],
        "product_name": df["prd_nm"],
        "category_id": df["cat_id"],
        "category": df.get("cat", df.get("CAT_NM", "N/A")), # Handles different possible column names
        "subcategory": df.get("subcat", df.get("SUBCAT_NM", "N/A")),
        "cost": df["prd_cost"],
        "product_line": df["prd_line"],
        "start_date": df["prd_start_dt"],
        "end_date": df["prd_end_dt"]
    })

    # 6. Add Surrogate Key (Standard Data Warehouse Practice)
    dim_products = dim_products.sort_values("product_number").reset_index(drop=True)
    dim_products.insert(0, "product_key", dim_products.index + 1)

    # 7. LOAD TO SQL SERVER
    cursor.execute("IF OBJECT_ID('curated.dim_products', 'U') IS NOT NULL DROP TABLE curated.dim_products")
    cursor.execute("""
        CREATE TABLE curated.dim_products (
            product_key INT, 
            product_number VARCHAR(100), 
            product_name VARCHAR(255),
            category_id VARCHAR(50), 
            category VARCHAR(100), 
            subcategory VARCHAR(100),
            cost FLOAT, 
            product_line VARCHAR(50), 
            start_date DATE, 
            end_date DATE
        )
    """)
    
    cursor.fast_executemany = True
    data_to_insert = [tuple(x) for x in dim_products.replace({np.nan: None}).to_numpy()]
    cursor.executemany("INSERT INTO curated.dim_products VALUES (?,?,?,?,?,?,?,?,?,?)", data_to_insert)
    
    conn.commit()
    print("✅ Success! curated.dim_products loaded.")

except Exception as e:
    print(f"❌ Error: {e}")
finally:
    if 'conn' in locals():
        conn.close()