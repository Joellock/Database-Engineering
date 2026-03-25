import pandas as pd
import pyodbc
import numpy as np
import warnings

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)

server = r'LAPTOP-UATOMV5O\SQLEXPRESS'
database = 'dwh'
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;'

try:
    conn = pyodbc.connect(conn_str)
    
    # 1. Extract the clean data from Transformation and the new Dimension keys from Curated
    sales_df = pd.read_sql("SELECT * FROM transformation.sales_details_cleaned", conn)
    dim_prd = pd.read_sql("SELECT product_key, product_number FROM curated.dim_products", conn)
    dim_cust = pd.read_sql("SELECT customer_key, customer_id FROM curated.dim_customers", conn)

    print("✅ Cleaned sales and dimension keys loaded.")

    # 2. Join Sales to Product Dimension to get 'product_key'
    df = pd.merge(sales_df, dim_prd, left_on="sls_prd_key", right_on="product_number", how="left")
    
    # 3. Join Result to Customer Dimension to get 'customer_key'
    df = pd.merge(df, dim_cust, left_on="sls_cust_id", right_on="customer_id", how="left")

    # 4. Build the Final Fact Table
    # Note: We use the names from your tutor's PostgreSQL example
    fact_sales = pd.DataFrame({
        "product_key": df["product_key"],
        "customer_key": df["customer_key"],
        "order_number": df["sls_ord_num"],
        "order_date": df["sls_order_dt"],
        "shipping_date": df["sls_ship_dt"],
        "due_date": df["sls_due_dt"],
        "sales": df["sls_sales"],
        "quantity": df["sls_quantity"],
        "price": df["sls_price"]
    })

    # 5. Add a unique Sales Key (Surrogate Key for the fact table)
    fact_sales.insert(0, "sales_key", fact_sales.index + 1)

    # 6. LOAD TO SQL SERVER
    cursor = conn.cursor()
    cursor.execute("IF OBJECT_ID('curated.fact_sales', 'U') IS NOT NULL DROP TABLE curated.fact_sales")
    cursor.execute("""
        CREATE TABLE curated.fact_sales (
            sales_key INT, 
            product_key INT, 
            customer_key INT, 
            order_number VARCHAR(50),
            order_date DATE, 
            shipping_date DATE, 
            due_date DATE, 
            sales DECIMAL(10,2),
            quantity INT, 
            price DECIMAL(10,2)
        )
    """)
    
    cursor.fast_executemany = True
    data_to_insert = [tuple(x) for x in fact_sales.replace({np.nan: None}).to_numpy()]
    cursor.executemany("INSERT INTO curated.fact_sales VALUES (?,?,?,?,?,?,?,?,?,?)", data_to_insert)
    
    conn.commit()
    print("🚀 Success! Final fact_sales loaded into curated.fact_sales.")
    print("✨ Your Star Schema is now complete.")

except Exception as e:
    print(f"❌ Error: {e}")
finally:
    if 'conn' in locals():
        conn.close()