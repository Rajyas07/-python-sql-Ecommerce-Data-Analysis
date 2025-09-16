import pandas as pd
import mysql.connector
import os

# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('geolocation.csv', 'geolocation'),
    ('orders.csv', 'orders'),
    ('order_items.csv', 'order_items'),
    ('products.csv', 'products'),
    ('sellers.csv', 'sellers'),
    ('payments.csv', 'payments')
]

# Custom schema definitions (override pandas detection where needed)
schema_dict = {
    "customers": {
        "customer_id": "VARCHAR(50) PRIMARY KEY",
        "customer_unique_id": "VARCHAR(50)",
        "customer_zip_code_prefix": "INT",
        "customer_city": "TEXT",
        "customer_state": "TEXT"
    },
    "geolocation": {
        "geolocation_zip_code_prefix": "INT",
        "geolocation_lat": "FLOAT",
        "geolocation_lng": "FLOAT",
        "geolocation_city": "TEXT",
        "geolocation_state": "TEXT"
    },
    "orders": {
        "order_id": "VARCHAR(50) PRIMARY KEY",
        "customer_id": "VARCHAR(50)",
        "order_status": "TEXT",
        "order_purchase_timestamp": "DATETIME",
        "order_approved_at": "DATETIME",
        "order_delivered_carrier_date": "DATETIME",
        "order_delivered_customer_date": "DATETIME",
        "order_estimated_delivery_date": "DATETIME"
    },
    "order_items": {
        "order_id": "VARCHAR(50)",
        "order_item_id": "INT",
        "product_id": "VARCHAR(50)",
        "seller_id": "VARCHAR(50)",
        "shipping_limit_date": "DATETIME",
        "price": "DECIMAL(10,2)",
        "freight_value": "DECIMAL(10,2)"
    },
    "products": {
        "product_id": "VARCHAR(50) PRIMARY KEY",
        "product_category": "TEXT",
        "product_name_length": "INT",
        "product_description_length": "INT",
        "product_photos_qty": "INT",
        "product_weight_g": "FLOAT",
        "product_length_cm": "FLOAT",
        "product_height_cm": "FLOAT",
        "product_width_cm": "FLOAT"
    },
    "sellers": {
        "seller_id": "VARCHAR(50) PRIMARY KEY",
        "seller_zip_code_prefix": "INT",
        "seller_city": "TEXT",
        "seller_state": "TEXT"
    },
    "payments": {
        "order_id": "VARCHAR(50)",
        "payment_sequential": "INT",
        "payment_type": "TEXT",
        "payment_installments": "INT",
        "payment_value": "DECIMAL(10,2)"
    }
}

# Connect to the MySQL database
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Password',
    database='ecommerce'
)
cursor = conn.cursor()

# Folder containing the CSV files
folder_path = 'C:\\Users\\Dell\\OneDrive\\Desktop\\Ecommerce'

for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    
    # Parse dates if datetime columns exist in schema
    date_cols = [col for col, dtype in schema_dict[table_name].items() if "DATETIME" in dtype]
    df = pd.read_csv(file_path, parse_dates=date_cols if date_cols else None)
    
    # Replace NaN with None for SQL NULL
    df = df.where(pd.notnull(df), None)

    print(f"Processing {csv_file} → {table_name} ({len(df)} rows)")
    
    # Drop table if exists
    cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
    
    # Generate CREATE TABLE query from schema dict
    columns = ', '.join([f'`{col}` {dtype}' for col, dtype in schema_dict[table_name].items()])
    create_table_query = f'CREATE TABLE `{table_name}` ({columns})'
    cursor.execute(create_table_query)

    # Prepare INSERT query
    cols = ', '.join([f'`{col}`' for col in df.columns])
    placeholders = ', '.join(['%s'] * len(df.columns))
    sql = f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})"
    
    # Convert DataFrame rows to list of tuples
    values_list = [tuple(None if pd.isna(x) else x for x in row) for row in df.itertuples(index=False)]
    
    # Bulk insert
    cursor.executemany(sql, values_list)
    conn.commit()

print(" All CSVs successfully loaded into MySQL database.")

# Close connection
cursor.close()
conn.close()
