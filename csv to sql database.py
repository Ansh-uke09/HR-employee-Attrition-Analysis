import pandas as pd
import mysql.connector
import os

# List of CSV files and their corresponding table names
csv_files = [
    ('HR Employee Attrition.csv', 'HR_Employee_Attrition'),
      
]

# Folder containing the CSV files
folder_path = '/Users/mac/Desktop'

# MySQL connection config
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Ansh@123',
    database='Employee',
    auth_plugin='mysql_native_password'  # add this if you get auth errors
)
cursor = conn.cursor()

def get_sql_type(dtype):
    """Map pandas dtype to MySQL column type."""
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)

    print(f"Processing file: {csv_file}")

    # Read CSV with latin1 encoding to avoid Unicode errors
    df = pd.read_csv(file_path, encoding='latin1')

    # Replace NaN with None for SQL NULL compatibility
    df = df.where(pd.notnull(df), None)

    # Clean column names for SQL (replace spaces, dashes, dots)
    df.columns = [col.strip().replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

    # Generate CREATE TABLE query
    columns_definitions = ', '.join(
        f"`{col}` {get_sql_type(df[col].dtype)}"
        for col in df.columns
    )
    create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({columns_definitions})"
    cursor.execute(create_table_query)

    # Prepare INSERT query template
    cols = ', '.join(f"`{col}`" for col in df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))
    insert_query = f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})"

    # Batch insert for better performance (batch size 1000)
    batch_size = 1000
    data = [tuple(row) for row in df.itertuples(index=False, name=None)]

    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        cursor.executemany(insert_query, batch)
        conn.commit()

    print(f"Inserted {len(data)} rows into {table_name}")

cursor.close()
conn.close()
print("All CSV files imported successfully.")
