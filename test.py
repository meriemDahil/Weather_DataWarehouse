import numpy as np
import pandas as pd
import mysql.connector
"""
# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    database="testdb"  
)

cursor = conn.cursor()
cursor.execute("USE testdb;")
csv_file = 'Classeur1.csv'

create_table_query = """
# CREATE TABLE IF NOT EXISTS data (
#     STATION VARCHAR(255),
#     NAME VARCHAR(255)
# );
"""
cursor.execute(create_table_query)

with open(csv_file, 'r') as file:
    next(file) 
    for line in file:
        data = line.strip().split(',')
        cursor.execute("INSERT INTO data VALUES (%s, %s)", data[:2])


conn.commit()
cursor.close()
conn.close()

"""


file_paths = ['Weather_1920-1959_TUNISIA.csv', 'Weather_1960-1989_TUNISIA.csv','Weather_1990-2019_TUNISIA.csv','Weather_2020-2022_TUNISIA.csv']
dataframes = [pd.read_csv(file) for file in file_paths]


for i, df in enumerate(dataframes):
    print(f"DataFrame {i+1} shape:")
    print(df.shape) 
    print(f"DataFrame {i+1} Info:")
    print(df.info())  
    
    print("\n")
    non_null_counts = df.notna().sum()
    print(non_null_counts)
    print("\n")

for i, df in enumerate(dataframes):
    pd.set_option('display.max_columns', None)  
    pd.set_option('display.max_rows', None)
    print(f"First few rows of DataFrame {i+1}:")
    print(df.head())
    print("\n")

print("\n")


for i, df in enumerate(dataframes):
    
    columns_with_only_null = df.columns[df.isna().sum() > 70000]
    print(columns_with_only_null)
    print("\n")


threshold = 1000  

non_null_counts = df.notna().sum()

columns_drop = non_null_counts[non_null_counts < threshold].index
df_filled = df.drop(columns=columns_drop)

