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


file_paths = ['Weather_1920-1959_MOROCCO.csv', 'Weather_1960-1989_MOROCCO.csv','Weather_1990-2019_MOROCCO.csv',
              'Weather_2020-2022_MOROCCO.csv','Weather_1920-1959_TUNISIA.csv','Weather_1960-1989_TUNISIA.csv',
              'Weather_1990-2019_TUNISIA.csv','Weather_2020-2022_TUNISIA.csv']

dataframes = [pd.read_csv(file,low_memory=False) for file in file_paths]

dataframe_copy=dataframes

file_paths_Morocco=['Weather_1920-1959_MOROCCO.csv', 'Weather_1960-1989_MOROCCO.csv',
                    'Weather_1990-2019_MOROCCO.csv','Weather_2020-2022_MOROCCO.csv']

file_paths_Tunisia=['Weather_1920-1959_TUNISIA.csv','Weather_1960-1989_TUNISIA.csv',
                    'Weather_1990-2019_TUNISIA.csv','Weather_2020-2022_TUNISIA.csv']

dataframes_Morocco = [pd.read_csv(file,low_memory=False) for file in file_paths_Morocco]
dataframes_Tunisia = [pd.read_csv(file,low_memory=False) for file in file_paths_Tunisia]

dataframes_Morocco_copy=dataframes_Morocco.copy()
dataframes_Tunisia_copy=dataframes_Tunisia.copy()

for i, df in enumerate(dataframe_copy):
    
    common_columns = set.intersection(*(set(df.columns) for df in dataframe_copy))


for df in dataframe_copy:

    columns_to_drop = [col for col in df.columns if col not in common_columns]
    if columns_to_drop:
        df.drop(columns=columns_to_drop, inplace=True)

    print(columns_to_drop)
    print(df.shape)


for i, df in enumerate(dataframe_copy):
    print(f"DataFrame {i+1} shape:")
    print(df.shape) 
    # print(f"DataFrame {i+1} Info:")
    # print(df.info())  
    print("\n")
    #non_null_counts = df.notna().sum()
    #print(non_null_counts)
    print("\n")

"""
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

"""

