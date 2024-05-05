import pandas as pd
import pymysql.cursors

# MySQL connection parameters
host = 'localhost'
user = 'root'
database = 'testdatabase'

# Read the new CSV file into a DataFrame
dataframes = pd.read_csv('Weather_1960-1989_MOROCCO.csv', low_memory=False)
# Drop unused columns
columns_to_keep = ["STATION", "NAME", "LATITUDE", "LONGITUDE", "ELEVATION", "DATE", "PRCP", "PRCP_ATTRIBUTES", "TAVG", "TAVG_ATTRIBUTES", "TMAX", "TMAX_ATTRIBUTES", "TMIN", "TMIN_ATTRIBUTES"]
dataframes = dataframes[columns_to_keep]
print(dataframes.info)
# Ensure 'DATE' column is formatted correctly
dataframes['DATE'] = pd.to_datetime(dataframes['DATE'])

# Fill NaN values with mean for numerical attributes and mode for object attributes
for column in dataframes.columns:
    if dataframes[column].dtype == 'object':
        dataframes[column].fillna(dataframes[column].mode()[0], inplace=True)
    else:
        dataframes[column].fillna(dataframes[column].mean(), inplace=True)
        
# Modify the DataFrame to split the 'NAME' column into 'STATION_CODE' and 'LOCATION'
dataframes[['STATION_CODE', 'LOCATION']] = dataframes['NAME'].str.split(', ', expand=True)

# Drop the original 'NAME' column
dataframes.drop(columns=['NAME'], inplace=True)

# Ensure 'STATION_CODE' and 'LOCATION' columns are formatted correctly
dataframes['STATION_CODE'] = dataframes['STATION_CODE'].astype(str)
dataframes['LOCATION'] = 212  # Assuming a constant value for LOCATION for now

try:
    # Establish a connection to MySQL database using pymysql
    with pymysql.connect(host=host, user=user, database=database, cursorclass=pymysql.cursors.DictCursor) as connection:
        # Open cursor
        with connection.cursor() as cursor:
            # Create Dimension_Géographique table
             create_table_sql = """
                CREATE TABLE IF NOT EXISTS Dimension_Station (
                    STATION VARCHAR(255) ,
                    NAME VARCHAR(255),
                    LATITUDE FLOAT,
                    LONGITUDE FLOAT,
                    ELEVATION FLOAT
                )
                """
             cursor.execute(create_table_sql)

            # Insert data into Dimension_Station table
             for index, row in dataframes.iterrows():
                # Prepare the SQL query
                insert_station_sql = """
                INSERT INTO Dimension_Station (STATION, NAME, LATITUDE, LONGITUDE, ELEVATION)
                VALUES (%s, %s, %s, %s, %s)
                """
                # Execute the SQL query
                cursor.execute(insert_station_sql, (row['STATION'], row['STATION_CODE'], row['LATITUDE'], row['LONGITUDE'], row['ELEVATION']))

            # Create Dimension_Mesures table
             create_table3_sql = """
            CREATE TABLE IF NOT EXISTS Dimension_Mesures (
                DATE DATE,
                PRCP FLOAT,
                TAVG FLOAT,
                TMAX FLOAT,
                TMIN FLOAT,
                STATION_CODE VARCHAR(30),
                LOCATION INT
            )
            """
             cursor.execute(create_table3_sql)

            # Insert data into Dimension_Station table
             for index, row in dataframes.iterrows():
                # Prepare the SQL query
                insert_station_sql = """
                INSERT INTO Dimension_Station (STATION, NAME)
                VALUES (%s, %s)
                """
                # Execute the SQL query
                cursor.execute(insert_station_sql, (row['STATION'], row['STATION_CODE']))

                # Insert data into Dimension_Mesures table
                insert_mesures_sql = """
                INSERT INTO Dimension_Mesures (DATE, PRCP, TAVG, TMAX, TMIN, STATION_CODE, LOCATION)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_mesures_sql, (row['DATE'], row['PRCP'], row['TAVG'], row['TMAX'], row['TMIN'], row['STATION_CODE'], row['LOCATION']))
            
        # Commit the transaction
        connection.commit()
        print("Tables created and data inserted successfully.")

except Exception as e:
    # Rollback the transaction if an error occurs
    print(f"Error creating tables or inserting data to MySQL: {e}")
