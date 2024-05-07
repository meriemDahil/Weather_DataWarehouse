import pandas as pd
import pymysql.cursors
import os

host = '................'
user = '.........'
database = '............'
csv_dir = 'MO_TU'

csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]

all_dataframes = []

for csv_file in csv_files:
    file_path = os.path.join(csv_dir, csv_file)
    df = pd.read_csv(file_path, low_memory=False)
    all_dataframes.append(df)

# Concatenate all dataframes into one
dataframes = pd.concat(all_dataframes, ignore_index=True)

# Keep only the necessary columns
columns_to_keep = ["STATION", "NAME", "LATITUDE", "LONGITUDE", "ELEVATION", "DATE", "PRCP", "PRCP_ATTRIBUTES", "TAVG", "TAVG_ATTRIBUTES", "TMAX", "TMAX_ATTRIBUTES", "TMIN", "TMIN_ATTRIBUTES"]
dataframes = dataframes[columns_to_keep]

# Reformat the date
dataframes['DATE'] = pd.to_datetime(dataframes['DATE'])

# Fill NaN values with mean for numerical attributes and mode for object attributes
for column in dataframes.columns:
    if dataframes[column].dtype == 'object':
        dataframes[column].fillna(dataframes[column].mode()[0], inplace=True)
    else:
        dataframes[column].fillna(dataframes[column].mean(), inplace=True)
        
# Split the 'NAME' column into 'STATION_CODE' and 'LOCATION' location for country
dataframes[['STATION_CODE', 'LOCATION']] = dataframes['NAME'].str.split(', ', expand=True)
dataframes['DATE'] = pd.to_datetime(dataframes['DATE'])
# Drop the original 'NAME' column
dataframes.drop(columns=['NAME'], inplace=True)

# Ensure 'STATION_CODE' and 'LOCATION' columns are formatted correctly
dataframes['STATION_CODE'] = dataframes['STATION_CODE'].astype(str)
dataframes['LOCATION'] = dataframes['LOCATION'].astype(str)

# Add ID_date and ID_station columns with unique identifiers
dataframes['ID_date'] = pd.Series(range(1, len(dataframes) + 1))
dataframes['ID_station'] = pd.Series(range(1, len(dataframes) + 1))

try:
    # Connect to MySQL 
    with pymysql.connect(host=host, user=user, database=database, cursorclass=pymysql.cursors.DictCursor) as connection:
        
        with connection.cursor() as cursor:
            # Create Dimension_Station table
            create_station_table_sql = """
            CREATE TABLE IF NOT EXISTS Dimension_Station (
                ID_station INT AUTO_INCREMENT PRIMARY KEY,
                STATION VARCHAR(255),
                LATITUDE FLOAT,
                LONGITUDE FLOAT,
                ELEVATION FLOAT,
                LOCATION VARCHAR(255)
            )
            """
            cursor.execute(create_station_table_sql)

            # Insert data into Dimension_Station table
            for index, row in dataframes.iterrows():
                insert_station_sql = """
                INSERT INTO Dimension_Station (STATION, LATITUDE, LONGITUDE, ELEVATION, LOCATION)
                VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(insert_station_sql, (row['STATION'], row['LATITUDE'], row['LONGITUDE'], row['ELEVATION'], row['LOCATION']))

            # Create Dimension_Date table
            create_date_table_sql = """
            CREATE TABLE IF NOT EXISTS Dimension_Date (
                ID_date INT AUTO_INCREMENT PRIMARY KEY,
                DATE DATE,
                MOIS INT,
                ANNÉE INT,
                JOUR INT,
                TRIMESTRE INT,
                SAISON VARCHAR(255)
            )
            """
            cursor.execute(create_date_table_sql)

            # Insert data into Dimension_Date table
            for index, row in dataframes.iterrows():
                mois = row['DATE'].month
                année = row['DATE'].year
                jour = row['DATE'].day
                trimestre = (row['DATE'].month - 1) // 3 + 1
                saison = 'Printemps' if 3 <= row['DATE'].month <= 5 else \
                         'Été' if 6 <= row['DATE'].month <= 8 else \
                         'Automne' if 9 <= row['DATE'].month <= 11 else 'Hiver'
                insert_date_sql = """
                INSERT INTO Dimension_Date (DATE, MOIS, ANNÉE, JOUR, TRIMESTRE, SAISON)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_date_sql, (row['DATE'], mois, année, jour, trimestre, saison))

            # Create Dimension_Mesures table with foreign keys
            create_table3_sql = """
            CREATE TABLE IF NOT EXISTS Dimension_Mesures (
                ID_mesure INT AUTO_INCREMENT PRIMARY KEY,
                DATE_fk DATE,
                PRCP FLOAT,
                TAVG FLOAT,
                TMAX FLOAT,
                TMIN FLOAT,
                LOCATION_fk VARCHAR(25),
                ID_date INT,
                ID_station INT,
                FOREIGN KEY (ID_date) REFERENCES Dimension_Date(ID_date),
                FOREIGN KEY (ID_station) REFERENCES Dimension_Station(ID_station)
            )
            """
            cursor.execute(create_table3_sql)

            # Insert data into Dimension_Mesures table
            for index, row in dataframes.iterrows():
                insert_mesures_sql = """
                INSERT INTO Dimension_Mesures (DATE_fk, PRCP, TAVG, TMAX, TMIN, LOCATION_fk, ID_date, ID_station)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_mesures_sql, (row['DATE'], row['PRCP'], row['TAVG'], row['TMAX'], row['TMIN'], row['LOCATION'], row['ID_date'], row['ID_station']))

        # Commit the transaction
        connection.commit()
        print("Tables created and data inserted successfully.")

except Exception as e:
    # Rollback the transaction if an error occurs
    print(f"Error creating tables or inserting data to MySQL: {e}")
