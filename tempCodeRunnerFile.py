            # # Create Dimension_Station table
            # create_station_table_sql = """
            # CREATE TABLE IF NOT EXISTS Dimension_Station (
            #     ID_station INT AUTO_INCREMENT PRIMARY KEY,
            #     STATION VARCHAR(255),
            #     LATITUDE FLOAT,
            #     LONGITUDE FLOAT,
            #     ELEVATION FLOAT,
            #     LOCATION VARCHAR(255)
            # )
            # """
            # cursor.execute(create_station_table_sql)

            # # Insert data into Dimension_Station table
            # for index, row in dataframes.iterrows():
            #     insert_station_sql = """
            #     INSERT INTO Dimension_Station (STATION, LATITUDE, LONGITUDE, ELEVATION, LOCATION)
            #     VALUES (%s, %s, %s, %s, %s)
            #     """
            #     cursor.execute(insert_station_sql, (row['STATION'], row['LATITUDE'], row['LONGITUDE'], row['ELEVATION'], row['LOCATION']))

            # # Create Dimension_Date table
            # create_date_table_sql = """
            # CREATE TABLE IF NOT EXISTS Dimension_Date (
            #     ID_date INT AUTO_INCREMENT PRIMARY KEY,
            #     DATE DATE,
            #     MOIS INT,
            #     ANNÉE INT,
            #     JOUR INT,
            #     TRIMESTRE INT,
            #     SAISON VARCHAR(255)
            # )
            # """
            # cursor.execute(create_date_table_sql)

            # # Insert data into Dimension_Date table
            # for index, row in dataframes.iterrows():
            #     mois = row['DATE'].month
            #     année = row['DATE'].year
            #     jour = row['DATE'].day
            #     trimestre = (row['DATE'].month - 1) // 3 + 1
            #     saison = 'Printemps' if 3 <= row['DATE'].month <= 5 else \
            #              'Été' if 6 <= row['DATE'].month <= 8 else \
            #              'Automne' if 9 <= row['DATE'].month <= 11 else 'Hiver'
            #     insert_date_sql = """
            #     INSERT INTO Dimension_Date (DATE, MOIS, ANNÉE, JOUR, TRIMESTRE, SAISON)
            #     VALUES (%s, %s, %s, %s, %s, %s)
            #     """
            #     cursor.execute(insert_date_sql, (row['DATE'], mois, année, jour, trimestre, saison))
