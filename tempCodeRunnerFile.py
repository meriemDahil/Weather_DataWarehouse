
try:
    # Open cursor
    with connection.cursor() as cursor:
        # Create Dimension Géographique table
        create_dim_geographique_sql = """
        CREATE TABLE IF NOT EXISTS dimension_geographique (
            id_geographie INT AUTO_INCREMENT PRIMARY KEY,
            latitude FLOAT,
            longitude FLOAT,
            elevation INT
        )
        """
        cursor.execute(create_dim_geographique_sql)
        
        # Create Dimension Station table
        create_dim_station_sql = """
        CREATE TABLE IF NOT EXISTS dimension_station (
            id_station INT AUTO_INCREMENT PRIMARY KEY,
            code_station VARCHAR(255),
            nom_station VARCHAR(255)
        )
        """
        cursor.execute(create_dim_station_sql)

        # Create Table des Faits MesuresMétéorologiques table
        create_table_des_faits_sql = """
        CREATE TABLE IF NOT EXISTS mesures_meteorologiques (
            ID_Mesure INT AUTO_INCREMENT PRIMARY KEY,
            id_date DATE,
            id_geographie INT,
            id_station INT,
            precipitation FLOAT,
            epaisseur_neige FLOAT,
            chute_neige FLOAT,
            temperature_moyenne FLOAT,
            temperature_max FLOAT,
            temperature_min FLOAT,
            direction_vent_max FLOAT,
            heure_pointe_rafale FLOAT,
            vitesse_vent_max FLOAT,
            types_temps VARCHAR(255)
        )
        """
        cursor.execute(create_table_des_faits_sql)

        # Insert data into the Dimension Géographique table
        for index, row in dataframes.iterrows():
            insert_dim_geographique_sql = """
            INSERT INTO dimension_geographique (latitude, longitude, elevation)
            VALUES (%s, %s, %s)
            """
            cursor.execute(insert_dim_geographique_sql, (row['LATITUDE'], row['LONGITUDE'], row['ELEVATION']))

        # Insert data into the Dimension Station table
        for index, row in dataframes.iterrows():
            insert_dim_station_sql = """
            INSERT INTO dimension_station (code_station, nom_station)
            VALUES (%s, %s)
            """
            cursor.execute(insert_dim_station_sql, (row['STATION'], row['NAME']))

        # Insert data into the Table des Faits MesuresMétéorologiques table
        # Insert data into the Table des Faits MesuresMétéorologiques table
        for index, row in dataframes.iterrows():
            # Generate a unique identifier for id_geographie
            id_geographie = index + 1
            insert_table_des_faits_sql = """
            INSERT INTO mesures_meteorologiques (id_date, id_geographie, id_station, precipitation, epaisseur_neige, chute_neige, temperature_moyenne, temperature_max, temperature_min, direction_vent_max, heure_pointe_rafale, vitesse_vent_max, types_temps)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
    # Commit the transaction
    connection.commit()
    print("Tables created and data inserted successfully.")

except Exception as e:
    # Rollback the transaction if an error occurs
    connection.rollback()
    print(f"Error creating tables or inserting data to MySQL: {e}")

finally:
    # Close the connection
    connection.close()
cursor.execute(insert_table_des_faits_sql, (row['DATE'], id_geographie, row['STATION'], row['PRCP'], row['TAVG'], row['TMAX'], row['TMIN'], row['PRCP_ATTRIBUTES'], row['TAVG_ATTRIBUTES'], row['TMAX_ATTRIBUTES'], row['TMIN_ATTRIBUTES'], row['PRCP_ATTRIBUTES'], row['TAVG_ATTRIBUTES'], row['TMAX_ATTRIBUTES'], row['TMIN_ATTRIBUTES']))
